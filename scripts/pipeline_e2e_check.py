# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""instruments-service data-pipeline end-to-end smoke check (`/data-pipeline-check-is`).

Proves, on REAL infrastructure (real VM launches, real adapter calls, all writes routed
to the ``-test-`` GCS bucket sibling via ``--test-run``), that for every ``(asset_group,
venue)`` shard on the operator's ``--day``:

  1. **force leg**  — a genuinely-missing (or force-refreshed) shard's CLI -> adapter ->
     GCS-write -> manifest-row contract actually works when forced.
  2. **skip leg**    — an already-captured shard's skip-if-fresh logic actually fires (the
     freshness pre-flight log line, checked via ``contains_skip_signal``) and
     leaves the previously-written object UNCHANGED (proven via a before/after
     ``object_signature`` comparison of one representative parquet). Per
     plan finding #2, IS's skip-leg is fully self-contained (the freshness read and the
     force-leg's write both resolve to the SAME test bucket under ``IS_TEST_RUN=true``),
     so a skip proven against a real pre-existing baseline is labeled ``skip_proof=
     "genuine"`` here — unlike MTDS, which needs a separate PROD pre-check.
  3. **live leg**    — the same launcher, ``--test-run``-scoped to venues that are actually
     in :data:`unified_api_contracts...mvp_scope.MVP_SCOPE`.

This is the instruments-service thin adapter over the shared
``unified_trading_library.pipeline_e2e_check`` engine (``launcher``/``shard_verify``/
``log_grep``/``report``) per ``unified-trading-pm``'s ``data_pipeline_e2e_check`` plan,
section 4. It reuses ``enumerate_cells``/``SmokeCell`` (+ the already-proven
``resolve_test_bucket``/``expected_write_prefix`` helpers) from the sibling
``smoke_matrix.py`` rather than re-deriving shard enumeration — see that module's
docstring for the cell model.

Real IS shard atom (confirmed via ``instruments_service/cli/main.py`` +
``instruments_service/engine/orchestrator/catalogue.py``): ``(asset_group, venue, day)``
ONLY — there is no ``--data-types``/``--instrument-ids``/``--shard-key`` on this CLI.
Per-venue instrument-type / data_type coverage is a REPORTING dimension layered on the
shard, never a separate shard key, so this script collapses ``smoke_matrix.py``'s finer
``(asset_group, venue, data_type)`` cells down to unique ``(asset_group, venue)`` targets
before driving anything (see :func:`_dedupe_shard_targets`).

Known infra gaps discovered while building this adapter (NOT fixable from this file —
flagged for the launcher-diff / setup-script owners):

  * ``deployment-service/scripts/vm/setup-data-pipeline-vm.sh``'s ``instruments-backfill``
    branch hardcodes ``BASE_CLI="--operation instruments --mode batch --asset-group ..."``
    — there is currently NO metadata path to select ``--mode live`` through
    ``launch-instruments-backfill-vm.sh``. So the "live leg" driven here, as of this
    writing, actually still exercises ``--mode batch`` under the hood rather than the
    true ``ScheduledIO``/``_Adapter`` live code path whose ``force=True`` behavior
    (``unified_trading_library/service_framework/_adapter.py:158``, "live always
    force-refreshes") the plan's live-leg rationale cites. This script still drives the
    live leg (per spec: same launcher, ``--test-run``, no explicit ``--force``,
    MVP-scoped) since that is the best available proxy today — the report labels a
    passing live-leg cell with this caveat inline (see ``_LIVE_LEG_CAVEAT``) rather than
    silently claiming it proved ``--mode live``. Needs either a ``VM_MODE``/``--live``
    addition to the setup script + launcher, or a dedicated live launcher, as a tracked
    follow-up.
  * The launcher-diff scope (plan section 2) adds ``--venues``/``--vm-name``/``--test-run``
    to ``launch-instruments-backfill-vm.sh`` but does not mention a ``--sports-provider``
    passthrough (-> ``VM_SPORTS_PROVIDER`` metadata, which ``setup-data-pipeline-vm.sh``
    already reads). Most SPORTS shards (instruments-service's own reference-data
    providers — API_FOOTBALL + T1 enrichment) are driven via ``--sports-provider``, not
    ``--venues`` (confirmed via ``smoke_matrix.py``'s own ``build_cli_args``); ONE SPORTS
    cell (bare ``BETFAIR`` — see ``smoke_matrix.py::_enumerate_sports_cells``) is
    venue-routed via ``--venues`` like any CEFI/DEFI/TRADFI venue (``cell.sports_provider``
    is the discriminator — see ``_build_launcher_argv`` below). The launcher needs the
    ``--sports-provider`` passthrough before a provider-routed SPORTS shard can actually be
    checked end-to-end; until then, those legs will fail at the CLI-arg-building step on the
    launcher side (VM_SPORTS_PROVIDER never gets set), which surfaces here as a normal
    per-shard ``failed`` outcome (shard-level isolation — the matrix still completes and
    reports every other asset_group). The REMAINING UAC-registry sports venues (ODDS_API,
    PINNACLE, BETFAIR_SB_UK, BETFAIR_EX_UK, BETFAIR_EX_EU, DRAFTKINGS, FANDUEL) are
    MTDS-owned (``NO_ADAPTER_YET`` in instruments-service's own
    ``venue_adapter_keys.py`` — registry-consolidation Decision C, 2026-06-29) and
    correctly enumerate ZERO cells here; see market-tick-data-service's own pipeline
    checker for those venues instead.

Usage::

    python scripts/pipeline_e2e_check.py --day 2026-07-01 \\
        --asset-group CEFI --venue BINANCE-FUTURES --legs force,skip,live

    # Full asset_group sweep, force+skip only:
    python scripts/pipeline_e2e_check.py --day 2026-07-01 --asset-group DEFI --legs force,skip
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from unified_api_contracts import VenueMapping
from unified_api_contracts.canonical.crosscutting.mvp_scope import (
    MVP_SCOPE,
    CeFiMvpRule,
    DeFiMvpRule,
    PredictionMvpRule,
    TradFiMvpRule,
)
from unified_api_contracts.registry.capability_declarations._defi import (
    KNOWN_CHAINS,
    parse_defi_venue,
)
from unified_trading_library import (
    PipelineCheckReport,
    ShardCheckResult,
    contains_skip_signal,
    fetch_run_log,
    get_project_id,
    get_storage_client,
    launch_vm_and_wait,
    object_signature,
    render_markdown,
    verify_manifest_row,
    verify_write,
    write_report,
)

# Same-directory import of the sibling smoke harness (no package/__init__.py in
# scripts/ — this sys.path shim matches the existing precedent in
# populate_is_index_v9_2026_06_19.py within this same directory).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from smoke_matrix import (
    SmokeCell,
    enumerate_cells,
    expected_write_prefix,
    resolve_test_bucket,
)

logger = logging.getLogger(__name__)

# The plan's write_report()/render_markdown() filename+frontmatter convention keys off
# `PipelineCheckReport.service` — set to the exact slug the docstring names so the emitted
# file lands at `data_pipeline_e2e_check_is_<date>.md` verbatim.
_REPORT_SERVICE_SLUG = "data_pipeline_e2e_check_is"
_DEFAULT_TIMEOUT_SEC = 1800
_DEFAULT_POLL_INTERVAL_SEC = 30
_GCE_NAME_MAX = 63
_VALID_LEGS = frozenset({"force", "skip", "live"})
_LIVE_LEG_CAVEAT = (
    " [NOTE: routed via launch-instruments-backfill-vm.sh, which currently always runs "
    "--mode batch under setup-data-pipeline-vm.sh -- this leg does not yet prove the "
    "true --mode live code path; see script module docstring]"
)

# VM-name prefix stub per the registered `VM_PREFIX_TO_BUCKET` entries
# (`deployment-service/scripts/vm/vm_zombie_watchdog.py`) — note PREDICTION's stub is the
# abbreviated "pred", matching the launcher's own `instr-backfill-pred` VM + the registry.
_AG_VM_PREFIX_STUB: dict[str, str] = {
    "CEFI": "cefi",
    "DEFI": "defi",
    "TRADFI": "tradfi",
    "SPORTS": "sports",
    "PREDICTION": "pred",
}
_LEG_STUB: dict[str, str] = {"force": "f", "skip": "s", "live": "l"}

# MVP_SCOPE rule types that declare a per-venue `venues: frozenset[str]` field (SportsMvpRule
# does not — sports MVP membership is league-scoped, handled as a special case below).
_VENUE_SCOPED_RULE_TYPES = (CeFiMvpRule, DeFiMvpRule, TradFiMvpRule, PredictionMvpRule)


# ---------------------------------------------------------------------------
# Shard enumeration (reuses smoke_matrix.py; collapses to the real IS shard atom)
# ---------------------------------------------------------------------------


def _dedupe_shard_targets(cells: list[SmokeCell]) -> list[SmokeCell]:
    """Collapse smoke_matrix.py's (asset_group, venue, data_type) cells to the real IS
    shard atom (asset_group, venue) — data_type/instrument-type coverage is a reporting
    dimension layered on the shard, never a separate shard key (confirmed ground truth:
    IS's CLI has no --data-types/--instrument-ids at all)."""
    seen: set[tuple[str, str]] = set()
    targets: list[SmokeCell] = []
    for cell in cells:
        key = (cell.asset_group, cell.venue)
        if key in seen:
            continue
        seen.add(key)
        targets.append(cell)
    return targets


# ---------------------------------------------------------------------------
# Manifest match-dict construction (mirrors the REAL write-time parse in
# instruments_service/engine/orchestrator/catalogue.py::_write_catalogue_record — not
# smoke_matrix.py's raw `venue == cell.venue` exact-match, which does not correctly match
# DeFi rows since the writer splits "PROTOCOL-CHAIN" into separate venue/chain columns).
# ---------------------------------------------------------------------------


def _manifest_match(cell: SmokeCell) -> dict[str, str]:
    """Build the availability-index column-filter dict for this shard."""
    ag = cell.asset_group.upper()
    if ag == "SPORTS" and cell.sports_provider:
        # Provider-routed SPORTS cells (API_FOOTBALL + T1 enrichment): the writer
        # keys these rows on a data_type derived from the provider name (venue=""
        # always) — smoke_matrix.py's own verify_manifest_row also skips the
        # venue/data_type filter for these and matches on asset_group + date alone;
        # mirrored here for the same reason (deriving the exact per-provider data_type
        # would duplicate _write_catalogue_record's private parsing without adding a
        # meaningful narrowing for this smoke check's purpose).
        return {"asset_group": ag}
    if ag == "DEFI" and "-" in cell.venue:
        protocol, chain = parse_defi_venue(cell.venue)
        if chain in KNOWN_CHAINS:
            return {"asset_group": ag, "venue": protocol.upper(), "chain": chain}
    # Venue-routed SPORTS (bare BETFAIR) writes through the SAME generic per-venue
    # instrument-catalog path as CEFI/DEFI/TRADFI (writers.py::_write_venue) — venue
    # IS a real, meaningful column for it, so it must NOT take the provider shortcut.
    return {"asset_group": ag, "venue": cell.venue}


# ---------------------------------------------------------------------------
# Benign pre-launch honest-empty detection
# ---------------------------------------------------------------------------


def _benign_pre_launch_start(cell: SmokeCell, day: str, manifest_ok: bool, manifest_status: str) -> str | None:
    """Return the venue's UAC start date when this shard-leg is a BENIGN pre-launch
    honest-empty case, else ``None``.

    instruments-service correctly writes an honest ``empty_confirmed`` reference-data
    row (there is no instrument universe to enumerate for a venue BEFORE its
    registered launch date) but NO parquet object, so the write-verification leg
    would otherwise mark a correct honest-absence day as a failure (issue:
    ``coinbase_cde_mtds_batch_adapter_missing_2026_07_13``, IS-leg section — e.g.
    COINBASE-CDE has ``venue_start_dates`` entry ``2026-07-10``, so a ``2026-07-09``
    check is legitimately empty). Only fires when the manifest row itself is a valid
    ``empty_confirmed`` (``manifest_ok`` True) AND ``day`` predates the venue's UAC
    start date. Both ``day`` and the start date are ``YYYY-MM-DD`` ISO strings, so a
    lexical ``<`` comparison matches chronological order."""
    if not (manifest_ok and manifest_status == "empty_confirmed"):
        return None
    start = VenueMapping().get_venue_start_date(cell.venue)
    if start is not None and day < start:
        return start
    return None


# ---------------------------------------------------------------------------
# MVP-venue gating for the live leg
# ---------------------------------------------------------------------------


def _venue_in_mvp_scope(asset_group: str, venue: str) -> bool:
    """True iff ``venue`` is within :data:`MVP_SCOPE` for ``asset_group``."""
    ag = asset_group.lower()
    if ag == "sports":
        # MVP membership for SPORTS is league-scoped (94 MVP football leagues), not
        # provider-scoped — every provider fetches whichever leagues are in scope, so the
        # live-leg smoke check exercises every provider rather than gating on a
        # provider-level MVP flag that does not exist in MVP_SCOPE.
        return True
    rule = MVP_SCOPE.get(ag)
    if not isinstance(rule, _VENUE_SCOPED_RULE_TYPES):
        return False
    return venue.upper() in {v.upper() for v in rule.venues}


# ---------------------------------------------------------------------------
# VM naming — GCE (RFC1035) safe, matches the registered `instr-backfill-{stub}-` prefix
# ---------------------------------------------------------------------------


def _slugify(value: str) -> str:
    """Lowercase, GCE-safe (``[a-z0-9-]``) slug — underscores/colons become hyphens."""
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "x"


def _shard_vm_name(asset_group: str, venue: str, leg: str, run_ts: str) -> str:
    """Build an RFC1035-legal VM name that still matches the registered
    ``instr-backfill-{stub}-`` prefix (``VM_PREFIX_TO_BUCKET`` in
    ``vm_zombie_watchdog.py`` — a ``-pipelinecheck-<ts>``-style suffix still matches per
    the plan's finding 4).

    The plan's literal template (``instr-backfill-{ag}-pipelinecheck-{run_ts}``) is
    extended here with a per-venue/per-leg suffix: this script processes multiple venues
    sequentially within one run under the SAME asset_group, and the bare template would
    collide across venues/legs (GCE instance names must be unique per zone).
    """
    stub = _AG_VM_PREFIX_STUB.get(asset_group.upper(), _slugify(asset_group))
    leg_stub = _LEG_STUB.get(leg, _slugify(leg))
    prefix = f"instr-backfill-{stub}-pchk-{run_ts}-{leg_stub}-"
    venue_slug = _slugify(venue)
    budget = _GCE_NAME_MAX - len(prefix)
    if len(venue_slug) > budget:
        digest = hashlib.sha256(venue.encode("utf-8")).hexdigest()[:6]
        keep = max(budget - len(digest) - 1, 1)
        venue_slug = f"{venue_slug[:keep]}-{digest}"
    name = f"{prefix}{venue_slug}".rstrip("-")
    return name[:_GCE_NAME_MAX]


def _default_launcher_path() -> Path:
    """Resolve deployment-service's IS backfill launcher, assuming the standard per-tab
    sibling-repo layout (``.../<tab>/deployment-service``, ``.../<tab>/instruments-service``)
    that every slot worktree uses. Override via ``--launcher-path`` if this ever runs
    outside that layout."""
    tab_root = Path(__file__).resolve().parents[2]
    return tab_root / "deployment-service" / "scripts" / "vm" / "launch-instruments-backfill-vm.sh"


def _build_launcher_argv(cell: SmokeCell, day: str, vm_name: str, project_id: str, *, force: bool) -> list[str]:
    """Build the launch-instruments-backfill-vm.sh argv for one shard-leg.

    Per the plan: ``--asset-group {ag} --venues {venue} --start {day} --end {day}
    --vm-name instr-backfill-{ag}-pipelinecheck-{run_ts} --test-run [--force]``.
    Provider-routed SPORTS cells (``cell.sports_provider`` set — API_FOOTBALL + T1
    enrichment) are driven via ``--sports-provider`` instead of ``--venues``
    (confirmed via smoke_matrix.py's own ``build_cli_args``) — see the module
    docstring's "known infra gaps" for the launcher-side flag this currently
    requires but does not yet implement. Every other cell — including venue-routed
    SPORTS (bare BETFAIR, ``cell.sports_provider is None``) — uses ``--venues`` like
    CEFI/DEFI/TRADFI; gating on ``asset_group == "SPORTS"`` alone (the pre-fix
    behaviour) incorrectly sent BETFAIR through ``--sports-provider BETFAIR``, which
    the real CLI does not recognise as a valid provider value.
    """
    argv = [
        "--asset-group",
        cell.asset_group,
        "--start",
        day,
        "--end",
        day,
        "--vm-name",
        vm_name,
        "--test-run",
        "--project",
        project_id,
    ]
    if cell.sports_provider:
        argv.extend(["--sports-provider", cell.venue])
    else:
        argv.extend(["--venues", cell.venue])
    if force:
        argv.append("--force")
    return argv


# ---------------------------------------------------------------------------
# GCS helper (representative-object fingerprinting for the skip-leg proof)
# ---------------------------------------------------------------------------


def _representative_parquet_uri(bucket: str, prefix: str) -> str | None:
    """Return the ``gs://`` URI of one deterministic (lowest-name) parquet under the
    shard's expected write prefix, or ``None`` if nothing has been written yet."""
    client = get_storage_client()
    blobs = sorted(
        (b for b in client.list_blobs(bucket=bucket, prefix=prefix) if b.name.endswith(".parquet")),
        key=lambda b: b.name,
    )
    if not blobs:
        return None
    return f"gs://{bucket}/{blobs[0].name}"


# ---------------------------------------------------------------------------
# Per-shard-leg runner (launch -> verify write -> verify manifest -> [skip-signal] ->
# [fingerprint]) — shard-level isolation: every step below is wrapped so a single
# shard-leg's failure never raises out of the matrix.
# ---------------------------------------------------------------------------


def _run_leg(
    *,
    cell: SmokeCell,
    day: str,
    leg: str,
    launcher_path: Path,
    project_id: str,
    code_bucket: str,
    timeout_sec: int,
    poll_interval_sec: int,
    run_ts: str,
) -> ShardCheckResult:
    started = datetime.now(UTC)
    bucket = resolve_test_bucket(cell.asset_group, project_id)
    prefix = expected_write_prefix(cell, day)
    match = _manifest_match(cell)
    force = leg == "force"
    is_skip_leg = leg == "skip"
    vm_name = _shard_vm_name(cell.asset_group, cell.venue, leg, run_ts)

    result = ShardCheckResult(
        shard_label=f"{cell.asset_group}/{cell.venue}/{day}",
        leg=leg,
        status="failed",
        vm_name=vm_name,
        attempt_ts=started.isoformat(),
        # IS's parquet write and manifest write are the SAME bucket under
        # --test-run (finding #2 — IS is fully test-bucket self-contained,
        # unlike MTDS) — set both up front so even an early-return failure
        # path still reports where this leg was actually checking.
        parquet_bucket=bucket,
        manifest_bucket=bucket,
    )

    fp_before: str | None = None
    if is_skip_leg:
        try:
            uri_before = _representative_parquet_uri(bucket, prefix)
            fp_before = object_signature(uri_before) if uri_before else None
        except Exception as exc:  # pragma: no cover — storage transport failure
            logger.warning("pre-skip fingerprint read failed for %s/%s: %s", cell.asset_group, cell.venue, exc)
    result.fingerprint_before = fp_before

    argv = _build_launcher_argv(cell, day, vm_name, project_id, force=force)
    logger.info("[%s/%s leg=%s] launching vm=%s argv=%s", cell.asset_group, cell.venue, leg, vm_name, argv)
    try:
        vm_result = launch_vm_and_wait(
            launcher_script=str(launcher_path),
            argv=argv,
            vm_name=vm_name,
            project_id=project_id,
            code_bucket=code_bucket,
            timeout_sec=timeout_sec,
            poll_interval_sec=poll_interval_sec,
        )
    except Exception as exc:  # shard-level isolation — never raise out of the matrix
        result.reason = f"vm_launch_error:{type(exc).__name__}:{exc}"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result

    result.exit_status = vm_result.exit_status
    if vm_result.exit_status is None or vm_result.exit_status != 0:
        result.reason = f"vm_run_not_successful:{vm_result.reason}"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result

    try:
        write_ok, parquet_count = verify_write(bucket, [prefix])
    except Exception as exc:  # pragma: no cover — storage transport failure
        result.reason = f"write_verify_error:{type(exc).__name__}:{exc}"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result
    result.write_verified = write_ok
    result.parquet_count = parquet_count
    if write_ok:
        try:
            found_uri = _representative_parquet_uri(bucket, prefix)
            result.parquet_uri = found_uri or ""
        except Exception as exc:  # pragma: no cover — storage transport failure
            logger.warning("parquet_uri lookup failed for %s/%s: %s", cell.asset_group, cell.venue, exc)

    try:
        manifest_ok, manifest_status = verify_manifest_row(bucket, match, day)
    except Exception as exc:  # pragma: no cover — manifest read failure
        result.reason = f"manifest_verify_error:{type(exc).__name__}:{exc}"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result
    result.manifest_ok = manifest_ok
    result.manifest_status = manifest_status

    benign_start = _benign_pre_launch_start(cell, day, manifest_ok, manifest_status)
    if benign_start is not None:
        # Honest pre-launch empty: a correct empty_confirmed manifest row + no parquet
        # (there is nothing to enumerate before the venue's own start date) is a PASS,
        # not a write-verification failure (issue:
        # coinbase_cde_mtds_batch_adapter_missing_2026_07_13, IS-leg section).
        result.status = "passed"
        result.reason = (
            f"benign_empty_pre_launch (day {day} < venue_start_date {benign_start}; "
            "honest empty_confirmed, no parquet expected)"
        )
        if is_skip_leg:
            result.skip_proof = "not_applicable"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result

    reasons: list[str] = []
    if not write_ok:
        reasons.append(f"no_parquet_at:gs://{bucket}/{prefix}")
    if not manifest_ok:
        reasons.append(f"manifest_status_invalid:{manifest_status}")

    if is_skip_leg:
        # IS's freshness pre-flight skip line (process_preflight.py):
        #   "SKIP date=%s: all %d venues/entities already fresh in manifest
        #    (use --force to re-fetch)"
        pattern = f"SKIP date={day}: all "
        log_text = fetch_run_log(code_bucket, vm_name)
        result.skip_signal_found = contains_skip_signal(log_text, pattern)
        if not result.skip_signal_found:
            reasons.append("skip_signal_not_found")

        fp_after: str | None = None
        try:
            uri_after = _representative_parquet_uri(bucket, prefix)
            fp_after = object_signature(uri_after) if uri_after else None
        except Exception as exc:  # pragma: no cover — storage transport failure
            logger.warning("post-skip fingerprint read failed for %s/%s: %s", cell.asset_group, cell.venue, exc)
        result.fingerprint_after = fp_after

        if fp_before is not None and fp_before != fp_after:
            reasons.append("fingerprint_changed_on_skip_leg")

        if reasons:
            result.status = "failed"
            result.skip_proof = "not_applicable"
        elif fp_before is None:
            # Skip signal fired + manifest/write are fine, but there was no pre-existing
            # object to fingerprint against (this skip leg ran with no prior write in
            # THIS test bucket — e.g. invoked standalone without a preceding force leg).
            # The skip mechanism worked, but "unchanged since force-run" is unproven —
            # per plan finding #2, IS's skip-leg proof requires an actual baseline, even
            # though (unlike MTDS) it needs no separate PROD pre-check to get there.
            result.status = "ambiguous"
            result.skip_proof = "not_applicable"
        else:
            result.status = "passed"
            result.skip_proof = "genuine"
    else:
        result.status = "failed" if reasons else "passed"
        if leg == "live" and result.status == "passed":
            reasons.append(_LIVE_LEG_CAVEAT)

    result.reason = "; ".join(reasons) if reasons else "ok"
    result.duration_sec = (datetime.now(UTC) - started).total_seconds()
    return result


def _skipped_live_leg(cell: SmokeCell, day: str) -> ShardCheckResult:
    """A live-leg cell explicitly recorded as skipped (venue not in MVP scope) rather
    than silently omitted — honest-absence over a silent gap in the report."""
    return ShardCheckResult(
        shard_label=f"{cell.asset_group}/{cell.venue}/{day}",
        leg="live",
        status="skipped",
        reason="not_in_mvp_scope",
        attempt_ts=datetime.now(UTC).isoformat(),
    )


def _force_consolidate_test_buckets(buckets: set[str]) -> None:
    """Phase-0: force a fresh manifest consolidation of each ``-test-`` bucket the
    legs will read BEFORE Phase 1 runs.

    ``-test-`` buckets have NO standing consolidator cron by design (only ``-prd-``
    buckets are on the ``*/1`` schedule), so their
    ``_index/availability_index.parquet`` silently re-freezes between sweeps — a
    stale index makes the local ``verify_manifest_row`` reads fall back to a slow
    per-VM-shard scan (or raise ``ManifestConsolidatorStaleError``) and can hide a
    fresh row behind an older, now-superseded one (issue:
    ``cefi_manifest_consolidator_14day_stale_recovered_2026_07_13``). Force-
    consolidating here gives every leg a fresh consolidated index to read against.

    Fail-loud-but-not-fatal: a consolidation error warns and continues (the sweep
    still runs; it just reads a possibly-stale index). PROD (``-prd-``) buckets are
    never force-consolidated from here — that is the standing cron's job, and doing
    it manually risks the consolidator prune-race window (``resolve_test_bucket``
    only ever yields ``-test-`` names, so this guard is belt-and-braces)."""
    for bucket in sorted(buckets):
        if "-test-" not in bucket:
            logger.warning("Phase-0 consolidation: refusing to force-consolidate non-test bucket %s", bucket)
            continue
        # Invoke the consolidator's own sanctioned one-off CLI form (``python -m``)
        # in a subprocess rather than a deep ``unified_trading_library.*`` import —
        # the import-patterns gate only permits top-level UTL re-exports.
        cmd = [
            sys.executable,
            "-m",
            "unified_trading_library.manifest_consolidator",
            "--bucket",
            bucket,
            "--force",
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
            tail_lines = (proc.stdout or proc.stderr or "").strip().splitlines()
            tail = tail_lines[-1] if tail_lines else ""
            if proc.returncode == 0:
                logger.info("Phase-0 consolidation: %s OK — %s", bucket, tail)
            else:
                logger.warning(
                    "Phase-0 consolidation FAILED for %s (rc=%d): %s — continuing sweep with a possibly-stale index",
                    bucket,
                    proc.returncode,
                    tail,
                )
        except Exception as exc:  # shard-level isolation — a stale-index warning must not abort the sweep
            logger.warning(
                "Phase-0 consolidation FAILED for %s: %s — continuing sweep with a possibly-stale index",
                bucket,
                exc,
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_legs(raw: str) -> list[str]:
    legs = [leg.strip().lower() for leg in raw.split(",") if leg.strip()]
    invalid = [leg for leg in legs if leg not in _VALID_LEGS]
    if invalid:
        raise ValueError(f"invalid --legs value(s) {invalid!r}; must be a subset of {sorted(_VALID_LEGS)}")
    if not legs:
        raise ValueError("--legs must name at least one of force,skip,live")
    return legs


def _validate_day(day: str) -> str:
    try:
        date.fromisoformat(day)  # date-only validation — no naive-datetime (DTZ007) concern
    except ValueError as exc:
        raise ValueError(f"--day must be YYYY-MM-DD, got {day!r}") from exc
    return day


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="instruments-service-pipeline-e2e-check",
        description=(
            "End-to-end data-pipeline smoke check for instruments-service: launches real "
            "VMs (force-leg + skip-leg per shard, plus an MVP-scoped live-leg), all routed "
            "to the -test- GCS bucket sibling via --test-run, and verifies the real "
            "CLI-launch -> GCS-write -> manifest-row contract on real infrastructure."
        ),
    )
    parser.add_argument("--day", required=True, help="Shard day to check (YYYY-MM-DD).")
    parser.add_argument(
        "--asset-group", default=None, help="Restrict to one asset_group (CEFI/DEFI/TRADFI/SPORTS/PREDICTION)."
    )
    parser.add_argument("--venue", default=None, help="Restrict to one venue (or sports provider).")
    parser.add_argument(
        "--legs",
        default="force,skip,live",
        help="Comma list of legs to run: force,skip,live (default: all three).",
    )
    parser.add_argument(
        "--report-dir",
        default="./pipeline_e2e_check_reports",
        help="Directory to write the .md/.json report into (default: ./pipeline_e2e_check_reports).",
    )
    parser.add_argument("--project", default=None, help="GCP project id (default: UTL get_project_id()).")
    parser.add_argument(
        "--launcher-path",
        default=None,
        help="Path to launch-instruments-backfill-vm.sh (default: sibling deployment-service checkout).",
    )
    parser.add_argument(
        "--timeout-sec", type=int, default=_DEFAULT_TIMEOUT_SEC, help="Per-VM wait timeout (default 1800s)."
    )
    parser.add_argument(
        "--poll-interval-sec",
        type=int,
        default=_DEFAULT_POLL_INTERVAL_SEC,
        help="VM status poll interval (default 30s).",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        day = _validate_day(args.day)
        legs = _parse_legs(args.legs)
    except ValueError as exc:
        logger.error(str(exc))
        return 2

    # get_project_id() is process-wide (lru_cache-backed) and every downstream UTL
    # call (resolve_test_bucket, read_availability_index, get_storage_client, ...)
    # resolves the project id independently by calling it again — so --project must
    # be exported into the environment BEFORE the first such call, not just captured
    # into this local variable, or those deeper calls silently fall back to whatever
    # (or nothing) the shell already has set.
    if args.project:
        os.environ.setdefault("GCP_PROJECT_ID", args.project)
    # This process's own local verify calls (verify_manifest_row, the
    # pre-skip fingerprint read) hit the SAME manifest-consolidator staleness guard
    # the VM side does (see the launcher's MANIFEST_ALLOW_STALE_FALLBACK metadata) —
    # setting it here too covers the local-side read, not just the VM-side one.
    # Deliberately process-wide, not scoped only to test-bucket calls: every
    # bucket this script's own local reads touch (test-bucket verifies, plus the
    # narrow single-shard PROD samples used for the skip-genuineness check) is
    # either a small test bucket or a single-row PROD sample, so the OOM risk the
    # guard exists for is bounded here — a documented, deliberate simplification
    # for this smoke-test tool, not a blanket recommendation for production reads.
    os.environ.setdefault("MANIFEST_ALLOW_STALE_FALLBACK", "true")
    project_id = args.project or get_project_id()
    code_bucket = f"deployment-scripts-{project_id}"
    launcher_path = Path(args.launcher_path).resolve() if args.launcher_path else _default_launcher_path()
    if not launcher_path.exists():
        logger.error("launcher script not found: %s (pass --launcher-path to override)", launcher_path)
        return 2

    run_started_at = datetime.now(UTC)
    run_ts = run_started_at.strftime("%m%d%H%M%S")

    cells = enumerate_cells(asset_group_filter=args.asset_group, venue_filter=args.venue)
    shard_targets = _dedupe_shard_targets(cells)
    logger.info(
        "enumerated %d shard target(s) (asset_group=%s venue=%s legs=%s day=%s)",
        len(shard_targets),
        args.asset_group,
        args.venue,
        legs,
        day,
    )

    # Phase-0: force-consolidate the -test- bucket(s) these legs will read (they have
    # no standing consolidator cron and re-freeze between sweeps).
    _force_consolidate_test_buckets({resolve_test_bucket(cell.asset_group, project_id) for cell in shard_targets})

    pipeline_report = PipelineCheckReport(
        service=_REPORT_SERVICE_SLUG,
        run_date=day,
        legs=legs,
        started_at=run_started_at.isoformat(),
    )

    for cell in shard_targets:
        for leg in ("force", "skip"):
            if leg not in legs:
                continue
            result = _run_leg(
                cell=cell,
                day=day,
                leg=leg,
                launcher_path=launcher_path,
                project_id=project_id,
                code_bucket=code_bucket,
                timeout_sec=args.timeout_sec,
                poll_interval_sec=args.poll_interval_sec,
                run_ts=run_ts,
            )
            pipeline_report.record(result)
            logger.info(
                "[%s/%s leg=%s] status=%s reason=%s duration=%.1fs",
                cell.asset_group,
                cell.venue,
                leg,
                result.status,
                result.reason,
                result.duration_sec,
            )

        if "live" in legs:
            if _venue_in_mvp_scope(cell.asset_group, cell.venue):
                result = _run_leg(
                    cell=cell,
                    day=day,
                    leg="live",
                    launcher_path=launcher_path,
                    project_id=project_id,
                    code_bucket=code_bucket,
                    timeout_sec=args.timeout_sec,
                    poll_interval_sec=args.poll_interval_sec,
                    run_ts=run_ts,
                )
            else:
                result = _skipped_live_leg(cell, day)
            pipeline_report.record(result)
            logger.info(
                "[%s/%s leg=live] status=%s reason=%s duration=%.1fs",
                cell.asset_group,
                cell.venue,
                result.status,
                result.reason,
                result.duration_sec,
            )

    pipeline_report.finished_at = datetime.now(UTC).isoformat()
    out_path = write_report(pipeline_report, args.report_dir)
    print(render_markdown(pipeline_report))
    print(f"report written to {out_path}")

    return 1 if (pipeline_report.failed or pipeline_report.ambiguous) else 0


if __name__ == "__main__":
    sys.exit(main())
