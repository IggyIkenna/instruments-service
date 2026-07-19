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

PREDICTION Phase-D adaptation (``prediction_consolidated_closeout_2026_07_18.md``,
prediction-scoped — non-prediction behavior is byte-unchanged): the ``(asset_group, venue)``
atom above collapses away two IS-PRODUCED prediction reference grains, so this script adds,
for prediction cells only, (A) a ``canonical`` regression cell asserting the freshly-written
instruments parquet's ``canonical_instrument_id`` / ``instrument_type`` / soccer
``af_fixture_match_status`` are canonical (:func:`_run_prediction_canonical_cell`), and
(B) force/skip smoke cells for the CQG cluster bundle
(``data_type=prediction_canonical_question_group``, a manifest-only bundle) and
``market_lifecycle`` (the market-id lifecycle parquet), both written by the SAME prediction
backfill VM the force/skip legs already launch (:func:`_run_prediction_grain_cells`). MTDS
cannot smoke these two grains — it only reads ``market_lifecycle`` and re-derives the CQG
bundle at manifest rebuild, never on the tick backfill — so their coverage lives here, on
their genuine producer.

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

from unified_api_contracts import VENUE_TO_ADAPTER_KEY, InstrumentType, VenueMapping
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
    read_availability_index,
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
    is_prediction_venue_cell,
    list_prediction_instruments_objects,
    prediction_instruments_object_matches,
    resolve_test_bucket,
    verify_prediction_parquet_written,
)

logger = logging.getLogger(__name__)

# The plan's write_report()/render_markdown() filename+frontmatter convention keys off
# `PipelineCheckReport.service` — set to the exact slug the docstring names so the emitted
# file lands at `data_pipeline_e2e_check_is_<date>.md` verbatim.
_REPORT_SERVICE_SLUG = "data_pipeline_e2e_check_is"
_DEFAULT_TIMEOUT_SEC = 1800
_DEFAULT_POLL_INTERVAL_SEC = 30
_GCE_NAME_MAX = 63
# ``canonical`` is a PREDICTION-only regression cell (prediction_consolidated_closeout_
# 2026_07_18.md Phase-D, item A) that reads the freshly-written instruments parquet and
# asserts the canonical prediction shape — it records ``skipped`` for every non-prediction
# cell (mirroring the MTDS engine's tradfi-only canonical leg), so it is safe to request
# alongside force/skip/live for any asset_group. It launches NO VM (it verifies the force
# leg's write). ``canonical`` is NOT in the default ``--legs`` (force,skip,live), so a
# default run is byte-unchanged for every asset_group.
_VALID_LEGS = frozenset({"force", "skip", "live", "canonical"})
_LIVE_LEG_CAVEAT = (
    " [NOTE: routed via launch-instruments-backfill-vm.sh, which currently always runs "
    "--mode batch under setup-data-pipeline-vm.sh -- this leg does not yet prove the "
    "true --mode live code path; see script module docstring]"
)

# ---------------------------------------------------------------------------
# PREDICTION Phase-D adaptation (prediction_consolidated_closeout_2026_07_18.md):
#   (A) a canonical regression cell over the freshly-written instruments parquet, and
#   (B) force/skip smoke coverage for the two IS-PRODUCED prediction reference grains the
#       generic ``(asset_group, venue)`` shard atom collapses away — the CQG cluster bundle
#       (data_type=prediction_canonical_question_group, a manifest-only bundle written by
#       ``engine/orchestrator/process_write.py``) and ``market_lifecycle`` (the market-id
#       lifecycle parquet at ``market_lifecycle/by_canonical_group/…`` written by
#       ``engine/orchestrator/writers.py::_write_market_lifecycle``). Both grains are
#       produced by the SAME prediction backfill VM the force/skip legs already launch, so
#       these cells verify that VM's writes — they launch no extra VM.
# ---------------------------------------------------------------------------
# The ONE canonical prediction instrument_type (UAC SSOT). A per-CID prediction row whose
# ``instrument_type`` is anything else is an A0 non-canonical target: lowercase dupes
# (``prediction``/``prediction_market``), underlying-asset LEAKAGE, or ``''``.
_CANONICAL_PREDICTION_INSTRUMENT_TYPE = InstrumentType.PREDICTION_MARKET.value
# Underlying-asset leakage tokens (A0-enumerated 2026-07-18) — used only to LABEL a
# violation; the canonical gate is the ``== PREDICTION_MARKET`` equality any leakage fails.
_PREDICTION_UNDERLYING_LEAKAGE_TYPES = frozenset(
    {"BTC", "ETH", "SPX", "DJIA", "NDX", "GOLD", "SILVER", "CRUDE_OIL", "DOGE", "XRP", "BNB", "HYPE", "OTHER"}
)
# Soccer fixture-match closed set — mirrors instruments-service
# ``reference_data/adapters/prediction/fixture_match.py::FixtureMatchStatus`` (the writer of
# the ``af_fixture_match_status`` column). A stamped (non-null) value MUST be one of these.
_AF_FIXTURE_MATCH_STATUS_CLOSED_SET = frozenset({"MATCHED", "UNRESOLVED_TEAM_NAME", "NO_FIXTURE_DATA"})
_PRED_WHITESPACE_RE = re.compile(r"\s")
# A canonical canonical_question_group value is an UPPERCASE snake token (A0: "81 canonical
# UPPERCASE values, no dupes") — this shape check needs no deep enum import.
_CANONICAL_CQG_RE = re.compile(r"^[A-Z0-9_]+$")
_CQG_BUNDLE_DATA_TYPE = "prediction_canonical_question_group"
# The lifecycle manifest data_type spellings seen in the wild (the writer stamps
# ``prediction_market_lifecycle``; UAC's DATA_TYPES_BY_ASSET_GROUP carries
# ``market_lifecycle``/``MARKET_LIFECYCLE`` — accept any so the cell is robust to that
# A0-flagged naming drift instead of failing on it).
_MARKET_LIFECYCLE_DATA_TYPES = ("prediction_market_lifecycle", "market_lifecycle", "MARKET_LIFECYCLE")
_MARKET_LIFECYCLE_PREFIX_TPL = "market_lifecycle/by_canonical_group/day={day}/"
_MARKET_LIFECYCLE_OBJECT_SUFFIX = "market_lifecycle.parquet"

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


def _representative_prediction_parquet_uri(bucket: str, day: str, venue: str) -> str | None:
    """PREDICTION analogue of :func:`_representative_parquet_uri`: the deterministic
    (lowest-name) ``instruments.parquet`` for (day, venue) under the CQG-FIRST availability
    layout, or ``None``. Reuses ``list_prediction_instruments_objects`` so the skip-leg
    fingerprint compares the SAME day+venue-scoped object the write-verify counted — a plain
    listing under prediction's coarse base prefix would fingerprint an arbitrary other
    day/venue's parquet (the CQG segment precedes day/venue, so no literal day-first prefix
    scopes it)."""
    names = list_prediction_instruments_objects(bucket, day, venue)
    return f"gs://{bucket}/{names[0]}" if names else None


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
    # PREDICTION lands the CQG-FIRST availability layout, which prefix (the coarse base
    # by_date/ tree) cannot scope on its own — every write-verify / fingerprint touchpoint
    # below routes through the shared day+venue substring helpers for prediction instead.
    is_prediction = is_prediction_venue_cell(cell)
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
            uri_before = (
                _representative_prediction_parquet_uri(bucket, day, cell.venue)
                if is_prediction
                else _representative_parquet_uri(bucket, prefix)
            )
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
        if is_prediction:
            write_ok, parquet_count = verify_prediction_parquet_written(bucket, day, cell.venue)
        else:
            write_ok, parquet_count = verify_write(bucket, [prefix])
    except Exception as exc:  # pragma: no cover — storage transport failure
        result.reason = f"write_verify_error:{type(exc).__name__}:{exc}"
        result.duration_sec = (datetime.now(UTC) - started).total_seconds()
        return result
    result.write_verified = write_ok
    result.parquet_count = parquet_count
    if write_ok:
        try:
            found_uri = (
                _representative_prediction_parquet_uri(bucket, day, cell.venue)
                if is_prediction
                else _representative_parquet_uri(bucket, prefix)
            )
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
            uri_after = (
                _representative_prediction_parquet_uri(bucket, day, cell.venue)
                if is_prediction
                else _representative_parquet_uri(bucket, prefix)
            )
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
# PREDICTION Phase-D — (A) canonical regression cell + (B) CQG/lifecycle grain cells
# ---------------------------------------------------------------------------


def _assert_prediction_records_canonical(
    ids: list[str], types: list[str], fixture_statuses: list[str | None]
) -> tuple[int, int, list[str]]:
    """Prediction canonical assertion over instruments-parquet rows (Phase-D item A).

    A canonical prediction instrument row has ``instrument_type == PREDICTION_MARKET``
    (the ONE canonical value — this single equality catches every A0 drift: lowercase
    ``prediction``/``prediction_market`` dupes, underlying-asset LEAKAGE, and ``''``) and a
    ``canonical_instrument_id`` that is non-empty, whitespace-free, and — when it carries the
    ``VENUE:TYPE:SYMBOL`` shape — embeds ``PREDICTION_MARKET`` as its TYPE segment. For a
    SOCCER row (a stamped, non-null ``af_fixture_match_status``) the status must be one of
    the closed-set values ``MATCHED``/``UNRESOLVED_TEAM_NAME``/``NO_FIXTURE_DATA``.

    Returns ``(checked, canonical, violations_sample)`` (violations bounded at 50), mirroring
    the tradfi/MTDS helpers' convention.
    """
    if not (len(ids) == len(types) == len(fixture_statuses)):
        msg = f"ids/types/fixture_statuses must be the same length, got {len(ids)}/{len(types)}/{len(fixture_statuses)}"
        raise ValueError(msg)

    checked = 0
    canonical = 0
    violations: list[str] = []
    for row_id, declared_type, fixture_status in zip(ids, types, fixture_statuses, strict=True):
        checked += 1
        reasons: list[str] = []
        if declared_type != _CANONICAL_PREDICTION_INSTRUMENT_TYPE:
            if declared_type.strip().lower() in ("prediction", "prediction_market"):
                reasons.append(f"lowercase-instrument_type:{declared_type!r}")
            elif declared_type.strip().upper() in _PREDICTION_UNDERLYING_LEAKAGE_TYPES:
                reasons.append(f"underlying-leakage-instrument_type:{declared_type!r}")
            else:
                reasons.append(f"noncanonical-instrument_type:{declared_type!r}")
        if not row_id:
            reasons.append("empty-id")
        else:
            if _PRED_WHITESPACE_RE.search(row_id):
                reasons.append("whitespace")
            parts = row_id.split(":", 2)
            id_type = parts[1].strip() if len(parts) >= 2 else ""
            if len(parts) >= 3 and id_type != _CANONICAL_PREDICTION_INSTRUMENT_TYPE:
                if id_type.lower() in ("prediction", "prediction_market"):
                    reasons.append(f"lowercase-id-type:{id_type}")
                elif id_type.upper() in _PREDICTION_UNDERLYING_LEAKAGE_TYPES:
                    reasons.append(f"underlying-leakage-id-type:{id_type}")
                else:
                    reasons.append(f"noncanonical-id-type:{id_type}")
        # Soccer rows only: a STAMPED (non-null / non-empty) status must be in the closed set.
        if fixture_status not in (None, "") and str(fixture_status) not in _AF_FIXTURE_MATCH_STATUS_CLOSED_SET:
            reasons.append(f"noncanonical-af_fixture_match_status:{fixture_status!r}")
        if reasons:
            if len(violations) < 50:
                violations.append(f"{row_id} [{declared_type}]: {', '.join(reasons)}")
        else:
            canonical += 1
    return checked, canonical, violations


def _read_instruments_parquet_rows(
    bucket: str, prefix: str, smoke_date: str, venue: str
) -> tuple[list[str], list[str], list[str | None]] | None:
    """Read (canonical_instrument_id, instrument_type, af_fixture_match_status) across every
    ``instruments.parquet`` written for (smoke_date, venue) under the CQG-FIRST availability
    layout (one object per ``canonical_question_group`` subfolder). ``prefix`` is the coarse
    base ``by_date/`` tree (``expected_write_prefix`` cannot express the CQG-first scope as a
    literal prefix), so the read is scoped to THIS day+venue via
    ``prediction_instruments_object_matches`` — without it the canonical check would read every
    day+venue accumulated in the ``-test-`` bucket. Returns ``None`` when nothing is readable.
    """
    from io import BytesIO

    import pandas as pd

    client = get_storage_client()
    frames: list[pd.DataFrame] = []
    want = ["canonical_instrument_id", "instrument_type", "af_fixture_match_status"]
    for blob in client.list_blobs(bucket=bucket, prefix=prefix):
        if not prediction_instruments_object_matches(blob.name, smoke_date, venue):
            continue
        try:
            df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
        except Exception as exc:  # pragma: no cover — storage/parquet transport failure
            logger.warning("instruments parquet read failed %s: %s", blob.name, exc)
            continue
        cols = [c for c in want if c in df.columns]
        if "canonical_instrument_id" not in cols and "instrument_id" in df.columns:
            df = df.rename(columns={"instrument_id": "canonical_instrument_id"})
            cols = [c for c in want if c in df.columns]
        if cols:
            frames.append(df[cols])
    if not frames:
        return None
    merged = pd.concat(frames, ignore_index=True)
    ids = [str(v) for v in merged.get("canonical_instrument_id", pd.Series([], dtype=str)).tolist()]
    types = (
        [str(v) for v in merged["instrument_type"].tolist()]
        if "instrument_type" in merged.columns
        else ["" for _ in ids]
    )
    if "af_fixture_match_status" in merged.columns:
        statuses: list[str | None] = [
            None if pd.isna(v) else str(v) for v in merged["af_fixture_match_status"].tolist()
        ]
    else:
        statuses = [None for _ in ids]
    return ids, types, statuses


def _run_prediction_canonical_cell(cell: SmokeCell, day: str, project_id: str) -> ShardCheckResult:
    """(A) PREDICTION canonical regression cell — asserts the freshly-written instruments
    parquet's ``canonical_instrument_id`` / ``instrument_type`` / soccer
    ``af_fixture_match_status`` are canonical. Records ``skipped`` for non-prediction cells
    (mirrors the MTDS engine's tradfi-only canonical leg). Launches no VM."""
    attempt_ts = datetime.now(UTC).isoformat()
    if cell.asset_group.upper() != "PREDICTION":
        return ShardCheckResult(
            shard_label=f"{cell.asset_group}/{cell.venue}/{day}",
            leg="canonical",
            status="skipped",
            reason="canonical_shape_check_is_prediction_only",
            attempt_ts=attempt_ts,
        )
    bucket = resolve_test_bucket(cell.asset_group, project_id)
    prefix = expected_write_prefix(cell, day)
    try:
        rows = _read_instruments_parquet_rows(bucket, prefix, day, cell.venue)
    except Exception as exc:  # pragma: no cover — shard-level isolation, no raise
        return ShardCheckResult(
            shard_label=f"PREDICTION/{cell.venue}/{day}",
            leg="canonical",
            status="failed",
            reason=f"canonical_read_error:{type(exc).__name__}:{exc}",
            attempt_ts=attempt_ts,
            manifest_bucket=bucket,
        )
    if rows is None:
        return ShardCheckResult(
            shard_label=f"PREDICTION/{cell.venue}/{day}",
            leg="canonical",
            status="failed",
            reason=f"canonical_no_instruments_parquet_at:gs://{bucket}/{prefix} (day={day} venue={cell.venue})",
            attempt_ts=attempt_ts,
            manifest_bucket=bucket,
        )
    ids, types, statuses = rows
    checked, canonical, violations = _assert_prediction_records_canonical(ids, types, statuses)
    if checked == 0:
        return ShardCheckResult(
            shard_label=f"PREDICTION/{cell.venue}/{day}",
            leg="canonical",
            status="passed",
            reason="no prediction rows in this shard (vacuous)",
            attempt_ts=attempt_ts,
            manifest_ok=True,
            parquet_bucket=bucket,
        )
    passed = checked == canonical
    reason = f"checked={checked} canonical={canonical} raw={checked - canonical}"
    if violations:
        reason += f"; e.g. {violations[0]}"
    return ShardCheckResult(
        shard_label=f"PREDICTION/{cell.venue}/{day}",
        leg="canonical",
        status="passed" if passed else "failed",
        reason=reason,
        attempt_ts=attempt_ts,
        parquet_count=checked,
        manifest_ok=passed,
        parquet_bucket=bucket,
    )


def _noncanonical_cqg_values(bucket: str, venue: str, day: str) -> list[str]:
    """Best-effort: return any non-canonical ``canonical_question_group`` value (carried in
    the CQG bundle row's ``instrument_id``) for this (venue, day). Empty list = all canonical
    or unreadable (presence is proven separately via ``verify_manifest_row``)."""
    try:
        frame = read_availability_index(bucket, columns=["date", "venue", "data_type", "instrument_id"])
    except Exception:  # pragma: no cover — best-effort canonicality only
        return []
    if frame is None or frame.empty or "instrument_id" not in frame.columns:
        return []
    mask = (
        (frame["venue"].astype(str).str.upper() == venue.upper())
        & (frame["data_type"].astype(str) == _CQG_BUNDLE_DATA_TYPE)
        & (frame["date"].astype(str).str[:10] == day)
    )
    bad: list[str] = []
    for cqg in (str(v) for v in frame[mask]["instrument_id"].tolist()):
        if not _CANONICAL_CQG_RE.match(cqg):
            bad.append(cqg)
    return bad[:5]


def _list_market_lifecycle_objects(bucket: str, day: str, venue: str) -> list[str]:
    """Return the ``market_lifecycle.parquet`` object names for this (venue, day) — the
    venue-partitioned objects when present, else the legacy venue-less ones (both resolve;
    the venue level was added 2026-07-14 — see ``writers.py::_write_market_lifecycle``)."""
    client = get_storage_client()
    prefix = _MARKET_LIFECYCLE_PREFIX_TPL.format(day=day)
    names = [
        b.name
        for b in client.list_blobs(bucket=bucket, prefix=prefix)
        if b.name.endswith(_MARKET_LIFECYCLE_OBJECT_SUFFIX)
    ]
    venue_scoped = [n for n in names if f"venue={venue}/" in n]
    return venue_scoped or names


def _manifest_row_present_any_dt(bucket: str, venue: str, day: str, data_types: tuple[str, ...]) -> bool:
    """True iff a manifest row exists for (PREDICTION, venue, day) under ANY of ``data_types``
    (robust to the ``market_lifecycle`` naming drift A0 flagged)."""
    for dt in data_types:
        try:
            ok, _status = verify_manifest_row(
                bucket, {"asset_group": "PREDICTION", "venue": venue, "data_type": dt}, day
            )
        except Exception:  # pragma: no cover — manifest read failure, try next spelling
            continue
        if ok:
            return True
    return False


def _run_prediction_grain_cells(
    cell: SmokeCell, day: str, leg: str, main_result: ShardCheckResult, project_id: str
) -> list[ShardCheckResult]:
    """(B) Force/skip smoke coverage for the two IS-produced prediction reference grains the
    ``(asset_group, venue)`` shard atom collapses away — emitted as distinct cells, verified
    against the SAME force/skip VM the main leg launched (no extra VM).

    * CQG cluster grain (``data_type=prediction_canonical_question_group``): a MANIFEST-ONLY
      bundle (UTL ``MANIFEST_ONLY_BUNDLE_DATA_TYPES`` — no GCS object), so its proof is the
      manifest bundle row's presence + the bundle key (``instrument_id`` = the CQG value)
      being canonical.
    * ``market_lifecycle`` grain: an object-backed parquet at
      ``market_lifecycle/by_canonical_group/…`` + a manifest row.

    Force leg → prove written. Skip leg → prove still-present AND the main leg's skip signal
    fired (the grains ride the same skipped VM, so an unchanged main parquet + fired skip
    signal means the grains were not re-fetched either)."""
    attempt_ts = datetime.now(UTC).isoformat()
    bucket = resolve_test_bucket(cell.asset_group, project_id)
    is_skip = leg == "skip"
    skip_ok = bool(main_result.skip_signal_found)
    cells: list[ShardCheckResult] = []

    # --- CQG cluster grain (manifest-only bundle) ---
    cqg_reasons: list[str] = []
    try:
        cqg_present = _manifest_row_present_any_dt(bucket, cell.venue, day, (_CQG_BUNDLE_DATA_TYPE,))
        noncanon = _noncanonical_cqg_values(bucket, cell.venue, day)
    except Exception as exc:  # pragma: no cover — shard-level isolation
        cqg_present, noncanon = False, []
        cqg_reasons.append(f"cqg_read_error:{type(exc).__name__}:{exc}")
    if not cqg_present:
        cqg_reasons.append("cqg_bundle_manifest_row_missing")
    if noncanon:
        cqg_reasons.append(f"noncanonical_cqg_values:{noncanon}")
    if is_skip and not skip_ok:
        cqg_reasons.append("skip_signal_not_found")
    cells.append(
        ShardCheckResult(
            shard_label=f"PREDICTION/{cell.venue}/prediction_canonical_question_group/{day}",
            leg=leg,
            status="failed" if cqg_reasons else "passed",
            reason="; ".join(cqg_reasons) if cqg_reasons else "cqg_bundle_present_and_canonical",
            attempt_ts=attempt_ts,
            manifest_bucket=bucket,
            skip_proof=("genuine" if (is_skip and not cqg_reasons) else "not_applicable"),
        )
    )

    # --- market_lifecycle grain (object-backed) ---
    lc_reasons: list[str] = []
    try:
        lc_objects = _list_market_lifecycle_objects(bucket, day, cell.venue)
        lc_manifest = _manifest_row_present_any_dt(bucket, cell.venue, day, _MARKET_LIFECYCLE_DATA_TYPES)
    except Exception as exc:  # pragma: no cover — shard-level isolation
        lc_objects, lc_manifest = [], False
        lc_reasons.append(f"lifecycle_read_error:{type(exc).__name__}:{exc}")
    if not lc_objects:
        lc_reasons.append("no_market_lifecycle_parquet")
    if not lc_manifest:
        lc_reasons.append("market_lifecycle_manifest_row_missing")
    if is_skip and not skip_ok:
        lc_reasons.append("skip_signal_not_found")
    cells.append(
        ShardCheckResult(
            shard_label=f"PREDICTION/{cell.venue}/market_lifecycle/{day}",
            leg=leg,
            status="failed" if lc_reasons else "passed",
            reason="; ".join(lc_reasons) if lc_reasons else f"market_lifecycle_present ({len(lc_objects)} object(s))",
            attempt_ts=attempt_ts,
            parquet_bucket=bucket,
            manifest_bucket=bucket,
            parquet_count=len(lc_objects),
            skip_proof=("genuine" if (is_skip and not lc_reasons) else "not_applicable"),
        )
    )
    return cells


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
        "--tardis-only",
        action="store_true",
        help="Restrict to venues sourced via the Tardis adapter (VENUE_TO_ADAPTER_KEY == 'tardis'); "
        "excludes native-REST HYPERLIQUID/ASTER/LIGHTER-ZKSYNC/PACIFICA-SOLANA/EXTENDED-STARKNET. "
        "Tardis cells are N=1 serial on the shared IP (tardis-concurrency-guard enforces it).",
    )
    parser.add_argument(
        "--legs",
        default="force,skip,live",
        help="Comma list of legs to run: force,skip,live,canonical (default: force,skip,live). "
        "'canonical' is a PREDICTION-only regression cell (skipped for other asset_groups); a "
        "prediction Phase-D run uses --legs force,skip,canonical.",
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
    if args.tardis_only:
        # Tardis-only scope: keep only venues our pipeline sources via the Tardis adapter
        # (authoritative UAC routing, NOT Tardis's catalog — the native-REST venues are
        # catalogued by Tardis but fetched via their own adapters and are excluded here).
        cells = [c for c in cells if VENUE_TO_ADAPTER_KEY.get(c.venue) == "tardis"]
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
            # (B) PREDICTION-only: the same force/skip VM also produced the two
            # IS-domain reference grains the (asset_group, venue) atom collapses away
            # (CQG cluster bundle + market_lifecycle) — emit them as distinct cells.
            if cell.asset_group.upper() == "PREDICTION":
                for extra in _run_prediction_grain_cells(cell, day, leg, result, project_id):
                    pipeline_report.record(extra)
                    logger.info(
                        "[%s/%s leg=%s] grain=%s status=%s reason=%s",
                        cell.asset_group,
                        cell.venue,
                        leg,
                        extra.shard_label,
                        extra.status,
                        extra.reason,
                    )

        # (A) PREDICTION-only canonical regression cell over the freshly-written
        # instruments parquet (records skipped for non-prediction cells; launches no VM).
        if "canonical" in legs:
            canonical_result = _run_prediction_canonical_cell(cell, day, project_id)
            pipeline_report.record(canonical_result)
            logger.info(
                "[%s/%s leg=canonical] status=%s reason=%s",
                cell.asset_group,
                cell.venue,
                canonical_result.status,
                canonical_result.reason,
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
