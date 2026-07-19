# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
# SCHEMA_PROVENANCE_EXEMPT — script-local result dataclasses for smoke harness output.
"""instruments-service smoke matrix (Phase 2 — institutional smoke matrix plan).

Runs a cell-level smoke across every (category x venue x data_type) combination
declared in UAC capability declarations. Each cell exercises the real
instruments-service CLI with ``IS_TEST_RUN=true`` so writes route to the
``instruments-store-{category}-test-{project_id}`` bucket.

3-step assertion contract (enforced per cell — SSOT: institutional_smoke_matrix plan):

    1. RUN the service CLI with the cell-specific args.
    2. VERIFY GCS WRITE: at least one ``.parquet`` exists under the category's
       expected TEST-bucket prefix (varies for SPORTS — see
       ``codex/02-data/per-category-bucket-layouts.md``).
    3. VERIFY TEST MANIFEST: a row in the test bucket's
       ``_index/availability_index.parquet`` exists with
       ``capture_status in {captured, empty_confirmed}`` for the (date, category,
       venue, data_type) tuple. ``empty_confirmed`` is a PASS, not a SKIP.

Shard-level isolation: a failed cell is logged and the matrix continues.
Return code is ``1`` if any cell fails, ``0`` otherwise.

Usage::

    # Enumerate cells only (default — safe)
    python -m instruments_service.smoke

    # Run the full smoke (may take a long time; hits real external APIs)
    python -m instruments_service.smoke --execute

    # Scoped smoke (single category)
    python -m instruments_service.smoke --execute --asset-group CEFI

    # JSON report for CI parsing
    python -m instruments_service.smoke --execute --report /tmp/smoke.json

Phase 3 dependency: sports cells respect the api-football T0 ordering. When
``IS_TEST_RUN=true``, the adapter factory raises ``DependencyError`` for
api-football-dependent venues unless api-football has already been run for the
date. The smoke matrix catches this and marks the cell ``skipped`` with
``reason=api_football_missing`` rather than ``attempted_failed``.
"""

# SCHEMA_PROVENANCE_EXEMPT — smoke-matrix harness report structs, not domain contracts

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess  # nosec B404 — invokes our own CLI, not external input
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from unified_api_contracts import DATA_TYPES_BY_ASSET_GROUP, VENUES_BY_ASSET_GROUP
from unified_trading_library import (
    get_bucket_name,
    get_project_id,
    get_storage_client,
    resolve_bucket_name,
)

logger = logging.getLogger(__name__)

CellStatus = Literal["passed", "failed", "skipped"]

_SPORTS_T0_PROVIDER: str = "API_FOOTBALL"
_DEFAULT_TIMEOUT_SEC: int = 600

# SPORTS venues that are genuinely MTDS-owned, NOT instruments-service-owned
# (registry-consolidation Decision C, 2026-06-29 — see
# instruments_service/engine/orchestrator/venue_core.py::get_venues_for_asset_groups
# and unified_api_contracts/registry/venue_adapter_keys.py, where every one of these
# resolves to NO_ADAPTER_YET). Re-confirmed with LIVE evidence 2026-07-12
# (unified-trading-pm/plans/active/data_pipeline_e2e_check_2026_07_10.md todo 26
# Progress Log): force-refetching each of these against instruments-service's real
# CLI produces "WARNING No active venues" — a fast no-op, NOT a genuine capture.
# They are ODDS_API's aggregated bookmaker-fanout OUTPUT tags (MTDS manifest
# sub-venues), not independently-fetchable instruments-service shard keys. Use
# market-tick-data-service's own pipeline checker for these venues instead.
_MTDS_ONLY_SPORTS_VENUES: frozenset[str] = frozenset(
    {
        "ODDS_API",
        "PINNACLE",
        "BETFAIR_SB_UK",
        "BETFAIR_EX_UK",
        "BETFAIR_EX_EU",
        "DRAFTKINGS",
        "FANDUEL",
    }
)
_SERVICE_MODULE: str = "instruments_service"
_SERVICE_NAME: str = "instruments-service"


@dataclass
class SmokeCell:
    """A single (service, asset_group, venue, data_type) smoke cell."""

    asset_group: str
    venue: str
    data_type: str
    sports_provider: str | None = None

    def label(self) -> str:
        base = f"{self.asset_group}:{self.venue}:{self.data_type}"
        if self.sports_provider:
            base = f"{base}:{self.sports_provider}"
        return base


@dataclass
class CellResult:
    """Per-cell smoke outcome."""

    cell: SmokeCell
    status: CellStatus
    reason: str = ""
    attempt_ts: str = ""
    parquet_count: int = 0
    manifest_status: str = ""
    duration_sec: float = 0.0
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass
class SmokeReport:
    """Final matrix report."""

    service: str
    smoke_date: str
    total_cells: int
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    started_at: str = ""
    finished_at: str = ""
    results: list[CellResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cell enumeration
# ---------------------------------------------------------------------------


def enumerate_cells(
    asset_group_filter: str | None = None,
    venue_filter: str | None = None,
    data_type_filter: str | None = None,
) -> list[SmokeCell]:
    """Enumerate viable (asset_group, venue, data_type) cells for instruments-service.

    SPORTS cells are mostly specialised: instruments-service drives its own
    reference-data providers (API_FOOTBALL T0 + 5 T1 enrichment providers) via
    ``--sports-provider`` rather than ``--venues`` + ``--data-types`` — api-football
    (T0) is emitted FIRST; T1 enrichment providers follow — see
    ``codex/02-data/sports-adapter-dependency-order.md``. ONE SPORTS venue is the
    exception: bare ``BETFAIR`` has a real, credential-gated instruments-service
    adapter (``instruments_service/reference_data/adapters/sports/adapters/betfair.py``)
    and is driven the normal ``--venues`` way, same as CEFI/DEFI/TRADFI. See
    ``_enumerate_sports_cells`` for the full instruments-service-vs-MTDS venue-
    ownership split (registry-consolidation Decision C, 2026-06-29).
    """
    cells: list[SmokeCell] = []
    asset_groups = [c.upper() for c in DATA_TYPES_BY_ASSET_GROUP]

    for ag in asset_groups:
        if asset_group_filter and ag != asset_group_filter.upper():
            continue

        if ag == "SPORTS":
            cells.extend(_enumerate_sports_cells(data_type_filter, venue_filter))
            continue

        venues = VENUES_BY_ASSET_GROUP.get(ag.lower(), [])
        data_types = DATA_TYPES_BY_ASSET_GROUP.get(ag.lower(), [])
        for venue in venues:
            if venue_filter and venue != venue_filter:
                continue
            for data_type in data_types:
                if data_type_filter and data_type != data_type_filter:
                    continue
                cells.append(SmokeCell(asset_group=ag, venue=venue, data_type=data_type))

    return cells


def _enumerate_sports_cells(
    data_type_filter: str | None,
    venue_filter: str | None,
) -> list[SmokeCell]:
    """SPORTS enumeration: instruments-service-owned providers + the one real venue.

    instruments-service's SPORTS registry is DELIBERATELY disjoint from UAC's
    global ``VENUES_BY_ASSET_GROUP["sports"]`` (registry-consolidation Decision C,
    2026-06-29 — see ``instruments_service/engine/orchestrator/venue_core.py::
    get_venues_for_asset_groups`` + ``unified_api_contracts/registry/
    venue_adapter_keys.py``). Two genuinely different instruments-service SPORTS
    shard families exist:

    1. Reference-data PROVIDERS (T0 api-football + 5 T1 enrichment providers),
       driven via ``--sports-provider``. Each provider is modelled as a cell,
       ordered so the pre-flight DependencyError for T1-without-T0 fires only
       when an operator explicitly skips the T0 cell.
    2. Bare ``BETFAIR`` — a real, credential-gated instruments-service adapter
       (``betfair`` key in ``venue_adapter_keys.py``), driven via ``--venues``
       like any CEFI/DEFI/TRADFI venue (NOT ``--sports-provider``). Currently
       BLOCKED-CREDENTIALS with zero captured rows ever in PROD (same gap as
       ``wsfeedconnector_phase35_gap_2026_07_06.md`` gap-009) — smoked anyway so
       the checker reports an honest, informative failure instead of silently
       omitting the one real SPORTS venue cell.

    The REMAINING UAC sports venues (ODDS_API, PINNACLE, BETFAIR_SB_UK,
    BETFAIR_EX_UK, BETFAIR_EX_EU, DRAFTKINGS, FANDUEL — see
    ``_MTDS_ONLY_SPORTS_VENUES``) are MTDS-owned odds/bookmaker venues with
    NO instruments-service adapter (``NO_ADAPTER_YET`` sentinel). A
    ``venue_filter`` naming one of these correctly enumerates ZERO cells here —
    that is not a bug in this function, it is the honest answer for an
    instruments-service smoke check. See market-tick-data-service's own
    pipeline checker for those venues.
    """
    if venue_filter and venue_filter.upper() in _MTDS_ONLY_SPORTS_VENUES:
        logger.warning(
            "venue=%s is MTDS-owned (NO_ADAPTER_YET in instruments-service's "
            "venue_adapter_keys.py — registry-consolidation Decision C, 2026-06-29). "
            "instruments-service has no adapter for it and never will by design; "
            "0 cells is the correct, honest answer here. Check "
            "market-tick-data-service's pipeline checker for this venue instead.",
            venue_filter.upper(),
        )
        return []

    providers_ordered: list[str] = [
        _SPORTS_T0_PROVIDER,  # T0
        "OPEN_METEO",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "UNDERSTAT",
        "FOOTYSTATS",
    ]
    sports_data_types = DATA_TYPES_BY_ASSET_GROUP.get("sports", [])
    cells: list[SmokeCell] = []
    for provider in providers_ordered:
        # Provider-routed cells: --sports-provider, not --venues.
        if venue_filter and provider != venue_filter:
            continue
        for data_type in sports_data_types:
            if data_type_filter and data_type != data_type_filter:
                continue
            cells.append(
                SmokeCell(
                    asset_group="SPORTS",
                    venue=provider,  # model provider as the "venue" axis for SPORTS
                    data_type=data_type,
                    sports_provider=provider,
                )
            )

    # Bare BETFAIR: real venue-routed (--venues) SPORTS cell, sports_provider=None
    # so build_cli_args/expected_write_prefix/verify_manifest_row treat it like any
    # other venue-based instrument-catalog write (instrument_availability/by_date/
    # .../venue=BETFAIR/), not the sports_reference/ provider path.
    if (not venue_filter or venue_filter.upper() == "BETFAIR") and (
        not data_type_filter or data_type_filter == "instruments"
    ):
        cells.append(SmokeCell(asset_group="SPORTS", venue="BETFAIR", data_type="instruments"))

    return cells


# ---------------------------------------------------------------------------
# GCS / manifest verification
# ---------------------------------------------------------------------------


def resolve_test_bucket(asset_group: str, project_id: str | None = None) -> str:
    """Return the ``-test-`` bucket name for instruments-service + asset group.

    Prediction's instruments store is a DEDICATED flat yaml kind
    (``instruments-store-prediction`` -> ``instruments-store-pred-${DEPLOYMENT_ENV_SHORT}-
    ${pid}`` — the abbreviated ``pred`` token), NOT a ``PREDICTION`` entry in the
    per-asset_group ``instruments-store`` dict. So it MUST resolve via its own flat kind
    with ``deployment_env="test"`` — EXACTLY like the IS write path
    (``engine/orchestrator/catalogue.py::_get_instruments_bucket`` via
    ``resolve_instruments_store_kind``) and the MTDS harness's ``_test_bucket``. The
    generic ``get_bucket_name(...)+`.replace('-{pid}','-test-{pid}')`` path below yields
    the non-existent LONG ``instruments-store-prediction-test-{pid}`` (it uses ``prediction``
    verbatim as the asset_group suffix and never abbreviates to ``pred``), which 404s — the
    same string-mangle anti-pattern the real ``get_write_bucket_name`` fix already retired.
    Non-prediction asset_groups (cefi/tradfi/defi/sports) are byte-unchanged.
    """
    if asset_group.lower() == "prediction":
        return resolve_bucket_name(
            cloud="gcp",
            kind="instruments-store-prediction",
            deployment_env="test",
        )
    pid = project_id or get_project_id()
    prod = get_bucket_name("instruments", asset_group, pid)
    return prod.replace(f"-{pid}", f"-test-{pid}")


# PREDICTION writes ``instruments.parquet`` under the CQG-FIRST availability layout
# ``instrument_availability/by_date/canonical_question_group={CQG}/day={day}/venue={venue}/``
# (``engine/orchestrator/process_write.py::_write_prediction_venue``, partition
# ``{day, venue, canonical_question_group}`` — the sink orders the segments CQG-first). The
# data-dependent CQG segment PRECEDES ``day=``/``venue=``, so NO single literal day-first prefix
# (the shape every other asset_group uses) can ever match a prediction object. The prediction
# write-verify therefore lists under the base ``by_date/`` tree and substring-scopes by
# day+venue — mirrors MTDS ``_verify_write_scoped_to_data_type``. The filter is deliberately
# layout-agnostic (it also matches legacy day-first prediction objects), so it stays correct
# across the writer's CQG migration.
_PREDICTION_INSTRUMENTS_LIST_PREFIX = "instrument_availability/by_date/"
_PREDICTION_INSTRUMENTS_FILENAME = "instruments.parquet"


def is_prediction_venue_cell(cell: SmokeCell) -> bool:
    """True for a prediction venue-routed cell (POLYMARKET/KALSHI), whose writer lands the
    CQG-first availability layout. ``sports_provider`` cells never reach that writer, so they
    are excluded defensively (prediction never sets ``sports_provider`` anyway)."""
    return cell.asset_group.upper() == "PREDICTION" and not cell.sports_provider


def prediction_instruments_object_matches(name: str, smoke_date: str, venue: str) -> bool:
    """True iff a GCS object name is a prediction ``instruments.parquet`` for (smoke_date,
    venue), under EITHER the CQG-first or a legacy day-first availability layout — the
    substring test a day-first single-prefix listing cannot express."""
    return (
        name.endswith(_PREDICTION_INSTRUMENTS_FILENAME) and f"day={smoke_date}/" in name and f"venue={venue}/" in name
    )


def list_prediction_instruments_objects(
    bucket: str,
    smoke_date: str,
    venue: str,
    storage_client: object | None = None,
) -> list[str]:
    """Sorted GCS object names of the prediction ``instruments.parquet`` files for
    (smoke_date, venue) under the CQG-first availability layout — the single, shared
    day+venue-scoped enumeration every prediction verify/read path reuses (write-verify,
    skip-fingerprint, canonical read) so they all match the SAME object set."""
    client = storage_client if storage_client is not None else get_storage_client()
    blobs = client.list_blobs(  # pyright: ignore[reportAttributeAccessIssue]
        bucket=bucket, prefix=_PREDICTION_INSTRUMENTS_LIST_PREFIX
    )
    return sorted(
        getattr(b, "name", "")
        for b in blobs
        if prediction_instruments_object_matches(getattr(b, "name", ""), smoke_date, venue)
    )


def expected_write_prefix(cell: SmokeCell, smoke_date: str) -> str:
    """Return the GCS prefix under which the CLI is expected to land parquet(s).

    Provider-routed SPORTS cells (``cell.sports_provider`` set — API_FOOTBALL +
    T1 enrichment) write ``sports_reference/by_date/day=...`` via
    ``_fetch_sports_reference_data`` (NOT ``instrument_availability/``). PREDICTION
    venue cells land ``instruments.parquet`` under the CQG-FIRST availability layout
    (``instrument_availability/by_date/canonical_question_group={CQG}/day={day}/
    venue={venue}/``); the CQG segment is data-dependent and precedes day/venue, so the
    coarsest literal ancestor is the base ``instrument_availability/by_date/`` tree — the
    day+venue scope is applied by ``verify_prediction_parquet_written``'s substring filter,
    NOT by this prefix. Every other cell — including venue-routed SPORTS (bare BETFAIR) —
    flows through the generic per-venue instrument-catalog writer (``_write_venue`` /
    ``_write_all_venues``, ``instruments_service/engine/orchestrator/writers.py``), which
    always lands under ``instrument_availability/by_date/day={date}/venue={venue}/``
    regardless of asset_group.
    """
    if cell.sports_provider:
        return f"sports_reference/by_date/day={smoke_date}/"
    if is_prediction_venue_cell(cell):
        return _PREDICTION_INSTRUMENTS_LIST_PREFIX
    return f"instrument_availability/by_date/day={smoke_date}/venue={cell.venue}/"


def verify_parquet_written(
    bucket: str,
    prefix: str,
    storage_client: object | None = None,
) -> tuple[bool, int]:
    """Step 2: assert at least one parquet exists under ``gs://{bucket}/{prefix}``."""
    client = storage_client if storage_client is not None else get_storage_client()
    blobs = list(client.list_blobs(bucket=bucket, prefix=prefix))  # pyright: ignore[reportAttributeAccessIssue]
    parquet_blobs = [b for b in blobs if getattr(b, "name", "").endswith(".parquet")]
    return (bool(parquet_blobs), len(parquet_blobs))


def verify_prediction_parquet_written(
    bucket: str,
    smoke_date: str,
    venue: str,
    storage_client: object | None = None,
) -> tuple[bool, int]:
    """PREDICTION Step-2 write-verify against the CQG-FIRST availability layout.

    Lists under ``instrument_availability/by_date/`` and counts ``instruments.parquet``
    objects whose path carries BOTH ``day={smoke_date}/`` AND ``venue={venue}/`` (see
    ``prediction_instruments_object_matches``). The day-first single-prefix listing
    ``verify_parquet_written`` uses for cefi/tradfi/defi/sports can NEVER match prediction's
    CQG-first objects — the CQG segment precedes day/venue — so it reports n=0 even when real
    objects exist; prediction needs this substring post-filter instead."""
    names = list_prediction_instruments_objects(bucket, smoke_date, venue, storage_client)
    return (bool(names), len(names))


def verify_manifest_row(
    bucket: str,
    cell: SmokeCell,
    smoke_date: str,
    storage_client: object | None = None,
) -> tuple[bool, str]:
    """Step 3: assert a row with acceptable ``capture_status`` exists in the TEST manifest.

    Returns ``(ok, capture_status)``. ``empty_confirmed`` is an acceptable
    status — it means the adapter legitimately found zero rows for the date.
    """
    import pandas as pd

    client = storage_client if storage_client is not None else get_storage_client()
    manifest_path = "_index/availability_index.parquet"
    try:
        blob = client.bucket(bucket).blob(manifest_path)  # pyright: ignore[reportAttributeAccessIssue]
        if not blob.exists():
            return (False, "manifest_missing")
        raw = blob.download_as_bytes()
    except Exception as exc:  # pragma: no cover — storage transport failures
        logger.warning("manifest read failed bucket=%s: %s", bucket, exc)
        return (False, "manifest_read_error")

    from io import BytesIO

    df = pd.read_parquet(BytesIO(raw))
    if df.empty:
        return (False, "manifest_empty")

    # Filter by the cell's shard tuple. Provider-routed SPORTS cells (API_FOOTBALL
    # + T1 enrichment) key on sports_provider/entity, not venue+data_type — the
    # writer stamps manifest_venue="" for those (see writers.py::_write_venue).
    # Venue-routed SPORTS (bare BETFAIR) writes through the SAME generic
    # venue+data_type keying as CEFI/DEFI/TRADFI, so it must NOT skip that filter.
    is_sports_provider_cell = cell.asset_group == "SPORTS" and bool(cell.sports_provider)
    mask = df.get("date", df.get("day")) == smoke_date
    if "asset_group" in df.columns:
        mask = mask & (df["asset_group"].astype(str).str.upper() == cell.asset_group)
    elif "category" in df.columns:
        mask = mask & (df["category"].astype(str).str.upper() == cell.asset_group)
    if "venue" in df.columns and not is_sports_provider_cell:
        mask = mask & (df["venue"] == cell.venue)
    if "data_type" in df.columns and not is_sports_provider_cell:
        mask = mask & (df["data_type"] == cell.data_type)

    matching = df[mask]
    if matching.empty:
        return (False, "no_matching_row")

    # Prefer the most recent attempt; accept captured OR empty_confirmed.
    status = str(matching["capture_status"].iloc[-1]) if "capture_status" in matching.columns else ""
    ok = status in ("captured", "empty_confirmed")
    return (ok, status)


# ---------------------------------------------------------------------------
# Per-cell runner (3-step contract)
# ---------------------------------------------------------------------------


def build_cli_args(cell: SmokeCell, smoke_date: str) -> list[str]:
    """Build the subprocess argv for one cell. SSOT: cli-convention.md (--operation/--mode/--asset-group)."""
    argv: list[str] = [
        sys.executable,
        "-m",
        _SERVICE_MODULE,
        "--operation",
        "instruments",
        "--mode",
        "batch",
        "--asset-group",
        cell.asset_group,
        "--start-date",
        smoke_date,
        "--end-date",
        smoke_date,
    ]
    if cell.sports_provider:
        # Provider-routed SPORTS cell (API_FOOTBALL + T1 enrichment): --sports-provider.
        argv.extend(["--sports-provider", cell.sports_provider])
    else:
        # Every other cell — including venue-routed SPORTS (bare BETFAIR) — uses
        # --venues like CEFI/DEFI/TRADFI. Falling through to a bare --asset-group
        # SPORTS invocation here (the pre-fix behaviour for a sports_provider=None
        # cell) silently ran the FULL default SPORTS provider set instead of the
        # one requested venue.
        argv.extend(["--venues", cell.venue])
    return argv


def run_cell(
    cell: SmokeCell,
    smoke_date: str,
    project_id: str | None = None,
    timeout_sec: int = _DEFAULT_TIMEOUT_SEC,
    subprocess_runner: object | None = None,
    storage_client: object | None = None,
) -> CellResult:
    """Run a single cell through the 3-step assertion contract.

    Shard-level isolation: all failures caught, logged, and returned as
    ``status="failed"``. No exception propagates out of this function.
    """
    attempt_ts = datetime.now(UTC).isoformat()
    started = datetime.now(UTC)
    result = CellResult(cell=cell, status="failed", attempt_ts=attempt_ts)

    bucket = resolve_test_bucket(cell.asset_group, project_id)
    prefix = expected_write_prefix(cell, smoke_date)
    argv = build_cli_args(cell, smoke_date)
    env = dict(os.environ)
    env["IS_TEST_RUN"] = "true"

    runner = subprocess_runner if subprocess_runner is not None else subprocess.run
    try:
        logger.info("[cell start] %s argv=%s", cell.label(), " ".join(argv))
        completed = runner(  # pyright: ignore[reportGeneralTypeIssues]
            argv,
            env=env,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        result.reason = f"cli_timeout_{timeout_sec}s"
        result.stderr_tail = str(exc)[-2000:]
        return _finalise(result, started)
    except Exception as exc:  # pragma: no cover — subprocess infra failure
        result.reason = f"cli_launch_error:{type(exc).__name__}"
        result.stderr_tail = str(exc)[-2000:]
        return _finalise(result, started)

    stdout = (
        completed.stdout.decode("utf-8", errors="replace")
        if isinstance(completed.stdout, bytes)
        else str(completed.stdout or "")
    )
    stderr = (
        completed.stderr.decode("utf-8", errors="replace")
        if isinstance(completed.stderr, bytes)
        else str(completed.stderr or "")
    )
    result.stdout_tail = stdout[-2000:]
    result.stderr_tail = stderr[-2000:]

    # SPORTS T1 pre-flight DependencyError => SKIP, not FAIL.
    if completed.returncode != 0:
        if "DependencyError" in stderr or "api-football reference data missing" in stderr:
            result.status = "skipped"
            result.reason = "api_football_missing"
            return _finalise(result, started)
        result.reason = f"cli_nonzero_rc={completed.returncode}"
        return _finalise(result, started)

    # Step 2: verify parquet written. PREDICTION writes the CQG-FIRST availability layout
    # (CQG segment precedes day/venue), so it lists the base by_date/ tree + substring-scopes
    # by day+venue rather than the day-first single-prefix listing the other asset_groups use.
    try:
        if is_prediction_venue_cell(cell):
            ok_parquet, count = verify_prediction_parquet_written(bucket, smoke_date, cell.venue, storage_client)
        else:
            ok_parquet, count = verify_parquet_written(bucket, prefix, storage_client)
    except Exception as exc:  # pragma: no cover — storage transport failure
        result.reason = f"gcs_list_error:{type(exc).__name__}"
        return _finalise(result, started)
    result.parquet_count = count
    if not ok_parquet:
        result.reason = f"no_parquet_at:gs://{bucket}/{prefix}"
        return _finalise(result, started)

    # Step 3: verify manifest row.
    try:
        ok_manifest, manifest_status = verify_manifest_row(bucket, cell, smoke_date, storage_client)
    except Exception as exc:  # pragma: no cover — manifest read failure
        result.reason = f"manifest_read_error:{type(exc).__name__}"
        return _finalise(result, started)
    result.manifest_status = manifest_status
    if not ok_manifest:
        result.reason = f"manifest_status_invalid:{manifest_status}"
        return _finalise(result, started)

    result.status = "passed"
    return _finalise(result, started)


def _finalise(result: CellResult, started: datetime) -> CellResult:
    """Stamp duration and log one-liner. Always returns the result unchanged-shape."""
    result.duration_sec = (datetime.now(UTC) - started).total_seconds()
    logger.info(
        "[cell done ] %s status=%s reason=%s duration=%.1fs parquet=%d manifest=%s",
        result.cell.label(),
        result.status,
        result.reason,
        result.duration_sec,
        result.parquet_count,
        result.manifest_status,
    )
    return result


# ---------------------------------------------------------------------------
# Matrix driver + CLI
# ---------------------------------------------------------------------------


def run_matrix(
    cells: list[SmokeCell],
    smoke_date: str,
    execute: bool,
    project_id: str | None = None,
    timeout_sec: int = _DEFAULT_TIMEOUT_SEC,
) -> SmokeReport:
    """Run every cell with shard-level isolation. Returns a structured report."""
    report = SmokeReport(
        service=_SERVICE_NAME,
        smoke_date=smoke_date,
        total_cells=len(cells),
        started_at=datetime.now(UTC).isoformat(),
    )

    if not execute:
        # Dry-run: enumerate only, mark every cell as "skipped" with reason=dry_run.
        for cell in cells:
            report.results.append(
                CellResult(
                    cell=cell,
                    status="skipped",
                    reason="dry_run",
                    attempt_ts=datetime.now(UTC).isoformat(),
                )
            )
            report.skipped += 1
        report.finished_at = datetime.now(UTC).isoformat()
        return report

    for cell in cells:
        cell_result = run_cell(cell, smoke_date, project_id=project_id, timeout_sec=timeout_sec)
        report.results.append(cell_result)
        if cell_result.status == "passed":
            report.passed += 1
        elif cell_result.status == "skipped":
            report.skipped += 1
        else:
            report.failed += 1

    report.finished_at = datetime.now(UTC).isoformat()
    return report


def print_summary(report: SmokeReport) -> None:
    print(f"\n==== instruments-service smoke matrix ({report.smoke_date}) ====")
    print(f"total:   {report.total_cells}")
    print(f"passed:  {report.passed}")
    print(f"failed:  {report.failed}")
    print(f"skipped: {report.skipped}")
    if report.failed:
        print("\n-- failed cells --")
        for r in report.results:
            if r.status == "failed":
                print(f"  FAIL  {r.cell.label():<60} reason={r.reason}")
    if report.skipped:
        print("\n-- skipped cells --")
        for r in report.results:
            if r.status == "skipped":
                print(f"  SKIP  {r.cell.label():<60} reason={r.reason}")


def write_json_report(report: SmokeReport, path: str) -> None:
    """Persist a structured report for CI / dashboards."""
    payload = asdict(report)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.info("wrote smoke report to %s", path)


def _default_smoke_date() -> str:
    return (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="instruments-service-smoke",
        description="instruments-service smoke matrix (TEST-bucket routed).",
    )
    parser.add_argument(
        "--asset-group", type=str, default=None, help="Restrict to one category (CEFI/TRADFI/DEFI/SPORTS/PREDICTION)"
    )
    parser.add_argument("--venue", type=str, default=None, help="Restrict to one venue (or sports provider)")
    parser.add_argument("--data-type", type=str, default=None, help="Restrict to one data_type")
    parser.add_argument("--date", type=str, default=None, help="Smoke date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually run the CLI per cell. Without this flag, only enumeration is performed.",
    )
    parser.add_argument("--report", type=str, default=None, help="Write JSON report to this path.")
    parser.add_argument("--timeout-sec", type=int, default=_DEFAULT_TIMEOUT_SEC, help="Per-cell subprocess timeout.")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    smoke_date = args.date or _default_smoke_date()
    cells = enumerate_cells(
        asset_group_filter=args.asset_group,
        venue_filter=args.venue,
        data_type_filter=args.data_type,
    )
    logger.info(
        "enumerated %d cells (asset_group=%s venue=%s data_type=%s execute=%s)",
        len(cells),
        args.asset_group,
        args.venue,
        args.data_type,
        args.execute,
    )

    report = run_matrix(
        cells=cells,
        smoke_date=smoke_date,
        execute=args.execute,
        timeout_sec=args.timeout_sec,
    )
    print_summary(report)

    if args.report:
        write_json_report(report, args.report)

    # rc=1 if any cell failed; rc=0 otherwise (dry-run always rc=0).
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
