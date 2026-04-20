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
    python -m instruments_service.smoke --execute --category CEFI

    # JSON report for CI parsing
    python -m instruments_service.smoke --execute --report /tmp/smoke.json

Phase 3 dependency: sports cells respect the api-football T0 ordering. When
``IS_TEST_RUN=true``, the adapter factory raises ``DependencyError`` for
api-football-dependent venues unless api-football has already been run for the
date. The smoke matrix catches this and marks the cell ``skipped`` with
``reason=api_football_missing`` rather than ``attempted_failed``.
"""

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

from unified_api_contracts import DATA_TYPES_BY_CATEGORY, VENUES_BY_CATEGORY
from unified_trading_library import (
    get_bucket_name,
    get_project_id,
    get_storage_client,
)

logger = logging.getLogger(__name__)

CellStatus = Literal["passed", "failed", "skipped"]

_SPORTS_T0_PROVIDER: str = "API_FOOTBALL"
_SPORTS_T0_VENUE: str = "ODDS_API"  # Placeholder: sports reference is driven via --sports-provider, not venue.
_DEFAULT_TIMEOUT_SEC: int = 600
_SERVICE_MODULE: str = "instruments_service"
_SERVICE_NAME: str = "instruments-service"


@dataclass
class SmokeCell:
    """A single (service, category, venue, data_type) smoke cell."""

    category: str
    venue: str
    data_type: str
    sports_provider: str | None = None

    def label(self) -> str:
        base = f"{self.category}:{self.venue}:{self.data_type}"
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
    category_filter: str | None = None,
    venue_filter: str | None = None,
    data_type_filter: str | None = None,
) -> list[SmokeCell]:
    """Enumerate viable (category, venue, data_type) cells for instruments-service.

    SPORTS cells are specialised: instruments-service drives SPORTS via
    ``--sports-provider`` rather than ``--venues`` + ``--data-types``. api-football
    (T0) is emitted FIRST; T1 enrichment providers follow — see
    ``codex/02-data/sports-adapter-dependency-order.md``.
    """
    cells: list[SmokeCell] = []
    categories = [c.upper() for c in DATA_TYPES_BY_CATEGORY]

    for category in categories:
        if category_filter and category != category_filter.upper():
            continue

        if category == "SPORTS":
            cells.extend(_enumerate_sports_cells(data_type_filter, venue_filter))
            continue

        venues = VENUES_BY_CATEGORY.get(category.lower(), [])
        data_types = DATA_TYPES_BY_CATEGORY.get(category.lower(), [])
        for venue in venues:
            if venue_filter and venue != venue_filter:
                continue
            for data_type in data_types:
                if data_type_filter and data_type != data_type_filter:
                    continue
                cells.append(SmokeCell(category=category, venue=venue, data_type=data_type))

    return cells


def _enumerate_sports_cells(
    data_type_filter: str | None,
    venue_filter: str | None,
) -> list[SmokeCell]:
    """SPORTS enumeration honours T0 (api-football) first, then T1 enrichment.

    Each provider is modelled as a cell. The smoke matrix orders them
    deterministically so the pre-flight DependencyError for T1-without-T0
    fires only when an operator explicitly skips the T0 cell.
    """
    providers_ordered: list[str] = [
        _SPORTS_T0_PROVIDER,  # T0
        "OPEN_METEO",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "UNDERSTAT",
        "FOOTYSTATS",
    ]
    sports_data_types = DATA_TYPES_BY_CATEGORY.get("sports", [])
    cells: list[SmokeCell] = []
    for provider in providers_ordered:
        # SPORTS instruments-service writes reference data (fixtures, odds, etc.).
        # Sports venue filter is a pass-through — instruments-service routes by
        # --sports-provider rather than --venues.
        if venue_filter and provider != venue_filter:
            continue
        for data_type in sports_data_types:
            if data_type_filter and data_type != data_type_filter:
                continue
            cells.append(
                SmokeCell(
                    category="SPORTS",
                    venue=provider,  # model provider as the "venue" axis for SPORTS
                    data_type=data_type,
                    sports_provider=provider,
                )
            )
    return cells


# ---------------------------------------------------------------------------
# GCS / manifest verification
# ---------------------------------------------------------------------------


def resolve_test_bucket(category: str, project_id: str | None = None) -> str:
    """Return the ``-test-`` bucket name for instruments-service + category."""
    pid = project_id or get_project_id()
    prod = get_bucket_name("instruments", category, pid)
    return prod.replace(f"-{pid}", f"-test-{pid}")


def expected_write_prefix(cell: SmokeCell, smoke_date: str) -> str:
    """Return the GCS prefix under which the CLI is expected to land parquet(s).

    SPORTS uses ``sports_reference/by_date/day=...`` (NOT ``instrument_availability/``).
    Other categories use ``instrument_availability/by_date/day={date}/venue={venue}/``.
    """
    if cell.category == "SPORTS":
        return f"sports_reference/by_date/day={smoke_date}/"
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

    # Filter by the cell's shard tuple. SPORTS manifests key on
    # sports_provider/entity, others key on venue+data_type.
    mask = df.get("date", df.get("day")) == smoke_date
    if "category" in df.columns:
        mask = mask & (df["category"].astype(str).str.upper() == cell.category)
    if "venue" in df.columns and cell.category != "SPORTS":
        mask = mask & (df["venue"] == cell.venue)
    if "data_type" in df.columns and cell.category != "SPORTS":
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
    """Build the subprocess argv for one cell. SSOT: cli-convention.md (--operation/--mode/--category)."""
    argv: list[str] = [
        sys.executable,
        "-m",
        _SERVICE_MODULE,
        "--operation",
        "instruments",
        "--mode",
        "batch",
        "--category",
        cell.category,
        "--start-date",
        smoke_date,
        "--end-date",
        smoke_date,
    ]
    if cell.category == "SPORTS":
        # SPORTS is driven via --sports-provider, not --venues.
        if cell.sports_provider:
            argv.extend(["--sports-provider", cell.sports_provider])
    else:
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

    bucket = resolve_test_bucket(cell.category, project_id)
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

    # Step 2: verify parquet written.
    try:
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
        "--category", type=str, default=None, help="Restrict to one category (CEFI/TRADFI/DEFI/SPORTS/PREDICTION)"
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
        category_filter=args.category,
        venue_filter=args.venue,
        data_type_filter=args.data_type,
    )
    logger.info(
        "enumerated %d cells (category=%s venue=%s data_type=%s execute=%s)",
        len(cells),
        args.category,
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
