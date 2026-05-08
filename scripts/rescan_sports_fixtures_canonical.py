#!/usr/bin/env python3
"""Rescan GCS sports parquets and emit per-(date, data_type, league_id) manifest rows.

SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md`` (Phase 5 fix)
      ``unified-trading-pm/plans/active/sports_manifest_shard_migration_cleanup_2026_04_21.md``

**Why a new script?** ``rescan_sports_manifest.py`` (the existing rescan) dedups
by entity and writes ``league_id=""`` — it can't produce per-league drilldown.
This script walks existing GCS parquets and emits honest per-league manifest
rows WITHOUT re-running adapters.

Multi-entity support
--------------------

Use ``--entity-type`` to pick which entity to rescan:

* ``FIXTURES`` (default) — fixtures.parquet per day, group by ``af_league_id``
  via ``get_league_by_api_football_id()``.
* ``WEATHER`` — weather.parquet per day, join venue_id→league_id via fixtures.
* ``XG`` — understat_xg.parquet per day, group by ``league`` column.

Entity handlers are registered in ``_ENTITY_HANDLERS``. Each handler declares
its own GCS prefix, blob filename, and scanner function. The scaffold is
designed to accept additional entities (INJURIES, STANDINGS, FIXTURE_STATS,
FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS) via drop-in handler entries.

Three execution modes (single-VM, worker, coordinator) — see ``Modes`` below.
Designed for VM execution — do NOT run on a laptop over a multi-year window.

Modes
-----

**single-VM (default)** — one process scans all dates (or the ``--date`` arg)
and writes directly to the canonical index. Last-writer-wins: safe only when
no other rescan is running. This is the historical behaviour and is still
the right shape for small/interactive backfills.

**worker** (``--chunk-id X --run-id Y --date-start A --date-end B``) — scans
a disjoint date range and writes rows to
``_index/partial/<run-id>/<chunk-id>.parquet``. Never reads or writes the
canonical index. Safe to run N of these in parallel as long as ``--run-id``
is shared and ``--chunk-id`` values are distinct.

**coordinator** (``--coordinate --run-id Y``) — reads the canonical index,
globs ``_index/partial/<run-id>/*.parquet``, merges (preserving non-FIXTURES
rows + replacing per-(date, league_id) FIXTURES rows with the partial
contents), writes the canonical index atomically, then deletes the partial
shards. Must run exactly once after all workers finish.

SSOT for the chunk-safe pattern:
``codex/02-data/chunk-safe-manifest-migrations.md``.

VM launch recipe
----------------

1. Refresh the SPORTS tarball::

    bash deployment-service/scripts/vm/create-code-tarballs.sh --asset-group SPORTS

2. Launch a small-test VM first (single date)::

    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh \\
      --date 2024-09-01 --dry-run

   Verify the dry-run output lists the expected per-league counts, then re-run
   without ``--dry-run``. Confirm the manifest row count changes as expected
   via ``python instruments-service/scripts/verify_instrument_manifest_coverage.py``.

3. Small backfill (single VM, all dates)::

    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh --workers 16

4. Large parallel backfill (N worker VMs + 1 coordinator)::

    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh --chunks 10
    # wait for all workers to finish (see launcher output for how to poll)
    bash deployment-service/scripts/launch-sports-manifest-rescan-vm.sh --coordinate --run-id <stamp>

   Singleton-locked: the coordinator refuses to launch if another coordinator is
   running. Workers are allowed to coexist as long as chunk-ids are distinct.

Usage
-----

::

    # Dry run — show what would be written without touching the index
    python scripts/rescan_sports_fixtures_canonical.py --dry-run --date 2024-09-01

    # Single-VM, single date, write canonical index
    python scripts/rescan_sports_fixtures_canonical.py --date 2024-09-01

    # Single-VM full rescan (VM only — see VM launch recipe above)
    python scripts/rescan_sports_fixtures_canonical.py --workers 16

    # Worker chunk (writes to _index/partial/<run-id>/<chunk-id>.parquet)
    python scripts/rescan_sports_fixtures_canonical.py \\
      --chunk-id 3-of-10 --run-id 20260421-120000 \\
      --date-start 2024-01-01 --date-end 2024-04-15 --workers 16

    # Coordinator (merges partials into canonical index)
    python scripts/rescan_sports_fixtures_canonical.py \\
      --coordinate --run-id 20260421-120000
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import cast

import pandas as pd
from unified_api_contracts.sports import (
    get_expected_leagues_for_source,
    get_league_by_api_football_id,
    get_league_fixture_calendar,
)
from unified_trading_library import ManifestMigrator, StorageClient, chunked_date_ranges, get_storage_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_NOW = datetime.now(UTC).isoformat()

BUCKET_NAME = "instruments-store-sports-central-element-323112"
FIXTURES_PREFIX = "sports_reference/by_date/"
INDEX_BLOB = "_index/availability_index.parquet"

_AF_LEAGUES_CACHE: list[str] | None = None
_IN_SEASON_CACHE: dict[str, frozenset[str]] = {}


def _af_league_ids() -> list[str]:
    """Canonical league_ids sourced from api-football (cached once per process)."""
    global _AF_LEAGUES_CACHE
    if _AF_LEAGUES_CACHE is None:
        _AF_LEAGUES_CACHE = [league.league_id for league in get_expected_leagues_for_source("api_football")]
    return _AF_LEAGUES_CACHE


def _leagues_in_season_on(date_str: str) -> frozenset[str]:
    """Canonical league_ids that are in-season on ``date_str``."""
    cached = _IN_SEASON_CACHE.get(date_str)
    if cached is not None:
        return cached
    out: set[str] = set()
    for lid in _af_league_ids():
        if get_league_fixture_calendar(lid, date_str, date_str):
            out.add(lid)
    frozen = frozenset(out)
    _IN_SEASON_CACHE[date_str] = frozen
    return frozen


def _parse_date(blob_name: str) -> str | None:
    for p in blob_name.split("/"):
        if p.startswith("day="):
            return p[4:]
    return None


def _list_entity_blob_paths(
    storage: StorageClient,
    bucket: str,
    handler: _EntityHandler,
    date_str: str | None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[str]:
    """List GCS blobs matching ``handler`` for a single date or a date range."""
    prefix = f"{FIXTURES_PREFIX}day={date_str}/{handler.prefix_suffix}" if date_str else FIXTURES_PREFIX
    suffix_match = f"/{handler.prefix_suffix}{handler.blob_filename}"

    start_d = date.fromisoformat(date_start) if date_start else None
    end_d = date.fromisoformat(date_end) if date_end else None

    matches: list[str] = []
    for meta in storage.list_blobs(bucket, prefix=prefix):
        if not meta.name.endswith(suffix_match):
            continue
        if start_d is not None or end_d is not None:
            parsed = _parse_date(meta.name)
            if parsed is None:
                continue
            try:
                d = date.fromisoformat(parsed)
            except ValueError:
                continue
            if start_d is not None and d < start_d:
                continue
            if end_d is not None and d > end_d:
                continue
        matches.append(meta.name)
    return matches


def _manifest_row(
    *,
    date_str: str,
    data_type: str,
    league_id: str,
    count: int,
    capture_status: str,
) -> dict[str, object]:
    """Build one v5 manifest row for a (date, data_type, league_id) shard."""
    return {
        "date": date_str,
        "venue": "",
        "data_type": data_type,
        "service_name": "instruments-service",
        "instrument_count": int(count),
        "written_at": _NOW,
        "schema_version": 5,
        "timeframe": "",
        "league_id": league_id,
        "chain": "",
        "instrument_type": "",
        "capture_status": capture_status,
        "error_reason": "",
        "attempted_at": _NOW,
        "expected": True,
        "available": capture_status == "captured",
    }


def _fixture_row(
    *,
    date_str: str,
    league_id: str,
    count: int,
    capture_status: str,
) -> dict[str, object]:
    """Build one v5 manifest row for a (date, league_id) FIXTURES shard."""
    return _manifest_row(
        date_str=date_str,
        data_type="FIXTURES",
        league_id=league_id,
        count=count,
        capture_status=capture_status,
    )


def _scan_fixtures_blob(storage: StorageClient, bucket: str, blob_path: str) -> list[dict[str, object]]:
    """Read one fixtures.parquet and emit per-canonical-league manifest rows.

    Groups fixtures by ``af_league_id`` and resolves to canonical league via
    UAC ``get_league_by_api_football_id``. Emits ``empty_confirmed`` for
    in-season leagues that had zero fixtures that day.
    """
    date_str = _parse_date(blob_path)
    if date_str is None:
        return []

    try:
        raw = storage.download_bytes(bucket, blob_path)
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("Failed to read %s: %s", blob_path, exc)
        return []

    per_league: dict[str, int] = {}
    unmapped = 0
    if not df.empty and "af_league_id" in df.columns:
        for af_id, group in df.groupby("af_league_id"):
            try:
                af_id_int = int(af_id)
            except (ValueError, TypeError):
                unmapped += len(group)
                continue
            league = get_league_by_api_football_id(af_id_int)
            if league is None:
                unmapped += len(group)
                continue
            per_league[league.league_id] = per_league.get(league.league_id, 0) + len(group)

    entries: list[dict[str, object]] = []
    for lid, count in per_league.items():
        entries.append(_fixture_row(date_str=date_str, league_id=lid, count=count, capture_status="captured"))

    captured_lids = set(per_league.keys())
    in_season = _leagues_in_season_on(date_str)
    for lid in sorted(in_season - captured_lids):
        entries.append(_fixture_row(date_str=date_str, league_id=lid, count=0, capture_status="empty_confirmed"))

    if unmapped:
        logger.debug("%s: %d fixtures unmapped (af_league_id not in LEAGUE_REGISTRY)", date_str, unmapped)

    return entries


# Back-compat alias — preserved for callers that imported ``_scan_blob_path``
# (e.g. unit tests prior to Phase-1 refactor).
_scan_blob_path = _scan_fixtures_blob


# ── WEATHER entity scanner ──────────────────────────────────────────────────
#
# Weather parquets live at ``sports_reference/by_date/day={D}/entity=weather/
# weather.parquet``. The parquet has columns ``venue_id``, etc.  venue_id →
# league_id requires a join against the same day's fixtures.parquet.


def _load_venue_to_leagues(
    storage: StorageClient,
    bucket: str,
    date_str: str,
) -> dict[str, set[str]]:
    """Build ``venue_id → {league_id, ...}`` from ``entity=fixtures`` for ``date_str``.

    Weather captures are keyed by ``venue_id`` (SCREAMING_SNAKE'd stadium key).
    We need to join against fixtures to reach league granularity. A venue can
    host multiple leagues on the same day (cup + league doubles), so return a
    set-valued mapping.
    """
    prefix = f"{FIXTURES_PREFIX}day={date_str}/entity=fixtures/"
    mapping: dict[str, set[str]] = {}
    for meta in storage.list_blobs(bucket, prefix=prefix):
        if not meta.name.endswith("fixtures.parquet"):
            continue
        try:
            raw = storage.download_bytes(bucket, meta.name)
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("WEATHER rescan: failed to read %s: %s", meta.name, exc)
            continue
        if df.empty or "venue_name" not in df.columns or "league_id" not in df.columns:
            continue
        # Orchestrator's WEATHER emission uses SCREAMING_SNAKE'd venue_name as
        # venue_id. Replicate the same normalisation here so we match.
        for _, row in df[["venue_name", "league_id"]].dropna().iterrows():
            vname = cast("str", str(row["venue_name"])).strip()
            lid = cast("str", str(row["league_id"])).strip()
            if not vname or not lid:
                continue
            snake = re.sub(r"\s+", "_", re.sub(r"[^A-Za-z0-9 ]", "", vname).strip()).upper()
            mapping.setdefault(snake, set()).add(lid)
    return mapping


def _scan_weather_blob(storage: StorageClient, bucket: str, blob_path: str) -> list[dict[str, object]]:
    """Read one weather.parquet, join venue→league via fixtures, emit per-league rows."""
    date_str = _parse_date(blob_path)
    if date_str is None:
        return []

    try:
        raw = storage.download_bytes(bucket, blob_path)
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("WEATHER rescan: failed to read %s: %s", blob_path, exc)
        return []

    if df.empty or "venue_id" not in df.columns:
        return []

    venue_to_leagues = _load_venue_to_leagues(storage, bucket, date_str)

    per_league: dict[str, int] = {}
    for vid_raw in df["venue_id"].dropna().astype(str).unique():
        vid = cast("str", str(vid_raw))
        for lid in venue_to_leagues.get(vid, set()):
            per_league[lid] = per_league.get(lid, 0) + 1

    entries: list[dict[str, object]] = []
    for lid, count in per_league.items():
        entries.append(
            _manifest_row(
                date_str=date_str,
                data_type="WEATHER",
                league_id=lid,
                count=count,
                capture_status="captured",
            )
        )
    return entries


# ── XG entity scanner ──────────────────────────────────────────────────────
#
# XG parquets live at ``sports_reference/by_date/day={D}/entity=understat_xg/
# understat_xg.parquet`` (legacy) or ``.../league=<lid>/understat_xg.parquet``
# (per-league partitioned). Group by ``league`` column.


def _scan_xg_blob(storage: StorageClient, bucket: str, blob_path: str) -> list[dict[str, object]]:
    """Read one understat_xg.parquet and emit per-league manifest rows."""
    date_str = _parse_date(blob_path)
    if date_str is None:
        return []

    try:
        raw = storage.download_bytes(bucket, blob_path)
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("XG rescan: failed to read %s: %s", blob_path, exc)
        return []

    if df.empty or "league" not in df.columns:
        return []

    per_league: dict[str, int] = {}
    for lid, group in df.groupby("league"):
        lid_s = cast("str", str(lid)).strip()
        if not lid_s:
            continue
        per_league[lid_s] = per_league.get(lid_s, 0) + len(group)

    entries: list[dict[str, object]] = []
    for lid, count in per_league.items():
        entries.append(
            _manifest_row(
                date_str=date_str,
                data_type="XG",
                league_id=lid,
                count=count,
                capture_status="captured",
            )
        )
    return entries


# ── Entity-handler registry ────────────────────────────────────────────────


@dataclass(frozen=True)
class _EntityHandler:
    """Config for one rescan entity: where to look, how to scan, what to purge."""

    data_type: str
    prefix_suffix: str  # appended after ``day={D}/`` to form the blob prefix
    blob_filename: str  # the parquet filename to match
    scan_blob: Callable[[StorageClient, str, str], list[dict[str, object]]]
    # Predicate for canonical-index row removal (rows this rescan replaces).
    drop_filter_data_type: str  # data_type value whose rows to drop on merge


_FIXTURES_HANDLER = _EntityHandler(
    data_type="FIXTURES",
    prefix_suffix="entity=fixtures/",
    blob_filename="fixtures.parquet",
    scan_blob=_scan_fixtures_blob,
    drop_filter_data_type="FIXTURES",
)

_WEATHER_HANDLER = _EntityHandler(
    data_type="WEATHER",
    prefix_suffix="entity=weather/",
    blob_filename="weather.parquet",
    scan_blob=_scan_weather_blob,
    drop_filter_data_type="WEATHER",
)

_XG_HANDLER = _EntityHandler(
    data_type="XG",
    prefix_suffix="entity=understat_xg/",
    blob_filename="understat_xg.parquet",
    scan_blob=_scan_xg_blob,
    drop_filter_data_type="XG",
)


_ENTITY_HANDLERS: dict[str, _EntityHandler] = {
    "FIXTURES": _FIXTURES_HANDLER,
    "WEATHER": _WEATHER_HANDLER,
    "XG": _XG_HANDLER,
}


def _build_drop_filter(entity_data_type: str) -> Callable[[pd.Series], bool]:
    """Build the ManifestMigrator drop filter for ``entity_data_type``.

    Keeps rows with non-empty ``league_id`` (per-league ones we're about to
    replace). Legacy ``league_id=""`` rows for the same entity are preserved
    here — the separate purge step (``purge_legacy_unsharded_manifest_rows.py``)
    handles them.
    """

    def _filter(row: pd.Series) -> bool:
        lid = row.get("league_id", "")
        lid_s = "" if lid is None or (isinstance(lid, float) and pd.isna(lid)) else str(lid)
        return (
            str(row.get("data_type", "")) == entity_data_type
            and str(row.get("service_name", "")) == "instruments-service"
            and lid_s != ""
        )

    return _filter


def _scan_range(
    storage: StorageClient,
    bucket: str,
    blob_paths: list[str],
    workers: int,
    *,
    handler: _EntityHandler = _FIXTURES_HANDLER,
    migrator: ManifestMigrator | None = None,
    run_id: str | None = None,
) -> list[dict[str, object]]:
    """Parallel-scan blobs using ``handler.scan_blob``. Shared across modes."""
    all_entries: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(handler.scan_blob, storage, bucket, p): p for p in blob_paths}
        for done, future in enumerate(as_completed(futures), 1):
            try:
                all_entries.extend(future.result())
            except Exception as exc:
                logger.warning("scan failed: %s", exc)
            if done % 200 == 0:
                logger.info("Progress: %d / %d", done, len(blob_paths))
                if migrator is not None:
                    migrator.notify_progress(done, len(blob_paths), run_id=run_id)
    return all_entries


# Back-compat alias — callers upstream of the Phase-1 refactor (including
# existing unit tests) import this name directly. New call sites should use
# ``_build_drop_filter("<DATA_TYPE>")``.
_drop_instruments_fixtures_per_league = _build_drop_filter("FIXTURES")


def _print_top_leagues(entries: list[dict[str, object]], *, data_type: str) -> None:
    df = pd.DataFrame(entries)
    logger.info(
        "Top-10 leagues by %s instrument_count (Phase 5 target >= 200 per league/season):",
        data_type,
    )
    top = df.groupby("league_id")["instrument_count"].sum().sort_values(ascending=False).head(10)
    print(top.to_string())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rescan sports parquets (FIXTURES/WEATHER/XG) with canonical league mapping"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing index/partial")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (thread pool for blob scan)")
    parser.add_argument("--bucket", type=str, default=BUCKET_NAME, help="GCS bucket")
    parser.add_argument(
        "--entity-type",
        type=str,
        default="FIXTURES",
        choices=sorted(_ENTITY_HANDLERS.keys()),
        help="Which entity to rescan (default: FIXTURES).",
    )

    parser.add_argument("--date", type=str, help="Scan a single date (YYYY-MM-DD)")
    parser.add_argument("--date-start", type=str, help="Inclusive date lower bound (YYYY-MM-DD, worker mode)")
    parser.add_argument("--date-end", type=str, help="Inclusive date upper bound (YYYY-MM-DD, worker mode)")

    parser.add_argument("--chunk-id", type=str, help="Worker chunk label (e.g. '3-of-10'). Enables worker mode.")
    parser.add_argument(
        "--run-id",
        type=str,
        help="Shared run identifier for all workers + coordinator in one rescan job",
    )
    parser.add_argument(
        "--coordinate",
        action="store_true",
        help="Coordinator mode — merge _index/partial/<run-id>/*.parquet into canonical index",
    )
    parser.add_argument(
        "--split-range",
        nargs=3,
        metavar=("START", "END", "N"),
        help="Utility: print N tab-separated chunk boundaries for [START..END] and exit (no GCS calls)",
    )

    args = parser.parse_args()

    dry_run: bool = bool(args.dry_run)
    workers: int = int(args.workers)
    bucket_name: str = str(args.bucket)
    entity_type: str = str(args.entity_type).upper()
    date_single: str | None = str(args.date) if args.date else None
    date_start: str | None = str(args.date_start) if args.date_start else None
    date_end: str | None = str(args.date_end) if args.date_end else None
    chunk_id: str | None = str(args.chunk_id) if args.chunk_id else None
    run_id: str | None = str(args.run_id) if args.run_id else None
    coordinate: bool = bool(args.coordinate)
    split_range: list[str] | None = (
        [str(x) for x in args.split_range] if args.split_range else None  # pyright: ignore[reportAny]
    )

    if split_range is not None:
        s_start, s_end, s_n = split_range
        for cs, ce in chunked_date_ranges(s_start, s_end, int(s_n)):
            print(f"{cs}\t{ce}")
        return

    handler = _ENTITY_HANDLERS[entity_type]

    storage = get_storage_client()
    logger.info("Targeting bucket gs://%s (entity=%s)", bucket_name, handler.data_type)

    migrator = ManifestMigrator(
        bucket_name,
        "instruments-service",
        _build_drop_filter(handler.drop_filter_data_type),
        storage=storage,
        index_blob=INDEX_BLOB,
    )

    worker_mode = chunk_id is not None
    coord_mode = coordinate

    if worker_mode and coord_mode:
        logger.error("--chunk-id and --coordinate are mutually exclusive")
        sys.exit(2)

    if coord_mode and run_id is None:
        logger.error("--coordinate requires --run-id")
        sys.exit(2)

    if worker_mode and run_id is None:
        logger.error("--chunk-id requires --run-id")
        sys.exit(2)

    if coord_mode:
        assert run_id is not None
        migrator.run_coordinator(run_id, dry_run=dry_run)
        return

    if worker_mode:
        assert run_id is not None
        assert chunk_id is not None

        def scan_fn() -> list[dict[str, object]]:
            paths = _list_entity_blob_paths(storage, bucket_name, handler, date_single, date_start, date_end)
            logger.info(
                "Worker %s: found %d %s files (entity=%s)",
                chunk_id,
                len(paths),
                handler.blob_filename,
                handler.data_type,
            )
            if not paths:
                logger.warning("Worker %s: nothing to scan", chunk_id)
                return []
            return _scan_range(
                storage,
                bucket_name,
                paths,
                workers,
                handler=handler,
                migrator=migrator,
                run_id=run_id,
            )

        migrator.run_worker(run_id, chunk_id, scan_fn, dry_run=dry_run)
        return

    paths = _list_entity_blob_paths(storage, bucket_name, handler, date_single)
    logger.info("Found %d %s files (entity=%s)", len(paths), handler.blob_filename, handler.data_type)
    if not paths:
        logger.warning("Nothing to do.")
        return

    def scan_fn() -> list[dict[str, object]]:
        entries_local = _scan_range(
            storage,
            bucket_name,
            paths,
            workers,
            handler=handler,
            migrator=migrator,
            run_id=None,
        )
        logger.info(
            "Scan complete: %d per-(date, league_id) %s rows across %d blobs",
            len(entries_local),
            handler.data_type,
            len(paths),
        )
        if entries_local:
            _print_top_leagues(entries_local, data_type=handler.data_type)
        return entries_local

    try:
        migrator.run_single_vm(scan_fn, dry_run=dry_run)
    except RuntimeError as exc:
        logger.warning("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
