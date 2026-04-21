#!/usr/bin/env python3
"""Rescan GCS sports FIXTURES parquets and emit per-(date, canonical_league_id) manifest rows.

SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md`` (Phase 5 fix).

**Why a new script?** ``rescan_sports_manifest.py`` (the existing rescan) dedups
by entity and writes ``league_id=""`` — it can't produce per-league drilldown
for the FIXTURES entity. ``migrate_sports_per_league.py`` physically split
FIXTURE_EVENTS / FIXTURE_LINEUPS / FIXTURE_STATS / PLAYER_STATS / INJURIES
into per-league parquets, but FIXTURES itself remains a single per-day file
containing all leagues (by API-Football numeric id). Consequence: the
manifest's per-league FIXTURES coverage was ~0.2% honest (EPL=5 rows across
all seasons vs ~2300 expected; USL_CHAMPIONSHIP=639 is correct because USL
IDs hit the canonical mapping reliably).

This script:

1. Walks every ``sports_reference/by_date/day=.../entity=fixtures/fixtures.parquet``
   on GCS.
2. For each blob, reads the parquet, joins ``af_league_id`` →
   canonical ``league_id`` via ``get_league_by_api_football_id()`` (UAC
   ``unified_api_contracts.sports.league_data``), groups by canonical
   league, and counts fixtures.
3. Emits one v5 manifest row per (date, league_id) with:
     - ``data_type="FIXTURES"``, ``venue=""`` (not a venue — per SSOT)
     - ``league_id=<canonical>``
     - ``instrument_count=<# of fixtures that league had that day>``
     - ``capture_status="captured"`` (rows exist on GCS)
     - ``schema_version=5``
4. Preserves non-FIXTURES rows (other sports entities + other services).

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

    bash deployment-service/scripts/vm/create-code-tarballs.sh --category SPORTS

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
import contextlib
import io
import logging
import sys
import tempfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import get_league_by_api_football_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_NOW = datetime.now(UTC).isoformat()

BUCKET_NAME = "instruments-store-sports-central-element-323112"
FIXTURES_PREFIX = "sports_reference/by_date/"
INDEX_BLOB = "_index/availability_index.parquet"
PARTIAL_PREFIX = "_index/partial"


def _list_fixtures_blobs(
    bucket: storage.Bucket,
    date_str: str | None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[storage.Blob]:
    """List every day=<D>/entity=fixtures/fixtures.parquet blob.

    Optionally scoped to a single date (``date_str``) or a closed date range
    (``date_start`` .. ``date_end`` inclusive). When a range is provided, the
    prefix-scan still reads the whole ``sports_reference/by_date/`` tree and
    filters in Python — GCS list is O(# of objects) either way.
    """
    prefix = f"{FIXTURES_PREFIX}day={date_str}/entity=fixtures/" if date_str else FIXTURES_PREFIX

    start_d = date.fromisoformat(date_start) if date_start else None
    end_d = date.fromisoformat(date_end) if date_end else None

    matches: list[storage.Blob] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith("/entity=fixtures/fixtures.parquet"):
            continue
        if start_d is not None or end_d is not None:
            parsed = _parse_date(blob.name)
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
        matches.append(blob)
    return matches


def _parse_date(blob_name: str) -> str | None:
    for p in blob_name.split("/"):
        if p.startswith("day="):
            return p[4:]
    return None


def _scan_blob(bucket: storage.Bucket, blob: storage.Blob) -> list[dict[str, object]]:
    """Read one fixtures.parquet and emit per-canonical-league manifest rows."""
    date_str = _parse_date(blob.name)
    if date_str is None:
        return []

    try:
        raw = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("Failed to read %s: %s", blob.name, exc)
        return []

    if df.empty or "af_league_id" not in df.columns:
        return []

    per_league: dict[str, int] = {}
    unmapped = 0
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
        entries.append(
            {
                "date": date_str,
                "venue": "",
                "data_type": "FIXTURES",
                "service_name": "instruments-service",
                "instrument_count": int(count),
                "written_at": _NOW,
                "schema_version": 5,
                "timeframe": "",
                "league_id": lid,
                "chain": "",
                "instrument_type": "",
                "capture_status": "captured",
                "error_reason": "",
                "attempted_at": _NOW,
                "expected": True,
                "available": True,
            }
        )

    if unmapped:
        logger.debug("%s: %d fixtures unmapped (af_league_id not in LEAGUE_REGISTRY)", date_str, unmapped)

    return entries


def _scan_range(
    bucket: storage.Bucket,
    blobs: list[storage.Blob],
    workers: int,
) -> list[dict[str, object]]:
    """Parallel-scan blobs. Shared helper for single-VM and worker modes."""
    all_entries: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_scan_blob, bucket, b): b for b in blobs}
        for done, future in enumerate(as_completed(futures), 1):
            try:
                all_entries.extend(future.result())
            except Exception as exc:
                logger.warning("scan failed: %s", exc)
            if done % 200 == 0:
                logger.info("Progress: %d / %d", done, len(blobs))
    return all_entries


def _upload_parquet(bucket: storage.Bucket, blob_name: str, rows: Iterable[dict[str, object]]) -> int:
    """Write rows to a GCS parquet via a tempfile (avoids Bandit B108)."""
    df = pd.DataFrame(list(rows))
    tmp_dir = Path(tempfile.gettempdir())
    tmp_path = tmp_dir / f"rescan_{blob_name.replace('/', '_')}.parquet"
    df.to_parquet(tmp_path, index=False)
    bucket.blob(blob_name).upload_from_filename(str(tmp_path))
    with contextlib.suppress(OSError):
        tmp_path.unlink()
    return len(df)


def _merge_into_canonical(bucket: storage.Bucket, new_entries: list[dict[str, object]]) -> int:
    """Read canonical, drop the FIXTURES-per-league rows we're replacing, union with new_entries, write back.

    Returns the total row count of the written manifest.
    """
    index_blob = bucket.blob(INDEX_BLOB)
    existing_entries: list[dict[str, object]] = []
    if index_blob.exists():
        logger.info("Reading existing manifest ...")
        existing_df = pd.read_parquet(io.BytesIO(index_blob.download_as_bytes()))
        # Keep everything except FIXTURES rows from instruments-service with non-empty league_id —
        # those are the rows we're replacing. Preserve legacy empty-league_id FIXTURES rows
        # (rescan_sports_manifest bootstrap output) so we don't lose sparse-entity entries.
        mask_keep = ~(
            (existing_df.get("data_type") == "FIXTURES")
            & (existing_df.get("service_name") == "instruments-service")
            & (existing_df.get("league_id", pd.Series(dtype=str)).astype(str) != "")
        )
        existing_entries = existing_df[mask_keep].to_dict("records")
        logger.info(
            "Preserving %d existing rows; replacing %d FIXTURES per-league rows",
            len(existing_entries),
            len(existing_df) - len(existing_entries),
        )

    combined = pd.DataFrame(new_entries + existing_entries)
    count = _upload_parquet(bucket, INDEX_BLOB, combined.to_dict("records"))
    logger.info("Wrote %d rows to gs://%s/%s", count, bucket.name, INDEX_BLOB)
    return count


def _worker_partial_blob(run_id: str, chunk_id: str) -> str:
    safe_chunk = chunk_id.replace("/", "-")
    return f"{PARTIAL_PREFIX}/{run_id}/{safe_chunk}.parquet"


def _run_worker(
    bucket: storage.Bucket,
    run_id: str,
    chunk_id: str,
    date_start: str | None,
    date_end: str | None,
    date_single: str | None,
    workers: int,
    dry_run: bool,
) -> None:
    """Worker mode: scan a disjoint date range, write to _index/partial/<run-id>/<chunk-id>.parquet."""
    logger.info(
        "Worker %s | run-id=%s | date_start=%s date_end=%s date=%s",
        chunk_id,
        run_id,
        date_start,
        date_end,
        date_single,
    )
    blobs = _list_fixtures_blobs(bucket, date_single, date_start, date_end)
    logger.info("Worker %s: found %d fixtures.parquet files", chunk_id, len(blobs))
    if not blobs:
        logger.warning("Worker %s: nothing to scan", chunk_id)
        return

    entries = _scan_range(bucket, blobs, workers)
    logger.info(
        "Worker %s: produced %d per-(date, league_id) rows across %d blobs",
        chunk_id,
        len(entries),
        len(blobs),
    )
    if dry_run:
        logger.info("Worker %s: DRY RUN — not writing partial", chunk_id)
        return

    partial_blob = _worker_partial_blob(run_id, chunk_id)
    _upload_parquet(bucket, partial_blob, entries)
    logger.info("Worker %s: wrote %d rows to gs://%s/%s", chunk_id, len(entries), bucket.name, partial_blob)


def _run_coordinator(bucket: storage.Bucket, run_id: str, dry_run: bool) -> None:
    """Coordinator mode: read canonical, glob _index/partial/<run-id>/*.parquet, merge, write canonical, delete partials."""
    logger.info("Coordinator | run-id=%s", run_id)
    partial_prefix = f"{PARTIAL_PREFIX}/{run_id}/"
    partials = list(bucket.list_blobs(prefix=partial_prefix))
    if not partials:
        logger.error("No partials found under gs://%s/%s — aborting", bucket.name, partial_prefix)
        sys.exit(1)
    logger.info("Found %d partial shards under gs://%s/%s", len(partials), bucket.name, partial_prefix)

    all_entries: list[dict[str, object]] = []
    for blob in partials:
        if not blob.name.endswith(".parquet"):
            continue
        df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        all_entries.extend(df.to_dict("records"))
        logger.info("  loaded %d rows from %s", len(df), blob.name)

    logger.info("Coordinator: %d partial rows total", len(all_entries))
    if not all_entries:
        logger.warning("No entries in partials; aborting coordinator merge")
        sys.exit(1)

    if dry_run:
        logger.info("Coordinator: DRY RUN — not writing canonical or deleting partials")
        return

    _merge_into_canonical(bucket, all_entries)

    logger.info("Coordinator: deleting %d partial shards ...", len(partials))
    for blob in partials:
        try:
            blob.delete()
        except Exception as exc:
            logger.warning("failed to delete %s: %s", blob.name, exc)
    logger.info("Coordinator: merge complete")


def _run_single_vm(
    bucket: storage.Bucket,
    date_single: str | None,
    workers: int,
    dry_run: bool,
) -> None:
    """Single-VM mode: scan everything, write canonical directly (historical behaviour)."""
    blobs = _list_fixtures_blobs(bucket, date_single)
    logger.info("Found %d fixtures.parquet files", len(blobs))
    if not blobs:
        logger.warning("Nothing to do.")
        return

    entries = _scan_range(bucket, blobs, workers)
    logger.info(
        "Scan complete: %d per-(date, league_id) FIXTURES rows across %d blobs",
        len(entries),
        len(blobs),
    )
    if not entries:
        logger.warning("No entries produced. Aborting.")
        sys.exit(1)

    df = pd.DataFrame(entries)
    logger.info("Top-10 leagues by fixture count (Phase 5 target >= 200 per league/season):")
    top = df.groupby("league_id")["instrument_count"].sum().sort_values(ascending=False).head(10)
    print(top.to_string())

    if dry_run:
        logger.info("DRY RUN — not writing manifest.")
        return

    _merge_into_canonical(bucket, entries)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescan sports FIXTURES with canonical league mapping")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing index/partial")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (thread pool for blob scan)")
    parser.add_argument("--bucket", type=str, default=BUCKET_NAME, help="GCS bucket")

    # Date selection
    parser.add_argument("--date", type=str, help="Scan a single date (YYYY-MM-DD)")
    parser.add_argument("--date-start", type=str, help="Inclusive date lower bound (YYYY-MM-DD, worker mode)")
    parser.add_argument("--date-end", type=str, help="Inclusive date upper bound (YYYY-MM-DD, worker mode)")

    # Chunk-safe mode flags (see module docstring)
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

    # Narrow argparse.Namespace -> typed locals (basedpyright strict mode)
    dry_run: bool = bool(args.dry_run)
    workers: int = int(args.workers)
    bucket_name: str = str(args.bucket)
    date_single: str | None = str(args.date) if args.date else None
    date_start: str | None = str(args.date_start) if args.date_start else None
    date_end: str | None = str(args.date_end) if args.date_end else None
    chunk_id: str | None = str(args.chunk_id) if args.chunk_id else None
    run_id: str | None = str(args.run_id) if args.run_id else None
    coordinate: bool = bool(args.coordinate)
    split_range: list[str] | None = (
        [str(x) for x in args.split_range] if args.split_range else None  # pyright: ignore[reportAny]
    )

    # Utility mode: just print chunk boundaries (no GCS client required)
    if split_range is not None:
        s_start, s_end, s_n = split_range
        for cs, ce in _split_date_range(s_start, s_end, int(s_n)):
            print(f"{cs}\t{ce}")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logger.info("Targeting bucket gs://%s", bucket_name)

    # Validate mode selection
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
        _run_coordinator(bucket, run_id, dry_run)
        return

    if worker_mode:
        assert run_id is not None
        assert chunk_id is not None
        _run_worker(
            bucket,
            run_id=run_id,
            chunk_id=chunk_id,
            date_start=date_start,
            date_end=date_end,
            date_single=date_single,
            workers=workers,
            dry_run=dry_run,
        )
        return

    # Default: single-VM mode
    _run_single_vm(bucket, date_single, workers, dry_run)


def _split_date_range(start: str, end: str, chunks: int) -> list[tuple[str, str]]:
    """Split a closed date range into N roughly-equal chunks.

    Exposed as a helper so the launcher / tests / future migration scripts can
    reuse the same slicing logic.
    """
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end)
    if d1 < d0:
        raise ValueError("date-end must be >= date-start")
    total_days = (d1 - d0).days + 1
    if chunks <= 0:
        raise ValueError("chunks must be >= 1")
    if chunks > total_days:
        chunks = total_days
    size = total_days // chunks
    remainder = total_days % chunks
    out: list[tuple[str, str]] = []
    cursor = d0
    for i in range(chunks):
        span = size + (1 if i < remainder else 0)
        chunk_start = cursor
        chunk_end = cursor + timedelta(days=span - 1)
        out.append((chunk_start.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)
    return out


if __name__ == "__main__":
    main()
