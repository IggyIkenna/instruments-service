#!/usr/bin/env python3
"""Reconcile phantom-captured manifest rows by flipping them to attempted_failed.

A "phantom" row claims ``capture_status=captured`` but no parquet exists at
any candidate GCS path (per-league subpartition + bare). These came from
the legacy ``rescan_sports_manifest.py`` which wrote rows with
``league_id=""`` for entities that actually use the per-league
subpartition layout — leaving stale claims that don't match reality.

The fix: probe each captured row against the canonical
``candidate_parquet_paths`` from UAC (SSOT). If no candidate exists, flip
to ``attempted_failed`` so the orchestrator's ``_should_skip_shard``
treats it as retry-eligible on the next backfill VM cycle.

Idempotent: a row already at ``attempted_failed`` is left alone. A row at
``captured`` whose parquet truly exists is left alone.

Usage::

    cd instruments-service
    .venv/bin/python scripts/reconcile_phantom_manifest_rows.py --dry-run
    .venv/bin/python scripts/reconcile_phantom_manifest_rows.py            # full
    .venv/bin/python scripts/reconcile_phantom_manifest_rows.py --data-types ODDS,PREDICTIONS

The script reads/writes the canonical
``gs://instruments-store-sports-{project_id}/_index/availability_index.parquet``.
Per-VM shards are NOT touched (they're append-only).
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    SPORTS_DATA_TYPE_TO_FOLDER,
    candidate_parquet_paths,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"


def _row_has_parquet(
    bucket: storage.Bucket,
    data_type: str,
    day: str,
    league_id: str,
) -> bool:
    """Probe candidate paths from UAC SSOT; return True if any exists.

    Slow path — does individual exists() calls. Use ``_build_existing_set``
    + in-memory check for bulk audits.
    """
    paths = candidate_parquet_paths(data_type, day, league_id)
    for p in paths:
        try:
            if bucket.blob(p).exists():
                return True
        except Exception:
            continue
    return False


def _list_day_parquets(bucket: storage.Bucket, day: str) -> set[str]:
    """List every parquet under ``sports_reference/by_date/day={day}/``.

    Returns the set of blob names (relative to bucket root). One GCS list
    call per day — much faster than per-row exists() probes.
    """
    prefix = f"sports_reference/by_date/day={day}/"
    return {b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")}


def _build_existing_set(
    bucket: storage.Bucket,
    days: list[str],
    workers: int,
    flat_paths: list[str],
) -> set[str]:
    """Bulk-list all parquets across days and flat paths into a single set.

    Returns ``{blob_name, ...}`` covering everything currently in GCS under
    ``sports_reference/``. Used to check manifest claims against reality
    in O(1) per row.
    """
    out: set[str] = set()
    # Flat paths (singletons like VENUES)
    for p in flat_paths:
        try:
            if bucket.blob(p).exists():
                out.add(p)
        except Exception:
            pass

    t0 = time.monotonic()

    def _list_day(day: str) -> set[str]:
        try:
            return _list_day_parquets(bucket, day)
        except Exception:
            return set()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_list_day, d): d for d in days}
        n = 0
        for fut in as_completed(futures):
            out.update(fut.result())
            n += 1
            if n % 200 == 0:
                logger.info(
                    "list pass: %d/%d days in %.1fs (%d blobs cached)",
                    n,
                    len(days),
                    time.monotonic() - t0,
                    len(out),
                )
    logger.info(
        "list pass complete: %d days in %.1fs (%d total blobs cached)",
        len(days),
        time.monotonic() - t0,
        len(out),
    )
    return out


def _row_has_parquet_in_set(
    data_type: str,
    day: str,
    league_id: str,
    existing: set[str],
) -> bool:
    """O(1) membership check using pre-built set from ``_build_existing_set``."""
    paths = candidate_parquet_paths(data_type, day, league_id)
    return any(p in existing for p in paths)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--data-types",
        type=str,
        default="",
        help="Comma-separated subset (default = all SPORTS data_types in UAC SSOT).",
    )
    parser.add_argument("--workers", type=int, default=64)
    args = parser.parse_args(argv)

    requested = {dt.strip().upper() for dt in args.data_types.split(",") if dt.strip()}
    target_dts = requested or set(SPORTS_DATA_TYPE_TO_FOLDER.keys())

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)

    logger.info("Reading canonical manifest…")
    raw = bucket.blob(INDEX_BLOB).download_as_bytes()
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d rows / %d cols", len(df), len(df.columns))

    # Filter to candidate rows: instruments-service captured for target data_types.
    candidates = df[
        (df["service_name"] == "instruments-service")
        & (df["capture_status"] == "captured")
        & (df["data_type"].isin(target_dts))
    ]
    logger.info("Candidate rows to probe: %d", len(candidates))

    # Bulk-list all parquets under sports_reference/ once, then check rows
    # against the in-memory set. Dramatically faster than per-row exists().
    distinct_days = sorted(candidates["date"].dropna().astype(str).unique())
    flat_paths = ["sports_reference/venues/venues.parquet"]
    existing = _build_existing_set(bucket, distinct_days, args.workers, flat_paths)

    indices_to_flip: list[int] = []
    by_dt: Counter[str] = Counter()
    by_dt_kept: Counter[str] = Counter()

    t0 = time.monotonic()
    for idx, row in candidates.iterrows():
        ok = _row_has_parquet_in_set(
            data_type=str(row["data_type"]),
            day=str(row["date"]),
            league_id=str(row.get("league_id", "") or ""),
            existing=existing,
        )
        dt = str(row["data_type"])
        if ok:
            by_dt_kept[dt] += 1
        else:
            indices_to_flip.append(idx)
            by_dt[dt] += 1
    logger.info("checked all %d rows in %.1fs", len(candidates), time.monotonic() - t0)

    logger.info("=" * 60)
    logger.info("Reconciliation summary (rows with no parquet at any candidate path):")
    logger.info("%-25s %10s %10s %8s", "data_type", "phantom", "real", "phantom%")
    for dt in sorted(target_dts):
        p = by_dt[dt]
        r = by_dt_kept[dt]
        total = p + r
        if total == 0:
            continue
        pct = 100 * p / total
        logger.info("  %-25s %10d %10d %7.1f%%", dt, p, r, pct)
    logger.info("=" * 60)
    logger.info("Total to flip: %d", len(indices_to_flip))

    if args.dry_run:
        logger.info("DRY RUN — not writing")
        return 0

    if not indices_to_flip:
        logger.info("Nothing to flip; manifest is reality-aligned.")
        return 0

    # Flip in-memory.
    now_iso = datetime.now(UTC).isoformat()
    df.loc[indices_to_flip, "capture_status"] = "attempted_failed"
    df.loc[indices_to_flip, "available"] = False
    df.loc[indices_to_flip, "error_reason"] = "phantom_captured_no_parquet_at_canonical_path"
    df.loc[indices_to_flip, "attempted_at"] = now_iso

    # Write back to canonical (overwrite). Per-VM shards untouched.
    logger.info("Writing %d rows back to canonical (with %d flipped)…", len(df), len(indices_to_flip))
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    bucket.blob(INDEX_BLOB).upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Wrote canonical manifest: %d rows", len(df))
    logger.info("VMs will pick up flipped rows on next _should_skip_shard check.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
