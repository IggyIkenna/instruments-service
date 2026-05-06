"""Write the corrective-flipped phantom rows to a per-VM shard.

Background: ``flip_phantom_to_attempted_failed.py`` (instruments-service
``2821111``) wrote the corrective flip directly to the canonical
``_index/availability_index.parquet``. The UTL ManifestReader's
``_read_consolidated_if_fresh`` checks canonical's file mtime against a
120s staleness threshold; once canonical aged past that threshold (~2
minutes after my upload), the reader falls back to merging per-VM shards
— which don't have my corrective flip. Result: the FIXTURES backfill
VM read stale per-VM data and skip-cycled instead of re-attempting.

Fix: replicate the corrective-flip rows into a new per-VM shard
``_index/per_vm/flip-phantom-corrective-{ts}.parquet``. The reader's
per-VM merge will then include the corrective rows alongside the legacy
per-VM shards, producing the correct merged state regardless of canonical
mtime.

Idempotent. Reads canonical, filters by
``error_reason='phantom_re_attempt_after_writer_fix_f36651c'``, writes the
filtered subset (~176k rows) to a per-VM shard with a unique timestamp
suffix.

Usage:
  python scripts/write_phantom_reflip_per_vm_shard.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_REFLIP_MARKER = "phantom_re_attempt_after_writer_fix_f36651c"


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _per_vm_shard_path(run_ts: str) -> str:
    return f"_index/per_vm/flip-phantom-corrective-{run_ts}.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default="central-element-323112")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = _bucket_for_project(args.project_id)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    shard_path = _per_vm_shard_path(run_ts)

    storage = get_storage_client(project_id=args.project_id)
    logger.info("Reading canonical gs://%s/%s", bucket, _INDEX_PATH)
    raw = storage.download_bytes(bucket, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Canonical rows: %d", len(df))

    mask = df["error_reason"].astype(str) == _REFLIP_MARKER
    flipped = df.loc[mask].copy()
    logger.info("Rows with reflip marker: %d", len(flipped))
    if flipped.empty:
        logger.info("No reflipped rows found — manifest may have been re-consolidated. Exiting.")
        return 0

    # Per-data-type breakdown
    by_dt = flipped.groupby("data_type").size().sort_values(ascending=False)
    for dt, n in by_dt.items():
        logger.info("    %-20s %d", dt, n)

    if args.dry_run:
        logger.info("[dry-run] Would write %d rows to gs://%s/%s", len(flipped), bucket, shard_path)
        return 0

    out_buf = io.BytesIO()
    flipped.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(bucket, shard_path, out_buf.read())
    logger.info(
        "Wrote %d corrective-flip rows to gs://%s/%s (per-VM shard for reader merge)",
        len(flipped),
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
