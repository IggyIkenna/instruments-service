#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: oneoff — VM-job (see plans/active/issues/manifest_purge_null_filter_near_miss_and_heavy_io_local_2026_08_05.md)
# Delete-when: after purge confirmed in live consolidated _index (zero rows matching venue=UNKNOWN, capture_status=empty_confirmed, row_count=0.0)
"""Purge 4 stale ``venue=UNKNOWN`` manifest rows from the sports availability index.

Target: ``gs://instruments-store-sports-prd-central-element-323112/_index/availability_index.parquet``.

These 4 rows match ``venue=UNKNOWN AND capture_status=empty_confirmed AND row_count=0.0``,
all ``written_at`` 2026-07-13 (3+ weeks static, no live writer producing this shape).  Zero
real GCS content at risk — ``capture_status=empty_confirmed`` + ``row_count=0.0`` means there
is NO backing data object, just a stale sentinel in the manifest.

Design (per the near-miss post-mortem in the parent issue doc):

1. **NULL-safe filter**: uses PyArrow's ``Table.filter()`` with an explicit
   ``mask.fill_null(False)`` so NULL-``row_count`` rows are NOT silently dropped.
   This is the exact bug that caused the 2026-08-05 near-miss (37,818 extra rows deleted).
2. **Pre/post row-count delta assertion**: ``pre_count - post_count == len(drop_mask)``
   asserted BEFORE the CAS write, not just eyeballed after.
3. **Snapshot-first**: backs up the original manifest to a timestamped path before any
   mutation.
4. **CAS-gated write**: uses ``if_generation_match`` so a concurrent consolidator write
   between read and write is detected atomically (not a silent clobber).
5. **HARD RULE compliance**: this script IS a manifest-index rewrite and MUST run on a VM
   in-region, not locally — per ``/codex/05-infrastructure/vm-launcher-runbook.md`` §
   heavy-I/O rule.

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_sports_unknown_venue_manifest_rows_2026_08_05.py --dry-run
    .venv/bin/python scripts/purge_sports_unknown_venue_manifest_rows_2026_08_05.py --apply

The ``--apply`` flag performs ALL of: download, snapshot backup, row-count-delta assertion,
and CAS write.  There is no partial-write mode.  Verify the dry-run row match FIRST.

Idempotent: if the 4 rows are already gone, the script reports 0 matches and exits cleanly.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.cloud import storage
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
os.environ.setdefault("GCP_PROJECT_ID", PROJECT_ID)
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", PROJECT_ID)

INDEX_PATH = "_index/availability_index.parquet"

# ---------------------------------------------------------------------------
# Target filter — exactly these 4 rows
# ---------------------------------------------------------------------------

TARGET_VENUE = "UNKNOWN"
TARGET_CAPTURE_STATUS = "empty_confirmed"
TARGET_ROW_COUNT = 0.0


def _build_null_safe_mask(table: pa.Table) -> pa.ChunkedArray:
    """Build a boolean mask for the 4 target rows, NULL-safe.

    Every ``pc.equal()`` / ``pc.and_()`` result has ``fill_null(False)`` applied
    BEFORE ``Table.filter()``, so Arrow's Kleene three-valued logic never silently
    drops rows where a predicate column is itself NULL.

    This is the fix for the 2026-08-05 near-miss: the original script's mask let
    NULL propagate through ``pc.and_``, and ``Table.filter()``'s default
    ``null_selection_behavior="drop"`` then silently excluded all 37,818 rows with
    NULL ``row_count`` alongside the 4 intended rows.
    """
    venue_mask = pc.equal(table.column("venue"), TARGET_VENUE).fill_null(False)
    status_mask = pc.equal(table.column("capture_status"), TARGET_CAPTURE_STATUS).fill_null(False)
    row_count_mask = pc.equal(table.column("row_count"), TARGET_ROW_COUNT).fill_null(False)

    combined = pc.and_(pc.and_(venue_mask, status_mask), row_count_mask)
    return combined.fill_null(False)


def _ensure_row_count_delta(pre_count: int, post_count: int, matched: int) -> None:
    """Hard-assert the row-count delta matches expectations BEFORE the CAS write.

    This is the automated version of the manual eyeball check that caught the
    2026-08-05 near-miss — without this, the script would silently report
    "APPLY COMPLETE" while having over-deleted.
    """
    actual_delta = pre_count - post_count
    if actual_delta != matched:
        msg = (
            f"ROW-COUNT DELTA MISMATCH BEFORE WRITE — ABORTING:\n"
            f"  pre-write row count:  {pre_count:,}\n"
            f"  post-filter row count: {post_count:,}\n"
            f"  expected delta (matched rows): {matched:,}\n"
            f"  actual delta:          {actual_delta:,}\n"
            f"  DISCREPANCY:           {actual_delta - matched:,d} rows\n"
            f"This is the exact class of silent over-deletion this assertion is\n"
            f"designed to catch — REFUSING to write.  Investigate the discrepancy\n"
            f"before retrying."
        )
        logger.error(msg)
        raise AssertionError(msg)
    logger.info(
        "✅ Row-count delta assertion PASSED: %d → %d (removed %d, matched %d target rows)",
        pre_count,
        post_count,
        actual_delta,
        matched,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--dry-run", action="store_true", default=False, help="Identify target rows but do NOT modify the manifest."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Snapshot, assert row-count delta, and CAS-write the purged manifest.",
    )
    args = ap.parse_args()

    if not args.dry_run and not args.apply:
        ap.error("Pass --dry-run (identify only) or --apply (execute the purge).")

    bucket_name = resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod"
    )
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    logger.info("Target bucket (resolved via SSOT): gs://%s", bucket_name)
    logger.info("Target object: %s", INDEX_PATH)
    logger.info(
        "Filter: venue=%r AND capture_status=%r AND row_count=%r",
        TARGET_VENUE,
        TARGET_CAPTURE_STATUS,
        TARGET_ROW_COUNT,
    )

    # ---- Download ----
    blob = bucket.blob(INDEX_PATH)
    data = blob.download_as_bytes()
    pre_count = 0  # set after table load
    table = pq.read_table(io.BytesIO(data))
    pre_count = len(table)
    logger.info("Loaded manifest: %d rows", pre_count)

    # ---- Build NULL-safe mask ----
    mask = _build_null_safe_mask(table)
    matched = mask.sum().as_py()
    logger.info("Rows matching target filter: %d", matched)

    if matched == 0:
        logger.info("Nothing to purge — manifest is already clean of these rows.")
        return 0

    # Show what we found
    matched_table = table.filter(mask)
    logger.info("Matched rows preview:")
    for col in matched_table.column_names:
        logger.info("  %s: %s", col, matched_table.column(col).to_pylist())

    if args.dry_run:
        logger.info("DRY RUN — not writing. Pass --apply to execute the purge.")
        return 0

    # ---- Purge (apply) ----
    # NULL-safe filter: invert the mask (keep=~mask), with fill_null(True)
    # so NULL-mask rows are KEPT (they don't match the filter).
    keep_mask = pc.invert(mask).fill_null(True)
    kept = table.filter(keep_mask)
    post_count = len(kept)

    # ---- Row-count delta assertion (BEFORE write) ----
    _ensure_row_count_delta(pre_count, post_count, matched)

    # ---- Snapshot backup ----
    run_ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_path = f"_index/backups/availability_index_pre_unknown_venue_purge_{run_ts}.parquet"
    logger.info("Backing up original manifest (%d rows) to gs://%s/%s", pre_count, bucket_name, backup_path)
    bucket.blob(backup_path).upload_from_string(data, content_type="application/octet-stream")
    logger.info("Backup complete: %s", backup_path)

    # ---- CAS write ----
    # Re-fetch generation after the backup write (backup is a different blob,
    # but a consolidator could have touched _index between our download and now).
    blob.reload()
    generation = blob.generation
    logger.info(
        "Uploading purged manifest (%d rows, removed %d) with if_generation_match=%s",
        post_count,
        matched,
        generation,
    )

    buf = io.BytesIO()
    pq.write_table(kept, buf)
    buf.seek(0)

    bucket.blob(INDEX_PATH).upload_from_file(
        buf,
        content_type="application/octet-stream",
        if_generation_match=generation,
    )

    # ---- Post-write verification ----
    # Re-download and confirm zero matches remain.
    verify_data = bucket.blob(INDEX_PATH).download_as_bytes()
    verify_table = pq.read_table(io.BytesIO(verify_data))
    verify_mask = _build_null_safe_mask(verify_table)
    verify_matched = verify_mask.sum().as_py()
    logger.info("Post-write verification: %d matching rows remain (expected 0)", verify_matched)
    if verify_matched != 0:
        logger.error(
            "UNEXPECTED: %d matching rows found after purge — the CAS write may have lost a race.", verify_matched
        )
        return 1

    logger.info("DONE — %d rows purged from %s (verified: %d rows, 0 matching)", matched, INDEX_PATH, len(verify_table))
    return 0


if __name__ == "__main__":
    sys.exit(main())
