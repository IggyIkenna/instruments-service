#!/usr/bin/env python3
"""Phase 3 of data_status_coverage_gaps_and_prediction_manifest_fix_2026_05_22.

Combines 3.2 + 3.3:
  3.2 — Snapshot prediction manifest, then purge all 3940 legacy rows
        (data_type=BTC/ETH/SOL/etc., blank underlying, capture_status=None/captured).
  3.3 — Delete all legacy GCS parquets under instrument_availability/by_date/day=*/
        (these use wrong partition key; canonical paths use canonical_question_group=* prefix).

The new orchestrator already writes canonical_question_group paths; this script
clears out all legacy cruft so Phase 3.4 (re-backfill) starts from a clean slate.

Usage:
    .venv/bin/python scripts/fix_prediction_manifest_and_gcs_2026_05_22.py --dry-run
    .venv/bin/python scripts/fix_prediction_manifest_and_gcs_2026_05_22.py --apply
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET_NAME = f"instruments-store-prediction-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"
LEGACY_PREFIX = "instrument_availability/by_date/day="
CANONICAL_PREFIX = "instrument_availability/by_date/canonical_question_group="


def _list_legacy_blobs(client: storage.Client) -> list[str]:
    """List all GCS objects under the legacy day= partition prefix."""
    blobs = client.list_blobs(BUCKET_NAME, prefix=LEGACY_PREFIX)
    names = [b.name for b in blobs]
    logger.info("Found %d legacy objects under %s", len(names), LEGACY_PREFIX)
    return names


def _delete_blob(bucket: storage.Bucket, name: str) -> str:
    bucket.blob(name).delete()
    return name


def step_32_purge_manifest(client: storage.Client, dry_run: bool) -> int:
    """Download index, snapshot it, remove all legacy prediction rows, re-upload."""
    bucket = client.bucket(BUCKET_NAME)

    # Download current index
    data = bucket.blob(INDEX_PATH).download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("Loaded prediction index: %d rows", len(df))

    # Show current state
    if "data_type" in df.columns:
        logger.info("data_type distribution before purge:\n%s", df["data_type"].value_counts(dropna=False).to_string())
    if "capture_status" in df.columns:
        logger.info("capture_status before purge:\n%s", df["capture_status"].value_counts(dropna=False).to_string())

    # We purge ALL rows — the entire prediction manifest needs rebuild with canonical format
    # (the old rows have data_type=BTC/ETH/SOL/etc. and blank underlying)
    legacy_mask = df["venue"].isin(["POLYMARKET", "KALSHI"]) if "venue" in df.columns else pd.Series([True] * len(df))
    # Safety: only purge rows with non-canonical data_type (anything that isn't prediction_canonical_question_group
    # or prediction_market_lifecycle)
    canonical_data_types = {"prediction_canonical_question_group", "prediction_market_lifecycle"}
    if "data_type" in df.columns:
        legacy_mask = legacy_mask & ~df["data_type"].isin(canonical_data_types)

    purge_count = legacy_mask.sum()
    logger.info("Rows to purge: %d (legacy format with wrong data_type)", purge_count)

    if purge_count == 0:
        logger.info("No legacy rows found — manifest already clean.")
        return 0

    if dry_run:
        logger.info("DRY RUN — would purge %d rows", purge_count)
        return purge_count

    # Snapshot original
    run_ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snapshot_path = f"_index/snapshots/pre_prediction_fix_{run_ts}.parquet"
    logger.info("Snapshotting original to %s", snapshot_path)
    bucket.blob(snapshot_path).upload_from_string(data, content_type="application/octet-stream")

    cleaned = df[~legacy_mask].reset_index(drop=True)
    logger.info("Cleaned index: %d rows (purged %d)", len(cleaned), purge_count)

    buf = io.BytesIO()
    cleaned.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    bucket.blob(INDEX_PATH).upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Uploaded cleaned prediction index (%d rows)", len(cleaned))
    return purge_count


def step_33_delete_legacy_gcs(client: storage.Client, dry_run: bool) -> int:
    """Delete all legacy day=* GCS parquets using parallel workers (32 threads)."""
    names = _list_legacy_blobs(client)
    if not names:
        logger.info("No legacy GCS objects found — already clean.")
        return 0

    if dry_run:
        logger.info("DRY RUN — would delete %d legacy GCS objects", len(names))
        for n in names[:10]:
            logger.info("  Would delete: %s", n)
        if len(names) > 10:
            logger.info("  ... and %d more", len(names) - 10)
        return len(names)

    bucket = client.bucket(BUCKET_NAME)
    deleted = 0
    errors = 0
    logger.info("Deleting %d legacy GCS objects (32 workers)...", len(names))
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        futures = {pool.submit(_delete_blob, bucket, n): n for n in names}
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
                deleted += 1
                if deleted % 100 == 0:
                    logger.info("  Deleted %d/%d...", deleted, len(names))
            except Exception as exc:
                logger.error("  Failed to delete %s: %s", name, exc)
                errors += 1

    logger.info("Deleted %d objects, %d errors", deleted, errors)
    return deleted


def verify(client: storage.Client) -> None:
    """Verify the bucket state post-cleanup."""
    logger.info("\n=== Post-cleanup verification ===")

    # Check manifest
    data = client.bucket(BUCKET_NAME).blob(INDEX_PATH).download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("Prediction manifest: %d rows", len(df))
    if len(df) > 0 and "data_type" in df.columns:
        logger.info("data_type distribution:\n%s", df["data_type"].value_counts(dropna=False).to_string())

    # Check legacy GCS paths still exist
    legacy_remaining = sum(1 for _ in client.list_blobs(BUCKET_NAME, prefix=LEGACY_PREFIX, max_results=5))
    logger.info("Legacy day= GCS objects remaining (first 5): %d", legacy_remaining)

    # Check canonical paths exist
    canonical_remaining = sum(1 for _ in client.list_blobs(BUCKET_NAME, prefix=CANONICAL_PREFIX, max_results=5))
    logger.info("Canonical canonical_question_group= GCS objects (first 5): %d", canonical_remaining)


def main(dry_run: bool) -> None:
    client = storage.Client(project=PROJECT_ID)

    logger.info("=== Step 3.2: Purge legacy prediction manifest rows ===")
    purged = step_32_purge_manifest(client, dry_run)

    logger.info("\n=== Step 3.3: Delete legacy GCS parquets ===")
    deleted = step_33_delete_legacy_gcs(client, dry_run)

    if not dry_run:
        logger.info("\n=== Verification ===")
        verify(client)

    logger.info("\nSummary: purged=%d manifest rows, deleted=%d GCS objects (dry_run=%s)", purged, deleted, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--apply", action="store_true", default=False)
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("Pass --dry-run or --apply")

    main(dry_run=args.dry_run)
