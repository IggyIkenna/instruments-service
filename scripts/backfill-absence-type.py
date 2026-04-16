#!/usr/bin/env python3
"""Backfill absence_type column on existing injury parquets in GCS.

Option A: reads existing parquets, applies the deterministic classifier
from UAC, adds the absence_type column, and rewrites in place.
No API calls needed — the raw reason string is already in the parquets.

Usage:
    python scripts/backfill-absence-type.py --bucket instruments-store-sports-PROJECT_ID
    python scripts/backfill-absence-type.py --bucket instruments-store-sports-PROJECT_ID --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from unified_api_contracts.canonical.domain.sports.injury import (
    AbsenceType,
    classify_absence,
)
from unified_trading_library import get_storage_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill_injury_parquets(bucket_name: str, dry_run: bool = False) -> None:
    """Scan all injury parquets in the bucket and add absence_type column."""
    storage = get_storage_client()
    prefix = "sports_reference/by_date/"
    suffix = "entity=injuries/"

    logger.info("Scanning %s/%s*%s for injury parquets...", bucket_name, prefix, suffix)

    blobs = list(storage.list_blobs(bucket=bucket_name, prefix=prefix))
    injury_blobs = [b for b in blobs if suffix in str(b) and str(b).endswith(".parquet")]

    logger.info("Found %d injury parquet files", len(injury_blobs))

    updated = 0
    skipped = 0
    errors = 0

    for blob_path in injury_blobs:
        blob_str = str(blob_path)
        try:
            uri = f"gs://{bucket_name}/{blob_str}"
            df = pd.read_parquet(uri)

            if df.empty:
                skipped += 1
                continue

            # Check if absence_type already exists
            if "absence_type" in df.columns and df["absence_type"].notna().all():
                skipped += 1
                continue

            # Apply classifier
            if "reason" in df.columns:
                df["absence_type"] = df["reason"].apply(
                    lambda r: classify_absence(str(r) if pd.notna(r) else None).value
                )
            else:
                df["absence_type"] = AbsenceType.OTHER.value

            if dry_run:
                logger.info(
                    "DRY RUN: would update %s (%d rows, types: %s)",
                    blob_str,
                    len(df),
                    df["absence_type"].value_counts().to_dict(),
                )
            else:
                df.to_parquet(uri, index=False)
                logger.info(
                    "Updated %s (%d rows, types: %s)",
                    blob_str,
                    len(df),
                    df["absence_type"].value_counts().to_dict(),
                )

            updated += 1

        except Exception as exc:
            logger.error("Failed to process %s: %s", blob_str, exc)
            errors += 1

    logger.info(
        "Backfill complete: %d updated, %d skipped, %d errors (total %d files)",
        updated,
        skipped,
        errors,
        len(injury_blobs),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill absence_type on injury parquets")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--dry-run", action="store_true", help="Log changes without writing")
    args = parser.parse_args()

    backfill_injury_parquets(args.bucket, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
