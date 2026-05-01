#!/usr/bin/env python3
"""Rebuild CeFi manifest from actual GCS blobs.

Scans instrument_availability/by_date/ partitions and adds any (date, venue)
shards missing from the availability_index.parquet manifest.

Usage (on a VM with GCS credentials):
    python3 scripts/rebuild_cefi_manifest.py --dry-run   # preview
    python3 scripts/rebuild_cefi_manifest.py              # write

Also supports other categories:
    python3 scripts/rebuild_cefi_manifest.py --asset-group DEFI
    python3 scripts/rebuild_cefi_manifest.py --asset-group TRADFI
    python3 scripts/rebuild_cefi_manifest.py --asset-group SPORTS
"""

from __future__ import annotations

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild manifest from GCS blobs")
    parser.add_argument("--asset-group", default="CEFI", help="Category (CEFI, DEFI, TRADFI, SPORTS)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--prefix", default="instrument_availability/by_date", help="GCS data prefix")
    args = parser.parse_args()

    from unified_trading_library import get_bucket_name, read_availability_index, rebuild_manifest

    bucket = get_bucket_name("instruments", args.category)
    logger.info("Bucket: %s", bucket)

    # Show current state
    existing = read_availability_index(bucket)
    logger.info(
        "Current manifest: %d entries, %d unique dates",
        len(existing),
        existing["date"].nunique() if not existing.empty else 0,
    )

    # Rebuild
    logger.info("Scanning GCS blobs under %s/%s/ ...", bucket, args.prefix)
    df = rebuild_manifest(
        bucket=bucket,
        service_name="instruments-service",
        prefix=args.prefix,
        dry_run=args.dry_run,
    )

    new_count = len(df) - len(existing)
    logger.info(
        "Result: %d total entries (%+d new), %d unique dates, %d venues",
        len(df),
        new_count,
        df["date"].nunique() if not df.empty else 0,
        df["venue"].nunique() if not df.empty else 0,
    )

    if args.dry_run and new_count > 0:
        logger.info("Re-run without --dry-run to write the updated manifest")


if __name__ == "__main__":
    main()
