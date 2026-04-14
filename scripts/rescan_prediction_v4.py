#!/usr/bin/env python3
"""Re-scan PREDICTION availability index with expanded v4 underlyings.

Reads the existing v4 index, identifies dates where data_type=OTHER might
contain SPX/AAPL/etc, re-reads the GCS instrument parquets, and replaces
OTHER entries with properly sharded entries.

Usage:
    python3 scripts/rescan_prediction_v4.py --dry-run   # preview
    python3 scripts/rescan_prediction_v4.py              # write
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import date as date_type

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _extract_shard(base_asset: str) -> str:
    """Extract data_type shard from base_asset. No allowlist."""
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN":
        return parts[3]
    return "OTHER"


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-scan PREDICTION index with expanded v4 shards")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    from unified_trading_library import ManifestWriter, UnifiedCloudConfig, get_storage_client, read_availability_index

    client = get_storage_client()
    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = f"instruments-store-prediction-{project_id}"
    logger.info("Bucket: %s", bucket)

    # Read existing index
    index = read_availability_index(bucket)
    logger.info("Existing index: %d rows", len(index))

    # Find all dates — we need to re-scan ALL dates to:
    # 1. Split OTHER into proper SPX/DJIA/NDX/etc shards
    # 2. Handle dates that might not have shard entries at all
    all_dates = sorted(index["date"].unique())
    logger.info("Total dates: %d", len(all_dates))

    # Build new index entries
    new_rows: list[dict[str, str | int]] = []
    skipped = 0
    errors = 0

    for i, date_str in enumerate(all_dates):
        date_str = str(date_str)
        gcs_path = f"instrument_availability/by_date/day={date_str}/venue=POLYMARKET/instruments.parquet"
        try:
            raw = client.download_bytes(bucket, gcs_path)
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.debug("Skip %s: %s", date_str, exc)
            errors += 1
            continue

        if "base_asset" not in df.columns:
            skipped += 1
            continue

        # Compute shard counts with expanded underlyings
        shard_counts: dict[str, int] = {}
        for ba in df["base_asset"]:
            shard = _extract_shard(str(ba))
            shard_counts[shard] = shard_counts.get(shard, 0) + 1

        for shard_name, count in shard_counts.items():
            new_rows.append(
                {
                    "date": date_str,
                    "venue": "POLYMARKET",
                    "data_type": shard_name,
                    "instrument_count": count,
                }
            )

        if (i + 1) % 50 == 0:
            logger.info("  Progress: %d/%d dates, %d new rows", i + 1, len(all_dates), len(new_rows))

    logger.info("Scan complete: %d new rows, %d errors, %d skipped", len(new_rows), errors, skipped)

    if args.dry_run:
        # Show shard summary
        shard_summary: dict[str, int] = {}
        for row in new_rows:
            dt = str(row["data_type"])
            shard_summary[dt] = shard_summary.get(dt, 0) + 1
        logger.info("DRY RUN — shard summary (date-count per shard):")
        for k in sorted(shard_summary.keys()):
            logger.info("  %s: %d dates", k, shard_summary[k])
        return

    # Write new index — completely replace existing entries
    logger.info("Writing %d rows to manifest...", len(new_rows))
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        batch_size=100,
    )
    for row in new_rows:
        writer.add(
            processing_date=date_type.fromisoformat(str(row["date"])),
            row_count=int(row["instrument_count"]),
            venue=str(row["venue"]),
            data_type=str(row["data_type"]),
        )
    writer.close()
    logger.info("Done writing manifest")

    # Verify
    updated = read_availability_index(bucket)
    logger.info("Updated index: %d rows (was %d)", len(updated), len(index))
    if "data_type" in updated.columns:
        dt_vals = sorted(updated["data_type"].unique())
        logger.info("data_type values: %s", dt_vals)


if __name__ == "__main__":
    main()
