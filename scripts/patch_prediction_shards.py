#!/usr/bin/env python3
"""Patch PREDICTION availability index with per-underlying shard entries.

Reads existing instrument parquets from GCS for each date, extracts the
underlying shard from base_asset (BTC, ETH, SOL, FOOTBALL, OTHER, etc.),
and adds shard-level manifest entries (venue=POLYMARKET:BTC, etc.) to the
availability index. The overall POLYMARKET entry is preserved.

Usage (on a VM or machine with GCS credentials):
    python3 scripts/patch_prediction_shards.py --dry-run   # preview
    python3 scripts/patch_prediction_shards.py              # write
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _extract_shard(base_asset: str) -> str:
    """Parse base_asset to get the shard venue name. No allowlist."""
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "POLYMARKET:FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN":
        return f"POLYMARKET:{parts[3]}"
    return "POLYMARKET:OTHER"


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch PREDICTION manifest with shard entries")
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

    # Find all dates with POLYMARKET venue (the ones we need to enrich)
    polymarket_mask = index["venue"] == "POLYMARKET"
    polymarket_dates = sorted(index.loc[polymarket_mask, "date"].unique())
    logger.info("POLYMARKET dates to process: %d", len(polymarket_dates))

    # Check which dates already have shard entries
    shard_mask = index["venue"].str.contains(":", na=False)
    existing_shard_dates = set(index.loc[shard_mask, "date"].unique())
    dates_to_process = [d for d in polymarket_dates if str(d) not in existing_shard_dates]
    logger.info(
        "Dates needing shard entries: %d (skipping %d already done)",
        len(dates_to_process),
        len(polymarket_dates) - len(dates_to_process),
    )

    if not dates_to_process:
        logger.info("All dates already have shard entries — nothing to do")
        return

    # Process each date — read the instrument parquet, compute shards
    writer = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket, batch_size=50)
    total_shards = 0

    for i, date_str in enumerate(dates_to_process):
        date_str = str(date_str)
        gcs_path = f"instrument_availability/by_date/day={date_str}/venue=POLYMARKET/instruments.parquet"
        try:
            raw = client.download_bytes(bucket, gcs_path)
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("Skip %s: %s", date_str, exc)
            continue

        if "base_asset" not in df.columns:
            logger.warning("Skip %s: no base_asset column", date_str)
            continue

        # Compute shard counts
        shard_counts: dict[str, int] = {}
        for ba in df["base_asset"]:
            shard = _extract_shard(str(ba))
            shard_counts[shard] = shard_counts.get(shard, 0) + 1

        if args.dry_run:
            if i < 3 or i == len(dates_to_process) - 1:
                logger.info("  [DRY RUN] %s: %s", date_str, shard_counts)
        else:
            from datetime import date as date_type

            for shard_name, count in shard_counts.items():
                # v4: venue=POLYMARKET, data_type=underlying (e.g. BTC)
                parts = shard_name.split(":", 1)
                _venue = parts[0]
                dt_val = parts[1] if len(parts) > 1 else ""  # BTC, FOOTBALL, OTHER
                writer.add(
                    processing_date=date_type.fromisoformat(date_str),
                    row_count=count,
                    venue=shard_name,  # Keep full name for backward compat
                    data_type=dt_val,
                )
            total_shards += len(shard_counts)

        if (i + 1) % 50 == 0:
            logger.info("  Progress: %d/%d dates", i + 1, len(dates_to_process))

    if args.dry_run:
        logger.info("DRY RUN complete — would add shard entries for %d dates", len(dates_to_process))
    else:
        writer.close()
        logger.info("Wrote %d shard entries across %d dates", total_shards, len(dates_to_process))

    # Verify
    updated = read_availability_index(bucket)
    logger.info("Updated index: %d rows (was %d)", len(updated), len(index))
    shard_venues = sorted(updated.loc[updated["venue"].str.contains(":", na=False), "venue"].unique())
    logger.info("Shard venues: %s", shard_venues)


if __name__ == "__main__":
    main()
