#!/usr/bin/env python3
"""Split existing PREDICTION instruments into per-market partitions.

Reads instrument_availability/by_date/day={date}/venue=POLYMARKET/instruments.parquet,
groups by underlying market, writes each to market={MARKET}/instruments.parquet.

Usage:
    python3 scripts/split_prediction_by_market.py --dry-run
    python3 scripts/split_prediction_by_market.py
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _extract_market(base_asset: str) -> str:
    """Extract market from base_asset. No allowlist."""
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN":
        return parts[3]
    return "OTHER"


def main() -> None:
    parser = argparse.ArgumentParser(description="Split prediction instruments by market")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from unified_trading_library import UnifiedCloudConfig, get_storage_client, read_availability_index

    client = get_storage_client()
    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = f"instruments-store-prediction-{project_id}"
    logger.info("Bucket: %s", bucket)

    idx = read_availability_index(bucket)
    all_dates = sorted(idx["date"].unique())
    logger.info("Total dates: %d", len(all_dates))

    total_written = 0
    errors = 0

    for i, date_str in enumerate(all_dates):
        date_str = str(date_str)
        src_path = f"instrument_availability/by_date/day={date_str}/venue=POLYMARKET/instruments.parquet"
        try:
            raw = client.download_bytes(bucket, src_path)
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception:
            errors += 1
            continue

        if df.empty or "base_asset" not in df.columns:
            continue

        df["_market"] = df["base_asset"].apply(_extract_market)
        markets = sorted(df["_market"].unique())

        if args.dry_run:
            if i < 3 or i == len(all_dates) - 1:
                counts = {m: int((df["_market"] == m).sum()) for m in markets}
                logger.info("  [DRY RUN] %s: %d instruments → %d markets %s", date_str, len(df), len(markets), counts)
        else:
            for mkt in markets:
                mkt_df = df[df["_market"] == mkt].drop(columns=["_market"])
                dst_path = (
                    f"instrument_availability/by_date/day={date_str}/venue=POLYMARKET/market={mkt}/instruments.parquet"
                )
                buf = mkt_df.to_parquet(index=False)
                client.upload_bytes(bucket, dst_path, buf, content_type="application/octet-stream")
            total_written += len(markets)

        if (i + 1) % 50 == 0:
            logger.info("  Progress: %d/%d dates", i + 1, len(all_dates))

    if args.dry_run:
        logger.info("DRY RUN complete")
    else:
        logger.info("Split %d market files across %d dates (%d errors)", total_written, len(all_dates), errors)


if __name__ == "__main__":
    main()
