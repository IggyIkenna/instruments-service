#!/usr/bin/env python3
"""Purge manifest rows for deprecated TradFi ETFs.

MVP scope reduction 2026-05-05 dropped from launch-tradfi-backfill-vm.sh:
- BATS-listed: FBTC, ARKB, FETH
- NYSE Arca-listed: GBTC, ETHE, BITO

Only NASDAQ IBIT + NASDAQ ETHA remain in MVP for the BlackRock spot-ETF leg
(see deployment-service/scripts/vm/launch-tradfi-backfill-vm.sh comments).

Existing manifest rows for the deprecated ETFs are stale residue that pollute
the data-status denominator. This script purges them via CAS write.

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_deprecated_etf_manifest_rows_2026_05_16.py --dry-run
    .venv/bin/python scripts/purge_deprecated_etf_manifest_rows_2026_05_16.py

Idempotent. CAS via `if_generation_match` guards against concurrent
consolidator-daemon writes.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET_NAME = f"market-data-tick-tradfi-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"

# Deprecated ETF tickers per MVP scope reduction 2026-05-05.
# Match against the `instrument_id` column (substring match — instrument_ids
# look like NYSE:ETF:ETHE or BATS:ETF:FBTC etc.).
DEPRECATED_TICKERS = ("ETHE", "GBTC", "BITO", "FBTC", "ARKB", "FETH")
# Restrict to venues that historically held the deprecated rows. NASDAQ IBIT
# + NASDAQ ETHA are in-scope and must NEVER match.
DEPRECATED_VENUES = ("NYSE", "NYSE_ARCA", "BATS", "CBOE_BZX")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT_ID)
    args = parser.parse_args()

    gcs = storage.Client(project=args.project)
    bucket = gcs.bucket(BUCKET_NAME)
    blob = bucket.blob(INDEX_PATH)
    blob.reload()
    generation = blob.generation

    import pyarrow.parquet as pq

    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data))
    df: pd.DataFrame = table.to_pandas()
    logger.info("Loaded manifest: %d rows", len(df))

    venue_mask = df["venue"].astype(str).str.upper().isin(DEPRECATED_VENUES)
    ticker_re = "|".join(DEPRECATED_TICKERS)
    instr_mask = df["instrument_id"].astype(str).str.contains(ticker_re, regex=True, na=False)
    underlying_mask = df["underlying"].astype(str).str.contains(ticker_re, regex=True, na=False)
    drop_mask = venue_mask & (instr_mask | underlying_mask)
    drop = df[drop_mask]
    keep = df[~drop_mask]
    logger.info("Drop mask matched %d rows; keep %d rows", len(drop), len(keep))

    if len(drop) == 0:
        logger.info("No deprecated-ETF rows to purge. Manifest is clean.")
        return 0

    logger.info("Sample of rows to drop (first 10):")
    logger.info(
        "\n%s",
        drop[["venue", "instrument_id", "underlying", "data_type", "capture_status"]].head(10).to_string(index=False),
    )

    if args.dry_run:
        logger.info("--dry-run: NOT writing manifest. Re-run without --dry-run to delete.")
        return 0

    out = io.BytesIO()
    keep.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    logger.info(
        "Uploading purged manifest (%d rows total; %d deprecated-ETF rows removed) with if_generation_match=%s",
        len(keep),
        len(drop),
        generation,
    )
    bucket.blob(INDEX_PATH).upload_from_string(
        out.getvalue(),
        content_type="application/octet-stream",
        if_generation_match=generation,
    )
    logger.info("Done at %s", datetime.now(UTC).isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
