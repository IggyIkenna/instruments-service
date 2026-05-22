#!/usr/bin/env python3
"""Purge the 11 BITGET phantom null-capture_status rows from the CeFi manifest.

Phase 1 of data_status_coverage_gaps_and_prediction_manifest_fix_2026_05_22.

These rows have capture_status=None (not attempted_failed, not captured) and
were diagnosed as "phantom" entries that pre-date Tardis having real data for
these specific dates. The orchestrator skips dates that already have any row,
so these phantom None rows block legitimate backfill.

BITGET-FUTURES phantom dates: 2025-09-04, 2025-10-13, 2025-10-22, 2025-10-30,
                               2025-11-14, 2026-03-02
BITGET-SPOT phantom dates:    2025-11-16, 2025-12-17, 2025-12-30, 2026-01-16,
                               2026-03-25

Usage:
    .venv/bin/python scripts/purge_bitget_phantom_null_rows.py --dry-run
    .venv/bin/python scripts/purge_bitget_phantom_null_rows.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"instruments-store-cefi-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"

_PHANTOM_ROWS: list[dict[str, str]] = [
    {"venue": "BITGET-FUTURES", "date": "2025-09-04"},
    {"venue": "BITGET-FUTURES", "date": "2025-10-13"},
    {"venue": "BITGET-FUTURES", "date": "2025-10-22"},
    {"venue": "BITGET-FUTURES", "date": "2025-10-30"},
    {"venue": "BITGET-FUTURES", "date": "2025-11-14"},
    {"venue": "BITGET-FUTURES", "date": "2026-03-02"},
    {"venue": "BITGET-SPOT", "date": "2025-11-16"},
    {"venue": "BITGET-SPOT", "date": "2025-12-17"},
    {"venue": "BITGET-SPOT", "date": "2025-12-30"},
    {"venue": "BITGET-SPOT", "date": "2026-01-16"},
    {"venue": "BITGET-SPOT", "date": "2026-03-25"},
]


def main(dry_run: bool = True) -> None:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET)

    # Download canonical index
    data = bucket.blob(INDEX_PATH).download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("Loaded canonical index: %d rows", len(df))

    # Build mask for phantom rows
    mask = pd.Series([False] * len(df), index=df.index)
    found = 0
    for p in _PHANTOM_ROWS:
        row_mask = (df["venue"] == p["venue"]) & (df["date"].astype(str) == p["date"]) & (df["capture_status"].isna())
        cnt = row_mask.sum()
        if cnt == 0:
            logger.warning("Phantom row not found: venue=%s date=%s — may already be purged", p["venue"], p["date"])
        else:
            logger.info("Found phantom row: venue=%s date=%s count=%d", p["venue"], p["date"], cnt)
            found += cnt
        mask |= row_mask

    logger.info("Total phantom rows to remove: %d / %d", found, len(_PHANTOM_ROWS))

    if found == 0:
        logger.info("Nothing to purge — all phantom rows already gone.")
        return

    cleaned = df[~mask].reset_index(drop=True)
    logger.info("Cleaned index: %d rows (removed %d)", len(cleaned), len(df) - len(cleaned))

    if dry_run:
        logger.info("DRY RUN — not writing. Pass --apply to write.")
        return

    # Backup original
    run_ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_path = f"_index/backups/availability_index_pre_bitget_phantom_purge_{run_ts}.parquet"
    logger.info("Backing up original to %s", backup_path)
    bucket.blob(backup_path).upload_from_string(data, content_type="application/octet-stream")

    # Write cleaned index
    buf = io.BytesIO()
    cleaned.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    bucket.blob(INDEX_PATH).upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Uploaded cleaned index (%d rows)", len(cleaned))
    logger.info("DONE — %d phantom rows purged from %s", found, BUCKET)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--apply", action="store_true", default=False)
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("Pass --dry-run or --apply")

    main(dry_run=args.dry_run)
