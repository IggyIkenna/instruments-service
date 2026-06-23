#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after XG blank-league phantom rows confirmed empty_confirmed in golden window
"""Reclassify legacy XG blank-league ``attempted_failed`` rows to ``empty_confirmed``.

Background (2026-06-23):

Before the per-league migration (April 2026), the Understat XG orchestrator wrote
aggregate rows with blank ``league_id`` and blank ``venue``. After the migration to
per-league rows, the phantom reconciler (reconcile_phantom_manifest_rows_all.py) flipped
those old aggregate rows from ``captured`` → ``attempted_failed`` with
``error_reason = "phantom_captured_no_parquet_at_canonical_path"``.

These 48 rows in the golden window (2025-09-01 → 2025-11-30) should be
``empty_confirmed`` with reason ``EXPECTED_NO_FIXTURE``:
  - They represent the superseded aggregate format (no parquet ever existed at
    the canonical per-league path, nor will one).
  - The per-league rows (with specific ``league_id``) now carry the real coverage.
  - Leaving them as ``attempted_failed`` falsely inflates the failed count and
    blocks 100% coverage for the XG golden window audit.

Filter: ``data_type == 'XG'`` AND ``capture_status == 'attempted_failed'``
        AND ``error_reason == 'phantom_captured_no_parquet_at_canonical_path'``
        AND ``league_id`` is blank/NaN
        AND ``date`` in 2025-09-01 → 2025-11-30

Idempotent: rows already ``empty_confirmed`` are left alone.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \\
        .venv/bin/python scripts/reclassify_xg_blank_league_phantoms.py --dry-run
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \\
        .venv/bin/python scripts/reclassify_xg_blank_league_phantoms.py
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_GOLDEN_WINDOW_START = "2025-09-01"
_GOLDEN_WINDOW_END = "2025-11-30"
_BLOB = "_index/availability_index.parquet"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--bucket",
        default="instruments-store-sports-prd-central-element-323112",
        help="GCS bucket holding the sports instruments manifest",
    )
    p.add_argument("--blob", default=_BLOB, help="Manifest blob path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    client = storage.Client()
    bucket = client.bucket(args.bucket)
    blob = bucket.blob(args.blob)

    logger.info("Loading manifest from gs://%s/%s", args.bucket, args.blob)
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes(timeout=300)))
    logger.info("Manifest rows: %d", len(df))

    # Normalise date column for comparison
    date_col = df["date"].astype(str)

    failed_mask = df["capture_status"].fillna("") == "attempted_failed"
    xg_mask = df["data_type"].fillna("") == "XG"
    phantom_mask = (
        df["error_reason"].fillna("") == "phantom_captured_no_parquet_at_canonical_path"
    )
    # Blank league_id (empty string or NaN)
    blank_league_mask = df["league_id"].fillna("").astype(str).str.strip() == ""
    date_mask = (date_col >= _GOLDEN_WINDOW_START) & (date_col <= _GOLDEN_WINDOW_END)

    target_mask = failed_mask & xg_mask & phantom_mask & blank_league_mask & date_mask

    n_target = int(target_mask.sum())
    logger.info("Rows matching XG blank-league phantom criteria: %d", n_target)

    if n_target == 0:
        logger.info("Nothing to reclassify. Exiting.")
        return 0

    sub = df[target_mask]
    logger.info("Date range: %s → %s", sub["date"].min(), sub["date"].max())
    logger.info("Unique dates: %d", sub["date"].nunique())
    logger.info("By venue (top 5):")
    for v, n in sub["venue"].fillna("(blank)").value_counts().head(5).items():
        logger.info("  %-30s %d", v, n)

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified.")
        return 0

    df.loc[target_mask, "capture_status"] = "empty_confirmed"
    df.loc[target_mask, "error_reason"] = "EXPECTED_NO_FIXTURE"

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info(
        "Reclassified %d rows attempted_failed → empty_confirmed (EXPECTED_NO_FIXTURE)",
        n_target,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
