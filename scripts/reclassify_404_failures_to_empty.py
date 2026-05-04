#!/usr/bin/env python3
"""Reclassify ``attempted_failed`` manifest rows whose ``error_reason``
indicates a definitive 404 from the source archive into
``empty_confirmed``.

Background (2026-05-04):

The ``attempted_failed`` capture status is reserved for adapter-side
exceptions where retry might help (network errors, 5xx, parser bugs).
A 404 from Tardis (or any historical archive) means the source
DEFINITIVELY does not have that ``(date, symbol, data_type)`` combo —
no amount of retry will produce data. Per the v5 honest-coverage
schema, these rows belong in ``empty_confirmed`` (counts in
denominator only, no alert) instead of ``attempted_failed`` (counts
+ triggers alerts + auto-retries).

The 2026-05-04 cefi audit revealed 101,332 such 404 rows across
DERIBIT historical chains, KRAKEN-FUTURES (cryptofacilities Tardis
archive sparse pre-2024), BITFINEX-FUTURES (bitfinex-derivatives same),
and BITGET-FUTURES (Tardis bitget-futures archive starts 2024-11-08).
Reclassifying drops cefi false-failed rate from 12% → ~5% without any
new backfill work.

Idempotent: rows whose ``error_reason`` doesn't start with the 404
prefix list are left alone.

Usage::

    cd instruments-service
    .venv/bin/python scripts/reclassify_404_failures_to_empty.py \\
        --bucket market-data-tick-cefi-central-element-323112 --dry-run
    .venv/bin/python scripts/reclassify_404_failures_to_empty.py \\
        --bucket market-data-tick-cefi-central-element-323112
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

# Error-reason prefixes that mean "source archive definitively lacks this
# (date, symbol, data_type) combo — retry won't help, treat as empty".
_DEFINITIVE_NOT_FOUND_PREFIXES = (
    "404 GET ",
    "HTTP 404",
    "404 Not Found",
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True, help="GCS bucket holding the canonical manifest")
    p.add_argument("--blob", default="_index/availability_index.parquet", help="Manifest blob path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    client = storage.Client()
    bucket = client.bucket(args.bucket)
    blob = bucket.blob(args.blob)

    logger.info("Loading manifest from gs://%s/%s", args.bucket, args.blob)
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes(timeout=300)))
    logger.info("Manifest rows: %d", len(df))

    failed_mask = df["capture_status"].fillna("") == "attempted_failed"
    er = df["error_reason"].fillna("").astype(str)
    not_found_mask = failed_mask & er.apply(
        lambda s: any(s.startswith(p) for p in _DEFINITIVE_NOT_FOUND_PREFIXES)
    )

    n_target = int(not_found_mask.sum())
    logger.info("Rows matching definitive 404 reasons: %d", n_target)
    if n_target == 0:
        logger.info("Nothing to reclassify. Exiting.")
        return 0

    # Distribution preview
    sub = df[not_found_mask]
    logger.info("By venue (top 10):")
    for v, n in sub["venue"].value_counts().head(10).items():
        logger.info("  %-25s %d", v, n)
    logger.info("By data_type (top 10):")
    for dt, n in sub["data_type"].value_counts().head(10).items():
        logger.info("  %-25s %d", dt, n)

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified.")
        return 0

    # Flip status + clear error_reason (it's no longer an error).
    df.loc[not_found_mask, "capture_status"] = "empty_confirmed"
    df.loc[not_found_mask, "error_reason"] = ""

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Reclassified %d rows attempted_failed → empty_confirmed", n_target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
