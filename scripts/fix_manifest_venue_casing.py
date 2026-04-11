#!/usr/bin/env python3
"""Fix manifest venue name casing — normalize all venues to UPPER CASE.

Reads availability_index.parquet, upper-cases venue names, deduplicates
(summing instrument_count for same date+venue), and overwrites the index.

Usage:
    python3 scripts/fix_manifest_venue_casing.py --category SPORTS --dry-run
    python3 scripts/fix_manifest_venue_casing.py --category SPORTS
"""

from __future__ import annotations

import argparse
import io
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix manifest venue casing")
    parser.add_argument("--category", default="SPORTS", help="Category")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    from unified_trading_library import get_bucket_name, get_storage_client, read_availability_index

    bucket = get_bucket_name("instruments", args.category)
    logger.info("Bucket: %s", bucket)

    existing = read_availability_index(bucket)
    if existing.empty:
        logger.info("Manifest is empty — nothing to fix")
        return

    logger.info("Current manifest: %d entries", len(existing))
    venues_before = sorted(existing["venue"].unique())
    logger.info("Venues before: %s", venues_before)

    # Normalize venue names to UPPER CASE
    existing["venue"] = existing["venue"].str.upper()

    # Deduplicate: group by (date, venue), keep first row's metadata, sum instrument_count
    count_col = "instrument_count" if "instrument_count" in existing.columns else "row_count"
    agg_dict = {count_col: "sum"}
    # Preserve metadata columns
    for col in ["service_name", "written_at", "schema_version", "data_type"]:
        if col in existing.columns:
            agg_dict[col] = "first"

    deduped = existing.groupby(["date", "venue"], as_index=False).agg(agg_dict)

    venues_after = sorted(deduped["venue"].unique())
    logger.info("Venues after:  %s", venues_after)
    logger.info("Entries: %d → %d (removed %d duplicates)", len(existing), len(deduped), len(existing) - len(deduped))

    if len(existing) == len(deduped):
        logger.info("No duplicates found — manifest is clean")
        return

    if args.dry_run:
        logger.info("[DRY-RUN] Would rewrite manifest with %d entries", len(deduped))
        return

    # Overwrite the parquet file directly (not via ManifestWriter which merges/appends)
    client = get_storage_client()
    native_client = getattr(client, "_client", None)
    if native_client is None:
        logger.error("Cannot get native GCS client for direct overwrite")
        return

    buf = io.BytesIO()
    deduped.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    blob = native_client.bucket(bucket).blob(_INDEX_PATH)
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")

    logger.info("Overwrote manifest with %d entries", len(deduped))

    # Verify
    updated = read_availability_index(bucket)
    logger.info(
        "Verified: %d entries, %d venues: %s",
        len(updated),
        updated["venue"].nunique() if not updated.empty else 0,
        sorted(updated["venue"].unique()) if not updated.empty else [],
    )


if __name__ == "__main__":
    main()
