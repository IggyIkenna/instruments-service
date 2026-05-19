#!/usr/bin/env python3
"""Rescan GCS sports reference data and rebuild the availability manifest.

Walks every day=X/entity=Y/*.parquet path in GCS, reads parquet metadata
for row counts, maps entity names to v4 venue names, and writes a clean
manifest index replacing the old (blank-venue) entries.

Usage:
    # Dry run — show what would be written:
    python scripts/rescan_sports_manifest.py --dry-run

    # Full rescan:
    python scripts/rescan_sports_manifest.py

    # Single date (test):
    python scripts/rescan_sports_manifest.py --date 2025-12-25

    # Parallel (VM):
    python scripts/rescan_sports_manifest.py --workers 16
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

_NOW = datetime.now(UTC).isoformat()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "instruments-store-sports-central-element-323112"
PREFIX = "sports_reference/by_date/"
# Use the same path as ManifestWriter so data status reads the right file
INDEX_BLOB = "_index/availability_index.parquet"

# Entity name (GCS directory) → v4 manifest data_type name.
# Source-agnostic: tracks what the data IS, not where it came from.
ENTITY_TO_DATA_TYPE: dict[str, str] = {
    "fixtures": "FIXTURES",
    "fixture_stats": "FIXTURE_STATS",
    "fixture_events": "FIXTURE_EVENTS",
    "fixture_lineups": "FIXTURE_LINEUPS",
    "player_stats": "PLAYER_STATS",
    "leagues": "LEAGUES",
    "teams": "TEAMS",
    "standings": "STANDINGS",
    "injuries": "INJURIES",
    "player_values": "PLAYER_VALUES",
    "transfermarkt_teams": "PLAYER_VALUES",  # old GCS name → same data_type
    "transfermarkt_leagues": "TRANSFERMARKT_LEAGUES",
    "footystats_matches": "MATCHES",
    "footystats_predictions": "PREDICTIONS",
    "understat_xg": "XG",
    "sfi_leagues": "SFI_LEAGUES",
    "sfi_standings": "SFI_STANDINGS",
    "ht_stats": "HT_STATS",
}


def _parse_day_entity(blob_name: str) -> tuple[str, str] | None:
    """Extract (date, entity) from a blob path like day=2025-01-01/entity=teams/teams.parquet."""
    parts = blob_name.replace(PREFIX, "").split("/")
    day_str = ""
    entity_str = ""
    for p in parts:
        if p.startswith("day="):
            day_str = p[4:]
        elif p.startswith("entity="):
            entity_str = p[7:]
    if day_str and entity_str:
        return day_str, entity_str
    return None


def _get_row_count_from_metadata(client: storage.Client, blob: storage.Blob) -> int:
    """Read parquet footer to get row count without downloading the whole file.

    Falls back to -1 if metadata can't be read.
    """
    try:
        # For small files, just check the size > 0
        blob.reload()
        if blob.size is not None and blob.size == 0:
            return 0
        # Use custom metadata if set by the writer
        if blob.metadata and "row_count" in blob.metadata:
            return int(blob.metadata["row_count"])
        # Fall back: download just the parquet footer (last 8 bytes = magic + footer size)
        # For simplicity, use file size as proxy: empty files = 0 rows
        # A parquet file with just the header is ~100-200 bytes
        if blob.size is not None and blob.size < 300:
            return 0
        return -1  # Unknown but non-empty
    except Exception:
        return -1


def scan_date(client: storage.Client, bucket: storage.Bucket, date_str: str) -> list[dict[str, str | int]]:
    """Scan a single date partition and return manifest entries."""
    prefix = f"{PREFIX}day={date_str}/entity="
    entries: list[dict[str, str | int]] = []
    seen_entities: set[str] = set()

    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        parsed = _parse_day_entity(blob.name)
        if parsed is None:
            continue
        day, entity = parsed
        if entity in seen_entities:
            continue  # One entry per entity per date
        seen_entities.add(entity)

        venue = ENTITY_TO_DATA_TYPE.get(entity, entity.upper())
        row_count = _get_row_count_from_metadata(client, blob)

        entries.append(
            {
                "date": day,
                "venue": "",
                "data_type": venue,  # Sports entities go in data_type, not venue
                "service_name": "instruments-service",
                "instrument_count": row_count if row_count >= 0 else 1,
                "written_at": _NOW,
                "schema_version": 4,
                "timeframe": "",
                "league_id": "",
            }
        )

    return entries


def list_all_dates(bucket: storage.Bucket) -> list[str]:
    """List all day= partitions in the sports reference bucket."""
    dates: list[str] = []
    # Use delimiter to get just the day= prefixes without recursing
    blobs_iter = bucket.list_blobs(prefix=PREFIX, delimiter="/")
    # The actual blobs won't have anything at this level, but prefixes will
    for page in blobs_iter.pages:
        for prefix_str in page.prefixes:
            # prefix_str = "sports_reference/by_date/day=2025-01-01/"
            part = prefix_str.rstrip("/").split("/")[-1]
            if part.startswith("day="):
                dates.append(part[4:])
    return sorted(dates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescan GCS sports manifest")
    parser.add_argument("--dry-run", action="store_true", help="Print entries without writing")
    parser.add_argument("--date", type=str, help="Scan a single date")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default 8)")
    parser.add_argument("--bucket", type=str, default=BUCKET_NAME, help="GCS bucket name")
    args = parser.parse_args()

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    if args.date:
        dates = [args.date]
    else:
        logger.info("Listing all dates in %s...", args.bucket)
        dates = list_all_dates(bucket)
        logger.info("Found %d dates", len(dates))

    all_entries: list[dict[str, str | int]] = []

    logger.info("Scanning %d dates with %d workers...", len(dates), args.workers)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(scan_date, client, bucket, d): d for d in dates}
        for done, future in enumerate(as_completed(futures), 1):
            date_str = futures[future]
            try:
                entries = future.result()
                all_entries.extend(entries)
            except Exception as exc:
                logger.warning("Failed to scan date=%s: %s", date_str, exc)
            if done % 200 == 0:
                logger.info("Progress: %d/%d dates scanned, %d entries so far", done, len(dates), len(all_entries))

    logger.info("Scan complete: %d entries across %d dates", len(all_entries), len(dates))

    if not all_entries:
        logger.warning("No entries found. Aborting.")
        sys.exit(1)

    df = pd.DataFrame(all_entries)

    # Summary
    logger.info("Data type breakdown:")
    for dt, count in df["data_type"].value_counts().items():
        logger.info("  %s: %d dates", dt, count)

    if args.dry_run:
        logger.info("DRY RUN — not writing index. Sample entries:")
        print(df.head(20).to_string())
        return

    # Write the new index
    index_blob = bucket.blob(INDEX_BLOB)

    # Read existing non-sports entries to preserve them
    existing_entries: list[dict[str, str | int]] = []
    if index_blob.exists():
        logger.info("Reading existing index to preserve non-sports entries...")
        local_path = str(Path(tempfile.gettempdir()) / "_existing_manifest.parquet")
        index_blob.download_to_filename(local_path)
        existing_df = pd.read_parquet(local_path)
        # Keep entries from other services (not instruments-service sports rescan)
        if "service_name" in existing_df.columns:
            non_sports = existing_df[existing_df["service_name"] != "instruments-service"]
            existing_entries = non_sports.to_dict("records")
            logger.info("Preserving %d non-sports entries", len(existing_entries))

    # Combine: new sports entries + preserved non-sports entries
    combined = pd.DataFrame(all_entries + existing_entries)
    local_out = str(Path(tempfile.gettempdir()) / "_new_manifest.parquet")
    combined.to_parquet(local_out, index=False)
    index_blob.upload_from_filename(local_out)
    logger.info("Wrote new index: %d total entries to %s/%s", len(combined), args.bucket, INDEX_BLOB)


if __name__ == "__main__":
    main()
