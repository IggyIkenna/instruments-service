#!/usr/bin/env python3
"""Backfill league_id into sports entity parquets by joining to fixtures.

Existing sports entity parquets (fixture_stats, fixture_events, lineups, etc.)
don't have league_id. This script joins each entity parquet to the fixtures
parquet for the same date on fixture_id to add league_id.

GCS layout:
  sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet
  sports_reference/by_date/day={date}/entity={entity}/{entity}.parquet

Usage:
    python3 scripts/backfill_entity_league_id.py --bucket my-bucket --entity fixture_stats --dry-run
    python3 scripts/backfill_entity_league_id.py --bucket my-bucket --entity fixture_stats
    python3 scripts/backfill_entity_league_id.py --bucket my-bucket --entity all
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Entities that have fixture_id and should get league_id backfilled
BACKFILL_ENTITIES = [
    "fixture_stats",
    "fixture_events",
    "fixture_lineups",
    "fixture_players",
    "fixture_predictions",
]

SPORTS_PREFIX = "sports_reference/by_date/"
FIXTURES_ENTITY = "fixtures"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill league_id into sports entity parquets by joining to fixtures.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name containing sports_reference/ prefix",
    )
    parser.add_argument(
        "--entity",
        required=True,
        help=("Entity name to backfill (e.g., fixture_stats, fixture_lineups) or 'all' for all supported entities"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview only — scan and report without modifying (default: True)",
    )
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Actually rewrite parquets with league_id added",
    )
    return parser.parse_args()


def _discover_dates(bucket: storage.Bucket) -> list[str]:
    """Discover all day= partitions under sports_reference/by_date/."""
    day_blobs = bucket.list_blobs(prefix=SPORTS_PREFIX, delimiter="/")
    list(day_blobs)  # consume to populate .prefixes
    dates: list[str] = []
    for prefix in sorted(day_blobs.prefixes):
        # prefix: sports_reference/by_date/day=2025-01-01/
        parts = prefix.rstrip("/").split("=")
        if len(parts) >= 2:
            dates.append(parts[-1])
    return dates


def _read_parquet_from_gcs(bucket: storage.Bucket, path: str) -> pd.DataFrame | None:
    """Read a parquet file from GCS, returning None if not found."""
    blob = bucket.blob(path)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data))
    return table.to_pandas()


def _write_parquet_to_gcs(bucket: storage.Bucket, path: str, df: pd.DataFrame) -> None:
    """Write a DataFrame as zstd-compressed parquet to GCS."""
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    blob = bucket.blob(path)
    blob.upload_from_file(buf, content_type="application/octet-stream")


def _backfill_entity(
    bucket: storage.Bucket,
    entity: str,
    dates: list[str],
    dry_run: bool,
) -> dict[str, int]:
    """Backfill league_id for a single entity across all dates.

    Returns stats dict with counts.
    """
    stats = {
        "dates_scanned": 0,
        "already_has_league_id": 0,
        "no_fixtures": 0,
        "no_entity": 0,
        "no_fixture_id_column": 0,
        "join_failed": 0,
        "backfilled": 0,
    }

    for date_str in dates:
        stats["dates_scanned"] += 1
        entity_path = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/{entity}.parquet"
        fixtures_path = f"{SPORTS_PREFIX}day={date_str}/entity={FIXTURES_ENTITY}/{FIXTURES_ENTITY}.parquet"

        # Read entity parquet
        entity_df = _read_parquet_from_gcs(bucket, entity_path)
        if entity_df is None:
            stats["no_entity"] += 1
            continue

        # Already has league_id?
        if "league_id" in entity_df.columns:
            non_null = entity_df["league_id"].notna().sum()
            if non_null > 0:
                stats["already_has_league_id"] += 1
                continue

        # Need fixture_id column to join
        if "fixture_id" not in entity_df.columns:
            logger.warning(
                "Entity %s on %s has no fixture_id column — cannot join to fixtures",
                entity,
                date_str,
            )
            stats["no_fixture_id_column"] += 1
            continue

        # Read fixtures for league_id lookup
        fixtures_df = _read_parquet_from_gcs(bucket, fixtures_path)
        if fixtures_df is None:
            logger.warning(
                "No fixtures parquet for %s — cannot backfill %s",
                date_str,
                entity,
            )
            stats["no_fixtures"] += 1
            continue

        if "fixture_id" not in fixtures_df.columns or "league_id" not in fixtures_df.columns:
            logger.warning(
                "Fixtures parquet for %s missing fixture_id or league_id columns",
                date_str,
            )
            stats["join_failed"] += 1
            continue

        # Build fixture_id -> league_id mapping from fixtures
        fixture_league_map = fixtures_df[["fixture_id", "league_id"]].drop_duplicates(subset=["fixture_id"])

        # Drop existing league_id if it was all-null
        if "league_id" in entity_df.columns:
            entity_df = entity_df.drop(columns=["league_id"])

        # Join
        merged = entity_df.merge(fixture_league_map, on="fixture_id", how="left")

        matched = merged["league_id"].notna().sum()
        total = len(merged)

        if dry_run:
            logger.info(
                "[DRY RUN] %s / %s: %d/%d rows would get league_id",
                date_str,
                entity,
                matched,
                total,
            )
        else:
            _write_parquet_to_gcs(bucket, entity_path, merged)
            logger.info(
                "Backfilled %s / %s: %d/%d rows got league_id",
                date_str,
                entity,
                matched,
                total,
            )

        stats["backfilled"] += 1

    return stats


def run(args: argparse.Namespace) -> None:
    client = storage.Client()
    bucket = client.bucket(args.bucket)

    entities = BACKFILL_ENTITIES if args.entity == "all" else [args.entity]
    logger.info("Entities to backfill: %s", entities)

    # Discover dates
    dates = _discover_dates(bucket)
    logger.info("Found %d date partitions in gs://%s/%s", len(dates), args.bucket, SPORTS_PREFIX)

    if not dates:
        logger.info("No date partitions found — nothing to do.")
        return

    for entity in entities:
        logger.info("--- Backfilling entity: %s ---", entity)
        stats = _backfill_entity(bucket, entity, dates, args.dry_run)
        logger.info("Entity %s stats:", entity)
        for key, val in stats.items():
            logger.info("  %s: %d", key, val)


def main() -> None:
    args = _parse_args()
    run(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
