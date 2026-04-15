#!/usr/bin/env python3
"""Migrate sports entity parquets from single-file to per-league partitioned layout.

Existing layout:
    sports_reference/by_date/day={date}/entity={name}/{name}.parquet

Target layout (matches fixtures pattern):
    sports_reference/by_date/day={date}/entity={name}/league={LEAGUE}/{name}.parquet

For per-fixture entities (fixture_stats, fixture_events, fixture_lineups, player_stats),
the league is determined by joining fixture_id to the fixtures parquet for the same date.

For entities with league_id column (injuries), the league is read directly.

For entities with canonical_fixture_id (footystats_predictions, footystats_matches,
understat_xg), the league is extracted from the canonical fixture_id prefix.

Usage:
    python3 scripts/migrate_sports_per_league.py --bucket instruments-store-sports-prod --entity all --dry-run
    python3 scripts/migrate_sports_per_league.py --bucket instruments-store-sports-prod --entity fixture_stats --no-dry-run
    python3 scripts/migrate_sports_per_league.py --bucket instruments-store-sports-prod --entity all --no-dry-run --workers 8
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from unified_api_contracts import get_prediction_leagues

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Entities that use AF numeric fixture_id and need a fixtures join for league lookup
PER_FIXTURE_ENTITIES = [
    "fixture_stats",
    "fixture_events",
    "fixture_lineups",
    "player_stats",
]

# Entities with canonical_fixture_id (league is the prefix before first colon)
CANONICAL_FID_ENTITIES = [
    "footystats_predictions",
    "footystats_matches",
]

# Entities with direct league_id column
LEAGUE_ID_ENTITIES = [
    "injuries",
]

# Entities with a 'league' column (Understat xG)
LEAGUE_COL_ENTITIES = [
    "understat_xg",
]

ALL_ENTITIES = PER_FIXTURE_ENTITIES + CANONICAL_FID_ENTITIES + LEAGUE_ID_ENTITIES + LEAGUE_COL_ENTITIES

SPORTS_PREFIX = "sports_reference/by_date/"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate sports entity parquets from single-file to per-league partitioned layout.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name (e.g. instruments-store-sports-prod)",
    )
    parser.add_argument(
        "--entity",
        required=True,
        help="Entity name (e.g. fixture_stats) or 'all' for all supported entities",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers for date processing (default: 4)",
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
        help="Actually split and write per-league files, then delete the single file",
    )
    return parser.parse_args()


def _discover_dates(bucket: storage.Bucket) -> list[str]:
    """Discover all day= partitions under sports_reference/by_date/."""
    day_blobs = bucket.list_blobs(prefix=SPORTS_PREFIX, delimiter="/")
    list(day_blobs)  # consume iterator to populate .prefixes
    dates: list[str] = []
    for prefix in sorted(day_blobs.prefixes):
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


def _has_per_league_files(bucket: storage.Bucket, date_str: str, entity: str) -> bool:
    """Check if per-league partitioned files exist AND the single file is gone.

    Returns True only when migration is complete (per-league files exist,
    single file removed). If both exist or only league=UNKNOWN exists,
    returns False so migration re-runs.
    """
    # Check if single file still exists — if so, migration isn't done
    single_path = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/{entity}.parquet"
    single_blob = bucket.blob(single_path)
    if single_blob.exists():
        return False  # Single file still present — needs migration

    # No single file — check if real (non-UNKNOWN) per-league files exist
    prefix = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/league="
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
    real_leagues = [b for b in blobs if "league=UNKNOWN" not in b.name]
    return len(real_leagues) > 0


def _build_af_fixture_to_league(bucket: storage.Bucket, date_str: str) -> dict[str, str]:
    """Build AF fixture_id -> canonical league_id mapping from fixtures parquet."""
    # Build reverse mapping from UAC: af_league_id -> canonical league_id
    af_league_to_canonical: dict[int, str] = {}
    for league_def in get_prediction_leagues():
        if league_def.api_football_id is not None:
            af_league_to_canonical[league_def.api_football_id] = league_def.league_id

    fixtures_path = f"{SPORTS_PREFIX}day={date_str}/entity=fixtures/fixtures.parquet"
    fixtures_df = _read_parquet_from_gcs(bucket, fixtures_path)
    if fixtures_df is None:
        # Try per-league fixture files
        prefix = f"{SPORTS_PREFIX}day={date_str}/entity=fixtures/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        league_dfs: list[pd.DataFrame] = []
        for blob in blobs:
            if blob.name.endswith(".parquet") and "league=" in blob.name:
                data = blob.download_as_bytes()
                league_dfs.append(pq.read_table(io.BytesIO(data)).to_pandas())
        if league_dfs:
            fixtures_df = pd.concat(league_dfs, ignore_index=True)
        else:
            return {}

    result: dict[str, str] = {}
    if "af_fixture_id" not in fixtures_df.columns:
        return result

    if "league_id" in fixtures_df.columns:
        for _, row in fixtures_df[["af_fixture_id", "league_id"]].dropna().iterrows():
            result[str(int(row["af_fixture_id"]))] = str(row["league_id"])
    elif "af_league_id" in fixtures_df.columns:
        for _, row in fixtures_df[["af_fixture_id", "af_league_id"]].dropna().iterrows():
            af_lid = int(row["af_league_id"])
            canonical = af_league_to_canonical.get(af_lid)
            if canonical:
                result[str(int(row["af_fixture_id"]))] = canonical

    return result


def _get_league_column(df: pd.DataFrame, entity: str, fid_to_league: dict[str, str]) -> pd.Series | None:
    """Determine the league for each row based on entity type.

    Returns a Series of league_id strings, or None if no league info is available.
    """
    if entity in PER_FIXTURE_ENTITIES:
        # Column is "af_fixture_id" in per-fixture entity data (not "fixture_id")
        fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
        if fid_col not in df.columns or not fid_to_league:
            return None
        return df[fid_col].astype(str).str.split(".").str[0].map(fid_to_league)

    if entity in CANONICAL_FID_ENTITIES:
        if "canonical_fixture_id" not in df.columns:
            return None
        leagues = df["canonical_fixture_id"].str.split(":").str[0]
        # Replace empty strings with None
        return leagues.replace("", pd.NA)

    if entity in LEAGUE_ID_ENTITIES:
        if "league_id" in df.columns and df["league_id"].notna().any():
            return df["league_id"]
        # Fallback: try fixture_id canonical format
        if "fixture_id" in df.columns:
            sample = df["fixture_id"].dropna()
            if not sample.empty and ":" in str(sample.iloc[0]):
                return df["fixture_id"].str.split(":").str[0]
        return None

    if entity in LEAGUE_COL_ENTITIES:
        if "league" in df.columns and df["league"].notna().any():
            return df["league"]
        return None

    return None


def _migrate_entity_date(
    bucket: storage.Bucket,
    entity: str,
    date_str: str,
    dry_run: bool,
) -> dict[str, int]:
    """Migrate a single entity for a single date from single-file to per-league.

    Returns stats dict.
    """
    stats: dict[str, int] = {
        "already_per_league": 0,
        "no_single_file": 0,
        "no_league_info": 0,
        "migrated": 0,
        "leagues_written": 0,
        "rows_unmapped": 0,
    }

    # Skip if per-league files already exist
    if _has_per_league_files(bucket, date_str, entity):
        stats["already_per_league"] = 1
        return stats

    # Read the single-file parquet
    entity_path = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/{entity}.parquet"
    entity_df = _read_parquet_from_gcs(bucket, entity_path)
    if entity_df is None or entity_df.empty:
        stats["no_single_file"] = 1
        return stats

    # Build fixture_id -> league map if needed for per-fixture entities
    fid_to_league: dict[str, str] = {}
    if entity in PER_FIXTURE_ENTITIES:
        fid_to_league = _build_af_fixture_to_league(bucket, date_str)
        if not fid_to_league:
            logger.warning(
                "date=%s entity=%s: no fixture->league mapping available, skipping",
                date_str,
                entity,
            )
            stats["no_league_info"] = 1
            return stats

    # Get league for each row
    league_series = _get_league_column(entity_df, entity, fid_to_league)
    if league_series is None:
        logger.warning(
            "date=%s entity=%s: cannot determine league column, skipping",
            date_str,
            entity,
        )
        stats["no_league_info"] = 1
        return stats

    entity_df["_league"] = league_series
    has_league = entity_df["_league"].notna() & (entity_df["_league"].astype(str) != "")
    with_league = entity_df[has_league]
    without_league = entity_df[~has_league]

    if with_league.empty:
        logger.warning(
            "date=%s entity=%s: all %d rows have no league mapping",
            date_str,
            entity,
            len(entity_df),
        )
        stats["no_league_info"] = 1
        return stats

    if dry_run:
        league_counts = with_league.groupby("_league").size()
        logger.info(
            "[DRY RUN] date=%s entity=%s: would split %d rows into %d leagues, %d unmapped",
            date_str,
            entity,
            len(with_league),
            len(league_counts),
            len(without_league),
        )
        for lid, count in league_counts.items():
            logger.info("  league=%s: %d rows", lid, count)
        stats["migrated"] = 1
        stats["leagues_written"] = len(league_counts)
        stats["rows_unmapped"] = len(without_league)
        return stats

    # Write per-league files
    leagues_written = 0
    for lid, league_df in with_league.groupby("_league"):
        lid_str = str(lid)
        clean_df = league_df.drop(columns=["_league"])
        league_path = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/league={lid_str}/{entity}.parquet"
        _write_parquet_to_gcs(bucket, league_path, clean_df)
        leagues_written += 1

    # Write unmapped rows to catch-all (keep original single-file path)
    if not without_league.empty:
        unmapped_df = without_league.drop(columns=["_league"])
        unmapped_path = f"{SPORTS_PREFIX}day={date_str}/entity={entity}/{entity}_unmapped.parquet"
        _write_parquet_to_gcs(bucket, unmapped_path, unmapped_df)
        stats["rows_unmapped"] = len(unmapped_df)

    # Delete the original single file only after all per-league writes succeed
    old_blob = bucket.blob(entity_path)
    if old_blob.exists():
        old_blob.delete()
        logger.info(
            "date=%s entity=%s: migrated %d rows into %d leagues, deleted single file",
            date_str,
            entity,
            len(with_league),
            leagues_written,
        )

    stats["migrated"] = 1
    stats["leagues_written"] = leagues_written
    return stats


def _migrate_entity(
    bucket_name: str,
    entity: str,
    dates: list[str],
    dry_run: bool,
    workers: int,
) -> dict[str, int]:
    """Migrate a single entity across all dates using thread pool."""
    totals: dict[str, int] = {
        "dates_scanned": 0,
        "already_per_league": 0,
        "no_single_file": 0,
        "no_league_info": 0,
        "migrated": 0,
        "leagues_written": 0,
        "rows_unmapped": 0,
    }

    def _process_date(date_str: str) -> dict[str, int]:
        client = storage.Client()
        bkt = client.bucket(bucket_name)
        return _migrate_entity_date(bkt, entity, date_str, dry_run)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_date, d): d for d in dates}
        for future in as_completed(futures):
            date_str = futures[future]
            totals["dates_scanned"] += 1
            try:
                result = future.result()
                for k, v in result.items():
                    totals[k] = totals.get(k, 0) + v
            except Exception as exc:
                logger.warning("date=%s entity=%s: error — %s", date_str, entity, exc)

    return totals


def run(args: argparse.Namespace) -> None:
    entities = ALL_ENTITIES if args.entity == "all" else [args.entity]
    logger.info("Entities to migrate: %s", entities)
    logger.info("Mode: %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("Workers: %d", args.workers)

    # Discover dates
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    dates = _discover_dates(bucket)
    logger.info("Found %d date partitions in gs://%s/%s", len(dates), args.bucket, SPORTS_PREFIX)

    if not dates:
        logger.info("No date partitions found — nothing to do.")
        return

    for entity in entities:
        logger.info("--- Migrating entity: %s ---", entity)
        stats = _migrate_entity(
            bucket_name=args.bucket,
            entity=entity,
            dates=dates,
            dry_run=args.dry_run,
            workers=args.workers,
        )
        logger.info("Entity %s results:", entity)
        for key, val in stats.items():
            logger.info("  %s: %d", key, val)


def main() -> None:
    args = _parse_args()
    run(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
