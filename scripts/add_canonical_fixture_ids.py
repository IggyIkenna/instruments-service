#!/usr/bin/env python3
"""Add canonical fixture_id to existing Understat/FootyStats parquets in GCS.

Reads each parquet, resolves team names to canonical IDs using UAC mappings,
builds fixture_id from (league + canonical_home + canonical_away + date),
writes back in place. No API calls — pure local transformation.

Usage:
    # Dry run:
    python scripts/add_canonical_fixture_ids.py --dry-run --entity understat_xg

    # Single date:
    python scripts/add_canonical_fixture_ids.py --entity understat_xg --date 2025-01-05

    # All dates (parallel):
    python scripts/add_canonical_fixture_ids.py --entity understat_xg --workers 16

    # FootyStats:
    python scripts/add_canonical_fixture_ids.py --entity footystats_matches --workers 16
    python scripts/add_canonical_fixture_ids.py --entity footystats_predictions --workers 16
"""

from __future__ import annotations

import argparse
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage
from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
from unified_api_contracts.sports import (
    FOOTYSTATS_HISTORICAL_SEASON_IDS,
    resolve_footystats_team,
    resolve_understat_team,
)

# Historical season_id → canonical league_id (covers ALL seasons 2010-2026)
_FT_COMP_TO_LEAGUE: dict[str, str] = {str(k): v for k, v in FOOTYSTATS_HISTORICAL_SEASON_IDS.items()}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET_NAME = "instruments-store-sports-central-element-323112"
PREFIX = "sports_reference/by_date"

# Entity → (home_col_candidates, away_col_candidates, league_col_candidates, resolver_fn)
# Multiple candidates because different backfill versions used different column names.
ENTITY_CONFIG: dict[str, tuple[list[str], list[str], list[str], object]] = {
    "understat_xg": (
        ["h_title", "home_team_name", "home_team"],
        ["a_title", "away_team_name", "away_team"],
        ["league", "league_name"],
        resolve_understat_team,
    ),
    "footystats_matches": (
        ["home_team", "home_team_name", "h_title"],
        ["away_team", "away_team_name", "a_title"],
        ["league", "league_name"],
        resolve_footystats_team,
    ),
    "footystats_predictions": (
        ["home_team", "home_team_name"],
        ["away_team", "away_team_name"],
        ["league", "league_name"],
        resolve_footystats_team,
    ),
}


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def process_date(
    client: storage.Client,
    bucket_name: str,
    entity: str,
    date_str: str,
    dry_run: bool,
) -> str:
    """Add fixture_id to one date's parquet file."""
    home_candidates, away_candidates, league_candidates, resolver = ENTITY_CONFIG[entity]

    blob_path = f"{PREFIX}/day={date_str}/entity={entity}/{entity}.parquet"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        return f"SKIP {date_str}: no parquet"

    # Download
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))

    if df.empty:
        return f"SKIP {date_str}: empty"

    # Already has CANONICAL fixture_id with league prefix?
    if "canonical_fixture_id" in df.columns and df["canonical_fixture_id"].notna().all():
        # Check if league is present (should NOT start with ":")
        sample = str(df["canonical_fixture_id"].iloc[0])
        if not sample.startswith(":"):
            return f"SKIP {date_str}: canonical_fixture_id already has league"

    # Find columns
    home_col = _find_col(df, home_candidates)
    away_col = _find_col(df, away_candidates)
    league_col = _find_col(df, league_candidates)

    if not home_col or not away_col:
        return f"SKIP {date_str}: no home/away columns (have: {list(df.columns)[:5]})"

    def _build_fid(row: pd.Series) -> str:
        home = str(row.get(home_col, "") or "")
        away = str(row.get(away_col, "") or "")
        league = str(row.get(league_col, "") or "") if league_col else ""
        # For FootyStats: extract league from fixture_id prefix (competition_id)
        if not league and "fixture_id" in row.index:
            fid = str(row.get("fixture_id", "") or "")
            if ":" in fid:
                comp_str = fid.split(":")[0]
                league = _FT_COMP_TO_LEAGUE.get(comp_str, "")
        if not home or not away:
            return ""
        return build_fixture_id(
            league_id=league,
            home_team_id=resolver(home),
            away_team_id=resolver(away),
            date_str=date_str,
        )

    df["canonical_fixture_id"] = df.apply(_build_fid, axis=1)

    if dry_run:
        sample = df[["canonical_fixture_id", home_col, away_col]].head(3).to_string()
        return f"DRY-RUN {date_str}: {len(df)} rows, sample:\n{sample}"

    # Write back
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    return f"UPDATED {date_str}: {len(df)} rows with fixture_id"


def list_dates_for_entity(bucket: storage.Bucket, entity: str) -> list[str]:
    """Find all dates that have this entity in GCS."""
    dates: list[str] = []
    prefix = f"{PREFIX}/"
    for page in bucket.list_blobs(prefix=prefix, delimiter="/").pages:
        for p in page.prefixes:
            day_part = p.rstrip("/").split("/")[-1]
            if day_part.startswith("day="):
                dates.append(day_part[4:])
    return sorted(dates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Add canonical fixture_id to parquets")
    parser.add_argument("--entity", required=True, choices=list(ENTITY_CONFIG.keys()))
    parser.add_argument("--date", type=str, help="Single date")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--bucket", type=str, default=BUCKET_NAME)
    args = parser.parse_args()

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    if args.date:
        dates = [args.date]
    else:
        logger.info("Listing dates for entity=%s...", args.entity)
        dates = list_dates_for_entity(bucket, args.entity)
        logger.info("Found %d dates", len(dates))

    updated = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_date, client, args.bucket, args.entity, d, args.dry_run): d for d in dates}
        for done, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if "UPDATED" in result:
                updated += 1
            else:
                skipped += 1
            if done % 100 == 0 or "UPDATED" in result or "DRY-RUN" in result:
                logger.info("[%d/%d] %s", done, len(dates), result)

    logger.info("Done: %d updated, %d skipped", updated, skipped)


if __name__ == "__main__":
    main()
