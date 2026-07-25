#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + orphan sweep re-run shows canonical coverage for these 16 days
"""Migrate-forward the v2 staging-tree post-floor rows into canonical per-league objects.

Source: `sports_reference_v2/by_date/day={date}/entity={fixtures,fixture_stats}/{entity}.parquet`
(the 2026-04-28 v1->v2 migration staging tree), for the 16 post-floor days (day >= 2020-06-06)
identified in `plans/active/issues/sports_legacy_duplicate_triage_2026_07_22.md` section 4
("58 v2 post-floor rows", disposition: migrate-forward).

Target: canonical per-league `sports_reference/by_date/day={date}/entity={entity}/league={L}/{entity}.parquet`
(mirrors `migrate_sports_per_league.py`'s per-fixture-league-join logic).

NOT A DELETE (per the issue doc's explicit recommendation): the v2 source object is left in place.
This is an ADDITIVE-ONLY write:
  - If no canonical object exists yet for (day, entity, league): write the migrated rows directly.
  - If a canonical object ALREADY exists (partial per-league coverage, per the issue doc's finding that
    existing canonical objects hold only a small fraction of the v2 row count): never overwrite it. Instead
    write only the delta rows (af_fixture_id present in v2, absent from the existing canonical object) to a
    sibling file `{entity}_v2_migrated.parquet` in the same league directory.

The canonical `entity=fixtures` schema has evolved since these v2 rows were captured (53 columns today vs.
31 in the v2 source — additive fields only, e.g. `available_at`, `match_end_time`, `match_result`). Migrated
rows are written with their native (older) schema rather than padded with nulls for the newer columns —
this matches how per-league files already vary in schema across capture dates in this corpus, and a
downstream reader unions multiple per-league files with pandas/pyarrow's standard missing-column-as-null
concat semantics.

Usage:
    python3 scripts/migrate_sports_v2_postfloor_per_league_2026_07_25.py --dry-run
    python3 scripts/migrate_sports_v2_postfloor_per_league_2026_07_25.py --no-dry-run
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
from unified_api_contracts import get_prediction_leagues
from unified_trading_library import resolve_bucket_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SPORTS_FLOOR_DAY = "2020-06-06"
V2_PREFIX = "sports_reference_v2/by_date/"
CANONICAL_PREFIX = "sports_reference/by_date/"
ENTITIES = ("fixtures", "fixture_stats")


def _discover_v2_postfloor_days(bucket: storage.Bucket) -> list[str]:
    it = bucket.list_blobs(prefix=V2_PREFIX, delimiter="/")
    list(it)
    days = sorted(p.rstrip("/").split("=")[-1] for p in it.prefixes)
    return [d for d in days if d >= SPORTS_FLOOR_DAY]


def _read_parquet(bucket: storage.Bucket, path: str) -> pd.DataFrame | None:
    blob = bucket.blob(path)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    return pq.read_table(io.BytesIO(data)).to_pandas()


def _write_parquet(bucket: storage.Bucket, path: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    blob = bucket.blob(path)
    blob.upload_from_file(buf, content_type="application/octet-stream")


def _af_league_to_canonical_map() -> dict[int, str]:
    mapping: dict[int, str] = {}
    for league_def in get_prediction_leagues():
        if league_def.api_football_id is not None:
            mapping[league_def.api_football_id] = league_def.league_id
    return mapping


def _fixtures_league_series(df: pd.DataFrame, af_to_canon: dict[int, str]) -> pd.Series:
    """fixtures rows carry af_league_id directly."""
    return df["af_league_id"].dropna().astype(int).map(af_to_canon)


def _fid_to_league_from_v2_fixtures(fixtures_df: pd.DataFrame, af_to_canon: dict[int, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    sub = fixtures_df[["af_fixture_id", "af_league_id"]].dropna()
    for _, row in sub.iterrows():
        canon = af_to_canon.get(int(row["af_league_id"]))
        if canon:
            result[str(int(row["af_fixture_id"]))] = canon
    return result


def _canonical_league_path(day: str, entity: str, league: str) -> str:
    return f"{CANONICAL_PREFIX}day={day}/entity={entity}/league={league}/{entity}.parquet"


def _plan_writes_for_entity(
    bucket: storage.Bucket,
    day: str,
    entity: str,
    entity_df: pd.DataFrame,
    league_series: pd.Series,
    id_col: str,
    dry_run: bool,
    stats: dict[str, int],
) -> None:
    entity_df = entity_df.copy()
    entity_df["_league"] = league_series
    has_league = entity_df["_league"].notna()
    with_league = entity_df[has_league]
    unmapped = len(entity_df) - len(with_league)
    if unmapped:
        logger.warning(
            "day=%s entity=%s: %d/%d rows have no league mapping (af_league_id unknown to UAC)",
            day,
            entity,
            unmapped,
            len(entity_df),
        )
        stats["rows_unmapped"] += unmapped

    for league, league_df in with_league.groupby("_league"):
        league_df = league_df.drop(columns=["_league"])
        target_path = _canonical_league_path(day, entity, str(league))
        existing = _read_parquet(bucket, target_path)

        if existing is None:
            logger.info(
                "[%s] day=%s entity=%s league=%s: NEW canonical file, %d rows",
                "DRY-RUN" if dry_run else "WRITE",
                day,
                entity,
                league,
                len(league_df),
            )
            stats["new_files"] += 1
            stats["rows_written"] += len(league_df)
            if not dry_run:
                _write_parquet(bucket, target_path, league_df)
            continue

        existing_ids = set(existing[id_col].astype(str))
        delta_df = league_df[~league_df[id_col].astype(str).isin(existing_ids)]
        if delta_df.empty:
            logger.info(
                "day=%s entity=%s league=%s: all %d v2 rows already present in canonical, skip",
                day,
                entity,
                league,
                len(league_df),
            )
            stats["already_covered"] += 1
            continue

        delta_path = target_path.replace(f"{entity}.parquet", f"{entity}_v2_migrated.parquet")
        logger.info(
            "[%s] day=%s entity=%s league=%s: existing canonical has %d rows, "
            "writing %d DELTA rows (never overwriting existing) to %s",
            "DRY-RUN" if dry_run else "WRITE",
            day,
            entity,
            league,
            len(existing),
            len(delta_df),
            delta_path,
        )
        stats["delta_files"] += 1
        stats["rows_written"] += len(delta_df)
        if not dry_run:
            _write_parquet(bucket, delta_path, delta_df)


def run(dry_run: bool) -> int:
    client = storage.Client()
    bucket_name = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    bucket = client.bucket(bucket_name)

    days = _discover_v2_postfloor_days(bucket)
    logger.info(
        "Mode: %s. Bucket: %s. Post-floor v2 days: %d", "DRY RUN" if dry_run else "LIVE", bucket_name, len(days)
    )

    af_to_canon = _af_league_to_canonical_map()

    stats: dict[str, int] = {
        "new_files": 0,
        "delta_files": 0,
        "already_covered": 0,
        "rows_written": 0,
        "rows_unmapped": 0,
        "days_with_no_fixtures_source": 0,
    }

    for day in days:
        fixtures_df = _read_parquet(bucket, f"{V2_PREFIX}day={day}/entity=fixtures/fixtures.parquet")
        if fixtures_df is None:
            logger.warning("day=%s: no v2 fixtures source, skipping day entirely", day)
            stats["days_with_no_fixtures_source"] += 1
            continue

        fixtures_league = _fixtures_league_series(fixtures_df, af_to_canon)
        _plan_writes_for_entity(bucket, day, "fixtures", fixtures_df, fixtures_league, "af_fixture_id", dry_run, stats)

        stats_df = _read_parquet(bucket, f"{V2_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet")
        if stats_df is not None:
            fid_to_league = _fid_to_league_from_v2_fixtures(fixtures_df, af_to_canon)
            stats_league = stats_df["af_fixture_id"].astype(str).map(fid_to_league)
            _plan_writes_for_entity(
                bucket, day, "fixture_stats", stats_df, stats_league, "af_fixture_id", dry_run, stats
            )

    logger.info("=== Summary ===")
    for k, v in stats.items():
        logger.info("  %s: %d", k, v)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview only — scan and report without writing (default: True)",
    )
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Actually write the migrated per-league / delta files",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    sys.exit(run(args.dry_run))


if __name__ == "__main__":
    main()
