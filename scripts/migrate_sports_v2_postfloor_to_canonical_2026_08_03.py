#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + orphan-sweep flips these 16 days to A_canonical
"""Migrate-forward the sports v2-staging post-floor rows into canonical per-league fixtures.

Source of record: /plans/active/issues/sports_legacy_duplicate_triage_2026_07_22.md § 4, § 7 todo 2.
The 5-part delete-safety proof found the v2 staging tree (``sports_reference_v2/by_date/``)
holds the ONLY (or far more complete) copy of fixtures/fixture_stats data for 16 post-floor
days (2024-12-24 .. 2026-04-20) — the 2026-04-28 bare-file cutover never re-ran the modern
per-league fan-out for these dates, so canonical (``sports_reference/by_date/``) holds only a
tiny fraction of the rows. This is an ADDITIVE migrate-forward (never deletes the v2 source,
never deletes/overwrites a canonical row that already exists) — no delete-safety gate applies.

For each post-floor v2 day:
  1. entity=fixtures — league comes directly from the row's own af_league_id (via UAC
     get_prediction_leagues()), same mapping migrate_sports_per_league.py's
     _build_af_fixture_to_league() builds from canonical fixtures data.
  2. entity=fixture_stats — no league column; joined via af_fixture_id using the day's
     fixtures mapping built in step 1 (mirrors migrate_sports_per_league.py's
     PER_FIXTURE_ENTITIES join pattern).
  3. Per league, merge with any existing canonical per-league file: union rows, dedupe on
     af_fixture_id keeping the canonical row on conflict (canonical is presumed authoritative
     where it already exists; the legacy row only fills genuinely missing fixture_ids).

Byte-identical ``pipeline_mode=batch_api_football`` duplicate copies exist alongside the bare
v2 objects (a separate, already-tracked finding — triage doc § 7 todo 6); this script reads
whichever variant exists first (content is identical either way) and does not touch that
duplication itself.

Usage:
    python3 scripts/migrate_sports_v2_postfloor_to_canonical_2026_08_03.py --bucket instruments-store-sports-prd-central-element-323112 --dry-run
    python3 scripts/migrate_sports_v2_postfloor_to_canonical_2026_08_03.py --bucket instruments-store-sports-prd-central-element-323112 --no-dry-run
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

V2_PREFIX = "sports_reference_v2/by_date/"
CANONICAL_PREFIX = "sports_reference/by_date/"
FLOOR_DAY = "2020-06-06"
ENTITIES = ("fixtures", "fixture_stats")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="GCS bucket name (sports reference bucket)")
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
        help="Actually write merged per-league canonical files",
    )
    return parser.parse_args()


def _discover_postfloor_days(bucket: storage.Bucket) -> list[str]:
    day_blobs = bucket.list_blobs(prefix=V2_PREFIX, delimiter="/")
    list(day_blobs)
    dates = sorted(p.rstrip("/").split("=")[-1] for p in day_blobs.prefixes)
    return [d for d in dates if d >= FLOOR_DAY]


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


def _read_v2_entity(bucket: storage.Bucket, day: str, entity: str) -> pd.DataFrame | None:
    """Read the v2 staging object for (day, entity) — bare path first, falling back to the
    byte-identical pipeline_mode=batch_api_football-tagged twin (§ 7 todo 6)."""
    bare_path = f"{V2_PREFIX}day={day}/entity={entity}/{entity}.parquet"
    df = _read_parquet(bucket, bare_path)
    if df is not None:
        return df
    prefix = f"{V2_PREFIX}day={day}/pipeline_mode="
    blobs = list(bucket.list_blobs(prefix=prefix))
    tagged_path = next(
        (b.name for b in blobs if b.name.endswith(f"entity={entity}/{entity}.parquet")),
        None,
    )
    if tagged_path is None:
        return None
    return _read_parquet(bucket, tagged_path)


def _af_league_to_canonical() -> dict[int, str]:
    mapping: dict[int, str] = {}
    for league_def in get_prediction_leagues():
        if league_def.api_football_id is not None:
            mapping[league_def.api_football_id] = league_def.league_id
    return mapping


def _merge_into_canonical_league_file(
    bucket: storage.Bucket,
    canonical_path: str,
    legacy_rows: pd.DataFrame,
    dry_run: bool,
) -> tuple[int, int]:
    """Union legacy_rows into the existing canonical per-league file (if any), deduped on
    af_fixture_id with the canonical row winning on conflict. Returns (rows_before, rows_added)."""
    existing = _read_parquet(bucket, canonical_path)
    rows_before = 0 if existing is None else len(existing)
    if existing is not None and not existing.empty:
        existing_ids = set(existing["af_fixture_id"].astype(str))
        new_rows = legacy_rows[~legacy_rows["af_fixture_id"].astype(str).isin(existing_ids)]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = legacy_rows.reset_index(drop=True)
        new_rows = legacy_rows
    rows_added = len(new_rows)
    if rows_added and not dry_run:
        _write_parquet(bucket, canonical_path, merged)
    return rows_before, rows_added


def _migrate_day(bucket: storage.Bucket, day: str, dry_run: bool) -> dict[str, int]:
    stats = {
        "fixtures_leagues_touched": 0,
        "fixtures_rows_added": 0,
        "fixture_stats_leagues_touched": 0,
        "fixture_stats_rows_added": 0,
        "unmapped_rows": 0,
    }

    fixtures_df = _read_v2_entity(bucket, day, "fixtures")
    if fixtures_df is None or fixtures_df.empty:
        logger.warning("day=%s: no v2 fixtures object found, skipping day entirely", day)
        return stats
    if "af_league_id" not in fixtures_df.columns:
        logger.warning("day=%s: v2 fixtures has no af_league_id column, skipping", day)
        return stats

    af_league_to_canonical = _af_league_to_canonical()
    fixtures_df = fixtures_df.copy()
    fixtures_df["_league"] = fixtures_df["af_league_id"].map(af_league_to_canonical)
    has_league = fixtures_df["_league"].notna()
    stats["unmapped_rows"] += int((~has_league).sum())
    mapped_fixtures = fixtures_df[has_league]

    fid_to_league: dict[str, str] = dict(
        zip(
            mapped_fixtures["af_fixture_id"].astype(str),
            mapped_fixtures["_league"],
            strict=False,
        )
    )

    for lid, league_df in mapped_fixtures.groupby("_league"):
        clean_df = league_df.drop(columns=["_league"])
        canonical_path = f"{CANONICAL_PREFIX}day={day}/entity=fixtures/league={lid}/fixtures.parquet"
        _, added = _merge_into_canonical_league_file(bucket, canonical_path, clean_df, dry_run)
        if added:
            stats["fixtures_leagues_touched"] += 1
            stats["fixtures_rows_added"] += added
        logger.info(
            "day=%s entity=fixtures league=%s: %d legacy rows, %d newly added%s",
            day,
            lid,
            len(clean_df),
            added,
            " [DRY RUN]" if dry_run else "",
        )

    fixture_stats_df = _read_v2_entity(bucket, day, "fixture_stats")
    if fixture_stats_df is None or fixture_stats_df.empty:
        logger.info("day=%s: no v2 fixture_stats object, entity skipped", day)
        return stats
    if "af_fixture_id" not in fixture_stats_df.columns:
        logger.warning("day=%s: v2 fixture_stats has no af_fixture_id column, skipping", day)
        return stats

    fixture_stats_df = fixture_stats_df.copy()
    fixture_stats_df["_league"] = fixture_stats_df["af_fixture_id"].astype(str).map(fid_to_league)
    has_league = fixture_stats_df["_league"].notna()
    stats["unmapped_rows"] += int((~has_league).sum())
    mapped_stats = fixture_stats_df[has_league]

    for lid, league_df in mapped_stats.groupby("_league"):
        clean_df = league_df.drop(columns=["_league"])
        canonical_path = f"{CANONICAL_PREFIX}day={day}/entity=fixture_stats/league={lid}/fixture_stats.parquet"
        _, added = _merge_into_canonical_league_file(bucket, canonical_path, clean_df, dry_run)
        if added:
            stats["fixture_stats_leagues_touched"] += 1
            stats["fixture_stats_rows_added"] += added
        logger.info(
            "day=%s entity=fixture_stats league=%s: %d legacy rows, %d newly added%s",
            day,
            lid,
            len(clean_df),
            added,
            " [DRY RUN]" if dry_run else "",
        )

    return stats


def run(args: argparse.Namespace) -> None:
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    logger.info("Mode: %s", "DRY RUN" if args.dry_run else "LIVE")

    days = _discover_postfloor_days(bucket)
    logger.info("Found %d post-floor v2 staging days in gs://%s/%s", len(days), args.bucket, V2_PREFIX)
    if not days:
        logger.info("No post-floor v2 days found — nothing to do.")
        return

    totals = {
        "fixtures_leagues_touched": 0,
        "fixtures_rows_added": 0,
        "fixture_stats_leagues_touched": 0,
        "fixture_stats_rows_added": 0,
        "unmapped_rows": 0,
    }
    for day in days:
        stats = _migrate_day(bucket, day, args.dry_run)
        for k, v in stats.items():
            totals[k] += v

    logger.info("=== TOTALS across %d post-floor days ===", len(days))
    for key, val in totals.items():
        logger.info("  %s: %d", key, val)


def main() -> None:
    args = _parse_args()
    run(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
