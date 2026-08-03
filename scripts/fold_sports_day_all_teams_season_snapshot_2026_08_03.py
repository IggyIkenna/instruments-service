#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: after the fold below is confirmed applied in prod (verify via
#   --dry-run over the full season range reporting 0 remaining un-migrated seasons).
"""Fold the legacy ``day=all/entity=teams`` season-keyed team x venue snapshot into the
new UAC ``TEAMS_SEASON_SNAPSHOT`` FLAT_PER_SEASON layout.

Todo (last open item) of
``unified-trading-pm/plans/archive/2026_08/sports_day_all_teams_venues_fold_key_scheme_mismatch_2026_07_25.md``:
**RULED 2026-07-28 — Option A: add a net-new season-keyed UAC FLAT layout for TEAMS.** The
schema/routing half of that ruling shipped as ``unified-api-contracts``'s
``SportsPathLayout.FLAT_PER_SEASON`` + ``TEAMS_SEASON_SNAPSHOT`` data_type (see
``unified_api_contracts.canonical.domain.sports.gcs_paths``). This script is the second
half: the actual one-time data migration.

Source (already backed up + verified present by the earlier VENUES-parallel delete task,
see the parent issue doc's todo 1 Progress Log):
    ``gs://<sports-bucket>/sports_reference/_legacy_archive/by_date/day=all/entity=teams/teams.parquet``
    (30,069 rows, 22,241 unique (team_id, season) pairs, seasons 2019-2025, 17 columns).

Destination (new canonical layout, one file per season):
    ``gs://<sports-bucket>/sports_reference/teams/season={season}/teams.parquet``
    (built via ``unified_api_contracts.sports.candidate_parquet_paths(TEAMS_SEASON_SNAPSHOT,
    day, season=season)`` -- never a hand-rolled path string.)

Manifest registration: deliberately SKIPPED. ``ManifestWriter.record_captured``'s own
``row_key`` docstring states ``date`` is REQUIRED -- this data is genuinely season-keyed with
no date dimension at all, so forcing a fake ``date`` into row_key would repeat exactly the
"invent a fake label to fit an existing shape" anti-pattern the FLAT_PER_SEASON layout was
created to AVOID. Matches the only existing FLAT-layout precedent (VENUES,
``instruments_service.engine.orchestrator.writers._write_venues_from_teams``), which the
live writer also never registers in the manifest.

Flow per season (mirrors the delete-safety-protocol precedent used for the sibling VENUES
delete task in the same parent issue doc, applied to a pure ADD rather than a delete):
  1. Describe the destination path: if it ALREADY exists, HARD-ABORT that season only (never
     silently overwrite -- a second run of this script must be a safe no-op check, not a
     blind re-write).
  2. Write the season's rows to the destination (upload_from_file_obj -- server-side write,
     never a local temp file).
  3. Verify: re-download the destination, parse, confirm row count matches the source
     season's row count exactly.
  4. Aggregate: verify sum(rows written across all seasons) == total source row count (no
     data loss, no duplication).

Usage::

    cd instruments-service
    .venv/bin/python scripts/fold_sports_day_all_teams_season_snapshot_2026_08_03.py --dry-run
    .venv/bin/python scripts/fold_sports_day_all_teams_season_snapshot_2026_08_03.py --apply

Never runs from an operator's local machine for corpus-scale I/O (heavy-I/O rule) -- this is
a single ~780KB source object split into <10 season files, well within the shared-host
bounded-analysis budget; no VM launch needed.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from dataclasses import dataclass

import pandas as pd
from unified_api_contracts.sports import TEAMS_SEASON_SNAPSHOT, candidate_parquet_paths
from unified_trading_library import gcs_describe_object, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fold_sports_day_all_teams_season_snapshot")

_SOURCE_PATH = "sports_reference/_legacy_archive/by_date/day=all/entity=teams/teams.parquet"
_ASSET_GROUP = "sports"


@dataclass
class SeasonResult:
    season: str
    src_rows: int
    dst_path: str
    status: str  # written | already_present_skip | dry_run | aborted_mismatch


def _season_dst_path(season: str) -> str:
    paths = candidate_parquet_paths(TEAMS_SEASON_SNAPSHOT, day="1970-01-01", season=season)
    assert len(paths) == 1, f"expected exactly one FLAT_PER_SEASON candidate for season={season}, got {paths}"
    return paths[0]


def _fold_season(bucket: str, season: str, season_df: pd.DataFrame, *, apply: bool) -> SeasonResult:
    dst_path = _season_dst_path(season)
    dst_uri = f"gs://{bucket}/{dst_path}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    src_rows = len(season_df)

    existing = gcs_describe_object(dst_uri)
    if existing is not None:
        logger.warning(
            "season=%s: destination %s ALREADY EXISTS (size=%s) — skipping to avoid overwrite; "
            "re-run is then a safe no-op for this season.",
            season,
            dst_path,
            existing.size,
        )
        return SeasonResult(season=season, src_rows=src_rows, dst_path=dst_path, status="already_present_skip")

    if not apply:
        logger.info("season=%s: DRY-RUN — would write %d rows to %s", season, src_rows, dst_path)
        return SeasonResult(season=season, src_rows=src_rows, dst_path=dst_path, status="dry_run")

    storage = get_storage_client()
    buf = io.BytesIO()
    season_df.to_parquet(buf, index=False)
    buf.seek(0)
    storage.upload_from_file_obj(bucket, dst_path, buf)  # type: ignore[attr-defined]
    logger.info("season=%s: WROTE %d rows to %s", season, src_rows, dst_path)

    verify_meta = gcs_describe_object(dst_uri)
    if verify_meta is None:
        logger.error("season=%s: HARD-ABORT — destination missing immediately after write.", season)
        return SeasonResult(season=season, src_rows=src_rows, dst_path=dst_path, status="aborted_mismatch")
    verify_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, dst_path)))
    if len(verify_df) != src_rows:
        logger.error(
            "season=%s: HARD-ABORT — row count mismatch after write (wrote %d, read back %d).",
            season,
            src_rows,
            len(verify_df),
        )
        return SeasonResult(season=season, src_rows=src_rows, dst_path=dst_path, status="aborted_mismatch")
    logger.info("season=%s: VERIFIED %d rows readable at %s", season, len(verify_df), dst_path)
    return SeasonResult(season=season, src_rows=src_rows, dst_path=dst_path, status="written")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="classify + would-write only, never writes")
    mode.add_argument("--apply", action="store_true", help="perform the real per-season writes")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=_ASSET_GROUP)
    storage = get_storage_client()

    if not storage.blob_exists(bucket, _SOURCE_PATH):
        logger.error("HARD-ABORT: source archive %s not found in bucket %s.", _SOURCE_PATH, bucket)
        return 2
    src_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, _SOURCE_PATH)))
    logger.info("source: %d rows, %d columns, read from %s", len(src_df), len(src_df.columns), _SOURCE_PATH)
    if "season" not in src_df.columns:
        logger.error("HARD-ABORT: source parquet has no 'season' column — cannot split by season.")
        return 2

    total_src_rows = len(src_df)
    results: list[SeasonResult] = []
    for season, season_df in src_df.groupby("season", sort=True):
        results.append(_fold_season(bucket, str(season), season_df, apply=args.apply))

    aborted = [r.season for r in results if r.status == "aborted_mismatch"]
    written_rows = sum(r.src_rows for r in results if r.status in ("written", "already_present_skip"))
    logger.info(
        "=== VERDICT fold-sports-day-all-teams-season-snapshot: seasons=%d written=%d skipped=%d "
        "dry_run=%d aborted=%d total_src_rows=%d accounted_rows=%d ===",
        len(results),
        sum(1 for r in results if r.status == "written"),
        sum(1 for r in results if r.status == "already_present_skip"),
        sum(1 for r in results if r.status == "dry_run"),
        len(aborted),
        total_src_rows,
        written_rows,
    )
    if aborted:
        logger.error("HARD-ABORT on %d season(s): %s — inspect before re-running.", len(aborted), aborted)
        return 3
    if args.apply and written_rows != total_src_rows:
        logger.error(
            "HARD-ABORT: accounted rows (%d) != source rows (%d) — data loss or double-count detected.",
            written_rows,
            total_src_rows,
        )
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
