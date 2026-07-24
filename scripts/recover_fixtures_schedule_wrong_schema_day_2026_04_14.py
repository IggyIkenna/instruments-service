# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after all 85 leagues verified holding the correct fixtures schema for day=2026-04-14
"""Recover the 85 `entity=fixtures_schedule` shards for `day=2026-04-14` that carry an
instrument-catalogue schema instead of real fixtures data.

**KNOWN ISSUE — DO NOT RUN --apply YET (2026-07-24)**: `_AFFECTED_LEAGUES` below is the
literal list of `league=<X>` folder names pulled directly off the 85 bad shards. A live
PROD dry-run found these strings are NOT registered UAC `api_football` canonical
league_ids under any tier (e.g. `ENGLAND_CHAMPIONSHIP` doesn't exist anywhere in
`unified-api-contracts` — the real registered id is the abbreviated `ENG_CHAMPIONSHIP`),
so `_canonical_league_id()` on a real fetch can never produce a match — the filter in
`_fetch_and_filter()` will always return 0 rows against these exact strings. A
concurrent investigation (see the issue doc's DIAG todo, slot 12) found duplicate
correctly-shaped shards already exist under some of these leagues' ABBREVIATED alias
codes for this same day, written later (2026-07-19) — the root cause + the correct
per-league target mapping are still open. Fix `_AFFECTED_LEAGUES` (or the filter logic)
to use the CURRENT canonical ids once that's resolved, then re-verify with --dry-run
before ever running --apply. The read-only bad-shard enumeration, snapshot-then-write
CAS-free pattern, and post-write verification below are otherwise ready to reuse as-is.

Context (see
``unified-trading-pm/plans/active/issues/sports_fixtures_schedule_wrong_schema_day_2026_04_14.md``):
85 `league=<L>/fixtures_schedule.parquet` shards under `day=2026-04-14` fail a
column-projection read (`af_league_id` missing) because they hold an
instrument-catalogue/registry schema (`instrument_key`, `venue`, `tick_size`, ...)
instead of fixtures data. The exact historical writer could not be pinned (see the
issue doc's DIAG todo), but a structural guard (`_assert_not_cross_domain_contamination`,
`instruments-service@b3cb6f8c`) now rejects this class of mix-up at every
`_gated_sink_write` call, so re-running the normal fixtures write path for this one
date is safe going forward.

This script: (1) snapshots each of the 85 bad objects before touching them, (2) fetches
`api_football`'s real fixtures for 2026-04-14 ONCE (a single day-level call, not
per-league — mirrors `_ensure_canonical_fixtures_for_override`'s pattern), (3) filters
the result to exactly the 85 affected canonical league_ids, (4) writes via the real
`_write_fixtures_per_league()` — now guarded — so `entity=fixtures_schedule` +
`entity=fixtures_outcomes` land correctly, (5) verifies every one of the 85 target
shards now parses with the fixtures schema, reporting any league the real fetch didn't
cover as an explicit, named gap (never silently left broken).

Usage:
  python scripts/recover_fixtures_schedule_wrong_schema_day_2026_04_14.py --dry-run
  python scripts/recover_fixtures_schedule_wrong_schema_day_2026_04_14.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import ApiKeyReloader, get_storage_client, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _canonical_league_id,
    _flatten_canonical_fixture_for_disk,
    _sports_ref_sink_for,
    _write_fixtures_per_league,
)
from instruments_service.reference_data.adapters.sports.factory import create_sports_reference_adapter

logger = logging.getLogger(__name__)

_DATE = "2026-04-14"
_PROJECT_ID = "central-element-323112"
_PREFIX = f"sports_reference/by_date/day={_DATE}/pipeline_mode=batch_api_football/entity=fixtures_schedule/"

# The 85 canonical league_ids confirmed (this session, live corpus scan) to carry the
# wrong instrument-catalogue schema for day=2026-04-14. Re-derivable via the same scan:
# list _PREFIX, try `pd.read_parquet(..., columns=["af_league_id"])` on each, collect the
# leagues whose read raises.
_AFFECTED_LEAGUES: frozenset[str] = frozenset(
    {
        "ARGENTINA_LIGA_PROFESIONAL_ARGENTINA",
        "ARGENTINA_PRIMERA_B_METROPOLITANA",
        "ARGENTINA_RESERVE_LEAGUE",
        "ARMENIA_FIRST_LEAGUE",
        "ARMENIA_PREMIER_LEAGUE",
        "ARUBA_DIVISION_DI_HONOR",
        "AUSTRIA_REGIONALLIGA_OST",
        "BANGLADESH_FEDERATION_CUP",
        "BARBADOS_PREMIER_LEAGUE",
        "BOLIVIA_PRIMERA_DIVISION",
        "BRAZIL_BRASILEIRO_U20_A",
        "BULGARIA_FIRST_LEAGUE",
        "BULGARIA_THIRD_LEAGUE_SOUTHEAST",
        "CHILE_PRIMERA_DIVISION",
        "CHINA_LEAGUE_TWO",
        "COLOMBIA_PRIMERA_B",
        "CONGO_DR_LIGUE_1",
        "CYPRUS_1_DIVISION",
        "CZECH_REPUBLIC_4_LIGA_DIVIZIE_D",
        "ECUADOR_COPA_ECUADOR",
        "EGYPT_PREMIER_LEAGUE",
        "ENGLAND_CHAMPIONSHIP",
        "ENGLAND_LEAGUE_ONE",
        "ENGLAND_LEAGUE_TWO",
        "ENGLAND_NATIONAL_LEAGUE",
        "ENGLAND_NATIONAL_LEAGUE_NORTH",
        "ENGLAND_NATIONAL_LEAGUE_SOUTH",
        "ENGLAND_NON_LEAGUE_PREMIER_ISTHMIAN",
        "ENGLAND_NON_LEAGUE_PREMIER_SOUTHERN_CENTRAL",
        "ENGLAND_NON_LEAGUE_PREMIER_SOUTHERN_SOUTH",
        "ENGLAND_PROFESSIONAL_DEVELOPMENT_LEAGUE",
        "ENGLAND_U18_PREMIER_LEAGUE_NORTH",
        "ENGLAND_U18_PREMIER_LEAGUE_SOUTH",
        "ETHIOPIA_PREMIER_LEAGUE",
        "FINLAND_SUOMEN_CUP",
        "GERMANY_OBERLIGA_BAYERN_NORD",
        "GERMANY_OBERLIGA_BAYERN_SUD",
        "GERMANY_OBERLIGA_BREMEN",
        "GERMANY_OBERLIGA_HAMBURG",
        "GERMANY_REGIONALLIGA_BAYERN",
        "GERMANY_REGIONALLIGA_NORDOST",
        "HONDURAS_LIGA_NACIONAL",
        "HUNGARY_NB_I",
        "INDIA_I_LEAGUE_2ND_DIVISION",
        "IRAQ_IRAQI_LEAGUE",
        "ISRAEL_LIGA_LEUMIT",
        "ITALY_SERIE_B",
        "JORDAN_LEAGUE",
        "KENYA_FKF_PREMIER_LEAGUE",
        "KENYA_SUPER_LEAGUE",
        "LATVIA_VIRSLIGA",
        "LIBERIA_LFA_FIRST_DIVISION",
        "LIECHTENSTEIN_CUP",
        "MACEDONIA_FIRST_LEAGUE",
        "MALTA_PREMIER_LEAGUE",
        "NETHERLANDS_U19_DIVISIE_1",
        "NIGERIA_NPFL",
        "NORWAY_3_DIVISION_GIRONE_5",
        "PANAMA_LIGA_PANAMENA_DE_FUTBOL",
        "PERU_PRIMERA_DIVISION",
        "POLAND_III_LIGA_GROUP_3",
        "PORTUGAL_LIGA_REVELACAO_U23",
        "ROMANIA_LIGA_II",
        "SAUDI_ARABIA_DIVISION_1",
        "SAUDI_ARABIA_PRO_LEAGUE",
        "SCOTLAND_CHAMPIONSHIP",
        "SCOTLAND_LEAGUE_ONE",
        "SLOVAKIA_CUP",
        "SLOVENIA_1_SNL",
        "SPAIN_PRIMERA_DIVISION_RFEF_GROUP_1",
        "SPAIN_SEGUNDA_DIVISION_RFEF_GROUP_5",
        "SWEDEN_SUPERETTAN",
        "TANZANIA_LIGI_KUU_BARA",
        "UKRAINE_U19_LEAGUE",
        "USA_US_OPEN_CUP",
        "UZBEKISTAN_SUPER_LEAGUE",
        "WORLD_AFC_CHAMPIONS_LEAGUE_ELITE",
        "WORLD_CONMEBOL_LIBERTADORES",
        "WORLD_CONMEBOL_NATIONS_LEAGUE_WOMEN",
        "WORLD_CONMEBOL_SUDAMERICANA",
        "WORLD_FRIENDLIES_WOMEN",
        "WORLD_OFC_PRO_LEAGUE",
        "WORLD_UEFA_CHAMPIONS_LEAGUE",
        "WORLD_WORLD_CUP_WOMEN_QUALIFICATION_CONCACAF",
        "WORLD_WORLD_CUP_WOMEN_QUALIFICATION_EUROPE",
    }
)


def _bad_shard_paths(bucket: str) -> dict[str, str]:
    """Return ``{canonical_league_id: blob_path}`` for the 85 affected shards
    currently present under ``_PREFIX``."""
    storage = get_storage_client(project_id=_PROJECT_ID)
    out: dict[str, str] = {}
    for blob in storage.list_blobs(bucket, prefix=_PREFIX):
        if "league=" not in blob.name:
            continue
        league = blob.name.split("league=")[1].split("/")[0]
        if league in _AFFECTED_LEAGUES:
            out[league] = blob.name
    return out


def _snapshot(bucket: str, path: str, run_ts: str) -> None:
    storage = get_storage_client(project_id=_PROJECT_ID)
    raw = storage.download_bytes(bucket, path)
    backup_path = path.replace(
        "fixtures_schedule.parquet",
        f"fixtures_schedule.{run_ts}.wrong_schema.bak.parquet",
    )
    storage.upload_bytes(bucket, backup_path, raw)
    logger.info("Snapshotted gs://%s/%s -> gs://%s/%s", bucket, path, bucket, backup_path)


async def _fetch_and_filter() -> pd.DataFrame:
    key_reloader = ApiKeyReloader(venues=["api_football"], project_id=_PROJECT_ID)
    key_reloader.start()
    try:
        api_key = key_reloader.current_keys.get("api_football")
        if not api_key:
            raise RuntimeError("No api_football API key available from Secret Manager — aborting.")
        adapter = create_sports_reference_adapter("api_football", api_key=api_key)
        logger.info("Fetching api_football fixtures for %s (single day-level call)...", _DATE)
        fx_pairs = await adapter.get_fixtures_with_raw(_DATE)
        logger.info("Fetched %d raw fixtures for %s", len(fx_pairs), _DATE)
    finally:
        key_reloader.stop()

    fx_dicts = [_flatten_canonical_fixture_for_disk(fx, _DATE, af_response=raw) for fx, raw in fx_pairs]
    fx_df = pd.DataFrame(fx_dicts)
    if "timestamp" in fx_df.columns:
        fx_df["available_at"] = pd.to_datetime(fx_df["timestamp"], utc=True, errors="coerce") - pd.Timedelta(days=7)

    fx_df["_canonical_league_id"] = fx_df["af_league_id"].apply(
        lambda v: _canonical_league_id(v) if pd.notna(v) else None
    )
    filtered = fx_df[fx_df["_canonical_league_id"].isin(_AFFECTED_LEAGUES)].drop(columns=["_canonical_league_id"])
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Fetch + report only, no snapshot/write.")
    mode.add_argument("--apply", action="store_true", help="Snapshot the bad shards then write the real fixtures.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
    bad_shards = _bad_shard_paths(bucket)
    logger.info("Bad shards currently present: %d of %d expected leagues", len(bad_shards), len(_AFFECTED_LEAGUES))
    missing_shards = _AFFECTED_LEAGUES - set(bad_shards)
    if missing_shards:
        logger.warning(
            "Expected-but-absent bad shard for %d league(s): %s", len(missing_shards), sorted(missing_shards)
        )

    filtered_df = asyncio.run(_fetch_and_filter())
    fetched_leagues = set(filtered_df["af_league_id"].apply(_canonical_league_id)) if not filtered_df.empty else set()
    not_covered = _AFFECTED_LEAGUES - fetched_leagues
    logger.info(
        "Real fetch covers %d of %d affected leagues (%d fixture rows total)",
        len(fetched_leagues),
        len(_AFFECTED_LEAGUES),
        len(filtered_df),
    )
    if not_covered:
        logger.warning(
            "NOT COVERED by today's real fetch (no fixtures returned for %s on this date) — will remain "
            "un-recovered this pass, needs its own follow-up: %s",
            _DATE,
            sorted(not_covered),
        )
    if not filtered_df.empty:
        counts = filtered_df["af_league_id"].apply(_canonical_league_id).value_counts()
        for league, n in sorted(counts.items()):
            logger.info("  %s: %d fixtures", league, n)

    if args.dry_run:
        logger.info(
            "[dry-run] Would snapshot %d bad shards then write %d fixture rows.", len(bad_shards), len(filtered_df)
        )
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    for path in bad_shards.values():
        _snapshot(bucket, path, run_ts)

    if filtered_df.empty:
        logger.error("No fixtures fetched for the affected leagues — nothing to write. Investigate before re-running.")
        return 2

    sink = _sports_ref_sink_for(bucket, _DATE, "fixtures_schedule")
    _write_fixtures_per_league(
        sink,
        filtered_df,
        _DATE,
        source_label="wrong-schema-recovery-2026-07-24",
        bucket=bucket,
    )
    logger.info(
        "Write complete. Verifying all %d target shards now parse with the fixtures schema...", len(_AFFECTED_LEAGUES)
    )

    storage = get_storage_client(project_id=_PROJECT_ID)
    still_bad: list[str] = []
    for league in sorted(_AFFECTED_LEAGUES):
        path = f"{_PREFIX}league={league}/fixtures_schedule.parquet"
        try:
            raw = storage.download_bytes(bucket, path)
            pd.read_parquet(io.BytesIO(raw), columns=["af_league_id"])
        except Exception as exc:
            still_bad.append(league)
            logger.error("STILL BAD after recovery: %s (%s)", league, exc)

    if still_bad:
        logger.error(
            "%d of %d leagues remain unrecovered (either no fixtures today or a residual write issue): %s",
            len(still_bad),
            len(_AFFECTED_LEAGUES),
            still_bad,
        )
        return 3

    logger.info(
        "All %d affected leagues now read with the correct fixtures schema for %s.", len(_AFFECTED_LEAGUES), _DATE
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
