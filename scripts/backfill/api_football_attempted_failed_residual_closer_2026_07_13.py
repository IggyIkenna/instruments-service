# Epic: sports_master
# Lifecycle: ONE-OFF — closes the historical api_football `attempted_failed`
#   residual root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (todo "api_football: fix root causes + re-attempt failed cells").
#   The 4 underlying bug classes were already fixed in code
#   (instruments-service@9ce3450e). This script re-drives the ~2,796
#   PRE-EXISTING stale (date, league_id, data_type) cells left over from
#   before that fix (INJURIES/FIXTURES/TEAMS/FIXTURE_STATS/FIXTURE_EVENTS/
#   FIXTURE_LINEUPS/PLAYER_STATS) so they get a genuine live re-attempt under
#   the fixed code.
# Delete-when: manifest shows 0 attempted_failed for
#   (api_football, INJURIES|FIXTURES|TEAMS|FIXTURE_STATS|FIXTURE_EVENTS|
#   FIXTURE_LINEUPS|PLAYER_STATS).
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""api_football_attempted_failed_residual_closer_2026_07_13.py — closes the
live ``attempted_failed`` residual for api_football, mirroring
``sports_attempted_failed_residual_closer_2026_07_13.py`` (footystats/SFI/
open_meteo) and ``backfill_teams_61_leagues_2026_07_13.py``.

SCOPE (live-verified 2026-07-13, unchanged from the plan's baseline): 3,257
`attempted_failed` rows for `source=api_football`, of which 461 carry a
BLANK `data_type` (the `process_completeness.py` shard-completeness-gate
leak, fixed going-forward in 9ce3450e — these 461 rows have no `data_type`/
`league_id` identity to re-drive against; they are a separate manifest
CLEANUP item, out of scope here, already flagged in the plan's
"IMPLEMENTATION dispatch" Progress Log entry). The remaining 2,796 rows DO
map to a real (date, data_type[, league_id]) cell and are what this script
re-attempts:

  - INJURIES (1,946 rows, date-wide — no per-league grain): re-driven via
    ``_fetch_sports_reference_data(entities_to_fetch=["INJURIES"])``.
  - TEAMS (24 rows, per-league) + FIXTURE_STATS/FIXTURE_EVENTS/
    FIXTURE_LINEUPS/PLAYER_STATS (161 rows total, per-league per-fixture):
    same function, entities_to_fetch scoped to whatever's stale that date.
    These 6 "reference-entity" data_types self-manifest via the module's own
    ``_AfManifestHooks`` (record_captured/record_failed/record_empty) when a
    real ``ManifestWriter`` is passed in — confirmed by reading
    ``sports_reference_core.py``/``sports_reference_fixtures.py`` in full.
  - FIXTURES (665 rows, ALL per-league): this data_type is written by the
    main URDI/instruments pipeline (not the sports-reference entity fetch),
    so it's re-driven via the top-level ``process_instruments()`` — the SAME
    production entry point the daily CLI/VM job calls — scoped to
    ``venue_override=["API_FOOTBALL"]`` + ``sports_entity_filter="FIXTURES"``
    + ``league_filter=<only the leagues that actually failed on that date>``.

    IMPORTANT (root-caused live via a --limit-dates smoke test this session,
    see the plan's Progress Log): ``redo_all=True`` must NOT be used here.
    ``_freshness_preflight`` short-circuits to ``missing_entities=[]`` whenever
    ``redo_all=True`` — and ``_fetch_sports_reference_block`` then reads
    ``entities_to_fetch=missing_entities if missing_entities else None`` as
    ``None`` ("fetch everything"), silently ignoring ``sports_entity_filter``
    entirely. A first attempt with ``redo_all=True`` on ONE date triggered
    "406 fixtures x 4 entities = 1624 calls" (full per-fixture enrichment for
    the WHOLE day across every league) and hit provider rate-limits within 2
    seconds — exactly the "blind full re-scan" this script must avoid. The
    fix: ``redo_all=False`` (the default) + explicit
    ``sports_entity_filter="FIXTURES"``. This routes through
    ``_build_expected_entities`` (only reachable when NOT ``redo_all``), which
    narrows ``expected=["FIXTURES"]`` — and since ``"FIXTURES"`` is in
    ``_SPORTS_PER_LEAGUE_ENTITIES``, the per-league deferred-freshness path
    fires unconditionally (never coarse-skipped by another league's fresh
    row), while stage 7's ``_fetch_sports_reference_block`` receives
    ``missing_entities=["FIXTURES"]`` — which matches neither a core nor a
    per-fixture entity name inside ``_fetch_sports_reference_data``, so ZERO
    extra reference/enrichment calls fire. Root cause of the 665 rows was a
    false-positive in ``_fixtures_fetch_failed`` on genuinely-zero-fixture
    days (fixed in 9ce3450e) — re-running the date now correctly resolves to
    ``empty_confirmed(EXPECTED_NO_FIXTURE)`` instead of re-failing.

Dates needing BOTH a FIXTURES fix AND a reference-entity fix (12 of them)
are handled ONCE via ``process_instruments()`` only (stage 7 of that
pipeline already re-drives sports-reference entities for the same date, so a
second, separate ``_fetch_sports_reference_data`` call would be a redundant
double-fetch).

Usage:
  api_football_attempted_failed_residual_closer_2026_07_13.py [--main-conc 6]
      [--fixtures-conc 3] [--max-rounds 4] [--vm-name <tag>] [--limit-dates N]
      [--dry-run]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_attempted_failed_residual_closer")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--main-conc", type=int, default=6, help="Concurrency for sports-reference-entity calls")
    p.add_argument("--fixtures-conc", type=int, default=3, help="Concurrency for full process_instruments() calls")
    p.add_argument("--retry-conc", type=int, default=3)
    p.add_argument("--max-rounds", type=int, default=4)
    p.add_argument("--vm-name", default="api-football-attempted-failed-residual-closer")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument(
        "--limit-dates",
        type=int,
        default=0,
        help="Restrict to the first N dates per bucket (smoke test only).",
    )
    p.add_argument("--dry-run", action="store_true", help="Characterize + plan only, no API calls / writes")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

import pandas as pd
import unified_trading_library.manifest_writer as _mw
from unified_trading_library import (
    ManifestWriter,
    read_availability_index,
    resolve_bucket_name,
    validate_api_keys_for_venues,
)

from instruments_service.engine.orchestrator import (
    _fetch_sports_reference_data,
    process_instruments,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
SOURCE = "api_football"
REF_ENTITY_TYPES = ("INJURIES", "TEAMS", "FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS", "PLAYER_STATS")


def _live_failed(bucket: str) -> pd.DataFrame:
    """Live (uncached) read of the api_football attempted_failed slice."""
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(bucket)
    af = df[(df.get("source", "") == SOURCE) & (df["capture_status"] == "attempted_failed")].copy()
    af["data_type"] = af["data_type"].fillna("").astype(str)
    return af[af["data_type"] != ""].copy()  # blank data_type = out-of-scope cleanup item, see module docstring


def _plan(af: pd.DataFrame) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Return (fixtures_plan, ref_plan): date -> [league_ids] / date -> [entities]."""
    fx = af[af["data_type"] == "FIXTURES"]
    fixtures_plan: dict[str, list[str]] = {}
    for d, grp in fx.groupby("date"):
        fixtures_plan[str(d)] = sorted({str(x) for x in grp["league_id"].dropna().unique() if str(x)})

    ref = af[af["data_type"].isin(REF_ENTITY_TYPES)]
    ref_plan: dict[str, list[str]] = {}
    for d, grp in ref.groupby("date"):
        ref_plan[str(d)] = sorted(set(grp["data_type"].unique()))
    # Dates with BOTH a FIXTURES issue and a ref-entity issue: process_instruments()
    # (stage 7) already re-drives sports-reference entities for that date, so drop
    # them from the separate ref-only plan to avoid a redundant double-fetch.
    for d in list(ref_plan):
        if d in fixtures_plan:
            del ref_plan[d]
    return fixtures_plan, ref_plan


async def _run_fixtures(fixtures_plan: dict[str, list[str]], api_key: str, conc: int, tally: dict) -> None:
    sem = asyncio.Semaphore(conc)

    async def _one(date: str, leagues: list[str]) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    # redo_all MUST stay False here — see the module docstring's
                    # "IMPORTANT" note. sports_entity_filter="FIXTURES" alone
                    # (only honoured when redo_all=False) is what keeps this call
                    # scoped to just FIXTURES, with zero stage-7 side effects.
                    await process_instruments(
                        date=date,
                        asset_groups=["SPORTS"],
                        redo_all=False,
                        api_keys={"api_football": api_key},
                        venue_override=["API_FOOTBALL"],
                        mode="batch",
                        sports_entity_filter="FIXTURES",
                        league_filter=leagues,
                    )
                    tally["fixtures_dates"] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(30, 3 * 2**attempt))
                        continue
                    tally["fixtures_raised"] += 1
                    log.error(
                        "RAISED fixtures date=%s leagues=%s: %s: %s", date, leagues, type(exc).__name__, str(exc)[:200]
                    )

    await asyncio.gather(*(_one(d, lg) for d, lg in fixtures_plan.items()))
    log.info("[fixtures] processed=%s raised=%s", tally["fixtures_dates"], tally["fixtures_raised"])


async def _run_ref(ref_plan: dict[str, list[str]], api_key: str, conc: int, tally: dict) -> None:
    sem = asyncio.Semaphore(conc)

    async def _one(date: str, entities: list[str]) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
                    await _fetch_sports_reference_data(
                        date=date,
                        api_key=api_key,
                        bucket=BUCKET,
                        entities_to_fetch=entities,
                        manifest=manifest,
                        redo_all=True,
                    )
                    tally["ref_dates"] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(30, 3 * 2**attempt))
                        continue
                    tally["ref_raised"] += 1
                    log.error(
                        "RAISED ref date=%s entities=%s: %s: %s", date, entities, type(exc).__name__, str(exc)[:200]
                    )

    await asyncio.gather(*(_one(d, ents) for d, ents in ref_plan.items()))
    log.info("[ref] processed=%s raised=%s", tally["ref_dates"], tally["ref_raised"])


async def main() -> None:
    af = _live_failed(BUCKET)
    log.info("LIVE api_football attempted_failed (non-blank data_type): %d rows", len(af))
    log.info("by data_type:\n%s", af["data_type"].value_counts().to_string())

    fixtures_plan, ref_plan = _plan(af)
    log.info(
        "PLAN: %d FIXTURES dates (%d total league-cells), %d ref-only dates",
        len(fixtures_plan),
        sum(len(v) for v in fixtures_plan.values()),
        len(ref_plan),
    )

    if ARGS.limit_dates:
        fixtures_plan = dict(list(fixtures_plan.items())[: ARGS.limit_dates])
        ref_plan = dict(list(ref_plan.items())[: ARGS.limit_dates])
        log.info(
            "--limit-dates %d: restricting to %d fixtures dates, %d ref dates",
            ARGS.limit_dates,
            len(fixtures_plan),
            len(ref_plan),
        )

    if ARGS.dry_run:
        print(f"\nDRY-RUN: {len(fixtures_plan)} FIXTURES dates, {len(ref_plan)} ref-only dates — no API calls / writes")
        return

    keys = validate_api_keys_for_venues(venues=["api_football"], project_id=ARGS.project)
    api_key = keys.get("api_football")
    if not api_key:
        log.error("No api_football key resolved from Secret Manager — aborting")
        sys.exit(2)

    tally: dict = dict.fromkeys(
        ["fixtures_dates", "fixtures_raised", "ref_dates", "ref_raised"],
        0,
    )

    await _run_fixtures(fixtures_plan, api_key, ARGS.fixtures_conc, tally)
    await _run_ref(ref_plan, api_key, ARGS.main_conc, tally)

    # Retry rounds — mirrors sports_attempted_failed_residual_closer_2026_07_13.py.
    for rnd in range(1, ARGS.max_rounds + 1):
        await asyncio.sleep(30)  # let any provider rate-limit cool
        af_now = _live_failed(BUCKET)
        fixtures_plan, ref_plan = _plan(af_now)
        log.info(
            "[VERIFY round=%s] fixtures_dates=%d ref_dates=%d attempted_failed(non-blank) rows remaining=%d",
            rnd,
            len(fixtures_plan),
            len(ref_plan),
            len(af_now),
        )
        if not fixtures_plan and not ref_plan:
            log.info("=== ALL RE-DRIVABLE api_football CELLS RESOLVED ===")
            break
        if fixtures_plan:
            await _run_fixtures(fixtures_plan, api_key, ARGS.retry_conc, tally)
        if ref_plan:
            await _run_ref(ref_plan, api_key, ARGS.retry_conc, tally)
    else:
        log.warning("=== MAX ROUNDS reached; residual may remain — see final tally below ===")

    # Explicit pre-exit drain (not atexit — races the event loop teardown,
    # see plans/active/issues/manifest_atexit_drain_races_asyncio_shutdown_2026_07_09.md).
    flushed = _mw.flush_all_pending_buckets()
    log.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    log.info("FINAL TALLY: %s", tally)
    af_final = _live_failed(BUCKET)
    log.info("=== FINAL api_football attempted_failed (non-blank data_type) remaining: %d ===", len(af_final))
    if len(af_final):
        log.info("remaining by data_type:\n%s", af_final["data_type"].value_counts().to_string())


if __name__ == "__main__":
    asyncio.run(main())
