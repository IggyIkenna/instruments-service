# Epic: sports_master
# Lifecycle: ONE-OFF — closes the open_meteo/soccer_football_info/transfermarkt
#   blank-error_reason expected_unattempted residual seeded by the v2 sports
#   enumerator's daily forward-poll (item #6, sports_p2_history_reference_and_odds).
# Delete-when: manifest shows 0 blank-error_reason expected_unattempted rows for
#   (open_meteo, WEATHER) / (soccer_football_info, SFI_PROGRESSIVE_STATS) /
#   (transfermarkt, PLAYER_VALUES).
# SSOT: plans/active/sports_p2_history_reference_and_odds_2015_to_present_2026_06_27.md (item #6)
"""sports_daily_enum_residual_closer_2026_07_12.py — closes the daily-forward-
poll blank-``error_reason`` ``expected_unattempted`` residual for weather, SFI,
and transfermarkt, mirroring ``understat_eu_residual_closer_2026_07_08.py`` /
``footystats_residual_closer_2026_07_12.py``.

ROOT CAUSE (diagnosed 2026-07-12, re-verifying item #6): the same v2 sports
enumerator LEAGUE-GRAIN bug already root-caused for understat (2026-07-08) also
seeds bare ``expected_unattempted`` rows (blank reason) for open_meteo, SFI, and
transfermarkt — all three sources' residual rows share IDENTICAL ``written_at``
timestamps (~01:30 UTC daily), confirming one enumerator run seeds all of them
in the same pass. The per-source calendar-guard fix shipped 2026-07-09
(``instruments-service@920b303``) stops NEW rows from staying source-blind /
un-written, but does not retroactively resolve rows the enumerator already
seeded before a real per-source fetch ever revisits that date.

FIX: no new fixture-calendar logic needed — the SHIPPED per-date capture
functions (``_fetch_weather_data`` / ``_fetch_sfi_data`` /
``_fetch_transfermarkt_data``) already resolve the honest per-league answer for
a given date (real capture, or a typed ``record_empty``/``record_expected_empty``
via their own season-window / transfer-window guards). This script just
force-refetches the EXACT residual dates per source, read live off the
manifest each run (RESUME-safe).

Usage:
  sports_daily_enum_residual_closer_2026_07_12.py [--conc 6] [--project central-element-323112]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date as date_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("sports_daily_enum_residual_closer")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--conc", type=int, default=6)
    p.add_argument("--vm-name", default="sports-daily-enum-residual-closer")
    p.add_argument("--project", default="central-element-323112")
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

import unified_trading_library.manifest_writer as _mw
from unified_trading_library import (
    read_availability_index,
    resolve_bucket_name,
    validate_api_keys_for_venues,
)

from instruments_service.engine.orchestrator import (
    _fetch_sfi_data,
    _fetch_transfermarkt_data,
    _fetch_weather_data,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

_SOURCES = {
    "open_meteo": "WEATHER",
    "soccer_football_info": "SFI_PROGRESSIVE_STATS",
    "transfermarkt": "PLAYER_VALUES",
}


def _manifest_view():
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(BUCKET)
    return df[df["data_type"].isin(_SOURCES.values())].copy()


def blank_reason_eu_dates() -> dict[str, list[str]]:
    """The live residual per source: expected_unattempted + blank error_reason."""
    df = _manifest_view()
    dedup_keys = ["data_type", "league_id", "date", "venue"]
    out: dict[str, list[str]] = {}
    for source, data_type in _SOURCES.items():
        sub = df[(df["data_type"] == data_type) & (df["source"] == source)]
        deduped = sub.sort_values("written_at").drop_duplicates(subset=dedup_keys, keep="last")
        reason = deduped["error_reason"].astype("string").fillna("")
        mask = (deduped["capture_status"] == "expected_unattempted") & (reason == "")
        out[source] = sorted(deduped.loc[mask, "date"].astype(str).unique())
    return out


async def _close_weather(date: str, api_key: str | None) -> None:
    await _fetch_weather_data(date=date, bucket=BUCKET, api_key=api_key)


async def _close_sfi(date: str, api_key: str) -> None:
    await _fetch_sfi_data(date=date, api_key=api_key, bucket=BUCKET, entity_filter="SFI_PROGRESSIVE_STATS", force=True)


async def _close_transfermarkt(date: str, api_key: str) -> None:
    day = date_type.fromisoformat(date)
    season = day.year if day.month >= 8 else day.year - 1
    # league_filter=None lets the function's own per-league transfer-window
    # guard resolve every expected league honestly for THIS historical date
    # (not "today's" trigger set) — the same real per-date capture path the
    # daily driver uses, just without the today-only league restriction.
    await _fetch_transfermarkt_data(
        date=date,
        api_key=api_key,
        bucket=BUCKET,
        entity_filter="PLAYER_VALUES",
        league_filter=None,
        season=season,
        force=True,
    )


async def process_source(
    source: str, dates: list[str], api_key: str | None, sem: asyncio.Semaphore, tally: dict
) -> None:
    fn = {"open_meteo": _close_weather, "soccer_football_info": _close_sfi, "transfermarkt": _close_transfermarkt}[
        source
    ]

    async def _one(d: str) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    await fn(d, api_key)
                    tally[source] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(20, 3 * 2**attempt))
                        continue
                    tally[f"{source}_raised"] += 1
                    log.error("RAISED source=%s date=%s: %s: %s", source, d, type(exc).__name__, str(exc)[:160])

    await asyncio.gather(*(_one(d) for d in dates))


async def main() -> None:
    log.info("bucket=%s conc=%s vm=%s", BUCKET, ARGS.conc, ARGS.vm_name)
    todo = blank_reason_eu_dates()
    for src, dates in todo.items():
        log.info("RESIDUAL %-22s %s blank-reason date(s): %s", src, len(dates), dates)

    keys = validate_api_keys_for_venues(
        venues=["open_meteo", "soccer_football_info", "transfermarkt"], project_id=ARGS.project
    )
    for src in ("soccer_football_info", "transfermarkt"):
        if todo.get(src) and not keys.get(src):
            log.error("Missing API key for %s — cannot close its residual this run", src)
            todo[src] = []

    sem = asyncio.Semaphore(ARGS.conc)
    tally: dict[str, int] = dict.fromkeys(_SOURCES, 0)
    tally.update({f"{s}_raised": 0 for s in _SOURCES})

    for source, dates in todo.items():
        if not dates:
            continue
        await process_source(source, dates, keys.get(source), sem, tally)

    log.info("PASS COMPLETE: %s", tally)

    # Explicit pre-exit drain (not atexit — races the asyncio loop teardown,
    # see plans/active/issues/manifest_atexit_drain_races_asyncio_shutdown_2026_07_09.md).
    flushed = _mw.flush_all_pending_buckets()
    log.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    remaining = blank_reason_eu_dates()
    for src, dates in remaining.items():
        log.info("=== %s: %s blank-reason date(s) remain ===", src, len(dates))


if __name__ == "__main__":
    asyncio.run(main())
