# Epic: sports_master
# Lifecycle: ONE-OFF — closes the footystats/soccer_football_info/open_meteo
#   attempted_failed residual root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (footystats: 179 TimeoutError + 15 phantom_captured_no_parquet_at_canonical_path
#   + 11 ArrowTypeError; soccer_football_info: 10 phantom; open_meteo: 51 phantom).
# Delete-when: manifest shows 0 attempted_failed for
#   (footystats, MATCHES|PREDICTIONS|ODDS), (soccer_football_info,
#   SFI_PROGRESSIVE_STATS), (open_meteo, WEATHER).
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""sports_attempted_failed_residual_closer_2026_07_13.py — closes the live
``attempted_failed`` residual for footystats (MATCHES/PREDICTIONS/ODDS),
soccer_football_info (SFI_PROGRESSIVE_STATS), and open_meteo (WEATHER),
mirroring ``footystats_residual_closer_2026_07_12.py`` /
``sports_daily_enum_residual_closer_2026_07_12.py``.

ROOT CAUSE (diagnosed 2026-07-13, see the plan above's Progress Log):
  - footystats TimeoutError (179 rows, dates 2019-01-01..2023-01-03, 100%
    blank league_id): a generic transient date-level bulk-call timeout from
    the original historical backfill pass. footystats' live endpoint is
    confirmed working today — plain retry with backoff, no adapter change.
  - footystats ArrowTypeError (11 rows, all ODDS, 2020-2023): pyarrow
    rejected a ``pd.Timestamp`` in the ``kickoff_utc`` string column for
    NaN-fill rows — FIXED by instruments-service@a4dfa6bd
    ("serialize NaN-fill kickoff_utc to ISO string"), which landed AFTER
    these rows failed. Plain re-attempt now succeeds under the fixed code.
  - phantom_captured_no_parquet_at_canonical_path (15 footystats MATCHES/
    SEGUNDA_DIVISION + 10 SFI + 51 weather): genuine phantom rows (live
    reprobed against every UAC candidate_parquet_paths() shape — 0/76 have a
    real parquet). Already correctly stamped attempted_failed (not
    "captured" claiming a file that doesn't exist) — closing them requires a
    real re-capture, which this script drives via the normal per-date fetch
    path (now hardened with per-league shard isolation,
    instruments-service@6091ad7b / rebased 0278061).

FIX: no new adapter/writer logic needed — the SHIPPED per-date capture
functions already resolve the honest per-league answer for a given date.
This script force-refetches the EXACT residual dates per source, read live
off the manifest each run (RESUME-safe).

Usage:
  sports_attempted_failed_residual_closer_2026_07_13.py [--main-conc 8]
      [--retry-conc 4] [--max-rounds 6] [--vm-name <tag>]
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
log = logging.getLogger("sports_attempted_failed_residual_closer")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--main-conc", type=int, default=8)
    p.add_argument("--retry-conc", type=int, default=4)
    p.add_argument("--max-rounds", type=int, default=6)
    p.add_argument("--vm-name", default="sports-attempted-failed-residual-closer")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument(
        "--sources",
        default="footystats,soccer_football_info,open_meteo",
        help="Comma-separated subset of {footystats,soccer_football_info,open_meteo} to close.",
    )
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
    read_availability_index,
    resolve_bucket_name,
    validate_api_keys_for_venues,
)

from instruments_service.engine.orchestrator import (
    _fetch_footystats_matches,
    _fetch_footystats_odds,
    _fetch_footystats_predictions,
    _fetch_sfi_data,
    _fetch_weather_data,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

_FOOTYSTATS_FETCH_FN = {
    "MATCHES": _fetch_footystats_matches,
    "PREDICTIONS": _fetch_footystats_predictions,
    "ODDS": _fetch_footystats_odds,
}
_FOOTYSTATS_DATA_TYPES = tuple(_FOOTYSTATS_FETCH_FN.keys())


def _manifest_view(source: str, data_types: tuple[str, ...]) -> pd.DataFrame:
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(BUCKET)
    dt = df["data_type"].astype("string").fillna("").str.upper()
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    return df[dt.isin(data_types) & (src == source)].copy()


def footystats_failed_dates() -> dict[str, list[str]]:
    """Live per-data_type residual: attempted_failed dates for footystats."""
    fs_df = _manifest_view("footystats", _FOOTYSTATS_DATA_TYPES)
    out: dict[str, list[str]] = {}
    for data_type in _FOOTYSTATS_DATA_TYPES:
        sub = fs_df[
            (fs_df["data_type"].astype("string").str.upper() == data_type)
            & (fs_df["capture_status"] == "attempted_failed")
        ]
        out[data_type] = sorted(sub["date"].astype(str).unique())
    return out


def sfi_failed_dates() -> list[str]:
    sub = _manifest_view("soccer_football_info", ("SFI_PROGRESSIVE_STATS",))
    return sorted(sub[sub["capture_status"] == "attempted_failed"]["date"].astype(str).unique())


def weather_failed_dates() -> list[str]:
    sub = _manifest_view("open_meteo", ("WEATHER",))
    return sorted(sub[sub["capture_status"] == "attempted_failed"]["date"].astype(str).unique())


async def process_footystats(dates_by_type: dict[str, list[str]], api_key: str, conc: int, tally: dict) -> None:
    union_dates = sorted(set().union(*dates_by_type.values())) if any(dates_by_type.values()) else []
    sem = asyncio.Semaphore(conc)

    async def _one(date: str) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    for data_type in _FOOTYSTATS_DATA_TYPES:
                        force = date in set(dates_by_type[data_type])
                        if not force:
                            continue
                        fn = _FOOTYSTATS_FETCH_FN[data_type]
                        await fn(date=date, api_key=api_key, bucket=BUCKET, force=True)
                        tally[f"footystats_{data_type}"] += 1
                    tally["footystats_dates"] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(30, 3 * 2**attempt))
                        continue
                    tally["footystats_raised"] += 1
                    log.error("RAISED footystats date=%s: %s: %s", date, type(exc).__name__, str(exc)[:160])

    await asyncio.gather(*(_one(d) for d in union_dates))
    log.info(
        "[footystats] processed=%s raised=%s",
        tally["footystats_dates"],
        tally["footystats_raised"],
    )


async def process_sfi(dates: list[str], api_key: str | None, conc: int, tally: dict) -> None:
    if not api_key:
        log.error("No soccer_football_info API key resolved — skipping SFI residual close")
        return
    sem = asyncio.Semaphore(conc)

    async def _one(date: str) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    await _fetch_sfi_data(
                        date=date, api_key=api_key, bucket=BUCKET, entity_filter="SFI_PROGRESSIVE_STATS", force=True
                    )
                    tally["sfi_dates"] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(30, 3 * 2**attempt))
                        continue
                    tally["sfi_raised"] += 1
                    log.error("RAISED sfi date=%s: %s: %s", date, type(exc).__name__, str(exc)[:160])

    await asyncio.gather(*(_one(d) for d in dates))
    log.info("[sfi] processed=%s raised=%s", tally["sfi_dates"], tally["sfi_raised"])


async def process_weather(dates: list[str], api_key: str | None, conc: int, tally: dict) -> None:
    sem = asyncio.Semaphore(conc)

    async def _one(date: str) -> None:
        async with sem:
            for attempt in range(1, 4):
                try:
                    await _fetch_weather_data(date=date, bucket=BUCKET, api_key=api_key)
                    tally["weather_dates"] += 1
                    return
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(min(30, 3 * 2**attempt))
                        continue
                    tally["weather_raised"] += 1
                    log.error("RAISED weather date=%s: %s: %s", date, type(exc).__name__, str(exc)[:160])

    await asyncio.gather(*(_one(d) for d in dates))
    log.info("[weather] processed=%s raised=%s", tally["weather_dates"], tally["weather_raised"])


async def main() -> None:
    sources = {s.strip() for s in ARGS.sources.split(",") if s.strip()}
    keys = validate_api_keys_for_venues(
        venues=["footystats", "soccer_football_info", "open_meteo"], project_id=ARGS.project
    )
    log.info(
        "bucket=%s main=%s retry=%s vm=%s sources=%s", BUCKET, ARGS.main_conc, ARGS.retry_conc, ARGS.vm_name, sources
    )

    tally: dict = dict.fromkeys(
        [
            "footystats_MATCHES",
            "footystats_PREDICTIONS",
            "footystats_ODDS",
            "footystats_dates",
            "footystats_raised",
            "sfi_dates",
            "sfi_raised",
            "weather_dates",
            "weather_raised",
        ],
        0,
    )

    if "footystats" in sources:
        ft_key = keys.get("footystats")
        if not ft_key:
            log.error("No footystats API key resolved — skipping footystats residual close")
        else:
            ft_failed = footystats_failed_dates()
            for dt, dates in ft_failed.items():
                log.info("RESIDUAL footystats %-11s %d attempted_failed date(s)", dt, len(dates))
            await process_footystats(ft_failed, ft_key, ARGS.main_conc, tally)

    if "soccer_football_info" in sources:
        sfi_dates = sfi_failed_dates()
        log.info("RESIDUAL soccer_football_info SFI_PROGRESSIVE_STATS %d attempted_failed date(s)", len(sfi_dates))
        await process_sfi(sfi_dates, keys.get("soccer_football_info"), ARGS.main_conc, tally)

    if "open_meteo" in sources:
        w_dates = weather_failed_dates()
        log.info("RESIDUAL open_meteo WEATHER %d attempted_failed date(s)", len(w_dates))
        await process_weather(w_dates, keys.get("open_meteo"), ARGS.main_conc, tally)

    # Retry rounds — mirrors footystats_residual_closer_2026_07_12.py / understat pattern.
    for rnd in range(1, ARGS.max_rounds + 1):
        await asyncio.sleep(30)  # let any provider rate-limit cool
        remaining: dict[str, object] = {}
        total_remaining = 0
        if "footystats" in sources and keys.get("footystats"):
            ft_remaining = footystats_failed_dates()
            remaining["footystats"] = ft_remaining
            total_remaining += sum(len(v) for v in ft_remaining.values())
        if "soccer_football_info" in sources:
            sfi_remaining = sfi_failed_dates()
            remaining["soccer_football_info"] = sfi_remaining
            total_remaining += len(sfi_remaining)
        if "open_meteo" in sources:
            w_remaining = weather_failed_dates()
            remaining["open_meteo"] = w_remaining
            total_remaining += len(w_remaining)
        log.info("[VERIFY round=%s] attempted_failed remaining across sources: %s", rnd, total_remaining)
        if not total_remaining:
            log.info("=== ALL SOURCES RESOLVED (0 attempted_failed) ===")
            break
        if "footystats" in sources and keys.get("footystats") and any(remaining.get("footystats", {}).values()):
            await process_footystats(remaining["footystats"], keys["footystats"], ARGS.retry_conc, tally)
        if "soccer_football_info" in sources and remaining.get("soccer_football_info"):
            await process_sfi(
                remaining["soccer_football_info"], keys.get("soccer_football_info"), ARGS.retry_conc, tally
            )
        if "open_meteo" in sources and remaining.get("open_meteo"):
            await process_weather(remaining["open_meteo"], keys.get("open_meteo"), ARGS.retry_conc, tally)
    else:
        log.warning("=== MAX ROUNDS reached; residual may remain — see final verify below ===")

    # Explicit pre-exit drain (not atexit — races the event loop teardown,
    # see plans/active/issues/manifest_atexit_drain_races_asyncio_shutdown_2026_07_09.md).
    flushed = _mw.flush_all_pending_buckets()
    log.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    log.info("FINAL TALLY: %s", tally)
    if "footystats" in sources:
        for dt, dates in footystats_failed_dates().items():
            log.info("=== FOOTYSTATS RESIDUAL — %s: %d attempted_failed date(s) remain ===", dt, len(dates))
    if "soccer_football_info" in sources:
        log.info("=== SFI RESIDUAL — %d attempted_failed date(s) remain ===", len(sfi_failed_dates()))
    if "open_meteo" in sources:
        log.info("=== WEATHER RESIDUAL — %d attempted_failed date(s) remain ===", len(weather_failed_dates()))


if __name__ == "__main__":
    asyncio.run(main())
