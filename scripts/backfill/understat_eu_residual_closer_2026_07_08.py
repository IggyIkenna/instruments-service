# Epic: sports_master
# Lifecycle: ONE-OFF — closes the understat XG/XG_SHOTS blank-error_reason
#   expected_unattempted residual (item #4, sports_p2_history_reference_and_odds).
# Delete-when: manifest shows 0 blank-error_reason expected_unattempted rows for
#   (understat, XG|XG_SHOTS, big-5 leagues).
# SSOT: plans/active/sports_p2_history_reference_and_odds_2015_to_present_2026_06_27.md (item #4)
#       plans/active/issues/sports_is_manifest_eu_regression_overwrite_2026_06_29.md (last todo)
"""understat_eu_residual_closer_2026_07_08.py — force-refetch the exact dates
the v2 sports enumerator (``_enumerate_v2_sports``) seeded a blank-reason
``expected_unattempted`` row for, on the big-5 native understat leagues.

ROOT CAUSE (diagnosed 2026-07-08, slot-2 + this session): the enumerator is
LEAGUE-GRAIN, not fixture-grain — for every alive (league, date) cell not
already present in the manifest it seeds a bare ``expected_unattempted`` with
NO reason, regardless of whether the league actually played that day. Normal
daily forward-poll only re-visits recent dates, so any such seed for a date
outside that rolling window is never revisited by ordinary traffic.
``understat_bulk_backfill.py`` (the sibling driver) does not touch these
dates either — its own ``enumerate_dates()`` only contains dates understat's
``getLeagueData`` marks ``isResult=true``, so a residual date that ISN'T a
real fixture day for ANY big-5 league never enters its date set.

FIX: this script does not need its own fixture-calendar logic — the SHIPPED
per-date capture path already resolves the real answer per (date, league):
``_fetch_understat_xg`` / ``_run_understat_shots_date`` (force=True) call the
adapter for that exact date and either (a) capture real data for a league
with a genuine fixture, or (b) ``record_empty(reason=EXPECTED_NO_FIXTURE)``
for a league with none — converting the blank-reason ``expected_unattempted``
into a correctly-typed terminal state either way. So we just need the RIGHT
date set: read the live manifest for the current blank-reason residual
(instead of guessing a season range), and force-refetch exactly those dates.

RESUME: re-reads the manifest each invocation, so re-running only touches
whatever residual remains (rows already resolved this era are absent from
the fresh read).

SELF-HEALING: after the main pass, re-checks for attempted_failed on the
touched dates and retries (lower concurrency + cooldown) until zero remain
or --max-rounds, mirroring understat_bulk_backfill.py.

Usage:
  understat_eu_residual_closer_2026_07_08.py [--main-conc 10] [--retry-conc 5]
      [--max-rounds 6] [--vm-name <tag>]
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
log = logging.getLogger("understat_eu_residual_closer")

_BIG5_CANON = {"EPL", "LA_LIGA", "BUNDESLIGA", "SERIE_A", "LIGUE_1"}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--main-conc", type=int, default=10)
    p.add_argument("--retry-conc", type=int, default=5)
    p.add_argument("--max-rounds", type=int, default=6)
    p.add_argument("--vm-name", default="understat-eu-residual-closer")
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
from unified_trading_library import read_availability_index, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _fetch_understat_xg,
    _run_understat_shots_date,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")


def _manifest_view():
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(BUCKET)
    return df[df["data_type"].isin(["XG", "XG_SHOTS"]) & df["league_id"].isin(_BIG5_CANON)].copy()


def blank_reason_eu_dates() -> list[str]:
    """The live residual: expected_unattempted + blank error_reason, big-5 only."""
    us = _manifest_view()
    reason = us["error_reason"].astype("string").fillna("")
    mask = (us["capture_status"] == "expected_unattempted") & (reason == "")
    return sorted(us.loc[mask, "date"].astype(str).unique())


def attempted_failed_dates(dates: set[str]) -> set[str]:
    us = _manifest_view()
    af = us.loc[us["capture_status"] == "attempted_failed", "date"].astype(str)
    return set(af) & dates


async def process_date(date: str, sem: asyncio.Semaphore, tally: dict, attempts: int = 3) -> None:
    async with sem:
        for attempt in range(1, attempts + 1):
            try:
                xg = await _fetch_understat_xg(date=date, bucket=BUCKET, force=True)
                sh = await _run_understat_shots_date(date=date, bucket=BUCKET, force=True)
                tally["dates"] += 1
                tally["xg_rows"] += sum(xg.values())
                tally["shot_rows"] += sum(sh.values())
                if tally["dates"] % 100 == 0:
                    log.info(
                        "...%s/%s | xg=%s shots=%s raised=%s",
                        tally["dates"],
                        tally["total"],
                        tally["xg_rows"],
                        tally["shot_rows"],
                        tally["raised"],
                    )
                return
            except Exception as exc:
                if attempt < attempts:
                    await asyncio.sleep(min(30, 3 * 2**attempt))
                    continue
                tally["raised"] += 1
                log.error("RAISED date=%s: %s: %s", date, type(exc).__name__, str(exc)[:160])


async def run_pass(dates: list[str], conc: int, label: str) -> None:
    tally = {"dates": 0, "xg_rows": 0, "shot_rows": 0, "raised": 0, "total": len(dates)}
    sem = asyncio.Semaphore(conc)
    await asyncio.gather(*(process_date(d, sem, tally) for d in dates))
    log.info(
        "[%s] processed=%s xg_rows=%s shot_rows=%s raised=%s",
        label,
        tally["dates"],
        tally["xg_rows"],
        tally["shot_rows"],
        tally["raised"],
    )


async def main() -> None:
    log.info("bucket=%s main=%s retry=%s vm=%s", BUCKET, ARGS.main_conc, ARGS.retry_conc, ARGS.vm_name)
    todo = blank_reason_eu_dates()
    log.info("RESIDUAL: %s blank-reason expected_unattempted date(s) to force-refetch", len(todo))
    if not todo:
        log.info("=== NOTHING TO DO — manifest already clean ===")
        return

    await run_pass(todo, ARGS.main_conc, "MAIN")
    touched = set(todo)

    for rnd in range(1, ARGS.max_rounds + 1):
        await asyncio.sleep(45)  # let any provider rate-limit cool
        failed = sorted(attempted_failed_dates(touched))
        log.info("[VERIFY %s] attempted_failed dates remaining: %s", rnd, len(failed))
        if not failed:
            log.info("=== ALL DATES RESOLVED (0 attempted_failed) ===")
            break
        await run_pass(failed, ARGS.retry_conc, f"RETRY-{rnd}")
    else:
        log.warning("=== MAX ROUNDS reached; still %s attempted_failed ===", len(attempted_failed_dates(touched)))

    remaining = blank_reason_eu_dates()
    log.info("=== UNDERSTAT EU RESIDUAL CLOSER COMPLETE — %s blank-reason date(s) remain ===", len(remaining))


if __name__ == "__main__":
    asyncio.run(main())
