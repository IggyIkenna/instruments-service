# Epic: infrastructure_master
# Lifecycle: ONE-OFF backfill — understat XG + XG_SHOTS full-history repair (2014→present).
# Delete-when: understat-vm-xg-complete gate flips green on real captured shots AND
#   the manifest shows 0 attempted_failed / 0 stale-empty for XG+XG_SHOTS on the big-5.
# SSOT: plans/active/understat_local_backfill_completion_2026_07_06.md
#       plans/active/issues/understat_bulk_download_backfill_2026_06_29.md
"""Understat XG + XG_SHOTS full-history bulk backfill — LOCAL, resume-aware, self-healing.

Reuses the SHIPPED per-date capture path (``_fetch_understat_xg`` +
``_run_understat_shots_date``, force=True) — same GCS layout, schema, honest
-absence and manifest atom as the normal pipeline. The ONLY bulk optimisation is
the adapter's module-level ``getLeagueData`` cache (shipped): enumeration fetches
each (league, season) once, so the per-date functions hit the cache instead of
re-fetching per calendar date.

Why LOCAL (not a SPOT VM — a deliberate one-off exception): understat is a
single-origin scraper with NO bulk shot endpoint, so XG_SHOTS is ~19k individual
``getMatchData`` calls from ONE IP — a fleet of VMs gives no parallelism benefit
(one IP is the rate ceiling), and the bottleneck is the per-date write path, not
compute. So it runs as one local process. This is the ONLY backfill exempted
from the SPOT-VM rule; all other backfills MUST still use the VM launchers.

RESUME: skips any date already fully captured by THIS backfill era (all big-5
XG+XG_SHOTS cells captured/empty with attempted_at >= --cutoff and none
attempted_failed / expected_unattempted / stale). So a restart / preemption
picks up where it left off; a stale falsely-empty XG cell (2026-05-07) is
re-processed.

SELF-HEALING: after the main pass, re-reads the manifest, finds any date left
attempted_failed, and re-runs it (lower concurrency + cooldown) until zero remain
or --max-rounds. The adapter already retries 429/5xx with backoff underneath.

Usage:
  understat_bulk_backfill.py [--start 2014] [--end 2025] [--main-conc 14]
      [--retry-conc 6] [--max-rounds 6] [--cutoff 2026-07-06] [--vm-name <tag>]
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
log = logging.getLogger("understat_backfill")

_BIG5 = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]  # RFPL not in the canonical write universe
_BIG5_CANON = {"EPL", "LA_LIGA", "BUNDESLIGA", "SERIE_A", "LIGUE_1"}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, default=2014)
    p.add_argument("--end", type=int, default=2025)
    p.add_argument("--main-conc", type=int, default=14)
    p.add_argument("--retry-conc", type=int, default=6)
    p.add_argument("--max-rounds", type=int, default=6)
    p.add_argument("--cutoff", default="2026-07-06")  # attempted_at >= this = "this backfill era" = done
    p.add_argument("--vm-name", default="understat-bulk-backfill")
    p.add_argument("--project", default="central-element-323112")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name
os.environ.pop("CLOUD_MOCK_MODE", None)

import httpx
from unified_trading_library import setup_events

setup_events("instruments-service", "local")

import unified_trading_library.manifest_writer as _mw
from unified_trading_library import read_availability_index, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _fetch_understat_xg,
    _run_understat_shots_date,
)
from instruments_service.reference_data.adapters.sports.adapters.understat import (
    _BASE_URL,
    _LEAGUE_MATCH_CACHE,
    _extract_dates_from_json,
)

SEASONS = list(range(ARGS.start, ARGS.end + 1))
BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
_H = {"User-Agent": _UA, "X-Requested-With": "XMLHttpRequest"}


def enumerate_dates() -> list[str]:
    """One getLeagueData per (league, season) → pre-populate the adapter cache,
    return the sorted unique set of result-fixture calendar dates."""
    dates: set[str] = set()
    with httpx.Client(timeout=40, follow_redirects=True) as c:
        c.get(_BASE_URL + "/", headers={"User-Agent": _UA})
        for season in SEASONS:
            for league in _BIG5:
                try:
                    raw = c.get(
                        f"{_BASE_URL}/getLeagueData/{league}/{season}",
                        headers={**_H, "Referer": f"{_BASE_URL}/league/{league}/{season}"},
                    ).json()
                except Exception as exc:
                    log.warning("getLeagueData %s/%s failed: %s", league, season, type(exc).__name__)
                    continue
                matches = _extract_dates_from_json(raw)
                _LEAGUE_MATCH_CACHE[(league, season)] = matches
                for m in matches:
                    if isinstance(m, dict) and m.get("isResult") and str(m.get("datetime", ""))[:10]:
                        dates.add(str(m["datetime"])[:10])
    return sorted(dates)


def _manifest_view():
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache
    df = read_availability_index(BUCKET)
    return df[df["data_type"].isin(["XG", "XG_SHOTS"]) & df["league_id"].isin(_BIG5_CANON)].copy()


def pending_dates(all_dates: list[str]) -> list[str]:
    """A date is PENDING unless every big-5 XG+XG_SHOTS cell is captured/empty with
    attempted_at >= cutoff and none attempted_failed/expected_unattempted. So we
    re-process seeds, failures and stale falsely-empty (old-timestamp) cells."""
    us = _manifest_view()
    us["_at"] = us["attempted_at"].astype(str)
    done: set[str] = set()
    for date, g in us.groupby("date"):
        st = set(g["capture_status"])
        if ("attempted_failed" in st) or ("expected_unattempted" in st):
            continue
        if (g["_at"] >= ARGS.cutoff).all():
            done.add(str(date))
    return [d for d in all_dates if d not in done]


def attempted_failed_dates() -> set[str]:
    us = _manifest_view()
    return set(us.loc[us["capture_status"] == "attempted_failed", "date"].astype(str).unique())


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
                    log.info("...%s/%s | xg=%s shots=%s raised=%s", tally["dates"], tally["total"], tally["xg_rows"], tally["shot_rows"], tally["raised"])
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
    log.info("[%s] processed=%s xg_rows=%s shot_rows=%s raised=%s", label, tally["dates"], tally["xg_rows"], tally["shot_rows"], tally["raised"])


async def main() -> None:
    log.info("bucket=%s seasons=%s..%s main=%s retry=%s cutoff=%s vm=%s", BUCKET, ARGS.start, ARGS.end, ARGS.main_conc, ARGS.retry_conc, ARGS.cutoff, ARGS.vm_name)
    all_dates = enumerate_dates()
    log.info("cache_entries=%s fixture-dates=%s", len(_LEAGUE_MATCH_CACHE), len(all_dates))
    if not all_dates:
        log.error("no dates enumerated — abort")
        sys.exit(1)

    todo = pending_dates(all_dates)
    log.info("RESUME: %s/%s dates pending (%s already captured this era)", len(todo), len(all_dates), len(all_dates) - len(todo))
    if todo:
        await run_pass(todo, ARGS.main_conc, "MAIN")

    for rnd in range(1, ARGS.max_rounds + 1):
        await asyncio.sleep(45)  # let any provider rate-limit cool
        failed = sorted(attempted_failed_dates())
        log.info("[VERIFY %s] attempted_failed dates remaining: %s", rnd, len(failed))
        if not failed:
            log.info("=== ALL DATES CAPTURED (0 attempted_failed) ===")
            break
        await run_pass(failed, ARGS.retry_conc, f"RETRY-{rnd}")
    else:
        log.warning("=== MAX ROUNDS reached; still %s attempted_failed ===", len(attempted_failed_dates()))

    log.info("=== UNDERSTAT BULK BACKFILL COMPLETE ===")


if __name__ == "__main__":
    asyncio.run(main())
