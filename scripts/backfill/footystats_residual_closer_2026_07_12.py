# Epic: sports_master
# Lifecycle: ONE-OFF — closes the footystats MATCHES/PREDICTIONS/ODDS
#   expected_unattempted residual (item #5, sports_p2_history_reference_and_odds;
#   todo #4 in footystats_matches_predictions_fetch_gaps_2026_07_08.md).
# Delete-when: manifest shows pending_fetch == 0 for
#   (footystats, MATCHES|PREDICTIONS|ODDS) within their SSOT-expected leagues.
# SSOT: plans/active/sports_p2_history_reference_and_odds_2015_to_present_2026_06_27.md (item #5)
#       plans/active/issues/footystats_matches_predictions_fetch_gaps_2026_07_08.md (todo #4)
"""footystats_residual_closer_2026_07_12.py — closes the footystats MATCHES /
PREDICTIONS / ODDS ``expected_unattempted`` residual left over after the
write-path fixes (instruments-service@1af6c92 / @78636dd / @e951813) landed.

Those fixes stop NEW rows from being seeded incorrectly going forward but do
NOT retroactively clean rows the manifest already carries — this script does
that cleanup in two parts, mirroring the diagnosis in the issue doc above:

1. SUBSCRIPTION-SCOPE TYPING (the MATCHES 4-league gap). The existing typing
   scripts (``type_footystats_matches_predictions_non_covered_leagues_2026_07_06.py``,
   ``type_footystats_odds_non_covered_leagues_2026_06_29.py``) determine
   "covered" dynamically — any league with >=1 ``captured`` row, ever. That
   heuristic is fooled by legacy incidental ``captured`` rows written BEFORE
   the write-gate fix for leagues that are actually out-of-subscription
   (``PRED_NO_FOOTYSTATS`` — CHILE_PRIMERA, K_LEAGUE_1, LIGA_MX,
   ARGENTINA_PRIMERA, A_LEAGUE). This script instead uses the SSOT expected
   set directly (the same ``get_expected_leagues_for_source("footystats", ...)``
   call the fetch functions themselves use to build ``_ft_expected``) so a
   league that is genuinely out-of-subscription is typed
   EXPECTED_NO_PROVIDER_COVERAGE regardless of any stale captured history.

2. FIXTURE-CALENDAR RESIDUAL CLOSE (the PREDICTIONS/ODDS cup-competition gap
   + any MATCHES tail). Rows for leagues that ARE in the SSOT expected set
   but still show ``expected_unattempted`` are pre-fix residue of the missing
   per-league completion loop (now shipped for all three entities — each
   emits ``record_empty(EXPECTED_NO_FIXTURE)`` per expected league with no
   fixture that date). Force-refetch (``force=True``) the exact residual
   DATES via the per-date capture path so it resolves every expected league
   for that date, either real capture or a typed empty. Per-entity force
   flags avoid re-forcing an entity that is already clean for that date (its
   own per-league skip-check still short-circuits quickly on ``force=False``).

RESUME: re-reads the manifest each phase, so re-running only touches
whatever residual remains.

SELF-HEALING: after the main force-refetch pass, re-checks for
``attempted_failed`` on the touched dates and retries (lower concurrency +
cooldown) until zero remain or ``--max-rounds``, mirroring
``understat_eu_residual_closer_2026_07_08.py``.

Usage:
  footystats_residual_closer_2026_07_12.py [--main-conc 8] [--retry-conc 4]
      [--max-rounds 6] [--vm-name <tag>] [--skip-typing]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from datetime import UTC, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("footystats_residual_closer")

_DATA_TYPES = ("MATCHES", "PREDICTIONS", "ODDS")
_NO_PROVIDER_COVERAGE_REASON = "EXPECTED_NO_PROVIDER_COVERAGE"
_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--main-conc", type=int, default=8)
    p.add_argument("--retry-conc", type=int, default=4)
    p.add_argument("--max-rounds", type=int, default=6)
    p.add_argument("--vm-name", default="footystats-residual-closer")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--skip-typing", action="store_true", help="Skip the subscription-scope typing pass.")
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

import gcsfs
import pandas as pd
import unified_trading_library.manifest_writer as _mw
from unified_api_contracts.sports import get_expected_leagues_for_source
from unified_trading_library import (
    read_availability_index,
    resolve_bucket_name,
    validate_api_keys_for_venues,
)

from instruments_service.engine.orchestrator import (
    _fetch_footystats_matches,
    _fetch_footystats_odds,
    _fetch_footystats_predictions,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

_FETCH_FN = {
    "MATCHES": _fetch_footystats_matches,
    "PREDICTIONS": _fetch_footystats_predictions,
    "ODDS": _fetch_footystats_odds,
}


def _expected_leagues() -> frozenset[str]:
    """SSOT footystats-expected league set — identical to ``_ft_expected`` in footystats.py."""
    return frozenset(
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    )


def _manifest_view() -> pd.DataFrame:
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(BUCKET)
    dt = df["data_type"].astype("string").fillna("").str.upper()
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    return df[dt.isin(_DATA_TYPES) & (source == "footystats")].copy()


def run_typing_pass(expected: frozenset[str]) -> int:
    """Type expected_unattempted/attempted_failed rows for out-of-subscription
    leagues as EXPECTED_NO_PROVIDER_COVERAGE, using the SSOT expected set
    (not the dynamic "ever captured" heuristic the older typing scripts use)."""
    fs = gcsfs.GCSFileSystem()
    log.info("Reading live _index gs://%s/%s for typing pass", BUCKET, _MANIFEST_BLOB)
    with fs.open(f"{BUCKET}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")

    dt = df["data_type"].astype("string").fillna("").str.upper()
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    cs = df["capture_status"].astype("string").fillna("")
    league = df["league_id"].astype("string").fillna("")
    mask = (
        dt.isin(_DATA_TYPES)
        & (source == "footystats")
        & cs.isin({"expected_unattempted", "attempted_failed"})
        & (~league.isin(expected))
    )
    n = int(mask.sum())
    log.info("Subscription-scope typing: %d row(s) for out-of-subscription leagues to type", n)
    if n == 0:
        return 0
    pending = df[mask]
    log.info("  By data_type: %s", dict(pending["data_type"].value_counts()))
    log.info(
        "  Unique out-of-subscription leagues: %s", sorted(pending["league_id"].astype("string").fillna("").unique())
    )

    now_iso = datetime.now(UTC).isoformat()
    shard_df = df.loc[mask].copy()
    shard_df["capture_status"] = "empty_confirmed"
    shard_df["error_reason"] = _NO_PROVIDER_COVERAGE_REASON
    shard_df["attempted_at"] = now_iso
    if "written_at" in shard_df.columns:
        shard_df["written_at"] = now_iso
    if "schema_version" in shard_df.columns:
        shard_df["schema_version"] = 9

    shard_path = _PER_VM_PATH_TEMPLATE.format(instance=f"{ARGS.vm_name}-typing")
    sbuf = io.BytesIO()
    shard_df.to_parquet(sbuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    sbuf.seek(0)
    with fs.open(f"{BUCKET}/{shard_path}", "wb") as fh:
        fh.write(sbuf.getvalue())
    log.info("APPLIED typing pass: %d row(s) -> gs://%s/%s", n, BUCKET, shard_path)
    return n


def residual_dates_by_type(expected: frozenset[str]) -> dict[str, list[str]]:
    """Live per-data_type residual: expected_unattempted rows within the SSOT-expected leagues."""
    fs_df = _manifest_view()
    out: dict[str, list[str]] = {}
    for data_type in _DATA_TYPES:
        sub = fs_df[
            (fs_df["data_type"].astype("string").str.upper() == data_type)
            & (fs_df["league_id"].astype("string").fillna("").isin(expected))
            & (fs_df["capture_status"] == "expected_unattempted")
        ]
        out[data_type] = sorted(sub["date"].astype(str).unique())
    return out


def attempted_failed_dates_by_type(expected: frozenset[str], touched: dict[str, set[str]]) -> dict[str, set[str]]:
    fs_df = _manifest_view()
    out: dict[str, set[str]] = {}
    for data_type, dates in touched.items():
        sub = fs_df[
            (fs_df["data_type"].astype("string").str.upper() == data_type)
            & (fs_df["league_id"].astype("string").fillna("").isin(expected))
            & (fs_df["capture_status"] == "attempted_failed")
        ]
        out[data_type] = set(sub["date"].astype(str)) & dates
    return out


async def process_date(
    date: str, per_type_force: dict[str, bool], api_key: str, sem: asyncio.Semaphore, tally: dict, attempts: int = 3
) -> None:
    async with sem:
        for attempt in range(1, attempts + 1):
            try:
                for data_type, force in per_type_force.items():
                    fn = _FETCH_FN[data_type]
                    result = await fn(date=date, api_key=api_key, bucket=BUCKET, force=force)
                    tally[data_type] += sum(result.values())
                tally["dates"] += 1
                if tally["dates"] % 50 == 0:
                    log.info("...%s/%s dates processed | raised=%s", tally["dates"], tally["total"], tally["raised"])
                return
            except Exception as exc:
                if attempt < attempts:
                    await asyncio.sleep(min(30, 3 * 2**attempt))
                    continue
                tally["raised"] += 1
                log.error("RAISED date=%s: %s: %s", date, type(exc).__name__, str(exc)[:160])


async def run_pass(date_force_map: dict[str, dict[str, bool]], api_key: str, conc: int, label: str) -> None:
    tally: dict = dict.fromkeys(_DATA_TYPES, 0)
    tally.update({"dates": 0, "raised": 0, "total": len(date_force_map)})
    sem = asyncio.Semaphore(conc)
    await asyncio.gather(*(process_date(d, f, api_key, sem, tally) for d, f in date_force_map.items()))
    log.info(
        "[%s] processed=%s MATCHES_rows=%s PREDICTIONS_rows=%s ODDS_rows=%s raised=%s",
        label,
        tally["dates"],
        tally["MATCHES"],
        tally["PREDICTIONS"],
        tally["ODDS"],
        tally["raised"],
    )


async def main() -> None:
    keys = validate_api_keys_for_venues(venues=["footystats"], project_id=ARGS.project)
    api_key = keys.get("footystats")
    if not api_key:
        log.error("No footystats API key resolved from Secret Manager — refusing to run.")
        return

    expected = _expected_leagues()
    log.info(
        "bucket=%s main=%s retry=%s vm=%s SSOT-expected footystats leagues=%d",
        BUCKET,
        ARGS.main_conc,
        ARGS.retry_conc,
        ARGS.vm_name,
        len(expected),
    )

    if not ARGS.skip_typing:
        run_typing_pass(expected)
    else:
        log.info("--skip-typing set: skipping subscription-scope typing pass")

    residual = residual_dates_by_type(expected)
    for data_type, dates in residual.items():
        log.info("RESIDUAL %s: %d date(s) within SSOT-expected leagues", data_type, len(dates))

    union_dates = sorted(set().union(*residual.values())) if any(residual.values()) else []
    log.info("UNION: %d distinct date(s) to force-refetch across MATCHES/PREDICTIONS/ODDS", len(union_dates))
    if not union_dates:
        log.info("=== NOTHING TO DO — manifest already clean within SSOT-expected leagues ===")
        return

    date_force_map = {d: {dt: (d in set(residual[dt])) for dt in _DATA_TYPES} for d in union_dates}
    await run_pass(date_force_map, api_key, ARGS.main_conc, "MAIN")
    touched = {dt: set(union_dates) for dt in _DATA_TYPES}

    for rnd in range(1, ARGS.max_rounds + 1):
        await asyncio.sleep(45)  # let any provider rate-limit cool
        failed = attempted_failed_dates_by_type(expected, touched)
        total_failed = sum(len(v) for v in failed.values())
        log.info("[VERIFY %s] attempted_failed dates remaining: %s", rnd, total_failed)
        if not total_failed:
            log.info("=== ALL DATES RESOLVED (0 attempted_failed) ===")
            break
        retry_union = sorted(set().union(*failed.values()))
        retry_force_map = {d: {dt: (d in failed[dt]) for dt in _DATA_TYPES} for d in retry_union}
        await run_pass(retry_force_map, api_key, ARGS.retry_conc, f"RETRY-{rnd}")
    else:
        remaining = attempted_failed_dates_by_type(expected, touched)
        log.warning("=== MAX ROUNDS reached; still %s attempted_failed ===", sum(len(v) for v in remaining.values()))

    # Explicit pre-exit drain (not atexit — races the event loop teardown,
    # see plans/active/issues/manifest_atexit_drain_races_asyncio_shutdown_2026_07_09.md).
    flushed = _mw.flush_all_pending_buckets()
    log.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    remaining_residual = residual_dates_by_type(expected)
    for data_type, dates in remaining_residual.items():
        log.info("=== FOOTYSTATS RESIDUAL CLOSER COMPLETE — %s: %d date(s) remain ===", data_type, len(dates))


if __name__ == "__main__":
    asyncio.run(main())
