#!/usr/bin/env python3
"""Targeted gap-fill for PLAYER_STATS — only the missing (date, league) pairs.

Mirrors ``/tmp/fill_missing_ohlcv.py`` (Databento OHLCV pattern) for
api_football PLAYER_STATS. Reads the canonical manifest, computes the set
of missing (date, league_id) pairs whose FIXTURES capture proves a fixture
existed but no PLAYER_STATS row was ever attempted, then calls api_football
directly per (date, league) — bypassing the orchestrator's chronological
date iteration that spends most of its wall-clock on ``_should_skip_shard``
checks for the 80%+ of dates already complete.

Workflow per missing (date, league):

  1. Read fixtures parquet from GCS (FIXTURES already 99.9% captured;
     no api_football quota burned).
  2. Filter ``af_fixture_id`` rows to the missing canonical leagues for
     this date (skip leagues already complete).
  3. Call ``_fetch_sports_reference_data(date, entities=["PLAYER_STATS"],
     fixture_ids_override=filtered_fids)`` from the orchestrator. That
     handles per-fixture concurrency (50), rate-limit-aware retries via
     the adapter's ``_get_with_retry``, parquet writes, and per-league
     manifest ``record_captured`` / ``record_empty`` / ``record_failed``.

Idempotent. Manifest concurrency principle (read-once + per-shard
freshness check + write-time CAS) is preserved — concurrent VMs writing
the same shard de-dup at consolidation time. Safe to interrupt and resume.

Usage::

    cd instruments-service
    .venv/bin/python scripts/fill_missing_player_stats.py --dry-run
    .venv/bin/python scripts/fill_missing_player_stats.py --concurrency 4
    .venv/bin/python scripts/fill_missing_player_stats.py --start-date 2020-06-06 --end-date 2026-05-04
    .venv/bin/python scripts/fill_missing_player_stats.py --limit 50  # spot-check 50 dates
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import sys
import time
from collections import defaultdict
from datetime import date as date_type

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    get_expected_leagues_for_source,
)
from unified_trading_library import ApiKeyReloader, ManifestWriter, UnifiedCloudConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
DATA_TYPE = "PLAYER_STATS"
SOURCE = "api_football"
DEFAULT_START = "2020-06-06"  # api_football PLAYER_STATS coverage override per UAC
DEFAULT_END = date_type.today().isoformat()


# ── Manifest analysis ────────────────────────────────────────────────────────
def _read_canonical_manifest() -> pd.DataFrame:
    blob = storage.Client().bucket(BUCKET).blob("_index/availability_index.parquet")
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    logger.info("Read canonical manifest: %d rows", len(df))
    return df


def _compute_gaps(
    df: pd.DataFrame,
    start_iso: str,
    end_iso: str,
) -> tuple[dict[str, set[str]], int]:
    """Return ``{date: set(missing_league_ids)}`` plus the missing-shard count.

    Expected denominator: FIXTURES rows with ``capture_status='captured'``
    constrained to api_football Prediction-tier leagues — the only leagues
    api_football ever attempts PLAYER_STATS for. A missing (date, league)
    is a fixture day the FIXTURES adapter captured but PLAYER_STATS never
    wrote any row (captured / empty_confirmed / attempted_failed) for.
    """
    prediction_leagues = {
        lg.league_id
        for lg in get_expected_leagues_for_source(
            SOURCE,
            classifications=["Prediction"],
        )
    }
    logger.info(
        "Prediction-tier api_football leagues: %d (denominator universe)",
        len(prediction_leagues),
    )

    # FIXTURES captured = the days a fixture actually existed for the league.
    fx = df[df["data_type"] == "FIXTURES"].copy()
    fx["date"] = fx["date"].astype(str)
    fx["league_id"] = fx["league_id"].fillna("").astype(str)
    fx_captured = fx[fx["capture_status"] == "captured"]
    fx_captured = fx_captured[
        (fx_captured["date"] >= start_iso)
        & (fx_captured["date"] <= end_iso)
        & fx_captured["league_id"].isin(prediction_leagues)
    ]
    expected = set(zip(fx_captured["date"], fx_captured["league_id"], strict=True))
    logger.info("Expected PLAYER_STATS shards (FIXTURES-captured ∩ Prediction): %d", len(expected))

    ps = df[df["data_type"] == DATA_TYPE].copy()
    ps["date"] = ps["date"].astype(str)
    ps["league_id"] = ps["league_id"].fillna("").astype(str)
    attempted = set(zip(ps["date"], ps["league_id"], strict=True))
    logger.info("Attempted PLAYER_STATS shards (any capture_status): %d", len(attempted))

    missing = expected - attempted
    by_date: dict[str, set[str]] = defaultdict(set)
    for d, lid in missing:
        by_date[d].add(lid)
    return by_date, len(missing)


# ── Per-(date, league) work ──────────────────────────────────────────────────
async def _process_date(
    date: str,
    missing_leagues: set[str],
    api_key: str,
    bucket: str,
) -> dict[str, int]:
    """Fetch PLAYER_STATS for one date, restricted to the missing leagues."""
    # Lazy import — orchestrator pulls in heavy deps; defer until call time
    # so ``--dry-run`` doesn't pay the import cost.
    from instruments_service.engine.orchestrator import (
        _build_fixture_league_map_from_gcs,
        _fetch_sports_reference_data,
    )

    # af_fixture_id → canonical_league_id, read from already-captured fixtures.
    fid_league_map = _build_fixture_league_map_from_gcs(bucket, date)
    if not fid_league_map:
        logger.info("date=%s: no fixtures parquet on disk — skipping", date)
        return {}

    target_fids: list[int] = []
    for fid_str, lid in fid_league_map.items():
        if lid not in missing_leagues:
            continue
        try:
            target_fids.append(int(float(fid_str)))
        except (TypeError, ValueError):
            continue

    if not target_fids:
        logger.info(
            "date=%s: no fixtures match missing leagues (%d expected, %d on disk)",
            date,
            len(missing_leagues),
            len(fid_league_map),
        )
        return {}

    manifest = ManifestWriter(
        service_name="fill-missing-player-stats",
        catalogue_bucket=bucket,
        per_vm_shards=True,
    )
    counts = await _fetch_sports_reference_data(
        date=date,
        api_key=api_key,
        bucket=bucket,
        entities_to_fetch=[DATA_TYPE],
        fixture_ids_override=target_fids,
        manifest=manifest,
    )
    manifest.flush()
    logger.info(
        "date=%s: %d fixtures across %d missing leagues → %s",
        date,
        len(target_fids),
        len(missing_leagues),
        counts or "(no rows captured — likely empty across all fixtures)",
    )
    return counts


# ── Driver ───────────────────────────────────────────────────────────────────
async def _run(args: argparse.Namespace) -> int:
    df = _read_canonical_manifest()
    by_date, missing_count = _compute_gaps(df, args.start_date, args.end_date)

    sorted_dates = sorted(by_date.keys())
    if args.limit and args.limit > 0:
        sorted_dates = sorted_dates[: args.limit]

    logger.info(
        "Gap analysis: %d missing shards across %d distinct dates (limit=%d → processing %d dates)",
        missing_count,
        len(by_date),
        args.limit or 0,
        len(sorted_dates),
    )

    if args.dry_run:
        per_year: dict[str, int] = defaultdict(int)
        for d, lids in by_date.items():
            per_year[d[:4]] += len(lids)
        print("\nMissing PLAYER_STATS shards by year:")
        for y in sorted(per_year):
            print(f"  {y}: {per_year[y]:>6} shards across leagues")
        print(f"\n  Total: {missing_count} shards across {len(by_date)} dates")
        print("  Mode:  DRY-RUN")
        return 0

    cfg = UnifiedCloudConfig()
    reloader = ApiKeyReloader(
        venues=["API_FOOTBALL"],
        project_id=cfg.gcp_project_id or None,
    )
    reloader.start()
    api_key = (reloader.current_keys or {}).get("api_football")
    if not api_key:
        logger.error("No api_football key in Secret Manager — aborting")
        return 2

    # Bounded concurrency: each date worker fans out internally to
    # 50 concurrent per-fixture api calls (orchestrator's semaphore).
    # Outer ceiling of 4 dates avoids stacking 200+ in-flight requests.
    sem = asyncio.Semaphore(args.concurrency)
    started = time.monotonic()
    completed = 0
    total = len(sorted_dates)

    async def _worker(d: str) -> None:
        nonlocal completed
        async with sem:
            try:
                await _process_date(d, by_date[d], api_key, BUCKET)
            except Exception as exc:
                logger.exception("date=%s: failed: %s", d, exc)
            completed += 1
            if completed % 25 == 0 or completed == total:
                elapsed = time.monotonic() - started
                rate = completed / max(1.0, elapsed)
                eta_min = (total - completed) / max(0.001, rate) / 60.0
                logger.info(
                    "Progress: %d/%d (%.1f%%) — %.2f dates/sec — ETA %.1f min",
                    completed,
                    total,
                    100.0 * completed / total,
                    rate,
                    eta_min,
                )

    await asyncio.gather(*[asyncio.ensure_future(_worker(d)) for d in sorted_dates])
    logger.info(
        "Done. Run consolidator: python -m unified_trading_library.manifest_consolidator --bucket %s",
        BUCKET,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Compute gap counts only")
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Concurrent date workers. Default 4. Each fans out to 50 per-fixture api calls.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N missing dates (spot-check / smoke). 0 = all.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
