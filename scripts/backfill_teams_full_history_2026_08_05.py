#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after backfill confirmed in live consolidated _index
"""Full-history TEAMS backfill — covers ALL api_football leagues with
``expected_unattempted`` cells, not just zero-captured leagues.

Supersedes ``backfill_teams_61_leagues_2026_07_13.py`` (which only handled
the 61 zero-captured leagues). The 61-league backfill completed 2026-07-13
(162,032 cells, 0 failed), leaving 67,782 ``expected_unattempted`` cells
across 322 leagues (42 zero-captured + 280 partially-covered) as of
2026-08-05. This script closes that remaining gap.

Built for the Track S2 TEAMS full-history backfill todo
(``sports_consolidated_native_ao_extract_2026_07_25.md``).

Same data model as the 61-league script + the daily orchestrator: one real,
live ``/teams`` API call per league (the provider has no historical per-date
team-roster endpoint — ``/teams`` always returns the CURRENT squad), then
writes + manifest-records that same roster across every missing historical
(league, date) cell.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_teams_full_history_2026_08_05.py --dry-run
    .venv/bin/python scripts/backfill_teams_full_history_2026_08_05.py --apply
    .venv/bin/python scripts/backfill_teams_full_history_2026_08_05.py --apply --limit-leagues 2  # smoke test
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.sports import get_expected_leagues_for_source
from unified_trading_library import ApiKeyReloader, ManifestWriter, UnifiedCloudConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-prd-central-element-323112"
SOURCE = "api_football"
DATA_TYPE = "TEAMS"


def _read_canonical_manifest() -> pd.DataFrame:
    # Column-projected pyarrow read (only the 5 columns the gap computation
    # touches). The full 9.25M-row index has ~30+ columns and ~7GB materialised
    # in pandas — an unfiltered read blew past the 6G bounded-analysis cap on
    # the shared host (2026-08-05, first dry-run attempt). Mirror the census
    # script's pyarrow GcsFileSystem path: range reads only the needed columns.
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs = pafs.GcsFileSystem()
    cols = ["date", "league_id", "capture_status", "source", "data_type"]
    tbl = pq.read_table(f"{BUCKET}/_index/availability_index.parquet", filesystem=fs, columns=cols)
    df = tbl.to_pandas()
    logger.info("Read canonical manifest (column-projected): %d rows", len(df))
    return df


def _compute_missing(df: pd.DataFrame) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Return ``({league_id: [missing dates]}, {league_id: api_football_id})``.

    FULL-HISTORY variant (2026-08-05): covers ALL leagues expected for
    api_football TEAMS that have ANY ``expected_unattempted`` cells — both
    zero-captured leagues AND partially-covered leagues with date gaps.

    Each missing cell gets the current roster stamped onto it (same data
    model as the daily orchestrator + the original 61-league backfill).
    Leagues with no ``api_football_id`` are skipped (cannot fetch).
    """
    teams = df[(df["source"] == SOURCE) & (df["data_type"] == DATA_TYPE)].copy()
    teams["league_id"] = teams["league_id"].fillna("").astype(str)

    expected = get_expected_leagues_for_source(SOURCE)
    # All expected leagues with valid API IDs — the full-history universe.
    af_id_map = {lg.league_id: int(lg.api_football_id) for lg in expected if lg.api_football_id is not None}
    valid_lids = set(af_id_map.keys())

    eu = teams[(teams["capture_status"] == "expected_unattempted") & (teams["league_id"].isin(valid_lids))]
    by_league: dict[str, list[str]] = {}
    for lid, grp in eu.groupby("league_id"):
        by_league[lid] = sorted(grp["date"].astype(str).unique())

    total_expected = len(valid_lids)
    with_gaps = len(by_league)
    total_cells = sum(len(v) for v in by_league.values())
    logger.info(
        "Full-history scope: %d/%d expected leagues have expected_unattempted cells (%d total cells)",
        with_gaps,
        total_expected,
        total_cells,
    )
    logger.info(
        "Leagues already fully covered (no expected_unattempted cells): %d",
        total_expected - with_gaps,
    )
    return by_league, af_id_map


async def _fetch_all_rosters(af_id_map: dict[str, int], api_key: str) -> dict[str, pd.DataFrame]:
    """One live ``get_teams()`` call per league — the real current roster."""
    from instruments_service.engine.orchestrator import (
        _coerce_adapter_output,
        create_sports_reference_adapter,
    )

    # No per-VM rate budget is stamped for a plain local run (that's a fleet
    # VM-launcher concern, SPORTS_ADAPTER_RATE_RPM defaults to 0/disabled) —
    # without it, sequential calls fire as fast as asyncio schedules them,
    # which blew through api_football's real per-minute limit on the first
    # full-run attempt (2026-07-13: ~60 calls in ~3s -> ApiFootballResponseError
    # rateLimit + several suspiciously-empty 0-team responses in the same
    # burst window). A conservative fixed delay keeps this well under any
    # plausible per-minute ceiling; each league is fetched exactly once
    # regardless, so the extra ~1 minute of wall-clock is negligible against
    # the write phase's ~60 minutes.
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)
    out: dict[str, pd.DataFrame] = {}
    for lid, af_id in sorted(af_id_map.items()):
        await asyncio.sleep(1.2)
        try:
            teams = await adapter.get_teams(af_id)
        except Exception:
            logger.exception("league=%s af_id=%s: get_teams failed — skipping this league", lid, af_id)
            continue
        rows = [_coerce_adapter_output(t) for t in teams]
        for r in rows:
            r["league_id"] = lid
        if rows:
            out[lid] = pd.DataFrame(rows)
            logger.info("league=%s af_id=%s: %d teams fetched (1 API call)", lid, af_id, len(rows))
        else:
            logger.warning("league=%s af_id=%s: 0 teams returned — no roster to backfill with", lid, af_id)
    return out


def _write_one(lid: str, date: str, base_df: pd.DataFrame, manifest: ManifestWriter) -> None:
    from instruments_service.engine.orchestrator import (
        PipelineMode,
        _canonical_league_id,
        _gated_sink_write,
        _is_in_canonical_write_universe,
        _sports_ref_sink_for,
        _sports_ref_source,
        stamp_available_at_explicit,
    )

    canon = _canonical_league_id(lid)
    if not _is_in_canonical_write_universe(canon):
        return
    stamped = stamp_available_at_explicit(base_df, when=datetime.now(UTC))
    _gated_sink_write(
        _sports_ref_sink_for(BUCKET, date, "teams"),
        data=stamped,
        partition={"entity": "teams", "league": canon},
        filename="teams.parquet",
        venue="api_football",
        entity="teams",
    )
    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": date, "data_type": "TEAMS", "league_id": canon},
        df=stamped,
        asset_group="sports",
        instrument_type="",
        data_type="TEAMS",
        league_id=canon,
        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        source=_sports_ref_source("teams"),
        service_emission_state=None,
    )


def _run(args: argparse.Namespace) -> int:
    df = _read_canonical_manifest()
    by_league, af_id_map = _compute_missing(df)

    total_cells = sum(len(v) for v in by_league.values())
    logger.info(
        "Gap analysis: %d missing leagues, %d missing (league, date) cells",
        len(by_league),
        total_cells,
    )
    for lid in sorted(by_league)[:10]:
        logger.info(
            "  %s: %d missing dates (%s .. %s)", lid, len(by_league[lid]), by_league[lid][0], by_league[lid][-1]
        )
    if len(by_league) > 10:
        logger.info("  ... and %d more leagues", len(by_league) - 10)

    if args.limit_leagues:
        keep = set(sorted(by_league)[: args.limit_leagues])
        by_league = {k: v for k, v in by_league.items() if k in keep}
        af_id_map = {k: v for k, v in af_id_map.items() if k in keep}
        logger.info("--limit-leagues %d: restricting to %s", args.limit_leagues, sorted(by_league))

    if args.dry_run:
        print(f"\nDRY-RUN: {len(by_league)} leagues, {sum(len(v) for v in by_league.values())} cells to backfill")
        print("Mode:  DRY-RUN (no API calls, no writes)")
        return 0

    from unified_trading_library import setup_events

    cfg = UnifiedCloudConfig()
    # mode="local": every per-cell record_captured() call hits the benign
    # SchemaContractNotFoundError->WARNING path (no UAC schema contract is
    # registered for asset_group="sports"/data_type="TEAMS" — the same
    # warn-only path the live per-league STANDINGS writer already takes in
    # production). At this script's ~190k-cell scale a durable GcsEventSink
    # would mean ~190k individual GCS event writes for a routine, already-
    # accepted warning — local mode logs it via the stdlib logger instead
    # (no GCS event persistence needed for a one-off backfill's own logging).
    setup_events(service_name="backfill-teams-full-history", mode="local")
    # Silence the per-cell "Event: MANIFEST_WRITE_SCHEMA_MISSING" INFO log
    # (fires once per record_captured() call at this script's ~190k-cell
    # scale — benign per the codex above, just log-volume noise here).
    logging.getLogger("unified_trading_library.events").setLevel(logging.WARNING)

    reloader = ApiKeyReloader(venues=["API_FOOTBALL"], project_id=cfg.gcp_project_id or None)
    reloader.start()
    api_key = (reloader.current_keys or {}).get("api_football")
    if not api_key:
        logger.error("No api_football key in Secret Manager — aborting")
        return 2

    rosters = asyncio.run(_fetch_all_rosters(af_id_map, api_key))
    if not rosters:
        logger.error("No rosters fetched — aborting (nothing to backfill)")
        return 2

    manifest = ManifestWriter(service_name="backfill-teams-full-history", catalogue_bucket=BUCKET, per_vm_shards=True)

    tasks: list[tuple[str, str]] = []
    for lid, dates in by_league.items():
        if lid not in rosters:
            continue
        tasks.extend((lid, d) for d in dates)
    total = len(tasks)
    logger.info(
        "Writing %d (league, date) cells across %d leagues with concurrency=%d", total, len(rosters), args.concurrency
    )

    started = time.monotonic()
    completed = 0
    failed = 0

    def _worker(lid: str, date: str) -> None:
        _write_one(lid, date, rosters[lid], manifest)

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {pool.submit(_worker, lid, d): (lid, d) for lid, d in tasks}
        for fut in as_completed(futures):
            lid, d = futures[fut]
            try:
                fut.result()
            except Exception:
                failed += 1
                logger.exception("league=%s date=%s: write failed", lid, d)
            completed += 1
            if completed % 2000 == 0 or completed == total:
                elapsed = time.monotonic() - started
                rate = completed / max(1.0, elapsed)
                eta_min = (total - completed) / max(0.001, rate) / 60.0
                logger.info(
                    "Progress: %d/%d (%.1f%%) — %.1f writes/sec — ETA %.1f min — %d failed so far",
                    completed,
                    total,
                    100.0 * completed / total,
                    rate,
                    eta_min,
                    failed,
                )

    manifest.close()
    logger.info(
        "Done. %d/%d cells written (%d failed). Manifest closed (per-VM shard drained).", total - failed, total, failed
    )
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Compute gap only, no API calls / writes")
    parser.add_argument("--apply", action="store_true", help="Actually fetch + write + record_captured")
    parser.add_argument("--limit-leagues", type=int, default=0, help="Restrict to first N missing leagues (smoke test)")
    parser.add_argument("--concurrency", type=int, default=32, help="Thread pool size for per-cell writes")
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        parser.error("Pass --dry-run or --apply")
    return _run(args)


if __name__ == "__main__":
    sys.exit(main())
