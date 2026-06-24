#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: golden-window (2025-09-01..2025-11-30) FIXTURES coverage reads 100% on the live
#   sports _index AND the campaign plan
#   sports_canonical_universe_and_apifootball_reference_expansion_2026_06_24 archives.
"""Targeted in-place reclassify: golden-window FIXTURES no-match-day cells -> EXPECTED_NO_FIXTURE.

Why: the data-type-aware honest-coverage already lands FIXTURES at ~100% for no-match days that
NEW writes type as ``EXPECTED_NO_FIXTURE``, but a residue of golden-window (2025-09-01..2025-11-30)
FIXTURES cells from OLDER code remain typed ``attempted_failed`` or blank/``SOURCE_RETURNED_ZERO``
``empty_confirmed`` on days the league did NOT play. Those cells (a) make the backfill VMs' pre-flight
RETRY a complete window and (b) under-read the coverage denominator (an attempted_failed counts
against, a blank empty is not data-type-resolved).

Fix: for a FIXTURES cell in the golden window whose ``(league_id, date)`` is NOT in the FIXTURES
truth-set (no FIXTURES-or-per-fixture-derived ``captured`` row exists anywhere in history for that
league+date -> the league genuinely did not play that day), relabel it
``capture_status=empty_confirmed`` + ``reason=EXPECTED_NO_FIXTURE``. A cell whose ``(league, date)``
IS in the truth-set (a fixture existed) is LEFT UNTOUCHED — a real ``attempted_failed`` there is a
genuine gap that must be backfilled, never masked.

In-place, PRESERVING every other column (source / asset_group / pipeline_mode / league_id all already
canonical — mirrors canonicalize_sports_index_cf234_2026_06_24.py). NOT a rebuild (the rebuild path
regresses source on empty re-emits). Snapshot-first to ``_index/snapshots/`` before any --apply;
the instruments-sports consolidator MUST be paused during --apply (it merges shards every minute —
pause to avoid a re-merge race, resume after).

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \
    .venv-workspace/bin/python scripts/reclassify_golden_window_fixtures_no_match_2026_06_24.py [--apply]
"""

from __future__ import annotations

import argparse
import logging
import tempfile

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reclassify_golden_window_fixtures")

INDEX_BLOB = "_index/availability_index.parquet"
GOLDEN_START = "2025-09-01"
GOLDEN_END = "2025-11-30"
_DATE_COLS = ("date", "processing_date", "day")
# A (league, date) "had a fixture" iff a captured FIXTURES row OR any captured per-fixture-derived
# row exists for it (robustness union — mirrors rebuild_sports_manifest_v9._build_fixtures_truth_set).
_PER_FIXTURE_DERIVED = frozenset(
    {
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "FIXTURE_STATS",
        "INJURIES",
        "PLAYER_STATS",
        "STANDINGS",
        "ODDS",
        "ODDS_SNAPSHOT",
        "ODDS_MOVEMENT",
        "ODDS_HORIZON_BUCKET",
        "XG",
        "XG_SHOTS",
        "MATCHES",
        "PREDICTIONS",
        "trades",
    }
)
# empty_confirmed reasons that are NOT a typed no-match verdict — safe to relabel when not-in-truth.
_RELABELLABLE_EMPTY_REASONS = {"", "none", "nan", "<na>", "source_returned_zero"}
_CAPTURED = "captured"
_EMPTY = "empty_confirmed"
_FAILED = "attempted_failed"


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _day10(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str[:10]


def _first_date_col(df: pd.DataFrame) -> str:
    for col in _DATE_COLS:
        if col in df.columns:
            return col
    raise SystemExit(f"No date column among {_DATE_COLS} in _index")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the reclassified index (default: dry-run)")
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    import gcsfs  # noqa: imports-inside-functions — script-only dep

    fs = gcsfs.GCSFileSystem()
    blob = f"{bucket}/{INDEX_BLOB}"
    tmp_in = f"{tempfile.gettempdir()}/sports_index_fixreclass_in.parquet"
    fs.get(blob, tmp_in)
    df = pd.read_parquet(tmp_in)
    logger.info("Loaded _index: %d rows", len(df))

    for col in ("data_type", "capture_status", "league_id"):
        if col not in df.columns:
            logger.error("MISSING required column %s — aborting", col)
            return 2

    reason_col = "reason" if "reason" in df.columns else "error_reason"
    if reason_col not in df.columns:
        df[reason_col] = ""
        logger.warning("No reason/error_reason column — created '%s'", reason_col)

    date_col = _first_date_col(df)
    dt = df["data_type"].fillna("").astype(str).str.strip().str.upper()
    status = df["capture_status"].fillna("").astype(str).str.strip()
    league = df["league_id"].fillna("").astype(str).str.strip().str.upper()
    day = _day10(df[date_col])
    reason_norm = df[reason_col].fillna("").astype(str).str.strip().str.lower()

    # Build the FIXTURES truth-set: {(league, date)} with a captured FIXTURES-or-derived row.
    truth_mask = (status == _CAPTURED) & ((dt == "FIXTURES") | dt.isin({d.upper() for d in _PER_FIXTURE_DERIVED}))
    truth = set(zip(league[truth_mask].tolist(), day[truth_mask].tolist(), strict=True))
    logger.info("FIXTURES truth-set: %d (league, date) cells with a known fixture", len(truth))

    in_golden = (day >= GOLDEN_START) & (day <= GOLDEN_END)
    is_fixtures = dt == "FIXTURES"
    relabellable_empty = (status == _EMPTY) & reason_norm.isin(_RELABELLABLE_EMPTY_REASONS)
    candidate = is_fixtures & in_golden & ((status == _FAILED) | relabellable_empty)
    not_in_truth = pd.Series(
        [(lg, dy) not in truth for lg, dy in zip(league.tolist(), day.tolist(), strict=True)],
        index=df.index,
    )
    target = candidate & not_in_truth

    gw_fixtures = int((is_fixtures & in_golden).sum())
    gw_captured = int((is_fixtures & in_golden & (status == _CAPTURED)).sum())
    gw_failed = int((is_fixtures & in_golden & (status == _FAILED)).sum())
    in_truth_failed = int((is_fixtures & in_golden & (status == _FAILED) & ~not_in_truth).sum())
    logger.info(
        "GOLDEN WINDOW FIXTURES: total=%d captured=%d attempted_failed=%d "
        "relabellable_empty=%d | reclassify→EXPECTED_NO_FIXTURE=%d | real-failures-kept(in-truth)=%d",
        gw_fixtures,
        gw_captured,
        gw_failed,
        int((is_fixtures & in_golden & relabellable_empty).sum()),
        int(target.sum()),
        in_truth_failed,
    )

    if not args.apply:
        logger.info("DRY RUN — no write. Re-run with --apply (consolidator MUST be paused) to snapshot + rewrite.")
        return 0

    if int(target.sum()) == 0:
        logger.info("Nothing to reclassify — golden window FIXTURES already clean. No write.")
        return 0

    df.loc[target, "capture_status"] = _EMPTY
    df.loc[target, reason_col] = "EXPECTED_NO_FIXTURE"

    from datetime import datetime, timezone  # noqa: imports-inside-functions

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap = f"{bucket}/_index/snapshots/pre_golden_fixture_reclass_{ts}.parquet"
    fs.cp(blob, snap)
    logger.info("Snapshot written: gs://%s", snap)

    tmp_out = f"{tempfile.gettempdir()}/sports_index_fixreclass_out.parquet"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)
    logger.info("Rewrote _index: gs://%s (%d rows; %d cells reclassified)", blob, len(df), int(target.sum()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
