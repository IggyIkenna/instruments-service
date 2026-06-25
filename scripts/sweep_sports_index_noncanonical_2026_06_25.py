#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports _index has zero non-canonical-form rows
#   (verify via audit_canonical_form; plan sports_manifest_canonical_form_migration_2026_06_25)
"""Sweep the sports availability _index for ALL remaining non-canonical-form rows.

Targets (from plan 2026-06-25 §2, cross-checked against live _index):

  1. Retired data_types → DELETE:
       TRANSFERMARKT_LEAGUES  (not in SOURCE_PRIORITY — retired provider key)
       SFI_LEAGUES            (retired)
       SFI_STANDINGS          (retired)
       odds (lowercase)       → rename to ODDS (case-normalisation; treated as canonical migrate)

  2. Blank data_type (127 rows) → DELETE (cannot recover canonical form without source-of-truth)

  3. Blank source on non-expected_unattempted rows where data_type is known → stamp from
     SOURCE_PRIORITY[("sports", data_type)][0]:
       TEAMS / STANDINGS blank-source empty_confirmed/attempted_failed → source=api_football
       Any other blank-source → derive from SOURCE_PRIORITY primary

  4. Blank pipeline_mode after source is known → stamp "batch_<source>"

  5. Blank asset_group → stamp "sports"

  6. Dedup on canonical shard key (date, data_type, league_id, source) keeping best status
     (captured > empty_confirmed > expected_unattempted > attempted_failed) to eliminate
     any duplicate rows introduced by earlier migrations.

Run dry-run first (default), then --apply to write. Snapshot taken before every --apply write.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sweep_sports_index_noncanonical")

INDEX_BLOB = "_index/availability_index.parquet"
SNAPSHOT_PREFIX = "_index/snapshots"

_EXEMPT_STATUS = {"expected_unattempted"}
_CAPTURE_STATUS_ORDER = {"captured": 0, "empty_confirmed": 1, "expected_unattempted": 2, "attempted_failed": 3}

# Retired data_type values — delete these rows entirely (not in SOURCE_PRIORITY)
_RETIRED_DATA_TYPES = frozenset({"TRANSFERMARKT_LEAGUES", "SFI_LEAGUES", "SFI_STANDINGS"})

# Lowercase aliases → canonical uppercase
_DATA_TYPE_RENAMES: dict[str, str] = {"odds": "ODDS"}

# SOURCE_PRIORITY primary for sports data types (derived from UAC; subset covering all in-index types)
_SPORTS_PRIMARY_SOURCE: dict[str, str] = {
    "ARBITRAGE": "odds_api",
    "FIXTURES": "api_football",
    "FIXTURE_EVENTS": "api_football",
    "FIXTURE_LINEUPS": "api_football",
    "FIXTURE_PLAYER_STATS": "api_football",
    "FIXTURE_STATS": "api_football",
    "INJURIES": "api_football",
    "LEAGUES": "api_football",
    "MATCHES": "footystats",
    "ODDS": "footystats",
    "ODDS_HORIZON_BUCKET": "mdps_odds_horizon_bucket",
    "ODDS_MOVEMENT": "odds_api",
    "ODDS_SNAPSHOT": "odds_api",
    "PLAYER_STATS": "api_football",
    "PLAYERS": "api_football",
    "PLAYER_VALUES": "transfermarkt",
    "PREDICTIONS": "footystats",
    "RESULTS": "api_football",
    "SFI_PROGRESSIVE_STATS": "soccer_football_info",
    "STANDINGS": "api_football",
    "TEAMS": "api_football",
    "TRANSFER_RECORDS": "transfermarkt",
    "UNDERSTAT_XG": "understat",
    "VENUES": "api_football",
    "WEATHER": "open_meteo",
    "WEATHER_FORECAST": "open_meteo",
    "XG": "understat",
    "XG_SHOTS": "understat",
}


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _blank(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the migrated index (default: dry-run)")
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    import gcsfs  # script-only dep — intentional lazy import

    fs = gcsfs.GCSFileSystem()
    blob = f"{bucket}/{INDEX_BLOB}"
    tmp_in = f"{tempfile.gettempdir()}/sports_index_noncanonical_in.parquet"
    fs.get(blob, tmp_in)
    df = pd.read_parquet(tmp_in)
    n_start = len(df)
    logger.info("Loaded _index: %d rows", n_start)

    for col in ("asset_group", "pipeline_mode", "source", "data_type", "capture_status"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # ── 1. Retired data_types: DELETE ──────────────────────────────────────────
    mask_retired = df["data_type"].isin(_RETIRED_DATA_TYPES)
    n_retired = int(mask_retired.sum())
    by_retired = df.loc[mask_retired, "data_type"].value_counts().to_dict()
    logger.info("Rows with RETIRED data_types: %d  (%s)", n_retired, by_retired)

    # ── 2. Blank data_type: DELETE ─────────────────────────────────────────────
    mask_blank_dt = _blank(df["data_type"])
    n_blank_dt = int(mask_blank_dt.sum())
    logger.info("Rows with BLANK data_type: %d", n_blank_dt)

    # Apply deletions
    mask_delete = mask_retired | mask_blank_dt
    df = df[~mask_delete].copy()
    logger.info("After deletions: %d rows (removed %d)", len(df), n_retired + n_blank_dt)

    # ── 3. Lowercase data_type aliases → canonical uppercase ───────────────────
    for lc, uc in _DATA_TYPE_RENAMES.items():
        mask_lc = df["data_type"] == lc
        n_lc = int(mask_lc.sum())
        if n_lc:
            logger.info("Renaming data_type %r → %r: %d rows", lc, uc, n_lc)
            df.loc[mask_lc, "data_type"] = uc

    # ── 4. Blank asset_group → stamp "sports" ─────────────────────────────────
    mask_blank_ag = _blank(df["asset_group"])
    n_blank_ag = int(mask_blank_ag.sum())
    if n_blank_ag:
        logger.info("Stamping blank asset_group → 'sports': %d rows", n_blank_ag)
        df.loc[mask_blank_ag, "asset_group"] = "sports"

    # ── 5. Blank source on non-exempt rows → derive from SOURCE_PRIORITY ───────
    mask_blank_src = _blank(df["source"]) & ~df["capture_status"].isin(_EXEMPT_STATUS)
    n_blank_src = int(mask_blank_src.sum())
    logger.info("Rows with blank source (non-exempt): %d", n_blank_src)
    if n_blank_src:
        by_dt = df.loc[mask_blank_src, "data_type"].value_counts().to_dict()
        logger.info("  By data_type: %s", by_dt)

    stamped_src = 0
    for dt, primary_src in _SPORTS_PRIMARY_SOURCE.items():
        mask = mask_blank_src & (df["data_type"] == dt)
        n = int(mask.sum())
        if n:
            logger.info("  Stamping source → %r for data_type=%s: %d rows", primary_src, dt, n)
            df.loc[mask, "source"] = primary_src
            stamped_src += n

    # Remaining blank-source rows with unknown data_type (shouldn't happen after above)
    still_blank_src = _blank(df["source"]) & ~df["capture_status"].isin(_EXEMPT_STATUS)
    n_still = int(still_blank_src.sum())
    if n_still:
        leftover_dt = df.loc[still_blank_src, "data_type"].value_counts().to_dict()
        logger.warning("STILL blank source after stamping (%d rows) — data_types: %s", n_still, leftover_dt)

    # ── 6. Blank pipeline_mode → stamp "batch_<source>" ───────────────────────
    mask_blank_pm = _blank(df["pipeline_mode"])
    # Only stamp where source is now known
    mask_src_set = ~_blank(df["source"])
    mask_stamp_pm = mask_blank_pm & mask_src_set
    n_blank_pm = int(mask_stamp_pm.sum())
    if n_blank_pm:
        logger.info("Stamping blank pipeline_mode → 'batch_<source>': %d rows", n_blank_pm)
        df.loc[mask_stamp_pm, "pipeline_mode"] = "batch_" + df.loc[mask_stamp_pm, "source"]

    still_blank_pm = _blank(df["pipeline_mode"])
    n_still_pm = int(still_blank_pm.sum())
    if n_still_pm:
        logger.warning("Still blank pipeline_mode after stamping: %d rows (source also blank)", n_still_pm)

    # ── 7. Dedup on canonical shard key ───────────────────────────────────────
    # Shard key: (date, data_type, league_id, source) — keep best capture_status
    shard_key = ["date", "data_type", "league_id", "source"]
    # Only dedup on rows where all key cols exist; pass-through rows missing league_id etc.
    missing_cols = [c for c in shard_key if c not in df.columns]
    if missing_cols:
        logger.warning("Shard key columns missing — skipping dedup: %s", missing_cols)
        n_dupes = 0
    else:
        df["_status_rank"] = df["capture_status"].map(_CAPTURE_STATUS_ORDER).fillna(99).astype(int)
        df.sort_values("_status_rank", inplace=True)
        n_before = len(df)
        df.drop_duplicates(subset=shard_key, keep="first", inplace=True)
        df.drop(columns=["_status_rank"], inplace=True)
        n_dupes = n_before - len(df)
        if n_dupes:
            logger.info("Dedup removed %d duplicate rows (kept best status per shard key)", n_dupes)

    n_end = len(df)
    logger.info(
        "Summary: start=%d removed=%d (retired=%d, blank_dt=%d, dupes=%d) "
        "stamped_ag=%d stamped_src=%d stamped_pm=%d end=%d",
        n_start,
        n_start - n_end,
        n_retired,
        n_blank_dt,
        n_dupes,
        n_blank_ag,
        stamped_src,
        n_blank_pm,
        n_end,
    )

    if not args.apply:
        logger.info("DRY-RUN complete. Pass --apply to write.")
        return 0

    # ── Snapshot before writing ────────────────────────────────────────────────
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_dir = f"{bucket}/{SNAPSHOT_PREFIX}/pre_noncanonical_sweep_{ts}"
    snap_blob = f"{snap_dir}/availability_index.parquet"
    logger.info("Snapshotting current index → %s", snap_blob)
    fs.copy(blob, snap_blob)
    logger.info("Snapshot done.")

    # ── Write back ─────────────────────────────────────────────────────────────
    tmp_out = f"{tempfile.gettempdir()}/sports_index_noncanonical_out.parquet"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)
    logger.info("Wrote %d rows → %s", n_end, blob)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
