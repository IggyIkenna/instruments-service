#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports _index is CF-1..CF-12 GREEN (asset_group/pipeline_mode/source
#   stamped) on real data-state AND the change is verified via audit_canonical_form, then the
#   campaign plan sports_canonical_universe_and_apifootball_reference_expansion_2026_06_24 archives.
"""Targeted in-place CF-2/CF-3/CF-4 stamp for the sports availability `_index`.

The GCS object migration (migrate_sports_canonical_v9 --apply) canonicalised object PATHS
(inserted ``pipeline_mode=``) but does NOT touch `_index` COLUMNS. The audit_canonical_form
verdict on the live sports `_index` is: CF-1 (v9) GREEN, CF-5 (typed reasons) GREEN, CF-7 GREEN,
but THREE column gaps remain:
  CF-2  asset_group blank  (~1.31M rows)
  CF-3  pipeline_mode blank (~65-88k rows)
  CF-4  source blank on external cells (~1.40M; expected_unattempted EXEMPT per the auditor)

This script stamps ONLY those three columns, IN PLACE, PRESERVING every other value (league_id
already canonicalised, reasons already typed). It does NOT re-project/re-classify (the
rebuild_sports_manifest_v9 path REGRESSES source by dropping it on empty re-emits — verified
2026-06-24). Deterministic derivation:

  asset_group:   blank -> "sports"  (this is the sports bucket)
  pipeline_mode: blank + source set        -> "batch_" + source
                 blank + source blank       -> "batch_" + SPORTS_DATA_TYPE_TO_SOURCE[data_type]
  source:        blank + status != expected_unattempted (EXEMPT) + pipeline_mode set
                     -> pipeline_mode without the "batch_" prefix

Snapshot-first to `_index/snapshots/` before any --apply write; consolidator MUST be paused
during apply (it merges shards into the consolidated index every minute — pause to avoid a race,
resume after). Mirrors canonicalize_sports_league_id_schema_2026_06_24.py.
"""

from __future__ import annotations

import argparse
import logging
import tempfile

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("canonicalize_sports_index_cf234")

INDEX_BLOB = "_index/availability_index.parquet"
_BATCH = "batch_"
_EXEMPT_STATUS = {"expected_unattempted"}


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _blank(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the stamped index (default: dry-run)")
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    from unified_api_contracts import SPORTS_DATA_TYPE_TO_SOURCE  # noqa: imports-inside-functions

    import gcsfs  # noqa: imports-inside-functions — script-only dep

    fs = gcsfs.GCSFileSystem()
    blob = f"{bucket}/{INDEX_BLOB}"
    tmp_in = f"{tempfile.gettempdir()}/sports_index_cf234_in.parquet"
    fs.get(blob, tmp_in)
    df = pd.read_parquet(tmp_in)
    n = len(df)
    logger.info("Loaded _index: %d rows", n)

    for col in ("asset_group", "pipeline_mode", "source", "data_type", "capture_status"):
        if col not in df.columns:
            logger.error("MISSING required column %s — aborting", col)
            return 2

    ag = df["asset_group"].fillna("").astype(str).str.strip()
    pm = df["pipeline_mode"].fillna("").astype(str).str.strip()
    src = df["source"].fillna("").astype(str).str.strip()
    dt = df["data_type"].fillna("").astype(str).str.strip()
    status = df["capture_status"].fillna("").astype(str).str.strip()

    ag_blank = _blank(df["asset_group"])
    pm_blank = _blank(df["pipeline_mode"])
    src_blank = _blank(df["source"])
    logger.info(
        "BEFORE: asset_group blank=%d  pipeline_mode blank=%d  source blank=%d",
        int(ag_blank.sum()),
        int(pm_blank.sum()),
        int(src_blank.sum()),
    )

    # CF-2: asset_group blank -> sports
    ag = ag.where(~ag_blank, "sports")

    # CF-3: pipeline_mode blank -> from source, else from data_type->source map
    dt_to_src = {k: v for k, v in SPORTS_DATA_TYPE_TO_SOURCE.items()}
    derived_src_from_dt = dt.map(dt_to_src).fillna("")
    pm_from_src = ("batch_" + src).where(src != "", "")
    pm_from_dt = ("batch_" + derived_src_from_dt).where(derived_src_from_dt != "", "")
    pm = pm.where(~pm_blank, pm_from_src)
    pm = pm.where(pm != "", pm_from_dt)  # still blank -> data_type path

    # CF-4: source blank (non-exempt) -> pipeline_mode without its {mode}_ prefix.
    # pipeline_mode = {mode}_{source}; mode in {batch,live,replay}. source = everything after the
    # FIRST underscore (handles batch_api_football->api_football, live_odds_api->odds_api,
    # batch_mdps_odds_horizon_bucket->mdps_odds_horizon_bucket).
    exempt = status.isin(_EXEMPT_STATUS)
    pm_src = pm.str.split("_", n=1).str[1].fillna("").where(pm.str.contains("_"), "")
    fill_src = src_blank & (~exempt) & (pm_src != "")
    src = src.where(~fill_src, pm_src)

    df["asset_group"] = ag
    df["pipeline_mode"] = pm
    df["source"] = src

    ag_blank2 = _blank(df["asset_group"])
    pm_blank2 = _blank(df["pipeline_mode"])
    src_blank2 = _blank(df["source"])
    src_blank_nonexempt = src_blank2 & (~exempt)
    logger.info(
        "AFTER:  asset_group blank=%d  pipeline_mode blank=%d  source blank=%d (non-exempt blank=%d)",
        int(ag_blank2.sum()),
        int(pm_blank2.sum()),
        int(src_blank2.sum()),
        int(src_blank_nonexempt.sum()),
    )

    if not args.apply:
        logger.info("DRY RUN — no write. Re-run with --apply (consolidator MUST be paused) to snapshot + rewrite.")
        return 0

    from datetime import datetime, timezone  # noqa: imports-inside-functions

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap = f"{bucket}/_index/snapshots/pre_cf234_stamp_{ts}.parquet"
    fs.cp(blob, snap)
    logger.info("Snapshot written: gs://%s", snap)

    tmp_out = f"{tempfile.gettempdir()}/sports_index_cf234_out.parquet"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)
    logger.info("Rewrote _index: gs://%s (%d rows)", blob, len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
