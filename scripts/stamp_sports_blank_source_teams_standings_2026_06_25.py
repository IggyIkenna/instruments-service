#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports _index has zero blank-source TEAMS/STANDINGS rows
#   (verify via audit_canonical_form or query; plan sports_manifest_canonical_form_migration_2026_06_25)
"""Stamp blank-source TEAMS/STANDINGS rows in the sports availability _index.

Root cause (2026-06-25): the CF234 script stamped source=<from pipeline_mode> but the
stamping condition required pipeline_mode to be set first. Rows that had BOTH blank source
AND blank pipeline_mode were skipped. TEAMS/STANDINGS were subsequently migrated to
source=api_football via the canonical-source migration, but ~46,844 empty_confirmed +
~32 attempted_failed rows (blank source, data_type in {TEAMS, STANDINGS}) remain.

Fix: for TEAMS/STANDINGS rows whose source is blank, stamp:
  source       = "api_football"
  pipeline_mode = "batch_api_football"  (if also blank)
  asset_group  = "sports"               (if also blank)

Snapshot-first before --apply. Dry-run by default.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import UTC

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stamp_sports_blank_source_teams_standings")

INDEX_BLOB = "_index/availability_index.parquet"
_TARGET_TYPES = frozenset({"TEAMS", "STANDINGS"})
_CANONICAL_SOURCE = "api_football"
_CANONICAL_PM = "batch_api_football"
_CANONICAL_AG = "sports"


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

    import gcsfs  # noqa: imports-inside-functions — script-only dep

    fs = gcsfs.GCSFileSystem()
    blob = f"{bucket}/{INDEX_BLOB}"
    tmp_in = f"{tempfile.gettempdir()}/sports_index_blank_src_in.parquet"
    fs.get(blob, tmp_in)
    df = pd.read_parquet(tmp_in)
    logger.info("Loaded _index: %d rows", len(df))

    for col in ("data_type", "source", "pipeline_mode", "asset_group", "capture_status"):
        if col not in df.columns:
            logger.error("MISSING required column %s — aborting", col)
            return 2

    src = df["source"].fillna("").astype(str).str.strip()
    pm = df["pipeline_mode"].fillna("").astype(str).str.strip()
    ag = df["asset_group"].fillna("").astype(str).str.strip()
    dt = df["data_type"].fillna("").astype(str).str.strip()

    src_blank = _blank(df["source"])
    pm_blank = _blank(df["pipeline_mode"])
    ag_blank = _blank(df["asset_group"])
    target_dt = dt.isin(_TARGET_TYPES)

    mask = src_blank & target_dt
    logger.info(
        "BEFORE: rows with blank source across all types=%d; TEAMS/STANDINGS with blank source=%d "
        "(empty_confirmed=%d, attempted_failed=%d)",
        int(src_blank.sum()),
        int(mask.sum()),
        int((mask & (df["capture_status"] == "empty_confirmed")).sum()),
        int((mask & (df["capture_status"] == "attempted_failed")).sum()),
    )

    if mask.sum() == 0:
        logger.info("Nothing to stamp — all TEAMS/STANDINGS rows already have source set.")
        return 0

    src = src.where(~mask, _CANONICAL_SOURCE)
    pm = pm.where(~(mask & pm_blank), _CANONICAL_PM)
    ag = ag.where(~(mask & ag_blank), _CANONICAL_AG)

    df["source"] = src
    df["pipeline_mode"] = pm
    df["asset_group"] = ag

    mask2 = _blank(df["source"]) & target_dt
    logger.info(
        "AFTER:  TEAMS/STANDINGS with blank source=%d (should be 0 after stamp)",
        int(mask2.sum()),
    )

    if not args.apply:
        logger.info("DRY RUN — no write. Re-run with --apply to snapshot + rewrite the _index.")
        return 0

    from datetime import datetime  # noqa: imports-inside-functions

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snap = f"{bucket}/_index/snapshots/pre_teams_standings_blank_src_{ts}.parquet"
    fs.cp(blob, snap)
    logger.info("Snapshot written: gs://%s", snap)

    tmp_out = f"{tempfile.gettempdir()}/sports_index_blank_src_out.parquet"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)
    logger.info("Stamped and rewrote _index: gs://%s (%d rows)", blob, len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
