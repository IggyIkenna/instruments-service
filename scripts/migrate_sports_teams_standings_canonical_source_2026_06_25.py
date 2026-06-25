#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after TEAMS/STANDINGS footystats→api_football re-stamp is verified in prod
#   (verify via audit_canonical_form; plan sports_manifest_canonical_form_migration_2026_06_25)
"""Re-stamp TEAMS/STANDINGS source footystats → api_football in both the seed and canonical index.

Root cause (2026-06-25): early sports backfill runs stamped TEAMS/STANDINGS rows with
source=footystats / pipeline_mode=batch_footystats. The canonical SOURCE_PRIORITY for
TEAMS and STANDINGS is api_football (not footystats). This script corrects both stores:

  1. _legacy_seed.parquet    — 288,657 rows affected (source/pipeline_mode columns)
  2. _index/availability_index.parquet — 243,560 rows affected

Target mask: data_type in {TEAMS, STANDINGS} AND source == "footystats"
Re-stamp:    source → "api_football", pipeline_mode → "batch_api_football"
             asset_group → "sports" (if blank)

Snapshot taken before each --apply write. Dry-run by default.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_sports_teams_standings_canonical_source")

INDEX_BLOB = "_index/availability_index.parquet"
SEED_BLOB = "_legacy_seed.parquet"
SNAPSHOT_PREFIX = "_index/snapshots"

_TARGET_TYPES = frozenset({"TEAMS", "STANDINGS"})
_OLD_SOURCE = "footystats"
_OLD_PM = "batch_footystats"
_CANONICAL_SOURCE = "api_football"
_CANONICAL_PM = "batch_api_football"
_CANONICAL_AG = "sports"


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _blank(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def _stamp_df(df: pd.DataFrame, label: str) -> tuple[pd.DataFrame, int]:
    """Re-stamp footystats TEAMS/STANDINGS rows to api_football. Returns (df, n_stamped)."""
    for col in ("data_type", "source", "pipeline_mode", "asset_group"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    target_dt = df["data_type"].isin(_TARGET_TYPES)
    old_src = df["source"] == _OLD_SOURCE

    mask = target_dt & old_src
    n = int(mask.sum())
    logger.info("%s: rows matching TEAMS/STANDINGS + source=footystats: %d", label, n)

    if n == 0:
        return df, 0

    by_dt = df.loc[mask, "data_type"].value_counts().to_dict()
    by_status = df.loc[mask, "capture_status"].value_counts().to_dict() if "capture_status" in df.columns else {}
    logger.info("  By data_type: %s", by_dt)
    logger.info("  By capture_status: %s", by_status)

    df.loc[mask, "source"] = _CANONICAL_SOURCE
    df.loc[mask, "pipeline_mode"] = _CANONICAL_PM

    # Stamp blank asset_group while we're here
    if "asset_group" in df.columns:
        ag_blank = _blank(df["asset_group"]) & mask
        n_ag = int(ag_blank.sum())
        if n_ag:
            logger.info("  Also stamping blank asset_group → 'sports': %d rows", n_ag)
            df.loc[ag_blank, "asset_group"] = _CANONICAL_AG

    # Verify
    still_old = (df["data_type"].isin(_TARGET_TYPES)) & (df["source"] == _OLD_SOURCE)
    logger.info("%s: AFTER stamp — rows still source=footystats for TEAMS/STANDINGS: %d", label, int(still_old.sum()))

    return df, n


def _process_blob(
    fs: object,
    bucket: str,
    blob_path: str,
    blob_label: str,
    snap_name: str,
    tmp_in_name: str,
    tmp_out_name: str,
    apply: bool,
) -> int:
    """Download, re-stamp, optionally snapshot + rewrite. Returns n_stamped."""
    import gcsfs  # noqa: imports-inside-functions — script-only dep  # type: ignore[import]

    blob = f"{bucket}/{blob_path}"
    tmp_in = f"{tempfile.gettempdir()}/{tmp_in_name}"
    fs.get(blob, tmp_in)  # type: ignore[union-attr]
    df = pd.read_parquet(tmp_in)
    logger.info("%s: loaded %d rows from %s", blob_label, len(df), blob)

    df, n_stamped = _stamp_df(df, blob_label)

    if not apply:
        logger.info("%s: DRY-RUN — %d rows would be re-stamped. Pass --apply to write.", blob_label, n_stamped)
        return n_stamped

    if n_stamped == 0:
        logger.info("%s: nothing to stamp — skipping write.", blob_label)
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_blob = f"{bucket}/{SNAPSHOT_PREFIX}/{snap_name}_{ts}/availability_index.parquet"
    logger.info("%s: snapshotting → %s", blob_label, snap_blob)
    fs.copy(blob, snap_blob)  # type: ignore[union-attr]
    logger.info("%s: snapshot done.", blob_label)

    tmp_out = f"{tempfile.gettempdir()}/{tmp_out_name}"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)  # type: ignore[union-attr]
    logger.info("%s: wrote %d rows → %s", blob_label, len(df), blob)
    return n_stamped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the re-stamped parquets (default: dry-run)")
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug)")
    parser.add_argument("--skip-seed", action="store_true", help="Skip _legacy_seed.parquet (index only)")
    parser.add_argument("--skip-index", action="store_true", help="Skip availability_index.parquet (seed only)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    import gcsfs  # noqa: imports-inside-functions — script-only dep  # type: ignore[import]

    fs = gcsfs.GCSFileSystem()

    total_stamped = 0

    if not args.skip_seed:
        n = _process_blob(
            fs=fs,
            bucket=bucket,
            blob_path=SEED_BLOB,
            blob_label="seed",
            snap_name="pre_teams_standings_canon_source_seed",
            tmp_in_name="sports_seed_canon_in.parquet",
            tmp_out_name="sports_seed_canon_out.parquet",
            apply=args.apply,
        )
        total_stamped += n

    if not args.skip_index:
        n = _process_blob(
            fs=fs,
            bucket=bucket,
            blob_path=INDEX_BLOB,
            blob_label="index",
            snap_name="pre_teams_standings_canon_source_index",
            tmp_in_name="sports_index_canon_in.parquet",
            tmp_out_name="sports_index_canon_out.parquet",
            apply=args.apply,
        )
        total_stamped += n

    if not args.apply:
        logger.info("DRY-RUN complete. Total rows that would be re-stamped: %d. Pass --apply to write.", total_stamped)
    else:
        logger.info("Migration complete. Total rows re-stamped: %d", total_stamped)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
