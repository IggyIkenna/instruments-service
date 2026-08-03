#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: (transfermarkt, PLAYER_VALUES) pending_fetch == 0 verified post-apply + consolidator merge
"""type_tm_non_provider_coverage_2026_06_27.py — type 71 non-Transfermarkt PLAYER_VALUES league rows
as EXPECTED_NO_PROVIDER_COVERAGE for the 2026-02-20→2026-06-26 gap range.

ROOT CAUSE (2026-06-27): The sports manifest contains expected_unattempted rows where
  ``data_type=PLAYER_VALUES``, ``source=transfermarkt``, ``capture_status=expected_unattempted``
  for 126 canonical league_ids in the range 2026-02-20→2026-06-26 (15,589 rows total, ~122/day).
  The instruments-service Transfermarkt orchestrator only covers 55 leagues (Prediction + Features
  from ``get_expected_leagues_for_source("transfermarkt")``) — the other 71 leagues are cup
  competitions, lower divisions, and regional leagues NOT covered by the Transfermarkt provider.
  The VM ``tm-backfill-20260627-222604`` resolves the 55 TM-covered leagues; this script resolves
  the remaining 71 as EXPECTED_NO_PROVIDER_COVERAGE.

FIX: type all expected_unattempted PLAYER_VALUES rows with source=transfermarkt where
league_id is in the 71 non-TM leagues (cup competitions, lower divisions not covered by
Transfermarkt player values API).

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet``. The consolidator's last-write-wins merge picks the shard
rows (fresher ``attempted_at``) over the canonical index rows.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=type-tm-non-prov-$(date +%s) \\
      .venv/bin/python scripts/type_tm_non_provider_coverage_2026_06_27.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("type_tm_non_provider_coverage")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
_NO_PROVIDER_COVERAGE_REASON = "EXPECTED_NO_PROVIDER_COVERAGE"

# The 55 Transfermarkt-covered leagues (Prediction + Features from
# get_expected_leagues_for_source("transfermarkt")) — these are handled by the VM.
# All OTHER expected_unattempted PLAYER_VALUES leagues in the gap range are
# cup competitions / lower divisions NOT covered by the TM provider.
_TM_COVERED_LEAGUES: frozenset[str] = frozenset(
    {
        "ALLSVENSKAN",
        "ARGENTINA_PRIMERA",
        "ARGENTINA_PRIMERA_NACIONAL",
        "AUSTRIAN_2_LIGA",
        "AUSTRIAN_BUNDESLIGA",
        "A_LEAGUE",
        "BELGIAN_FIRST_B",
        "BRASILEIRAO",
        "BRASILEIRAO_SERIE_B",
        "BUNDESLIGA",
        "BUNDESLIGA_2",
        "CHILE_PRIMERA",
        "CHILE_PRIMERA_B",
        "DANISH_1ST_DIVISION",
        "DANISH_SUPERLIGA",
        "EERSTE_DIVISIE",
        "EKSTRAKLASA",
        "ELITESERIEN",
        "ENG_CHAMPIONSHIP",
        "ENG_LEAGUE_ONE",
        "ENG_LEAGUE_TWO",
        "ENG_NATIONAL_LEAGUE",
        "EPL",
        "EREDIVISIE",
        "FRANCE_NATIONAL",
        "GREEK_SUPER_LEAGUE",
        "GREEK_SUPER_LEAGUE_2",
        "J1_LEAGUE",
        "J2_LEAGUE",
        "JUPILER_PRO",
        "K_LEAGUE_1",
        "K_LEAGUE_2",
        "LA_LIGA",
        "LIGA_3",
        "LIGA_EXPANSION_MX",
        "LIGA_MX",
        "LIGA_PORTUGAL_2",
        "LIGUE_1",
        "LIGUE_2",
        "MLS",
        "NORWAY_1_DIVISJON",
        "POLAND_I_LIGA",
        "PRIMEIRA_LIGA",
        "PRIMERA_RFEF",
        "SCOTTISH_CHAMPIONSHIP",
        "SCOTTISH_PREMIERSHIP",
        "SEGUNDA_DIVISION",
        "SERIE_A",
        "SERIE_B",
        "SUPERETTAN",
        "SUPER_LIG",
        "SWISS_CHALLENGE_LEAGUE",
        "SWISS_SUPER_LEAGUE",
        "TFF_FIRST_LEAGUE",
        "USL_CHAMPIONSHIP",
    }
)

# Gap range dates
_GAP_START = "2026-02-20"
_GAP_END = "2026-06-26"


def _build_mask(df: pd.DataFrame) -> pd.Series:
    """True for PLAYER_VALUES expected_unattempted rows with source=transfermarkt
    where league_id is NOT in the 55 TM-covered leagues (i.e., the 71 non-TM leagues)."""
    cs = df["capture_status"].astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    reason = df["error_reason"].astype("string").fillna("")
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    league = df["league_id"].astype("string").fillna("")
    date_col = df["date"].astype("string").fillna("")
    return (
        (dt == "PLAYER_VALUES")
        & (cs == "expected_unattempted")
        & (reason == "")
        & (source == "transfermarkt")
        & (~league.isin(_TM_COVERED_LEAGUES))
        & (date_col >= _GAP_START)
        & (date_col <= _GAP_END)
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the re-typed rows as a per-VM shard.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    instance = os.environ.get("VM_NAME", "")  # noqa: qg-empty-fallback — unset VM_NAME => `not instance` refuses --apply below
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique>. Refusing.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved bucket %s is not the expected env-short shape. Refusing.", bucket)
        return 1

    fs = gcsfs.GCSFileSystem()
    logger.info("Reading live _index gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)

    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")

    mask = _build_mask(df)
    n = int(mask.sum())
    logger.info("PLAYER_VALUES transfermarkt non-TM-covered rows to type: %d", n)

    if n == 0:
        logger.info("Nothing to type — manifest already clean for non-TM league PLAYER_VALUES rows.")
        return 0

    pending = df[mask]
    logger.info("  Unique leagues: %d", pending["league_id"].nunique())
    logger.info("  League list: %s", sorted(pending["league_id"].unique()))
    logger.info("  Date range: %s to %s", pending["date"].min(), pending["date"].max())
    logger.info("  Unique dates: %d", pending["date"].nunique())

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    shard_df = df.loc[mask].copy()
    shard_df["capture_status"] = "empty_confirmed"
    shard_df["error_reason"] = _NO_PROVIDER_COVERAGE_REASON
    shard_df["attempted_at"] = now_iso
    if "written_at" in shard_df.columns:
        shard_df["written_at"] = now_iso
    if "schema_version" in shard_df.columns:
        shard_df["schema_version"] = 9

    shard_path = _PER_VM_PATH_TEMPLATE.format(instance=instance)
    sbuf = io.BytesIO()
    shard_df.to_parquet(sbuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    sbuf.seek(0)
    with fs.open(f"{bucket}/{shard_path}", "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info(
        "APPLIED. Wrote %d re-typed rows as per-VM shard gs://%s/%s (consolidator merges next cycle).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
