#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: SFI_PROGRESSIVE_STATS pending_fetch (expected_unattempted blank reason source=soccer_football_info) == 0 (verified post-apply + consolidator merge)
"""type_sfi_eu_no_provider_coverage_2026_06_27.py — type 200,967 phantom
``expected_unattempted`` SFI_PROGRESSIVE_STATS rows for non-expected soccer_football_info leagues.

ROOT CAUSE (2026-06-27): The sports manifest contains 200,967 rows where
  ``data_type=SFI_PROGRESSIVE_STATS``, ``source=soccer_football_info``,
  ``capture_status=expected_unattempted``, ``error_reason=""`` (blank). These rows were written
  by the instruments-service SFI scheduler for dates 2026-02-20 to 2026-06-26. All 200,967 rows
  are for leagues NOT in the soccer_football_info expected set
  (``get_expected_leagues_for_source("soccer_football_info", classifications=["Prediction"])`` →
  33 leagues). Non-expected leagues will never have SFI data, so the honest classification is
  ``EXPECTED_NO_PROVIDER_COVERAGE``.

FIX: type all pending SFI_PROGRESSIVE_STATS rows (expected_unattempted + blank reason +
source=soccer_football_info) as ``empty_confirmed(reason=EXPECTED_NO_PROVIDER_COVERAGE)``.

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet``. The consolidator's last-write-wins merge picks the shard
rows (fresher ``attempted_at``) over the canonical index rows → the typing WINS without touching
the canonical blob.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=type-sfi-eu-$(date +%s) \\
      .venv/bin/python scripts/type_sfi_eu_no_provider_coverage_2026_06_27.py [--apply]
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
logger = logging.getLogger("type_sfi_eu_no_provider_coverage")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
_NO_PROVIDER_COVERAGE_REASON = "EXPECTED_NO_PROVIDER_COVERAGE"


def _build_mask(df: pd.DataFrame) -> pd.Series:
    """True for SFI_PROGRESSIVE_STATS expected_unattempted rows with blank reason and source=soccer_football_info."""
    cs = df["capture_status"].astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    reason = df["error_reason"].astype("string").fillna("")
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    return (
        (dt == "SFI_PROGRESSIVE_STATS")
        & (cs == "expected_unattempted")
        & (reason == "")
        & (source == "soccer_football_info")
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the re-typed rows as a per-VM shard.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    instance = os.environ.get("VM_NAME", "")
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique>. Refusing."
        )
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
    logger.info("SFI_PROGRESSIVE_STATS pending_fetch rows to type: %d", n)

    if n == 0:
        logger.info("Nothing to type — manifest already clean for SFI soccer_football_info EU rows.")
        return 0

    pending = df[mask]
    logger.info("  Unique leagues: %d", pending["league_id"].nunique())
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
