#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: (footystats, ODDS) pending_fetch == 0 verified post-apply + consolidator merge
"""type_footystats_odds_non_covered_leagues_2026_06_29.py — type footystats ODDS eu/af rows for
leagues that footystats does not cover as EXPECTED_NO_PROVIDER_COVERAGE.

ROOT CAUSE (2026-06-19 / 2026-06-23): The IS expected-universe enumerator wrote
``expected_unattempted`` rows for footystats ODDS for 58 leagues that the footystats ODDS API
never returns data for (cup competitions, lower divisions, regional leagues not in footystats
ODDS coverage). These 6,294 eu rows (2026-02-20→2026-06-23) plus ~286 pre-existing ``attempted_failed``
rows for the same uncovered leagues block the ``(footystats, ODDS) pending_fetch == 0`` gate.

STRATEGY: Determine covered leagues dynamically — any league that has ever had a
``captured`` footystats ODDS row is considered covered. All other leagues with eu or af
status are typed as EXPECTED_NO_PROVIDER_COVERAGE.

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet``. The consolidator's last-write-wins merge picks the shard
rows (fresher ``written_at``) over the canonical index rows.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=type-fs-odds-$(date +%s) \\
      .venv/bin/python scripts/type_footystats_odds_non_covered_leagues_2026_06_29.py [--apply]
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
logger = logging.getLogger("type_footystats_odds_non_covered")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
_NO_PROVIDER_COVERAGE_REASON = "EXPECTED_NO_PROVIDER_COVERAGE"


def _build_mask(df: pd.DataFrame, covered_leagues: frozenset[str]) -> pd.Series:
    """True for footystats ODDS eu/af rows where league_id is NOT in covered_leagues."""
    cs = df["capture_status"].astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    league = df["league_id"].astype("string").fillna("")
    return (
        (dt == "ODDS")
        & (source == "footystats")
        & (cs.isin({"expected_unattempted", "attempted_failed"}))
        & (~league.isin(covered_leagues))
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

    # Determine covered leagues: any league with ≥1 captured footystats ODDS row.
    odds_rows = df[
        (df["data_type"].astype("string").str.upper() == "ODDS")
        & (df.get("source", pd.Series("", index=df.index)).astype("string") == "footystats")
    ]
    captured_leagues: frozenset[str] = frozenset(
        odds_rows[odds_rows["capture_status"].astype("string") == "captured"]["league_id"]
        .astype("string")
        .fillna("")
        .unique()
    )
    logger.info("Footystats ODDS covered leagues (≥1 captured row): %d", len(captured_leagues))
    logger.info("  Covered: %s", sorted(captured_leagues))

    mask = _build_mask(df, captured_leagues)
    n = int(mask.sum())
    logger.info("Footystats ODDS non-covered-league eu/af rows to type: %d", n)

    if n == 0:
        logger.info("Nothing to type — manifest already clean for non-covered footystats ODDS leagues.")
        return 0

    pending = df[mask]
    logger.info("  By capture_status: %s", dict(pending["capture_status"].value_counts()))
    logger.info("  Unique uncovered leagues: %d", pending["league_id"].nunique())
    logger.info("  Uncovered leagues: %s", sorted(pending["league_id"].unique()))
    logger.info("  Date range: %s to %s", pending["date"].min(), pending["date"].max())

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
