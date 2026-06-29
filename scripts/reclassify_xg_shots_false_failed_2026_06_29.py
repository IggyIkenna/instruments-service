#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: XG_SHOTS attempted_failed==0 for native leagues (verified post-apply + consolidator merge)
"""reclassify_xg_shots_false_failed_2026_06_29.py — reclassify XG_SHOTS
``attempted_failed`` rows for native Understat leagues to ``empty_confirmed``.

ROOT CAUSE (2026-06-29): ``BaseSportsReferenceAdapter._classify_error`` performed
substring matching on the full exception message (which includes the URL). Understat
match IDs such as /getMatch/5401 caused ``"401" in msg`` to fire before
``status == 404`` was checked, returning INVALID_API_KEY (or RATE_LIMIT_EXCEEDED for
5429, FORBIDDEN for 5403) instead of HTTP_NOT_FOUND. This incremented
``_fetch_error_count`` causing the orchestrator to call ``record_failed(HTTP_NOT_FOUND)``
for the league on that date, instead of the correct ``record_empty(EXPECTED_NO_FIXTURE)``.

FIX: ``instruments-service@7bb8c26`` — prioritise HTTP status over substring matching.
After the fix, per-match 404s from /getMatch/ never increment ``_fetch_error_count``
and therefore resolve to ``record_empty``.

CLEANUP: This script reclassifies the accumulated ``attempted_failed`` XG_SHOTS rows
(written by pre-fix VMs) to ``empty_confirmed(EXPECTED_NO_FIXTURE)``.  Scope is
restricted to the 5 native Understat leagues only (EPL, LA_LIGA, BUNDESLIGA, SERIE_A,
LIGUE_1); non-native leagues have separate EXPECTED_NO_PROVIDER_COVERAGE typing.

Run AFTER the VM ``us-backfill-20260628-070120`` TERMINATES and the consolidator
merges its final shard (wait ~1 min post-TERMINATED).

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet``. The consolidator's last-write-wins merge picks the
shard rows (fresher ``written_at``) over the stale failed rows → the typing WINS
without touching the canonical blob.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=reclassify-xg-shots-$(date +%s) \\
      .venv/bin/python scripts/reclassify_xg_shots_false_failed_2026_06_29.py [--apply]
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
logger = logging.getLogger("reclassify_xg_shots_false_failed")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"

_NATIVE_LEAGUES: frozenset[str] = frozenset({"EPL", "LA_LIGA", "BUNDESLIGA", "SERIE_A", "LIGUE_1"})
_EXPECTED_NO_FIXTURE_REASON = "EXPECTED_NO_FIXTURE"


def _build_mask(df: pd.DataFrame) -> pd.Series:
    """True for XG_SHOTS attempted_failed rows for native leagues with HTTP_NOT_FOUND."""
    cs = df["capture_status"].astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    reason = df["error_reason"].astype("string").fillna("")
    league = df["league_id"].astype("string").fillna("")
    return (
        (dt == "XG_SHOTS")
        & (cs == "attempted_failed")
        & (reason == "HTTP_NOT_FOUND")
        & (league.isin(_NATIVE_LEAGUES))
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
    logger.info("XG_SHOTS false-failed rows to reclassify: %d", n)

    if n == 0:
        logger.info("Nothing to reclassify — XG_SHOTS attempted_failed already clean for native leagues.")
        return 0

    pending = df[mask]
    logger.info("  Unique native leagues: %s", sorted(pending["league_id"].unique()))
    logger.info("  Per-league counts: %s", pending["league_id"].value_counts().to_dict())
    logger.info("  Date range: %s to %s", pending["date"].min(), pending["date"].max())
    logger.info("  Unique dates: %d", pending["date"].nunique())

    # Sanity check: all should be HTTP_NOT_FOUND
    other_reasons = pending[pending["error_reason"] != "HTTP_NOT_FOUND"]["error_reason"].value_counts()
    if not other_reasons.empty:
        logger.warning("Unexpected error reasons (will NOT reclassify these): %s", other_reasons.to_dict())

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    shard_df = df.loc[mask].copy()
    shard_df["capture_status"] = "empty_confirmed"
    shard_df["error_reason"] = _EXPECTED_NO_FIXTURE_REASON
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
        "APPLIED. Wrote %d re-typed rows as per-VM shard gs://%s/%s "
        "(consolidator merges next cycle → XG_SHOTS attempted_failed=0 for native leagues).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
