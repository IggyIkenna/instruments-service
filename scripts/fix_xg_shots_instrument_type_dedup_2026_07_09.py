#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: XG_SHOTS instrument_type='shot' rows==0 in the sports canonical (verified post-apply + consolidator merge)
"""fix_xg_shots_instrument_type_dedup_2026_07_09.py — relabel the residual
understat XG_SHOTS ``instrument_type='shot'`` manifest rows to ``''`` so they
collapse against their blank-``instrument_type`` counterpart.

ROOT CAUSE: the understat XG_SHOTS producer (``instruments_service/engine/
orchestrator/understat.py``) wrote ``instrument_type='shot'`` on
``record_captured`` until ``instruments-service@4281a01d`` (2026-07-06) fixed
it to ``''`` — matching the sports-wide convention that the captured manifest
atom is per-(league_id, data_type, date), not per-shot (shot-vs-match grain is
carried by ``data_type`` alone). ``instrument_type`` is an
``_OPTIONAL_DEDUP_COLS`` member in ``manifest_consolidator.py``, so any row
carrying the non-empty ``'shot'`` value never dedups against a row of the same
logical fact written with the blank ``''``/``NULL`` convention — the two
partition into permanently-coexisting dedup-key groups.

The producer fix only prevents *future* writes from re-introducing the bug —
rows already written pre-fix with ``instrument_type='shot'`` sit in the
canonical forever until explicitly relabeled (a ``manifest_consolidator
--force`` full rebuild does NOT collapse them: window-dedup only merges rows
that already share an IDENTICAL key, and ``'shot'`` vs ``''``/``NULL`` are
different keys by design). This script performs that one-time relabel.

Scope: ONLY rows with ``data_type='XG_SHOTS'`` AND ``instrument_type='shot'``
(the exact residual pattern documented in
``plans/active/issues/sports_xg_shots_instrument_type_dedup_key_instability_2026_07_09.md``
— 5 groups, all ``date=2024-12-14``, big-5 leagues, ``capture_status=captured``).
Leaves already-blank rows untouched.

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet`` with a fresh ``attempted_at``/``written_at``
so the consolidator's last-write-wins window-dedup (ORDER BY attempted_at DESC,
written_at DESC) picks the corrected row over the stale ``'shot'`` row on the
next merge cycle — the canonical blob itself is never touched directly.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` +
``VM_NAME``. Run ``manifest_consolidator --force`` afterward to merge the
corrective shard and collapse the duplicate dedup-key groups.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=fix-xg-shots-dedup-$(date +%s) \\
      .venv/bin/python scripts/fix_xg_shots_instrument_type_dedup_2026_07_09.py [--apply]
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
logger = logging.getLogger("fix_xg_shots_instrument_type_dedup")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"


def _build_mask(df: pd.DataFrame) -> pd.Series:
    """True for XG_SHOTS rows still carrying the stale instrument_type='shot' tag."""
    dt = df["data_type"].astype("string").fillna("").str.upper()
    itype = df["instrument_type"].astype("string").fillna("")
    return (dt == "XG_SHOTS") & (itype == "shot")


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

    mask = _build_mask(df)
    n = int(mask.sum())
    logger.info("XG_SHOTS instrument_type='shot' rows to relabel: %d", n)

    if n == 0:
        logger.info("Nothing to relabel — XG_SHOTS instrument_type already clean.")
        return 0

    pending = df[mask]
    logger.info("  Unique leagues: %s", sorted(pending["league_id"].unique()))
    logger.info("  Per-league counts: %s", pending["league_id"].value_counts().to_dict())
    logger.info("  Unique dates: %d (%s)", pending["date"].nunique(), sorted(pending["date"].unique()))
    logger.info("  capture_status values present: %s", pending["capture_status"].value_counts().to_dict())

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    shard_df = df.loc[mask].copy()
    shard_df["instrument_type"] = ""
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
        "(consolidator --force merges next cycle → instrument_type='shot' collapses onto the blank-tagged row).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
