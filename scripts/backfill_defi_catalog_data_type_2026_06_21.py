#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: IS defi catalog data_type backfill verified complete post 2026-06-21

"""Backfill ``data_type='instrument-catalog'`` on all IS DeFi availability index rows.

ROOT CAUSE (Blocker B in data_completion_to_100_all_ag_2026_06_21.md):
  The deployed IS ``_write_catalogue_record()`` enters the ``elif '-' in venue_str:``
  DeFi branch but does NOT set ``manifest_data_type = 'instrument-catalog'`` before
  parsing venue/chain.  Result: ALL 145,467 rows in the IS defi availability index
  have ``data_type = ''``.

  The UTL preflight runner ``_filter_index()`` requires
  ``data_type == 'instrument-catalog'``  →  0 rows match  →
  ``get_last_attempted_at()`` returns ``None``  →  ``PreflightFailedError``  →
  ``assert_defi_catalog_fresh`` returns ``False``  →  ALL 60 MTDS DeFi backfill VMs
  route honest absence instead of capturing data.

FIX (this script):
  Set ``data_type = 'instrument-catalog'`` on every row in the IS defi availability
  index (all rows in this bucket ARE instrument-catalog data by definition).
  Also coerce ``asset_group = None`` → ``'defi'`` so the asset_group filter passes.

  The source-code fix (``e8acef1`` on LDR) ensures new IS writes set the column
  correctly.  This script repairs the historical rows already in the index.

Idempotent: rows already with ``data_type='instrument-catalog'`` are left unchanged.
Dry-run by default; ``--apply --confirm`` writes the live _index.

Usage::

    # inspect what will change
    python scripts/backfill_defi_catalog_data_type_2026_06_21.py

    # write the live _index
    python scripts/backfill_defi_catalog_data_type_2026_06_21.py --apply --confirm

SSOT: plans/active/data_completion_to_100_all_ag_2026_06_21.md (Blocker B task -008).
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd
from unified_trading_library import get_storage_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_CATALOG_DATA_TYPE = "instrument-catalog"
_DEFI_ASSET_GROUP = "defi"


def _fix_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Return ``(fixed_df, n_data_type_fixed, n_asset_group_fixed)``."""
    out = df.copy()

    # 1) Fix data_type column
    needs_dt = out["data_type"].astype(str) != _CATALOG_DATA_TYPE
    n_dt = int(needs_dt.sum())
    if n_dt:
        out.loc[needs_dt, "data_type"] = _CATALOG_DATA_TYPE

    # 2) Fix asset_group column — None / NaN / '' → 'defi'
    if "asset_group" in out.columns:
        needs_ag = out["asset_group"].isna() | (out["asset_group"].astype(str).isin({"None", "", "nan"}))
        n_ag = int(needs_ag.sum())
        if n_ag:
            out.loc[needs_ag, "asset_group"] = _DEFI_ASSET_GROUP
    else:
        n_ag = 0

    return out, n_dt, n_ag


def _process_bucket(bucket: str, *, apply: bool) -> None:
    client = get_storage_client()
    logger.info("── gs://%s/%s ──", bucket, _INDEX_BLOB)
    try:
        raw = client.download_bytes(bucket, _INDEX_BLOB)
    except Exception as exc:
        logger.warning("  skip — cannot download index (%s)", exc)
        return

    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("  rows=%d  data_type uniques=%s", len(df), df["data_type"].unique().tolist()[:6])
    logger.info(
        "  asset_group uniques=%s", df["asset_group"].unique().tolist()[:6] if "asset_group" in df.columns else "N/A"
    )
    logger.info("  capture_status uniques=%s", df["capture_status"].unique().tolist())
    logger.info("  date range: %s — %s", df["date"].min(), df["date"].max())

    fixed_df, n_dt, n_ag = _fix_frame(df)

    logger.info("  data_type changes: %d rows  ('' → '%s')", n_dt, _CATALOG_DATA_TYPE)
    logger.info("  asset_group changes: %d rows  (None/'' → '%s')", n_ag, _DEFI_ASSET_GROUP)

    if n_dt == 0 and n_ag == 0:
        logger.info("  ✓ already correct — nothing to write")
        return

    # Verify the fix will satisfy the preflight filter
    captured_after = fixed_df[
        (fixed_df["data_type"].astype(str) == _CATALOG_DATA_TYPE)
        & (fixed_df["capture_status"].astype(str) == "captured")
        & (fixed_df["asset_group"].astype(str) == _DEFI_ASSET_GROUP)
    ]
    logger.info(
        "  post-fix: %d rows will satisfy preflight filter (data_type=instrument-catalog, captured, asset_group=defi)",
        len(captured_after),
    )
    if captured_after.empty:
        logger.error("  ABORT: 0 rows would satisfy preflight after fix — something is wrong")
        return

    # Sample of date coverage
    dates = sorted(captured_after["date"].unique())
    logger.info("  date coverage: %d unique dates (%s … %s)", len(dates), dates[0], dates[-1])

    if not apply:
        logger.info("  (dry-run — pass --apply --confirm to write)")
        return

    buf = io.BytesIO()
    fixed_df.to_parquet(buf, index=False)
    client.upload_bytes(bucket, _INDEX_BLOB, buf.getvalue())
    logger.info(
        "  ✅ APPLIED — wrote fixed _index (%d rows, %d data_type fixed, %d asset_group fixed, %.1f KB)",
        len(fixed_df),
        n_dt,
        n_ag,
        len(buf.getvalue()) / 1024,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill data_type='instrument-catalog' on IS DeFi availability index.")
    ap.add_argument(
        "--bucket",
        default="instruments-store-defi-prd-central-element-323112",
        help="IS DeFi availability index bucket (default: prod)",
    )
    ap.add_argument("--apply", action="store_true", help="Write the live _index (requires --confirm)")
    ap.add_argument("--confirm", action="store_true", help="Confirm a live --apply write")
    args = ap.parse_args()

    if args.apply and not args.confirm:
        ap.error("--apply requires --confirm")

    apply = args.apply and args.confirm
    _process_bucket(args.bucket, apply=apply)


if __name__ == "__main__":
    main()
