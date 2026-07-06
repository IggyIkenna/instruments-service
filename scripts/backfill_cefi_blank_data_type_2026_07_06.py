#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after cefi blank data_type backfill verified complete post 2026-07-06

"""Backfill ``data_type='instruments'`` on IS CeFi availability index rows that
land with blank ``data_type`` since the 2026-06-29 regression.

REGRESSION (Issue is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md):
  Every cefi venue's IS captured shards since 2026-06-29 wrote ``data_type=""``
  (blank) into the availability index instead of the canonical
  ``data_type="instruments"``. Fleet-wide across all 26 cefi venues. Any
  downstream consumer that filters by
  ``capture_status=='captured' AND data_type=='instruments'`` (the canonical
  honest-coverage query) silently misses ~260 shards.

FIX (writer + this script):
  - Writer fix (writers.py:245) now stamps ``data_type="instruments"`` at
    ``record_captured`` call time — no new blank rows written.
  - This script promotes the historical blank rows already in the index to
    ``data_type="instruments"``, matching ``REFERENCE_DATA_TYPE`` in
    ``migrate_instruments_store_v9.py``.

Filter (per the issue doc's fix-worker todo):
  1. ``date >= 2026-06-27`` (transition window; pre-2026-06-27 rows already
     normalized by earlier migration runs)
  2. ``capture_status == "captured"``
  3. ``data_type`` is null OR empty string
  4. ``venue != ""`` (cefi venue-grain; excludes any accidental sports rows)

Typed values (e.g. ``pred``) are preserved — the filter only touches blank rows.
Idempotent: re-runs on a fixed index find 0 rows to change.

Dry-run by default; ``--apply --confirm`` writes the live ``_index``.

Usage::

    # inspect what will change
    cd instruments-service
    .venv/bin/python scripts/backfill_cefi_blank_data_type_2026_07_06.py

    # write the live _index
    .venv/bin/python scripts/backfill_cefi_blank_data_type_2026_07_06.py --apply --confirm

SSOT: plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
REFERENCE_DATA_TYPE = "instruments"  # matches migrate_instruments_store_v9.py:126
CUTOFF_DATE = "2026-06-27"


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _identify_target_rows(df: pd.DataFrame) -> pd.Index:
    """Return index of rows matching the issue-doc filter."""
    for col in ("date", "capture_status", "data_type", "venue"):
        if col not in df.columns:
            logger.error("Manifest missing required column '%s'", col)
            return df.index[:0]

    date_str = df["date"].astype(str)
    is_recent = date_str >= CUTOFF_DATE

    status = df["capture_status"].fillna("").astype(str)
    is_captured = status == "captured"

    data_type = df["data_type"].fillna("").astype(str)
    is_blank = data_type == ""

    venue = df["venue"].fillna("").astype(str)
    has_venue = venue != ""

    idx = df[is_recent & is_captured & is_blank & has_venue].index
    logger.info(
        "Target rows (date>=%s, captured, blank data_type, venue!=''): %d",
        CUTOFF_DATE,
        len(idx),
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    if len(idx) == 0:
        return
    sub = df.loc[idx]
    by_venue = sub["venue"].fillna("").value_counts().head(30)
    logger.info("Target rows by venue (top 30):\n%s", by_venue.to_string())
    dates = sub["date"].astype(str)
    logger.info("Target rows date range: %s → %s (%d unique)", dates.min(), dates.max(), dates.nunique())


def _apply_and_write(bucket: str, df: pd.DataFrame, idx: pd.Index) -> None:
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    typed_before = int((df["data_type"].fillna("").astype(str) != "").sum())

    df.loc[idx, "data_type"] = REFERENCE_DATA_TYPE

    captured_after = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    typed_after = int((df["data_type"].fillna("").astype(str) != "").sum())

    if captured_after != captured_before:
        raise RuntimeError(
            f"SAFETY: captured count changed {captured_before} → {captured_after} — aborting"
        )
    if typed_after != typed_before + len(idx):
        raise RuntimeError(
            f"SAFETY: typed data_type count delta {typed_after - typed_before} != expected {len(idx)} — aborting"
        )

    verified = df.loc[idx, "data_type"].astype(str) == REFERENCE_DATA_TYPE
    if not bool(verified.all()):
        raise RuntimeError("SAFETY: not every target row landed with the reference data_type — aborting")

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, INDEX_BLOB, out)
    logger.info(
        "Uploaded manifest: %d rows, %d blank cefi captured rows stamped data_type='%s' (captured=%d preserved)",
        len(df),
        len(idx),
        REFERENCE_DATA_TYPE,
        captured_after,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the live _index (requires --confirm)")
    p.add_argument("--confirm", action="store_true", help="Confirm a live --apply write")
    args = p.parse_args()

    if args.apply and not args.confirm:
        p.error("--apply requires --confirm")

    dry_run = not (args.apply and args.confirm)

    bucket = _get_cefi_prd_bucket()
    logger.info("CeFi PRD instruments-store bucket: gs://%s", bucket)

    df = _load_manifest(bucket)
    target_idx = _identify_target_rows(df)

    if len(target_idx) == 0:
        logger.info("No blank cefi captured rows post-%s. Nothing to do (idempotent).", CUTOFF_DATE)
        return 0

    _report_distribution(df, target_idx)

    if dry_run:
        logger.info(
            "DRY-RUN: %d blank cefi captured rows would be stamped data_type='%s'. "
            "Re-run with --apply --confirm to mutate.",
            len(target_idx),
            REFERENCE_DATA_TYPE,
        )
        return 0

    logger.info("APPLY: stamping %d blank cefi captured rows data_type='%s'...", len(target_idx), REFERENCE_DATA_TYPE)
    _apply_and_write(bucket, df, target_idx)
    logger.info("Done. Post-run verification: re-run without --apply → should report 0 target rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
