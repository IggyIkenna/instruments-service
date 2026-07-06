#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after cefi blank-data_type backfill verified complete post 2026-07-06

"""Backfill ``data_type='instruments'`` on blank/null cefi captured rows since 2026-06-27.

Context
-------
Between 2026-06-29 and 2026-07-06 the IS orchestrator emitted ``data_type=""``
(blank) instead of the canonical ``data_type="instruments"`` on every cefi
availability-index captured row. The writer regression was fixed at
instruments-service@46ba62b (writers.py:239 — the ``record_captured`` call now
passes ``data_type="instruments"`` on the cefi/tradfi/defi non-sports path).
This script rewrites the ~260 historical blank rows in
``instruments-store-cefi-prd/_index/availability_index.parquet`` to the
canonical value so downstream honest-coverage queries
(``capture_status == 'captured' AND data_type == 'instruments'``) stop
silently miscounting them as absent.

Filter (task-002 in plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md):

    date >= '2026-06-27'
    AND capture_status == 'captured'
    AND (data_type is null OR data_type == '')
    AND venue != ''

Rewrite:

    data_type = 'instruments'      # matches REFERENCE_DATA_TYPE in
                                   # scripts/migrate_instruments_store_v9.py:126

Idempotent: rows already at ``data_type=='instruments'`` are not selected, so a
second run is a no-op that logs "nothing to do" and exits 0.

Safety gates (ABORT before write if violated):
1. Total row count unchanged.
2. Count of ``captured`` rows unchanged (we only mutate ``data_type``).
3. Post-fix filter matches 0 rows (in-memory verification before upload).

SSOT: plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md (task-002).

Usage::

    cd instruments-service

    # dry-run (default)
    .venv/bin/python scripts/backfill_cefi_blank_data_type_2026_07_06.py

    # write the live _index
    .venv/bin/python scripts/backfill_cefi_blank_data_type_2026_07_06.py --apply --confirm
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

_INDEX_BLOB = "_index/availability_index.parquet"
_CUTOFF_DATE = "2026-06-27"  # inclusive — matches plan-issue task-002 filter
_TARGET_DATA_TYPE = "instruments"  # matches REFERENCE_DATA_TYPE in migrate_instruments_store_v9.py:126


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi", deployment_env="prd"
    )


def _load_index(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, _INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded index: %d rows from gs://%s/%s", len(df), bucket, _INDEX_BLOB)
    return df


def _identify_target_rows(df: pd.DataFrame) -> pd.Index:
    """Rows matching (date >= cutoff, captured, blank/null data_type, venue != '')."""
    for col in ("data_type", "capture_status", "date", "venue"):
        if col not in df.columns:
            logger.error("Index missing required column: %s", col)
            return df.index[:0]

    date_str = df["date"].astype(str)
    capture_status = df["capture_status"].fillna("").astype(str)
    data_type = df["data_type"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)

    mask = (
        (date_str >= _CUTOFF_DATE)
        & (capture_status == "captured")
        & (data_type == "")
        & (venue != "")
    )
    idx = df[mask].index
    logger.info(
        "Target rows (blank cefi captured, date>=%s, venue!=''): %d",
        _CUTOFF_DATE,
        len(idx),
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    sub = df.loc[idx]
    by_venue = sub["venue"].astype(str).value_counts().head(30)
    logger.info("Target rows by venue (top 30):\n%s", by_venue.to_string())
    dates = sorted(sub["date"].astype(str).unique())
    logger.info("Date coverage: %d unique dates (%s … %s)", len(dates), dates[0], dates[-1])


def _rewrite_and_upload(bucket: str, df: pd.DataFrame, idx: pd.Index) -> None:
    rows_before = len(df)
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())

    df.loc[idx, "data_type"] = _TARGET_DATA_TYPE

    if len(df) != rows_before:
        raise RuntimeError(f"SAFETY GATE FAILED: row count changed {rows_before} → {len(df)}")

    captured_after = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    if captured_before != captured_after:
        raise RuntimeError(
            f"SAFETY GATE FAILED: captured count changed {captured_before} → {captured_after}"
        )

    # In-memory verification: post-fix, the filter must match 0 rows.
    date_str = df["date"].astype(str)
    capture_status = df["capture_status"].fillna("").astype(str)
    data_type = df["data_type"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    remaining_blank = int(
        (
            (date_str >= _CUTOFF_DATE)
            & (capture_status == "captured")
            & (data_type == "")
            & (venue != "")
        ).sum()
    )
    if remaining_blank != 0:
        raise RuntimeError(
            f"SAFETY GATE FAILED: {remaining_blank} blank cefi captured rows would remain post-fix"
        )

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, _INDEX_BLOB, buf)
    logger.info(
        "Uploaded index: %d rows total, %d rows updated data_type='' → '%s' "
        "(captured count preserved at %d)",
        len(df),
        len(idx),
        _TARGET_DATA_TYPE,
        captured_after,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--apply", action="store_true", help="Write the live _index (requires --confirm)"
    )
    ap.add_argument(
        "--confirm", action="store_true", help="Confirm a live --apply write"
    )
    args = ap.parse_args()

    if args.apply and not args.confirm:
        ap.error("--apply requires --confirm")

    dry_run = not (args.apply and args.confirm)

    bucket = _get_cefi_prd_bucket()
    logger.info("Cefi PRD IS bucket: gs://%s", bucket)

    df = _load_index(bucket)
    idx = _identify_target_rows(df)

    if len(idx) == 0:
        logger.info(
            "No target rows — already canonical or no blank cefi captures since %s. Nothing to do.",
            _CUTOFF_DATE,
        )
        return 0

    _report_distribution(df, idx)

    if dry_run:
        logger.info(
            "DRY-RUN: %d rows would be updated data_type='' → '%s'. "
            "Re-run with --apply --confirm to mutate.",
            len(idx),
            _TARGET_DATA_TYPE,
        )
        return 0

    logger.info(
        "APPLY: rewriting %d rows data_type='' → '%s'...", len(idx), _TARGET_DATA_TYPE
    )
    _rewrite_and_upload(bucket, df, idx)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
