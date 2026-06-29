#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after blank empty_confirmed backfill confirmed complete
"""backfill_blank_empty_confirmed_2026_06_28.py

Correct 194,470 ``empty_confirmed`` rows that have a BLANK ``error_reason``
in the cefi prd manifest — a violation of "honest absence must be typed."

Background
----------
The UTL ``record_empty()`` writer raises ``LegacyBlankErrorReasonError`` when
called without a typed reason (hardened 2026-05-07 after a RED ALERT incident
where CeFi VMs wrote 96-100% empty rows with blank error_reason, masking real
fetch failures).

Rows written before this enforcement have a blank ``error_reason``.  Without
re-running the adapters we cannot reconstruct the original reason.  The safest
corrective action is to flip them to ``expected_unattempted`` so the next
backfill VM re-attempts each shard and the writer assigns the correct typed
reason (``SOURCE_RETURNED_ZERO`` with FetchEvidence, or ``attempted_failed``
if the real error re-surfaces, or ``captured`` if data now exists).

This is the same pattern used by:
  - ``reclassify_cefi_manifest_mvp_universe_2026_06_23.py``
    (stale in-MVP empty_confirmed → expected_unattempted)
  - ``reset_source_returned_zero_manifest.py``
    (delete SOURCE_RETURNED_ZERO rows for re-attempt)

Flip contract
-------------
Rows must satisfy ALL of:
1. ``capture_status == "empty_confirmed"``
2. ``error_reason`` is blank (empty string or NaN/None)

Flip target
-----------
- ``capture_status`` → ``"expected_unattempted"``
- ``error_reason``   → ``""`` (already blank — no change needed)

Safety gates (ABORT before write if violated)
---------------------------------------------
1. Per-VM shard isolation: ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``
   must be set when ``--apply`` is given.
2. Rows at ``capture_status=captured`` or ``attempted_failed`` are NEVER touched.
3. Count of ``captured`` rows must be unchanged after the flip.

Source: Plan honest_coverage_v2_instrument_denominator_2026_06_28.md Phase 0 P0 #5.

Usage::

    # Default: dry-run (no mutations)
    cd instruments-service
    .venv/bin/python scripts/backfill_blank_empty_confirmed_2026_06_28.py

    # Apply
    MANIFEST_PER_VM_SHARDS=true VM_NAME=cefi-backfill-blank-ec-$(date +%s) \\
    .venv/bin/python scripts/backfill_blank_empty_confirmed_2026_06_28.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _identify_blank_ec_rows(df: pd.DataFrame) -> pd.Index:
    """Return index of empty_confirmed rows with blank error_reason."""
    if "capture_status" not in df.columns:
        logger.error("Manifest missing 'capture_status' column")
        return df.index[:0]

    status = df["capture_status"].fillna("").astype(str)
    is_ec = status == "empty_confirmed"

    if "error_reason" in df.columns:
        reason = df["error_reason"].fillna("").astype(str)
        is_blank = reason == ""
    else:
        logger.warning("Manifest missing 'error_reason' column — treating ALL empty_confirmed as blank")
        is_blank = is_ec.copy()
        is_blank[:] = True

    idx = df[is_ec & is_blank].index
    total_ec = int(is_ec.sum())
    logger.info(
        "Blank empty_confirmed rows: %d of %d total empty_confirmed (%.1f%%)",
        len(idx),
        total_ec,
        100.0 * len(idx) / total_ec if total_ec else 0.0,
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    sub = df.loc[idx]
    if "venue" in sub.columns:
        by_venue = sub["venue"].fillna("").value_counts().head(20)
        logger.info("Blank-ec rows by venue (top 20):\n%s", by_venue.to_string())
    if "data_type" in sub.columns:
        by_dt = sub["data_type"].fillna("").value_counts().head(15)
        logger.info("Blank-ec rows by data_type (top 15):\n%s", by_dt.to_string())
    if "day" in sub.columns:
        day_range = sub["day"].fillna("").astype(str)
        logger.info("Blank-ec row day range: %s → %s", day_range.min(), day_range.max())


def _validate_apply_env() -> bool:
    per_vm = os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower()
    vm_name = os.environ.get("VM_NAME", "").strip()
    ok = True
    if per_vm not in ("1", "true", "yes"):
        logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true. Aborting.")
        ok = False
    if not vm_name:
        logger.error("--apply requires VM_NAME=<unique-tag>. Aborting.")
        ok = False
    return ok


def _flip_to_expected_unattempted(
    bucket: str,
    df: pd.DataFrame,
    idx: pd.Index,
) -> None:
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())

    df.loc[idx, "capture_status"] = "expected_unattempted"
    # error_reason already blank — leave it

    captured_after = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    if captured_after != captured_before:
        logger.error(
            "SAFETY GATE FAILED: captured count changed %d → %d — aborting write",
            captured_before,
            captured_after,
        )
        raise RuntimeError(
            f"captured count changed {captured_before} → {captured_after} after flip — BUG"
        )

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, INDEX_BLOB, out)
    logger.info(
        "Uploaded manifest: %d rows, %d blank-ec rows flipped to expected_unattempted "
        "(captured count preserved at %d)",
        len(df),
        len(idx),
        captured_after,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help=(
            "Flip blank empty_confirmed rows to expected_unattempted. "
            "Requires MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique> env vars."
        ),
    )
    args = p.parse_args()

    dry_run = not args.apply

    if not dry_run and not _validate_apply_env():
        return 4

    bucket = _get_cefi_prd_bucket()
    logger.info("CeFi PRD manifest bucket: gs://%s", bucket)

    df = _load_manifest(bucket)
    blank_idx = _identify_blank_ec_rows(df)

    if len(blank_idx) == 0:
        logger.info("No blank empty_confirmed rows found. Nothing to do.")
        return 0

    _report_distribution(df, blank_idx)

    if dry_run:
        logger.info(
            "DRY-RUN: %d blank-ec rows would be flipped empty_confirmed→expected_unattempted. "
            "Re-run with --apply (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to mutate.",
            len(blank_idx),
        )
        return 0

    logger.info("APPLY: flipping %d blank-ec rows to expected_unattempted...", len(blank_idx))
    _flip_to_expected_unattempted(bucket, df, blank_idx)
    logger.info("Done. Next backfill VM will re-attempt these %d shards with typed reasons.", len(blank_idx))
    return 0


if __name__ == "__main__":
    sys.exit(main())
