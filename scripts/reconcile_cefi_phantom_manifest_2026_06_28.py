#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after phantom reconciliation confirmed complete in cefi prd manifest
"""One-off runner: flip 12,958 phantom_captured_no_parquet rows in cefi prd manifest.

Target: gs://market-data-tick-cefi-prd-central-element-323112/_index/availability_index.parquet

A phantom row claims ``capture_status=captured`` in the manifest but no parquet
exists at the canonical GCS path.  These 12,958 cefi prd rows block the
orchestrator's ``_should_skip_shard`` pre-flight (trusts the manifest) from ever
retrying those shards.

This script is a thin wrapper around ``reconcile_phantom_manifest_rows_all.py``
that hard-wires the target to the cefi PRD bucket via
``resolve_bucket_name(..., deployment_env="prd")``.

Source: Plan honest_coverage_v2_instrument_denominator_2026_06_28.md Phase 0 P1.

Usage::

    # Default: dry-run (no mutations)
    cd instruments-service
    .venv/bin/python scripts/reconcile_cefi_phantom_manifest_2026_06_28.py --dry-run

    # Apply: flip phantom rows to attempted_failed
    MANIFEST_PER_VM_SHARDS=true VM_NAME=cefi-phantom-flip-$(date +%s) \\
    .venv/bin/python scripts/reconcile_cefi_phantom_manifest_2026_06_28.py --apply

Idempotent: rows already at ``attempted_failed`` are skipped by the underlying audit.

Per-VM shard isolation: ``--apply`` requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>`` env vars.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

# The 12,958 cefi prd phantom rows carry this error_reason (set by a prior
# audit run that flagged them but did not flip them — or by the orchestrator).
PHANTOM_ERROR_REASON = "phantom_captured_no_parquet_at_canonical_path"


def _get_cefi_prd_bucket() -> str:
    """Resolve the cefi PRD market-data bucket via UTL SSOT.

    ``deployment_env="prd"`` targets the PRD tier explicitly without
    mutating the process environment — the canonical approach per
    ``resolve_bucket_name`` docs (added 2026-05 for per-env targeting).
    """
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _identify_phantoms(df: pd.DataFrame) -> pd.Index:
    """Return index of rows that are phantom-captured.

    A phantom row has:
    - ``capture_status == "captured"``
    - ``error_reason == "phantom_captured_no_parquet_at_canonical_path"``

    These were previously detected by the audit pass but not yet flipped.
    Rows already at ``attempted_failed`` are skipped (idempotent).
    """
    if "capture_status" not in df.columns:
        logger.error("Manifest missing 'capture_status' column — cannot proceed")
        return df.index[:0]
    status = df["capture_status"].fillna("").astype(str)
    phantom_captured_mask = status == "captured"

    # Narrow to rows whose error_reason already marks them as phantom
    # (these are the rows the prior audit flagged but left in captured state,
    # OR rows that have the canonical phantom error_reason).
    if "error_reason" in df.columns:
        error_reason = df["error_reason"].fillna("").astype(str)
        phantom_captured_mask = phantom_captured_mask & (error_reason == PHANTOM_ERROR_REASON)

    idx = df[phantom_captured_mask].index
    logger.info("Phantom-captured rows (captured + phantom error_reason): %d", len(idx))
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    phantom_df = df.loc[idx]
    if "venue" in phantom_df.columns:
        by_venue = phantom_df["venue"].value_counts().head(15)
        logger.info("Phantom distribution by venue (top 15):\n%s", by_venue.to_string())
    if "data_type" in phantom_df.columns:
        by_dt = phantom_df["data_type"].value_counts().head(15)
        logger.info("Phantom distribution by data_type (top 15):\n%s", by_dt.to_string())


def _validate_apply_env() -> bool:
    """Check that per-VM shard isolation env vars are set for --apply mode."""
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


def _flip_phantoms(
    bucket: str,
    df: pd.DataFrame,
    idx: pd.Index,
) -> None:
    """Flip phantom rows captured→attempted_failed and write back to GCS."""
    now_iso = datetime.now(UTC).isoformat()
    df.loc[idx, "capture_status"] = "attempted_failed"
    df.loc[idx, "error_reason"] = PHANTOM_ERROR_REASON
    df.loc[idx, "attempted_at"] = now_iso

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, INDEX_BLOB, out)
    logger.info(
        "Uploaded reconciled manifest: %d rows, %d phantoms flipped to attempted_failed",
        len(df),
        len(idx),
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Default: scan and report phantom rows, no mutations.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help=(
            "Flip phantom-captured rows to attempted_failed in the cefi prd manifest. "
            "Requires MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique> env vars."
        ),
    )
    args = p.parse_args()

    if args.apply:
        args.dry_run = False
    else:
        args.dry_run = True

    bucket = _get_cefi_prd_bucket()
    logger.info("CeFi PRD manifest bucket: gs://%s", bucket)

    if not args.dry_run and not _validate_apply_env():
        return 4

    df = _load_manifest(bucket)
    phantom_idx = _identify_phantoms(df)

    if len(phantom_idx) == 0:
        logger.info("No phantom-captured rows found. Manifest is already clean.")
        return 0

    _report_distribution(df, phantom_idx)

    if args.dry_run:
        logger.info(
            "DRY-RUN: %d phantom rows would be flipped captured→attempted_failed. "
            "Re-run with --apply (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to mutate.",
            len(phantom_idx),
        )
        return 0

    # Apply
    logger.info("APPLY: flipping %d phantom rows captured→attempted_failed...", len(phantom_idx))
    _flip_phantoms(bucket, df, phantom_idx)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
