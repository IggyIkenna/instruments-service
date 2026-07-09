#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after transient cefi prd retry confirmed complete (manifest attempted_failed count reduced)
"""retry_transient_cefi_failures_2026_06_28.py

Flip ~60K genuinely-transient ``attempted_failed`` rows in the cefi prd manifest
back to ``expected_unattempted`` so the next backfill VM re-attempts them.

Root cause
----------
Tardis HTTP 500/503, connection timeouts, and incomplete-payload errors are
transient infrastructure failures, NOT permanent data-absence signals.  Leaving
them as ``attempted_failed`` permanently blocks the shard from re-trying.
Flipping to ``expected_unattempted`` re-queues them for the backfill VM without
altering the parquet itself (idempotent writes by the backfill path).

Flip contract
-------------
Rows must satisfy ALL of:
1. ``capture_status == "attempted_failed"``
2. ``error_reason`` matches one of the transient patterns (see TRANSIENT_PATTERNS)

Flip target
-----------
- ``capture_status``  → ``"expected_unattempted"``
- ``error_reason``    → ``""`` (cleared — next attempt records the real outcome)

Safety gates (ABORT before write if violated)
---------------------------------------------
1. Per-VM shard isolation: ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``
   must be set when ``--apply`` is given.
2. Rows at ``capture_status=captured`` or ``expected_unattempted`` are NEVER touched.
3. Count of ``captured`` rows must be unchanged after the flip.

Source: Plan honest_coverage_v2_instrument_denominator_2026_06_28.md Phase 0 P1.

Usage::

    # Default: dry-run (no mutations)
    cd instruments-service
    .venv/bin/python scripts/retry_transient_cefi_failures_2026_06_28.py

    # Apply
    MANIFEST_PER_VM_SHARDS=true VM_NAME=cefi-retry-transient-$(date +%s) \\
    .venv/bin/python scripts/retry_transient_cefi_failures_2026_06_28.py --apply
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

# Transient error patterns — exact matches and case-insensitive substrings.
# Exact / uppercase patterns (Tardis error codes):
_EXACT_PATTERNS: frozenset[str] = frozenset(
    {
        "TARDIS_HTTP_500",
        "TARDIS_HTTP_503",
        "HTTP_500",
        "HTTP_503",
    }
)

# Case-insensitive substring patterns (network/payload errors):
_SUBSTRING_PATTERNS_LOWER: tuple[str, ...] = (
    "connection timeout",
    "connectiontimeout",
    "payload incomplete",
    "payloadincomplete",
    "incomplete payload",
)


def _is_transient(error_reason: str) -> bool:
    """Return True if error_reason matches any transient failure pattern."""
    if not error_reason:
        return False
    # Exact match (case-sensitive — these are canonical Tardis error codes).
    if error_reason in _EXACT_PATTERNS:
        return True
    # Substring match (case-insensitive).
    er_lower = error_reason.lower()
    return any(pat in er_lower for pat in _SUBSTRING_PATTERNS_LOWER)


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _identify_transient_rows(df: pd.DataFrame) -> pd.Index:
    """Return index of attempted_failed rows with transient error reasons."""
    if "capture_status" not in df.columns:
        logger.error("Manifest missing 'capture_status' column")
        return df.index[:0]
    if "error_reason" not in df.columns:
        logger.warning("Manifest missing 'error_reason' column — no rows qualify")
        return df.index[:0]

    status = df["capture_status"].fillna("").astype(str)
    failed_mask = status == "attempted_failed"

    error_reason = df["error_reason"].fillna("").astype(str)
    transient_mask = error_reason.apply(_is_transient)

    combined = failed_mask & transient_mask
    idx = df[combined].index
    logger.info(
        "Transient attempted_failed rows: %d of %d attempted_failed total",
        len(idx),
        int(failed_mask.sum()),
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    sub = df.loc[idx]
    if "error_reason" in sub.columns:
        by_reason = sub["error_reason"].fillna("").value_counts().head(20)
        logger.info("Transient rows by error_reason (top 20):\n%s", by_reason.to_string())
    if "venue" in sub.columns:
        by_venue = sub["venue"].fillna("").value_counts().head(15)
        logger.info("Transient rows by venue (top 15):\n%s", by_venue.to_string())
    if "data_type" in sub.columns:
        by_dt = sub["data_type"].fillna("").value_counts().head(15)
        logger.info("Transient rows by data_type (top 15):\n%s", by_dt.to_string())


def _validate_apply_env() -> bool:
    # Env vars legitimately unset is the expected "not configured for --apply"
    # state; "" is the correctly-treated-as-falsy not-present value the checks
    # below fail fast on (ok=False -> caller aborts).
    per_vm = os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower()  # noqa: qg-empty-fallback — unset env => fails below
    vm_name = os.environ.get("VM_NAME", "").strip()  # noqa: qg-empty-fallback — unset env => `not vm_name` fails below
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
    """Mutate transient rows to expected_unattempted and write the manifest back."""
    # Safety: confirm captured count is unchanged after mutation.
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())

    df.loc[idx, "capture_status"] = "expected_unattempted"
    df.loc[idx, "error_reason"] = ""

    captured_after = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    if captured_after != captured_before:
        logger.error(
            "SAFETY GATE FAILED: captured count changed %d → %d — aborting write",
            captured_before,
            captured_after,
        )
        raise RuntimeError(f"captured count changed {captured_before} → {captured_after} after flip — BUG")

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, INDEX_BLOB, out)
    logger.info(
        "Uploaded manifest: %d rows, %d transient rows flipped to expected_unattempted "
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
            "Flip transient attempted_failed rows to expected_unattempted. "
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
    transient_idx = _identify_transient_rows(df)

    if len(transient_idx) == 0:
        logger.info("No transient attempted_failed rows found. Nothing to do.")
        return 0

    _report_distribution(df, transient_idx)

    if dry_run:
        logger.info(
            "DRY-RUN: %d transient rows would be flipped attempted_failed→expected_unattempted. "
            "Re-run with --apply (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to mutate.",
            len(transient_idx),
        )
        return 0

    # Apply
    logger.info("APPLY: flipping %d transient rows to expected_unattempted...", len(transient_idx))
    _flip_to_expected_unattempted(bucket, df, transient_idx)
    logger.info("Done. Next backfill VM will re-attempt these %d shards.", len(transient_idx))
    return 0


if __name__ == "__main__":
    sys.exit(main())
