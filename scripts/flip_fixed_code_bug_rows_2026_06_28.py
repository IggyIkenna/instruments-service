#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after fixed-code-bug manifest reconciliation confirmed complete
"""flip_fixed_code_bug_rows_2026_06_28.py

Flip ``attempted_failed`` rows whose error was a code bug that has since been
fixed back to ``expected_unattempted`` so the next backfill VM re-attempts them.

Context
-------
Several ``attempted_failed`` error patterns in the cefi prd manifest were caused
by bugs in MTDS, not genuine data absence.  Each bug has since been fixed
(committed to live-defi-rollout).  The manifest rows must be re-queued so the
backfill can succeed:

  * ``FUTURE row requires 'expiry_date'`` (~32,279 rows) — CRYPTOFACILITIES /
    Kraken dated-futures whose expiry could not be parsed.  Fixed by
    ``_parse_numeric_futures_expiry()`` in ``tardis_shared.py`` which now
    extracts the trailing ``YYYYMMDD`` / ``_YYMMDD`` stamp from the symbol
    (e.g. ``FI_XBTUSD_20240329`` → 2024-03-29).

  * ``was_instrument_alive() got an unexpected keyword argument 'venue'``
    (~167 rows) — TypeError from a mismatched kwarg at an IS catalogue
    call-site.  Fixed in ``partitioned_writer.py``.

  * ``unknown instrument_type='PERPETUAL'`` (~175 rows) — validation in
    ``build_partition_path`` rejecting the canonical uppercase UAC enum value.
    Fixed by normalising to lowercase before the membership check.

  * ``StreamingParquetWriter pre-write validation failed`` (~232 rows) —
    writer-side validation that rejected rows with the above instrument_type
    issue.  Resolved by the same normalization fix.

Flip contract
-------------
Rows must satisfy ALL of:
1. ``capture_status == "attempted_failed"``
2. ``error_reason`` matches one of the FIXED_BUG_PATTERNS (see below)

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
    .venv/bin/python scripts/flip_fixed_code_bug_rows_2026_06_28.py

    # Apply
    MANIFEST_PER_VM_SHARDS=true VM_NAME=cefi-flip-fixed-bugs-$(date +%s) \\
    .venv/bin/python scripts/flip_fixed_code_bug_rows_2026_06_28.py --apply
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

# Exact error_reason strings / prefixes whose underlying code bugs are now fixed.
# Only include patterns where the fix is confirmed committed to live-defi-rollout.
_FIXED_BUG_EXACT: frozenset[str] = frozenset({
    "was_instrument_alive() got an unexpected keyword argument 'venue'",
})

# Substring patterns (case-insensitive) for fixed bugs.
_FIXED_BUG_SUBSTRINGS_LOWER: tuple[str, ...] = (
    "future row requires 'expiry_date'",         # ~32,279 CRYPTOFACILITIES/Kraken futures
    "unknown instrument_type='perpetual'",        # ~175 rows — uppercase UAC enum rejected
    "unknown instrument_type=\"perpetual\"",
    "streamingparquetwriter pre-write validation failed",  # ~232 rows — writer validation
)


def _is_fixed_bug(error_reason: str) -> bool:
    """Return True if error_reason matches a fixed-code-bug pattern."""
    if not error_reason:
        return False
    if error_reason in _FIXED_BUG_EXACT:
        return True
    er_lower = error_reason.lower()
    return any(pat in er_lower for pat in _FIXED_BUG_SUBSTRINGS_LOWER)


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _identify_bug_rows(df: pd.DataFrame) -> pd.Index:
    """Return index of attempted_failed rows with fixed-code-bug error reasons."""
    if "capture_status" not in df.columns:
        logger.error("Manifest missing 'capture_status' column")
        return df.index[:0]
    if "error_reason" not in df.columns:
        logger.warning("Manifest missing 'error_reason' column — no rows qualify")
        return df.index[:0]

    status = df["capture_status"].fillna("").astype(str)
    failed_mask = status == "attempted_failed"

    error_reason = df["error_reason"].fillna("").astype(str)
    bug_mask = error_reason.apply(_is_fixed_bug)

    idx = df[failed_mask & bug_mask].index
    logger.info(
        "Fixed-code-bug attempted_failed rows: %d of %d attempted_failed total",
        len(idx),
        int(failed_mask.sum()),
    )
    return idx


def _report_distribution(df: pd.DataFrame, idx: pd.Index) -> None:
    sub = df.loc[idx]
    if "error_reason" in sub.columns:
        by_reason = sub["error_reason"].fillna("").value_counts().head(20)
        logger.info("Fixed-bug rows by error_reason (top 20):\n%s", by_reason.to_string())
    if "venue" in sub.columns:
        by_venue = sub["venue"].fillna("").value_counts().head(15)
        logger.info("Fixed-bug rows by venue (top 15):\n%s", by_venue.to_string())


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
    df.loc[idx, "error_reason"] = ""

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
        "Uploaded manifest: %d rows, %d fixed-bug rows flipped to expected_unattempted "
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
            "Flip fixed-code-bug attempted_failed rows to expected_unattempted. "
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
    bug_idx = _identify_bug_rows(df)

    if len(bug_idx) == 0:
        logger.info("No fixed-code-bug attempted_failed rows found. Nothing to do.")
        return 0

    _report_distribution(df, bug_idx)

    if dry_run:
        logger.info(
            "DRY-RUN: %d fixed-bug rows would be flipped attempted_failed→expected_unattempted. "
            "Re-run with --apply (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to mutate.",
            len(bug_idx),
        )
        return 0

    logger.info("APPLY: flipping %d fixed-bug rows to expected_unattempted...", len(bug_idx))
    _flip_to_expected_unattempted(bucket, df, bug_idx)
    logger.info("Done. Next backfill VM will re-attempt these %d shards.", len(bug_idx))
    return 0


if __name__ == "__main__":
    sys.exit(main())
