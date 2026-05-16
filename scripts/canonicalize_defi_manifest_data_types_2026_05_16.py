#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Canonicalize DeFi canonical-manifest ``data_type`` column vocabulary drift.

Workspace-wide audit 2026-05-16 found **6 of 7 DeFi canonical manifests** carry
both kebab-form (legacy) and snake-form (canonical) values in the ``data_type``
column.  ~116,000 rows are in kebab form; any downstream snake-only query silently
misses 30-60% of rows per bucket.

Root cause: kebab rows were emitted by pre-2026-04-23 handler revisions.  Current
production emission is snake-only.  The migration is a one-shot column rename; no
on-disk hive vocabulary changes needed (hive segments are already snake-only).

This script:

1. For each of the 6 affected buckets, reads
   ``gs://{family}-{project_id}/_index/availability_index.parquet``.
2. Counts rows where ``data_type == <kebab>`` (closed-set mapping below).
3. Prints per-bucket diagnostic table with sample rows for visual confirmation.
4. On ``--apply`` (+ ``--confirm``): renames kebab → snake in the ``data_type``
   column and uploads back to GCS.
5. Idempotent: buckets with zero kebab rows are skipped with a log message.

Closed-set kebab→snake mapping (from issue doc § Option A):

    lending-indices  → lending_indices
    oracle-prices    → oracle_prices
    lst-rates        → lst_rates
    perp-funding     → perp_funding
    dex-swaps        → dex_swaps
    dex-pools        → dex_pools

Bucket name pattern: ``{family}-{project_id}``
Manifest blob:       ``_index/availability_index.parquet``

Usage::

    cd instruments-service

    # Dry-run (default — reports findings, no writes)
    .venv/bin/python scripts/canonicalize_defi_manifest_data_types_2026_05_16.py

    # Dry-run for a single bucket
    .venv/bin/python scripts/canonicalize_defi_manifest_data_types_2026_05_16.py \\
        --bucket lending-indices

    # Apply across all 6 buckets (requires --confirm)
    .venv/bin/python scripts/canonicalize_defi_manifest_data_types_2026_05_16.py \\
        --apply --confirm

    # Apply to a single bucket
    .venv/bin/python scripts/canonicalize_defi_manifest_data_types_2026_05_16.py \\
        --apply --confirm --bucket dex-pools

References:
    * Issue doc: ``plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md``
      § Option A (recommended — workspace-wide canonicalisation)
    * Sister script: ``reconcile_lending_indices_phantom.py``
    * Reference: ``reconcile_phantom_manifest_rows_all.py`` (read/write pattern)
    * v8-tolerant: pd.read_parquet / df.to_parquet round-trip preserves unknown cols.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import sys
import tempfile

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Grandfathered per reconcile_phantom_manifest_rows_all.py precedent.
PROJECT_ID = "central-element-323112"

# Manifest blob path — identical across all 6 DeFi canonical buckets.
MANIFEST_BLOB = "_index/availability_index.parquet"

# Closed-set kebab→snake mapping for all 6 affected DeFi canonical manifests.
# Family name (= bucket prefix) maps to (kebab_form, snake_form).
# Source: plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md
DATA_TYPE_KEBAB_TO_SNAKE: dict[str, str] = {
    "lending-indices": "lending_indices",
    "oracle-prices": "oracle_prices",
    "lst-rates": "lst_rates",
    "perp-funding": "perp_funding",
    "dex-swaps": "dex_swaps",
    "dex-pools": "dex_pools",
}

# All affected bucket families in processing order.
ALL_FAMILIES: list[str] = list(DATA_TYPE_KEBAB_TO_SNAKE.keys())


def _bucket_name(family: str, project_id: str) -> str:
    """Return the canonical GCS bucket name for a DeFi family."""
    return f"{family}-{project_id}"


def _process_bucket(
    *,
    family: str,
    project_id: str,
    client: storage.Client,
    apply: bool,
) -> tuple[int, int]:
    """Audit (and optionally canonicalize) one bucket's manifest.

    Returns ``(kebab_count, total_rows)`` for the summary table.
    The bucket is idempotent: if zero kebab rows, logs and returns immediately.
    """
    kebab_form = family
    snake_form = DATA_TYPE_KEBAB_TO_SNAKE[family]
    bname = _bucket_name(family, project_id)

    bucket = client.bucket(bname)
    blob = bucket.blob(MANIFEST_BLOB)

    logger.info("  Loading gs://%s/%s ...", bname, MANIFEST_BLOB)

    # Per-invocation temp file — Bandit B108: use tempfile.gettempdir(), not /tmp.
    with tempfile.NamedTemporaryFile(
        prefix=f"canon-{family}-",
        suffix=".parquet",
        delete=False,
        dir=tempfile.gettempdir(),
    ) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(manifest_path)

    total = len(df)
    kebab_mask = df["data_type"].fillna("").astype(str) == kebab_form
    snake_mask = df["data_type"].fillna("").astype(str) == snake_form
    kebab_count = int(kebab_mask.sum())
    snake_count = int(snake_mask.sum())
    other_count = total - kebab_count - snake_count

    # --- Diagnostic table ---------------------------------------------------
    logger.info("  Bucket: %s", bname)
    logger.info("    total rows   : %d", total)
    logger.info("    kebab (%s): %d  (%.1f%%)", kebab_form, kebab_count, 100 * kebab_count / max(1, total))
    logger.info("    snake (%s): %d  (%.1f%%)", snake_form, snake_count, 100 * snake_count / max(1, total))
    if other_count:
        logger.info("    other        : %d", other_count)
    logger.info("    would-flip   : %d", kebab_count)

    # Sample rows for visual confirmation (up to 3 kebab + 3 snake).
    if kebab_count:
        sample_kebab = df.loc[kebab_mask].head(3)
        logger.info("    Sample kebab rows (up to 3):")
        for _, row in sample_kebab.iterrows():
            logger.info(
                "      date=%s venue=%s chain=%s data_type=%s capture_status=%s",
                row.get("date"),
                row.get("venue"),
                row.get("chain"),
                row.get("data_type"),
                row.get("capture_status"),
            )
    if snake_count:
        sample_snake = df.loc[snake_mask].head(3)
        logger.info("    Sample snake rows (up to 3):")
        for _, row in sample_snake.iterrows():
            logger.info(
                "      date=%s venue=%s chain=%s data_type=%s capture_status=%s",
                row.get("date"),
                row.get("venue"),
                row.get("chain"),
                row.get("data_type"),
                row.get("capture_status"),
            )

    # --- Idempotent guard ---------------------------------------------------
    if kebab_count == 0:
        logger.info("    already canonical — skipping (no kebab rows)")
        return 0, total

    # --- Apply mode ---------------------------------------------------------
    if not apply:
        # Dry-run: already printed findings above.
        return kebab_count, total

    # In-memory write: avoids a local temp file for the output; pattern matches
    # reconcile_lending_indices_phantom.py (io.BytesIO + blob.upload_from_file).
    df.loc[kebab_mask, "data_type"] = snake_form
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("    Uploading canonicalized manifest (%d rows, %d flipped) ...", total, kebab_count)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("    Upload complete.")
    return kebab_count, total


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report findings per bucket, no writes (default mode).",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Write canonicalized manifest back to GCS (requires --confirm).",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        default=False,
        help="Required safety belt when --apply is passed.",
    )
    p.add_argument(
        "--bucket",
        type=str,
        default="",
        help=(
            "Run on a single family bucket only (e.g. 'lending-indices'). "
            "Default: all 6 affected buckets."
        ),
    )
    p.add_argument(
        "--project-id",
        type=str,
        default=PROJECT_ID,
        help=f"GCP project ID (default: {PROJECT_ID}).",
    )
    args = p.parse_args()

    # Default to dry-run when neither mode is set.
    if not args.apply and not args.dry_run:
        args.dry_run = True

    # Safety belt: --apply requires --confirm.
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as a safety belt. Aborting.")
        return 1

    # Validate --bucket argument against closed set.
    if args.bucket:
        if args.bucket not in DATA_TYPE_KEBAB_TO_SNAKE:
            logger.error(
                "Unknown --bucket %r. Valid families: %s",
                args.bucket,
                ", ".join(sorted(DATA_TYPE_KEBAB_TO_SNAKE)),
            )
            return 1
        families = [args.bucket]
    else:
        families = ALL_FAMILIES

    project_id: str = args.project_id
    mode_label = "APPLY" if args.apply else "DRY-RUN"
    logger.info("=" * 60)
    logger.info("canonicalize_defi_manifest_data_types_2026_05_16 — %s", mode_label)
    logger.info("Buckets in scope: %s", families)
    logger.info("Project: %s", project_id)
    logger.info("=" * 60)

    client = storage.Client(project=project_id)

    summary: list[tuple[str, int, int]] = []  # (family, kebab_count, total)
    for family in families:
        logger.info("")
        logger.info("Processing bucket family: %s", family)
        try:
            kebab_count, total = _process_bucket(
                family=family,
                project_id=project_id,
                client=client,
                apply=args.apply,
            )
            summary.append((family, kebab_count, total))
        except Exception as exc:
            logger.error("FAILED processing %s: %s", family, exc)
            summary.append((family, -1, 0))

    # Final summary table.
    logger.info("")
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY — %s", mode_label)
    logger.info("%-20s  %10s  %10s  %10s", "bucket-family", "kebab-rows", "total-rows", "action")
    logger.info("-" * 60)
    total_flipped = 0
    total_all = 0
    for family, kebab_count, total in summary:
        if kebab_count < 0:
            action = "ERROR"
        elif kebab_count == 0:
            action = "skipped (clean)"
        elif args.apply:
            action = "FLIPPED"
            total_flipped += kebab_count
            total_all += total
        else:
            action = "would-flip (dry-run)"
            total_flipped += kebab_count
            total_all += total
        logger.info("%-20s  %10d  %10d  %s", family, kebab_count if kebab_count >= 0 else 0, total, action)
    logger.info("-" * 60)
    logger.info("Total rows touched: %d / %d total across %d buckets", total_flipped, total_all, len(families))
    if args.dry_run:
        logger.info("DRY-RUN — no manifests modified. Re-run with --apply --confirm to commit.")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
