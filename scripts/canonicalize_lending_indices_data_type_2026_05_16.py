#!/usr/bin/env python3
"""Canonicalize ``data_type`` in the lending-indices manifest — kebab → snake.

Per ``plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md``
Option A: the canonical form is ``lending_indices`` (snake_case) per the
asset_group vocabulary rule (CLAUDE.md § "Asset-group vocabulary"). Legacy
emissions (pre-2026-04-23) wrote ``lending-indices`` (kebab); those rows are
static + idempotent to flip.

Walks `gs://lending-indices-central-element-323112/_index/availability_index.parquet`,
flips every row with `data_type == "lending-indices"` to `data_type == "lending_indices"`,
preserves all other columns, and writes back via a per-VM shard so the manifest
consolidator merges the rewrites into canonical on next cycle.

Idempotent: re-runs find 0 kebab rows after the first apply.

Usage:
    python scripts/canonicalize_lending_indices_data_type_2026_05_16.py --dry-run
    python scripts/canonicalize_lending_indices_data_type_2026_05_16.py --apply

Companion to:
    instruments-service/scripts/reconcile_lending_indices_phantom.py (slot 2's
    in-flight phantom reconciler; this script unblocks its data_type filter).

Closes:
    plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md

Execution ownership (Runbook SSOT):
  execution:
    owner: slot-4-ikenna (cross-slot pickup from slot-2 ownership 2026-05-16)
    cadence: one-shot
    verifier: groupby data_type returns 1 canonical row after apply
    last_executed: 2026-05-16
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "lending-indices-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_SHARD_BLOB = "_index/per_vm/manifest-canonicalize-data-type-kebab-to-snake.parquet"

LEGACY_VALUE = "lending-indices"
CANONICAL_VALUE = "lending_indices"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report counts; no writes")
    parser.add_argument("--apply", action="store_true", help="write canonicalised shard")
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        parser.error("pass --dry-run or --apply")

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(INDEX_BLOB)
    local_path = f"{tempfile.gettempdir()}/lending_indices_pre_canonicalize.parquet"
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))

    if "data_type" not in df.columns:
        logger.error("manifest missing data_type column (cols=%s)", sorted(df.columns))
        return 1

    by_dt = df["data_type"].astype(str).value_counts()
    logger.info("data_type distribution (pre-canonicalize):\n%s", by_dt.to_string())

    legacy_mask = df["data_type"].astype(str) == LEGACY_VALUE
    legacy_count = int(legacy_mask.sum())
    canonical_count = int((df["data_type"].astype(str) == CANONICAL_VALUE).sum())
    logger.info("Legacy (%s) rows: %d", LEGACY_VALUE, legacy_count)
    logger.info("Canonical (%s) rows: %d", CANONICAL_VALUE, canonical_count)

    if legacy_count == 0:
        logger.info("Nothing to canonicalize — manifest is clean.")
        return 0

    # Build the per-VM shard with the flipped rows. Stamp written_at/attempted_at
    # to detect the rewrite in audit logs; preserve every other column verbatim.
    legacy_rows = df.loc[legacy_mask].copy()
    legacy_rows["data_type"] = CANONICAL_VALUE
    now_iso = datetime.now(UTC).isoformat()
    if "written_at" in legacy_rows.columns:
        legacy_rows["written_at"] = now_iso
    if "attempted_at" in legacy_rows.columns:
        legacy_rows["attempted_at"] = now_iso
    if "error_reason" in legacy_rows.columns:
        legacy_rows.loc[:, "error_reason"] = (
            legacy_rows["error_reason"].fillna("").astype(str)
            + ("|" if legacy_rows["error_reason"].fillna("").astype(str).str.len().gt(0).any() else "")
            + "canonicalized_from_lending-indices_kebab_2026_05_16"
        )

    logger.info("Will write %d canonicalised rows to per-VM shard", len(legacy_rows))
    sample_cols = [c for c in ("venue", "chain", "date", "capture_status", "data_type") if c in legacy_rows.columns]
    if sample_cols:
        logger.info(
            "Sample 5 rewrites (cols=%s):\n%s",
            sample_cols,
            legacy_rows[sample_cols].head(5).to_string(index=False),
        )

    if args.dry_run:
        logger.info("DRY RUN — no manifest writes")
        return 0

    out = io.BytesIO()
    legacy_rows.to_parquet(out, index=False)
    out.seek(0)
    bucket.blob(PER_VM_SHARD_BLOB).upload_from_file(out, content_type="application/octet-stream")
    logger.info(
        "Per-VM shard written: %d canonicalised rows at gs://%s/%s",
        len(legacy_rows),
        BUCKET,
        PER_VM_SHARD_BLOB,
    )
    logger.info(
        "Consolidator will merge on next cycle (last-writer-wins). Verify post-merge: "
        "manifest groupby data_type should show 1 row (%s only).",
        CANONICAL_VALUE,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
