#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: cefi manifest blank data_type patch verified applied to prod (issue doc is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06 resolved)

"""Patch cefi availability index rows written 2026-06-27+ with blank ``data_type`` → ``'instruments'``.

ROOT CAUSE (issue doc ``plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md``):
    The IS writer (``instruments_service.engine.orchestrator.writers.py:239``) has been calling
    ``record_captured(..., data_type="", ...)`` for the cefi/tradfi/defi non-sports emit path since
    the 2026-06-11 orchestrator split (commit ``cb51c98a``). Pre-2026-06-29 the periodic
    ``migrate_instruments_store_v9.py`` promoted these blank rows to
    ``data_type=REFERENCE_DATA_TYPE`` (``"instruments"``). The 2026-06-29 UAC-producer
    consolidation (``4da6fe8``) coincides with the migration going quiet on cefi, so all cefi
    captured rows written 2026-06-27→today land + stay with ``data_type=""``.

    Impact: the canonical honest-coverage query
    ``capture_status == 'captured' AND data_type == 'instruments'`` silently misses ~260 shards
    (26 venues × ~10 days), reading them as absent.

FIX (this script — the (b) follow-on in the issue doc's Recommended decision):
    Read the cefi availability index, select captured rows since 2026-06-27 with blank ``data_type``
    at cefi venue-grain (``venue != ""``), rewrite ``data_type = "instruments"``, snapshot the
    pre-patch index, upload the fixed index back. This mirrors the migrate script's
    CF-13 promotion step (``migrate_instruments_store_v9.py:345``:
    ``out.loc[blank_dt, "data_type"] = REFERENCE_DATA_TYPE``) but scoped to the recent
    regression window on the cefi bucket only.

    Idempotent: a re-run on an already-fixed index finds 0 rows to flip and skips the write.
    Dry-run by default; ``--apply --confirm`` writes the live ``_index/availability_index.parquet``.

Companion fix (issue doc item (a) — separate task): change the writer's ``data_type=""`` argument
to ``data_type="instruments"`` at emission time so this regression cannot recur.

Usage::

    cd instruments-service

    # inspect what will change (default — dry-run)
    .venv/bin/python scripts/patch_cefi_manifest_blank_data_type_2026_07_06.py

    # apply the patch to the live prod bucket
    .venv/bin/python scripts/patch_cefi_manifest_blank_data_type_2026_07_06.py --apply --confirm

SSOTs:
    * Issue doc: ``plans/active/issues/is_cefi_manifest_blank_data_type_since_2026_06_29_2026_07_06.md``
    * Reference data_type constant: ``scripts/migrate_instruments_store_v9.py::REFERENCE_DATA_TYPE``
    * Availability manifest contract: ``codex/02-data/availability-manifest-and-data-status.md``
    * Canonical update path (snapshot + upload):
      ``scripts/migrate_instruments_store_v9.py::_snapshot_and_write_index``
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client
from unified_trading_library.cloud_interface import StorageClient, resolve_bucket_name  # noqa: qg-deep-import

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Availability index paths — identical to migrate_instruments_store_v9.py (INDEX_REL / SNAPSHOT_DIR).
INDEX_REL = "_index/availability_index.parquet"
SNAPSHOT_DIR = "_index/snapshots"

# Canonical cefi reference-data data_type — mirrors ``REFERENCE_DATA_TYPE`` in
# ``scripts/migrate_instruments_store_v9.py`` (SOURCE_PRIORITY[("reference", "instruments")]).
REFERENCE_DATA_TYPE = "instruments"

# Regression-window start date — issue doc § "What I found": the transition window opens at
# 2026-06-27 (both types coexist through 2026-06-28; blank-only from 2026-06-29).
REGRESSION_START_DATE = "2026-06-27"


def _blank_captured_mask(df: pd.DataFrame) -> pd.Series:
    """Rows targeted by the patch: captured cefi venue-grain rows with blank data_type since regression start."""
    date_col = df["date"].astype(str)
    capture_status = df["capture_status"].astype(str)
    data_type = df["data_type"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    return (
        (date_col >= REGRESSION_START_DATE)
        & (capture_status == "captured")
        & (data_type == "")
        & (venue != "")
    )


def _report_frame(df: pd.DataFrame) -> None:
    """Log the pre-patch state of the index for visual confirmation."""
    logger.info("  rows=%d", len(df))
    logger.info("  data_type uniques=%s", df["data_type"].fillna("").astype(str).value_counts().head(8).to_dict())
    logger.info("  capture_status uniques=%s", df["capture_status"].value_counts(dropna=False).head(6).to_dict())
    logger.info("  date range: %s → %s", df["date"].min(), df["date"].max())


def _report_target_rows(df: pd.DataFrame, mask: pd.Series) -> None:
    """Log the rows we would flip (up to 5) + per-venue + per-date counts for visual confirmation."""
    target = df.loc[mask]
    if target.empty:
        logger.info("  0 rows target — index already canonical for the regression window")
        return
    logger.info("  target rows: %d", len(target))
    per_date = target["date"].astype(str).value_counts().sort_index().head(20).to_dict()
    logger.info("  per-date (up to 20): %s", per_date)
    per_venue = target["venue"].fillna("").astype(str).value_counts().head(10).to_dict()
    logger.info("  top venues (up to 10): %s", per_venue)
    logger.info("  sample rows (up to 5):")
    keep_cols = [c for c in ("date", "venue", "chain", "data_type", "capture_status", "instrument_count") if c in target.columns]
    for _, row in target[keep_cols].head(5).iterrows():
        logger.info("    %s", {c: row[c] for c in keep_cols})


def _verify_post_patch(fixed: pd.DataFrame) -> int:
    """Return the number of blank-data_type captured cefi venue rows STILL present post-patch (must be 0)."""
    return int(_blank_captured_mask(fixed).sum())


def _snapshot_and_write(
    storage_client: StorageClient,
    bucket: str,
    raw_old: bytes,
    fixed_df: pd.DataFrame,
    stamp: str,
) -> None:
    """Snapshot the pre-patch _index bytes then upload the fixed frame (mirrors migrate script's canonical update path)."""
    snap_rel = f"{SNAPSHOT_DIR}/pre_cefi_data_type_patch_{stamp}.parquet"
    storage_client.upload_from_file_obj(bucket, snap_rel, io.BytesIO(raw_old), content_type="application/octet-stream")
    logger.info("  snapshot written: gs://%s/%s (%.1f KB)", bucket, snap_rel, len(raw_old) / 1024)
    out = io.BytesIO()
    fixed_df.to_parquet(out, index=False)
    out.seek(0)
    body = out.getvalue()
    storage_client.upload_from_file_obj(bucket, INDEX_REL, io.BytesIO(body), content_type="application/octet-stream")
    logger.info("  fixed _index written: gs://%s/%s (%d rows, %.1f KB)", bucket, INDEX_REL, len(fixed_df), len(body) / 1024)


def _process_bucket(bucket: str, *, apply: bool) -> int:
    """Return the number of rows flipped (dry-run: would-flip). ``0`` = idempotent no-op / already canonical."""
    storage_client = get_storage_client()
    logger.info("── gs://%s/%s ──", bucket, INDEX_REL)

    raw_old = storage_client.download_bytes(bucket, INDEX_REL)
    df = pd.read_parquet(io.BytesIO(raw_old))
    _report_frame(df)

    mask = _blank_captured_mask(df)
    n_target = int(mask.sum())
    _report_target_rows(df, mask)

    if n_target == 0:
        logger.info("  ✓ nothing to flip — index already canonical")
        return 0

    fixed = df.copy()
    fixed.loc[mask, "data_type"] = REFERENCE_DATA_TYPE
    residual = _verify_post_patch(fixed)
    if residual != 0:
        logger.error("  ABORT: post-patch verification failed — %d blank cefi captured rows remain after mutation", residual)
        return -1
    logger.info("  post-patch verification: 0 blank cefi captured rows remain in the target window ✓")

    if not apply:
        logger.info("  (dry-run — re-run with --apply --confirm to write)")
        return n_target

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    _snapshot_and_write(storage_client, bucket, raw_old, fixed, stamp)
    logger.info("  ✅ APPLIED — %d rows flipped (data_type '' → '%s')", n_target, REFERENCE_DATA_TYPE)
    return n_target


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--bucket",
        default="",
        help=(
            "Override bucket name. Default: resolve via "
            "resolve_bucket_name(cloud='gcp', kind='instruments-store', asset_group='cefi')."
        ),
    )
    ap.add_argument("--apply", action="store_true", help="Write the fixed _index back to GCS (requires --confirm).")
    ap.add_argument("--confirm", action="store_true", help="Required safety belt for --apply.")
    args = ap.parse_args()

    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as a safety belt. Aborting.")
        return 1

    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    mode_label = "APPLY" if args.apply else "DRY-RUN"
    logger.info("=" * 72)
    logger.info("patch_cefi_manifest_blank_data_type_2026_07_06 — %s", mode_label)
    logger.info("Bucket: %s", bucket)
    logger.info("Filter: date >= %s AND capture_status == 'captured' AND data_type == '' AND venue != ''", REGRESSION_START_DATE)
    logger.info("Rewrite: data_type = '%s' (matches REFERENCE_DATA_TYPE in migrate_instruments_store_v9.py)", REFERENCE_DATA_TYPE)
    logger.info("=" * 72)

    n_flipped = _process_bucket(bucket, apply=args.apply)
    logger.info("=" * 72)
    if n_flipped < 0:
        logger.error("FINAL: patch aborted — see prior ABORT log line.")
        return 1
    if args.apply:
        logger.info("FINAL: %d rows flipped in gs://%s/%s.", n_flipped, bucket, INDEX_REL)
    else:
        logger.info("FINAL (dry-run): %d rows WOULD flip. Re-run with --apply --confirm to commit.", n_flipped)
    logger.info("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
