#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after purge verified live in the cefi _index (prd bucket) + G4 re-measure
#              confirms 0 DERIBIT/OPTION per-strike trades/book_snapshot_5 cells
"""Purge pre-v10 DERIBIT OPTION per-strike trades/book_snapshot_5 manifest rows.

Provenance: plans/active/mvp_backfill_cefi_tick_v10_2026_06_27.md G4 todo
("PURGE the ~536 pre-v10 Deribit per-strike trades/book5 manifest rows") +
operator ruling 2026-07-12, plan-reconciliation finding 30
(plans/active/issues/plan_reconciliation_operator_decisions_2026_07_11.md §A2):
"DELETE Deribit per-strike artifacts + ensure chain-level capture grain."

v10 canonicalised the Deribit CeFi OPTION capture grain to `options_chain`
ONLY (chain-level, bundled by underlying). The `trades` / `book_snapshot_5`
rows for `(venue=DERIBIT, instrument_type=OPTION)` predate that cut — they
are the pre-v10 per-strike capture artifacts the v10 scope explicitly
excludes and G4 requires at zero.

SNAPSHOT-FIRST (operator instruction): every blob this script rewrites gets
a pre-purge copy under `_index/snapshots/` BEFORE any delete, mirroring
``purge_prediction_index_final_residuals_2026_07_11.py`` /
``delete_aster_overseeded_capability_rows.py``.

Scope: the `-prd` cefi bucket only (measured live 2026-07-12: 1,048 rows,
all `capture_status=captured`, real per-underlying trades/book5 artifacts —
`trades`=930, `book_snapshot_5`=118). The legacy flat cefi bucket
(``market-data-tick-cefi-{project}``, no ``-prd``) carries a MUCH larger
(~6.65M) `empty_confirmed`/`expected_unattempted` skeleton for the same
(venue, instrument_type, data_type) triple — a 1000x+ scope difference from
the "~536" the task brief cites. That mismatch is unresolved pending
operator confirmation (see the Progress Log / blocked-question this task
filed) and is OUT OF SCOPE for this script; do not extend `--bucket` to the
legacy bucket without a follow-up ruling.

Also sweeps the prd bucket's `_index/per_vm/*.parquet` shards (0 hits
measured 2026-07-12, but a consolidator merge would resurrect the rows if a
shard did carry them and were skipped — same reasoning as
``delete_aster_overseeded_capability_rows.py``).

Usage::

    cd instruments-service

    # dry-run (default, read-only)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_deribit_option_per_strike_trades_book5_2026_07_12.py

    # apply (snapshot + delete + verify gate)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_deribit_option_per_strike_trades_book5_2026_07_12.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

_TARGET_VENUE = "DERIBIT"
_TARGET_INSTRUMENT_TYPE = "OPTION"
_TARGET_DATA_TYPES = frozenset({"trades", "book_snapshot_5"})

# STOP-ON-SURPRISE: measured live 2026-07-12 in the prd bucket canonical
# index = 1,048 rows (trades=930, book_snapshot_5=118). The task brief's
# "~536" is a stale estimate from an earlier plan snapshot (manifest counts
# drift as phantom-reconciles + backfills land). Bound generously around the
# measured live value, not the stale brief figure.
_EXPECTED_MIN = 200
_EXPECTED_MAX = 5_000


def _target_mask(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    venue = df["venue"].fillna("").astype(str).str.upper()
    itype = df["instrument_type"].fillna("").astype(str).str.upper() if "instrument_type" in df.columns else ""
    dt = df["data_type"].fillna("").astype(str)
    mask = (venue == _TARGET_VENUE) & (itype == _TARGET_INSTRUMENT_TYPE) & dt.isin(_TARGET_DATA_TYPES)
    return mask.fillna(False)


def _load(storage: object, bucket: str, blob: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw))


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_purge_deribit_option_{label}_{run_ts}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    df = _load(storage, bucket, blob)
    _write(storage, bucket, snap_blob, df)
    return snap_blob


def _process_blob(storage: object, bucket: str, blob: str, run_ts: str, *, apply: bool) -> tuple[int, int]:
    df = _load(storage, bucket, blob)
    total_before = len(df)
    if "venue" not in df.columns or "data_type" not in df.columns:
        return 0, total_before

    mask = _target_mask(df)
    n_target = int(mask.sum())
    logger.info(
        "gs://%s/%s: %d / %d rows match (DERIBIT/OPTION, trades|book_snapshot_5)",
        bucket,
        blob,
        n_target,
        total_before,
    )
    if n_target == 0:
        return 0, total_before

    if not apply:
        return n_target, total_before

    _snapshot(storage, bucket, blob, run_ts)
    df_purged = df.loc[~mask].reset_index(drop=True)
    _write(storage, bucket, blob, df_purged)
    logger.info("gs://%s/%s: deleted %d rows, %d remain", bucket, blob, n_target, len(df_purged))
    return n_target, total_before


def _verify_gate(storage: object, bucket: str, blobs: list[str]) -> int:
    logger.info("Verifying post-apply gate on gs://%s ...", bucket)
    residual_total = 0
    for blob in blobs:
        df_after = _load(storage, bucket, blob)
        if "venue" not in df_after.columns or "data_type" not in df_after.columns:
            continue
        residual = int(_target_mask(df_after).sum())
        residual_total += residual
        if residual:
            logger.error("GATE FAIL: gs://%s/%s still has %d target rows.", bucket, blob, residual)
    if residual_total:
        return 1
    logger.info("GATE PASSED: 0 DERIBIT/OPTION per-strike trades/book_snapshot_5 rows remain in gs://%s.", bucket)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Delete matching rows (snapshot first). Default: dry-run.")
    p.add_argument("--bucket", default=None, help="Override bucket (default: resolve_bucket_name prd cefi bucket).")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [INDEX_BLOB, *per_vm_names]

    total_target = 0
    per_blob_counts: dict[str, int] = {}
    for blob in blobs:
        n_target, _total_before = _process_blob(storage, bucket, blob, run_ts, apply=False)
        per_blob_counts[blob] = n_target
        total_target += n_target

    logger.info("TOTAL target rows across %d blob(s): %d", len(blobs), total_target)

    if not (_EXPECTED_MIN <= total_target <= _EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: total target row count %d outside expected range [%d, %d]. Diagnose before --apply.",
            total_target,
            _EXPECTED_MIN,
            _EXPECTED_MAX,
        )
        return 1

    if not args.apply:
        logger.info("DRY-RUN (default) — no write. Pass --apply to delete %d rows.", total_target)
        return 0

    if total_target == 0:
        logger.info("Nothing to delete.")
        return 0

    deleted = 0
    for blob in blobs:
        if per_blob_counts.get(blob, 0) == 0:
            continue
        n_deleted, _total_before = _process_blob(storage, bucket, blob, run_ts, apply=True)
        deleted += n_deleted

    rc = _verify_gate(storage, bucket, blobs)
    if rc == 0:
        logger.info("APPLY COMPLETE: %d DERIBIT/OPTION per-strike trades/book_snapshot_5 rows deleted.", deleted)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
