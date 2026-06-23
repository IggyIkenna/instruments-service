# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: cefi HL-liquidations + ASTER book_snapshot_5/liquidations manifest rows are 0 in the consolidated index AND no per-VM shard carries them (verified post-run); BUG #4 (A) migration complete.
#!/usr/bin/env python3
"""Purge cefi BATCH manifest rows for LIVE-ONLY + DROPPED (venue, data_type) cells.

BUG #4 (A) (``plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md``):
the OnchainPerpBatchHandler now EXCLUDES these (venue, data_type) cells from the
batch expected universe entirely — they are never attempted and never recorded.
The historical manifest still carries the stale batch-era rows (empty_confirmed /
expected_unattempted / attempted_failed) written before the fix. This one-off
purges them so the honest-cov denominator + data-status reflect the new model:

  - HYPERLIQUID ``liquidations``         → DROPPED ENTIRELY (HL publishes no liq
                                            feed on any transport: S3/REST/WS).
  - ASTER ``book_snapshot_5``            → LIVE-ONLY (Binance-compat WS captures it
  - ASTER ``liquidations``                 going forward via the native asterdex WS
                                            connector); the batch REST path can't
                                            serve them, so the BATCH manifest must
                                            not carry empty/expected/failed cells.

NOTE this is a manifest-row DELETE (not a masking empty write) per the brief.
It sweeps BOTH the consolidated ``_index/availability_index.parquet`` AND every
per-VM shard under ``_index/per_vm/`` (else the consolidator re-merges the stale
cells back). Uses ``unified_trading_library.cloud_interface.gcs_*`` for object ops.

Idempotent. ``--dry-run`` (default) reports counts without writing.

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_cefi_live_only_and_dropped_manifest_rows_2026_06_23.py --dry-run
    .venv/bin/python scripts/purge_cefi_live_only_and_dropped_manifest_rows_2026_06_23.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_describe_object,
)
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — get_storage_client factory for list_blobs / download / upload
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"market-data-tick-cefi-prd-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

# (venue, data_type) cells that must NOT exist in the BATCH manifest (BUG #4 A).
PURGE_CELLS: frozenset[tuple[str, str]] = frozenset(
    {
        ("HYPERLIQUID", "liquidations"),  # dropped entirely
        ("ASTER", "book_snapshot_5"),  # live-only
        ("ASTER", "liquidations"),  # live-only
    }
)


def _purge_mask(df: pd.DataFrame) -> pd.Series:
    if "venue" not in df.columns or "data_type" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    pairs = list(zip(df["venue"].astype(str), df["data_type"].astype(str), strict=True))
    return pd.Series([p in PURGE_CELLS for p in pairs], index=df.index)


def _process_blob(storage_client: object, object_key: str, *, apply: bool) -> int:
    raw = storage_client.download_bytes(BUCKET, object_key)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    mask = _purge_mask(df)
    n = int(mask.sum())
    if n == 0:
        return 0
    by_cell = (
        df[mask].groupby(["venue", "data_type"]).size().to_dict() if "venue" in df.columns else {}
    )
    logger.info("%s: %d rows to purge %s", object_key, n, by_cell)
    if not apply:
        return n
    keep = df[~mask]
    out = io.BytesIO()
    keep.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, object_key, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("%s: wrote filtered manifest (%d → %d rows)", object_key, len(df), len(keep))
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the filtered manifest (default: dry-run).")
    args = parser.parse_args()
    apply = bool(args.apply)

    storage_client = get_storage_client()

    # Verify the index exists before doing anything.
    if gcs_describe_object(f"gs://{BUCKET}/{INDEX_PATH}") is None:
        logger.error("Consolidated index not found: gs://%s/%s", BUCKET, INDEX_PATH)
        return 1

    total = 0
    total += _process_blob(storage_client, INDEX_PATH, apply=apply)

    # Per-VM shards — the consolidator merges these into canonical, so a stale
    # cell here resurfaces after the next consolidation. Sweep them too.
    raw_blobs = storage_client.list_blobs(BUCKET, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
    shard_keys = [b.name for b in raw_blobs if b.name.endswith(".parquet")]  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Sweeping %d per-VM shard(s) under %s", len(shard_keys), PER_VM_PREFIX)
    for key in shard_keys:
        total += _process_blob(storage_client, key, apply=apply)

    mode = "APPLIED" if apply else "DRY-RUN (no writes)"
    logger.info("%s — total rows %s: %d", mode, "purged" if apply else "to purge", total)
    if not apply:
        logger.info("Re-run with --apply to delete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
