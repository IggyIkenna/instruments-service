#!/usr/bin/env python3
# Epic: defi_master
# Lifecycle: oneoff
# Delete-when: defi_solana_dex_pools_fake_history_recurrence_prd_bucket_2026_07_23.md todo 2/3 both closed
"""Scope the legacy data_type=dex_pools fake-history population within migration_orphan_sweep's
own actionable output (checkpoint shards while the sweep is still running, or the final
per-asset-group report once it reaches ACCEPTANCE) -- read-only, no GCS writes.

Why this exists: filed as `plans/active/issues/defi_solana_dex_pools_fake_history_recurrence_prd_bucket_2026_07_23.md`
todo 2 -- the doc's own scope numbers (241,281/3,074,283 rows, 7.8%, ORCA+RAYDIUM, days 2025-01-01..17) were measured
against the sweep's in-progress checkpoint shards and are explicitly marked INCOMPLETE; todo 2 requires re-running this
exact scan against the FINAL report once the sweep completes, to get the true count/day-range before any
`backfill_orphan_class_e.py --apply` run touches this population. First written as a scratchpad one-off
(2026-07-23) and promoted here per the pre-compact "an open todo needs this, it is not a one-off" rule.

Trap hit getting this right: the final report (`_index/audit/orphan_sweep_<ag>.parquet`) and the in-progress
checkpoint shards (`_index/audit/_orphan_sweep_ckpt_<ag>_actionable_NNNNNN.parquet`) serialize the SAME
`SweptObject` schema (data_type/venue/day/uri columns all present in both) -- only the input SHAPE differs
(one flat file vs N numbered shards). Do not assume the final report needs different column handling.
"""

import argparse
import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client

_LEGACY_DATA_TYPE = "dex_pools"
_CANONICAL_DATA_TYPE = "dex_pool_state"


def _read_checkpoint_shards(client, bucket: str, asset_group: str) -> pd.DataFrame:
    """Read every already-written checkpoint shard (sweep still running)."""
    frames = []
    i = 0
    while True:
        path = f"_index/audit/_orphan_sweep_ckpt_{asset_group}_actionable_{i:06d}.parquet"
        if not client.blob_exists(bucket, path):
            break
        raw = client.download_bytes(bucket, path)
        frames.append(pd.read_parquet(io.BytesIO(raw)))
        i += 1
    print(f"shards scanned: {i} (sweep may still be running -- this is a partial view)")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _read_final_report(client, bucket: str, asset_group: str) -> pd.DataFrame:
    """Read the final, single-file actionable report (sweep reached ACCEPTANCE)."""
    path = f"_index/audit/orphan_sweep_{asset_group}.parquet"
    if not client.blob_exists(bucket, path):
        print(f"ERROR: final report not found at gs://{bucket}/{path} -- has the sweep reached ACCEPTANCE?")
        sys.exit(1)
    raw = client.download_bytes(bucket, path)
    return pd.read_parquet(io.BytesIO(raw))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default="market-data-tick-defi-prd-central-element-323112")
    parser.add_argument("--asset-group", default="defi")
    parser.add_argument(
        "--source",
        choices=("checkpoint", "final"),
        default="checkpoint",
        help="'checkpoint' while the sweep is still running; 'final' once it reaches ACCEPTANCE",
    )
    args = parser.parse_args()

    client = get_storage_client()
    df = (
        _read_checkpoint_shards(client, args.bucket, args.asset_group)
        if args.source == "checkpoint"
        else _read_final_report(client, args.bucket, args.asset_group)
    )

    total_rows = len(df)
    if total_rows == 0:
        print("no actionable rows found")
        return 0

    legacy = df[df["data_type"] == _LEGACY_DATA_TYPE]
    canonical_rows = len(df[df["data_type"] == _CANONICAL_DATA_TYPE])
    legacy_rows = len(legacy)

    print(f"total actionable rows: {total_rows}")
    print(
        f"data_type={_LEGACY_DATA_TYPE} (SUSPECT fake-history legacy shape): {legacy_rows} ({100 * legacy_rows / total_rows:.1f}%)"
    )
    print(f"data_type={_CANONICAL_DATA_TYPE} (canonical): {canonical_rows} ({100 * canonical_rows / total_rows:.1f}%)")
    print(f"other data_types: {total_rows - legacy_rows - canonical_rows}")
    if legacy_rows:
        venues = sorted(legacy["venue"].unique().tolist())
        days = legacy["day"].unique().tolist()
        print(f"venues seen in {_LEGACY_DATA_TYPE} rows: {venues}")
        print(f"day range in {_LEGACY_DATA_TYPE} rows: {min(days)} .. {max(days)} ({len(days)} distinct days)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
