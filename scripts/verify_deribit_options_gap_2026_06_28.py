#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after Deribit options_chain gap investigation resolved (Phase 1 confirmed or remediated)
"""verify_deribit_options_gap_2026_06_28.py

READ-ONLY diagnostic: audit Deribit options_chain coverage in the cefi prd manifest.

The cefi prd manifest shows only 2 ``options_chain`` cells captured for Deribit,
despite the cefi backfill plan claiming "G1 complete".  This script verifies
whether the Deribit BTC/ETH options surface is actually enumerated + captured,
or silently absent.

Output
------
* Per (data_type, instrument_type) combo: count by capture_status
* Focused options_chain breakdown for Deribit
* Comparison commentary against what the cefi backfill plan expects

Source: Plan honest_coverage_v2_instrument_denominator_2026_06_28.md Phase 1 P0.

Usage::

    cd instruments-service
    .venv/bin/python scripts/verify_deribit_options_gap_2026_06_28.py

READ-ONLY: no mutations, no writes.
"""

from __future__ import annotations

import io
import logging
import sys
from datetime import date

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
REPORT_DATE = date(2026, 6, 28)


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _filter_deribit(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows where venue is DERIBIT (case-insensitive)."""
    if "venue" not in df.columns:
        logger.error("Manifest missing 'venue' column")
        return df.iloc[:0]
    return df[df["venue"].str.upper() == "DERIBIT"].copy()


def _print_breakdown(deribit_df: pd.DataFrame) -> None:
    """Print per (data_type, instrument_type) breakdown by capture_status."""
    if deribit_df.empty:
        print("  [DERIBIT] No rows found in the manifest.")
        return

    total = len(deribit_df)
    print(f"\n  Total Deribit rows in manifest: {total}")

    # Which columns are present?
    has_dt = "data_type" in deribit_df.columns
    has_it = "instrument_type" in deribit_df.columns
    has_status = "capture_status" in deribit_df.columns

    if not has_status:
        print("  [WARN] manifest missing 'capture_status' column — cannot compute breakdown")
        return

    # Group by (data_type, instrument_type) then pivot on capture_status.
    group_cols: list[str] = []
    if has_dt:
        group_cols.append("data_type")
    if has_it:
        group_cols.append("instrument_type")

    if not group_cols:
        print("  [WARN] manifest missing 'data_type' and 'instrument_type' columns")
        # Minimal breakdown: just status counts.
        status_counts = deribit_df["capture_status"].fillna("").value_counts()
        for status, count in status_counts.items():
            print(f"    {status}={count}")
        return

    status_values = ["captured", "attempted_failed", "expected_unattempted", "empty_confirmed"]

    grouped = (
        deribit_df
        .groupby(group_cols + ["capture_status"])
        .size()
        .reset_index(name="count")
    )

    for key, grp in grouped.groupby(group_cols):
        if isinstance(key, str):
            key = (key,)  # single group col
        key_str = ", ".join(f"{c}={v}" for c, v in zip(group_cols, key))
        status_map = dict(zip(grp["capture_status"].tolist(), grp["count"].tolist()))
        parts = []
        for s in status_values:
            if s in status_map:
                parts.append(f"{s}={status_map[s]}")
        remainder = {k: v for k, v in status_map.items() if k not in status_values}
        for k, v in remainder.items():
            parts.append(f"{k}={v}")
        print(f"    {key_str}: {', '.join(parts)}")


def _check_options_chain(deribit_df: pd.DataFrame) -> None:
    """Focused check on options_chain coverage for Deribit."""
    if "data_type" not in deribit_df.columns:
        print("  [WARN] manifest missing 'data_type' column — cannot check options_chain")
        return

    options_df = deribit_df[deribit_df["data_type"].fillna("").str.lower() == "options_chain"]

    if options_df.empty:
        print("\n  [options_chain] ZERO rows for Deribit — options_chain is NOT enumerated in the manifest.")
        print("  → Layer-1/Layer-2 contradiction: plan claims G1 complete but no options_chain rows exist.")
        return

    has_status = "capture_status" in options_df.columns
    if has_status:
        status_counts = options_df["capture_status"].fillna("").value_counts()
        captured = int(status_counts.get("captured", 0))
        failed = int(status_counts.get("attempted_failed", 0))
        unattempted = int(status_counts.get("expected_unattempted", 0))
        empty = int(status_counts.get("empty_confirmed", 0))
    else:
        captured = failed = unattempted = empty = 0

    print(f"\n  [options_chain] Deribit rows: {len(options_df)}")
    print(f"    captured={captured}, attempted_failed={failed}, "
          f"expected_unattempted={unattempted}, empty_confirmed={empty}")
    if captured <= 2:
        print(
            f"  → Layer-1/Layer-2 contradiction: only {captured} captured vs expected universe "
            f"(see plan Phase 1 — G1 complete claim vs manifest reality)."
        )
    else:
        print(f"  → {captured} captured rows — larger than 2, re-check plan assertion.")

    # Date range covered
    if "date" in options_df.columns and has_status:
        captured_options = options_df[options_df["capture_status"] == "captured"]
        if not captured_options.empty:
            dates = captured_options["date"].astype(str)
            print(f"    Captured date range: {dates.min()} to {dates.max()}")

    # Instrument breakdown
    if "instrument_type" in options_df.columns:
        it_counts = options_df["instrument_type"].fillna("(none)").value_counts()
        print(f"    By instrument_type: {it_counts.to_dict()}")


def main() -> int:
    bucket = _get_cefi_prd_bucket()

    df = _load_manifest(bucket)
    deribit_df = _filter_deribit(df)

    print(f"\n=== Deribit options_chain manifest audit ({REPORT_DATE}) ===")
    print(f"Manifest source: gs://{bucket}/{INDEX_BLOB}")
    print(f"Total manifest rows: {len(df)}")
    print(f"Deribit rows: {len(deribit_df)}")

    if deribit_df.empty:
        print("\n  [CRITICAL] Zero Deribit rows in manifest.")
        print("  → Deribit is NOT enumerated at all in the cefi prd manifest.")
        print("  → G1 claim is FALSE — no instruments downloaded for Deribit.")
        return 0

    print("\nBreakdown by (data_type, instrument_type) x capture_status:")
    _print_breakdown(deribit_df)

    print("\nFocused options_chain check:")
    _check_options_chain(deribit_df)

    # Summary of all data_types present
    if "data_type" in deribit_df.columns:
        dt_counts = deribit_df["data_type"].fillna("(none)").value_counts()
        print("\nAll Deribit data_types in manifest:")
        for dt, cnt in dt_counts.items():
            print(f"  {dt}: {cnt} rows")

    print("\n=== END AUDIT ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
