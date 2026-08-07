#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports all-vendor honest-coverage convergence doc closes
"""Re-census all sports sources by capture_status from the availability manifest.

Single-walk discipline: one UTL-client-backed read of availability_index.parquet,
reading only 3 columns (date, source, capture_status) to minimise memory footprint.
Produces the same per-source breakdown table as the doc header for direct comparison.
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
DATA_FLOOR = "2020-06-06"  # SSOT: codex/02-data/sports-2020-06-data-floor.md


def main() -> int:
    today = datetime.now(UTC).date().isoformat()
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    # 3-column read only — minimise memory; source column is in canonical schema v9
    manifest = pd.read_parquet(io.BytesIO(raw), columns=["date", "source", "capture_status"])
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    total = len(manifest)
    print(f"manifest rows (post-floor, <=today): {total:,}", file=sys.stderr)

    pivot = manifest.groupby(["source", "capture_status"]).size().unstack(fill_value=0)

    # Ensure all four standard statuses are present as columns
    for col in ("attempted_failed", "captured", "empty_confirmed", "expected_unattempted"):
        if col not in pivot.columns:
            pivot[col] = 0

    pivot = pivot[["attempted_failed", "captured", "empty_confirmed", "expected_unattempted"]]
    pivot = pivot.sort_values("attempted_failed", ascending=False)

    print(f"\n=== Sports manifest re-census ({run_ts}, {total:,} rows) ===")
    print(
        f"{'source':<30} {'attempted_failed':>17} {'captured':>10} {'empty_confirmed':>17} {'expected_unattempted':>21}"
    )
    print("-" * 100)
    for source, row in pivot.iterrows():
        print(
            f"{source:<30} {int(row['attempted_failed']):>17,} {int(row['captured']):>10,} "
            f"{int(row['empty_confirmed']):>17,} {int(row['expected_unattempted']):>21,}"
        )

    print("\n=== Sources still needing attention (attempted_failed>0 or expected_unattempted>0) ===")
    needs_work = pivot[(pivot["attempted_failed"] > 0) | (pivot["expected_unattempted"] > 0)]
    if needs_work.empty:
        print("  ALL CLEAR — every source is at attempted_failed=0, expected_unattempted=0")
    else:
        for source, row in needs_work.iterrows():
            print(
                f"  {source}: attempted_failed={int(row['attempted_failed']):,}  "
                f"expected_unattempted={int(row['expected_unattempted']):,}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
