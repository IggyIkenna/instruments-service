#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports_all_vendor_honest_coverage_convergence_2026_08_07.md P2 EPL tail todo resolves
"""Re-census EPL odds_api attempted_failed rows from the availability manifest.

Targeted 6-column read: date, source, league_id, capture_status, error_reason, attempted_at.
Reports date range, error_reason breakdown, and most-recent attempted_at
so the exact residual gap can be identified before launching a narrow retry.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
DATA_FLOOR = "2020-06-06"


def main() -> int:
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")
    today = datetime.now(UTC).date().isoformat()

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    manifest = pd.read_parquet(
        io.BytesIO(raw),
        columns=["date", "source", "league_id", "capture_status", "error_reason", "attempted_at"],
    )
    # Filter: post-floor, EPL, odds_api
    mask = (
        (manifest["date"] >= DATA_FLOOR)
        & (manifest["date"] <= today)
        & (manifest["source"] == "odds_api")
        & (manifest["league_id"] == "EPL")
    )
    epl = manifest[mask].copy()
    total_epl = len(epl)
    print(f"\n=== EPL odds_api census ({run_ts}) ===")
    print(f"Total EPL odds_api rows: {total_epl:,}")

    by_status = epl.groupby("capture_status").size().to_dict()
    for status, count in sorted(by_status.items()):
        print(f"  {status}: {count:,}")

    failed = epl[epl["capture_status"] == "attempted_failed"].copy()
    af_count = len(failed)
    print(f"\nattempted_failed rows: {af_count:,}")
    if af_count == 0:
        print("  ✅ No attempted_failed rows — EPL gap is fully resolved.")
        return 0

    print(f"\nDate range: {failed['date'].min()} → {failed['date'].max()}")
    print(f"Most recent attempted_at: {failed['attempted_at'].max()}")

    print("\nError reason breakdown:")
    for reason, cnt in failed.groupby("error_reason").size().sort_values(ascending=False).items():
        print(f"  {reason!r}: {cnt:,}")

    # Show density by month
    failed["month"] = failed["date"].str[:7]
    monthly = failed.groupby("month").size()
    print(f"\nMonthly breakdown ({len(monthly)} months with failures):")
    for month, cnt in monthly.items():
        print(f"  {month}: {cnt:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
