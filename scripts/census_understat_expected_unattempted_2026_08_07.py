#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports_all_vendor_honest_coverage_convergence_2026_08_07.md closes
"""Targeted census: understat expected_unattempted rows — are they in-progress artifacts?

Reads only the understat rows from the availability manifest, filtered to
expected_unattempted, and prints date/venue/data_type breakdown of the 30 rows.
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
DATA_FLOOR = "2020-06-06"


def main() -> int:
    today = datetime.now(UTC).date().isoformat()
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    # Read only the columns we need — minimise memory
    manifest = pd.read_parquet(
        io.BytesIO(raw),
        columns=["date", "source", "capture_status", "venue", "data_type"],
    )
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    total = len(manifest)
    print(f"manifest rows (post-floor, <=today): {total:,}", file=sys.stderr)

    understat_eu = manifest[
        (manifest["source"] == "understat") & (manifest["capture_status"] == "expected_unattempted")
    ].copy()

    print(f"\n=== understat expected_unattempted census ({run_ts}) ===")
    print(f"Total rows: {len(understat_eu)}")

    if understat_eu.empty:
        print("CLEAR — 0 expected_unattempted rows for understat")
        return 0

    print("\n--- date distribution ---")
    date_counts = understat_eu["date"].value_counts().sort_index()
    for date, count in date_counts.items():
        print(f"  {date}: {count} row(s)")

    print("\n--- data_type breakdown ---")
    for dt, count in understat_eu["data_type"].value_counts().items():
        print(f"  {dt}: {count}")

    print("\n--- venue breakdown ---")
    for venue, count in understat_eu["venue"].value_counts().items():
        print(f"  {venue}: {count}")

    print("\n--- all rows ---")
    for _, row in understat_eu.sort_values("date").iterrows():
        print(f"  date={row['date']} venue={row['venue']} data_type={row['data_type']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
