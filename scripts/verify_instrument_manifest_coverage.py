#!/usr/bin/env python3
"""Verify manifest coverage for a venue/date range.

Checks `availability_index.parquet` for:
1) missing dates
2) dates below the expected minimum instrument count
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

import pandas as pd


def _date_range(start: date, end: date) -> list[date]:
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify manifest coverage for venue/date range")
    parser.add_argument("--category", required=True, help="Category (CEFI, TRADFI, DEFI, SPORTS, ...)")
    parser.add_argument("--venue", required=True, help="Venue name (e.g. DERIBIT, CBOE)")
    parser.add_argument("--start-date", required=True, help="Range start (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Range end (YYYY-MM-DD)")
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum expected instrument count per date (default: 1)",
    )
    parser.add_argument(
        "--max-missing-print",
        type=int,
        default=30,
        help="Print at most this many missing dates (default: 30)",
    )
    args = parser.parse_args()

    if args.min_count < 0:
        raise ValueError("--min-count must be >= 0")

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must be <= --end-date")

    from unified_trading_library import get_bucket_name, read_availability_index

    bucket = get_bucket_name("instruments", args.category)
    index = read_availability_index(bucket)
    if index.empty:
        raise RuntimeError(f"Manifest index is empty for bucket={bucket}")

    if "date" not in index.columns:
        raise RuntimeError(f"Manifest index missing required 'date' column: columns={index.columns.tolist()}")
    if "venue" not in index.columns:
        raise RuntimeError(f"Manifest index missing required 'venue' column: columns={index.columns.tolist()}")

    count_col = "instrument_count" if "instrument_count" in index.columns else "row_count"
    if count_col not in index.columns:
        raise RuntimeError(
            "Manifest index missing count column; expected one of 'instrument_count' or 'row_count', "
            f"columns={index.columns.tolist()}"
        )

    index = index.copy()
    index["date"] = pd.to_datetime(index["date"]).dt.date
    index["venue"] = index["venue"].astype(str).str.upper()
    venue = args.venue.upper()
    venue_rows = index[index["venue"] == venue]

    if venue_rows.empty:
        raise RuntimeError(f"No manifest rows found for venue={venue} in bucket={bucket}")

    expected_dates = _date_range(start, end)
    grouped = venue_rows.groupby("date", as_index=True)[count_col].sum()
    missing_dates = [d for d in expected_dates if d not in grouped.index]
    low_count_dates = [d for d in expected_dates if d in grouped.index and int(grouped.loc[d]) < args.min_count]

    print(f"bucket={bucket}")
    print(f"venue={venue}")
    print(f"range={start.isoformat()}..{end.isoformat()} ({len(expected_dates)} days)")
    print(f"rows_for_venue={len(venue_rows)}")
    print(f"present_dates={len(expected_dates) - len(missing_dates)}")
    print(f"missing_dates={len(missing_dates)}")
    print(f"low_count_dates={len(low_count_dates)} min_count={args.min_count}")

    if missing_dates:
        preview = ",".join(d.isoformat() for d in missing_dates[: args.max_missing_print])
        print(f"missing_preview={preview}")
    if low_count_dates:
        preview = ",".join(f"{d.isoformat()}:{int(grouped.loc[d])}" for d in low_count_dates[: args.max_missing_print])
        print(f"low_count_preview={preview}")

    if missing_dates or low_count_dates:
        raise SystemExit(1)

    print("coverage=OK")


if __name__ == "__main__":
    main()
