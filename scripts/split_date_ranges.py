#!/usr/bin/env python3
"""Split a date range into inclusive chunks.

Output format (CSV-like, one chunk per line):
    YYYY-MM-DD,YYYY-MM-DD
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Split date range into inclusive chunks")
    parser.add_argument("--start-date", required=True, help="Range start (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Range end (YYYY-MM-DD)")
    parser.add_argument(
        "--chunk-days",
        type=int,
        required=True,
        help="Chunk size in days (must be >= 1)",
    )
    args = parser.parse_args()

    if args.chunk_days < 1:
        raise ValueError("--chunk-days must be >= 1")

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if start > end:
        raise ValueError("--start-date must be <= --end-date")

    current = start
    delta = timedelta(days=args.chunk_days - 1)
    while current <= end:
        chunk_end = min(current + delta, end)
        print(f"{current.isoformat()},{chunk_end.isoformat()}")
        current = chunk_end + timedelta(days=1)


if __name__ == "__main__":
    main()
