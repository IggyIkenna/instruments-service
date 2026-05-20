#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Phase C — CeFi empty base_asset / quote_asset audit.

Defensive verification that `InstrumentRecord.base_asset` and `quote_asset` are
non-empty in GCS for all CeFi SPOT_PAIR / PERPETUAL instruments. Model_validator
Rule 1 (shipped in hard_schema_enforcement_2026_05_08.md Phase 1) enforces
non-empty at write-time; this script verifies no legacy rows escaped before the
validator landed.

Expected outcome: zero empty rows. Non-zero counts indicate either:
  (a) a pre-Phase-1 legacy row that slipped through before the validator shipped, or
  (b) an adapter bug that needs a targeted re-fetch.

Bucket pattern: instruments-store-cefi-{project_id}
Blob pattern:   instrument_availability/by_date/day=*/venue=*/instruments.parquet

Output:
  - Per-venue count table to stdout
  - JSON summary to audit_cefi_empty_base_quote_{date}.json in --output-dir

Read-only: never writes to GCS.

Usage::

    cd instruments-service

    # Default scan — all available dates
    .venv/bin/python scripts/audit_cefi_empty_base_quote_2026_05_19.py

    # Limit to recent window
    .venv/bin/python scripts/audit_cefi_empty_base_quote_2026_05_19.py \\
        --from-date 2026-01-01 --to-date 2026-05-19

    # Quick smoke check — first 20 blobs
    .venv/bin/python scripts/audit_cefi_empty_base_quote_2026_05_19.py \\
        --max-blobs 20

References:
  * Plan: plans/active/hard_schema_phase1_field_flip_migration_2026_05_19.md Phase C
  * Validator: unified_api_contracts/internal/reference/instrument.py Rule 1
  * Predecessor audit pattern: audit_defi_null_decimals_2026_05_19.py
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET_NAME = f"instruments-store-cefi-{PROJECT_ID}"
BLOB_PREFIX = "instrument_availability/by_date/"

# CeFi instrument types subject to Rule 1 (base_asset + quote_asset must be non-empty)
CEFI_PAIR_TYPES: frozenset[str] = frozenset({"SPOT_PAIR", "PERPETUAL"})


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit CeFi instrument parquets for empty base/quote asset")
    p.add_argument(
        "--from-date",
        default=None,
        help="Start date (inclusive) YYYY-MM-DD. Default: all available.",
    )
    p.add_argument(
        "--to-date",
        default=None,
        help="End date (inclusive) YYYY-MM-DD. Default: today.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Local directory for JSON report. Default: system temp dir.",
    )
    p.add_argument(
        "--max-blobs",
        type=int,
        default=0,
        help="Stop after this many blobs (0 = unlimited).",
    )
    return p.parse_args()


def _in_date_range(day_str: str, from_date: date | None, to_date: date | None) -> bool:
    try:
        d = date.fromisoformat(day_str)
    except ValueError:
        return False
    if from_date is not None and d < from_date:
        return False
    return not (to_date is not None and d > to_date)


def _audit_blob(
    bucket: object,
    blob_name: str,
    empty_base_by_venue: dict[str, int],
    empty_quote_by_venue: dict[str, int],
    sample_rows: list[dict[str, object]],
    venue: str,
) -> tuple[int, int]:
    """Audit one instruments.parquet blob. Returns (empty_base, empty_quote) counts."""
    try:
        blob = bucket.blob(blob_name)  # type: ignore[union-attr]
        raw = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("SKIP %s — read error: %s", blob_name, exc)
        return 0, 0

    if "instrument_type" not in df.columns:
        return 0, 0

    pair_mask = df["instrument_type"].isin(CEFI_PAIR_TYPES)
    pair_df = df[pair_mask]
    if pair_df.empty:
        return 0, 0

    empty_base = 0
    empty_quote = 0

    if "base_asset" in pair_df.columns:
        empty_base_mask = pair_df["base_asset"].isna() | (pair_df["base_asset"] == "")
        empty_base = int(empty_base_mask.sum())
        if empty_base > 0:
            empty_base_by_venue[venue] = empty_base_by_venue.get(venue, 0) + empty_base
            if len(sample_rows) < 10:
                for _, row in pair_df[empty_base_mask].head(3).iterrows():
                    sample_rows.append(
                        {
                            "venue": venue,
                            "field": "base_asset",
                            "instrument_type": str(row.get("instrument_type", "")),
                            "raw_symbol": str(row.get("raw_symbol", "")),
                            "blob": blob_name,
                        }
                    )

    if "quote_asset" in pair_df.columns:
        empty_quote_mask = pair_df["quote_asset"].isna() | (pair_df["quote_asset"] == "")
        empty_quote = int(empty_quote_mask.sum())
        if empty_quote > 0:
            empty_quote_by_venue[venue] = empty_quote_by_venue.get(venue, 0) + empty_quote
            if len(sample_rows) < 10:
                for _, row in pair_df[empty_quote_mask].head(3).iterrows():
                    sample_rows.append(
                        {
                            "venue": venue,
                            "field": "quote_asset",
                            "instrument_type": str(row.get("instrument_type", "")),
                            "raw_symbol": str(row.get("raw_symbol", "")),
                            "blob": blob_name,
                        }
                    )

    return empty_base, empty_quote


def main() -> int:
    args = _parse_args()

    from_date: date | None = date.fromisoformat(args.from_date) if args.from_date else None
    to_date: date | None = date.fromisoformat(args.to_date) if args.to_date else date.today()

    try:
        from google.cloud import storage as gcs  # type: ignore[import-untyped]
    except ImportError:
        logger.error("google-cloud-storage not installed. Run: uv pip install google-cloud-storage")
        return 1

    client = gcs.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    logger.info("Scanning bucket %s prefix %s", BUCKET_NAME, BLOB_PREFIX)

    empty_base_by_venue: dict[str, int] = {}
    empty_quote_by_venue: dict[str, int] = {}
    sample_rows: list[dict[str, object]] = []

    scanned = 0

    for blob in client.list_blobs(BUCKET_NAME, prefix=BLOB_PREFIX):
        name: str = blob.name
        if not name.endswith("instruments.parquet"):
            continue

        parts = name.split("/")
        day_part = next((p for p in parts if p.startswith("day=")), None)
        venue_part = next((p for p in parts if p.startswith("venue=")), None)
        if not day_part or not venue_part:
            continue

        day_str = day_part.removeprefix("day=")
        venue = venue_part.removeprefix("venue=")

        if not _in_date_range(day_str, from_date, to_date):
            continue

        _audit_blob(bucket, name, empty_base_by_venue, empty_quote_by_venue, sample_rows, venue)
        scanned += 1
        if scanned % 50 == 0:
            logger.info("  Scanned %d blobs...", scanned)
        if args.max_blobs > 0 and scanned >= args.max_blobs:
            logger.info("Stopping at --max-blobs %d", args.max_blobs)
            break

    logger.info("Scan complete. %d blobs scanned.", scanned)

    total_empty_base = sum(empty_base_by_venue.values())
    total_empty_quote = sum(empty_quote_by_venue.values())

    print("\n" + "=" * 70)
    print("AUDIT REPORT: CeFi empty base_asset (Rule 1)")
    print("=" * 70)
    if empty_base_by_venue:
        print(f"{'Venue':<30} {'EmptyCount':>12}")
        print("-" * 45)
        for v, count in sorted(empty_base_by_venue.items()):
            print(f"{v:<30} {count:>12}")
        print(f"\nTotal empty base_asset rows: {total_empty_base}")
    else:
        print("✅ Zero empty base_asset rows found in scanned range.")

    print("\n" + "=" * 70)
    print("AUDIT REPORT: CeFi empty quote_asset (Rule 1)")
    print("=" * 70)
    if empty_quote_by_venue:
        print(f"{'Venue':<30} {'EmptyCount':>12}")
        print("-" * 45)
        for v, count in sorted(empty_quote_by_venue.items()):
            print(f"{v:<30} {count:>12}")
        print(f"\nTotal empty quote_asset rows: {total_empty_quote}")
    else:
        print("✅ Zero empty quote_asset rows found in scanned range.")

    output_dir = Path(args.output_dir) if args.output_dir else Path(tempfile.gettempdir())
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"audit_cefi_empty_base_quote_{report_date}.json"

    report: dict[str, object] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "bucket": BUCKET_NAME,
        "from_date": str(from_date) if from_date else None,
        "to_date": str(to_date) if to_date else None,
        "blobs_scanned": scanned,
        "rule_1_empty_base_asset": {
            "total": total_empty_base,
            "by_venue": empty_base_by_venue,
        },
        "rule_1_empty_quote_asset": {
            "total": total_empty_quote,
            "by_venue": empty_quote_by_venue,
        },
        "sample_rows": sample_rows,
    }
    output_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nJSON report written to: {output_path}")

    return 0 if (total_empty_base == 0 and total_empty_quote == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
