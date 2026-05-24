#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Audit DeFi instrument parquets for null base_asset_decimals / quote_asset_decimals.

Phase A of hard_schema_phase1_field_flip_migration_2026_05_19 ships model_validator
rules that enforce non-null decimals for new rows. This script audits EXISTING rows
in GCS to quantify the null-decimals backlog so adapters can be prioritised for fixes.

Rules audited:
  Rule 6: base_asset_decimals non-null for ALL DEFI_ONCHAIN types
           (POOL / LENDING / LST / YIELD_BEARING / A_TOKEN / DEBT_TOKEN / STAKING / SPOT_ASSET)
  Rule 7: quote_asset_decimals non-null for POOL type only

Bucket pattern: instruments-store-defi-{project_id}
Blob pattern:   instrument_availability/by_date/day=*/venue=*/instruments.parquet

Output:
  - Per-venue/per-instrument_type null-count table to stdout
  - JSON summary to audit_defi_null_decimals_{date}.json in --output-dir

Read-only: never writes to GCS. Pre-condition: Phase A validator is already shipped
(new rows will not add to null count; this script measures legacy rows only).

Usage::

    cd instruments-service

    # Dry-run — prints findings, writes local JSON
    .venv/bin/python scripts/audit_defi_null_decimals_2026_05_19.py

    # Limit to specific date range
    .venv/bin/python scripts/audit_defi_null_decimals_2026_05_19.py \\
        --from-date 2026-01-01 --to-date 2026-05-19

    # Custom output directory
    .venv/bin/python scripts/audit_defi_null_decimals_2026_05_19.py \\
        --output-dir /path/to/output

References:
  * Plan: plans/active/hard_schema_phase1_field_flip_migration_2026_05_19.md Phase B
  * Validator: unified_api_contracts/internal/reference/instrument.py
    _enforce_per_asset_group_required_fields Rules 6+7 (uac@956bec1)
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
BUCKET_NAME = f"instruments-store-defi-{PROJECT_ID}"
BLOB_PREFIX = "instrument_availability/by_date/"

# Closed set from UAC unified_api_contracts/internal/reference/instrument.py
# DEFI_ONCHAIN_INSTRUMENT_TYPES as of uac@956bec1
DEFI_ONCHAIN_TYPES: frozenset[str] = frozenset(
    {
        "POOL",
        "LENDING",
        "LST",
        "YIELD_BEARING",
        "A_TOKEN",
        "DEBT_TOKEN",
        "STAKING",
        "SPOT_ASSET",
    }
)
# Only POOL is two-asset — requires quote_asset_decimals
POOL_TYPE = "POOL"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit DeFi instrument parquets for null decimals")
    p.add_argument(
        "--from-date",
        default=None,
        help="Start date (inclusive) in YYYY-MM-DD format. Default: scan all available dates.",
    )
    p.add_argument(
        "--to-date",
        default=None,
        help="End date (inclusive) in YYYY-MM-DD format. Default: today.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Local directory to write JSON report. Default: system temp dir.",
    )
    p.add_argument(
        "--max-blobs",
        type=int,
        default=0,
        help="Stop after scanning this many blobs (0 = unlimited). Useful for quick smoke checks.",
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
    null_base_by_key: dict[tuple[str, str], int],
    null_quote_pool_count: dict[str, int],
    sample_rows_base: list[dict[str, object]],
    sample_rows_quote: list[dict[str, object]],
    venue: str,
) -> tuple[int, int]:
    """Audit one instruments.parquet blob. Returns (null_base_count, null_quote_pool_count)."""
    try:
        blob = bucket.blob(blob_name)  # type: ignore[union-attr]
        raw = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("SKIP %s — read error: %s", blob_name, exc)
        return 0, 0

    if "instrument_type" not in df.columns:
        logger.debug("SKIP %s — no instrument_type column", blob_name)
        return 0, 0

    defi_mask = df["instrument_type"].isin(DEFI_ONCHAIN_TYPES)
    defi_df = df[defi_mask]
    if defi_df.empty:
        return 0, 0

    null_base_count = 0
    null_quote_count = 0

    if "base_asset_decimals" in defi_df.columns:
        null_base_mask = defi_df["base_asset_decimals"].isna()
        for itype, grp in defi_df[null_base_mask].groupby("instrument_type"):
            key = (venue, str(itype))
            null_base_by_key[key] = null_base_by_key.get(key, 0) + len(grp)
            null_base_count += len(grp)
            if len(sample_rows_base) < 5:
                for _, row in grp.head(2).iterrows():
                    sample_rows_base.append(
                        {
                            "venue": venue,
                            "instrument_type": str(itype),
                            "instrument_key": str(row.get("instrument_key", "")),
                            "blob": blob_name,
                        }
                    )
    else:
        logger.debug("SKIP base_asset_decimals column absent in %s", blob_name)

    if "quote_asset_decimals" in defi_df.columns:
        pool_mask = defi_df["instrument_type"] == POOL_TYPE
        pool_df = defi_df[pool_mask]
        if not pool_df.empty:
            null_quote_mask = pool_df["quote_asset_decimals"].isna()
            null_quote_pool = int(null_quote_mask.sum())
            if null_quote_pool > 0:
                key = venue
                null_quote_pool_count[key] = null_quote_pool_count.get(key, 0) + null_quote_pool
                null_quote_count = null_quote_pool
                if len(sample_rows_quote) < 5:
                    for _, row in pool_df[null_quote_mask].head(2).iterrows():
                        sample_rows_quote.append(
                            {
                                "venue": venue,
                                "instrument_type": POOL_TYPE,
                                "instrument_key": str(row.get("instrument_key", "")),
                                "blob": blob_name,
                            }
                        )

    return null_base_count, null_quote_count


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

    # Aggregate counts
    null_base_by_key: dict[tuple[str, str], int] = {}
    null_quote_pool_count: dict[str, int] = {}
    sample_rows_base: list[dict[str, object]] = []
    sample_rows_quote: list[dict[str, object]] = []

    total_blobs = 0
    scanned = 0

    for blob in client.list_blobs(BUCKET_NAME, prefix=BLOB_PREFIX):
        name: str = blob.name
        if not name.endswith("instruments.parquet"):
            continue

        # Extract day and venue from path: .../day=YYYY-MM-DD/venue=VENUE/instruments.parquet
        parts = name.split("/")
        day_part = next((p for p in parts if p.startswith("day=")), None)
        venue_part = next((p for p in parts if p.startswith("venue=")), None)
        if not day_part or not venue_part:
            continue

        day_str = day_part.removeprefix("day=")
        venue = venue_part.removeprefix("venue=")

        if not _in_date_range(day_str, from_date, to_date):
            continue

        total_blobs += 1
        _audit_blob(
            bucket,
            name,
            null_base_by_key,
            null_quote_pool_count,
            sample_rows_base,
            sample_rows_quote,
            venue,
        )
        scanned += 1
        if scanned % 50 == 0:
            logger.info("  Scanned %d blobs...", scanned)
        if args.max_blobs > 0 and scanned >= args.max_blobs:
            logger.info("Stopping at --max-blobs %d", args.max_blobs)
            break

    logger.info("Scan complete. %d blobs scanned.", scanned)

    # --- Print summary ---
    total_null_base = sum(null_base_by_key.values())
    total_null_quote = sum(null_quote_pool_count.values())

    print("\n" + "=" * 70)
    print("AUDIT REPORT: DeFi null base_asset_decimals (Rule 6)")
    print("=" * 70)
    if null_base_by_key:
        print(f"{'Venue':<30} {'InstrumentType':<20} {'NullCount':>10}")
        print("-" * 65)
        for (v, itype), count in sorted(null_base_by_key.items()):
            print(f"{v:<30} {itype:<20} {count:>10}")
        print(f"\nTotal null base_asset_decimals rows: {total_null_base}")
    else:
        print("✅ Zero null base_asset_decimals rows found in scanned range.")

    print("\n" + "=" * 70)
    print("AUDIT REPORT: DeFi null quote_asset_decimals for POOL (Rule 7)")
    print("=" * 70)
    if null_quote_pool_count:
        print(f"{'Venue':<30} {'NullCount':>10}")
        print("-" * 45)
        for v, count in sorted(null_quote_pool_count.items()):
            print(f"{v:<30} {count:>10}")
        print(f"\nTotal null quote_asset_decimals (POOL) rows: {total_null_quote}")
    else:
        print("✅ Zero null quote_asset_decimals (POOL) rows found in scanned range.")

    # --- Write JSON report ---
    output_dir = Path(args.output_dir) if args.output_dir else Path(tempfile.gettempdir())
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"audit_defi_null_decimals_{report_date}.json"

    report: dict[str, object] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "bucket": BUCKET_NAME,
        "from_date": str(from_date) if from_date else None,
        "to_date": str(to_date) if to_date else None,
        "blobs_scanned": scanned,
        "rule_6_null_base_asset_decimals": {
            "total": total_null_base,
            "by_venue_and_type": {f"{v}::{t}": c for (v, t), c in null_base_by_key.items()},
            "sample_rows": sample_rows_base,
        },
        "rule_7_null_quote_asset_decimals_pool": {
            "total": total_null_quote,
            "by_venue": null_quote_pool_count,
            "sample_rows": sample_rows_quote,
        },
    }
    output_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nJSON report written to: {output_path}")

    return 0 if (total_null_base == 0 and total_null_quote == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
