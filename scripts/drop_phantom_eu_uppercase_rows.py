#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: uppercase phantom EU rows confirmed dropped from canonical availability_index.parquet

"""Drop uppercase instrument_type expected_unattempted rows from canonical tradfi manifest.

The old v1 enumerator seeded expected_unattempted rows with uppercase instrument_type
(FUTURE, SPOT_PAIR, EQUITY, ETF, INDEX). The v2 enumerator uses lowercase (future,
spot_pair, equity, etf, index). These uppercase rows are phantoms — they will never be
matched by captured rows which use lowercase grain.

This script:
  1. Downloads availability_index.parquet from the tradfi manifest bucket
  2. Identifies rows where capture_status == 'expected_unattempted' AND instrument_type[0].isupper()
  3. In --apply mode: backs up to _index/snapshots/pre_phantom_eu_drop_<ts>.parquet, writes filtered result
  4. Reports counts before/after
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import storage


BUCKET_NAME = "market-data-tick-tradfi-prd-central-element-323112"
CANONICAL_BLOB = "_index/availability_index.parquet"
SNAPSHOT_PREFIX = "_index/snapshots/pre_phantom_eu_drop_"


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop uppercase EU phantom rows from tradfi manifest")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Show what would be dropped (default)")
    parser.add_argument("--apply", action="store_true", help="Actually perform the drop")
    args = parser.parse_args()

    apply_mode = args.apply

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(CANONICAL_BLOB)

    tmp_path = Path("/tmp/availability_index_tradfi.parquet")
    print(f"Downloading gs://{BUCKET_NAME}/{CANONICAL_BLOB} ...")
    blob.download_to_filename(str(tmp_path))

    df = pd.read_parquet(tmp_path)
    print(f"Loaded canonical index: {df.shape[0]:,} rows × {df.shape[1]} cols")

    eu_mask = df["capture_status"] == "expected_unattempted"
    uppercase_eu_mask = eu_mask & df["instrument_type"].str[0].str.isupper().fillna(False)

    total_rows = len(df)
    total_eu = eu_mask.sum()
    phantom_count = uppercase_eu_mask.sum()

    print(f"\nRow breakdown:")
    print(f"  Total rows:                     {total_rows:>10,}")
    print(f"  expected_unattempted rows:       {total_eu:>10,}")
    print(f"  Uppercase EU (phantom) rows:     {phantom_count:>10,}")

    if phantom_count == 0:
        print("\nNo uppercase EU phantom rows found — nothing to do.")
        return

    phantom_df = df[uppercase_eu_mask]
    print(f"\nPhantom EU breakdown by instrument_type:")
    for it, cnt in phantom_df["instrument_type"].value_counts().items():
        print(f"  {it:<30} {cnt:>10,}")

    if not apply_mode:
        print(f"\n[DRY-RUN] Would drop {phantom_count:,} uppercase EU rows.")
        print("Re-run with --apply to execute.")
        return

    # --- APPLY ---
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_blob_name = f"{SNAPSHOT_PREFIX}{ts}.parquet"

    print(f"\n[APPLY] Backing up canonical index to gs://{BUCKET_NAME}/{snapshot_blob_name} ...")
    snapshot_blob = bucket.blob(snapshot_blob_name)
    snapshot_blob.upload_from_filename(str(tmp_path))
    print("  Backup written.")

    filtered_df = df[~uppercase_eu_mask].copy()
    print(f"\n[APPLY] Filtered: {total_rows:,} → {len(filtered_df):,} rows (dropped {phantom_count:,})")

    filtered_path = Path("/tmp/availability_index_tradfi_filtered.parquet")
    filtered_df.to_parquet(str(filtered_path), index=False)

    print(f"[APPLY] Uploading filtered index to gs://{BUCKET_NAME}/{CANONICAL_BLOB} ...")
    blob.upload_from_filename(str(filtered_path))
    print("  Upload complete.")

    # Verify
    print("\n[VERIFY] Re-reading uploaded index ...")
    blob.download_to_filename(str(tmp_path))
    verify_df = pd.read_parquet(tmp_path)
    remaining_phantom = (
        (verify_df["capture_status"] == "expected_unattempted")
        & verify_df["instrument_type"].str[0].str.isupper().fillna(False)
    ).sum()
    print(f"  Rows in uploaded index:      {len(verify_df):,}")
    print(f"  Uppercase EU rows remaining: {remaining_phantom:,}")

    if remaining_phantom == 0:
        print("\nSUCCESS: All uppercase phantom EU rows removed from canonical index.")
    else:
        print(f"\nWARNING: {remaining_phantom:,} uppercase EU rows still present — investigate.")
        sys.exit(1)


if __name__ == "__main__":
    main()
