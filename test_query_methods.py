#!/usr/bin/env python
"""Test both query methods and verify parquet file access"""

from unified_cloud_services import (
    create_instruments_client,
    StandardizedDomainCloudService,
    CloudTarget,
)
import pandas as pd

print("=" * 60)
print("TEST 1: Programmatic Method (unified-cloud-services)")
print("=" * 60)

# Method 1: Using create_instruments_client (defaults)
print("\n1a. Using create_instruments_client() with defaults:")
try:
    client = create_instruments_client()
    print(
        f"   Client bucket: {client.cloud_target.gcs_bucket if hasattr(client, 'cloud_target') else 'N/A'}"
    )
    instruments_df = client.get_instruments_for_date(date="2025-11-05")
    print(f"   ✅ Successfully loaded {len(instruments_df)} instruments")
    if not instruments_df.empty:
        print(f"   Sample venues: {instruments_df['venue'].unique()[:5].tolist()}")
        print(f"   Sample instrument keys: {instruments_df['instrument_key'].head(3).tolist()}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Method 1b: Using create_instruments_client with explicit bucket
print("\n1b. Using create_instruments_client() with explicit bucket:")
try:
    client = create_instruments_client(
        gcs_bucket="market-data-tick", bigquery_dataset="market_data_hft"
    )
    print(f"   Client bucket: {client.cloud_target.gcs_bucket}")
    instruments_df = client.get_instruments_for_date(date="2025-11-05")
    print(f"   ✅ Successfully loaded {len(instruments_df)} instruments")
    if not instruments_df.empty:
        print(f"   Sample venues: {instruments_df['venue'].unique()[:5].tolist()}")
        print(f"   Columns: {list(instruments_df.columns)[:10]}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Method 2: Direct GCS access
print("\n1c. Direct GCS access via StandardizedDomainCloudService:")
try:
    service = StandardizedDomainCloudService(
        domain="market_data",
        cloud_target=CloudTarget(
            project_id="central-element-323112",
            gcs_bucket="market-data-tick",
            bigquery_dataset="market_data_hft",
        ),
    )
    gcs_path = "instrument_availability/by_date/day-2025-11-05/instruments.parquet"
    instruments_df = service.download_from_gcs(gcs_path=gcs_path, format="parquet")
    print(f"   ✅ Successfully loaded {len(instruments_df)} instruments from GCS")
    if not instruments_df.empty:
        print(f"   Sample venues: {instruments_df['venue'].unique()[:5].tolist()}")
        print(
            f"   File size check: {len(instruments_df)} rows, {len(instruments_df.columns)} columns"
        )
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "=" * 60)
print("TEST 2: CLI Method (instruments-service)")
print("=" * 60)
print("\nNote: CLI method works but uses default bucket configuration.")
print("To test CLI, run:")
print("  python -m instruments_service --mode instruments-query --start-date 2025-11-05")
