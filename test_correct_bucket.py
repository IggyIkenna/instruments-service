#!/usr/bin/env python
"""Test both query methods with correct bucket and date"""

from unified_cloud_services import create_instruments_client
import pandas as pd

print("=" * 70)
print("TEST: Query Methods with Correct Bucket (instruments-store-central-element-323112)")
print("=" * 70)

# Test date that has data
test_date = '2025-11-11'

print(f"\n📅 Testing with date: {test_date}")
print("-" * 70)

# Method 1: Using create_instruments_client (should now use correct default bucket)
print("\n1. Using create_instruments_client() with defaults:")
try:
    client = create_instruments_client()
    print(f"   ✅ Client bucket: {client.cloud_target.gcs_bucket}")
    
    instruments_df = client.get_instruments_for_date(date=test_date)
    print(f"   ✅ Successfully loaded {len(instruments_df)} instruments")
    
    if not instruments_df.empty:
        print(f"   📊 Sample venues: {instruments_df['venue'].unique()[:5].tolist()}")
        print(f"   📊 Total venues: {instruments_df['venue'].nunique()}")
        print(f"   📊 Total instrument types: {instruments_df['instrument_type'].nunique()}")
        print(f"   📊 Columns: {len(instruments_df.columns)}")
        print(f"   📊 Sample instrument keys:")
        for key in instruments_df['instrument_key'].head(3).tolist():
            print(f"      - {key}")
    else:
        print("   ⚠️ No instruments found")
        
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
print("✅ Test Complete")
print("=" * 70)

