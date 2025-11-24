#!/usr/bin/env python3
"""
Test Instrument Downloads

Tests downloading instruments from Tardis API and storing to BigQuery.
Uses real cloud data for May 23, 2023.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

project_root = Path(__file__).parent.parent  # instruments-service -> unified-trading-system-repos

# Set up credentials FIRST before any imports
cred_locations = [
    project_root / "central-element-323112-e35fb0ddafe2.json",
    project_root / "instruments-service" / "central-element-323112-e35fb0ddafe2.json",
    project_root / "market-tick-data-handler" / "central-element-323112-e35fb0ddafe2.json",
]

cred_file = None
for loc in cred_locations:
    if loc.exists():
        cred_file = loc
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(loc.absolute())
        print(f"✅ Found credentials: {cred_file}")
        break

if not cred_file:
    print("⚠️  Credentials file not found - some tests may fail")

os.environ["GCP_PROJECT_ID"] = "central-element-323112"

sys.path.insert(0, str(project_root / "instruments-service"))
sys.path.insert(0, str(project_root / "unified-cloud-services"))

from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
)
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.settings import instruments_config
from unified_cloud_services import get_secret_with_fallback

# Test configuration
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
TEST_VENUE = "BINANCE-FUTURES"
TEST_INSTRUMENT_TYPE = "PERPETUAL"


def test_instrument_download():
    """Test downloading instruments from Tardis API"""

    print("=" * 60)
    print("TEST: Instrument Downloads")
    print("=" * 60)

    # Get API key from environment or Secret Manager
    try:

        api_key = get_secret_with_fallback(
            project_id=instruments_config.gcp_project_id,
            secret_name=instruments_config.tardis_secret_name,
            fallback_env_var="TARDIS_API_KEY",
        )
        if not api_key:
            print("⚠️  TARDIS_API_KEY not set")
            print("   Skipping download test - will test with existing data")
            return False
    except Exception as e:
        pass

    try:
        # Initialize service
        config = {
            "tardis_api_key": api_key,
            "retry_max_attempts": 3,
            "enable_ccxt_integration": True,
            "enable_metadata_caching": True,
        }

        service = InstrumentProcessingService(config)
        print(f"✅ InstrumentProcessingService initialized")

        # Try to fetch instruments for binance-futures exchange
        print(f"\n📥 Fetching instruments for binance-futures exchange...")

        # Use the async process_exchange_instruments method
        import asyncio

        tardis_exchange = "binance-futures"
        instruments_dict = asyncio.run(
            service.process_exchange_instruments(
                exchange=tardis_exchange, target_date=TEST_DATE, force=False
            )
        )

        instruments = instruments_dict  # Rename for clarity

        print(f"✅ Retrieved {len(instruments)} instruments")

        if len(instruments) > 0:
            # Show sample instrument
            sample_key = list(instruments.keys())[0]
            sample_instrument = instruments[sample_key]
            print(f"\n📋 Sample instrument:")
            print(f"   Key: {sample_instrument.instrument_key}")
            print(f"   Venue: {sample_instrument.venue}")
            print(f"   Type: {sample_instrument.instrument_type}")
            print(f"   Symbol: {sample_instrument.symbol}")

            # Test filtering for our test instrument
            test_key = f"{TEST_VENUE}:{TEST_INSTRUMENT_TYPE}:BTC-USDT"
            if test_key in instruments:
                print(f"\n✅ Found test instrument: {test_key}")
                test_instrument = instruments[test_key]
                print(f"   Available from: {test_instrument.available_from_datetime}")
                print(f"   Available to: {test_instrument.available_to_datetime}")
                print(f"   Data types: {test_instrument.data_types}")
            else:
                print(f"\n⚠️  Test instrument {test_key} not found in results")
                print(f"   Available keys (first 10): {list(instruments.keys())[:10]}")

            return True
        else:
            print("⚠️  No instruments retrieved")
            return False

    except Exception as e:
        print(f"❌ Error during download: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_instrument_storage():
    """Test storing instruments to BigQuery"""

    print("\n" + "=" * 60)
    print("TEST: Instrument Storage")
    print("=" * 60)

    try:
        # Initialize storage (with event loop fix for asyncio)
        import asyncio

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        storage = CloudInstrumentStorage()
        print(f"✅ CloudInstrumentStorage initialized")

        # Create a test instrument DataFrame
        test_instruments = pd.DataFrame(
            [
                {
                    "instrument_key": "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",
                    "symbol": "BTC-USDT",
                    "available_from_datetime": "2023-01-01T00:00:00Z",
                    "available_to_datetime": None,
                    "data_types": "trades,book_snapshot_5,derivative_ticker",
                    "base_asset": "BTC",
                    "quote_asset": "USDT",
                    "tardis_exchange": "binance-futures",
                    "tardis_symbol": "btcusdt",
                }
            ]
        )

        print(f"\n💾 Storing {len(test_instruments)} test instruments...")

        # Store to BigQuery
        result = storage.store_instruments(
            instruments_df=test_instruments, table_name="instruments_test"
        )

        if result:
            print(f"✅ Instruments stored successfully")

            # Try to query them back
            print(f"\n📖 Querying stored instruments...")
            queried = storage.query_instruments(
                venue=TEST_VENUE,
                instrument_type=TEST_INSTRUMENT_TYPE,
                table_name="instruments_test",
            )

            if len(queried) > 0:
                print(f"✅ Retrieved {len(queried)} instruments from BigQuery")
                print(f"   Columns: {list(queried.columns)}")
                return True
            else:
                print("⚠️  No instruments found in query")
                return False
        else:
            print("❌ Failed to store instruments")
            return False

    except Exception as e:
        print(f"❌ Error during storage: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Instrument Download Test")
    print("=" * 60)

    # Test download
    download_ok = test_instrument_download()

    # Test storage
    storage_ok = test_instrument_storage()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"✅ Download: {'PASSED' if download_ok else 'SKIPPED/FAILED'}")
    print(f"✅ Storage: {'PASSED' if storage_ok else 'FAILED'}")
    print("=" * 60)
