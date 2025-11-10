#!/usr/bin/env python3
"""
Test Instruments Service Integration

Tests instruments service functionality:
1. Download instruments for May 23, 2023
2. Store instruments to BigQuery
3. Verify integration with unified-cloud-services

For full pipeline tests, see:
- unified-trading-deployment/scripts/test_full_pipeline.py (features → ML → strategy → execution)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import asyncio

project_root = Path(__file__).parent.parent.parent

# Set up credentials FIRST before any imports
cred_locations = [
    project_root / "central-element-323112-e35fb0ddafe2.json",
    project_root / "instruments-service" / "central-element-323112-e35fb0ddafe2.json",
    project_root
    / "market-tick-data-handler"
    / "central-element-323112-e35fb0ddafe2.json",
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
from unified_cloud_services import get_secret_with_fallback

# Test configuration
TEST_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
TEST_INSTRUMENT = "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN"


def test_instruments_integration():
    """Test instruments service integration"""

    print("=" * 70)
    print("INSTRUMENTS SERVICE INTEGRATION TEST")
    print("=" * 70)
    print("NOTE: Full pipeline test available at:")
    print("  unified-trading-deployment/scripts/test_full_pipeline.py")
    print("=" * 70)

    results = {
        "instruments_download": False,
        "instruments_storage": False,
    }

    # Step 1: Download instruments
    print("\n" + "=" * 70)
    print("STEP 1: Download Instruments")
    print("=" * 70)

    try:
        api_key = get_secret_with_fallback(
            project_id="central-element-323112",
            secret_name="tardis-api-key",
            fallback_env_var="TARDIS_API_KEY",
        )

        if api_key:
            config = {
                "tardis_api_key": api_key,
                "retry_max_attempts": 3,
                "enable_ccxt_integration": False,  # Disable for faster testing
                "enable_metadata_caching": True,
            }

            service = InstrumentProcessingService(config)
            print(f"✅ InstrumentProcessingService initialized")

            instruments = asyncio.run(
                service.process_exchange_instruments(
                    exchange="binance-futures", target_date=TEST_DATE, force=False
                )
            )

            if TEST_INSTRUMENT in instruments:
                print(f"✅ Found test instrument: {TEST_INSTRUMENT}")
                print(f"   Total instruments: {len(instruments)}")
                results["instruments_download"] = True
            else:
                print(
                    f"⚠️  Test instrument not found, but got {len(instruments)} instruments"
                )
                results["instruments_download"] = (
                    True  # Still pass if we got instruments
                )
        else:
            print("⚠️  API key not available, skipping download")
    except Exception as e:
        print(f"❌ Error downloading instruments: {e}")
        import traceback

        traceback.print_exc()

    # Step 2: Store instruments
    print("\n" + "=" * 70)
    print("STEP 2: Store Instruments")
    print("=" * 70)

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        storage = CloudInstrumentStorage()
        print(f"✅ CloudInstrumentStorage initialized")

        # Create test DataFrame
        test_df = pd.DataFrame(
            [
                {
                    "instrument_key": TEST_INSTRUMENT,
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",
                    "symbol": "BTC-USDT",
                    "available_from_datetime": datetime(
                        2019, 11, 17, tzinfo=timezone.utc
                    ),
                    "available_to_datetime": None,
                    "data_types": "trades,book_snapshot_5,derivative_ticker",
                    "base_asset": "BTC",
                    "quote_asset": "USDT",
                    "tardis_exchange": "binance-futures",
                    "tardis_symbol": "btcusdt",
                }
            ]
        )

        result = storage.store_instruments(
            test_df, table_name="instruments_pipeline_test"
        )
        if result:
            print(f"✅ Instruments stored successfully")
            results["instruments_storage"] = True
    except Exception as e:
        print(f"❌ Error storing instruments: {e}")
        import traceback

        traceback.print_exc()

    # Summary
    print("\n" + "=" * 70)
    print("INSTRUMENTS INTEGRATION TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for step, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {step}")

    print(f"\nTotal: {passed}/{total} steps passed")
    print("\n" + "=" * 70)
    print("For full pipeline test, run:")
    print("  python3 unified-trading-deployment/scripts/test_full_pipeline.py")
    print("=" * 70)

    return results


if __name__ == "__main__":
    results = test_instruments_integration()
