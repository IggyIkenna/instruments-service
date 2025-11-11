"""
Test script to verify historical data availability for Hyperliquid and Aster.

This script tests that historical data exists before adding data types to instrument definitions.
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add instruments_service to path
sys.path.insert(0, str(Path(__file__).parent))

from instruments_service.app.venues.defi.hyperliquid_adapter import HyperliquidAdapter
from instruments_service.app.venues.defi.aster_adapter import AsterAdapter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_hyperliquid_data():
    """Test Hyperliquid historical data availability."""
    logger.info("=" * 80)
    logger.info("Testing Hyperliquid Historical Data Availability")
    logger.info("=" * 80)

    adapter = HyperliquidAdapter()

    # Fetch perpetuals
    logger.info("\n📊 Fetching Hyperliquid perpetuals...")
    perpetuals = adapter.fetch_perpetuals()

    if not perpetuals:
        logger.error("❌ No perpetuals found!")
        return {}

    logger.info(f"✅ Found {len(perpetuals)} perpetuals")

    # Test data availability for top 5 coins
    test_coins = list(perpetuals.keys())[:5]
    test_date = datetime(2023, 5, 23)  # Test date from user's command
    end_date = test_date + timedelta(days=1)

    results = {}

    for inst_key in test_coins:
        inst = perpetuals[inst_key]
        coin = inst["base_asset"]
        logger.info(f"\n🔍 Testing data availability for {coin} ({inst_key})...")

        data_availability = adapter.test_historical_data_availability(
            coin=coin, start_date=test_date, end_date=end_date
        )

        results[inst_key] = {
            "coin": coin,
            "data_availability": data_availability,
        }

        # Log results
        if data_availability["trades"]:
            logger.info(f"  ✅ Trades: Available")
        else:
            logger.warning(f"  ❌ Trades: Not available")

        if data_availability["book_snapshot_5"]:
            logger.info(f"  ✅ Book Snapshot (5 levels): Available")
        else:
            logger.warning(f"  ❌ Book Snapshot (5 levels): Not available")

    return results


def test_aster_data():
    """Test Aster historical data availability."""
    logger.info("\n" + "=" * 80)
    logger.info("Testing Aster Historical Data Availability")
    logger.info("=" * 80)

    adapter = AsterAdapter()

    # Fetch perpetuals
    logger.info("\n📊 Fetching Aster perpetuals...")
    perpetuals = adapter.fetch_perpetuals()

    if not perpetuals:
        logger.error("❌ No perpetuals found!")
        return {}

    logger.info(f"✅ Found {len(perpetuals)} perpetuals")

    # Test data availability for top 5 symbols
    test_symbols = list(perpetuals.keys())[:5]
    test_date = datetime(2023, 5, 23)  # Test date from user's command
    end_date = test_date + timedelta(days=1)

    results = {}

    for inst_key in test_symbols:
        inst = perpetuals[inst_key]
        symbol = inst["exchange_raw_symbol"]  # Use raw symbol like "BTCUSDT"
        logger.info(f"\n🔍 Testing data availability for {symbol} ({inst_key})...")

        data_availability = adapter.test_historical_data_availability(
            symbol=symbol, start_date=test_date, end_date=end_date
        )

        results[inst_key] = {
            "symbol": symbol,
            "data_availability": data_availability,
        }

        # Log results
        if data_availability["trades"]:
            logger.info(f"  ✅ Trades: Available")
        else:
            logger.warning(f"  ❌ Trades: Not available")

        if data_availability["book_snapshot_5"]:
            logger.info(f"  ✅ Book Snapshot (5 levels): Available")
        else:
            logger.warning(f"  ❌ Book Snapshot (5 levels): Not available")

    return results


def main():
    """Run tests for both venues."""
    logger.info("🚀 Starting Historical Data Availability Tests")
    logger.info(f"Test Date: 2023-05-23")

    hyperliquid_results = test_hyperliquid_data()
    aster_results = test_aster_data()

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)

    logger.info(f"\nHyperliquid: Tested {len(hyperliquid_results)} instruments")
    if hyperliquid_results:
        trades_available = sum(
            1 for r in hyperliquid_results.values() if r["data_availability"]["trades"]
        )
        book_available = sum(
            1
            for r in hyperliquid_results.values()
            if r["data_availability"]["book_snapshot_5"]
        )
        logger.info(
            f"  - Trades available: {trades_available}/{len(hyperliquid_results)}"
        )
        logger.info(
            f"  - Book snapshot available: {book_available}/{len(hyperliquid_results)}"
        )

    logger.info(f"\nAster: Tested {len(aster_results)} instruments")
    if aster_results:
        trades_available = sum(
            1 for r in aster_results.values() if r["data_availability"]["trades"]
        )
        book_available = sum(
            1
            for r in aster_results.values()
            if r["data_availability"]["book_snapshot_5"]
        )
        logger.info(f"  - Trades available: {trades_available}/{len(aster_results)}")
        logger.info(
            f"  - Book snapshot available: {book_available}/{len(aster_results)}"
        )

    logger.info("\n✅ Testing complete!")


if __name__ == "__main__":
    main()
