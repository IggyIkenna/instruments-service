#!/usr/bin/env python3
"""
Example: Query Instruments Data

Demonstrates how to query instruments data using unified-trading-library.

This example shows:
- Querying instruments for a specific date
- Filtering by venue, instrument type, etc.
- Getting instrument details
- Summary statistics
"""

import logging
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext

# Simple imports - assumes packages are installed
from unified_trading_library import create_instruments_client

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def query_instruments_example():
    """Example: Query instruments for a specific date."""
    logger.info("=" * 60)
    logger.info("Example: Query Instruments for Date")
    logger.info("=" * 60)

    # Create client
    client = create_instruments_client()

    # Query instruments
    date = "2023-05-23"
    instruments_df = client.get_instruments_for_date(date=date, venue="BINANCE-FUTURES", instrument_type="PERPETUAL")

    logger.info(f"\n✅ Retrieved {len(instruments_df)} instruments for {date}")
    if not instruments_df.empty:
        logger.info("\nSample instruments:")
        logger.info(instruments_df[["instrument_key", "venue", "instrument_type", "symbol"]].head())

    return instruments_df


def query_instruments_filtered():
    """Example: Query instruments with multiple filters."""
    logger.info("\n" + "=" * 60)
    logger.info("Example: Query Instruments with Filters")
    logger.info("=" * 60)

    client = create_instruments_client()

    instruments_df = client.get_instruments_for_date(
        date="2023-05-23",
        venue="BINANCE-FUTURES",
        instrument_type="PERPETUAL",
        base_currency="BTC",
        quote_currency="USDT",
    )

    logger.info(f"\n✅ Retrieved {len(instruments_df)} BTC-USDT perpetuals")
    if not instruments_df.empty:
        logger.info("\nInstruments:")
        logger.info(instruments_df[["instrument_key", "symbol", "base_asset", "quote_asset"]].head())

    return instruments_df


def query_instrument_details():
    """Example: Get details for a specific instrument."""
    logger.info("\n" + "=" * 60)
    logger.info("Example: Get Instrument Details")
    logger.info("=" * 60)

    client = create_instruments_client()

    instrument_id = "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
    details = client.get_instrument_details(date="2023-05-23", instrument_id=instrument_id)

    if details:
        logger.info(f"\n✅ Found instrument: {instrument_id}")
        logger.info("\nDetails:")
        for key, value in details.items():
            if value:  # Only show non-empty values
                logger.info(f"  {key}: {value}")
    else:
        logger.info(f"\n⚠️ Instrument not found: {instrument_id}")

    return details


def query_summary_stats():
    """Example: Get summary statistics."""
    logger.info("\n" + "=" * 60)
    logger.info("Example: Summary Statistics")
    logger.info("=" * 60)

    client = create_instruments_client()

    stats = client.get_summary_stats(date="2023-05-23")

    logger.info("\n✅ Summary statistics for 2023-05-23:")
    logger.info(f"  Total instruments: {stats.get('total_instruments', 0)}")
    logger.info(f"  Venues: {stats.get('venues', 0)}")
    logger.info(f"  Instrument types: {stats.get('instrument_types', 0)}")

    if "venue_breakdown" in stats:
        logger.info("\n  Venue breakdown:")
        for venue, count in list(stats["venue_breakdown"].items())[:5]:
            logger.info(f"    {venue}: {count}")

    return stats


def query_by_data_type():
    """Example: Query instruments by data type."""
    logger.info("\n" + "=" * 60)
    logger.info("Example: Instruments by Data Type")
    logger.info("=" * 60)

    client = create_instruments_client()

    # Get instruments with liquidations data
    instruments_df = client.get_instruments_by_data_type(
        date="2023-05-23", data_type="liquidations", venue="BINANCE-FUTURES", limit=10
    )

    logger.info(f"\n✅ Found {len(instruments_df)} instruments with liquidations data")
    if not instruments_df.empty:
        logger.info("\nSample instruments:")
        logger.info(instruments_df[["instrument_key", "symbol", "data_types"]].head())

    return instruments_df


def query_date_range():
    """Example: Query instruments across date range."""
    logger.info("\n" + "=" * 60)
    logger.info("Example: Query Instruments Across Date Range")
    logger.info("=" * 60)

    client = create_instruments_client()

    instruments_df = client.get_instruments_date_range(
        start_date="2023-05-23",
        end_date="2023-05-24",
        venue="BINANCE-FUTURES",
        instrument_type="PERPETUAL",
    )

    logger.info(f"\n✅ Retrieved {len(instruments_df)} unique instruments across date range")
    if not instruments_df.empty:
        logger.info("\nSample instruments:")
        logger.info(instruments_df[["instrument_key", "venue", "symbol"]].head())

    return instruments_df


def main():
    """Run all query examples."""
    logger.info("🚀 Instruments Service - Query Examples")
    logger.info("=" * 60)
    logger.info("\nThis demonstrates how to query instruments data using unified-trading-library")
    logger.info("=" * 60)

    try:
        # Example 1: Basic query
        query_instruments_example()

        # Example 2: Filtered query
        query_instruments_filtered()

        # Example 3: Instrument details
        query_instrument_details()

        # Example 4: Summary statistics
        query_summary_stats()

        # Example 5: By data type
        query_by_data_type()

        # Example 6: Date range
        query_date_range()

        logger.info("\n" + "=" * 60)
        logger.info("✅ All examples completed successfully!")
        logger.info("=" * 60)

    except (OSError, ValueError, RuntimeError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        logger.info(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
