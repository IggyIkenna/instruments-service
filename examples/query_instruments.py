#!/usr/bin/env python3
"""
Example: Query Instruments Data

Demonstrates how to query instruments data using unified-cloud-services.

This example shows:
- Querying instruments for a specific date
- Filtering by venue, instrument type, etc.
- Getting instrument details
- Summary statistics
"""

import logging

# Simple imports - assumes packages are installed
from unified_cloud_services import create_instruments_client

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def query_instruments_example():
    """Example: Query instruments for a specific date."""
    print("=" * 60)
    print("Example: Query Instruments for Date")
    print("=" * 60)

    # Create client
    client = create_instruments_client()

    # Query instruments
    date = "2023-05-23"
    instruments_df = client.get_instruments_for_date(
        date=date, venue="BINANCE-FUTURES", instrument_type="PERPETUAL"
    )

    print(f"\n✅ Retrieved {len(instruments_df)} instruments for {date}")
    if not instruments_df.empty:
        print("\nSample instruments:")
        print(instruments_df[["instrument_key", "venue", "instrument_type", "symbol"]].head())

    return instruments_df


def query_instruments_filtered():
    """Example: Query instruments with multiple filters."""
    print("\n" + "=" * 60)
    print("Example: Query Instruments with Filters")
    print("=" * 60)

    client = create_instruments_client()

    instruments_df = client.get_instruments_for_date(
        date="2023-05-23",
        venue="BINANCE-FUTURES",
        instrument_type="PERPETUAL",
        base_currency="BTC",
        quote_currency="USDT",
    )

    print(f"\n✅ Retrieved {len(instruments_df)} BTC-USDT perpetuals")
    if not instruments_df.empty:
        print("\nInstruments:")
        print(instruments_df[["instrument_key", "symbol", "base_asset", "quote_asset"]].head())

    return instruments_df


def query_instrument_details():
    """Example: Get details for a specific instrument."""
    print("\n" + "=" * 60)
    print("Example: Get Instrument Details")
    print("=" * 60)

    client = create_instruments_client()

    instrument_id = "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
    details = client.get_instrument_details(date="2023-05-23", instrument_id=instrument_id)

    if details:
        print(f"\n✅ Found instrument: {instrument_id}")
        print("\nDetails:")
        for key, value in details.items():
            if value:  # Only show non-empty values
                print(f"  {key}: {value}")
    else:
        print(f"\n⚠️ Instrument not found: {instrument_id}")

    return details


def query_summary_stats():
    """Example: Get summary statistics."""
    print("\n" + "=" * 60)
    print("Example: Summary Statistics")
    print("=" * 60)

    client = create_instruments_client()

    stats = client.get_summary_stats(date="2023-05-23")

    print("\n✅ Summary statistics for 2023-05-23:")
    print(f"  Total instruments: {stats.get('total_instruments', 0)}")
    print(f"  Venues: {stats.get('venues', 0)}")
    print(f"  Instrument types: {stats.get('instrument_types', 0)}")

    if "venue_breakdown" in stats:
        print("\n  Venue breakdown:")
        for venue, count in list(stats["venue_breakdown"].items())[:5]:
            print(f"    {venue}: {count}")

    return stats


def query_by_data_type():
    """Example: Query instruments by data type."""
    print("\n" + "=" * 60)
    print("Example: Instruments by Data Type")
    print("=" * 60)

    client = create_instruments_client()

    # Get instruments with liquidations data
    instruments_df = client.get_instruments_by_data_type(
        date="2023-05-23", data_type="liquidations", venue="BINANCE-FUTURES", limit=10
    )

    print(f"\n✅ Found {len(instruments_df)} instruments with liquidations data")
    if not instruments_df.empty:
        print("\nSample instruments:")
        print(instruments_df[["instrument_key", "symbol", "data_types"]].head())

    return instruments_df


def query_date_range():
    """Example: Query instruments across date range."""
    print("\n" + "=" * 60)
    print("Example: Query Instruments Across Date Range")
    print("=" * 60)

    client = create_instruments_client()

    instruments_df = client.get_instruments_date_range(
        start_date="2023-05-23",
        end_date="2023-05-24",
        venue="BINANCE-FUTURES",
        instrument_type="PERPETUAL",
    )

    print(f"\n✅ Retrieved {len(instruments_df)} unique instruments across date range")
    if not instruments_df.empty:
        print("\nSample instruments:")
        print(instruments_df[["instrument_key", "venue", "symbol"]].head())

    return instruments_df


def main():
    """Run all query examples."""
    print("🚀 Instruments Service - Query Examples")
    print("=" * 60)
    print("\nThis demonstrates how to query instruments data using unified-cloud-services")
    print("=" * 60)

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

        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
