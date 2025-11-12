"""
Enhanced adapter methods to find earliest available data and check free sources
"""

import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging

try:
    import boto3

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

logger = logging.getLogger(__name__)


def check_hyperliquid_s3_metadata(coin: str) -> Optional[datetime]:
    """
    Check Hyperliquid S3 archive for metadata about when instrument was added.

    Args:
        coin: Coin symbol (e.g., "BTC")

    Returns:
        Earliest available date from S3 metadata or None
    """
    if not BOTO3_AVAILABLE:
        logger.warning("boto3 not available, cannot check S3 metadata")
        return None

    try:
        # Hyperliquid S3 bucket: s3://hyperliquid-archive
        # Structure: market_data/YYYYMMDD/HH/l2Book/COIN.lz4
        # We can list objects to find earliest date

        s3_client = boto3.client("s3")
        bucket = "hyperliquid-archive"
        prefix = f"market_data/"

        # List objects to find earliest date for this coin
        # Note: This requires AWS credentials and may incur costs
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        earliest_date = None
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Check if this object is for our coin
                    if coin in obj["Key"]:
                        # Extract date from path: market_data/YYYYMMDD/...
                        parts = obj["Key"].split("/")
                        if len(parts) >= 2:
                            date_str = parts[1]  # YYYYMMDD
                            try:
                                obj_date = datetime.strptime(date_str, "%Y%m%d")
                                if earliest_date is None or obj_date < earliest_date:
                                    earliest_date = obj_date
                            except ValueError:
                                continue

        return earliest_date

    except Exception as e:
        logger.warning(f"Could not check S3 metadata for {coin}: {e}")
        return None


def find_earliest_data_via_testing_hyperliquid(
    coin: str, max_days_back: int = 730, step_days: int = 30
) -> Optional[datetime]:
    """
    Find earliest available data by testing historical endpoints.

    Args:
        coin: Coin symbol
        max_days_back: Maximum days to search back
        step_days: Days to step back each iteration (larger = faster but less precise)

    Returns:
        Earliest date with available data
    """
    api_base_url = "https://api.hyperliquid.xyz"
    end_date = datetime.now()
    earliest_found = None

    # Binary search approach: start from most recent, work backwards
    for days_back in range(0, max_days_back, step_days):
        test_date = end_date - timedelta(days=days_back)
        start_ms = int((test_date - timedelta(days=1)).timestamp() * 1000)
        end_ms = int(test_date.timestamp() * 1000)

        try:
            response = requests.post(
                f"{api_base_url}/info",
                json={
                    "type": "candleSnapshot",
                    "req": {
                        "coin": coin,
                        "interval": "1h",  # Use 1h for faster checks
                        "startTime": start_ms,
                        "endTime": end_ms,
                    },
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 200:
                candles = response.json()
                if candles and len(candles) > 0:
                    earliest_found = test_date
                    logger.info(f"Found data for {coin} on {test_date.strftime('%Y-%m-%d')}")
                else:
                    # No data found, stop searching backwards
                    if earliest_found:
                        break
        except Exception as e:
            logger.debug(f"Error checking {test_date}: {e}")
            continue

    return earliest_found


def check_amberdata_free_tier(venue: str, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Check Amberdata free tier for historical metadata.

    Args:
        venue: "hyperliquid" or "aster"
        symbol: Instrument symbol

    Returns:
        Metadata dict with available_from date if found
    """
    # Amberdata API endpoint (would need API key for free tier)
    # Check their docs: https://docs.amberdata.io
    # Free tier may have limited access

    # Placeholder - would need to implement actual API call
    logger.info(f"Amberdata free tier check not yet implemented for {venue}/{symbol}")
    return None


def check_allium_free_tier(venue: str, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Check Allium free tier for historical metadata.

    Args:
        venue: "hyperliquid" or "aster"
        symbol: Instrument symbol

    Returns:
        Metadata dict with available_from date if found
    """
    # Allium API endpoint (would need API key)
    # Check their docs: https://docs.allium.so
    # Free tier may have limited access

    # Placeholder - would need to implement actual API call
    logger.info(f"Allium free tier check not yet implemented for {venue}/{symbol}")
    return None


def get_earliest_funding_rate_aster(symbol: str) -> Optional[datetime]:
    """
    Get earliest funding rate for Aster symbol to estimate listing date.
    Uses Aster fundingRate API with startTime=0 (epoch) and limit=1.

    According to context7 docs: /fapi/v1/fundingRate returns data in ascending order,
    so querying from epoch (0) with limit=1 gives us the earliest funding rate.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")

    Returns:
        Earliest funding rate timestamp or None
    """
    futures_api_base_url = "https://fapi.asterdex.com"

    try:
        # Query from epoch (0) with limit=1 - API returns in ascending order
        # The first record will be the earliest funding rate
        response = requests.get(
            f"{futures_api_base_url}/fapi/v1/fundingRate",
            params={
                "symbol": symbol,
                "startTime": 0,  # Start from epoch to get earliest
                "limit": 1,  # Only need the first (earliest) record
            },
            timeout=30,
        )

        if response.status_code == 200:
            funding_rates = response.json()
            if funding_rates and len(funding_rates) > 0:
                # Get the first (earliest) funding rate timestamp
                earliest_time_ms = funding_rates[0].get("fundingTime")
                if earliest_time_ms:
                    earliest_dt = datetime.fromtimestamp(earliest_time_ms / 1000)
                    # Filter out future dates and very recent dates (Aster may only return recent data)
                    now = datetime.now()
                    # If the "earliest" date is very recent (within last 30 days), Aster likely doesn't have historical data
                    # In this case, return None to use conservative default
                    days_ago = (now - earliest_dt).days
                    if earliest_dt <= now and days_ago > 30:
                        logger.debug(
                            f"Found earliest funding rate for {symbol}: {earliest_dt.isoformat()}"
                        )
                        return earliest_dt
                    else:
                        logger.debug(
                            f"Aster funding rate for {symbol} is too recent ({days_ago} days ago), using default"
                        )
                        return None
        elif response.status_code == 429:
            logger.warning(f"Rate limited when fetching funding rates for {symbol}")
        else:
            logger.debug(f"Failed to fetch funding rates for {symbol}: {response.status_code}")

    except Exception as e:
        logger.debug(f"Error getting earliest funding rate for {symbol}: {e}")

    return None


def get_earliest_trade_timestamp_aster(symbol: str) -> Optional[datetime]:
    """
    Get earliest trade timestamp from Aster recent trades endpoint.

    Note: Limited to last 1000 trades, so may not find true earliest.
    Better to use get_earliest_funding_rate_aster() first.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")

    Returns:
        Earliest trade timestamp found or None
    """
    futures_api_base_url = "https://fapi.asterdex.com"

    try:
        response = requests.get(
            f"{futures_api_base_url}/fapi/v1/trades",
            params={
                "symbol": symbol,
                "limit": 1000,  # Maximum allowed
            },
            timeout=30,
        )

        if response.status_code == 200:
            trades = response.json()
            if trades:
                # Find earliest timestamp
                earliest_time = None
                for trade in trades:
                    trade_time_ms = trade.get("time", 0)
                    if trade_time_ms:
                        trade_time = datetime.fromtimestamp(trade_time_ms / 1000)
                        if earliest_time is None or trade_time < earliest_time:
                            earliest_time = trade_time

                return earliest_time
    except Exception as e:
        logger.warning(f"Error getting earliest trade for {symbol}: {e}")

    return None


# Integration into adapters

INTEGRATION_EXAMPLE = """
# In HyperliquidAdapter._convert_asset_to_instrument():
# Try multiple sources in order of preference

available_from = None

# 1. Try S3 metadata (if AWS credentials available)
if not available_from:
    available_from = check_hyperliquid_s3_metadata(coin)

# 2. Try historical data testing (slower but free)
if not available_from:
    available_from = find_earliest_data_via_testing_hyperliquid(coin, max_days_back=365)

# 3. Try third-party free tiers
if not available_from:
    metadata = check_amberdata_free_tier("hyperliquid", coin)
    if metadata:
        available_from = metadata.get("available_from")

# 4. Fallback to launch date
if not available_from:
    available_from = datetime(2024, 1, 1).isoformat()

# In AsterAdapter._convert_symbol_to_instrument():
# Similar approach but with Aster-specific methods

available_from = None

# 1. Try earliest trade timestamp (limited to last 1000 trades)
if not available_from:
    earliest_trade = get_earliest_trade_timestamp_aster(symbol)
    if earliest_trade:
        available_from = earliest_trade.isoformat()

# 2. Try third-party free tiers
if not available_from:
    metadata = check_amberdata_free_tier("aster", symbol)
    if metadata:
        available_from = metadata.get("available_from")

# 3. Fallback to conservative estimate
if not available_from:
    available_from = datetime(2024, 1, 1).isoformat()
"""

if __name__ == "__main__":
    print("Historical Metadata Helper Functions")
    print("=" * 80)
    print(INTEGRATION_EXAMPLE)
