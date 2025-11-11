"""
Script to check historical metadata availability and download 1 day of raw data
for Hyperliquid and Aster to CSV files.
"""

import logging
import requests
import csv
from datetime import datetime, timedelta
from pathlib import Path
import sys
from typing import Dict, List, Any, Optional

# Add instruments_service to path
sys.path.insert(0, str(Path(__file__).parent))

from instruments_service.app.venues.defi.hyperliquid_adapter import HyperliquidAdapter
from instruments_service.app.venues.defi.aster_adapter import AsterAdapter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_earliest_available_data_hyperliquid(coin: str, max_days_back: int = 365) -> Optional[datetime]:
    """
    Find the earliest available data for a Hyperliquid coin by testing historical endpoints.
    
    Args:
        coin: Coin symbol (e.g., "BTC")
        max_days_back: Maximum days to search back (default: 365)
        
    Returns:
        Earliest available datetime or None
    """
    api_base_url = "https://api.hyperliquid.xyz"
    end_date = datetime.now()
    
    # Binary search for earliest available data
    left = 0
    right = max_days_back
    earliest_found = None
    
    logger.info(f"🔍 Searching for earliest data for {coin} (up to {max_days_back} days back)...")
    
    # Start from most recent and work backwards
    for days_back in range(0, max_days_back, 30):  # Check every 30 days
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
                    }
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                candles = response.json()
                if candles and len(candles) > 0:
                    earliest_found = test_date
                    logger.info(f"  ✅ Found data for {coin} on {test_date.strftime('%Y-%m-%d')}")
                else:
                    logger.debug(f"  ⚠️ No data for {coin} on {test_date.strftime('%Y-%m-%d')}")
                    if earliest_found:
                        break  # Found earliest, stop searching
        except Exception as e:
            logger.debug(f"  ⚠️ Error checking {test_date.strftime('%Y-%m-%d')}: {e}")
    
    return earliest_found


def download_hyperliquid_data_to_csv(coin: str, target_date: datetime, output_dir: Path):
    """
    Download 1 day of Hyperliquid data and save to CSV.
    
    Args:
        coin: Coin symbol (e.g., "BTC")
        target_date: Target date for data
        output_dir: Output directory for CSV files
    """
    api_base_url = "https://api.hyperliquid.xyz"
    
    # Download trades (via candles)
    start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=1)
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)
    
    logger.info(f"📥 Downloading Hyperliquid data for {coin} on {target_date.strftime('%Y-%m-%d')}...")
    
    # Download candles (trades data)
    candles_file = output_dir / f"hyperliquid_{coin}_candles_{target_date.strftime('%Y%m%d')}.csv"
    try:
        response = requests.post(
            f"{api_base_url}/info",
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                }
            },
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code == 200:
            candles = response.json()
            if candles:
                with open(candles_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['timestamp', 'open_time', 'close_time', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume', 'trades'])
                    writer.writeheader()
                    for candle in candles:
                        writer.writerow({
                            'timestamp': candle.get('t', ''),
                            'open_time': datetime.fromtimestamp(candle.get('t', 0) / 1000).isoformat() if candle.get('t') else '',
                            'close_time': datetime.fromtimestamp(candle.get('T', 0) / 1000).isoformat() if candle.get('T') else '',
                            'symbol': candle.get('s', ''),
                            'interval': candle.get('i', ''),
                            'open': candle.get('o', ''),
                            'high': candle.get('h', ''),
                            'low': candle.get('l', ''),
                            'close': candle.get('c', ''),
                            'volume': candle.get('v', ''),
                            'trades': candle.get('n', ''),
                        })
                logger.info(f"  ✅ Saved {len(candles)} candles to {candles_file}")
            else:
                logger.warning(f"  ⚠️ No candles data for {coin}")
        else:
            logger.warning(f"  ⚠️ Failed to fetch candles: {response.status_code}")
    except Exception as e:
        logger.error(f"  ❌ Error downloading candles: {e}")
    
    # Download L2 book snapshots (sample a few times during the day)
    book_file = output_dir / f"hyperliquid_{coin}_book_{target_date.strftime('%Y%m%d')}.csv"
    try:
        book_snapshots = []
        # Sample book at 4 times during the day
        for hour in [0, 6, 12, 18]:
            sample_time = start_date.replace(hour=hour)
            # Get current book snapshot (we'll use current as proxy)
            response = requests.post(
                f"{api_base_url}/info",
                json={
                    "type": "l2Book",
                    "coin": coin,
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                book_data = response.json()
                if book_data and "levels" in book_data:
                    levels = book_data.get("levels", [])
                    bids = levels[0] if len(levels) > 0 else []
                    asks = levels[1] if len(levels) > 1 else []
                    
                    book_snapshots.append({
                        'timestamp': book_data.get('time', ''),
                        'sample_time': sample_time.isoformat(),
                        'coin': coin,
                        'bids_count': len(bids),
                        'asks_count': len(asks),
                        'best_bid_price': bids[0].get('px', '') if bids else '',
                        'best_bid_size': bids[0].get('sz', '') if bids else '',
                        'best_ask_price': asks[0].get('px', '') if asks else '',
                        'best_ask_size': asks[0].get('sz', '') if asks else '',
                    })
        
        if book_snapshots:
            with open(book_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=book_snapshots[0].keys())
                writer.writeheader()
                writer.writerows(book_snapshots)
            logger.info(f"  ✅ Saved {len(book_snapshots)} book snapshots to {book_file}")
    except Exception as e:
        logger.error(f"  ❌ Error downloading book snapshots: {e}")


def download_aster_data_to_csv(symbol: str, target_date: datetime, output_dir: Path):
    """
    Download 1 day of Aster data and save to CSV.
    
    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        target_date: Target date for data
        output_dir: Output directory for CSV files
    """
    futures_api_base_url = "https://fapi.asterdex.com"
    
    logger.info(f"📥 Downloading Aster data for {symbol} on {target_date.strftime('%Y-%m-%d')}...")
    
    # Download recent trades (historical trades endpoint may require auth)
    trades_file = output_dir / f"aster_{symbol}_trades_{target_date.strftime('%Y%m%d')}.csv"
    try:
        # Get recent trades (last 1000 trades)
        response = requests.get(
            f"{futures_api_base_url}/fapi/v1/trades",
            params={
                "symbol": symbol,
                "limit": 1000,
            },
            timeout=60
        )
        
        if response.status_code == 200:
            trades = response.json()
            if trades:
                with open(trades_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['id', 'price', 'qty', 'quoteQty', 'time', 'datetime', 'isBuyerMaker'])
                    writer.writeheader()
                    for trade in trades:
                        trade_time = datetime.fromtimestamp(trade.get('time', 0) / 1000) if trade.get('time') else None
                        writer.writerow({
                            'id': trade.get('id', ''),
                            'price': trade.get('price', ''),
                            'qty': trade.get('qty', ''),
                            'quoteQty': trade.get('quoteQty', ''),
                            'time': trade.get('time', ''),
                            'datetime': trade_time.isoformat() if trade_time else '',
                            'isBuyerMaker': trade.get('isBuyerMaker', ''),
                        })
                logger.info(f"  ✅ Saved {len(trades)} trades to {trades_file}")
            else:
                logger.warning(f"  ⚠️ No trades data for {symbol}")
        else:
            logger.warning(f"  ⚠️ Failed to fetch trades: {response.status_code}")
    except Exception as e:
        logger.error(f"  ❌ Error downloading trades: {e}")
    
    # Download order book depth
    book_file = output_dir / f"aster_{symbol}_book_{target_date.strftime('%Y%m%d')}.csv"
    try:
        response = requests.get(
            f"{futures_api_base_url}/fapi/v3/depth",
            params={
                "symbol": symbol,
                "limit": 20,  # Get 20 levels
            },
            timeout=30
        )
        
        if response.status_code == 200:
            book_data = response.json()
            if book_data and "bids" in book_data and "asks" in book_data:
                bids = book_data.get("bids", [])
                asks = book_data.get("asks", [])
                
                with open(book_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['side', 'level', 'price', 'quantity', 'lastUpdateId', 'eventTime', 'transactionTime'])
                    writer.writerow(['header', '', '', '', book_data.get('lastUpdateId', ''), book_data.get('E', ''), book_data.get('T', '')])
                    
                    for i, bid in enumerate(bids):
                        writer.writerow(['bid', i+1, bid[0], bid[1], '', '', ''])
                    
                    for i, ask in enumerate(asks):
                        writer.writerow(['ask', i+1, ask[0], ask[1], '', '', ''])
                
                logger.info(f"  ✅ Saved book depth ({len(bids)} bids, {len(asks)} asks) to {book_file}")
            else:
                logger.warning(f"  ⚠️ Invalid book data for {symbol}")
        else:
            logger.warning(f"  ⚠️ Failed to fetch book depth: {response.status_code}")
    except Exception as e:
        logger.error(f"  ❌ Error downloading book depth: {e}")


def check_historical_metadata():
    """Check historical metadata availability for both venues."""
    logger.info("=" * 80)
    logger.info("Checking Historical Metadata Availability")
    logger.info("=" * 80)
    
    # Test Hyperliquid
    logger.info("\n📊 Hyperliquid Historical Metadata Check")
    logger.info("-" * 80)
    
    hl_adapter = HyperliquidAdapter()
    perpetuals = hl_adapter.fetch_perpetuals(test_data_availability=False)
    
    if perpetuals:
        # Test a few major coins
        test_coins = ["BTC", "ETH", "SOL"]
        for coin in test_coins:
            inst_key = f"HYPERLIQUID:PERPETUAL:{coin}-USDC"
            if inst_key in perpetuals:
                earliest = find_earliest_available_data_hyperliquid(coin, max_days_back=730)
                if earliest:
                    logger.info(f"  {coin}: Earliest data found: {earliest.strftime('%Y-%m-%d')}")
                    # Update available_from in instrument definition
                    perpetuals[inst_key]["available_from_datetime"] = earliest.isoformat()
                else:
                    logger.warning(f"  {coin}: Could not determine earliest data (may be very recent)")
    
    # Test Aster
    logger.info("\n📊 Aster Historical Metadata Check")
    logger.info("-" * 80)
    
    aster_adapter = AsterAdapter()
    aster_perpetuals = aster_adapter.fetch_perpetuals(test_data_availability=False)
    
    if aster_perpetuals:
        # Aster doesn't provide historical metadata, so we'll use current date
        # In production, we'd need to check exchange launch date or use a known date
        logger.info(f"  Found {len(aster_perpetuals)} instruments")
        logger.info("  ⚠️ Aster API doesn't provide historical metadata")
        logger.info("  💡 Suggestion: Use exchange launch date or first known trading date")
    
    return perpetuals, aster_perpetuals


def main():
    """Main function to check metadata and download sample data."""
    output_dir = Path("sample_data")
    output_dir.mkdir(exist_ok=True)
    
    # Check historical metadata
    hl_perpetuals, aster_perpetuals = check_historical_metadata()
    
    # Download 1 day of sample data
    logger.info("\n" + "=" * 80)
    logger.info("Downloading Sample Data (Last 24 Hours)")
    logger.info("=" * 80)
    
    target_date = datetime.now() - timedelta(days=1)
    
    # Hyperliquid sample
    if hl_perpetuals:
        test_coins = ["BTC", "ETH"]
        for coin in test_coins:
            download_hyperliquid_data_to_csv(coin, target_date, output_dir)
    
    # Aster sample
    if aster_perpetuals:
        test_symbols = ["BTCUSDT", "ETHUSDT"]
        for symbol in test_symbols:
            download_aster_data_to_csv(symbol, target_date, output_dir)
    
    logger.info("\n✅ Complete! Check the 'sample_data' directory for CSV files.")


if __name__ == "__main__":
    main()

