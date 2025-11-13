"""
Summary of Historical Metadata and Data Availability for Hyperliquid and Aster

Generated: 2025-11-10
"""

# Historical Metadata Availability

## Hyperliquid

**API Metadata:**
- ✅ Provides current instrument metadata via `/info` endpoint with `type: "meta"`
- ❌ Does NOT provide historical metadata (when instruments were added)
- ✅ Provides `isDelisted` flag for delisted instruments

**Data Availability:**
- ✅ Historical candles available via `candleSnapshot` API
- ✅ L2 book snapshots available via `l2Book` API
- ⚠️ API limits: Only most recent 5000 candles available per request
- ⚠️ Historical data may have gaps for older dates

**Earliest Data Detection:**
- Can test backwards from current date to find earliest available data
- Hyperliquid mainnet launched ~January 2024
- Recommended approach: Use launch date (2024-01-01) as default `available_from`
- For specific instruments, can test backwards to find actual earliest data

**Sample Data Structure:**
- **Candles**: timestamp, open_time, close_time, symbol, interval, open, high, low, close, volume, trades
- **Book Snapshots**: coin, time, levels (bids/asks with price, size, order count)

## Aster

**API Metadata:**
- ✅ Provides current instrument metadata via `/fapi/v1/exchangeInfo`
- ❌ Does NOT provide historical metadata (when instruments were added)
- ✅ Provides `status` field (TRADING, BREAK, etc.)

**Data Availability:**
- ✅ Recent trades available via `/fapi/v1/trades` (last 1000 trades)
- ⚠️ Historical trades endpoint (`/fapi/v1/historicalTrades`) may require authentication
- ✅ Order book depth available via `/fapi/v3/depth`

**Earliest Data Detection:**
- Aster API doesn't provide historical metadata
- Recommended approach: Use exchange launch date or first known trading date
- Can check earliest trade timestamp from recent trades endpoint (limited to last 1000)

**Sample Data Structure:**
- **Trades**: id, price, qty, quoteQty, time, datetime, isBuyerMaker
- **Book Depth**: side, level, price, quantity, lastUpdateId, eventTime, transactionTime

## Recommendations

### For Instrument Definitions:

1. **Hyperliquid:**
   - Use `available_from_datetime = "2024-01-01T00:00:00"` as default (Hyperliquid launch)
   - For major coins (BTC, ETH, SOL), can test backwards to find actual earliest data
   - Set `available_to_datetime = None` (perpetuals don't expire)

2. **Aster:**
   - Use `available_from_datetime = "2024-01-01T00:00:00"` as placeholder
   - Update with actual exchange launch date when known
   - Set `available_to_datetime = None` (perpetuals don't expire)

### For Backfilling Historical Metadata:

1. **Option 1: Use Known Launch Dates**
   - Hyperliquid: ~January 2024
   - Aster: Need to research actual launch date

2. **Option 2: Test Historical Data Availability**
   - For each instrument, test backwards from current date
   - Find earliest date with available data
   - Use that as `available_from_datetime`

3. **Option 3: Use Current Date (Fallback)**
   - If historical metadata unavailable and testing is too slow
   - Use current date as `available_from_datetime`
   - Will miss historical data but ensures current instruments are captured

### Data Types Available:

Both venues support:
- ✅ `trades` - Trade execution data
- ✅ `book_snapshot_5` - Order book depth (5+ levels)

Sample CSV files have been generated in `sample_data/` directory showing the actual data structure.
