# Historical Metadata Plan for Aster and Hyperliquid

## Problem Statement
Neither Aster nor Hyperliquid APIs provide direct historical metadata about when instruments were added. We need to determine `available_from_datetime` for accurate instrument definitions.

## Free Sources Identified

### Aster

#### ✅ **1. Funding Rate History Endpoint** (NEW FINDING!)
- **Endpoint**: `GET /fapi/v1/fundingRate`
- **Parameters**: `symbol`, `startTime`, `endTime`, `limit` (max 1000)
- **Method**: Query funding rates with very early `startTime` to find earliest available data
- **Advantage**: Funding rates are published regularly, earliest rate = approximate listing date
- **Status**: ✅ **IMPLEMENTABLE NOW**

```python
# Example implementation
def get_earliest_funding_rate_aster(symbol: str) -> Optional[datetime]:
    """Get earliest funding rate to estimate listing date"""
    response = requests.get(
        "https://fapi.asterdex.com/fapi/v1/fundingRate",
        params={
            "symbol": symbol,
            "startTime": 0,  # Start from epoch
            "limit": 1,  # Get first (earliest) record
        }
    )
    if response.status_code == 200:
        data = response.json()
        if data and len(data) > 0:
            return datetime.fromtimestamp(data[0]['fundingTime'] / 1000)
    return None
```

#### ✅ **2. Historical Trades with Pagination**
- **Endpoint**: `GET /fapi/v1/historicalTrades` or `/fapi/v3/historicalTrades`
- **Parameters**: `symbol`, `fromId`, `limit` (max 1000)
- **Method**: Paginate backwards using `fromId` to find earliest trade
- **Advantage**: Direct evidence of when trading started
- **Limitation**: May require many API calls, rate limits apply
- **Status**: ✅ **IMPLEMENTABLE**

#### ⚠️ **3. Recent Trades Endpoint** (Limited)
- **Endpoint**: `GET /fapi/v1/trades`
- **Limitation**: Only last 1000 trades
- **Status**: ✅ **IMPLEMENTABLE** (but limited accuracy)

### Hyperliquid

#### ✅ **1. S3 Archive Metadata** (BEST OPTION)
- **Bucket**: `s3://hyperliquid-archive`
- **Structure**: `market_data/YYYYMMDD/HH/l2Book/COIN.lz4`
- **Method**: List S3 objects to find earliest date for each coin
- **Advantage**: Direct metadata about when data was archived
- **Requirement**: AWS credentials (may incur small costs)
- **Status**: ✅ **IMPLEMENTABLE**

#### ✅ **2. Historical Candle Testing**
- **Endpoint**: `POST /info` with `type: "candleSnapshot"`
- **Method**: Binary search backwards to find earliest available data
- **Advantage**: Free, no API keys needed
- **Limitation**: Slow, API may have limits
- **Status**: ✅ **IMPLEMENTABLE** (already partially implemented)

#### ✅ **3. Historical Funding Rates**
- **Endpoint**: `POST /info` with `type: "historicalFundingRates"`
- **Method**: Query earliest funding rate for each asset
- **Status**: ✅ **IMPLEMENTABLE**

## Implementation Priority

### Phase 1: Quick Wins (This Week)
1. **Aster Funding Rate Method** ⭐ **HIGHEST PRIORITY**
   - Implement `get_earliest_funding_rate_aster()`
   - Use earliest funding rate as `available_from_datetime`
   - Fast, accurate, free

2. **Hyperliquid S3 Check**
   - Implement S3 bucket listing
   - Find earliest archived data per coin
   - Use as `available_from_datetime`

### Phase 2: Enhanced Methods (Next Sprint)
3. **Historical Trades Pagination (Aster)**
   - Implement pagination to find earliest trade
   - More accurate than funding rates but slower

4. **Historical Candle Testing (Hyperliquid)**
   - Optimize binary search algorithm
   - Cache results to avoid repeated queries

### Phase 3: Third-Party Integration (Future)
5. **Amberdata Free Tier**
   - Check if free tier includes historical metadata
   - Integrate if available

6. **Allium Free Tier**
   - Check if free tier includes historical metadata
   - Integrate if available

## Recommended Implementation Strategy

### For Aster:
```python
# Priority order:
1. Try funding rate history (fastest, most reliable)
2. Try historical trades pagination (more accurate but slower)
3. Fallback to conservative estimate (2024-01-01)
```

### For Hyperliquid:
```python
# Priority order:
1. Try S3 archive metadata (most accurate)
2. Try historical candle testing (free but slower)
3. Fallback to launch date (2024-01-01)
```

## Code Integration

See `instruments_service/app/venues/defi/historical_metadata_helpers.py` for helper functions.

## Expected Results

- **Aster**: Can determine `available_from_datetime` within ~1-2 days of actual listing
- **Hyperliquid**: Can determine `available_from_datetime` within ~1 day of actual listing
- **Fallback**: Both use 2024-01-01 if methods fail (ensures we capture current instruments)

## Next Steps

1. ✅ Implement Aster funding rate method (highest ROI)
2. ✅ Implement Hyperliquid S3 check
3. ⏳ Test with sample instruments
4. ⏳ Integrate into adapters
5. ⏳ Add caching to avoid repeated API calls
