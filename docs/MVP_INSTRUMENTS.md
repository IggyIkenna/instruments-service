# MVP Instruments

> **Related Documentation**:
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID specification
> - [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi instruments guide
> - [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter details

---

## Overview

To manage GCS and BigQuery storage costs during MVP development, we focus on a curated set of high-liquidity instruments across multiple venues and markets.

## Instrument Selection Criteria

- **High Liquidity**: Top cryptocurrencies by market capitalization and trading volume
- **Cross-Exchange Coverage**: Available on multiple exchanges where applicable
- **Market Coverage**: Both perpetual futures and spot markets (for crypto)
- **Cost Optimization**: Limits data storage to essential instruments for MVP phase

## Crypto MVP Instruments (CEX)

### Base Assets (21 instruments)

1. SOL (Solana)
2. BTC (Bitcoin)
3. ETH (Ethereum)
4. AVAX (Avalanche)
5. ADA (Cardano)
6. SUSHI (SushiSwap)
7. CAKE (PancakeSwap)
8. XRP (Ripple)
9. DOGE (Dogecoin)
10. XLM (Stellar)
11. LTC (Litecoin)
12. ALGO (Algorand)
13. FIL (Filecoin)
14. TRX (Tron)
15. BNB (Binance Coin)
16. LINK (Chainlink)
17. MATIC (Polygon)
18. APT (Aptos)
19. VET (VeChain)
20. ATOM (Cosmos)
21. NEAR (Near Protocol)

### Exchanges

- **Binance** (`BINANCE-SPOT`, `BINANCE-FUTURES`)
- **OKX** (`OKX`)
- **Bybit** (`BYBIT`)

### Instrument Count

- **Perpetuals**: 63 instruments (21 assets × 3 exchanges)
- **Spot Pairs**: 63 instruments (21 assets × 3 exchanges)
- **Total**: 126 instruments

### Example Instrument IDs

**Binance Futures Perpetuals**:
- `BINANCE-FUTURES:PERPETUAL:SOL-USDT@LIN`
- `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN`
- `BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN`

**OKX Spot**:
- `OKX:SPOT_PAIR:SOL-USDT`
- `OKX:SPOT_PAIR:BTC-USDT`

**Bybit Spot**:
- `BYBIT:SPOT_PAIR:ETH-USDT`

## DeFi MVP Instruments

### Position Instruments

#### Wallet Positions (SPOT_ASSET)
- `WALLET:SPOT_ASSET:USDT`
- `WALLET:SPOT_ASSET:ETH`
- `WALLET:SPOT_ASSET:EIGEN`
- `WALLET:SPOT_ASSET:ETHFI`

#### AAVE V3 Lending Positions (A_TOKEN)
- `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
- `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM`

#### AAVE V3 Borrowing Positions (DEBT_TOKEN)
- `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

#### EtherFi Staking Positions (LST)
- `ETHERFI:LST:WEETH@ETHEREUM`

#### Lido Staking Positions (LST)
- `LIDO:LST:STETH@ETHEREUM`
- `LIDO:LST:WSTETH@ETHEREUM`

#### DEX Market Pricing (SPOT_ASSET)
- `CURVE-ETH:SPOT_ASSET:WEETH@ETHEREUM`

#### Flash Loan Provider (SPOT_ASSET)
- `MORPHO:SPOT_ASSET:WETH@ETHEREUM`

### Trading Instruments (SPOT_PAIR)

#### DEX Swap Trading Pairs
- `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`
- `CURVE-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`
- `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM`
- `UNISWAPV3-ETH:SPOT_PAIR:ETH-WSTETH@ETHEREUM`

**Total DeFi Instruments**: 16 position instruments + 4 trading instruments = 20 instruments

## TradFi MVP Instruments (Planned)

### Commodities (Micro Futures/ETF Preferred)
- Sugar (micro futures/ETF)
- Coffee (micro futures/ETF)
- Pork Belly (micro futures/ETF)
- Cotton (micro futures/ETF)
- Cocoa (micro futures/ETF)
- Orange Juice (micro futures/ETF)
- Soybeans (micro futures/ETF)
- Crude Oil (micro futures/ETF)
- Natural Gas (micro futures/ETF)
- Gold (micro futures/ETF)

### Currencies (Micro Futures/ETF Preferred)
- G10 currencies (EUR, GBP, JPY, AUD, NZD, CAD, CHF, NOK, SEK, DKK)

### Equities (Micro Futures/ETF Preferred)
- Equity indices (micro futures/ETFs)
- S&P 500 index (SPY ETF, ES micro futures)
- S&P 500 stock components (individual stocks - most liquid micro futures/ETFs per stock)

**Status**: ⏳ Planned (Databento integration Week 7-8)

## Performance Benchmarks

### Compute Time
- **1 day**: ~30-60 seconds (depends on exchange)
- **Batch (730 days)**: ~6-12 hours (with optimizations)

### Memory Usage
- **Per exchange**: ~500MB
- **Batch processing**: Scales linearly with date range

### Throughput
- **Instruments/second**: ~100-200 instruments/second

### Performance Optimizations

**Key Optimizations Implemented**:
1. **Module-Level DeFi Adapter Imports**: Eliminates repeated import overhead
2. **Cached Secret Manager API Keys**: Reduces Secret Manager calls from O(n*dates*protocols) to O(secrets)
3. **Cached DeFi Adapter Instances**: Eliminates expensive adapter initialization for repeated calls
4. **Cached DeFi Instruments**: DeFi instruments fetched once, reused for all dates in batch
5. **Reused CloudDataProvider**: Single instance created before date loop, reused
6. **Static Configuration Caching**: MVP token list and protocol map calculated once, reused

**Estimated Speedup**: ~100-1000x faster for DeFi instrument generation in batch runs

### Date-Specific vs Date-Agnostic Instruments

**Date-Specific (TradFi)**:
- **Tardis exchanges**: Instruments can change daily (new listings, delistings)
- **Databento**: Futures/options have expiry dates, new contracts listed daily
- **Action**: Fetch per-date ✅

**Date-Agnostic (DeFi)**:
- **Uniswap V3/V2/V4 pools**: Current pool state (doesn't change historically)
- **Curve/Balancer pools**: Current pool state
- **AAVE markets**: Current reserve state
- **LST tokens (EtherFi, Lido)**: Current token state
- **Action**: Fetch once, cache, reuse ✅

## Cost Management Benefits

### Storage Impact
- **Reduced Scope**: MVP instruments vs. full universe (4,374+ instruments)
- **Focused Coverage**: High-liquidity pairs only
- **Cross-Exchange Analysis**: Multiple exchanges × assets × markets

### Data Volume Estimates
- **Per Instrument**: ~1,440 1-minute candles per day
- **Daily Total**: ~181,440 candles (126 crypto instruments × 1,440)
- **Monthly Storage**: ~5.4M candles (~2-3 GB BigQuery storage)

### Expansion Strategy
As MVP proves successful, expand incrementally:
1. Add more exchanges (Deribit options, CME futures)
2. Add more base assets (top 50 by market cap)
3. Add more quote currencies (USDC, BTC, ETH pairs)
4. Add options chains (Deribit, OKX options)

## Usage Examples

### Generate MVP Instruments

```bash
# Generate all MVP crypto instruments
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --exchanges binance-futures binance-spot okx bybit

# Generate DeFi instruments
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --defi
```

### Query MVP Instruments

```bash
# Query MVP perpetuals
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues BINANCE-FUTURES OKX BYBIT \
    --instrument-types PERPETUAL

# Query DeFi instruments
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues AAVE_V3_ETH ETHERFI LIDO UNISWAPV3-ETH
```

## Related Documentation

- [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Complete instrument ID specification
- [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and instruments
- [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter details


