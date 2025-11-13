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

## TradFi MVP Instruments

### CME Futures (via Databento GLBX.MDP3)

#### Equity Index Futures (5)
- **ES** (E-mini S&P 500) - `CME:FUTURE:SP500-USD-{expiry}@LIN`
- **NQ** (E-mini NASDAQ-100) - `CME:FUTURE:NASDAQ100-USD-{expiry}@LIN`
- **RTY** (E-mini Russell 2000) - `CME:FUTURE:RUSSELL2000-USD-{expiry}@LIN`
- **YM** (E-mini Dow Jones) - `CME:FUTURE:DOW-USD-{expiry}@LIN`
- **NKD** (Nikkei 225 Dollar) - `CME:FUTURE:NIKKEI225-USD-{expiry}@LIN`

#### Sector Futures (8) - SPDR Sector ETF Futures
- **XAF** (Energy Select Sector) - `CME:FUTURE:ENERGY_SECTOR-USD-{expiry}@LIN`
- **XAK** (Technology Select Sector) - `CME:FUTURE:TECH_SECTOR-USD-{expiry}@LIN`
- **XAY** (Consumer Discretionary) - `CME:FUTURE:CONSUMER_DISC_SECTOR-USD-{expiry}@LIN`
- **XAP** (Consumer Staples) - `CME:FUTURE:CONSUMER_STAPLES_SECTOR-USD-{expiry}@LIN`
- **XAV** (Health Care) - `CME:FUTURE:HEALTHCARE_SECTOR-USD-{expiry}@LIN`
- **XAI** (Industrials) - `CME:FUTURE:INDUSTRIALS_SECTOR-USD-{expiry}@LIN`
- **XAB** (Materials) - `CME:FUTURE:MATERIALS_SECTOR-USD-{expiry}@LIN`
- **XAU** (Utilities) - `CME:FUTURE:UTILITIES_SECTOR-USD-{expiry}@LIN`

#### Treasury Futures (4) - CBOT via CME
- **ZT** (2-Year T-Note) - `CME:FUTURE:TREASURY_2Y-USD-{expiry}@LIN`
- **ZF** (5-Year T-Note) - `CME:FUTURE:TREASURY_5Y-USD-{expiry}@LIN`
- **ZN** (10-Year T-Note) - `CME:FUTURE:TREASURY_10Y-USD-{expiry}@LIN`
- **ZB** (30-Year T-Bond) - `CME:FUTURE:TREASURY_30Y-USD-{expiry}@LIN`

#### Crypto Futures (2)
- **BTC** (Bitcoin) - `CME:FUTURE:BTC-USD-{expiry}@LIN`
- **ETH** (Ethereum) - `CME:FUTURE:ETH-USD-{expiry}@LIN`

#### Energy Commodities (4)
- **CL** (WTI Crude Oil) - `CME:FUTURE:CRUDE-USD-{expiry}@LIN`
- **NG** (Natural Gas) - `CME:FUTURE:NATGAS-USD-{expiry}@LIN`
- **HO** (Heating Oil) - `CME:FUTURE:HEATING_OIL-USD-{expiry}@LIN`
- **RB** (RBOB Gasoline) - `CME:FUTURE:GASOLINE-USD-{expiry}@LIN`

#### Metals (3)
- **GC** (Gold) - `CME:FUTURE:GOLD-USD-{expiry}@LIN`
- **SI** (Silver) - `CME:FUTURE:SILVER-USD-{expiry}@LIN`
- **HG** (Copper) - `CME:FUTURE:COPPER-USD-{expiry}@LIN`

#### Agricultural Commodities (6)
- **CT** (Cotton) - `CME:FUTURE:COTTON-USD-{expiry}@LIN`
- **ZS** (Soybeans) - `CME:FUTURE:SOYBEANS-USD-{expiry}@LIN`
- **ZC** (Corn) - `CME:FUTURE:CORN-USD-{expiry}@LIN`
- **ZW** (Wheat) - `CME:FUTURE:WHEAT-USD-{expiry}@LIN`
- **ZL** (Soybean Oil) - `CME:FUTURE:SOYBEAN_OIL-USD-{expiry}@LIN`
- **ZM** (Soybean Meal) - `CME:FUTURE:SOYBEAN_MEAL-USD-{expiry}@LIN`

#### FX Futures (10)
- **6E** (Euro) - `CME:FUTURE:EUR-USD-{expiry}@LIN`
- **6B** (British Pound) - `CME:FUTURE:GBP-USD-{expiry}@LIN`
- **6J** (Japanese Yen) - `CME:FUTURE:JPY-USD-{expiry}@LIN`
- **6A** (Australian Dollar) - `CME:FUTURE:AUD-USD-{expiry}@LIN`
- **6C** (Canadian Dollar) - `CME:FUTURE:CAD-USD-{expiry}@LIN`
- **6N** (New Zealand Dollar) - `CME:FUTURE:NZD-USD-{expiry}@LIN`
- **6S** (Swiss Franc) - `CME:FUTURE:CHF-USD-{expiry}@LIN`
- **6M** (Mexican Peso) - `CME:FUTURE:MXN-USD-{expiry}@LIN`
- **6Z** (South African Rand) - `CME:FUTURE:ZAR-USD-{expiry}@LIN`
- **6L** (Brazilian Real) - `CME:FUTURE:BRL-USD-{expiry}@LIN`

### CME Options (via Databento)

#### Equity Index Options - E-mini S&P 500
- **ES.OPT** (Standard Monthly/Quarterly) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`
- **EW1.OPT** (1st Week Friday) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`
- **EW2.OPT** (2nd Week Friday) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`
- **EW3.OPT** (3rd Week Friday) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`
- **EW4.OPT** (4th Week Friday) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`
- **EW5.OPT** (5th Week Friday) - `CME:OPTION:SP500-USD-{expiry}-{strike}-{CALL|PUT}@LIN`

### CBOE Instruments

#### Index (via Barchart)
- **VIX** (CBOE Volatility Index) - `CBOE:INDEX:VIX-USD`
  - **Data Provider**: Barchart (OHLCV 15-minute data)
  - **Trading Hours**: 9:30 AM - 4:15 PM ET
  - **Note**: VIX is an index, not a futures/options contract

### Trading Hours

**CME Futures & Options**: Nearly 24-hour trading
- **Hours**: Sunday 5:00 PM CT to Friday 4:00 PM CT
- **Maintenance Break**: Daily 4:00-5:00 PM CT (1 hour)
- **Total**: 23 hours per day, 5 days per week
- **Advantage**: Global market coverage, react to overnight news

**CBOE VIX**: Regular trading hours
- **Hours**: 9:30 AM - 4:15 PM ET (weekdays only)
- **Data Source**: Barchart (15-minute OHLCV data)

### Instrument Count

- **CME Futures**: 42 instruments
  - Equity Indices: 5
  - Sector Futures: 8
  - Treasuries: 4
  - Crypto: 2
  - Energy: 4
  - Metals: 3
  - Agriculture: 6
  - FX: 10
- **CME Options**: 6 parent symbols (ES.OPT + EW1-5.OPT) → thousands of individual option contracts
- **CBOE**: 1 index (VIX)
- **Total TradFi**: 49 instruments (excluding individual option contracts)

**Status**: ✅ Implemented (Databento GLBX.MDP3 + Barchart integration)

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

**Crypto Instruments** (1-minute OHLCV):
- **Per Instrument**: ~1,440 candles per day
- **Daily Total**: ~181,440 candles (126 crypto instruments × 1,440)
- **Monthly Storage**: ~5.4M candles (~2-3 GB BigQuery storage)

**TradFi Instruments**:
- **CME Futures** (1-minute OHLCV via Databento): ~1,440 candles per day per contract
  - 42 futures × multiple expiries × 1,440 candles = ~60,480+ candles per day
- **CME Options** (1-minute OHLCV via Databento): ~1,440 candles per day per option
  - 6 parent symbols → thousands of option contracts × 1,440 candles
- **CBOE VIX** (15-minute OHLCV via Barchart): ~96 candles per day
- **Total TradFi Daily**: ~60,000-100,000+ candles (depending on active futures/options contracts)
- **Combined Daily Total**: ~241,000-281,000+ candles (crypto + TradFi)

### Expansion Strategy
As MVP proves successful, expand incrementally:
1. ✅ **CME Futures**: Equity indices (ES, NQ, RTY, YM, NKD), Treasuries (ZT, ZF, ZN, ZB), Sectors (XAF-XAU)
2. ✅ **CME Options**: ES options (monthly, quarterly, weekly)
3. ✅ **CBOE VIX**: Added volatility index via Barchart (15-minute OHLCV data)
4. Add more crypto base assets (top 50 by market cap)
5. Add options on other indices (NQ.OPT, RTY.OPT)
6. Add CME micro futures (MES, MNQ, M2K, MYM) for smaller position sizes
7. Add end-of-month options (EOM) for ES

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

# Generate TradFi instruments (CME + VIX)
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --tradfi

# Generate CME instruments only
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --venues CME
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

# Query CME crypto futures
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues CME \
    --instrument-types FUTURE \
    --base-assets BTC ETH

# Query CBOE VIX index
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues CBOE \
    --instrument-types INDEX

# Query CME energy commodities
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues CME \
    --base-assets CRUDE NATGAS HEATING_OIL GASOLINE

# Query CME sector futures
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues CME \
    --base-assets ENERGY_SECTOR TECH_SECTOR HEALTHCARE_SECTOR

# Query ES weekly options
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --venues CME \
    --instrument-types OPTION
```

## Related Documentation

- [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Complete instrument ID specification
- [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and instruments
- [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter details
