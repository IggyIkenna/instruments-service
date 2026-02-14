# Venue Adapters

> **Related Documentation**:
>
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture
> - [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and data sources
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID specification

---

## Overview

The instruments-service uses a **venue adapter pattern** for all data source integrations. This provides:

- **Consistent Architecture**: All data sources follow the same pattern
- **Separation of Concerns**: API communication isolated from processing logic
- **Easy Testing**: Adapters can be tested independently
- **Maintainability**: Changes to data sources are localized
- **Extensibility**: Simple to add new data sources

## Architecture

### Venue Adapters Structure

All venue adapters are located in `instruments_service/app/venues/`:

```
venues/
├── tardis/
│   ├── __init__.py
│   └── tardis_adapter.py        # Crypto exchanges (Binance, Bybit, OKX, Deribit)
├── databento/
│   ├── __init__.py
│   └── databento_adapter.py     # TradFi instruments (CME, NASDAQ, NYSE)
└── defi/
    ├── __init__.py
    ├── the_graph_client.py      # GraphQL client for The Graph
    ├── envio_client.py           # Envio HyperSync client
    ├── uniswapv2_adapter.py     # Uniswap V2 pools
    ├── uniswapv3_adapter.py     # Uniswap V3 pools
    ├── uniswapv4_adapter.py     # Uniswap V4 pools
    ├── curve_adapter.py          # Curve pools
    ├── curve_rpc_adapter.py     # Curve RPC fallback
    ├── balancer_adapter.py       # Balancer pools
    ├── aave_adapter.py           # AAVE V3 markets
    ├── morpho_adapter.py         # Morpho markets
    └── lst_adapters.py           # EtherFi & Lido LST tokens
```

### Adapter Pattern

All adapters follow the same pattern:

1. **Initialization**: Use Secret Manager for API keys
2. **Caching**: Handle API response caching (TTL-based)
3. **Date Filtering**: Filter instruments by availability windows
4. **Data Transformation**: Convert API responses to standardized format
5. **Error Handling**: Graceful error handling with logging

### Integration

`InstrumentProcessingService` coordinates all adapters:

```python
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

service = InstrumentProcessingService(config={'project_id': '{project_id}'})  # Replace {project_id} with actual project ID

# TardisAdapter is used automatically for crypto exchanges
instruments = await service.process_exchange_instruments(
    exchange='binance-futures',
    target_date=datetime.now()
)

# DatabentoAdapter for TradFi
databento_instruments = service.fetch_databento_instruments(
    exchange='CME',
    symbols=['ES', 'NQ'],
    target_date=datetime.now()
)

# DeFi adapters
defi_instruments = service.fetch_defi_instruments(
    protocol='uniswap_v3',
    chain='ETHEREUM',
    base_currency='ETH'
)
```

## Supported Venues

### Crypto Exchanges (Tardis)

**Adapter**: `TardisAdapter` (`venues/tardis/tardis_adapter.py`)

**Supported Exchanges**:

- **Binance** (`BINANCE-SPOT`, `BINANCE-FUTURES`)
- **Bybit** (`BYBIT`)
- **OKX** (`OKX`)
- **Deribit** (`DERIBIT`)
- **Upbit** (`UPBIT`) - Korean exchange, spot only (KRW quote) - for kimchi premium
- **Coinbase** (`COINBASE`) - Spot only (USD quote) - for coinbase premium

**Features**:

- HTTP session management with retries
- TTL-based caching (1 hour)
- Date availability filtering
- Secret Manager integration
- MVP base asset filtering for Upbit/Coinbase (21 coins only)

**Usage**:

```python
from instruments_service.app.venues.tardis import TardisAdapter

adapter = TardisAdapter()  # Uses Secret Manager automatically
symbols, filtered_count = adapter.fetch_exchange_instruments(
    exchange='binance-futures',
    target_date=datetime.now()
)
```

**Instrument Types**:

- `SPOT_PAIR` (spot trading pairs)
- `PERPETUAL` (perpetual futures)
- `FUTURE` (dated futures)
- `OPTION` (options contracts)

**Premium Calculation Venues** (spot only, MVP coins):

- **Upbit** (`UPBIT`): Korean Won (KRW) quotes - for kimchi premium calculations
- **Coinbase** (`COINBASE`): US Dollar (USD) quotes - for coinbase premium calculations

These venues are filtered to only include the 21 MVP base assets (BTC, ETH, SOL, etc.)

### TradFi Exchanges (Databento)

**Adapter**: `DatabentoAdapter` (`venues/databento/databento_adapter.py`)

**Supported Exchanges**:

- **CME** (Chicago Mercantile Exchange) - Futures, options, commodities (GLBX.MDP3)
- **NASDAQ** - Equities, ETFs including Bitcoin ETFs (DBEQ.BASIC)
- **NYSE** - S&P 500 equities (DBEQ.BASIC)
- **CBOE** - VIX index (static definition)
- **FX** - KRW/USD forex pair (static definition, data via Yahoo Finance)
- **ICE** - Intercontinental Exchange (IFEU.IMPACT)

**S&P 500 Historical Universe (2020-2025)**:

- **Period**: 2020-2025 (all stocks that appeared in S&P 500 during this time)
- **Includes Removed**: Yes - stocks removed from index since 2020 are included for basket/historical analysis
- **Total Tickers**: ~603 (NASDAQ: ~102, NYSE: ~501)
- **Future Enhancements**: Can add weights, adjust for dividends/corporate actions later

**Features**:

- Exchange-to-dataset mapping
- Weekend date adjustment
- Symbol filtering by `security_type`:
  - CME: `FUT` (Future), `OOF` (Options on Futures), `STK` (Stock), `ETF` (ETF)
  - DBEQ: `E` (Equity/ETF), `C` (Common Stock), `O` (Ordinary shares), `""` (Class B shares like BRK.B)
- Holiday detection via `exchange_calendars` library
- DST-aware UTC trading hours conversion
- Secret Manager integration for API keys

**Static Instrument Definitions**:

- `create_vix_instrument_definition()` - CBOE VIX index
- `create_krwusd_instrument_definition()` - Yahoo Finance KRW/USD
- `create_bitcoin_etf_instrument_definition()` - NASDAQ Bitcoin ETFs (IBIT, FBTC, ARKB)

**Usage**:

```python
from instruments_service.app.venues.databento import DatabentoAdapter

adapter = DatabentoAdapter()  # Uses Secret Manager automatically

# Fetch CME futures
instruments = adapter.fetch_instrument_definitions(
    exchange='CME',
    symbols=['ES.FUT', 'NQ.FUT'],
    date=datetime.now()
)

# Create Bitcoin ETF (static definition)
etf_def = adapter.create_bitcoin_etf_instrument_definition('IBIT', datetime.now())

# Check if date is a US market holiday
is_holiday, holiday_name = adapter.is_us_market_holiday(date(2025, 1, 1))
# Returns: (True, "New Year's Day")
```

**Instrument Format**:

- **Futures**: `CME:FUTURE:SP500-USD-250321@LIN`
- **Options**: `CME:OPTION:SP500-USD-250321-5000-CALL@LIN`
- **Equities**: `NASDAQ:EQUITY:AAPL-USD`
- **Bitcoin ETFs**: `NASDAQ:ETF:IBIT-USD`
- **VIX Index**: `CBOE:INDEX:VIX-USD`
- **Forex**: `FX:SPOT_PAIR:KRW-USD`

**Translation Logic**:

- Maps Databento `security_type` to canonical `InstrumentType`:
  - CME: `FUT` → FUTURE, `OOF` → OPTION
  - DBEQ: `E` → EQUITY, `C` → EQUITY, `O` → EQUITY, `""` → EQUITY (Class B shares)
- Equity symbols (E, C, O, "") keep original ticker names (CL=Colgate, ES=Eversource, MSI=Motorola)
- CME futures codes (ES, CL, etc.) are converted to human-readable names (SP500, CRUDE, etc.)
- Filters by `security_type` (excludes spreads, settlement-only)
- Handles contract size classification (normal/mini/micro)
- US market holiday detection via `exchange_calendars`

### DeFi DEX Protocols

#### Uniswap V2/V3/V4

**Adapters**: `UniswapV2Adapter`, `UniswapV3Adapter`, `UniswapV4Adapter`

**Data Sources**:

- **The Graph**: Primary source for V2/V3 (subgraph queries)
- **Envio**: Fallback for V4 (HyperSync API)
- **RPC**: Future option (requires event tracking)

**Uniswap V4 Fallback Order**:

1. The Graph Network gateway (if subgraph ID available)
2. Envio indexer (primary fallback)
3. RPC queries (skipped for MVP)

**Usage**:

```python
from instruments_service.app.venues.defi import UniswapV3Adapter

adapter = UniswapV3Adapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH', min_liquidity=100000)
```

**Instrument Format**:

- **Pools**: `UNISWAPV3-ETH:POOL:ETH-USDC:3000@ETHEREUM`
- **Spot Pairs**: `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`

**Fee Tiers**:

- V2: 3000 bps (0.3%) implied
- V3: 100, 500, 3000, 10000 bps (0.01%, 0.05%, 0.3%, 1%)

#### Curve

**Adapter**: `CurveAdapter` (`venues/defi/curve_adapter.py`)

**Data Sources**:

- **The Graph**: Primary source (if subgraph available)
- **RPC**: Fallback via Curve Registry contract (`0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d9f5`)

**Curve Fallback Order**:

1. The Graph Network gateway (if subgraph ID available)
2. RPC direct contract queries (primary fallback)

**Usage**:

```python
from instruments_service.app.venues.defi import CurveAdapter

adapter = CurveAdapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH')
```

**Instrument Format**:

- **Pools**: `CURVE-ETH:POOL:ETH-USDT@ETHEREUM`
- **Spot Pairs**: `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM`

**Note**: Curve subgraph is deprecated. RPC adapter uses Curve Registry contract for pool discovery.

#### Balancer

**Adapter**: `BalancerAdapter` (`venues/defi/balancer_adapter.py`)

**Data Source**: Balancer API v3

**Usage**:

```python
from instruments_service.app.venues.defi import BalancerAdapter

adapter = BalancerAdapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH')
```

**Instrument Format**:

- **Pools**: `BALANCER-ETH:POOL:ETH-USDC@ETHEREUM`

### DeFi Lending Protocols

#### AAVE V3

**Adapter**: `AaveV3Adapter` (`venues/defi/aave_adapter.py`)

**Data Sources**:

- **The Graph**: AAVE V3 subgraph (primary)
- **AaveScan API**: Fallback

**Usage**:

```python
from instruments_service.app.venues.defi import AaveV3Adapter

adapter = AaveV3Adapter(chain='ETHEREUM')
markets = adapter.fetch_markets()
```

**Instrument Format**:

- **aTokens**: `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
- **Debt Tokens**: `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

**Metadata Includes**:

- Risk parameters (LTV, liquidation threshold, liquidation bonus)
- Interest rate model parameters (optimal utilization, slopes, base rate)
- Reserve factor
- eMode information (category, underlying, eMode LTV/thresholds)

#### Morpho

**Adapter**: `MorphoAdapter` (`venues/defi/morpho_adapter.py`)

**Data Sources**:

- Morpho API: `https://api.morpho.org/graphql`
- Morpho Subgraph (fallback)

**Usage**:

```python
from instruments_service.app.venues.defi import MorphoAdapter

adapter = MorphoAdapter(chain='ETHEREUM')
markets = adapter.fetch_markets()
```

**Instrument Format**:

- **Supply tokens**: `MORPHO-ETHEREUM:SUPPLY_TOKEN:SUPPLYUSDC@ETHEREUM`
- **Debt tokens**: `MORPHO-ETHEREUM:DEBT_TOKEN:DEBTUSDC@ETHEREUM`

### DeFi Staking Protocols

#### EtherFi

**Adapter**: `EtherFiAdapter` (`venues/defi/lst_adapters.py`)

**Data Sources**:

- Alchemy SDK: Token metadata
- On-chain calls: Contract addresses and exchange rates

**Usage**:

```python
from instruments_service.app.venues.defi import EtherFiAdapter

adapter = EtherFiAdapter(chain='ETHEREUM')
instruments = adapter.fetch_lst_instruments()
```

**Instrument Format**:

- **EtherFi**: `ETHERFI:LST:WEETH@ETHEREUM`

**Contract Address**: `0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee`

#### Lido

**Adapter**: `LidoAdapter` (`venues/defi/lst_adapters.py`)

**Data Sources**:

- Alchemy SDK: Token metadata
- On-chain calls: Contract addresses and exchange rates

**Usage**:

```python
from instruments_service.app.venues.defi import LidoAdapter

adapter = LidoAdapter(chain='ETHEREUM')
instruments = adapter.fetch_lst_instruments()
```

**Instrument Format**:

- **Lido**: `LIDO:LST:STETH@ETHEREUM`, `LIDO:LST:WSTETH@ETHEREUM`

**Contract Addresses**:

- stETH: `0xae7ab96520de3a18e5e111b5eaab095312d7fe84`
- wstETH: `0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0`

## Secret Manager Integration

All adapters use Secret Manager for API keys:

1. **Try Secret Manager first** (using secret name from env var)
2. **Fall back to environment variable** if Secret Manager fails
3. **Raise error** if neither is available

### Required Secrets

Add these secrets to GCP Secret Manager:

```bash
# Tardis API key
gcloud secrets create tardis-api-key --data-file=-

# Databento API key
gcloud secrets create databento-api-key --data-file=-

# The Graph API key
gcloud secrets create thegraph-api-key --data-file=-

# Alchemy API key
gcloud secrets create alchemy-api-key --data-file=-

# Envio API key
gcloud secrets create envio-api-key --data-file=-

# AaveScan API key (optional, for AAVE V3 adapter)
gcloud secrets create aavescan-api-key --data-file=-
```

### Environment Variables

Configure secret names in `.env`:

```bash
# Secret Manager secret names (keys stored in GCP Secret Manager)
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
THEGRAPH_SECRET_NAME=thegraph-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
ENVIO_SECRET_NAME=envio-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key
```

**Never commit actual API keys to `.env` files!**

## Adapter Responsibilities

Each adapter handles:

1. **API Communication**: HTTP/GraphQL requests with retry logic
2. **Caching**: TTL-based response caching
3. **Date Filtering**: Filter instruments by availability windows
4. **Data Transformation**: Convert API responses to standardized format
5. **Secret Manager**: Secure API key retrieval
6. **Error Handling**: Graceful error handling with logging

## Benefits

### Before (Embedded Logic)

- Tardis logic embedded in `InstrumentProcessingService`
- Inconsistent patterns across data sources
- Difficult to test API communication separately
- Hard to maintain and extend

### After (Adapter Pattern)

- ✅ Consistent architecture across all data sources
- ✅ Separation of concerns (API vs processing)
- ✅ Easy to test adapters independently
- ✅ Simple to add new data sources
- ✅ Centralized Secret Manager integration
- ✅ Reusable caching and error handling

## Testing

### Test Tardis Adapter

```python
from instruments_service.app.venues.tardis import TardisAdapter

adapter = TardisAdapter()
symbols, filtered_count = adapter.fetch_exchange_instruments(
    exchange='binance-futures',
    target_date=datetime.now()
)
print(f"Fetched {len(symbols)} instruments")
```

### Test Databento Adapter

```python
from instruments_service.app.venues.databento import DatabentoAdapter

adapter = DatabentoAdapter()
instruments = adapter.fetch_instrument_definitions(
    exchange='CME',
    symbols=['ES'],
    date=datetime.now()
)
print(f"Fetched {len(instruments)} instruments")
```

### Test DeFi Adapters

```python
from instruments_service.app.venues.defi import UniswapV3Adapter

adapter = UniswapV3Adapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH', min_liquidity=100000)
print(f"Fetched {len(pools)} Uniswap V3 pools")
```

## Troubleshooting

### Secret Manager Errors

If you see "Failed to retrieve API key from Secret Manager":

1. Check secret exists: `gcloud secrets list`
2. Verify secret name matches `.env` config
3. Check GCP credentials: `GOOGLE_APPLICATION_CREDENTIALS`
4. Verify project ID: `GCP_PROJECT_ID`

### Import Errors

If adapters fail to import:

1. Install dependencies: `pip install databento`
2. Check Python path includes `instruments_service`
3. Verify `unified-cloud-services` is installed

### API Rate Limits

- **Tardis**: Check your subscription limits
- **Databento**: Check your subscription limits
- **The Graph**: Free tier has rate limits, consider upgrading
- **AaveScan**: Free tier available, rate limits apply
- **Alchemy**: Free tier: 300M compute units/month

## Current Implementation Status

### ✅ Complete

- **TardisAdapter**: Crypto exchanges (Binance, Bybit, OKX, Deribit, Upbit, Coinbase)
- **DatabentoAdapter**: TradFi exchanges (CME, NASDAQ, NYSE)
- **UniswapV2Adapter**: Uniswap V2 pools (The Graph)
- **UniswapV3Adapter**: Uniswap V3 pools (The Graph)
- **UniswapV4Adapter**: Uniswap V4 pools (Envio fallback)
- **CurveAdapter**: Curve pools (RPC fallback)
- **BalancerAdapter**: Balancer pools (Balancer API v3)
- **AaveV3Adapter**: AAVE V3 markets (The Graph + AaveScan)
- **MorphoAdapter**: Morpho markets (Morpho API)
- **EtherFiAdapter**: EtherFi LST tokens
- **LidoAdapter**: Lido LST tokens

### ⏳ Pending Implementation

- **EulerAdapter**: Euler lending (Plasma chain)
- **FluidAdapter**: Fluid lending (Plasma chain)
- **HyperliquidAdapter**: Hyperliquid perpetuals (Hyperliquid chain)
- **AsterAdapter**: Aster perpetuals (Aster exchange)

## Related Documentation

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture and design decisions
- [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and data sources
- [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID specification
