# Databento & DeFi Integration Guide

**Date**: 2025-01-15  
**Status**: ✅ Complete  
**Version**: 1.0

---

## Overview

The instruments-service now supports fetching instruments from multiple data sources:

1. **Tardis** (existing) - Crypto exchanges (Binance, Bybit, OKX, Deribit)
2. **Databento** (new) - TradFi exchanges (CME, NASDAQ, NYSE, ICE)
3. **The Graph** (new) - DeFi DEX pools (Uniswap V3, Curve)
4. **Protocol SDKs** (new) - DeFi protocols (AAVE V3, EtherFi, Lido)

---

## Architecture

### Venue Adapters

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
    ├── uniswapv3_adapter.py     # Uniswap V3 pools
    ├── curve_adapter.py          # Curve pools
    ├── aave_adapter.py           # AAVE V3 markets
    └── lst_adapters.py           # EtherFi & Lido LST tokens
```

**Architecture**: All venue adapters follow the same pattern:
- Abstract API communication and caching
- Use Secret Manager for API keys
- Return standardized instrument data format
- Handle date filtering and availability windows

### Integration

All adapters are integrated into `InstrumentProcessingService`:

```python
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

service = InstrumentProcessingService(config={'project_id': 'central-element-323112'})

# Fetch Databento instruments
databento_instruments = service.fetch_databento_instruments(
    exchange='CME',
    symbols=['ES', 'NQ'],
    target_date=datetime.now()
)

# Fetch DeFi instruments
uniswap_instruments = service.fetch_defi_instruments(
    protocol='uniswap_v3',
    chain='ETHEREUM',
    base_currency='ETH'
)
```

---

## API Key Configuration

### Secret Manager Setup

All API keys are stored in **GCP Secret Manager** for security. The service automatically retrieves keys using secret names configured in `.env`.

**Never commit actual API keys to `.env` files!**

### Required Secrets

Add these secrets to GCP Secret Manager:

```bash
# Databento API key
gcloud secrets create databento-api-key --data-file=-

# AaveScan API key (optional, for AAVE V3 adapter)
gcloud secrets create aavescan-api-key --data-file=-

# Alchemy API key (optional, for The Graph queries)
gcloud secrets create alchemy-api-key --data-file=-
```

### Environment Variables

Configure secret names in `.env`:

```bash
# Secret Manager secret names (keys stored in GCP Secret Manager)
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
```

The service will:
1. Try Secret Manager first (using secret name from env var)
2. Fall back to environment variable (e.g., `DATABENTO_API_KEY`) if Secret Manager fails
3. Raise error if neither is available

---

## Databento Integration

### Supported Exchanges

- **CME** (Chicago Mercantile Exchange) - Futures, commodities
- **NASDAQ** - Equities
- **NYSE** - Equities
- **ICE** - Intercontinental Exchange

### Usage

```python
from instruments_service.app.venues.databento import DatabentoAdapter

adapter = DatabentoAdapter()  # Uses Secret Manager automatically

instruments = adapter.fetch_instrument_definitions(
    exchange='CME',
    symbols=['ES', 'NQ', 'QQQ'],
    date=datetime.now()
)
```

### Instrument Format

Databento instruments follow canonical format:
- **Futures**: `CME:FUTURE:ES-USD-250125`
- **Equities**: `NASDAQ:EQUITY:AAPL-USD`

---

## DeFi Integration

### Uniswap V3

Fetches DEX pools from The Graph subgraph.

**Instrument Format**:
- **Pools**: `UNISWAPV3-ETH:POOL:ETH-USDC:3000@ETHEREUM`
- **Spot Pairs**: `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`

**Usage**:
```python
from instruments_service.app.venues.defi import UniswapV3Adapter

adapter = UniswapV3Adapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH', min_liquidity=100000)
```

### Curve

Fetches Curve pools from The Graph subgraph.

**Instrument Format**:
- **Pools**: `CURVE-ETH:POOL:ETH-USDT@ETHEREUM`
- **Spot Pairs**: `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM`

**Usage**:
```python
from instruments_service.app.venues.defi import CurveAdapter

adapter = CurveAdapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH')
```

### AAVE V3

Fetches lending/borrowing market instruments via AaveScan API.

**Instrument Format**:
- **aTokens**: `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
- **Debt Tokens**: `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

**Usage**:
```python
from instruments_service.app.venues.defi import AaveV3Adapter

adapter = AaveV3Adapter(chain='ETHEREUM')
markets = adapter.fetch_markets()
```

### EtherFi & Lido

Fetches Liquid Staking Token (LST) instruments.

**Instrument Format**:
- **EtherFi**: `ETHERFI:LST:WEETH@ETHEREUM`
- **Lido**: `LIDO:LST:STETH@ETHEREUM`, `LIDO:LST:WSTETH@ETHEREUM`

**Usage**:
```python
from instruments_service.app.venues.defi import EtherFiAdapter, LidoAdapter

etherfi_adapter = EtherFiAdapter(chain='ETHEREUM')
etherfi_instruments = etherfi_adapter.fetch_lst_instruments()

lido_adapter = LidoAdapter(chain='ETHEREUM')
lido_instruments = lido_adapter.fetch_lst_instruments()
```

---

## Schema Updates

### DeFi-Specific Fields

The `InstrumentDefinition` model now includes DeFi-specific fields:

- `base_asset_contract_address`: ERC-20 contract address for base asset
- `quote_asset_contract_address`: ERC-20 contract address for quote asset
- `pool_address`: Pool contract address (for DEX pairs)
- `pool_fee_tier`: Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%)

These fields are optional and only populated for DeFi instruments.

### Updated Enums

**Venue Enum** (new venues):
- `CME`, `NASDAQ`, `NYSE`, `ICE` (TradFi)
- `UNISWAPV3_ETH`, `CURVE_ETH` (DeFi DEX)
- `AAVE_V3_ETH` (DeFi protocols)

**InstrumentType Enum** (new types):
- `POOL` (DEX liquidity pools)
- `EQUITY`, `COMMODITY`, `CURRENCY` (TradFi)

---

## Dependencies

Add to `requirements.txt`:

```
databento>=0.20.0  # TradFi instruments
requests>=2.28.0    # Already included, used for The Graph queries
```

Install:
```bash
pip install -r requirements.txt
```

---

## Testing

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

---

## Migration from Archive Code

The new adapters replace functionality from:
- `archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py`
- `archive/basis-strategy-v1/backend/src/basis_strategy_v1/core/models/instruments.py`

**Key Differences**:
- Uses Secret Manager for API keys (not env files)
- Integrated into `InstrumentProcessingService` (not standalone scripts)
- Follows canonical `InstrumentDefinition` schema
- Uses `unified-cloud-services` for cloud operations

---

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

- **Databento**: Check your subscription limits
- **The Graph**: Free tier has rate limits, consider upgrading
- **AaveScan**: Free tier available, rate limits apply

---

## References

- **Migration Plan**: `docs/DATABENTO_DEFI_MIGRATION_PLAN.md`
- **DeFi Instruments Spec**: `docs/MVP_DEFI_INSTRUMENTS.md`
- **Archive Code**: `archive/genConfig/`, `archive/basis-strategy-v1/`
- **API Documentation**:
  - Databento: https://docs.databento.com
  - The Graph: https://thegraph.com/docs
  - AaveScan: https://docs.aave.com

