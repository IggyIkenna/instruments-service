# Venue Adapter Architecture

**Date**: 2025-01-15  
**Status**: ✅ Complete  
**Version**: 2.0

---

## Overview

The instruments-service uses a **venue adapter pattern** for all data source integrations. This provides:

- **Consistent Architecture**: All data sources follow the same pattern
- **Separation of Concerns**: API communication isolated from processing logic
- **Easy Testing**: Adapters can be tested independently
- **Maintainability**: Changes to data sources are localized

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

service = InstrumentProcessingService(config={'project_id': 'central-element-323112'})

# TardisAdapter is used automatically for crypto exchanges
instruments = await service.process_exchange_instruments(
    exchange='binance-futures',
    target_date=datetime.now()
)

# DatabentoAdapter for TradFi
databento_instruments = service.fetch_databento_instruments(
    exchange='CME',
    symbols=['ES', 'NQ']
)

# DeFi adapters
defi_instruments = service.fetch_defi_instruments(
    protocol='uniswap_v3',
    chain='ETHEREUM'
)
```

---

## TardisAdapter

**Location**: `venues/tardis/tardis_adapter.py`

**Purpose**: Fetches crypto exchange instruments from Tardis API

**Features**:
- HTTP session management with retries
- TTL-based caching (1 hour)
- Date availability filtering
- Secret Manager integration

**Usage**:
```python
from instruments_service.app.venues.tardis import TardisAdapter

adapter = TardisAdapter()  # Uses Secret Manager automatically
symbols, filtered_count = adapter.fetch_exchange_instruments(
    exchange='binance-futures',
    target_date=datetime.now()
)
```

---

## DatabentoAdapter

**Location**: `venues/databento/databento_adapter.py`

**Purpose**: Fetches TradFi instruments from Databento API

**Features**:
- Exchange-to-dataset mapping
- Weekend date adjustment
- Symbol filtering
- Secret Manager integration

**Usage**:
```python
from instruments_service.app.venues.databento import DatabentoAdapter

adapter = DatabentoAdapter()  # Uses Secret Manager automatically
instruments = adapter.fetch_instrument_definitions(
    exchange='CME',
    symbols=['ES', 'NQ'],
    date=datetime.now()
)
```

---

## DeFi Adapters

**Location**: `venues/defi/`

### UniswapV3Adapter

Fetches Uniswap V3 pools from The Graph subgraph.

**Usage**:
```python
from instruments_service.app.venues.defi import UniswapV3Adapter

adapter = UniswapV3Adapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH', min_liquidity=100000)
```

### CurveAdapter

Fetches Curve pools from The Graph subgraph.

**Usage**:
```python
from instruments_service.app.venues.defi import CurveAdapter

adapter = CurveAdapter(chain='ETHEREUM')
pools = adapter.fetch_pools(base_currency='ETH')
```

### AaveV3Adapter

Fetches AAVE V3 markets via AaveScan API.

**Usage**:
```python
from instruments_service.app.venues.defi import AaveV3Adapter

adapter = AaveV3Adapter(chain='ETHEREUM')
markets = adapter.fetch_markets()
```

### EtherFiAdapter & LidoAdapter

Fetches Liquid Staking Token (LST) instruments.

**Usage**:
```python
from instruments_service.app.venues.defi import EtherFiAdapter, LidoAdapter

etherfi_adapter = EtherFiAdapter(chain='ETHEREUM')
etherfi_instruments = etherfi_adapter.fetch_lst_instruments()

lido_adapter = LidoAdapter(chain='ETHEREUM')
lido_instruments = lido_adapter.fetch_lst_instruments()
```

---

## Secret Manager Integration

All adapters use Secret Manager for API keys:

1. **Try Secret Manager first** (using secret name from env var)
2. **Fall back to environment variable** if Secret Manager fails
3. **Raise error** if neither is available

**Environment Variables** (secret names, not actual keys):
```bash
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
```

**Never commit actual API keys to `.env` files!**

---

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

---

## Migration Notes

**Removed**:
- Embedded Tardis HTTP session management
- Legacy cache methods (`_is_tardis_cache_valid`, etc.)
- Legacy date filtering methods (`_is_instrument_available_on_date`, etc.)
- Backward compatibility fallbacks

**All Tardis operations now go through `TardisAdapter`**.

---

## References

- **Integration Guide**: `docs/DATABENTO_DEFI_INTEGRATION.md`
- **Architecture**: `docs/ARCHITECTURE.md`
- **Unified Architecture**: `docs/UNIFIED_ARCHITECTURE_SPEC.md`

