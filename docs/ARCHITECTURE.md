# Instruments Service Architecture

> **Related Documentation**:
> - [`SETUP_GUIDE.md`](./SETUP_GUIDE.md) - Setup, installation, and quick start guide
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Complete instrument ID specification
> - [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter pattern and data sources
> - [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples
> - [`API_REFERENCE.md`](./API_REFERENCE.md) - API documentation

---

## Purpose

The Instruments Service generates canonical instrument definitions (metadata for centralized/normalized definitions and lookup of instrument attributes) from exchange APIs and DeFi protocol SDKs and stores them to GCS. It serves as the authoritative source for instrument metadata across the trading system.

**Key Responsibilities**:
- Discover available instruments (what instruments exist)
- Generate canonical instrument IDs following unified specification
- Enrich with metadata (contract addresses, fee tiers, tick sizes, etc.)
- Track availability windows and instrument lifecycle
- Store instrument definitions to GCS for downstream consumption

## Role in Trading System

The instruments-service is the **first service in the data pipeline**, providing instrument metadata that enables all downstream services:

```
instruments-service (this service)
    ↓
market-tick-data-handler (downloads market data using instrument IDs)
    ↓
market-data-processing-service (processes ticks into candles)
    ↓
features-* services (generate features from processed data)
    ↓
strategy-service (uses instruments for trading decisions)
    ↓
execution-service (uses instruments for order execution)
```

**Downstream Consumers**:
- **market-tick-data-handler**: Uses instrument IDs to download market data (trades, order books, etc.)
- **market-data-processing-service**: Uses instrument metadata for candle generation
- **features-* services**: Use instrument metadata for feature calculation
- **strategy-service**: Uses instrument definitions for trading decisions
- **execution-service**: Uses instrument metadata (contract addresses, fee tiers) for order execution

**Important**: Downstream clients should use `unified-cloud-services` directly to query instruments, NOT import `instruments-service`. See [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) for examples.

## Core Components

### 1. InstrumentProcessingService (`app/core/instrument_processing_service.py`)

Main orchestration service that:
- Coordinates venue adapters to fetch instruments from various data sources
- Generates canonical instrument IDs following unified specification
- Enriches with CCXT metadata (for CEX instruments)
- Filters by exchange configuration
- Handles batch processing for date ranges

### 2. Venue Adapters (`app/venues/`)

All data sources use a consistent venue adapter pattern:

- **TardisAdapter** (`venues/tardis/`) - Crypto exchanges (Binance, Bybit, OKX, Deribit)
- **DatabentoAdapter** (`venues/databento/`) - TradFi exchanges (CME, NASDAQ, NYSE)
- **DeFi Adapters** (`venues/defi/`) - DeFi protocols:
  - Uniswap V2/V3/V4 (The Graph, Envio)
  - Curve (The Graph, RPC)
  - Balancer (The Graph)
  - AAVE V3 (The Graph, Protocol SDKs)
  - EtherFi, Lido (Protocol SDKs, Alchemy)

Each adapter handles:
- API communication with retry logic
- Response caching (TTL-based)
- Date filtering (availability windows)
- Data transformation to standardized format
- Secret Manager integration for API keys

See [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) for detailed adapter architecture.

### 3. CloudInstrumentStorage (`app/core/cloud_instrument_storage.py`)

Handles storage operations:
- Stores instruments to GCS (Parquet format)
- Validates schema using `unified-cloud-services.SchemaValidator`
- Handles test bucket detection (automatically uses test buckets in test environment)
- Generates CSV samples for local development
- Uses `unified-cloud-services` for all cloud operations

### 4. InstrumentBatchProcessor (`app/core/batch_processor.py`)

Handles batch processing:
- Date range processing (start date to end date)
- Memory estimation for large batches
- Batch splitting for memory-constrained environments
- Gap detection and error handling

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
├─────────────────────────────────────────────────────────────┤
│  Tardis API → TardisAdapter                                 │
│  Databento API → DatabentoAdapter                            │
│  The Graph → UniswapV3Adapter, CurveAdapter                 │
│  Envio → UniswapV4Adapter                                   │
│  Protocol SDKs → AaveV3Adapter, EtherFiAdapter, LidoAdapter │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│         InstrumentProcessingService                         │
│  - Coordinates adapters                                      │
│  - Generates canonical IDs                                  │
│  - Enriches with CCXT metadata (CEX)                      │
│  - Enriches with contract addresses (DeFi)                 │
│  - Filters by exchange config                               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              InstrumentDefinition (Pydantic)                │
│  - Canonical instrument ID                                  │
│  - Metadata (addresses, fees, sizes, etc.)                 │
│  - Availability windows                                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│         CloudInstrumentStorage                              │
│  - Validates schema (unified-cloud-services)                │
│  - Stores to GCS (Parquet)                                  │
│  - Generates CSV samples (local dev)                        │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### Centralized Configuration (`config.py`)

All environment variables are accessed through `instruments_service/config.py` rather than scattered `os.getenv()` calls:

```python
# ✅ DO: Import from config.py
from instruments_service.config import instruments_config
bucket = instruments_config.gcs_bucket

# ❌ DON'T: Use os.getenv directly in code
import os
bucket = os.getenv("INSTRUMENTS_GCS_BUCKET")  # Avoid this pattern
```

**Why centralize configuration?**
- **Single Source of Truth**: All environment variable names defined in one place
- **Easy Refactoring**: Rename env vars or change defaults in one file
- **Environment-Aware**: Logic for test vs production routing handled centrally
- **Type Safety**: Pydantic BaseSettings provides validation and type coercion
- **Documentation**: All config options visible in one file with docstrings

**Key Config Classes**:
- `InstrumentsServiceConfig`: Service-level settings (buckets, project ID, secrets)
- `UnifiedInstrumentConfig`: Domain-specific instrument definitions loaded from JSON

### Static Data Abstraction (`data/` directory)

Static data (instrument definitions, ticker lists) is externalized to JSON files in `instruments_service/data/`:

```
instruments_service/data/
├── sp500_tickers.json      # S&P 500 equity ticker list
└── tradfi_instruments.json # TradFi instrument definitions + exchange mappings
```

**Why externalize static data?**
- **Testability**: Tests can exclude data files or mock them easily
- **Maintainability**: Update instrument definitions without modifying Python code
- **Separation of Concerns**: Data vs logic clearly separated
- **Version Control**: JSON changes are easy to review in PRs
- **Reduced File Size**: `config.py` reduced from 829 to 618 lines after externalization

**Loading Pattern**:
```python
# Data loaded lazily with caching
_TRADFI_INSTRUMENTS_CACHE = None

def _load_tradfi_instruments():
    global _TRADFI_INSTRUMENTS_CACHE
    if _TRADFI_INSTRUMENTS_CACHE is None:
        data_dir = Path(__file__).parent / "data"
        with open(data_dir / "tradfi_instruments.json") as f:
            _TRADFI_INSTRUMENTS_CACHE = json.load(f)
    return _TRADFI_INSTRUMENTS_CACHE
```

### Venue Adapter Pattern

All data sources use adapters for consistent architecture:
- **Separation of Concerns**: API communication separated from business logic
- **Testability**: Adapters can be tested independently
- **Extensibility**: Easy to add new data sources
- **Consistency**: Same patterns across all venues

### Unified Cloud Services

Uses `unified-cloud-services` for **all** cloud operations:
- **GCS**: Storage operations via `StandardizedDomainCloudService`
- **BigQuery**: Available but not used (batch data to GCS only)
- **Secret Manager**: API key retrieval via `get_secret_with_fallback`
- **Schema Validation**: Uses `SchemaValidator` with domain-specific schemas
- **Sampling Service**: CSV sample generation for local development

**DRY Compliance**: 100% - no custom cloud code, all operations use unified-cloud-services.

### Secret Manager Integration

All API keys retrieved from Secret Manager (no env vars for keys):
- Tardis API key: `tardis-api-key`
- Databento API key: `databento-api-key`
- The Graph API key: `thegraph-api-key`
- Alchemy API key: `alchemy-api-key`
- Envio API key: `envio-api-key`

Environment variables only specify secret names, not actual keys.

### Test Bucket Isolation

Automatically detects test environment and uses test buckets:
- Test buckets: `market-data-tick-test`, `market-data-hft-test`
- Production buckets: `market-data-tick`, `market-data-hft`
- Detection via environment variables or GCP project configuration

### Domain Boundaries

Instruments are part of `market_data` domain:
- GCS bucket: `market-data-tick`
- BigQuery dataset: `market_data_hft`
- Schema domain: `market_data`

## Storage Structure

### GCS Path Format

```
gs://market-data-tick/instrument_availability/by_date/day-YYYY-MM-DD/instruments.parquet
```

**Path Components**:
- `instrument_availability`: Top-level prefix
- `by_date`: Date-based partitioning
- `day-YYYY-MM-DD`: Daily partitions (e.g., `day-2025-01-15`)
- `instruments.parquet`: Parquet file with all instruments for that date

### BigQuery Table

- **Dataset**: `market_data_hft`
- **Table**: `instruments` (configurable)
- **Usage**: Available for querying, but batch data primarily stored in GCS

### Daily Snapshots

- Generated at midnight UTC
- Each snapshot contains all active instruments for that date
- Instruments automatically marked `active = false` at expiry
- Downstream services use availability windows to filter instruments

## Integration Points

### unified-cloud-services

**Required dependency** - provides all cloud infrastructure:
- GCS operations (read/write Parquet files)
- BigQuery operations (query instrument definitions)
- Secret Manager (API key retrieval)
- Schema validation (Parquet schema validation)
- Domain clients (`create_instruments_client` for downstream access)

### Venue Adapters

Abstract data source integrations:
- **TardisAdapter**: Crypto exchanges (Binance, Bybit, OKX, Deribit)
- **DatabentoAdapter**: TradFi exchanges (CME, NASDAQ, NYSE)
- **The Graph Adapters**: DeFi DEX pools (Uniswap V3, Curve, Balancer)
- **Envio Adapter**: Uniswap V4 pools
- **Protocol SDK Adapters**: DeFi protocols (AAVE V3, EtherFi, Lido)

### CCXT (Optional)

Exchange metadata enrichment for CEX instruments:
- Provides standardized exchange metadata
- Used for tick size, min size, contract size, etc.
- Optional - adapters work without CCXT

## Comparison with Archive Scripts

**Important Distinction**: The archive `basis-strategy-v1/scripts` fetched **market data** (rates, prices, OHLCV), while `instruments-service` fetches **instrument definitions** (metadata about what instruments exist).

### What Archive Scripts Fetched (Market Data)

- AAVE supply/borrow rates (APY)
- Oracle prices (weETH/ETH ratios)
- DEX pool OHLCV prices
- Staking yields
- Gas costs

### What Instruments-Service Fetches (Instrument Definitions)

- Pool contract addresses
- Token contract addresses
- Fee tiers
- Pool metadata (TVL for filtering)
- Creation timestamps
- Risk parameters (LTV, liquidation thresholds)
- Interest rate model parameters

### This Is Intentional

**Instruments-Service Purpose**:
- Discover **what instruments exist** (instrument catalog)
- Provide **metadata** for execution (addresses, fee tiers)
- **NOT** fetch market data (rates, prices, yields)

**Market Data Should Come From**:
- **market-tick-data-handler**: OHLCV, trades, funding rates (CEX + TradFi)
- **Separate Market Data Service** (future): DeFi rates, oracle prices, yields

## Instrument Lifecycle

1. **Discovery**: Venue adapters fetch available instruments from data sources
2. **Generation**: Canonical instrument IDs generated following unified specification
3. **Enrichment**: Metadata added (contract addresses, fee tiers, etc.)
4. **Storage**: Instruments stored to GCS with availability windows
5. **Expiry**: Instruments automatically marked `active = false` at expiry
6. **Daily Snapshots**: New snapshots generated at midnight UTC

## Batch Processing

### Date Range Processing

Processes instruments for date ranges:
- Start date to end date (inclusive)
- Handles large date ranges with memory estimation
- Splits batches if memory constrained
- Detects gaps and handles errors gracefully

### Performance Benchmarks

- **Compute Time (1 day)**: ~30-60 seconds (depends on exchange)
- **Memory Usage**: ~500MB (for typical exchange)
- **Throughput**: ~100-200 instruments/second

## Related Documentation

- [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Complete instrument ID specification
- [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter architecture and supported venues
- [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and data sources
- [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples and client patterns
- [`API_REFERENCE.md`](./API_REFERENCE.md) - Complete API documentation
