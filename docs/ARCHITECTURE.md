# Instruments Service Architecture

> **Related Documentation**:
> - [`QUICK_START.md`](./QUICK_START.md) - Quick start guide
> - [`SETUP_GUIDE.md`](./SETUP_GUIDE.md) - Setup and installation instructions
> - [`usage/USAGE_GUIDE.md`](./usage/USAGE_GUIDE.md) - Usage examples for clients and developers
> - [`reference/API_REFERENCE.md`](./reference/API_REFERENCE.md) - Complete API documentation
> - [`testing/TESTING.md`](./testing/TESTING.md) - Testing guide
> - [`INSTRUMENT_KEY.md`](./INSTRUMENT_KEY.md) - Instrument ID format and implementation details
> - **Canonical Spec**: [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md) - Complete canonical instrument ID specification
> - **Architecture**: [`docs/UNIFIED_ARCHITECTURE_SPEC.md`](../../docs/UNIFIED_ARCHITECTURE_SPEC.md) - Complete system architecture

---

## Purpose

The Instruments Service generates canonical instrument definitions (metadata for centralised / normalised definitions and lookup of instrument attributes) from exchange APIs adn Defi protocol SDKsand stores them to GCS. It serves as the authoritative source for instrument metadata across the trading system.

## Architecture

### Core Components

1. **InstrumentProcessingService** (`app/core/instrument_processing_service.py`)
   - Main orchestration service
   - Coordinates venue adapters to fetch instruments
   - Generates canonical instrument IDs
   - Enriches with CCXT metadata
   - Filters by exchange configuration

2. **Venue Adapters** (`app/venues/`)
   - **TardisAdapter** (`venues/tardis/`) - Crypto exchanges (Binance, Bybit, OKX, Deribit)
   - **DatabentoAdapter** (`venues/databento/`) - TradFi exchanges (CME, NASDAQ, NYSE)
   - **DeFi Adapters** (`venues/defi/`) - DeFi protocols (Uniswap V3, Curve, AAVE, EtherFi, Lido)
   - Each adapter handles API communication, caching, and data transformation

3. **CloudInstrumentStorage** (`app/core/cloud_instrument_storage.py`)
   - Stores instruments to GCS (Parquet format)
   - Handles test bucket detection
   - Generates CSV samples for local development
   - Uses unified-cloud-services for cloud operations

4. **InstrumentBatchProcessor** (`app/core/batch_processor.py`)
   - Handles date range processing
   - Memory estimation
   - Batch splitting

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
├─────────────────────────────────────────────────────────────┤
│  Tardis API → TardisAdapter                                 │
│  Databento API → DatabentoAdapter                            │
│  The Graph → UniswapV3Adapter, CurveAdapter                 │
│  Protocol SDKs → AaveV3Adapter, EtherFiAdapter, LidoAdapter │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│         InstrumentProcessingService                         │
│  - Coordinates adapters                                      │
│  - Generates canonical IDs                                  │
│  - Enriches with CCXT metadata                              │
│  - Filters by exchange config                               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              InstrumentDefinition (Pydantic)                │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│         CloudInstrumentStorage                              │
│  - Validates schema (unified-cloud-services)                │
│  - Stores to GCS (Parquet)                                  │
│  - Generates CSV samples (local dev)                        │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

- **Venue Adapter Pattern**: All data sources use adapters for consistent architecture
- **Unified Cloud Services**: Uses `unified-cloud-services` for all cloud operations (GCS, BigQuery, Secret Manager)
- **Secret Manager Integration**: All API keys retrieved from Secret Manager (no env vars for keys)
- **Schema Validation**: Uses `unified-cloud-services.SchemaValidator` with domain-specific schema definitions
- **Test Bucket Isolation**: Automatically detects test environment and uses test buckets
- **Domain Boundaries**: Instruments are part of `market_data` domain (not separate domain)

## Storage Structure

### GCS Path Format
```
gs://market-data-tick/instrument_availability/by_date/day-YYYY-MM-DD/instruments.parquet
```

### BigQuery Table
- Dataset: `market_data_hft`
- Table: `instruments` (configurable)

## Integration Points

- **unified-cloud-services**: Cloud operations (GCS, BigQuery, Secret Manager, SchemaValidator)
- **Venue Adapters**: Abstract data source integrations
  - **TardisAdapter**: Crypto exchanges (Binance, Bybit, OKX, Deribit)
  - **DatabentoAdapter**: TradFi exchanges (CME, NASDAQ, NYSE)
  - **DeFi Adapters**: The Graph (Uniswap V3, Curve), Protocol SDKs (AAVE, EtherFi, Lido)
- **CCXT**: Exchange metadata enrichment (optional)

## Downstream Client Pattern

**Important**: Downstream clients should use `unified-cloud-services` directly to query instruments, NOT import `instruments-service`.

See [`usage/USAGE_GUIDE.md`](./usage/USAGE_GUIDE.md) for examples.



