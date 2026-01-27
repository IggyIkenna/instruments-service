# Instruments Service

Service for generating canonical instrument definitions from exchange APIs.

## Status

**✅ COMPLETE**: Full end-to-end implementation with CLI, orchestration service, and comprehensive examples.

**Migration Status**: ✅ All instrument-related code has been successfully migrated from `market-tick-data-handler`. Old files have been deleted and all imports updated.

## Dependencies

- `unified-cloud-services` - For cloud operations (GCS, Secret Manager; BigQuery utilities for ad hoc use)
- `ccxt` - For exchange metadata enrichment
- `pydantic` - For data validation
- `requests` - For Tardis API integration

## Architecture

Follows unified repository structure per architecture plan:

```
instruments_service/
├── app/
│   ├── core/
│   │   ├── instruments_service.py        # Main orchestration service
│   │   ├── instrument_processing_service.py  # Instrument processing logic
│   │   ├── cloud_instrument_storage.py   # Stores instruments to GCS (batch data only)
│   │   ├── cloud_data_provider.py        # Reads instruments from unified-cloud-services
│   │   ├── batch_processor.py            # Batch processing with lookback
│   │   └── validation_service.py        # Service-specific validation
│   ├── venues/                           # ⚠️ DEVIATION: Venue-specific adapters (future)
│   └── visualization/
│       └── instrument_plotter.py         # Visualization utilities
├── cli/
│   ├── main.py                           # CLI entry point
│   ├── parser.py                         # Argument parsing
│   ├── base_handler.py                   # Base handler interface
│   └── handlers/
│       ├── instrument_handler.py         # Instrument generation handler
│       └── instruments_query_handler.py  # Query handler
├── models.py                             # InstrumentDefinition, InstrumentKey models
├── config.py                             # VenueMapping, ExchangeInstrumentConfig, DataTypeConfig, InstrumentsServiceConfig
└── requirements.txt
```

## Migration Status: ✅ COMPLETE

**All components have been successfully extracted and migrated from `market-tick-data-handler`:**

1. ✅ **Models** - `InstrumentDefinition`, `InstrumentKey`, `Venue`, `InstrumentType` (extracted and migrated)
2. ✅ **Configs** - `VenueMapping`, `ExchangeInstrumentConfig`, `DataTypeConfig` (extracted and migrated)
3. ✅ **Service** - `InstrumentProcessingService` (extracted and migrated, ~1547 lines)
4. ✅ **CLI Handlers** - Instrument generation and query handlers (extracted and migrated)

**Old files deleted from `market-tick-data-handler`:**
- ✅ `market_data_tick_handler/services/instrument_processing_service.py`
- ✅ `market_data_tick_handler/cli/handlers/instrument_handler.py`
- ✅ `market_data_tick_handler/cli/handlers/instruments_query_handler.py`
- ✅ `market_data_tick_handler/clients/instruments_client.py`

**Breaking Changes:**
- `market-tick-data-handler` CLI no longer supports instrument modes - use `instruments-service` CLI directly
- `market-tick-data-handler` clients module no longer exports `InstrumentsClient` - import from `instruments_service.clients.instruments_client`

### Integration Points:

- Uses `unified-cloud-services` for cloud operations
- Stores instruments to `market-data-tick` GCS bucket (market_data domain)
- Instruments are part of market_data domain (not separate domain)
- Uses Secret Manager for API key retrieval (no env var required)

## Quick Start

### Installation

**Prerequisites**: Both `instruments-service` and `unified-cloud-services` should be cloned as siblings.

```bash
# 1. Install unified-cloud-services first (required dependency)
pip install -e ../unified-cloud-services

# 2. Install instruments-service (automatically installs dependencies)
pip install -e .

# 3. Configure environment (copy example and edit if needed)
cp .env.example .env
# Edit .env to set your credentials path and preferences
```

**Configuration**: The service uses a `.env` file for configuration. Key settings:
- `ENVIRONMENT=development` - Auto-detects credentials, enables sampling
- `ENABLE_CSV_SAMPLING=true` - Enable CSV samples in development
- `CSV_SAMPLE_SIZE=10` - Number of rows per sample
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to credentials file (auto-detected if not set)

**That's it!** The service automatically detects credentials files in common locations - no manual setup needed.

**Note**: Make sure your `.env` file has the correct bucket name (`INSTRUMENTS_GCS_BUCKET=instruments-store-central-element-323112`) and credentials path.

### CLI Usage

```bash
# Generate instruments for all domains (CeFi + TradFi + DeFi)
python -m instruments_service --mode instruments \
    --start-date 2025-01-06 --end-date 2025-01-06 \
    --CEFI --TRADFI --DEFI --force

# Generate CeFi only (Binance, Deribit, Bybit, OKX via Tardis)
python -m instruments_service --mode instruments \
    --start-date 2025-01-06 --CEFI --force

# Generate TradFi only (CME, NASDAQ, NYSE via Databento)
python -m instruments_service --mode instruments \
    --start-date 2025-01-06 --TRADFI --force

# Generate DeFi only (Uniswap, Aave, Curve via The Graph)
python -m instruments_service --mode instruments \
    --start-date 2025-01-06 --DEFI --force
```

### Programmatic Usage

```python
from instruments_service.app.core.instruments_service import InstrumentsService
from datetime import datetime, timezone

# Initialize service
config = {
    'project_id': 'central-element-323112',
    'enable_ccxt_integration': True
}
service = InstrumentsService(config)

# Generate instruments for a date
result = await service.generate_instruments_for_date(
    date=datetime(2023, 5, 23, tzinfo=timezone.utc)
)

# Query instruments
instruments_df = service.query_instruments(
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL'
)
```

## Documentation

- [Service Overview](docs/SERVICE_OVERVIEW.md) - Architecture and design
- [API Reference](docs/API_REFERENCE.md) - Complete API documentation
- [Usage Guide](docs/USAGE_GUIDE.md) - Usage examples and patterns
- [Setup Guide](docs/SETUP_GUIDE.md) - Installation and configuration
- [Instrument Key Specification](docs/INSTRUMENT_KEY.md) - Instrument ID format
# Build trigger test Tue Jan 27 15:04:47 GMT 2026
