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

### Prerequisites

- Python 3.13.x (required - see installation below)
- SSH key configured with GitHub (for unified-cloud-services)

**Note:** GCP credentials are included in the repo (private repo). The setup script will auto-detect them.

### One-Command Setup

```bash
# 1. Install Python 3.13 SYSTEM-WIDE (not in venv - the venv is created from this)
pyenv install 3.13.1 && pyenv local 3.13.1

# 2. Clone instruments-service
git clone git@github.com:IggyIkenna/instruments-service.git
cd instruments-service

# 3. Run setup (creates venv, installs everything, auto-activates)
source ./scripts/setup.sh

# 4. Verify installation
python -m instruments_service --help

# 5. Run quality gates
./scripts/quality-gates.sh
```

The setup script will:

1. Ask you to confirm Python 3.13 is installed
2. Show installation instructions if needed (brew, pyenv)
3. Verify architecture on Apple Silicon (ARM64 required)
4. Create a virtual environment (.venv/)
5. Install unified-cloud-services (latest) from GitHub
6. Install instruments-service with all dependencies
7. Auto-detect and configure GCP credentials

### Known Working Command

```bash
# Lightweight test: Generate CEFI instruments (fast, ~2 min)
python -m instruments_service \
  --category CEFI \
  --start-date 2023-05-23 \
  --end-date 2023-05-23

# Dry run to verify setup
python -m instruments_service \
  --category CEFI \
  --start-date 2023-05-23 \
  --end-date 2023-05-23 \
  --dry-run
```

> **Note:** This is the first service in the data pipeline. No upstream dependencies required.

### Troubleshooting

If terminal fails with exit code 1, check Python version:

```bash
# What Python version is active?
python --version

# Should be Python 3.13.x
# If not:
pyenv local 3.13.1
python --version
```

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

## OpenBB Integration (Corporate Actions)

This service uses **OpenBB** for enhanced corporate actions data (earnings, dividends) via the `CorporateActionsAdapter`.

### Data Sources

| Data Type | Primary Provider | Fallback |
| --------- | ---------------- | -------- |
| Earnings  | FMP (via OpenBB) | yfinance |
| Dividends | FMP (via OpenBB) | yfinance |

OpenBB provides richer data (revenue, fiscal periods, surprise %) compared to yfinance.

### Setup

```bash
# Install with OpenBB support
pip install -e ".[openbb]"

# Or install openbb separately
pip install openbb
```

### API Keys

API keys are loaded from Secret Manager or environment variables:

| Secret Name          | Env Fallback  | Purpose                            |
| -------------------- | ------------- | ---------------------------------- |
| `openbb-fmp-api-key` | `FMP_API_KEY` | FMP fundamentals/corporate actions |

Get a free FMP API key at: https://financialmodelingprep.com/developer/docs/ (250 calls/day free tier)

### Usage

```python
from instruments_service.corporate_actions import CorporateActionsAdapter

# Use OpenBB as primary provider with yfinance fallback
adapter = CorporateActionsAdapter(
    provider="openbb",
    fallback_to_yfinance=True,
    project_id="your-project-id"
)

# Fetch earnings with enhanced data
earnings = adapter.fetch_earnings("AAPL", start_date, end_date)

# Fetch dividends
dividends = adapter.fetch_dividends("AAPL", start_date, end_date)
```

## Documentation

- [Service Overview](docs/SERVICE_OVERVIEW.md) - Architecture and design
- [API Reference](docs/API_REFERENCE.md) - Complete API documentation
- [Usage Guide](docs/USAGE_GUIDE.md) - Usage examples and patterns
- [Setup Guide](docs/SETUP_GUIDE.md) - Installation and configuration
- [Instrument Key Specification](docs/INSTRUMENT_KEY.md) - Instrument ID format

# Build trigger test Tue Jan 27 15:04:47 GMT 2026
