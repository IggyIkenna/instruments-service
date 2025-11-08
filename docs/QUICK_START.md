# Quick Start Guide

Get started with instruments-service in 5 minutes.

## Prerequisites

- Python 3.9+
- GCP project access
- Access to Secret Manager (for Tardis API key)

## Installation

```bash
# 1. Clone repositories (as siblings)
git clone <instruments-service-repo-url> instruments-service
git clone <unified-cloud-services-repo-url> unified-cloud-services

# 2. Create virtual environment
cd instruments-service
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install unified-cloud-services first (required)
pip install -e ../unified-cloud-services

# 4. Install instruments-service
pip install -e .
```

## Credentials Setup

**Automatic**: Place your GCP credentials file (`central-element-323112-e35fb0ddafe2.json`) in:
- Current directory
- Parent directory
- Grandparent directory (unified-trading-system-repos root)
- Home directory

The service will automatically detect it in development mode.

**Manual**: Set environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Quick Test

```bash
# Test import
python -c "from instruments_service import InstrumentProcessingService; print('✅ Import successful')"

# Test Secret Manager access
python -c "from unified_cloud_services import get_secret_with_fallback; key = get_secret_with_fallback('central-element-323112', 'tardis-api-key'); print('✅ Secret Manager works' if key else '❌ Secret Manager failed')"
```

## Generate Instruments

### Using CLI (Recommended - Same as unified-trading-deployment)

```bash
# Generate instruments for a date range
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24

# With force flag (regenerate even if exists)
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24 --force

# Filter specific exchanges
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24 --exchanges BINANCE-FUTURES BYBIT
```

**Note**: This is the same command used by `unified-trading-deployment`. The deployment uses `python -m instruments_service.cli.main` (equivalent to `python -m instruments_service`).

### Using Example Script (Alternative)

```bash
# Generate instruments for a date range
python examples/batch_generation.py --start-date 2023-05-23 --end-date 2023-05-24
```

## Query Instruments

### Using CLI (Recommended)

```bash
# List instruments for a date
python -m instruments_service --mode instruments-query --start-date 2023-05-23

# Filter by venue and instrument type
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --venues BINANCE-FUTURES --instrument-types PERPETUAL

# Get instrument details
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --query-type details --instrument-id BINANCE-FUTURES:PERPETUAL:BTC-USDT
```

### Using Example Script (Alternative)

```bash
# Query instruments using unified-cloud-services
python examples/query_instruments.py
```

Or in Python:

```python
from unified_cloud_services import create_instruments_client

client = create_instruments_client()
instruments_df = client.get_instruments_for_date(
    date='2023-05-23',
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL'
)
```

## Next Steps

- **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** - Detailed setup instructions
- **[usage/USAGE_GUIDE.md](./usage/USAGE_GUIDE.md)** - Comprehensive usage guide
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Architecture documentation
- **[reference/API_REFERENCE.md](./reference/API_REFERENCE.md)** - Complete API reference

---

*See [SETUP_GUIDE.md](./SETUP_GUIDE.md) for detailed setup instructions.*


