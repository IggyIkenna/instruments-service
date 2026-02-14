# Instruments Service Examples

This directory contains examples demonstrating how to use the **instruments-service** locally.

## Purpose

The `examples/` directory serves two purposes:

1. **Service Usage**: Demonstrate how to run and use THIS service locally
2. **Dependency Access**: Show how to access dependency service data (if applicable)

**Note**:

- `instruments-service` has no dependencies (it provides instruments to all other services), so dependency access examples are not needed here.
- Instrument definitions are relatively static and don't change frequently, so batch processing for historical dates is the primary use case.

## Examples

### Service Launch Examples

#### `batch_generation.py` - Batch Instrument Generation

Generate instruments for a date range in batch mode. This is the primary way to generate instruments since instrument definitions are relatively static and don't change frequently.

**Usage:**

```bash
# Generate for date range
python examples/batch_generation.py --start-date 2023-05-23 --end-date 2023-05-24

# Generate for specific exchanges
python examples/batch_generation.py \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --exchanges binance deribit

# Force regeneration
python examples/batch_generation.py \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --force
```

**Features:**

- Date range processing
- Batch processing with progress tracking
- Error handling and reporting
- Summary statistics

### Query Examples

#### `query_instruments.py` - Query Instruments Data

Query instruments data using unified-cloud-services domain clients.

**Usage:**

```bash
# Run all query examples
python examples/query_instruments.py
```

**Features:**

- Query instruments for specific date
- Filter by venue, instrument type, base/quote currency
- Get instrument details
- Summary statistics
- Query by data type
- Date range queries

**Example Code:**

```python
from unified_cloud_services import create_instruments_client

client = create_instruments_client()
instruments_df = client.get_instruments_for_date(
    date='2023-05-23',
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL'
)
```

## Prerequisites

1. **Install packages**:

   ```bash
   # Install instruments-service
   cd /path/to/instruments-service
   pip install -e .

   # Install unified-cloud-services
   cd /path/to/unified-cloud-services
   pip install -e .
   ```

2. **Configure credentials**:
   - Set up GCP credentials (see `docs/SETUP_GUIDE.md`)
   - Ensure Secret Manager access for API keys
   - Set environment variables if needed:
     - `GCP_PROJECT_ID` (default: central-element-323112)
     - `INSTRUMENTS_GCS_BUCKET` (default: instruments-store)
     - `INSTRUMENTS_BIGQUERY_DATASET` (default: instruments)

3. **Set up infrastructure**:
   - GCS bucket: `instruments-store-central-element-323112` (CEFI/TRADFI/DEFI variants)
   - See `docs/SETUP_GUIDE.md` for infrastructure details

## Quick Start

```bash
# 1. Install packages
pip install -e /path/to/instruments-service
pip install -e /path/to/unified-cloud-services

# 2. Generate instruments for a date range
python examples/batch_generation.py --start-date 2023-05-23 --end-date 2023-05-24

# 3. Query instruments
python examples/query_instruments.py
```

**Note**: Instrument definitions are relatively static and don't change frequently. Batch processing for historical dates or date ranges is the primary use case.

## Pattern

All examples follow this pattern:

```python
#!/usr/bin/env python3
"""
Example: [Description]

Demonstrates how to [what this example shows].
"""

# Simple imports - assumes packages are installed
from instruments_service import InstrumentProcessingService, CloudInstrumentStorage
from unified_cloud_services import create_instruments_client

# ... rest of example
```

## Dependency Access

**Note**: `instruments-service` has no dependencies. Other services that depend on instruments-service should use:

```python
from unified_cloud_services import create_instruments_client

client = create_instruments_client()
instruments_df = client.get_instruments_for_date(date='2023-05-23')
```

## Related Documentation

- `docs/USAGE_GUIDE.md` - Comprehensive usage guide
- `docs/API_REFERENCE.md` - API reference
- `docs/SETUP_GUIDE.md` - Setup and infrastructure instructions
- `docs/ARCHITECTURE.md` - Service architecture
