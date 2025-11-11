# Usage Guide

> **Related Documentation**:
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service overview and architecture
> - [`API_REFERENCE.md`](./API_REFERENCE.md) - Complete API documentation
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID format and examples

---

## For Downstream Clients

**Important**: Downstream clients should use `unified-cloud-services` directly to query instruments, NOT import `instruments-service`.

### Querying Instruments from GCS

```python
from unified_cloud_services import StandardizedDomainCloudService, CloudTarget

# Create market_data service (canonical pattern: direct instantiation)
service = StandardizedDomainCloudService(
    domain='market_data',
    cloud_target=CloudTarget(
        project_id='central-element-323112',
        gcs_bucket='market-data-tick',
        bigquery_dataset='market_data_hft'
    )
)

# Download instruments for a specific date
instruments_df = service.download_from_gcs(
    gcs_path='instrument_availability/by_date/day-2023-05-23/instruments.parquet',
    format='parquet'
)
```

### Querying Instruments from BigQuery

```python
# Query instruments from BigQuery
instruments_df = service.query_bigquery(
    query="""
    SELECT * FROM `instruments.instruments`
    WHERE venue = 'BINANCE-FUTURES'
      AND instrument_type = 'PERPETUAL'
    LIMIT 100
    """
)
```

## CLI Usage

### Generate Instruments

```bash
# Generate instruments for a single date
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23

# Generate instruments for a date range
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --force

# Generate for specific exchanges
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --exchanges binance-futures deribit
```

### Query Instruments

**Output Formats**: The `--output-format` option supports three formats:
- `summary` (default): Prints formatted summary to stdout
- `json`: Prints full JSON data to stdout
- `csv`: Saves data to a CSV file

```bash
# List instruments for a date (default: summary format)
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23

# Filter by venue and instrument type
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23 \
    --venues BINANCE-FUTURES \
    --instrument-types PERPETUAL

# Get instrument details
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23 \
    --query-type details \
    --instrument-id BINANCE-FUTURES:PERPETUAL:BTC-USDT

# Get summary statistics
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23 \
    --query-type summary

# Export to JSON (prints to stdout, can redirect with >)
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23 \
    --output-format json

# Export to CSV file
python -m instruments_service --mode instruments-query \
    --start-date 2023-05-23 \
    --output-format csv \
    --output-file instruments.csv
```

## For Service Developers

### Using Orchestration Service

```python
import asyncio
from datetime import datetime, timezone
from instruments_service.app.core.instruments_service import InstrumentsService

# Initialize orchestration service
config = {
    'project_id': 'central-element-323112',
    'enable_ccxt_integration': True,
    'enable_metadata_caching': True
}
service = InstrumentsService(config)

# Generate instruments for a date
result = await service.generate_instruments_for_date(
    date=datetime(2023, 5, 23, tzinfo=timezone.utc),
    force=False
)

# Generate instruments for a date range
result = await service.generate_instruments_date_range(
    start_date=datetime(2023, 5, 23, tzinfo=timezone.utc),
    end_date=datetime(2023, 5, 24, tzinfo=timezone.utc),
    force=False
)

# Query instruments
instruments_df = service.query_instruments(
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL'
)

# Cleanup
service.cleanup()
```

### Using Individual Services

```python
import asyncio
from datetime import datetime, timezone
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.config import VenueMapping

# Initialize processing service (uses Secret Manager for API key)
config = {
    'project_id': 'central-element-323112',
    'enable_ccxt_integration': True
}
processing_service = InstrumentProcessingService(config)

# Generate instruments for all exchanges
venue_mapping = VenueMapping()
target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)

instruments = await processing_service.generate_instruments_for_exchanges(
    exchanges=venue_mapping.all_tardis_exchanges,
    target_date=target_date
)

# Store to cloud
storage = CloudInstrumentStorage()
import pandas as pd
instruments_list = [inst.model_dump() for inst in instruments.values()]
instruments_df = pd.DataFrame(instruments_list)
storage.store_instruments(instruments_df, date=target_date)
```

### Using InstrumentsClient

```python
from instruments_service.clients.instruments_client import InstrumentsClient

# Initialize client
client = InstrumentsClient(
    project_id='central-element-323112',
    bucket_name='market-data-tick'
)

# Get instruments with filters
instruments_df = client.get_instruments_for_date(
    date='2023-05-23',
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL',
    base_currency='BTC'
)

# Get instrument details
details = client.get_instrument_details(
    date='2023-05-23',
    instrument_id='BINANCE-FUTURES:PERPETUAL:BTC-USDT'
)

# Get summary statistics
stats = client.get_summary_stats('2023-05-23')
```

## Examples

See `examples/` directory for complete working examples:
- `generate_instruments.py` - Single date generation
- `generate_instruments_date_range.py` - Date range processing
- `batch_generation.py` - Using orchestration service
- `query_instruments.py` - Query examples



