# Usage Guide

Comprehensive usage guide for instruments-service.

> **Related Documentation**:
>
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
        project_id='{project_id}',  # Replace with actual project ID
        gcs_bucket='market-data-tick',
        bigquery_dataset='market_data_hft'
    )
)

# Download instruments for a specific date
instruments_df = service.download_from_gcs(
    gcs_path='instrument_availability/by_date/day=2023-05-23/instruments.parquet',
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

---

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

# Generate by category
python -m instruments_service --mode instruments --start-date 2023-05-23 --CEFI
python -m instruments_service --mode instruments --start-date 2023-05-23 --TRADFI
python -m instruments_service --mode instruments --start-date 2023-05-23 --DEFI

# Generate for specific exchanges
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --exchanges binance-futures deribit
```

---

## Batch Processing

Guide for batch processing instruments across date ranges.

### Overview

Instrument definitions are relatively static and don't change frequently. Batch processing is the primary way to generate instruments for historical dates or date ranges.

### Batch Processing CLI

```bash
# Generate instruments for a date range
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-30

# Generate for specific exchanges
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --exchanges binance-futures deribit

# Force regeneration (overwrite existing)
python -m instruments_service --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --force

# Batch by category
python -m instruments_service --mode instruments \
    --start-date 2020-01-01 \
    --end-date 2025-12-01 \
    --CEFI --force
```

### Batch Processing Python API

```python
import asyncio
from datetime import datetime, timedelta
from instruments_service import InstrumentProcessingService, CloudInstrumentStorage
from instruments_service.config import VenueMapping

async def generate_batch(start_date: str, end_date: str):
    config = {
        'project_id': '{project_id}',  # Replace {project_id} with actual project ID
        'gcs_bucket': 'instruments-store',
        'bigquery_dataset': 'instruments'
    }

    processing_service = InstrumentProcessingService(config)
    storage_service = CloudInstrumentStorage(config)

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    current_date = start_dt
    while current_date <= end_dt:
        # Generate instruments for date
        instruments = await processing_service.process_exchange_instruments(
            exchange='binance-futures',
            target_date=current_date
        )

        # Store to cloud
        if instruments:
            import pandas as pd
            instruments_df = pd.DataFrame([inst.model_dump() for inst in instruments.values()])
            storage_service.store_instruments(
                instruments_df=instruments_df,
                table_name="instruments",
                date=current_date
            )

        current_date += timedelta(days=1)

# Run
asyncio.run(generate_batch('2023-05-23', '2023-05-24'))
```

### Batch Features

- **Date Range Processing**: Process multiple dates in sequence
- **Progress Tracking**: Logs progress for each date and exchange
- **Error Handling**: Continues processing even if individual dates fail
- **Summary Statistics**: Reports total instruments generated and errors
- **Exchange Filtering**: Process specific exchanges or all exchanges

### Batch Best Practices

1. **Start Small**: Test with a small date range first
2. **Monitor Progress**: Watch logs for errors and progress
3. **Use Force Sparingly**: Only use `--force` when you need to regenerate existing data
4. **Check Results**: Verify generated instruments using query examples

---

## For Service Developers

### Using Orchestration Service

```python
import asyncio
from datetime import datetime, timezone
from instruments_service.app.core.instruments_service import InstrumentsService

# Initialize orchestration service
config = {
    'project_id': '{project_id}',  # Replace with actual project ID
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
    'project_id': '{project_id}',  # Replace with actual project ID
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
    project_id='{project_id}',  # Replace with actual project ID
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

---

## Examples

See `examples/` directory for complete working examples:

- `batch_generation.py` - Batch instrument generation using orchestration service
- `query_instruments.py` - Query instruments using unified-cloud-services client

---

_Last Updated: December 2025_
