# Batch Processing Guide

Guide for batch processing instruments across date ranges.

## Overview

Instrument definitions are relatively static and don't change frequently. Batch processing is the primary way to generate instruments for historical dates or date ranges.

## Usage

### Command Line

```bash
# Generate instruments for a date range
python examples/batch_generation.py --start-date 2023-05-23 --end-date 2023-05-24

# Generate for specific exchanges
python examples/batch_generation.py \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --exchanges binance deribit

# Force regeneration (overwrite existing)
python examples/batch_generation.py \
    --start-date 2023-05-23 \
    --end-date 2023-05-24 \
    --force
```

### Python API

```python
import asyncio
from datetime import datetime, timedelta
from instruments_service import InstrumentProcessingService, CloudInstrumentStorage
from instruments_service.config import VenueMapping

async def generate_batch(start_date: str, end_date: str):
    config = {
        'project_id': 'central-element-323112',
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

## Features

- **Date Range Processing**: Process multiple dates in sequence
- **Progress Tracking**: Logs progress for each date and exchange
- **Error Handling**: Continues processing even if individual dates fail
- **Summary Statistics**: Reports total instruments generated and errors
- **Exchange Filtering**: Process specific exchanges or all exchanges

## Best Practices

1. **Start Small**: Test with a small date range first
2. **Monitor Progress**: Watch logs for errors and progress
3. **Use Force Sparingly**: Only use `--force` when you need to regenerate existing data
4. **Check Results**: Verify generated instruments using query examples

## Related Documentation

- **[usage/USAGE_GUIDE.md](./usage/USAGE_GUIDE.md)** - Comprehensive usage guide
- **[reference/API_REFERENCE.md](./reference/API_REFERENCE.md)** - API reference
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Architecture documentation

---

*See [examples/batch_generation.py](../../examples/batch_generation.py) for complete example.*


