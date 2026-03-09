# GCS Path Documentation - instruments-service

This document describes the Google Cloud Storage (GCS) output paths for the instruments-service.

## Overview

The instruments-service generates instrument definitions for all market categories and stores them in category-specific GCS buckets.

## Bucket Structure

### Production Buckets

| Category | Bucket Name                                   |
| -------- | --------------------------------------------- |
| CEFI     | `gs://instruments-store-cefi-{project_id}/`   |
| TRADFI   | `gs://instruments-store-tradfi-{project_id}/` |
| DEFI     | `gs://instruments-store-defi-{project_id}/`   |

### Test Buckets (for E2E tests)

| Category | Bucket Name                                        |
| -------- | -------------------------------------------------- |
| CEFI     | `gs://instruments-store-test-cefi-{project_id}/`   |
| TRADFI   | `gs://instruments-store-test-tradfi-{project_id}/` |
| DEFI     | `gs://instruments-store-test-defi-{project_id}/`   |

## Path Structure

### Instrument Definitions

**Pattern:**

```
instrument_availability/by_date/day={YYYY-MM-DD}/instruments.parquet
```

**Full Path Example:**

```
gs://instruments-store-cefi-{project_id}/instrument_availability/by_date/day=2024-01-15/instruments.parquet
```

### Path Components

| Component                  | Description                              | Example           |
| -------------------------- | ---------------------------------------- | ----------------- |
| `instrument_availability/` | Top-level prefix for instrument data     | -                 |
| `by_date/`                 | Date-partitioned data                    | -                 |
| `day={YYYY-MM-DD}/`        | Specific date partition                  | `day=2024-01-15/` |
| `instruments.parquet`      | Parquet file with instrument definitions | -                 |

## File Format

### Parquet Schema

The `instruments.parquet` file contains the following key columns:

| Column              | Type   | Description                                |
| ------------------- | ------ | ------------------------------------------ |
| `instrument_id`     | string | Canonical instrument ID                    |
| `venue`             | string | Exchange/venue name                        |
| `market_category`   | string | CEFI, TRADFI, or DEFI                      |
| `instrument_type`   | string | SPOT_PAIR, PERPETUAL, FUTURE, OPTION, etc. |
| `base_currency`     | string | Base asset                                 |
| `quote_currency`    | string | Quote asset                                |
| `trading_hours_utc` | string | Trading hours in UTC                       |
| `tick_size`         | float  | Minimum price increment                    |
| `lot_size`          | float  | Minimum quantity increment                 |
| `metadata`          | struct | Additional venue-specific metadata         |

## Usage Examples

### Reading Instrument Definitions

```python
from unified_trading_services import StandardizedDomainCloudService, CloudTarget

# Create cloud-agnostic service
target = CloudTarget(
    project_id="your-project-id",
    gcs_bucket="instruments-store-cefi-your-project-id",
)
service = StandardizedDomainCloudService(domain="instruments", cloud_target=target)

# Read instruments for a specific date
bucket = "instruments-store-cefi-{project_id}"  # Replace {project_id} with actual project ID
path = "instrument_availability/by_date/day=2024-01-15/instruments.parquet"

# Using polars
df = pl.read_parquet(f"gs://{bucket}/{path}")

# Filter by venue
binance_instruments = df.filter(pl.col("venue") == "BINANCE-FUTURES")
```

### Checking Data Availability

```bash
# Using the data catalog script
python scripts/data_catalog.py --start-date 2024-01-01 --end-date 2024-01-31 --category CEFI

# Using gsutil
gsutil ls gs://instruments-store-cefi-{project_id}/instrument_availability/by_date/day=2024-01-15/
```

## Downstream Dependencies

The following services read instrument definitions from these paths:

1. **market-tick-data-service** - Uses instrument IDs to determine what data to download
2. **strategy-service** - Uses instrument definitions for strategy configuration
3. **execution-services** - Uses instrument specs (tick size, lot size) for execution simulation

## Data Retention

- Instrument definitions are retained indefinitely
- Each date has its own partition for efficient querying
- Historical data can be regenerated using the `--force` flag

## Sharding Configuration

See `unified-trading-deployment-v2/configs/sharding.instruments-service.yaml` for deployment sharding:

- **Dimensions:** category, date
- **Granularity:** daily
- **Typical shards:** 3 categories × N days

## Related Documentation

- [INSTRUMENT_SPECIFICATION.md](INSTRUMENT_SPECIFICATION.md) - Detailed schema documentation
- [SETUP_GUIDE.md](SETUP_GUIDE.md) - Service setup instructions
- [DEPLOYMENT_GUIDE_FEMI.md](DEPLOYMENT_GUIDE_FEMI.md) - Deployment procedures
