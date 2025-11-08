# Schema Definitions

This directory contains schema definitions for instruments-service data storage.

## Files

### `parquet.py`
Defines the expected schema for Parquet files stored in GCS (batch historical data).

**Storage Location:**
- GCS Bucket: `instruments-store` (or `instruments-store-test` for tests)
- Path Format: `instrument_availability/by_date/day-{YYYY-MM-DD}/instruments.parquet`
- Format: Parquet (with headers/column names)

**Schema Source:**
- Based on `InstrumentDefinition` Pydantic model (`models.py`)
- Additional fields added during storage (e.g., `timestamp`)
- All fields validated via Pydantic before storage

**Usage:**
```python
from instruments_service.schemas.parquet import (
    INSTRUMENTS_PARQUET_SCHEMA,
    get_required_columns,
    validate_schema_compliance,
    get_schema_summary
)

# Get required columns
required = get_required_columns()
# ['instrument_key', 'venue', 'instrument_type', 'available_from_datetime', 'timestamp']

# Validate DataFrame columns
is_valid, missing = validate_schema_compliance(df.columns.tolist())

# Get schema summary
summary = get_schema_summary()
```

## Validation Flow

1. **Pydantic Model Validation** (`InstrumentDefinition`) - type checking, format validation
2. **Storage-Level Checks** (`CloudInstrumentStorage.store_instruments`) - required columns, timestamp conversion
3. **Parquet File Creation** - implicit schema (column names + pandas dtypes)

## Notes

- Parquet schema is **implicit** - derived from DataFrame column names and dtypes
- No explicit Parquet schema file is enforced - structure follows `InstrumentDefinition` model
- All datetime fields are stored as timezone-naive UTC (`datetime64[ns]`)
- String fields with defaults use empty string (`''`) not `None`
- Optional float fields (`contract_size`) can be `None`
- Optional datetime fields (`expiry`, `available_to_datetime`) can be `None`

## Future Schemas

If BigQuery live streaming is needed in the future, add `bigquery.py` here with BigQuery table schema definitions.

