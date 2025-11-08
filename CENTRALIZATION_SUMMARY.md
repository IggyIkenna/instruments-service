# Centralization Summary: Credentials & Sampling

**Date**: 2025-11-06  
**Status**: ✅ COMPLETE

## Overview

Centralized credentials handling and CSV sampling logic in `unified-cloud-services` following DRY principles from the unified architecture specification.

## Changes Made

### 1. ✅ Credentials Auto-Detection (unified-cloud-services)

**Location**: `unified-cloud-services/unified_cloud_services/core/cloud_auth_factory.py`

**Changes**:
- Added `_find_credentials_file()` function to auto-detect credentials in common locations
- Added `_auto_detect_credentials()` function that only runs in development mode
- Updated all `CloudAuthFactory` methods (GCS, BigQuery, Secret Manager) to auto-detect credentials in dev mode only
- Production mode: Uses VM service account (no credentials file needed)

**Behavior**:
- **Development mode** (`ENVIRONMENT=development`): Auto-detects credentials files
- **Production mode** (`ENVIRONMENT=production`): Uses VM service account (no auto-detection)

**Search Locations** (dev mode only):
1. Current directory
2. Parent directory
3. Grandparent directory (unified-trading-system-repos root)
4. Home directory

**Credentials Filenames**:
- `central-element-323112-e35fb0ddafe2.json`
- `credentials.json`
- `gcp-credentials.json`
- `service-account.json`

### 2. ✅ Centralized Sampling Service (unified-cloud-services)

**Location**: `unified-cloud-services/unified_cloud_services/core/sampling_service.py`

**Features**:
- Environment-aware (only samples in non-production)
- Configurable sample size via `CSV_SAMPLE_SIZE` env var (default: 10)
- Smart sampling for different data types (liquidations, book_snapshot_5, etc.)
- Production mode: No sampling (doesn't drop samples from data, just doesn't create CSV files)

**Exports**:
- `SamplingService` class
- `create_sampling_service()` factory function

**Usage**:
```python
from unified_cloud_services import create_sampling_service

sampling_service = create_sampling_service()
sampling_service.generate_csv_sample(
    df=dataframe,
    filename_prefix='instruments',
    data_type='liquidations',  # Optional: for smart sampling
    metadata={'date': date, 'instrument_id': '...'}  # Optional
)
```

### 3. ✅ Removed Duplicate Code (instruments-service)

**Removed Files**:
- `instruments_service/utils/credentials.py` - Credentials now handled by unified-cloud-services
- `instruments_service/utils/csv_sampling.py` - Sampling now handled by unified-cloud-services

**Updated Files**:
- `instruments_service/cli/main.py`: Removed credential auto-detection (handled by unified-cloud-services)
- `instruments_service/cli/handlers/instrument_handler.py`: Uses centralized sampling service
- `instruments_service/app/core/cloud_instrument_storage.py`: Uses centralized sampling service

### 4. ✅ Documentation Updates

**Updated**:
- `instruments-service/docs/SETUP_GUIDE.md`: Updated credentials section to reflect centralized handling
- Added `CSV_SAMPLE_SIZE` to `.env` example

## Benefits

1. **DRY Principle**: No duplicate credential/sampling logic across services
2. **Consistency**: All services use same credential detection and sampling logic
3. **Environment Awareness**: Credentials only auto-detected in dev mode, production uses VM service account
4. **Centralized Configuration**: `CSV_SAMPLE_SIZE` controlled via env var in one place
5. **Production Safety**: Production mode doesn't create CSV samples (but doesn't drop data samples)

## Migration Path for Other Services

Other services can migrate to centralized sampling:

1. **Remove local sampling code**:
   - Delete service-specific CSV sampling utilities
   - Remove `CSV_SAMPLE_SIZE` handling from service code

2. **Use centralized service**:
   ```python
   from unified_cloud_services import create_sampling_service
   
   sampling_service = create_sampling_service()
   sampling_service.generate_csv_sample(df, 'prefix', data_type='...')
   ```

3. **Update environment variables**:
   - Set `CSV_SAMPLE_SIZE=10` in `.env` (or use default)
   - Set `ENABLE_CSV_SAMPLING=true` for development

## Testing

### Credentials Auto-Detection
```bash
# Development mode (auto-detects)
ENVIRONMENT=development python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force

# Production mode (uses VM service account)
ENVIRONMENT=production python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force
```

### Sampling Service
```bash
# Enable sampling in development
ENVIRONMENT=development ENABLE_CSV_SAMPLING=true CSV_SAMPLE_SIZE=10 python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force

# Production mode (no sampling)
ENVIRONMENT=production python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force
```

## Next Steps

1. **Migrate other services** to use centralized sampling:
   - `market-tick-data-handler`: Update `candle_processing_service.py` to use centralized sampling
   - Other services: Remove duplicate sampling code

2. **Update architecture docs** to reflect centralized patterns:
   - Document credentials auto-detection in unified-cloud-services
   - Document centralized sampling service

## Conclusion

✅ **Centralization complete** - Credentials and sampling are now centralized in `unified-cloud-services`, following DRY principles and the unified architecture specification. All services can now use these centralized utilities without duplication.



