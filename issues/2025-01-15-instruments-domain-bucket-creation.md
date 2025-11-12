# Instruments Domain GCS Bucket Creation Required

**Date**: 2025-01-15
**Status**: ⚠️ Action Required
**Repository**: `instruments-service`, `unified-trading-system-repos`
**Session**: Domain separation - instruments moved to own bucket/dataset

## Issue Summary

After separating the instruments domain from the `market_data` domain, a new GCS bucket needs to be created for instruments data. The code has been updated to use the new bucket, but the infrastructure doesn't exist yet.

## Problem

**Current State**:
- ✅ Code updated to use `instruments-store` bucket
- ✅ Code updated to use `instruments` BigQuery dataset
- ✅ Environment variables configured
- ❌ **GCS bucket `instruments-store` does not exist**
- ❌ **GCS bucket `instruments-store-test` does not exist**
- ❌ **BigQuery dataset `instruments` does not exist**

**Impact**:
- `instruments-service` will fail when trying to write/read from non-existent bucket
- Tests will fail when trying to use test bucket
- BigQuery queries will fail when trying to access non-existent dataset

## Root Cause

The architectural change to separate domains (moving instruments from `market_data` domain to `instruments` domain) was implemented in code, but the infrastructure creation was deferred.

## Required Actions

### 1. Create GCS Buckets

**Production Bucket**:
```bash
gsutil mb -p central-element-323112 -c STANDARD -l asia-northeast1 gs://instruments-store
```

**Test Bucket**:
```bash
gsutil mb -p central-element-323112 -c STANDARD -l asia-northeast1 gs://instruments-store-test
```

**Or use automated script**:
```bash
./scripts/create_instruments_bucket.sh
```

### 2. Create BigQuery Dataset

```bash
bq mk --dataset \
  --location=asia-northeast1 \
  --description="Instruments domain data" \
  central-element-323112:instruments
```

### 3. Verify Region Alignment

**Region**: `asia-northeast1`
- ✅ Matches `market-data-tick` bucket region
- ✅ Matches BigQuery dataset location
- ✅ Consistent with unified architecture

### 4. Migrate Existing Data (if applicable)

If instruments data currently exists in `market-data-tick` bucket:
```bash
gsutil -m cp -r gs://market-data-tick/instrument_availability/ gs://instruments-store/
```

### 5. Update IAM Permissions

Grant access to service accounts:
```bash
# instruments-service service account
gsutil iam ch serviceAccount:instruments-service@central-element-323112.iam.gserviceaccount.com:objectAdmin gs://instruments-store

# Other services (read-only)
gsutil iam ch serviceAccount:market-data-tick-handler@central-element-323112.iam.gserviceaccount.com:objectViewer gs://instruments-store
```

## Documentation

- ✅ `docs/INSTRUMENTS_BUCKET_SETUP.md` - Complete setup guide
- ✅ `scripts/create_instruments_bucket.sh` - Automated creation script
- ✅ `docs/DOMAIN_BOUNDARIES.md` - Updated with bucket creation notes

## Verification Steps

After creation, verify:

1. **Bucket exists**:
   ```bash
   gsutil ls gs://instruments-store
   ```

2. **Bucket region**:
   ```bash
   gsutil ls -L -b gs://instruments-store | grep Location
   ```

3. **Dataset exists**:
   ```bash
   bq ls -d central-element-323112:instruments
   ```

4. **Dataset location**:
   ```bash
   bq show --format=prettyjson central-element-323112:instruments | grep location
   ```

## Priority

**High** - Required before `instruments-service` can function correctly with new domain separation.

## Related Issues

- Domain separation architecture change
- Environment variable updates
- Code refactoring to use new bucket/dataset
