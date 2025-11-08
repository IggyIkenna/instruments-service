# GCP Setup for Integration Tests

## Overview

Integration tests require GCP resources to test real cloud operations (GCS, BigQuery, Secret Manager). This document explains the setup requirements and best practices.

## Required GCP Resources

### 1. Service Account Credentials

**Same service account as production** - No separate test account needed.

**Location**: Service account JSON file should be in one of these locations:
- `central-element-323112-e35fb0ddafe2.json` in project root
- `instruments-service/central-element-323112-e35fb0ddafe2.json`
- Path specified in `GOOGLE_APPLICATION_CREDENTIALS` environment variable

**Required Permissions**:
- **GCS**: `storage.objects.create`, `storage.objects.get`, `storage.objects.list`, `storage.buckets.create` (for auto-creation)
- **BigQuery**: `bigquery.datasets.get`, `bigquery.tables.create`, `bigquery.tables.getData`, `bigquery.jobs.create`
- **Secret Manager**: `secretmanager.secrets.get`, `secretmanager.versions.access`

### 2. Test GCS Bucket

**Bucket Name**: `market-data-tick-test` (configured in `.env` as `INSTRUMENTS_GCS_BUCKET_TEST`)

**Automatic Setup**: ✅ **Tests automatically create the bucket if it doesn't exist!**

The test fixtures in `tests/conftest.py` automatically:
1. Check if test bucket exists
2. Create it in `asia-northeast1` region if it doesn't exist
3. Grant `storage.objectAdmin` permissions to the service account

**Manual Setup** (only needed if auto-creation fails):
```bash
# Create test bucket (if it doesn't exist) - use asia-northeast1 region
gsutil mb -p central-element-323112 -l asia-northeast1 gs://market-data-tick-test

# Grant service account permissions
gsutil iam ch serviceAccount:YOUR_SERVICE_ACCOUNT@central-element-323112.iam.gserviceaccount.com:roles/storage.objectAdmin gs://market-data-tick-test
```

**Why separate test bucket?**
- **Isolation**: Prevents test data from polluting production data
- **Safety**: Tests can write/delete without affecting production
- **Cost**: Can use lifecycle policies to auto-delete test data

### 3. BigQuery Dataset

**Dataset**: `market_data_hft` (or `market_data_hft_test` if using test suffix)

**Setup**:
```bash
# Create test dataset (if needed) - use asia-northeast1 region
bq mk --dataset --location=asia-northeast1 central-element-323112:market_data_hft_test

# Grant service account permissions
bq show --format=prettyjson central-element-323112:market_data_hft_test
```

**Note**: Tests use the same dataset as production, but write to test tables (e.g., `instruments_integration_test`).

### 4. Secret Manager

**Secret Name**: `tardis-api-key`

**Setup**:
```bash
# Create secret (if it doesn't exist)
echo -n "your-api-key" | gcloud secrets create tardis-api-key \
  --project=central-element-323112 \
  --data-file=-

# Grant service account access
gcloud secrets add-iam-policy-binding tardis-api-key \
  --project=central-element-323112 \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT@central-element-323112.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

## Best Practices

### ✅ DO: Use Same Service Account

**Why**: 
- Simpler setup (no need to manage multiple service accounts)
- Same permissions model as production
- Easier credential management

**How**: Use the same service account JSON file for both production and tests.

### ✅ DO: Use Separate Test Bucket

**Why**:
- Prevents test data from affecting production
- Allows tests to write/delete freely
- Can use lifecycle policies to auto-cleanup

**How**: Configure `INSTRUMENTS_GCS_BUCKET_TEST=market-data-tick-test` in `.env`

### ✅ DO: Use Test Table Names

**Why**:
- Tests can write to test tables without affecting production tables
- Easy to identify and clean up test data

**How**: Tests use table names like `instruments_integration_test` instead of `instruments`

### ✅ DO: Use CloudTarget for Test Detection

**Why**:
- Centralized test environment detection
- Automatic test suffix handling
- Consistent across all services

**How**: Tests automatically detect test environment via:
- `ENVIRONMENT=test` environment variable
- `pytest` detection (checks for `pytest` in process name)
- `PYTEST_CURRENT_TEST` environment variable

### ❌ DON'T: Create Separate Test Service Account

**Why**:
- Unnecessary complexity
- Harder to manage permissions
- No security benefit (test bucket isolation is sufficient)

### ❌ DON'T: Write to Production Buckets in Tests

**Why**:
- Risk of data corruption
- Cost implications
- Hard to clean up

**How**: Always use test bucket (`market-data-tick-test`) for tests.

## Setup Checklist

- [ ] Service account JSON file exists in one of the expected locations
- [ ] Service account has required GCP permissions (GCS, BigQuery, Secret Manager)
- [x] **Test bucket `market-data-tick-test`** - ✅ **Automatically created by tests if missing**
- [x] **Service account permissions** - ✅ **Automatically granted by tests**
- [ ] BigQuery dataset `market_data_hft` exists (or test variant)
- [ ] Service account has BigQuery permissions
- [ ] Secret `tardis-api-key` exists in Secret Manager
- [ ] Service account can access `tardis-api-key` secret
- [ ] `.env` file has `INSTRUMENTS_GCS_BUCKET_TEST=market-data-tick-test`

## Quick Setup Script (Optional)

**Note**: Tests automatically create the test bucket and grant permissions. This script is only needed for manual verification or if auto-creation fails.

```bash
#!/bin/bash
# Optional manual setup for integration tests (tests do this automatically)

PROJECT_ID="central-element-323112"
SERVICE_ACCOUNT="your-service-account@${PROJECT_ID}.iam.gserviceaccount.com"
TEST_BUCKET="market-data-tick-test"
DATASET="market_data_hft"

# Create test bucket - use asia-northeast1 region (tests do this automatically)
gsutil mb -p ${PROJECT_ID} -l asia-northeast1 gs://${TEST_BUCKET} || echo "Bucket already exists"

# Grant GCS permissions (tests do this automatically)
gsutil iam ch serviceAccount:${SERVICE_ACCOUNT}:roles/storage.objectAdmin gs://${TEST_BUCKET}

# Create BigQuery dataset (if needed) - use asia-northeast1 region
bq mk --dataset --location=asia-northeast1 ${PROJECT_ID}:${DATASET}_test || echo "Dataset already exists"

# Grant BigQuery permissions (usually handled by project-level IAM)
# bq show --format=prettyjson ${PROJECT_ID}:${DATASET}_test

# Verify Secret Manager access
gcloud secrets describe tardis-api-key --project=${PROJECT_ID} || echo "Secret doesn't exist - create it first"

echo "✅ Setup complete!"
```

## Troubleshooting

### Error: "Bucket not found"
**Solution**: Tests automatically create the bucket. If this error persists:
1. Check that service account has `storage.buckets.create` permission
2. Verify credentials file is correct
3. Manually create bucket (see Quick Setup Script above):
```bash
gsutil mb -p central-element-323112 -l asia-northeast1 gs://market-data-tick-test
```

### Error: "Permission denied"
**Solution**: Tests automatically grant permissions. If this error persists:
1. Check that service account has `storage.buckets.setIamPolicy` permission (or project-level permissions)
2. Verify service account email is correct
3. Manually grant permissions (see Quick Setup Script above):
```bash
gsutil iam ch serviceAccount:YOUR_SERVICE_ACCOUNT@central-element-323112.iam.gserviceaccount.com:roles/storage.objectAdmin gs://market-data-tick-test
```

### Error: "Secret not found"
**Solution**: Create secret or check secret name:
```bash
gcloud secrets describe tardis-api-key --project=central-element-323112
```

### Error: "Credentials not found"
**Solution**: Place service account JSON file in one of:
- Project root: `central-element-323112-e35fb0ddafe2.json`
- Service directory: `instruments-service/central-element-323112-e35fb0ddafe2.json`
- Or set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

## References

- [Unified Architecture Spec - Test Environment Detection](../../docs/UNIFIED_ARCHITECTURE_SPEC.md#1415-test-environment-detection-)
- [CloudTarget Documentation](../../unified-cloud-services/docs/CLOUD_TARGET.md)
- [GCP Service Accounts](https://cloud.google.com/iam/docs/service-accounts)
- [GCS Bucket Setup](https://cloud.google.com/storage/docs/creating-buckets)
- [BigQuery Dataset Setup](https://cloud.google.com/bigquery/docs/datasets)

