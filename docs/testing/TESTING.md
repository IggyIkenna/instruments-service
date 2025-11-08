# Testing Guide

> **Related Documentation**:
> - [`../ARCHITECTURE.md`](../ARCHITECTURE.md) - Service overview and architecture
> - [`../SETUP_GUIDE.md`](../SETUP_GUIDE.md) - Setup instructions
> - [`../reference/API_REFERENCE.md`](../reference/API_REFERENCE.md) - Complete API documentation
> - **Architecture**: [`docs/UNIFIED_REPOSITORY_STRUCTURE.md`](../../../docs/UNIFIED_REPOSITORY_STRUCTURE.md) - Repository structure standards

---

## Test Structure

Tests are organized into three tiers per `UNIFIED_REPOSITORY_STRUCTURE.md`:

- **Unit Tests** (`tests/unit/`): Component isolation with mocked dependencies
- **Integration Tests** (`tests/integration/`): Real component interaction with test bucket
- **E2E Tests** (`tests/e2e/`): Full system workflow tests

## Test Bucket Configuration

**ALL tests use test bucket**: `market-data-tick-test` (not prod bucket)

The test bucket is automatically configured via `conftest.py` fixtures. Tests verify they're using the test bucket and not writing to prod.

## Running Tests

### Run All Tests
```bash
pytest tests/ -v --cov=instruments_service
```

### Run Unit Tests Only
```bash
pytest tests/unit/ -v
```

### Run Integration Tests Only
```bash
pytest tests/integration/ -v
```

### Run E2E Tests Only
```bash
pytest tests/e2e/ -v -m e2e
```

## Prerequisites

1. **GCP Credentials**: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. **Secret Manager Access**: Ensure credentials have access to Secret Manager
3. **Test Bucket**: Ensure `market-data-tick-test` bucket exists in GCP project

## Environment Variables

- `GOOGLE_APPLICATION_CREDENTIALS`: Path to GCP credentials JSON (required)
- `GCP_PROJECT_ID`: GCP project ID (default: 'central-element-323112')
- `INSTRUMENTS_GCS_BUCKET_TEST`: Test bucket name (default: 'market-data-tick-test')
- `ENABLE_CSV_SAMPLING`: Enable CSV samples (default: 'true' in tests)
- `CSV_SAMPLE_DIR`: CSV sample directory (default: './data/samples')

## E2E Test

The E2E test (`test_instrument_generation_e2e.py`) tests:
- Instrument download for 2023-05-23 to 2023-05-24
- GCS upload to test bucket
- Secret Manager authentication
- CSV sample generation
- Data integrity verification

Run with:
```bash
pytest tests/e2e/test_instrument_generation_e2e.py -v -m e2e
```

## Success Criteria

All tests must pass with:
- ✅ Test bucket usage (not prod)
- ✅ Secret Manager authentication
- ✅ Real GCP credentials
- ✅ Data integrity verification



