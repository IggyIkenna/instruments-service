# Error Handling - Instruments Service

This document describes the error handling patterns, error classification, retry logic, and recovery procedures for instruments-service.

## Overview

Instruments-service is the **ROOT of the data pipeline** with no upstream GCS dependencies. All errors relate to external API calls (Tardis, Databento, The Graph) or GCS storage operations.

## Error Classification

### Error Categories Used

| Category | When It Occurs | Recovery Strategy |
|----------|----------------|-------------------|
| `API` | External API errors (Tardis, Databento, The Graph) | RETRY with exponential backoff |
| `AUTHENTICATION` | Invalid API keys | MANUAL_INTERVENTION |
| `RATE_LIMIT` | API rate limits exceeded | RETRY with longer delays |
| `DATA_VALIDATION` | Invalid instrument data | SKIP and continue |
| `STORAGE` | GCS write errors | RETRY with exponential backoff |
| `NETWORK` | Network connectivity issues | RETRY with exponential backoff |

### Error Severity Levels

| Severity | When Used |
|----------|-----------|
| `LOW` | Missing optional instrument fields |
| `MEDIUM` | Individual instrument processing errors |
| `HIGH` | Venue adapter failures |
| `CRITICAL` | Missing required API keys |

## Custom Exceptions

```python
from unified_trading_services.core.dependency_checker import DependencyError

# DependencyError - raised when required API keys are missing
# instruments-service has no upstream GCS dependencies (it's the root)
```

## Retry Logic

### API Call Decorators

```python
from unified_trading_services import handle_api_errors, handle_storage_errors

@handle_api_errors(max_retries=3)
async def fetch_tardis_instruments():
    """Fetch instruments from Tardis API"""
    pass

@handle_storage_errors(max_retries=2)
async def upload_to_gcs():
    """Upload instrument definitions to GCS"""
    pass
```

### Venue-Specific Retry Configuration

| Venue | Max Retries | Notes |
|-------|-------------|-------|
| Tardis (CeFi) | 3 | Contact for rate limits |
| Databento (TradFi) | 3 | 5 requests/second limit |
| The Graph (DeFi) | 3 | Varies by plan |

## Dependency Checking

Since instruments-service is the **ROOT** of the pipeline, it has no upstream GCS dependencies. The dependency checker validates:

1. **Required API keys** are available in Secret Manager
2. **GCS buckets** are accessible for writing

```python
from instruments_service.app.core.dependency_checker import DependencyChecker

checker = DependencyChecker()

# Check external API availability
report = checker.check_external_apis(categories=["CEFI", "TRADFI"])

if not report.required_available:
    for check in report.checks:
        if check.required and not check.available:
            print(f"Missing: {check.name}")

# Validate service can run
try:
    checker.validate_can_run(categories=["CEFI"])
except DependencyError as e:
    logger.error(f"Cannot run: {e}")
```

### CLI Dependency Check

```bash
# Check if all required APIs are available
python -m instruments_service.cli.main check-deps --category CEFI
```

## Health Checks

### Docker Health Check

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -m instruments_service.cli.main health || exit 1
```

## Recovery Procedures

### Missing API Keys

1. Service fails fast with CRITICAL error
2. Check Secret Manager for key names:
   - `tardis-api-key` (CEFI)
   - `databento-api-key` (TRADFI)
   - `graph-api-key` (DEFI)
3. Verify IAM permissions for service account
4. Set fallback env vars for local development

### API Rate Limits

1. Service automatically backs off exponentially
2. For persistent rate limiting:
   - Check API plan limits
   - Consider requesting higher limits
   - Reduce parallel requests

### Invalid Instrument Data

1. Invalid instruments are skipped (not uploaded)
2. Error logged with specific reason
3. Review venue adapter logic
4. Check if exchange API schema changed

## Related Documentation

- [CONFIGURATION.md](CONFIGURATION.md) - Configuration options
- [DEPENDENCIES.md](DEPENDENCIES.md) - Pipeline position and dependencies
- [SECRETS_SETUP.md](SECRETS_SETUP.md) - API key configuration
