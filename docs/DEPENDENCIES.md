# Dependencies Documentation - instruments-service

This document describes the upstream and downstream dependencies for instruments-service.

## Dependency Position

**instruments-service is the ROOT of the data pipeline.**

It has NO upstream GCS data dependencies. All data sources are external APIs.

```
[External APIs] → instruments-service → [Downstream Services]
```

## Upstream Dependencies (External APIs)

### CEFI (Centralized Finance)

| API | Purpose | Secret Name | Required |
|-----|---------|-------------|----------|
| Tardis API | Exchange instrument data | `tardis-api-key` | Yes |

**Supported Exchanges:**
- BINANCE-SPOT
- BINANCE-FUTURES
- DERIBIT
- BYBIT
- OKX
- UPBIT
- COINBASE

### TRADFI (Traditional Finance)

| API | Purpose | Secret Name | Required |
|-----|---------|-------------|----------|
| Databento API | Market data metadata | `databento-api-key` | Yes |
| yfinance | Corporate actions | None (free) | No |

**Supported Venues:**
- CME (Globex)
- NASDAQ
- NYSE
- CBOE
- ICE

### DEFI (Decentralized Finance)

| API | Purpose | Secret Name | Required |
|-----|---------|-------------|----------|
| The Graph | Subgraph queries | `graph-api-key` | Yes |
| Alchemy | Ethereum RPC | `alchemy-api-key` | No |
| AAVEScan | AAVE protocol data | `aavescan-api-key` | No |
| Envio | Price data | None | No |

**Supported Protocols:**
- Uniswap V2/V3/V4
- AAVE V3
- Hyperliquid
- Curve
- Balancer
- Morpho
- Lido
- EtherFi
- Ethena

## Downstream Dependents

Services that depend on instruments-service output:

### 1. market-tick-data-handler

**Why:** Needs instrument IDs to know what market data to download.

**Check Path:**
```
gs://instruments-store-{category}-{project_id}/instrument_availability/by_date/day-{date}/instruments.parquet
```

### 2. strategy-service

**Why:** Needs instrument definitions for strategy configuration.

**Check Path:**
```
gs://instruments-store-{category}-{project_id}/instrument_availability/by_date/day-{date}/instruments.parquet
```

### 3. execution-services

**Why:** Needs instrument specs (tick size, lot size) for execution simulation.

**Check Path:**
```
gs://instruments-store-{category}-{project_id}/instrument_availability/by_date/day-{date}/instruments.parquet
```

## Dependency Check Implementation

### Runtime Validation

```python
from instruments_service.app.core.dependency_checker import DependencyChecker

checker = DependencyChecker()

# Check external API availability
report = checker.check_external_apis(categories=["CEFI", "TRADFI"])

if not report.required_available:
    print("Missing required API keys!")
    for check in report.checks:
        if check.required and not check.available:
            print(f"  - {check.name}: {check.message}")

# Validate service can run
try:
    checker.validate_can_run(categories=["CEFI"])
except RuntimeError as e:
    print(f"Cannot run: {e}")
```

### CLI Check

```bash
# Check if all required APIs are available
python -c "
from instruments_service.app.core.dependency_checker import check_dependencies
report = check_dependencies()
print(f'Required available: {report.required_available}')
for c in report.checks:
    status = '✅' if c.available else '❌'
    print(f'  {status} {c.name}: {c.message}')
"
```

## Execution Order

In the unified trading system deployment:

```
1. instruments-service      ← Root (this service)
2. market-tick-data-handler
3. market-data-processing-service
4. features-*-services
5. ml-*-services
6. strategy-service
7. execution-services       ← Leaf
```

## Infrastructure Dependencies

### GCP Services

| Service | Purpose |
|---------|---------|
| Cloud Storage (GCS) | Output storage |
| Secret Manager | API key storage |
| Cloud Run | Deployment platform |
| Cloud Build | CI/CD |

### Python Packages

| Package | Purpose |
|---------|---------|
| `unified-cloud-services` | Cloud operations |
| `databento` | TradFi API client |
| `ccxt` | Exchange metadata |
| `web3` | Ethereum interactions |

## Failure Modes

### Missing API Keys

If required API keys are missing:
1. Service will fail fast with clear error message
2. Specify which category requires which key
3. Instructions to add key to Secret Manager

### API Rate Limits

External APIs have rate limits:
- Tardis: Contact for limits
- Databento: 5 requests/second
- The Graph: Varies by plan

### Network Failures

- Retry logic with exponential backoff
- Timeout after configurable duration
- Fallback to cached data if available

## Related Documentation

- [GCS_PATHS.md](GCS_PATHS.md) - Output path documentation
- [SECRETS_SETUP.md](SECRETS_SETUP.md) - API key configuration
- [VENUE_ADAPTERS.md](VENUE_ADAPTERS.md) - Adapter implementation details
