# Configuration - Instruments Service

This document describes the configuration options, environment variables, and secret management for instruments-service.

## Overview

Instruments-service uses Pydantic `BaseSettings` via `InstrumentsServiceConfig`, extending `UnifiedCloudServicesConfig` from `unified-cloud-services`.

## Config Class Structure

```python
from unified_cloud_services import UnifiedCloudServicesConfig
from pydantic import Field, AliasChoices

class InstrumentsServiceConfig(UnifiedCloudServicesConfig):
    """Configuration for instruments-service."""

    # Category-specific buckets
    instruments_gcs_bucket_cefi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
    )
    instruments_gcs_bucket_tradfi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
    )
    instruments_gcs_bucket_defi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
    )

    # Secret names
    tardis_secret_name: str = Field(
        default="tardis-api-key",
        validation_alias=AliasChoices("TARDIS_SECRET_NAME"),
    )
    databento_secret_name: str = Field(
        default="databento-api-key",
        validation_alias=AliasChoices("DATABENTO_SECRET_NAME"),
    )
    graph_secret_name: str = Field(
        default="graph-api-key",
        validation_alias=AliasChoices("GRAPH_SECRET_NAME"),
    )
```

## Environment Variables

### Required Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | GCP project ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON |

### Bucket Configuration

| Variable | Description |
|----------|-------------|
| `INSTRUMENTS_GCS_BUCKET_CEFI` | CEFI instruments output bucket |
| `INSTRUMENTS_GCS_BUCKET_TRADFI` | TRADFI instruments output bucket |
| `INSTRUMENTS_GCS_BUCKET_DEFI` | DEFI instruments output bucket |
| `INSTRUMENTS_GCS_BUCKET_TEST` | Test bucket (optional) |

### Secret Names

| Variable | Default | Description |
|----------|---------|-------------|
| `TARDIS_SECRET_NAME` | `tardis-api-key` | Tardis API key secret name |
| `DATABENTO_SECRET_NAME` | `databento-api-key` | Databento API key secret name |
| `GRAPH_SECRET_NAME` | `graph-api-key` | The Graph API key secret name |

### DeFi Configuration

| Variable | Description |
|----------|-------------|
| `UNISWAP_V3_GRAPH_URL` | Uniswap V3 subgraph URL |
| `ENVIO_API_URL` | Envio API URL |
| `ALCHEMY_API_KEY` | Alchemy RPC key (optional) |

## Secret Management

### Required Secrets by Category

| Category | Secret Name | Required |
|----------|-------------|----------|
| CEFI | `tardis-api-key` | Yes |
| TRADFI | `databento-api-key` | Yes |
| DEFI | `graph-api-key` | Yes |
| DEFI | `alchemy-api-key` | No |

### Using get_secret_with_fallback()

```python
from unified_cloud_services import get_secret_with_fallback

api_key = get_secret_with_fallback(
    secret_name=config.tardis_secret_name,
    project_id=config.gcp_project_id,
    fallback_env_var="TARDIS_API_KEY",
)
```

### Local Development

For local development, set API keys in `.env`:

```bash
# .env (never commit!)
TARDIS_API_KEY=your-tardis-api-key
DATABENTO_API_KEY=your-databento-api-key
GRAPH_API_KEY=your-graph-api-key
```

## Bucket Naming Patterns

### GCP

```
instruments-store-{category}-{project_id}
```

Examples:
- `instruments-store-cefi-{project_id}`
- `instruments-store-tradfi-{project_id}`
- `instruments-store-defi-{project_id}`

### AWS

```
unified-trading-instruments-{category}-{account_id}
```

## Singleton Pattern

```python
from instruments_service.config import get_config

config = get_config()
project_id = config.gcp_project_id
bucket = config.instruments_gcs_bucket_cefi
```

## Example .env File

```bash
# Cloud Provider
CLOUD_PROVIDER=gcp

# GCP Configuration
GCP_PROJECT_ID={project_id}  # Replace with actual project ID
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Bucket Configuration
INSTRUMENTS_GCS_BUCKET_CEFI=instruments-store-cefi-{project_id}
INSTRUMENTS_GCS_BUCKET_TRADFI=instruments-store-tradfi-{project_id}
INSTRUMENTS_GCS_BUCKET_DEFI=instruments-store-defi-{project_id}

# Secret Names (defaults usually sufficient)
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
GRAPH_SECRET_NAME=graph-api-key

# DeFi URLs
UNISWAP_V3_GRAPH_URL=https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3
```

## Related Documentation

- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error handling patterns
- [DEPENDENCIES.md](DEPENDENCIES.md) - Pipeline position
- [SECRETS_SETUP.md](SECRETS_SETUP.md) - Setting up API keys
