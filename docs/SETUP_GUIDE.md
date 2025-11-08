# Setup Guide

> **Related Documentation**:
> - [`QUICK_START.md`](./QUICK_START.md) - Quick start guide
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service overview and architecture
> - [`usage/USAGE_GUIDE.md`](./usage/USAGE_GUIDE.md) - Usage examples after setup
> - [`testing/TESTING.md`](./testing/TESTING.md) - Testing guide
> - [`reference/API_REFERENCE.md`](./reference/API_REFERENCE.md) - Complete API documentation

---

## Prerequisites

- Python 3.9+
- GCP project access
- Access to Secret Manager (for Tardis API key)

## Directory Structure

Both repositories should be cloned as siblings:

```
<your-root>/
├── instruments-service/
│   ├── instruments_service/
│   ├── tests/
│   └── ...
└── unified-cloud-services/
    ├── unified_cloud_services/
    └── ...
```

**Note**: The root directory name doesn't matter. What matters is that both repos are siblings.

## Installation Steps

### 1. Clone Repositories

```bash
git clone <instruments-service-repo-url> instruments-service
git clone <unified-cloud-services-repo-url> unified-cloud-services
```

### 2. Create Virtual Environment

```bash
cd instruments-service
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install unified-cloud-services (Required First)

**IMPORTANT**: `unified-cloud-services` must be installed before `instruments-service`:

```bash
# Install unified-cloud-services with all dependencies
pip install -e ../unified-cloud-services
```

**Important**: Do NOT use `--no-deps` flag. The package requires dependencies like `pydantic-settings`, `google-cloud-storage`, etc. to function properly.

**Note**: If you already have `unified-cloud-services` installed, reinstall it to ensure all dependencies are up to date:

```bash
pip install -e ../unified-cloud-services --force-reinstall
```

### 4. Install instruments-service

```bash
pip install -e .
```

This will automatically install all dependencies from `requirements.txt` and `setup.py`.

**Note**: The `setup.py` includes a note about the `unified-cloud-services` dependency. Make sure to install it first as shown in step 3.

## Credentials Setup

### Automatic Credentials Detection ✅

**Credentials are automatically handled by `unified-cloud-services`** based on the `ENVIRONMENT` variable:

- **Development mode** (`ENVIRONMENT=development`): Auto-detects credentials files in common locations
- **Production mode** (`ENVIRONMENT=production`): Uses VM service account (no credentials file needed)

**Development Mode Auto-Detection**:
The service searches for credentials files in these locations (in order of preference):

1. **Current directory** (where you run the command)
2. **Parent directory**
3. **Grandparent directory** (unified-trading-system-repos root)
4. **Home directory**

It looks for these filenames:
- `central-element-323112-e35fb0ddafe2.json` (project-specific)
- `credentials.json`
- `gcp-credentials.json`
- `service-account.json`

**Simply place your credentials file in any of these locations and the service will find it automatically in development mode!**

### Manual Credentials Setup (Optional)

If you prefer to set credentials manually:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/central-element-323112-e35fb0ddafe2.json
```

Or add to `.env` file (see below).

## Environment Configuration

### Create `.env` File

Create `instruments-service/.env`:

```bash
# GCP Configuration
GOOGLE_APPLICATION_CREDENTIALS=../central-element-323112-e35fb0ddafe2.json
GCP_PROJECT_ID=central-element-323112

# Instruments Service Configuration
INSTRUMENTS_GCS_BUCKET=market-data-tick
INSTRUMENTS_GCS_BUCKET_TEST=market-data-tick-test
INSTRUMENTS_BIGQUERY_DATASET=market_data_hft
BIGQUERY_LOCATION=US

# Development Settings
ENVIRONMENT=development
ENABLE_CSV_SAMPLING=true
CSV_SAMPLE_DIR=./data/samples
```

**Note**: `TARDIS_API_KEY` is NOT needed - the service uses Secret Manager automatically.

## Secret Manager Setup

The service automatically retrieves the Tardis API key from Secret Manager. Ensure:

1. GCP credentials have Secret Manager access
2. Secret `tardis-api-key` exists in your GCP project
3. Service account has `Secret Manager Secret Accessor` role

## Verification

### Test Installation

```bash
python -c "from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService; print('✅ Import successful')"
```

### Test Secret Manager Access

```bash
python -c "from unified_cloud_services import get_secret_with_fallback; key = get_secret_with_fallback('central-element-323112', 'tardis-api-key'); print('✅ Secret Manager access works' if key else '❌ Secret Manager access failed')"
```

### Run Tests

```bash
pytest tests/unit/ -v
```

## Quick Start

Once setup is complete, see `examples/` directory for usage examples and [`QUICK_START.md`](./QUICK_START.md) for a quick overview.



