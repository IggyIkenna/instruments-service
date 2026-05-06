<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Setup Guide

Complete setup and installation guide for instruments-service.

> **Related Documentation**:
>
> - [`SECRETS_SETUP.md`](./SECRETS_SETUP.md) - API keys and secrets setup
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service overview and architecture
> - [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples after setup
> - [`TESTING.md`](./TESTING.md) - Testing guide

---

## Quick Start (5 minutes)

### Prerequisites

- Python 3.9+
- GCP project access
- Access to Secret Manager (for API keys)

### Installation

```bash
# 1. Clone repositories (as siblings)
git clone <instruments-service-repo-url> instruments-service
git clone <unified-trading-services-repo-url> unified-trading-services

# 2. Create virtual environment
cd instruments-service
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install unified-trading-services first (required)
pip install -e ../unified-trading-services

# 4. Install instruments-service
pip install -e .
```

### Credentials Setup

**Automatic**: Place your GCP credentials file (`{project_id}-e35fb0ddafe2.json`, replace {project_id} with actual project ID) in:

- Current directory
- Parent directory
- Grandparent directory (unified-trading-system-repos root)
- Home directory

The service will automatically detect it in development mode.

**Manual**: Set environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

### Quick Test

```bash
# Test import
python -c "from instruments_service import InstrumentProcessingService; print('✅ Import successful')"

# Test Secret Manager access
python -c "from unified_trading_services import get_secret_client; key = get_secret_client('{project_id}', 'tardis-api-key'); print('✅ Secret Manager works' if key else '❌ Secret Manager failed')"  # Replace {project_id} with actual project ID
```

### Generate Instruments

```bash
# Generate instruments for a date range
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24

# With force flag (regenerate even if exists)
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24 --force

# Filter by category
python -m instruments_service --mode instruments --start-date 2023-05-23 --CEFI
python -m instruments_service --mode instruments --start-date 2023-05-23 --TRADFI
python -m instruments_service --mode instruments --start-date 2023-05-23 --DEFI
```

---

## Detailed Setup

### Directory Structure

Both repositories should be cloned as siblings:

```
<your-root>/
├── instruments-service/
│   ├── instruments_service/
│   ├── tests/
│   └── ...
└── unified-trading-services/
    ├── unified_trading_services/
    └── ...
```

**Note**: The root directory name doesn't matter. What matters is that both repos are siblings.

### Step 1: Clone Repositories

```bash
git clone <instruments-service-repo-url> instruments-service
git clone <unified-trading-services-repo-url> unified-trading-services
```

### Step 2: Create Virtual Environment

```bash
cd instruments-service
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install unified-trading-services (Required First)

**IMPORTANT**: `unified-trading-services` must be installed before `instruments-service`:

```bash
# Install unified-trading-services with all dependencies
pip install -e ../unified-trading-services
```

**Important**: Do NOT use `--no-deps` flag. The package requires dependencies like `pydantic-settings`, `google-cloud-storage`, etc. to function properly.

**Note**: If you already have `unified-trading-services` installed, reinstall it to ensure all dependencies are up to date:

```bash
pip install -e ../unified-trading-services --force-reinstall
```

### Step 4: Install instruments-service

```bash
pip install -e .
```

This will automatically install all dependencies from `requirements.txt` and `setup.py`.

---

## Environment Configuration

### Create `.env` File

Create `instruments-service/.env`:

```bash
# GCP Configuration
GOOGLE_APPLICATION_CREDENTIALS=../{project_id}-e35fb0ddafe2.json  # Replace {project_id} with actual project ID
GCP_PROJECT_ID={project_id}  # Replace with actual project ID

# Instruments Service Configuration
INSTRUMENTS_GCS_BUCKET=instruments-store-{project_id}
INSTRUMENTS_GCS_BUCKET_TEST=instruments-store-test-{project_id}
INSTRUMENTS_BIGQUERY_DATASET=instruments
BIGQUERY_LOCATION=asia-northeast1

# Development Settings
ENVIRONMENT=development
ENABLE_CSV_SAMPLING=true
CSV_SAMPLE_DIR=./data/samples
```

**Note**: API keys are NOT stored in `.env` - the service uses Secret Manager automatically.

### Secret Manager Setup

The service automatically retrieves API keys from Secret Manager. Ensure:

1. GCP credentials have Secret Manager access
2. Required secrets exist in your GCP project
3. Service account has `Secret Manager Secret Accessor` role

See [`SECRETS_SETUP.md`](./SECRETS_SETUP.md) for complete secrets configuration.

---

## Verification

### Test Installation

```bash
python -c "from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService; print('✅ Import successful')"
```

### Test Secret Manager Access

```bash
python -c "from unified_trading_services import get_secret_client; key = get_secret_client('{project_id}', 'tardis-api-key'); print('✅ Secret Manager access works' if key else '❌ Secret Manager access failed')"  # Replace {project_id} with actual project ID
```

### Run Tests

```bash
pytest tests/unit/ -v
```

---

## Query Instruments

### Using CLI (Recommended)

```bash
# List instruments for a date (default: summary format)
python -m instruments_service --mode instruments-query --start-date 2023-05-23

# Filter by venue and instrument type
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --venues BINANCE-FUTURES --instrument-types PERPETUAL

# Get instrument details
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --query-type details --instrument-id BINANCE-FUTURES:PERPETUAL:BTC-USDT

# Export to JSON (prints to stdout)
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --output-format json

# Export to CSV file
python -m instruments_service --mode instruments-query --start-date 2023-05-23 \
    --output-format csv --output-file instruments.csv
```

### Using Python

```python
from unified_trading_services import create_instruments_client

client = create_instruments_client()
instruments_df = client.get_instruments_for_date(
    date='2023-05-23',
    venue='BINANCE-FUTURES',
    instrument_type='PERPETUAL'
)
```

---

## Next Steps

- **[USAGE_GUIDE.md](./USAGE_GUIDE.md)** - Comprehensive usage guide
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Architecture documentation
- **[API_REFERENCE.md](./API_REFERENCE.md)** - Complete API reference
- **[SECRETS_SETUP.md](./SECRETS_SETUP.md)** - API keys and secrets setup

---

_Last Updated: December 2025_
