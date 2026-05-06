<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`, `data_status_multi_axis_shard_propagation_2026_05_06.plan.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Secrets Setup Guide

Complete guide for setting up all secrets required by instruments-service.

> **Related Documentation**:
>
> - [`SETUP_GUIDE.md`](./SETUP_GUIDE.md) - Installation and setup
> - [`TESTING.md`](./TESTING.md) - Testing guide

---

## Overview

The instruments-service requires several API keys stored in GCP Secret Manager. This guide covers all secrets setup including:

- GCP credentials and authentication
- GitHub secrets for CI/CD
- External API keys (Tardis, Databento, The Graph, etc.)

---

## 1. GCP Service Account Setup

### Automatic Credentials Detection ✅

**Credentials are automatically handled by `unified-trading-services`** based on the `ENVIRONMENT` variable:

- **Development mode** (`ENVIRONMENT=development`): Auto-detects credentials files in common locations
- **Production mode** (`ENVIRONMENT=production`): Uses VM service account (no credentials file needed)

**Development Mode Auto-Detection**:
The service searches for credentials files in these locations (in order of preference):

1. **Current directory** (where you run the command)
2. **Parent directory**
3. **Grandparent directory** (unified-trading-system-repos root)
4. **Home directory**

It looks for these filenames:

- `{project_id}-e35fb0ddafe2.json` (project-specific, replace {project_id} with actual project ID)
- `credentials.json`
- `gcp-credentials.json`
- `service-account.json`

**Simply place your credentials file in any of these locations and the service will find it automatically in development mode!**

### Manual Credentials Setup (Optional)

If you prefer to set credentials manually:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/{project_id}-e35fb0ddafe2.json  # Replace {project_id} with actual project ID
```

---

## 2. GitHub Secrets (for CI/CD)

### Required: GCP_SERVICE_ACCOUNT_JSON

This is the service account JSON file content needed for GCP authentication in GitHub Actions.

**Steps:**

1. Go to your `instruments-service` repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `GCP_SERVICE_ACCOUNT_JSON`
5. Value: Copy the **entire contents** of `{project_id}-e35fb0ddafe2.json` file (replace {project_id} with actual project ID)
6. Click **Add secret**

### Required: GH_PAT (for private unified-trading-services)

**Note:** GitHub doesn't allow secret names starting with `GITHUB_`, so we use `GH_PAT` instead.

A GitHub Personal Access Token (PAT) with access to the private `unified-trading-services` repository.

**Steps:**

1. Go to GitHub.com → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**
2. Click **Generate new token** → **Generate new token (classic)**
3. Give it a name: `instruments-service-ci`
4. Select expiration (recommend: 90 days or No expiration for automation)
5. Check these scopes:
   - ✅ `repo` (Full control of private repositories)
   - ✅ `read:packages` (Download packages from GitHub Package Registry)
6. Click **Generate token**
7. **Copy the token immediately** (you won't see it again!)
8. Go back to your `instruments-service` repository
9. Navigate to **Settings** → **Secrets and variables** → **Actions**
10. Click **New repository secret**
11. Name: `GH_PAT` (note: cannot start with GITHUB\_)
12. Value: Paste the token you copied
13. Click **Add secret**

### Optional GitHub Secrets (with defaults)

These secrets have defaults built into the workflow:

- `GCP_PROJECT_ID` (default: `{project_id}` - replace with actual project ID)
- `GCS_REGION` (default: `asia-northeast1-c`)
- `GCS_LOCATION` (default: `asia-northeast1`)
- `INSTRUMENTS_GCS_BUCKET` (default: `instruments-store-{project_id}`)
- `INSTRUMENTS_GCS_BUCKET_TEST` (default: `instruments-store-test-{project_id}`)
- `INSTRUMENTS_GCS_BUCKET_CEFI` (default: `instruments-store-cefi-{project_id}`)
- `INSTRUMENTS_GCS_BUCKET_TRADFI` (default: `instruments-store-tradfi-{project_id}`)
- `INSTRUMENTS_GCS_BUCKET_DEFI` (default: `instruments-store-defi-{project_id}`)
- `INSTRUMENTS_BIGQUERY_DATASET` (default: `instruments`)
- `BIGQUERY_LOCATION` (default: `asia-northeast1`)

---

## 3. API Keys in GCP Secret Manager

All API keys are retrieved from Secret Manager (not environment variables).

### Creating Secrets

```bash
# Tardis API key (CeFi crypto data)
echo "YOUR_TARDIS_API_KEY" | gcloud secrets create tardis-api-key \
  --project={project_id} \
  --data-file=-

# Databento API key (TradFi data)
echo "YOUR_DATABENTO_API_KEY" | gcloud secrets create databento-api-key \
  --project={project_id} \
  --data-file=-

# The Graph API key (DeFi subgraphs)
echo "YOUR_GRAPH_API_KEY" | gcloud secrets create graph-api-key \
  --project={project_id} \
  --data-file=-

# Alchemy API key (DeFi RPC)
echo "YOUR_ALCHEMY_API_KEY" | gcloud secrets create alchemy-api-key \
  --project={project_id} \
  --data-file=-

# Envio API key (Uniswap V4 fallback)
echo "YOUR_ENVIO_API_KEY" | gcloud secrets create envio-api-key \
  --project={project_id} \
  --data-file=-

# AaveScan API key (AAVE fallback - optional)
echo "YOUR_AAVESCAN_API_KEY" | gcloud secrets create aavescan-api-key \
  --project={project_id} \
  --data-file=-
```

### Adding New Version to Existing Secret

```bash
echo "NEW_API_KEY" | gcloud secrets versions add graph-api-key \
  --project={project_id} \
  --data-file=-
```

### Verify Secrets

```bash
# List all secrets
gcloud secrets list --project={project_id}

# View secret metadata
gcloud secrets describe tardis-api-key --project={project_id}

# Test secret access
python3 -c "
from unified_trading_services import get_secret_client
api_key = get_secret_client(
    project_id='{project_id}',  # Replace {project_id} with actual project ID
    secret_name='tardis-api-key',
    fallback_env_var='TARDIS_API_KEY',
)
print(f'✅ Retrieved: {api_key[:10]}...' if api_key else '❌ Not found')
"
```

---

## 4. Environment Variables

Configure secret names (not the actual keys!) in `.env`:

```bash
# GCP Configuration
GOOGLE_APPLICATION_CREDENTIALS=../{project_id}-e35fb0ddafe2.json  # Replace {project_id} with actual project ID
GCP_PROJECT_ID={project_id}  # Replace with actual project ID

# Secret Manager secret names (actual keys stored in GCP Secret Manager)
TARDIS_SECRET_NAME=tardis-api-key
DATABENTO_SECRET_NAME=databento-api-key
THEGRAPH_SECRET_NAME=graph-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
ENVIO_SECRET_NAME=envio-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key

# Environment
ENVIRONMENT=development
```

**⚠️ Never commit actual API keys to `.env` files!**

---

## 5. API Key Sources

| Secret              | Provider  | Get Key From                     | Purpose          | Pricing                 |
| ------------------- | --------- | -------------------------------- | ---------------- | ----------------------- |
| `tardis-api-key`    | Tardis    | https://tardis.dev               | CeFi crypto data | Subscription-based      |
| `databento-api-key` | Databento | https://databento.com            | TradFi data      | Pay-per-use             |
| `graph-api-key`     | The Graph | https://thegraph.com/studio      | DeFi subgraphs   | **$2 per 100k queries** |
| `alchemy-api-key`   | Alchemy   | https://dashboard.alchemy.com    | DeFi RPC         | Pay-as-you-go           |
| `envio-api-key`     | Envio     | https://envio.dev/app/api-tokens | Uniswap V4       | Free tier available     |
| `aavescan-api-key`  | AaveScan  | https://aavescan.com             | AAVE fallback    | Free                    |

### The Graph Billing Notes

- **Free tier**: 100k queries/month per API key
- **Paid tier**: $2 per 100k queries (billed in GRT tokens on Arbitrum)
- **Billing portal**: https://thegraph.com/studio/billing/
- **Adding funds**: Deposit GRT tokens to your API key wallet on Arbitrum One
- **Subgraphs used**: Uniswap V2/V3/V4, AAVE V3, Balancer, Curve

---

## 6. Troubleshooting

### "Failed to retrieve API key from Secret Manager"

1. Check secret exists: `gcloud secrets list`
2. Verify secret name matches `.env` config
3. Check GCP credentials: `GOOGLE_APPLICATION_CREDENTIALS`
4. Verify project ID: `GCP_PROJECT_ID`

### "Permission denied"

Ensure service account has `Secret Manager Secret Accessor` role:

```bash
gcloud projects add-iam-policy-binding {project_id} \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT@{project_id}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### "Secret not found"

Create the secret:

```bash
echo "YOUR_API_KEY" | gcloud secrets create SECRET_NAME \
  --project={project_id} \
  --data-file=-
```

---

## 7. Quick Reference

### Required Secrets

- ✅ `tardis-api-key` - Required for CeFi crypto
- ✅ `databento-api-key` - Required for TradFi
- ✅ `graph-api-key` - Required for DeFi (The Graph)
- ✅ `alchemy-api-key` - Required for DeFi (RPC)

### Optional Secrets

- ⚠️ `envio-api-key` - Optional (Uniswap V4 fallback)
- ⚠️ `aavescan-api-key` - Optional (AAVE fallback)

---

## 8. CI/CD Workflows

The repository has two GitHub Actions workflows in `.github/workflows/`:

### Quality Gates Workflow (`quality-gates.yml`)

Runs automatically on:

- Push to `main` branch
- Pull requests to `main` branch

**What it does:**

1. Sets up Python 3.13
2. Creates GCP credentials from `GCP_SERVICE_ACCOUNT_JSON` secret
3. Clones `unified-trading-services` (sibling repo)
4. Runs `scripts/run_quality_gates.py` with 65% coverage threshold
5. Uploads coverage reports as artifacts

**Testing locally before push:**

```bash
python scripts/run_quality_gates.py --coverage-threshold 65
```

**Manual trigger:** Not supported (runs on push/PR only)

### Publish Package Workflow (`publish-package.yml`)

Runs on:

- GitHub Release creation
- Tag push (e.g., `v0.1.0`)
- Manual workflow dispatch

**Publishing a new version:**

```bash
# Option 1: Create a GitHub release (triggers workflow automatically)
gh release create v1.0.0 --title "v1.0.0" --notes "Release notes"

# Option 2: Push a version tag
git tag v1.0.0
git push origin v1.0.0

# Option 3: Manual trigger via GitHub UI
# Go to Actions → Publish to GitHub Packages → Run workflow
```

**Installing from GitHub:**

```bash
# Install directly from GitHub repo
pip install git+https://x-access-token:$GH_PAT@github.com/IggyIkenna/instruments-service.git

# Or with SSH
pip install git+ssh://git@github.com/IggyIkenna/instruments-service.git
```

**Note:** GitHub Packages doesn't support Python/PyPI packages directly. Use the git install method above.

---

## 9. Setting Up CI/CD for New Repos

To replicate this CI/CD setup in another repository:

### Step 1: Copy Workflows

```bash
cp -r .github/workflows/ ../new-repo/.github/workflows/
```

### Step 2: Add Required GitHub Secrets

| Secret                     | Required    | Description                                     |
| -------------------------- | ----------- | ----------------------------------------------- |
| `GCP_SERVICE_ACCOUNT_JSON` | ✅ Yes      | Full contents of GCP service account JSON file  |
| `GH_PAT`                   | ✅ Yes      | GitHub PAT with `repo` + `read:packages` scopes |
| `SSH_PRIVATE_KEY`          | ⚠️ Optional | SSH key for git clone (alternative to PAT)      |

### Step 3: Ensure unified-trading-services is accessible

The workflow clones `unified-trading-services` from GitHub. Ensure:

- Your `GH_PAT` has access to the `unified-trading-services` repo
- Or set up `SSH_PRIVATE_KEY` for SSH access

### Step 4: Update repo-specific values in workflow

Edit `quality-gates.yml`:

- Line 91: Update repo URL `git@github.com:IggyIkenna/unified-trading-services.git`
- Line 262: Adjust coverage threshold `--coverage-threshold 65`

### Step 5: Configure branch protection (optional)

Go to Settings → Branches → Add rule:

- Branch name pattern: `main`
- ✅ Require status checks to pass before merging
- Select "quality-gates" as required check

---

## 10. Verifying CI/CD Setup

### Test Quality Gates Locally

```bash
# Run the same script CI uses
python scripts/run_quality_gates.py --coverage-threshold 65 --skip-performance
```

### Verify GitHub Secrets Are Set

Go to repository Settings → Secrets and variables → Actions

You should see:

- ✅ `GCP_SERVICE_ACCOUNT_JSON` (required)
- ✅ `GH_PAT` (required for private repos)

### Test Workflow Manually

```bash
# Trigger a test run by pushing to main
git add .
git commit -m "Test quality gates"
git push origin main

# Check Actions tab: https://github.com/IggyIkenna/instruments-service/actions
```

---

_Last Updated: December 2025_
