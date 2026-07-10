# Setup Guide

Complete setup, secrets, cloud-operations, and testing walkthrough for `instruments-service`: environment setup →
secrets/API keys → cloud config → running tests.

---

## 1. Prerequisites

- **Python 3.13** (`requires-python = ">=3.13,<3.14"` in `pyproject.toml`) — the service pins to the 3.13.x line
  only; a different interpreter minor version will fail `scripts/setup.sh`'s version check.
- [`uv`](https://github.com/astral-sh/uv) (the only allowed `pip install` in this workspace; `scripts/setup.sh`
  bootstraps it if missing).
- GCP project access + `gcloud` CLI (for Secret Manager and, if you don't use ADC, service-account credentials).
- macOS (Apple Silicon): ARM64 Python is required — `scripts/setup.sh` rejects an x86_64 (Rosetta) interpreter.

---

## 2. Quick Start (5 minutes)

```bash
# 1. Clone as siblings under one workspace root (see Directory Structure below)
git clone <instruments-service-repo-url> instruments-service
git clone <unified-trading-library-repo-url> unified-trading-library
git clone <unified-api-contracts-repo-url> unified-api-contracts

# 2. Canonical setup — idempotent, handles venv + sibling deps + project install
cd instruments-service
bash scripts/setup.sh

# 3. Authenticate to GCP (local dev default: ADC, no key file needed)
gcloud auth application-default login

# 4. Verify
python -c "from instruments_service import InstrumentProcessingService; print('Import OK')"

# 5. Generate instruments for a date range
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24
```

---

## 3. Detailed Setup

### 3.1 Directory Structure

`instruments-service` depends on two sibling repos, declared as **editable path dependencies** in `pyproject.toml`:

```toml
[tool.uv.sources.unified-trading-library]
path = "../unified-trading-library"
editable = true

[tool.uv.sources.unified-api-contracts]
path = "../unified-api-contracts"
editable = true
```

So the three repos must be cloned as siblings:

```
<workspace-root>/
├── instruments-service/
├── unified-trading-library/      # UTL — cloud/secrets/events/config primitives
└── unified-api-contracts/        # UAC — shared schemas/contracts
```

Every import in `instruments_service/` is `from unified_trading_library import ...` (UTL) or
`from unified_api_contracts import ...` (UAC) for shared schema types.

### 3.2 Canonical Setup Script

**`bash scripts/setup.sh`** is the single source of truth for environment setup — idempotent, safe to re-run. It:

1. Detects repo type (Python, since `pyproject.toml` is present).
2. Validates the Python version (`>=3.13,<3.14`) and (macOS only) rejects a Rosetta/x86_64 interpreter.
3. Bootstraps `uv` if missing.
4. Creates/recreates `.venv` if missing or version-mismatched.
5. Runs `uv lock`.
6. Installs sibling repos as editables, reading `unified-trading-pm/workspace-manifest.json` (the workspace SSOT for
   sibling paths) — you do **not** need to run `uv pip install -e ../unified-trading-library` yourself.
7. Installs the project + dev deps (`uv pip install -e .`).
8. Re-pins the sibling deps back to their local editable checkouts (a plain `-e .` install can otherwise resolve
   `unified-trading-library`/`unified-api-contracts` as version-pinned wheels from Artifact Registry and silently
   shadow your local sibling checkouts).
9. Runs `uv sync` to apply the full lock file (transitive deps).

Flags:

```bash
bash scripts/setup.sh              # Full setup (idempotent)
bash scripts/setup.sh --check      # Verify setup without making changes
bash scripts/setup.sh --force      # Force reinstall (ignores the stamp cache)
bash scripts/setup.sh --isolated   # Standalone setup, no workspace sibling deps
```

`--isolated` is for running this repo outside the multi-repo workspace; in that mode you install
`unified-trading-library`/`unified-api-contracts` from Artifact Registry yourself
(`uv pip install <dep>`) instead of from local sibling checkouts.

`scripts/setup.sh` owns the entire setup flow, including the editable re-pinning — prefer it over any hand-rolled
`uv venv` / `uv pip install -e ...` sequence.

---

## 4. Environment Configuration

Copy `.env.example` → `.env` (never commit `.env`). Key fields from `instruments-service/.env.example`:

```bash
ENVIRONMENT=development
enable_ccxt_integration=true
enable_metadata_caching=true
cache_ttl_hours=24
max_batch_size=1000
lookback_days=0

CLOUD_PROVIDER=gcp
GCP_PROJECT_ID=your-gcp-project-id

GCS_REGION=asia-northeast1-c
GCS_LOCATION=asia-northeast1

# Category-specific buckets (NOT a single INSTRUMENTS_GCS_BUCKET)
INSTRUMENTS_GCS_BUCKET_CEFI=instruments-store-cefi-your-gcp-project-id
INSTRUMENTS_GCS_BUCKET_TRADFI=instruments-store-tradfi-your-gcp-project-id
INSTRUMENTS_GCS_BUCKET_DEFI=instruments-store-defi-your-gcp-project-id
INSTRUMENTS_GCS_BUCKET_CEFI_TEST=instruments-store-test-cefi-your-gcp-project-id
INSTRUMENTS_GCS_BUCKET_TRADFI_TEST=instruments-store-test-tradfi-your-gcp-project-id
INSTRUMENTS_GCS_BUCKET_DEFI_TEST=instruments-store-test-defi-your-gcp-project-id

INSTRUMENTS_BIGQUERY_DATASET=instruments
BIGQUERY_LOCATION=asia-northeast1

ENABLE_CSV_SAMPLING=true
CSV_SAMPLE_SIZE=20000
CSV_SAMPLE_DIR=./data/samples

# Secret Manager secret NAMES only — never the actual key values
TARDIS_SECRET_NAME=tardis-api-key-full
TARDIS_FULL_SECRET_NAME=tardis-api-key-full
DATABENTO_SECRET_NAME=databento-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
GRAPH_SECRET_NAME=thegraph-api-key
IBKR_CREDENTIALS_SECRET_NAME=ibkr-account-credentials

GH_PAT=
```

Buckets are per-asset-group (`_CEFI`/`_TRADFI`/`_DEFI`, each with a `_TEST` variant) — see §6.
`GOOGLE_APPLICATION_CREDENTIALS` is not in the template; ADC is the default local-dev credentials path (§5).

---

## 5. Credentials & Authentication

`InstrumentsServiceConfig` (in `instruments_service/config/service_config.py`) subclasses UTL's
`UnifiedCloudConfig` — a pydantic-settings model that reads `.env`. Credential resolution is handled entirely by
UTL/UCI (`unified_trading_library` / the cloud-interface layer beneath it), not by any file-search logic inside
instruments-service:

- **Local dev (default)**: use Application Default Credentials — `gcloud auth application-default login`. No key
  file needed.
- **Explicit override**: `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json` — UTL's
  `get_credentials_path()` reads this env var directly and nothing else.
- **Production**: the VM's attached service account is used; no credentials file involved.

There is no multi-location file search (current directory / parent / grandparent / home) for a credentials file —
`unified_trading_library`'s `get_credentials_path()` is a one-line read of `GOOGLE_APPLICATION_CREDENTIALS`. Use ADC
for local dev rather than dropping a key file in a parent directory.

---

## 6. Secrets & API Keys

### 6.1 Principle: Secret Manager only, never `os.getenv` for API keys

All API keys live in GCP Secret Manager — never hardcoded, never read from a bare environment variable in service
code. `.env`/config only ever holds **secret names** (which secret to fetch), not secret **values**.

### 6.2 Resolving a secret in code

The current UTL API (`unified_trading_library`) is a no-arg cached client with a `.get_secret(name)` method, or a
one-line convenience function:

```python
from unified_trading_library import get_secret_client

sc = get_secret_client()                 # cached, auto-detects GCP/AWS provider
api_key = sc.get_secret("alchemy-api-key")
```

```python
from unified_trading_library import get_secret

api_key = get_secret("tardis-api-key-full")   # raises RuntimeError if missing — fails loud
```

Never resolve the secret **name** by hand — pull it from config:

```python
from instruments_service.config import instruments_config
from unified_trading_library import get_secret_client

sc = get_secret_client()
api_key = sc.get_secret(instruments_config.tardis_secret_name)
```

`get_secret_client()` takes no required args and returns a **client object**; call `.get_secret(secret_name)` on it
to fetch a value. There is no `fallback_env_var` kwarg.

### 6.3 Config fields (inherited from UTL's `UnifiedCloudConfig`)

`InstrumentsServiceConfig` inherits these secret-name fields from `UnifiedCloudConfig` — override the env var to
point at a differently-named secret, never hardcode the secret name in code:

| Config field                   | Env var (aliases)                             | Default secret name        | Used for            |
| ------------------------------ | --------------------------------------------- | -------------------------- | ------------------- |
| `tardis_secret_name`           | `TARDIS_SECRET_NAME`                          | `tardis-api-key`           | CEFI (Tardis)       |
| `tardis_full_secret_name`      | `TARDIS_FULL_SECRET_NAME`                     | `tardis-api-key-full`      | CEFI (Tardis, full) |
| `databento_secret_name`        | `DATABENTO_SECRET_NAME`                       | `databento-api-key`        | TRADFI (Databento)  |
| `thegraph_secret_name`         | `THEGRAPH_SECRET_NAME` or `GRAPH_SECRET_NAME` | `thegraph-api-key`         | DEFI (The Graph)    |
| `alchemy_secret_name`          | `ALCHEMY_SECRET_NAME`                         | `alchemy-api-key`          | DEFI (Alchemy RPC)  |
| `aavescan_secret_name`         | `AAVESCAN_SECRET_NAME`                        | `aavescan-api-key`         | AAVE fallback       |
| `ibkr_credentials_secret_name` | `IBKR_CREDENTIALS_SECRET_NAME`                | `ibkr-account-credentials` | TRADFI (IBKR)       |

`GRAPH_SECRET_NAME` works as a back-compat env-var alias for `thegraph_secret_name`; the canonical env var is
`THEGRAPH_SECRET_NAME` (both resolve to the same field, default secret name `thegraph-api-key`).

Instruments-service adds no service-specific secret-name overrides beyond these; if you need one, add it to
`UnifiedCloudConfig` upstream (in `unified-trading-library`), not to `InstrumentsServiceConfig`.

### 6.4 Creating / managing secrets in GCP

```bash
# Create a new secret
echo "YOUR_API_KEY" | gcloud secrets create tardis-api-key-full --project=<project-id> --data-file=-

# Add a new version to an existing secret
echo "NEW_API_KEY" | gcloud secrets versions add thegraph-api-key --project=<project-id> --data-file=-

# List / inspect
gcloud secrets list --project=<project-id>
gcloud secrets describe tardis-api-key-full --project=<project-id>
```

Ensure the calling service account has the `roles/secretmanager.secretAccessor` IAM role:

```bash
gcloud projects add-iam-policy-binding <project-id> \
  --member="serviceAccount:<sa>@<project-id>.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### 6.5 API key sources & pricing

| Secret                               | Provider  | Get key from                  | Purpose          | Pricing                 |
| ------------------------------------ | --------- | ----------------------------- | ---------------- | ----------------------- |
| `tardis-api-key-full`                | Tardis    | https://tardis.dev            | CeFi crypto data | Subscription-based      |
| `databento-api-key`                  | Databento | https://databento.com         | TradFi data      | Pay-per-use             |
| `graph-api-key` / `thegraph-api-key` | The Graph | https://thegraph.com/studio   | DeFi subgraphs   | **$2 per 100k queries** |
| `alchemy-api-key`                    | Alchemy   | https://dashboard.alchemy.com | DeFi RPC         | Pay-as-you-go           |
| `aavescan-api-key`                   | AaveScan  | https://aavescan.com          | AAVE fallback    | Free                    |
| `ibkr-account-credentials`           | IBKR      | (IBKR account portal)         | TradFi (IBKR)    | Brokerage account       |

**The Graph billing**: free tier 100k queries/month; paid tier $2/100k queries, billed in GRT on Arbitrum One
(top up your API key's wallet at https://thegraph.com/studio/billing/). Subgraphs used: Uniswap V2/V3/V4, AAVE V3,
Balancer, Curve.

### 6.6 Which mode needs which keys

| Mode / category           | Required                                                                            | Optional             |
| ------------------------- | ----------------------------------------------------------------------------------- | -------------------- |
| CEFI (Tardis/CCXT venues) | Tardis                                                                              | —                    |
| TRADFI (Databento venues) | Databento                                                                           | IBKR (if using IBKR) |
| DEFI                      | The Graph                                                                           | Alchemy, AaveScan    |
| Sports (per venue)        | api_football, footystats, transfermarkt, soccer_football_info, open_meteo (as used) | understat (keyless)  |
| Corporate actions only    | None (yfinance, exchange-calendars)                                                 | —                    |

Only fetch the keys the requested run actually needs — don't require Tardis for a TRADFI-only or DEFI-only run.
`DataSourceMapping.get_required_secrets(venues)` (`unified_api_contracts.canonical.canonical_mappings`, a thin
classmethod wrapper over the module-level `get_required_secrets()`/`DATA_SOURCE_TO_SECRET`) resolves the
venue→secret-name mapping; UTL's `ApiKeyReloader` (`unified_trading_library/api_key_reloader.py`, backed by
`validate_api_keys_for_venues()` in `unified_trading_library/startup_validation.py`) calls it to fetch + hot-reload
only the keys the active venues need. instruments-service wires this up in
`instruments_service/cli/instruments_handler.py`'s `_start_key_reloader()`. Per-venue Sports secret names (from
`DATA_SOURCE_TO_SECRET`): `api_football` → `api-football-api-key`, `footystats` → `footystats-api-key`,
`soccer_football_info` → `soccer-football-info-api-key`, `transfermarkt` → `transfermarkt-api-key`, `open_meteo` →
`open-meteo-api-key` (the adapter also works keyless against Open-Meteo's free tier — the secret only unlocks the
Pro-tier customer endpoints); `understat` has no secret entry (`None` in `DATA_SOURCE_TO_SECRET`) — always keyless.

### 6.7 Adding a new API key

1. Add `{service}_secret_name` to `UnifiedCloudConfig` in `unified-trading-library` (with an `AliasChoices` env-var
   binding), not to `InstrumentsServiceConfig` directly.
2. Add the env var to `.env.example` here.
3. Create the secret in GCP Secret Manager (§6.4).
4. Resolve it via `get_secret_client().get_secret(config.{service}_secret_name)` at the call site — never
   `os.environ.get`.
5. Use context7 ("use context7") when integrating a new market-data API to confirm the actual required credential
   shape before wiring it up.

### 6.8 Troubleshooting

**"Failed to retrieve API key from Secret Manager"**

1. `gcloud secrets list --project=<project-id>` — confirm the secret exists.
2. Confirm the `.env` secret-name var matches the real secret name.
3. Confirm `GCP_PROJECT_ID` is set and `gcloud auth application-default login` has run (or
   `GOOGLE_APPLICATION_CREDENTIALS` points at a valid file).

**"Permission denied"** — grant `roles/secretmanager.secretAccessor` (§6.4).

**"Secret not found"** — create it (§6.4).

---

## 7. Cloud Operations

### 7.1 GCS bucket resolution

Buckets are resolved via UTL's `resolve_bucket_name`, never hardcoded or built with an inline `gs://` string:

```python
from unified_trading_library import resolve_bucket_name, get_storage_client

bucket = resolve_bucket_name(cloud="gcp", kind="instruments", asset_group="cefi")
# -> "instruments-store-cefi-<gcp_project_id>"

storage = get_storage_client()
storage.upload_bytes(bucket, "path/to/file.parquet", data)
data = storage.download_bytes(bucket, "path/to/file.parquet")
```

GCS object-level operations (copy/delete/describe) go through UTL helpers
(`gcs_copy_object` / `gcs_delete_object` / `gcs_describe_object`) — never a subprocess call to `gcloud`/`gsutil`.
`resolve_bucket_name(...)` + `get_storage_client()` (both from `unified_trading_library`) are the only pattern used
in production `instruments_service/` code. `CloudTarget` (`unified_trading_library.domain_client.cloud_target`)
still exists but is only used in test fixtures (`tests/conftest.py`) to provision test buckets/datasets, not in
production code; `StandardizedDomainCloudService` has zero references anywhere in this repo.

### 7.2 Config anti-pattern

```python
# WRONG — no os.getenv()/get_config-with-defaults for API keys or project config
import os
project_id = os.environ.get("GCP_PROJECT_ID", "fallback")

# CORRECT — use the typed config object's attributes directly
from instruments_service.config import instruments_config
project_id = instruments_config.gcp_project_id
```

This is enforced by the repo's quality gates (`scripts/quality-gates.sh` bans `os.getenv()`/`os.environ.get()`
outside a small allow-listed "config-bootstrap" layer inside UTL itself).

---

## 8. Running Tests & Quality Gates

### 8.1 Test layout

```
tests/
├── unit/          # 171 test files — no credentials, no network
├── integration/   # library-contract tests: verify imported symbols/API shape
├── e2e/           # writes real data; requires IS_TEST_RUN=true + a real GCP project
├── smoke/
├── fixtures/
├── reference_data/
└── scripts/
```

Pytest markers (`pyproject.toml` `[tool.pytest.ini_options]`):

- `unit` — plain unit test
- `integration` — library-contract test (imported symbols exist + expected API shape); runs in CI, no credentials
- `e2e` — end-to-end, writes real data; requires `IS_TEST_RUN=true` and a real GCP project
- `live` — needs live external API access / real GCP credentials; skipped in CI
- `smoke`

### 8.2 Running the gate locally

**Canonical**: `bash scripts/quality-gates.sh` — this is what CI and Cloud Build both invoke; do not run `pytest`
directly.

Useful flags:

```bash
bash scripts/quality-gates.sh --quick               # fast iteration (skips heavier profiling passes)
bash scripts/quality-gates.sh --no-fix              # don't auto-reformat the tree (safe default; the agent path)
bash scripts/quality-gates.sh --fix                 # deliberate tree-wide reformat (ruff --fix, prettier --write)
bash scripts/quality-gates.sh --skip-typecheck       # skip basedpyright
bash scripts/quality-gates.sh --lint                # lint-only (no tests)
bash scripts/quality-gates.sh --test                # tests-only (no lint)
bash scripts/quality-gates.sh --skip-tests | --skip-lint | --skip-codex | --skip-version-alignment
```

Coverage floor is a **ratcheted, moving target**, not a fixed constant — check `MIN_COVERAGE` at the top of
`scripts/quality-gates.sh` for the live number (currently `88`, tracked in `[tool.coverage.report] fail_under` in
`pyproject.toml` too). `RUN_INTEGRATION=false` for this repo, so `tests/integration/` is **not** part of the default
local/CI run by default — run it explicitly with `pytest -m integration` if you need to exercise it.

The local entry point is the bash script `scripts/quality-gates.sh`, whose body is
`unified-trading-pm/scripts/quality-gates-base/base-service.sh` — a shared harness across all services, not a
per-repo Python script. Read the script for the live coverage floor rather than hardcoding a number in a doc.

### 8.3 CI / Cloud Build alignment

Local (`scripts/quality-gates.sh`), GitHub Actions, and Cloud Build all invoke the **same** underlying gate logic
(the shared `base-service.sh` harness) rather than three hand-maintained copies of a pytest command line — so
"keeping 3 environments in sync" is now largely automatic rather than a manual-parity checklist. Concretely:

- **GitHub Actions**: `.github/workflows/quality-gates-v2.yml` — the required check is `quality-gates-v2` (not
  `quality-gates`). It also declares `dep_repos: "unified-trading-library unified-api-contracts"` so CI clones both
  sibling repos before running `uv sync`.
- **Cloud Build**: `cloudbuild.yaml` — required steps include `quality-gates` (re-runs the gate inside the built
  image) and `scan-check` (CVE gate) before `push`.

Test selection is marker-based (`live`, `integration`, etc. — see §8.1), with `pytest-socket`'s network isolation
enforcing that unit tests can't reach the network unless explicitly allow-listed.

---

## 9. CI/CD Reference

### 9.1 Workflows in this repo

`.github/workflows/` currently has (beyond `quality-gates-v2.yml`): `agent-audit.yml`, `image-build-gate.yml`,
`main-backmerge-to-ldr.yml`, `major-bump-issue-handler.yml`, `plan-alignment-agent.yml`, `publish-package.yml`,
`request-major-bump.yml`, `semver-agent.yml`, `staging-backmerge-to-ldr.yml`, `staging-lock-check.yml`,
`update-dependency-version.yml`, `version-registry-notify.yml`. Most of these are shared workspace-fleet automation
(semver, plan-alignment, backmerge) rather than instruments-service-specific — don't hand-edit a per-repo copy;
changes to the shared templates roll out from `unified-trading-pm`.

### 9.2 Required GitHub secrets

- **`GH_PAT`** — used across most of the workflows above (cross-repo dispatch/escalation, cloning private sibling
  repos in CI). GitHub doesn't allow a secret named starting with `GITHUB_`, hence `GH_PAT` not `GH_TOKEN`.
- `GCP_SERVICE_ACCOUNT_JSON` is **not referenced in any current workflow file in this repo** — GCP auth for Cloud
  Build steps is handled by the Cloud Build service account itself, not a GitHub Actions secret. This section may
  not be authoritative for an org fork with a differently-wired shared/reusable workflow — verify against that
  workflow directly rather than this doc if you're on a fork.

### 9.3 Publishing a package version

`publish-package.yml` runs on GitHub Release creation, a `v*` tag push, or manual dispatch; it builds the wheel/sdist
with `python -m build` and uploads them as a workflow artifact (**not** to GitHub Packages — GitHub Packages does not
support Python/PyPI packages). To let another machine install a specific commit/tag directly from GitHub:

```bash
uv pip install git+https://x-access-token:$GH_PAT@github.com/iggyikenna/instruments-service.git
```

---

## 10. Verification Checklist

```bash
# 1. Import works
python -c "from instruments_service import InstrumentProcessingService; print('Import OK')"

# 2. Secret Manager access works
python -c "
from unified_trading_library import get_secret_client
sc = get_secret_client()
key = sc.get_secret('tardis-api-key-full')
print('Secret Manager OK' if key else 'Secret Manager FAILED')
"

# 3. Quality gates green
bash scripts/quality-gates.sh --quick

# 4. Generate + query instruments
python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24
python -m instruments_service --mode instruments-query --start-date 2023-05-23 --output-format json
```
