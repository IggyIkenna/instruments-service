#!/usr/bin/env bash
# Run instruments-service in local batch mode.
# Purpose: surface missing env vars and import errors before GCP sandbox deployment.
# Gate: service starts and processes one synthetic batch record without crashing.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Load .env if present
if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a && source .env && set +a
fi

# Local-mode overrides — never use live GCS or Secret Manager
export CLOUD_PROVIDER="${CLOUD_PROVIDER:-local}"
export SERVICE_MODE="${SERVICE_MODE:-batch}"
export ENVIRONMENT="${ENVIRONMENT:-development}"
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-local-dev}"
export USE_MOCK_DATA="${USE_MOCK_DATA:-true}"
export USE_SECRET_MANAGER="${USE_SECRET_MANAGER:-false}"
export ENABLE_CSV_SAMPLING="${ENABLE_CSV_SAMPLING:-true}"
export CSV_SAMPLE_SIZE="${CSV_SAMPLE_SIZE:-100}"
export CSV_SAMPLE_DIR="${CSV_SAMPLE_DIR:-./data/samples}"
export enable_metadata_caching="${enable_metadata_caching:-true}"
export cache_ttl_hours="${cache_ttl_hours:-24}"
export max_batch_size="${max_batch_size:-100}"

# Stub bucket names so config validation passes
export INSTRUMENTS_GCS_BUCKET_CEFI="${INSTRUMENTS_GCS_BUCKET_CEFI:-local-mock-bucket}"
export INSTRUMENTS_GCS_BUCKET_TRADFI="${INSTRUMENTS_GCS_BUCKET_TRADFI:-local-mock-bucket}"
export INSTRUMENTS_GCS_BUCKET_DEFI="${INSTRUMENTS_GCS_BUCKET_DEFI:-local-mock-bucket}"

START_DATE="${1:-2024-01-02}"
END_DATE="${2:-2024-01-02}"

echo "[run_local] instruments-service: CLOUD_PROVIDER=${CLOUD_PROVIDER} SERVICE_MODE=${SERVICE_MODE}"
echo "[run_local] Date range: ${START_DATE} — ${END_DATE}"

python -m instruments_service.cli.main \
  --CEFI \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --log-level DEBUG
