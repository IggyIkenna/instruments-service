#!/usr/bin/env bash
# Run instruments-service in real dev mode against live GCP.
#
# Uses the real project from .env (CLOUD_MOCK_MODE=false), real Secret Manager
# for API keys, and real GCS buckets. API keys are fetched by the service itself
# via UTL's validate_api_keys_for_venues() — no local key config needed.
#
# Prerequisites:
#   gcloud auth application-default login   (sets ADC for Secret Manager + GCS)
#
# Usage:
#   bash scripts/run_local.sh                         # DEFI, March 22nd 2026
#   bash scripts/run_local.sh DEFI 2026-03-22         # explicit category + date
#   bash scripts/run_local.sh CEFI 2026-03-22
#   bash scripts/run_local.sh ALL 2026-03-22 2026-03-22

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Load .env — contains GCP_PROJECT_ID, CLOUD_MOCK_MODE=false, secret names, etc.
if [[ -f ".env" ]]; then
    set -a && source .env && set +a
fi

CATEGORY="${1:-DEFI}"
START_DATE="${2:-2026-03-22}"
END_DATE="${3:-$START_DATE}"

export _RUN_CATEGORY="$CATEGORY"
export _RUN_START="$START_DATE"
export _RUN_END="$END_DATE"

# Enable CSV sampling so you can inspect outputs in instruments_service/data/
export ENABLE_CSV_SAMPLING="${ENABLE_CSV_SAMPLING:-true}"
export CSV_SAMPLE_SIZE="${CSV_SAMPLE_SIZE:-50}"
export CSV_SAMPLE_DIR="${CSV_SAMPLE_DIR:-instruments_service/data}"

# Write instrument parquet to GCS (not local disk) — DataSink backend
export PROTOCOL_DATA_SINK_BACKEND="${PROTOCOL_DATA_SINK_BACKEND:-gcs}"

echo "[run_local] Project: ${GCP_PROJECT_ID}"
echo "[run_local] Mock mode: ${CLOUD_MOCK_MODE:-false}"
echo "[run_local] Category: ${CATEGORY}  |  ${START_DATE} → ${END_DATE}"
echo ""

.venv/bin/python - << PYEOF
import os, asyncio, logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

from unified_trading_library import GCSEventSink
from unified_events_interface import setup_events
from instruments_service.config import get_config

config = get_config()

# Wire real event sink — events go to GCS in real mode
sink = GCSEventSink(
    project_id=config.gcp_project_id,
    bucket=f"{config.gcp_project_id}-events",
    service_name="instruments-service",
)
setup_events(service_name="instruments-service", mode="batch", sink=sink)

from instruments_service.engine.orchestrator import process_instruments, get_venues_for_categories, is_venue_available
from unified_trading_library import validate_api_keys_for_venues

category = os.environ.get("_RUN_CATEGORY", "DEFI")
start_date = os.environ.get("_RUN_START", "2026-03-22")
end_date = os.environ.get("_RUN_END", start_date)

venues = get_venues_for_categories([category])
active = [v for v in venues if is_venue_available(v, start_date)]
print(f"Active venues for {category}: {len(active)}")

# Validate API keys — fails with clear error if any required key is missing in Secret Manager
print("Validating API keys via Secret Manager...")
api_keys = validate_api_keys_for_venues(active, project_id=config.gcp_project_id)
if api_keys:
    print(f"API keys obtained for: {sorted(api_keys.keys())}")

async def run():
    result = await process_instruments(start_date, [category], api_keys=api_keys)
    total = sum(result.values())
    print(f"\nDone: {total} instruments written across {len(result)} venues")
    for venue, count in sorted(result.items()):
        print(f"  {venue}: {count}")

asyncio.run(run())
PYEOF
