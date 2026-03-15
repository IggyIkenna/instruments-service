#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-events-interface)
SERVICE_NAME="instruments-service"
SOURCE_DIR="instruments_service"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# config-bootstrap: cli/main.py uses os.environ for LOG_LEVEL before config is available
OS_ENV_EXCLUDE_GLOBS=("--glob" "!**/cli/main.py")
# Entry-point file has asyncio.run() + for loops — not an asyncio.run-inside-loop bug
ASYNCIO_RUN_EXCLUDE_GLOBS=("!**/defi_processor.py" "!**/live_mode_handler.py" "!**/instrument_handler.py")
# Lazy imports to avoid circular deps in instrument_crud.py and venue adapter dynamic loading
IMPORT_INSIDE_EXCLUDE_GLOBS=("--glob" "!**/instrument_crud.py" "--glob" "!**/venue_adapter_loader.py")
# Pre-existing large files — tracked for refactoring
FUNCTION_SIZE_EXTRA_EXCLUDES=("! -path" "./instruments_service/app/core/cloud_instrument_storage.py" "! -path" "./instruments_service/app/core/instrument_sync.py" "! -path" "./instruments_service/app/core/instrument_processing_base.py" "! -path" "./instruments_service/app/core/instrument_validation.py")
MAX_FILE_LINES=1100
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
