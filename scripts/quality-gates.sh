#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
SERVICE_NAME="instruments-service"
SOURCE_DIR="instruments_service"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# asyncio.run() sync-over-async bridge — Documented: QUALITY_GATE_BYPASS_AUDIT.md §1.2
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/engine/processors/defi_processor.py"
    "!**/cli/handlers/instrument_handler.py"
    "!**/cli/handlers/live_mode_handler.py"
)
# Deferred optional-dep imports in UMI adapters, config reloaders — QUALITY_GATE_BYPASS_AUDIT.md §1.2
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/engine/venues/**" "--glob" "!**/engine/processors/**"
    "--glob" "!**/engine/operations/**" "--glob" "!**/monitors/**"
    "--glob" "!**/sports/**" "--glob" "!**/cli/**" "--glob" "!**/utils/**"
    "--glob" "!**/app/**" "--glob" "!**/config_reloaders.py"
    "--glob" "!**/orchestration/instrument_utils.py"
)
# Infra complexity by design — QUALITY_GATE_BYPASS_AUDIT.md §1.3
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./instruments_service/app/core/*"
    "!" "-path" "./instruments_service/utils/*"
    "!" "-path" "./instruments_service/engine/processors/*"
    "!" "-path" "./instruments_service/engine/venues/*"
    "!" "-path" "./instruments_service/engine/operations/*"
    "!" "-path" "./instruments_service/cli/*"
    "!" "-path" "./instruments_service/sports/*"
    "!" "-path" "./instruments_service/corporate_actions/*"
    "!" "-path" "./tests/*"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
