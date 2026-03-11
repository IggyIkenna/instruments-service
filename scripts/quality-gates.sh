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
# asyncio.run() in CLI/processors is a known sync-over-async bridge pattern
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/engine/processors/defi_processor.py"
    "!**/cli/handlers/instrument_handler.py"
    "!**/cli/handlers/live_mode_handler.py"
)
# Deferred optional-dep imports inside functions (UMI adapters, config reloaders, market utils)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/engine/venues/venue_adapter_loader.py"
    "--glob" "!**/engine/venues/ccxt_service.py"
    "--glob" "!**/engine/processors/derived_fields_populator.py"
    "--glob" "!**/engine/processors/canonical_key_generator.py"
    "--glob" "!**/engine/processors/symbol_parser.py"
    "--glob" "!**/engine/operations/instruments/**"
    "--glob" "!**/monitors/instruments_freshness.py"
    "--glob" "!**/sports/team_aliases.py"
    "--glob" "!**/cli/**"
    "--glob" "!**/utils/**"
    "--glob" "!**/config_reloaders.py"
    "--glob" "!**/app/**"
    "--glob" "!**/orchestration/instrument_utils.py"
)
# Large orchestration, processing, and CLI files — infra complexity by design
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

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
