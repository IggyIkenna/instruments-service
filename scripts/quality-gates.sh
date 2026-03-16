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

# --- Documented exclusions (see QUALITY_GATE_BYPASS_AUDIT.md §1.1) ---

# CLI bootstrap: main.py reads LOG_LEVEL before any config system is initialised
OS_ENV_EXCLUDE_GLOBS=("--glob" "!**/cli/main.py")

# asyncio.run() false positives: sync CLI / entry-point files that bridge to async
# defi_processor.py: sync caller invokes asyncio.run(adapter.fetch_pools(...)) per protocol
ASYNCIO_RUN_EXCLUDE_GLOBS=("!**/defi_processor.py" "!**/cli/**")

# Lazy adapter loading by design (venue_adapter_loader.py loads adapters on first use)
# Base adds --glob prefix for each element automatically
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/venue_adapter_loader.py"
    "!**/adapter_loader.py"
    "!**/instruments_service.py"
    "!**/instrument_processing_service.py"
    "!**/symbol_parser.py"
    "!**/canonical_key_generator.py"
    "!**/live_mode_handler.py"
    "!**/cloud_instrument_storage.py"
    "!**/parser.py"
    "!**/main.py"
    "!**/ccxt_service.py"
    "!**/corporate_actions_handler.py"
    "!**/corporate_actions_backfill_handler.py"
    "!**/corporate_actions_production_handler.py"
    "!**/corporate_actions_update_handler.py"
    "!**/dependency_checker.py"
    "!**/instrument_crud.py"
    "!**/config_reloaders.py"
    "!**/derived_fields_populator.py"
    "!**/cefi_processor.py"
    "!**/defi_orchestration.py"
    "!**/selective_validation.py"
    "!**/cefi_orchestration.py"
    "!**/instrument_sync.py"
    "!**/team_aliases.py"
    "!**/instrument_utils.py"
    "!**/tradfi_orchestration.py"
)

# Pre-existing large functions in complex domain service (see QUALITY_GATE_BYPASS_AUDIT.md §1.7)
# Excludes: tests, engine operations/processors/venues, CLI handlers, app/core orchestration
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "! -path" "./tests/*"
    "! -path" "./instruments_service/app/core/*"
    "! -path" "./instruments_service/cli/*"
    "! -path" "./instruments_service/engine/*"
    "! -path" "./instruments_service/utils/*"
    "! -path" "./instruments_service/corporate_actions/*"
    "! -path" "./instruments_service/sports/*"
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
