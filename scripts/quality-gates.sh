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
MIN_COVERAGE=72
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# defi_processor.py uses asyncio.run() as top-level entrypoint; loops are in separate scope
ASYNCIO_RUN_EXCLUDE_GLOBS=("!**/engine/processors/defi_processor.py" "!**/cli/handlers/instrument_handler.py" "!**/cli/handlers/live_mode_handler.py")
# TYPE_CHECKING blocks (correct pattern — QG regex matches indented imports including if TYPE_CHECKING blocks)
# venue_adapter_loader: intentional plugin lazy loading — correct pattern for adapter systems
# instrument_utils.py: determine_market_category deferred (noqa: domain-ucs — not yet in UDC)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/__init__.py"
    "!**/engine/venues/venue_adapter_loader.py"
    "!**/engine/processors/symbol_parser.py"
    "!**/engine/processors/derived_fields_populator.py"
    "!**/engine/processors/canonical_key_generator.py"
    "!**/engine/venues/ccxt_service.py"
    "!**/engine/operations/instruments/orchestration/cefi_orchestration.py"
    "!**/engine/operations/instruments/orchestration/defi_orchestration.py"
    "!**/engine/operations/instruments/orchestration/instrument_utils.py"
    "!**/engine/operations/instruments/orchestration/tradfi_orchestration.py"
    "!**/engine/operations/instruments/processors/cefi_processor.py"
    "!**/utils/ccxt_service.py"
    "!**/app/core/instrument_crud.py"
    "!**/app/core/instrument_sync.py"
    "!**/app/core/selective_validation.py"
    "!**/app/core/processors/symbol_parser.py"
    "!**/app/core/processors/canonical_key_generator.py"
    "!**/cli/parser.py"
)
# Tests contain inherently large files (coverage boost suites); cli/engine dirs have large data/processor classes
FUNCTION_SIZE_EXTRA_EXCLUDES=("! -path ./tests/*")
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
