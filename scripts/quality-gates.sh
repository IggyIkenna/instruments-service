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
RUN_INTEGRATION=true
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
MAX_DURATION=180

# instruments-service has many data-processing methods that legitimately exceed 50L
# (exchange sync, corporate actions fetch, adapter loaders, CLI handlers).
# All documented in QUALITY_GATE_BYPASS_AUDIT.md §1.7.
MAX_METHOD_LINES=200

# Exclude test files and specific orchestration files from file-size and function-size
# checks. Orchestration files contain complex multi-step data processing that cannot
# be trivially decomposed. See QUALITY_GATE_BYPASS_AUDIT.md §1.7.
FUNCTION_SIZE_EXTRA_EXCLUDES=(
  "!" "-path" "./tests/*"
  "!" "-path" "./instruments_service/app/core/cloud_instrument_storage.py"
  "!" "-path" "./instruments_service/app/core/instruments_service.py"
  "!" "-path" "./instruments_service/app/core/processors/symbol_parser.py"
  "!" "-path" "./instruments_service/app/core/processors/canonical_key_generator.py"
  "!" "-path" "./instruments_service/cli/handlers/instrument_handler.py"
  "!" "-path" "./instruments_service/cli/parser.py"
)

# asyncio.run() in defi_processor.py / instrument_handler.py / live_mode_handler.py are
# all synchronous-bridge entry-points (called from a non-async CLI dispatcher), not nested
# in loops. The >=8-space heuristic fires because they are inside try/if blocks.
# See QUALITY_GATE_BYPASS_AUDIT.md §2.2.
ASYNCIO_RUN_EXCLUDE_GLOBS=(
  "!**/engine/processors/defi_processor.py"
  "!**/cli/handlers/live_mode_handler.py"
  "!**/cli/handlers/instrument_handler.py"
)

# Intentional lazy imports to avoid circular deps, heavy adapter loading, or
# TYPE_CHECKING blocks. All documented in QUALITY_GATE_BYPASS_AUDIT.md §1.3.
IMPORT_INSIDE_EXCLUDE_GLOBS=(
  "!**/engine/venues/venue_adapter_loader.py"
  "!**/engine/venues/ccxt_service.py"
  "!**/engine/processors/symbol_parser.py"
  "!**/engine/processors/canonical_key_generator.py"
  "!**/engine/processors/derived_fields_populator.py"
  "!**/engine/operations/instruments/orchestration/cefi_orchestration.py"
  "!**/engine/operations/instruments/orchestration/tradfi_orchestration.py"
  "!**/engine/operations/instruments/orchestration/defi_orchestration.py"
  "!**/engine/operations/instruments/orchestration/instrument_utils.py"
  "!**/engine/operations/instruments/processors/cefi_processor.py"
  "!**/app/core/instrument_sync.py"
  "!**/app/core/instrument_crud.py"
  "!**/app/core/instruments_service.py"
  "!**/app/core/selective_validation.py"
  "!**/app/core/processors/symbol_parser.py"
  "!**/app/core/processors/canonical_key_generator.py"
  "!**/cli/parser.py"
  "!**/utils/ccxt_service.py"
  "!**/sports/team_aliases.py"
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
