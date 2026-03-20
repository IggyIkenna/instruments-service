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

# ── DOCUMENTED BYPASSES (see QUALITY_GATE_BYPASS_AUDIT.md) ────────────────────
# Function size: declarative argparse, per-venue CCXT extraction loops, and
# functions barely over the 50L limit after aggressive refactoring (session 2026-03-18).
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "! -path ./instruments_service/cli/parser.py"               # parse_arguments(): 235L argparse declarations (27 add_argument calls)
    "! -path ./instruments_service/utils/ccxt_service.py"       # get_metadata/get_leverage_limits: per-venue CCXT loops (123L each)
    "! -path ./instruments_service/cli/handlers/live_mode_handler.py"  # _run_live_mode/process_cycle: live orchestration (96/92L)
    "! -path ./instruments_service/cli/handlers/corporate_actions_update_handler.py"  # run/get_outdated: corp actions (96/60L)
)

# Import-inside-function: TYPE_CHECKING-only imports (type annotations only, not runtime)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/engine/venues/ccxt_service.py"               # TYPE_CHECKING VenueMapping + ThreadPoolExecutor
    "!**/cli/parser.py"                                # TYPE_CHECKING get_data_source (query help text)
    "!**/engine/processors/derived_fields_populator.py"  # TYPE_CHECKING ExchangeInstrumentConfig
    "!**/engine/processors/canonical_key_generator.py"   # TYPE_CHECKING DataTypeConfig, ExchangeInstrumentConfig
    "!**/engine/processors/symbol_parser.py"             # TYPE_CHECKING ExchangeInstrumentConfig
    "!**/app/core/processors/symbol_parser.py"           # TYPE_CHECKING ExchangeInstrumentConfig (app/core path)
    "!**/app/core/processors/canonical_key_generator.py" # TYPE_CHECKING DataTypeConfig, ExchangeInstrumentConfig (app/core path)
    "!**/app/core/instrument_sync.py"                    # Circular dep avoidance (InstrumentProcessingService)
    "!**/app/core/selective_validation.py"               # Lazy load LEAGUE_REGISTRY (~4MB sports data)
    "!**/app/core/instruments_service.py"                # Lazy load sports_orchestration (~4MB sports data)
    "!**/sports/team_aliases.py"                         # Lazy load ~2MB sports mapping data
    "!**/orchestration/cefi_orchestration.py"             # Circular dep avoidance (InstrumentProcessingService)
    "!**/orchestration/instrument_utils.py"               # noqa: domain-ucs migration pending
    "!**/engine/operations/instruments/orchestration/defi_orchestration.py"   # Circular dep avoidance (InstrumentProcessingService)
    "!**/engine/operations/instruments/orchestration/tradfi_orchestration.py" # Circular dep avoidance (InstrumentProcessingService)
    "!**/engine/operations/instruments/processors/cefi_processor.py"          # Circular dep avoidance (DerivedFieldsPopulator)
)

# Deep unified lib imports: UAC sports facade and URDI adapters are exempt.
# unified_api_contracts.sports is the published domain surface (not internal).
# URDI adapters are not exported at URDI __init__.py level; follow-up: promote to URDI top-level.
DEEP_IMPORT_EXCLUDE_GLOBS=(
    "!**/sports/league_registry.py"       # Re-exports from UAC sports facade — correct per architecture rules
    "!**/app/core/instrument_sync.py"     # URDI adapters not in URDI __init__.py; follow-up: promote to URDI top-level
)

# GCP_PROJECT_ID in error message string (legitimate): service_config.py error message tells
# operators which env var to set. Not a variable usage violation.
GCP_PROJECT_ID_EXCLUDE_GLOBS=(
    "!**/config/service_config.py"   # Error message string: "Set GCP_PROJECT_ID or..."
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
