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
RUN_INTEGRATION=true
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()

# Bypass §1.1 — CLI handlers use asyncio.run() as the sync→async bridge (one call per CLI
# invocation). defi_processor.py uses asyncio.run() per protocol (sync caller, async adapter).
# These trigger the "file has for/while + asyncio.run" heuristic as false-positives.
# Documented in QUALITY_GATE_BYPASS_AUDIT.md §1.1.
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/defi_processor.py"
    "!**/cli/**"
)

# Bypass §1.3 — lazy/conditional imports documented in QUALITY_GATE_BYPASS_AUDIT.md §1.3, §1.7.
# venue_adapter_loader: deferred optional heavy deps (TardisAdapter, DatabentoAdapter, Hyperliquid).
# ccxt_service: optional VenueMapping / concurrent.futures (heavy deps not always needed).
# derived_fields_populator: lazy import of ExchangeInstrumentConfig to avoid circular imports.
# config_reloaders, instrument_crud, instrument_sync, *_orchestration, orchestrator_base,
# team_aliases, cefi_processor: all avoid circular imports or lazy-load optional deps.
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/venue_adapter_loader.py"
    "--glob" "!**/ccxt_service.py"
    "--glob" "!**/derived_fields_populator.py"
    "--glob" "!**/config_reloaders.py"
    "--glob" "!**/instrument_crud.py"
    "--glob" "!**/instrument_sync.py"
    "--glob" "!**/defi_orchestration.py"
    "--glob" "!**/tradfi_orchestration.py"
    "--glob" "!**/orchestrator_base.py"
    "--glob" "!**/team_aliases.py"
    "--glob" "!**/cefi_processor.py"
    "--glob" "!**/instruments_service.py"
    "--glob" "!**/instrument_processing_service.py"
    "--glob" "!**/dependency_checker.py"
    "--glob" "!**/symbol_parser.py"
    "--glob" "!**/canonical_key_generator.py"
    "--glob" "!**/live_mode_handler.py"
    "--glob" "!**/cloud_instrument_storage.py"
    "--glob" "!**/parser.py"
    "--glob" "!**/main.py"
    "--glob" "!**/corporate_actions_handler.py"
    "--glob" "!**/corporate_actions_backfill_handler.py"
    "--glob" "!**/corporate_actions_production_handler.py"
    "--glob" "!**/corporate_actions_update_handler.py"
    "--glob" "!**/selective_validation.py"
    "--glob" "!**/monitors/**"
    "--glob" "!**/generate_date_views_handler.py"
    "--glob" "!**/orchestration/cefi_orchestration.py"
    "--glob" "!**/orchestration/orchestrator.py"
    "--glob" "!**/orchestration/instrument_utils.py"
)

# Bypass §1.6, §1.7 — pre-existing large files and complex orchestration methods.
# All are tracked for Phase 3 refactoring in QUALITY_GATE_BYPASS_AUDIT.md §1.7.
# Excludes: coverage-boost test files (consolidation backlog), app/core orchestration,
# CLI handlers, engine operations/processors (all documented in §1.7).
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./tests/unit/test_coverage_boost_4.py"
    "!" "-path" "./tests/unit/test_coverage_boost_instruments_3.py"
    "!" "-path" "./tests/unit/test_coverage_boost_instruments_2.py"
    "!" "-path" "./tests/unit/test_boost_coverage_5.py"
    "!" "-path" "./instruments_service/app/core/instrument_processing_base.py"
    "!" "-path" "./instruments_service/app/core/cloud_instrument_storage.py"
    "!" "-path" "./instruments_service/app/core/instrument_validation.py"
    "!" "-path" "./instruments_service/app/core/instrument_sync.py"
    "!" "-path" "./instruments_service/app/core/instrument_processing_mixins.py"
    "!" "-path" "./instruments_service/app/core/instruments_service.py"
    "!" "-path" "./instruments_service/app/core/cloud_data_provider.py"
    "!" "-path" "./instruments_service/app/core/instrument_crud.py"
    "!" "-path" "./instruments_service/app/core/instrument_processing_handlers.py"
    "!" "-path" "./instruments_service/app/core/processors/symbol_parser.py"
    "!" "-path" "./instruments_service/app/core/processors/canonical_key_generator.py"
    "!" "-path" "./instruments_service/utils/ccxt_service.py"
    "!" "-path" "./instruments_service/cli/parser.py"
    "!" "-path" "./instruments_service/cli/handlers/live_mode_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/instrument_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/corporate_actions_production_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/corporate_actions_backfill_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/corporate_actions_update_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/corporate_actions_handler.py"
    "!" "-path" "./instruments_service/cli/handlers/generate_date_views_handler.py"
    "!" "-path" "./instruments_service/sports/fixture_parser.py"
    "!" "-path" "./instruments_service/corporate_actions/adapter.py"
    "!" "-path" "./instruments_service/engine/operations/corporate_actions/adapter.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/processors/tradfi_processor.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/processors/base_processor.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/processors/cefi_processor.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/orchestrator.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/lifecycle_monitor.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/orchestration/orchestrator.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/orchestration/cefi_orchestration.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/scheduler.py"
    "!" "-path" "./instruments_service/engine/operations/instruments/orchestrator_processors.py"
    "!" "-path" "./instruments_service/engine/processors/symbol_parser.py"
    "!" "-path" "./instruments_service/engine/processors/canonical_key_generator.py"
    "!" "-path" "./instruments_service/engine/processors/defi_processor.py"
    "!" "-path" "./instruments_service/engine/venues/ccxt_service.py"
    "!" "-path" "./tests/unit/test_instrument_processing_service.py"
    "!" "-path" "./tests/unit/test_instruments_service.py"
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
