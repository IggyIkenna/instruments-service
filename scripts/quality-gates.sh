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

# §1.1 asyncio.run() false-positive exclusions — these files use asyncio.run() as a
# CLI/sync-entry-point bridge; they also contain for/while loops that trigger the heuristic.
# Documented in QUALITY_GATE_BYPASS_AUDIT.md §1.1.
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!instruments_service/engine/processors/defi_processor.py"
    "!instruments_service/cli/handlers/live_mode_handler.py"
    "!instruments_service/cli/handlers/instrument_handler.py"
)

# §1.2 Import-inside-function exclusions — lazy-loading and circular-import avoidance
# patterns documented in QUALITY_GATE_BYPASS_AUDIT.md §1.2 and §1.3.
# These directories contain many files with legitimate lazy imports; use directory-level globs.
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!instruments_service/engine/**"
    "!instruments_service/app/core/**"
    "!instruments_service/cli/**"
    "!instruments_service/monitors/**"
    "!instruments_service/sports/team_aliases.py"
    "!instruments_service/utils/ccxt_service.py"
    "!instruments_service/config_reloaders.py"
)

# §1.7 Function/class/method size exclusions — complex data-processing orchestration files,
# AI-generated coverage test files, and CLI handlers where decomposition adds no value.
# Documented in QUALITY_GATE_BYPASS_AUDIT.md §1.7.
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "! -path ./tests/unit/test_boost_*"
    "! -path ./tests/unit/test_coverage_boost_*"
    "! -path ./tests/live/*"
    "! -path ./instruments_service/app/core/*"
    "! -path ./instruments_service/utils/ccxt_service.py"
    "! -path ./instruments_service/cli/handlers/*"
    "! -path ./instruments_service/cli/parser.py"
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
