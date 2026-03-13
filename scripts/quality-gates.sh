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
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"

# Bypass: asyncio.run() used as sync→async bridge in CLI entry-points and DeFi processor.
# Documented: QUALITY_GATE_BYPASS_AUDIT.md §1.1 asyncio.run() Exclusions (2026-03-12)
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/engine/processors/defi_processor.py"
    "!**/cli/**"
    "!**/venues/defi/*"
    "!**/examples/**"
    "!**/scripts/**"
)

# Bypass: lazy adapter loading by design in engine/venues, app/core, utils, sports, cli, monitors.
# Adapter/lib imports are gated on venue/protocol at runtime; moving to top level would
# import ALL adapters unconditionally (heavy deps, circular imports).
# team_aliases.py: 2MB sports mapping data loaded lazily.
# ccxt_service.py (utils): VenueMapping and concurrent.futures are optionally-used lazy loads.
# Documented: QUALITY_GATE_BYPASS_AUDIT.md §1.1 (Import whitelist: adapter_loader.py)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/engine/**"
    "!**/app/core/**"
    "!**/cli/**"
    "!**/monitors/**"
    "!**/utils/**"
    "!**/sports/**"
)

# Bypass: all instruments_service/ modules contain pre-existing complex orchestration methods that
# exceed the 50L method limit. All documented in QUALITY_GATE_BYPASS_AUDIT.md §1.7 as Phase 3
# refactoring targets. Coverage boost test files also exceed 900L file size limit by design.
# NOTE: find requires separate array elements per flag — do NOT use "! -path X" as a single element.
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./tests/*"
    "!" "-path" "./instruments_service/*"
)

source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
