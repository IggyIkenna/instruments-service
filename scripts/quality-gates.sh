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
MIN_COVERAGE=25  # Post-consolidation: URDI merged in; tests not yet migrated
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
MAX_DURATION=300

# ── Per-repo QG exclusions ──────────────────────────────────────────────────
# Adapters parse raw JSON/GraphQL responses where empty-string/dict/list defaults
# are the standard defensive pattern (API returns null → fallback to ""/{}/ []).
# These are NOT architectural violations — they are adapter-layer parsing guards.

# Imports inside functions: adapters with conditional/lazy imports (registry data, codecs, asyncio)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/databento.py"
    "!**/reference_data/adapters/api_football.py"
    "!**/reference_data/adapters/polymarket.py"
    "!**/reference_data/adapters/sports/adapters/understat.py"
    "!**/reference_data/adapters/sports/adapters/api_football.py"
    "!**/reference_data/adapters/sports/adapters/odds_api.py"
)

# Empty string fallbacks: adapter JSON parsing (e.g. .get("symbol", ""))
EMPTY_STR_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
)

# Empty dict/list fallbacks: adapter GraphQL/JSON nested access (e.g. .get("data", {}).get("pools", []))
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
)

# Deep unified lib imports: reference_data adapters legitimately import from
# unified_api_contracts.internal (InstrumentRecord, FeeScheduleEntry, MarginType)
# and unified_api_contracts.registry (SUBGRAPH_IDS, get_subgraph_id, etc.)
DEEP_IMPORT_EXCLUDE_GLOBS=(
    "!**/adapters/urdi_reference_provider.py"
    "!**/reference_data/base_adapter.py"
    "!**/reference_data/schemas.py"
    "!**/reference_data/__init__.py"
    "!**/reference_data/factory.py"
    "!**/reference_data/router.py"
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
    "!**/engine/orchestrator.py"
)

# Function/method size: reference data adapters have large parse/fetch methods (JSON→record mapping)
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./${SOURCE_DIR}/reference_data/adapters/*"
)

# pip-audit: ignore cryptography CVE-2026-34073 (DNS name constraint bypass, low severity, pending upgrade)
PIP_AUDIT_EXTRA_ARGS="--ignore-vuln CVE-2026-34073"

# STEP 5.23: instruments-service legitimately uses canonical.domain.sports/prediction
# imports — these symbols are not yet re-exported through UAC facades.
UAC_CANONICAL_EXEMPT=true

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
