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
MIN_COVERAGE=25
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
MAX_DURATION=300
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
# instruments-service is the URDI reference-data implementation layer.
# It legitimately uses UAC canonical types directly (not via domain facade).
UAC_CANONICAL_EXEMPT=true
# Adapter files parse external REST API JSON — empty string/dict/list defaults are intentional.
EMPTY_STR_EXCLUDE_GLOBS=("!instruments_service/reference_data/adapters/**")
EMPTY_DICT_LIST_EXCLUDE_GLOBS=("!instruments_service/reference_data/adapters/**")
# unified_api_contracts.internal is the correct subpackage for internal domain types.
DEEP_IMPORT_EXCLUDE_GLOBS=("!instruments_service/reference_data/adapters/**" "!instruments_service/reference_data/base_adapter.py" "!instruments_service/reference_data/schemas.py" "!instruments_service/reference_data/universe_snapshot.py" "!instruments_service/adapters/**")
# Sports adapters have lazy TYPE_CHECKING imports; URDI adapter has lazy UAC canonical imports.
IMPORT_INSIDE_EXCLUDE_GLOBS=("!instruments_service/reference_data/adapters/sports/**" "!instruments_service/reference_data/adapters/api_football.py" "!instruments_service/reference_data/adapters/polymarket.py")
# Reference data adapters implement complex protocol-specific parsing — methods are intrinsically long.
# These are folded from unified-reference-data-interface which had its own QG rules.
FUNCTION_SIZE_EXTRA_EXCLUDES=("! -path ./instruments_service/reference_data/adapters/*")
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"
# Lifecycle events (STARTED/STOPPED/FAILED) are handled automatically by ServiceBootstrap (STEP 5.61).
# Services do NOT emit these manually — see CLAUDE.md § Service Infrastructure Requirements.
