#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-trading-library)
SERVICE_NAME="instruments-service"
SOURCE_DIR="instruments_service"
# ratcheted 2026-04-19 from coverage.xml (was 75)
# temporarily lowered 2026-04-21 from 78 → 77 after UTL rolling-window migration
# (b0152fb) deleted cli/rolling_window.py (+ its 307-line test file). New
# replacement tests added (+55 stmts) don't restore the full delta because a
# portion of the deleted module moved upstream to UTL. Target: ratchet back to
# 78 once instruments-service adds coverage for a currently-untested branch
# (e.g. reference_data/sports dependency fallback paths). See QG-residual
# cleanup report 2026-04-21.
MIN_COVERAGE=77
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# Type check + pytest + codex on a large tree often exceeds 300s locally.
MAX_DURATION=600

# ── Per-repo QG exclusions ──────────────────────────────────────────────────
# Adapters parse raw JSON/GraphQL responses where empty-string/dict/list defaults
# are the standard defensive pattern (API returns null → fallback to ""/{}/ []).
# These are NOT architectural violations — they are adapter-layer parsing guards.

# Imports inside functions: adapters with conditional/lazy imports (registry data, codecs, asyncio)
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/tradfi/databento.py"
    "!**/reference_data/adapters/api_football.py"
    "!**/reference_data/adapters/prediction/polymarket.py"
    "!**/reference_data/adapters/defi/raydium.py"
    "!**/reference_data/adapters/defi/orca.py"
    "!**/reference_data/adapters/defi/kamino.py"
    "!**/reference_data/adapters/defi/_solana_utils.py"
    "!**/reference_data/adapters/tradfi/tradfi_live.py"
    "!**/reference_data/base_adapter.py"
    "!**/reference_data/factory.py"
    "!**/reference_data/utils/evm_creation_resolver.py"
    "!**/reference_data/adapters/sports/adapters/understat.py"
    "!**/reference_data/adapters/sports/adapters/api_football.py"
    "!**/reference_data/adapters/sports/adapters/odds_api.py"
    "!**/reference_data/adapters/sports/adapters/base.py"
    "!**/engine/orchestrator.py"
    "!**/reference_data/adapters/prediction/kalshi.py"
    "!**/triggers/sports_fixtures_daily_repoll.py"
)

# Broad excepts in resolver/cache utilities are intentional defensive wrappers around
# network/storage boundaries and are audited in this repo.
BE_EXCLUDE_GLOBS=(
    "**/reference_data/adapters/defi/_solana_utils.py"
    "**/reference_data/utils/evm_creation_resolver.py"
)

# Empty string fallbacks: adapter JSON parsing (e.g. .get("symbol", ""))
EMPTY_STR_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/cefi/*.py"
    "!**/reference_data/adapters/defi/*.py"
    "!**/reference_data/adapters/tradfi/*.py"
    "!**/reference_data/adapters/prediction/*.py"
    "!**/reference_data/intent_resolver.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
    "!**/engine/orchestrator.py"
)

# Empty dict/list fallbacks: adapter GraphQL/JSON nested access (e.g. .get("data", {}).get("pools", []))
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/cefi/*.py"
    "!**/reference_data/adapters/defi/*.py"
    "!**/reference_data/adapters/tradfi/*.py"
    "!**/reference_data/adapters/prediction/*.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
)

# Deep unified lib imports: reference_data adapters legitimately import from
# unified_api_contracts.internal (InstrumentRecord, FeeScheduleEntry, MarginType)
# and unified_api_contracts.registry (SUBGRAPH_IDS, get_subgraph_id, etc.)
DEEP_IMPORT_EXCLUDE_GLOBS=(
    "!**/engine/urdi_reference_provider.py"
    "!**/reference_data/base_adapter.py"
    "!**/reference_data/schemas.py"
    "!**/reference_data/__init__.py"
    "!**/reference_data/factory.py"
    "!**/reference_data/router.py"
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/cefi/*.py"
    "!**/reference_data/adapters/defi/*.py"
    "!**/reference_data/adapters/tradfi/*.py"
    "!**/reference_data/adapters/prediction/*.py"
    "!**/reference_data/utils/*.py"
    "!**/reference_data/intent_resolver.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
    "!**/reference_data/catalogue/*.py"
    "!**/engine/orchestrator.py"
    "!**/triggers/sports_fixtures_daily_repoll.py"
)

# Protocol-specific symbol checks: cache helper names (_get_gcs_bucket) in these
# utility modules are not cloud protocol coupling in service orchestration paths.
HARDCODED_PROTO_EXCLUDE_GLOBS=(
    "--glob=!**/reference_data/adapters/defi/_solana_utils.py"
    "--glob=!**/reference_data/utils/evm_creation_resolver.py"
)

# One-off migration/backfill scripts legitimately use google.cloud.storage directly —
# they run as admin operations, not as part of the live service pipeline.
CLOUD_SDK_EXCLUDE_GLOBS=(
    "!scripts/**"
)

# Function/method size: reference data adapters have large parse/fetch methods (JSON→record mapping)
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./${SOURCE_DIR}/reference_data/adapters/*"
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator.py"
    "!" "-path" "./${SOURCE_DIR}/triggers/sports_fixtures_daily_repoll.py"
    "!" "-path" "./${SOURCE_DIR}/cli/instruments_handler.py"
)

# pip-audit: ignore cryptography CVE-2026-34073 (DNS name constraint bypass, low severity, pending upgrade)
PIP_AUDIT_EXTRA_ARGS="--ignore-vuln CVE-2026-34073"

# STEP 5.23: instruments-service legitimately uses canonical.domain.sports/prediction
# imports — these symbols are not yet re-exported through UAC facades.
UAC_CANONICAL_EXEMPT=true

# Temporary rollout tolerance for known codex debt under active remediation.
# Remaining: bandit /tmp usage (orchestrator fixture cache), backward-compat docstring,
# pip-audit CVE pending upgrade.
CODEX_MAX_VIOLATIONS=4
export CODEX_MAX_VIOLATIONS

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    # -U: allow multiline call sites (e.g. log_event(\n  "STARTED", ...))
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -U -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
