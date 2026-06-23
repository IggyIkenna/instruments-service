#!/usr/bin/env bash
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-pm/codex/06-coding-standards/quality-gates-service-template.sh
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
MIN_COVERAGE=88
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
    # databento/polymarket split into packages 2026-06-12 (codex ratchet plan) — same
    # conditional/lazy-import justification carries to the package modules.
    "!**/reference_data/adapters/tradfi/databento/*.py"
    "!**/reference_data/adapters/api_football.py"
    "!**/reference_data/adapters/prediction/polymarket/*.py"
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
    # engine/orchestrator package: pre-existing lazy in-function imports moved
    # verbatim in the orchestrator.py split (pure code motion — hoisting them is
    # behaviour change / cycle risk, e.g. catalogue.refresh_catalogue's import is
    # cycle-breaking). Scoped to ONLY the carrying modules, not the package.
    # Plan: unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md
    "!**/engine/orchestrator/catalogue.py"
    "!**/engine/orchestrator/footystats.py"
    "!**/engine/orchestrator/process.py"
    "!**/engine/orchestrator/sfi.py"
    "!**/engine/orchestrator/sports_reference.py"
    "!**/engine/orchestrator/transfermarkt.py"
    "!**/engine/orchestrator/understat.py"
    "!**/engine/orchestrator/weather.py"
    "!**/engine/orchestrator/writers.py"
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
    # 2026-06-12: adapters split into packages (codex ratchet plan) — same adapter-layer
    # parsing-guard justification carries one level down.
    "!**/reference_data/adapters/cefi/tardis/*.py"
    "!**/reference_data/adapters/tradfi/databento/*.py"
    "!**/reference_data/adapters/prediction/polymarket/*.py"
    "!**/reference_data/adapters/*.py"
    "!**/reference_data/adapters/cefi/*.py"
    "!**/reference_data/adapters/defi/*.py"
    "!**/reference_data/adapters/tradfi/*.py"
    "!**/reference_data/adapters/prediction/*.py"
    "!**/reference_data/intent_resolver.py"
    "!**/reference_data/adapters/sports/adapters/*.py"
    # engine/orchestrator package: pre-existing adapter-style `.get(key, "")`
    # parsing guards moved verbatim in the orchestrator.py split (fail-fast
    # conversion is a behaviour change, out of scope for the pure-motion split).
    # Scoped to ONLY the carrying modules, not the package.
    # Plan: unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md
    "!**/engine/orchestrator/footystats.py"
    "!**/engine/orchestrator/prediction.py"
    "!**/engine/orchestrator/sfi.py"
    "!**/engine/orchestrator/transfermarkt.py"
    "!**/engine/orchestrator/weather.py"
    "!**/engine/orchestrator/writers.py"
)

# Empty dict/list fallbacks: adapter GraphQL/JSON nested access (e.g. .get("data", {}).get("pools", []))
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(
    # 2026-06-12: adapters split into packages (codex ratchet plan) — same adapter-layer
    # parsing-guard justification carries one level down.
    "!**/reference_data/adapters/cefi/tardis/*.py"
    "!**/reference_data/adapters/tradfi/databento/*.py"
    "!**/reference_data/adapters/prediction/polymarket/*.py"
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
    # 2026-06-12: adapters split into packages (codex ratchet plan) — same adapter-layer
    # parsing-guard justification carries one level down.
    "!**/reference_data/adapters/cefi/tardis/*.py"
    "!**/reference_data/adapters/tradfi/databento/*.py"
    "!**/reference_data/adapters/prediction/polymarket/*.py"
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
    # engine/orchestrator package: module-level deep imports now live in the
    # package __init__.py (auto-exempt via the check's !**/__init__.py glob).
    # Only in-function lazy deep imports (capability_declarations._defi,
    # external.understat) remain — moved verbatim in the orchestrator.py split;
    # scoped to ONLY the carrying modules.
    # Plan: unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md
    "!**/engine/orchestrator/catalogue.py"
    "!**/engine/orchestrator/understat.py"
    "!**/engine/orchestrator/writers.py"
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
    # engine/orchestrator package: the 8,192-line orchestrator.py monolith was
    # split into 16 cohesion modules + a thin __init__ (2026-06-11). The split
    # was PURE CODE MOTION — legacy oversized functions (the sports fetchers
    # 206-882L) kept their existing size by design, so ONLY the modules that
    # carry them stay excluded. 2026-06-11 follow-up (same plan): process.py
    # (process_instruments 1,931L → staged process_* sibling modules) and
    # sports_reference.py (_fetch_sports_reference_data 882L →
    # sports_reference_core/_fixtures sibling modules) were decomposed and
    # REMOVED from this list — they now pass the 900-line/200-line gates
    # directly. Decomposing the remaining fetcher bodies is follow-up work
    # under the plan below.
    # Plan: unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator/footystats.py"
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator/sfi.py"
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator/transfermarkt.py"
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator/understat.py"
    "!" "-path" "./${SOURCE_DIR}/engine/orchestrator/weather.py"
    "!" "-path" "./${SOURCE_DIR}/triggers/sports_fixtures_daily_repoll.py"
    "!" "-path" "./${SOURCE_DIR}/cli/instruments_handler.py"
)

# pip-audit: ignore cryptography CVE-2026-34073 (DNS name constraint bypass, low severity, pending upgrade)
PIP_AUDIT_EXTRA_ARGS="--ignore-vuln CVE-2026-34073"


# Temporary rollout tolerance for known codex debt under active remediation.
# Ratcheted 4 → 3 on 2026-06-11: the function/file-size violation class CLEARED
# — process_instruments (1,931L) + _fetch_sports_reference_data (882L) were
# decomposed into staged sibling modules and urdi_reference_provider's
# fetch_instruments_for_all_venues (246L) split, so every non-excluded file now
# passes the 900/200/50 size gates. Remaining 3 classes: os.getenv/os.environ
# (DEPLOYMENT_ENV test shims + polymarket cursor overrides), the bare
# `pip install uv` Dockerfile bootstrap, and broad `except Exception:`
# shard-isolation handlers.
# Plan: unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md
CODEX_MAX_VIOLATIONS=3
export CODEX_MAX_VIOLATIONS

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: lifecycle triple (STARTED / STOPPED / FAILED) via UTL — not duplicated in service code.
# See: unified-trading-pm/codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
if rg -q 'fastapi_uei_lifespan\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: fastapi_uei_lifespan (canonical HTTP wiring in UTL)"
elif rg -q 'ServiceBootstrap\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: ServiceBootstrap (canonical CLI wiring in UTL)"
else
    for event in STARTED STOPPED FAILED; do
        # -U: allow multiline call sites (e.g. log_event(\n  "STARTED", ...))
        run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -U -q \
            || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
    done
fi

# STEP 5.70: IS-MTDS contract integrity gates (is_mtds_contract_audit_2026_05_20 Phase 7)
log_section "[5.70/6] IS-MTDS CONTRACT INTEGRITY"
QG_SCRIPTS_DIR="${WORKSPACE_ROOT}/unified-trading-pm/scripts/qg"
if [[ -d "${QG_SCRIPTS_DIR}" ]]; then
    if [[ -f "${QG_SCRIPTS_DIR}/no_silent_absence_handlers.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_silent_absence_handlers.sh" "${WORKSPACE_ROOT}" \
            || log_warn "IS-MTDS: silent-absence violation — see plans/active/is_mtds_contract_audit_2026_05_20.md Phase 3"
    else
        log_warn "IS-MTDS QG scripts not found at ${QG_SCRIPTS_DIR}"
    fi
    if [[ -f "${QG_SCRIPTS_DIR}/no_hardcoded_venue_urls.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_hardcoded_venue_urls.sh" "${WORKSPACE_ROOT}" \
            || log_warn "IS-MTDS: hardcoded venue URL violation — see plans/active/is_mtds_contract_audit_2026_05_20.md Phase 7"
    fi
    if [[ -f "${QG_SCRIPTS_DIR}/no_hardcoded_venue_universe.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_hardcoded_venue_universe.sh" "${WORKSPACE_ROOT}" \
            || log_warn "IS-MTDS: hardcoded venue universe violation — see plans/active/is_mtds_contract_audit_2026_05_20.md Phase 7"
    fi
    if [[ -f "${QG_SCRIPTS_DIR}/no_inline_coverage_formula.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_inline_coverage_formula.sh" "${WORKSPACE_ROOT}" \
            || log_warn "Honest coverage: inline formula violation — see honest_coverage_formula_consolidation_2026_05_19.md Phase 6"
    fi
    if [[ -f "${QG_SCRIPTS_DIR}/honest_coverage_ratchet.sh" ]]; then
        _RATCHET_BUCKET="instruments-store-defi-${GCP_PROJECT_ID:-central-element-323112}"
        run_timeout 60 bash "${QG_SCRIPTS_DIR}/honest_coverage_ratchet.sh" \
            "instruments-service" "${_RATCHET_BUCKET}" "defi" \
            || log_warn "Honest coverage: IS defi coverage regression — see honest_coverage_formula_consolidation_2026_05_19.md Phase 6"
    fi
    # STEP 5.83: adapter contract-call regression ratchet (lint_sweep_774602ea8 audit Phase 1)
    if [[ -f "${QG_SCRIPTS_DIR}/no_adapter_contract_regression.sh" ]]; then
        run_timeout 60 bash "${QG_SCRIPTS_DIR}/no_adapter_contract_regression.sh" "${WORKSPACE_ROOT}" \
            || log_warn "Adapter contract-call regression — see plans/active/issues/lint_sweep_774602ea8_regression_audit_2026_05_20.md"
    fi
    # STEP 5.84: schema-version compliance — no schema_version < 8 in service source (mega audit B1 Pattern 3)
    if [[ -f "${QG_SCRIPTS_DIR}/no_legacy_schema_version.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_legacy_schema_version.sh" "${WORKSPACE_ROOT}" \
            || log_warn "Legacy schema_version < 8 in source — see codex/04-architecture/service-contract-audit-template.md § Pattern 3"
    fi
    # STEP 5.85: honest-absence reason taxonomy — no blank/string-literal record_empty reasons (mega audit B1 Pattern 4)
    if [[ -f "${QG_SCRIPTS_DIR}/no_blank_empty_reason.sh" ]]; then
        run_timeout 30 bash "${QG_SCRIPTS_DIR}/no_blank_empty_reason.sh" "${WORKSPACE_ROOT}" \
            || log_warn "Blank or string-literal record_empty reason — use EmptyConfirmedReason enum. SSOT: service-contract-audit-template.md § Pattern 4"
    fi
else
    log_warn "IS-MTDS QG scripts dir not found at ${QG_SCRIPTS_DIR}"
fi
