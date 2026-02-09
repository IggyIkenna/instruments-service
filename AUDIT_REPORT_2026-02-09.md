# Instruments-Service Audit Report
**Date:** February 9, 2026
**Auditor:** Comprehensive Codebase Audit
**Status:** Production Service

---

## Executive Summary

The instruments-service has **improved** compliance with workspace standards. Production code uses `UnifiedCloudServicesConfig` correctly via `instruments_config`, follows UTC datetime standards, and config management violations in core paths have been resolved. Remaining issues are concentrated in imports inside functions, broad exception handling, scripts/tests, and hardcoded domain constants.

**Key Metrics:**
- **Critical Issues (P0):** 0 (previously reported issues have been fixed)
- **High Priority (P1):** 4
- **Medium Priority (P2):** 3
- **Low Priority (P3):** 0

---

## Resolved Issues (Previously P0)

The following were reported in earlier audits and have been **fixed** in the current codebase:

- **Config management:** `cloud_data_provider.py`, `cloud_instrument_storage.py`, `databento_adapter.py`, and `instrument_handler.py` now use `instruments_config` instead of `get_config(key, default)`.
- **datetime.utcnow():** `corporate_actions/models.py` uses `default_factory=lambda: datetime.now(timezone.utc)`.
- **os.getenv in instrument_handler:** `instrument_handler.py` uses `instruments_config.deployment_id` and `instruments_config.shard_launched_at` instead of `os.getenv`.

---

## High (P1) Issues

### 1. Imports Inside Functions

**Impact:** Performance overhead on each call, unclear dependencies.

**Affected Files (instrument paths corrected):**
- `instruments_service/cli/main.py` lines 19, 57
- `instruments_service/cli/handlers/corporate_actions_handler.py` line 474
- `instruments_service/cli/handlers/corporate_actions_production_handler.py` line 128
- `instruments_service/cli/handlers/corporate_actions_backfill_handler.py` line 93
- `instruments_service/cli/parser.py` line 275
- `instruments_service/app/core/dependency_checker.py` lines 16, 125, 194, 257
- `instruments_service/app/core/instruments_service.py` line 70
- `instruments_service/app/core/cloud_instrument_storage.py` lines 29, 39, 165-167, 317
- `instruments_service/app/venues/databento/databento_adapter.py` lines 51, 1511
- `instruments_service/app/venues/defi/*_adapter.py` (e.g., euler, fluid, morpho: Web3 imports)
- `instruments_service/app/venues/defi/the_graph_client.py` lines 34, 78
- `instruments_service/app/venues/defi/hyperliquid_adapter.py` line 81
- `instruments_service/app/venues/onchain_perps/hyperliquid_adapter.py` lines 79, 170, 286
- `instruments_service/corporate_actions/adapter.py` lines 50, 115
- `instruments_service/utils/ccxt_service.py` line 67

**Rule:** Workspace rules require imports at module top.

---

### 2. Broad Exception Handling

**Pattern:** `except Exception` used extensively without `@handle_api_errors` or `@handle_storage_errors`.

**Affected Files:**
- `instruments_service/cli/handlers/corporate_actions_handler.py` line 161: `except Exception: return []`
- `instruments_service/cli/handlers/instrument_handler.py` line 157: `except Exception: pass`
- `instruments_service/app/venues/databento/databento_adapter.py` line 1260: `except Exception: pass`
- `instruments_service/app/venues/defi/curve_rpc_adapter.py` lines 237, 245: bare `except Exception`
- `instruments_service/corporate_actions/adapter.py` line 514: bare `except Exception`
- Numerous `except Exception as e` blocks across adapters, orchestration, and handlers

**Fix:** Prefer `@handle_api_errors` and `@handle_storage_errors` for API and storage calls.

**Checklist Item:** 04b

---

### 3. Hardcoded Configuration Anti-Patterns

**Issue:** Large inline dictionaries for domain configuration.

**Locations:**
- `instruments_service/config.py` lines 1166-1255: `DATABENTO_VALID_PARENT_SYMBOLS`
- `instruments_service/config.py` lines 1258+: `DATABENTO_VALID_OPTIONS_SYMBOLS` and related mappings
- `config.py`: `KNOWN_ETFS`, `SPACE_TO_DOT_SYMBOLS` if still inline

**Impact:** Harder to maintain and test; consider moving to YAML or config files.

**Checklist Item:** 03c

---

### 4. Direct GCP Imports in Scripts and Tests

**Affected Files:**
- `scripts/ensure_test_buckets.py` line 11: `from google.cloud import storage`
- `scripts/check_envio_config.py` line 46: `from google.cloud import secretmanager`
- `tests/conftest.py` lines 61-62: `from google.cloud import storage`, `from google.oauth2 import service_account`

**Impact:** Breaks cloud-agnostic design and complicates AWS migration.

**Checklist Item:** 06b

---

## Medium (P2) Issues

### 5. Hardcoded Project IDs in Tests

**Locations:**
- `tests/integration/test_performance.py` lines 34, 67, 105, 146
- `tests/integration/test_cli_handlers.py` line 25
- `tests/conftest.py` (test helper `get_config(key, "central-element-323112")` fallbacks)

**Pattern:** `get_config("GCP_PROJECT_ID", "central-element-323112")` in test setup.

**Fix:** Use fixtures or env-driven config; avoid hardcoded project IDs.

---

### 6. os.getenv() in Scripts and Tests

**Locations:**
- `tests/conftest.py` line 36: `os.getenv("GOOGLE_APPLICATION_CREDENTIALS")`
- `scripts/run_quality_gates.py` lines 45-46, 209, 242-243, 272, 284
- `scripts/find_subgraph_ids.py` line 149: `os.getenv("THEGRAPH_API_KEY", "test-key")`
- `pytest_load_env.py` lines 33, 54, 58, 62

**Impact:** Bypasses unified config; acceptable for test/CI scripts but should be documented.

---

### 7. Test Helper get_config() Usage

**Location:** `tests/conftest.py` defines `get_config(key, default)` for integration/e2e tests.

**Context:** Tests use this instead of `instruments_config` for flexibility. Consider migrating to fixtures that inject config attributes for consistency.

---

## Positive Findings

✅ **Strengths:**
- Config extends `UnifiedCloudServicesConfig`; production code uses `instruments_config`
- Uses `validate_timestamp_date_alignment` before GCS upload
- Datetime operations use UTC (`datetime.now(timezone.utc)`)
- `setup_cloud_logging()` used with resource monitoring
- `GracefulShutdownHandler` implemented
- Exit codes handled correctly (`sys.exit(1)` on failures)
- Test structure: unit, integration, e2e, smoke
- Coverage configured in `pytest.ini`: `--cov=instruments_service --cov-report=term-missing --cov-fail-under=30`
- Uses `get_storage_client()` / unified-cloud-services in production paths (no direct GCP in core app code)

---

## Recommended Fix Priority

### Phase 1 (Weeks 1–2)
1. Move imports out of functions in highest-traffic modules (orchestration, handlers, databento adapter)
2. Replace bare `except Exception` in critical paths with `@handle_api_errors` and `@handle_storage_errors`
3. Replace direct GCP imports in scripts with `get_storage_client()` / `get_secret_client()` from unified-cloud-services

### Phase 2 (Weeks 2–3)
4. Extract `DATABENTO_VALID_*` and similar mappings to YAML or config files
5. Update tests to use fixtures instead of hardcoded project IDs
6. Gradually replace `os.getenv` in scripts with config attributes where feasible

### Phase 3 (Ongoing)
7. Broader use of error-handling decorators across adapters
8. Consolidate remaining import-inside-function patterns

---

## Impact Assessment

**Production Risk:** Low–Medium
- Core service logic complies with config and datetime rules
- Remaining issues are mainly technical debt in handlers, adapters, and test/script code

**Technical Debt:** Medium
- 20+ imports inside functions
- 60+ broad exception handlers
- Scripts and tests use legacy GCP and env patterns

**Effort Estimate:** 2–3 weeks (1 developer)

---

## Compliance Summary

| Checklist Category      | Status       | Issues                          |
|-------------------------|-------------|----------------------------------|
| Config Management (03b, 03c) | ✅ Passing | 0 (prod code fixed)              |
| UTC Datetime (04f)      | ✅ Passing   | 0                               |
| Error Handling (04b)    | ⚠️ Partial   | 1 P1 (broad exceptions)         |
| Cloud-Agnostic (06b)    | ⚠️ Partial   | 1 P1 (scripts/tests)            |
| Logging (04)            | ✅ Passing   | 0                               |
| Exit Codes (04g)        | ✅ Passing   | 0                               |
| Tests (07-12)           | ⚠️ Partial   | 2 P2 (hardcoded IDs, get_config) |

---

**Report Generated:** 2026-02-09
**Next Review:** After Phase 1 fixes completed
