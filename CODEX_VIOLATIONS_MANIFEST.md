# Codex Violations Manifest: instruments-service

**Total Violations**: 25
**Generated**: 2026-02-14T12:16:31.866388+00:00

## Summary by Type

- **standards_violation**: 25 violations

---

## Violations by Category

### Coding Standards

**Count**: 25

#### 1. os.getenv() usage in instruments-service/pytest_load_env.py

- **Priority**: P2-medium
- **Gap ID**: `COD-GETENV-instruments-service-pytest_load_env`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File uses os.getenv() which violates coding standards. Should extend UnifiedCloudServicesConfig instead.

**Affected Files**:
- `instruments-service/pytest_load_env.py`

**Codex Reference**: `06-coding-standards/README.md#configuration`

---

#### 2. Print statement in instruments-service/pytest_load_env.py

- **Priority**: P3-low
- **Gap ID**: `COD-PRINT-instruments-service-pytest_load_env`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
File contains print() statement. Should use logger.info() instead.

**Affected Files**:
- `instruments-service/pytest_load_env.py`

**Codex Reference**: `06-coding-standards/README.md`

---

#### 3. Import inside function in instruments-service/instruments_service/app/core/dependency_checker.py:124

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-dependency_checker-124`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 124. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/core/dependency_checker.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 4. Import inside function in instruments-service/instruments_service/app/core/dependency_checker.py:193

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-dependency_checker-193`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 193. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/core/dependency_checker.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 5. Import inside function in instruments-service/instruments_service/app/core/dependency_checker.py:256

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-dependency_checker-256`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 256. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/core/dependency_checker.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 6. Import inside function in instruments-service/instruments_service/app/core/instruments_service.py:69

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-instruments_service-69`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 69. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/core/instruments_service.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 7. Import inside function in instruments-service/instruments_service/app/core/cloud_data_provider.py:35

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-cloud_data_provider-35`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 35. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/core/cloud_data_provider.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 8. Using requests in async code in instruments-service/instruments_service/app/venues/onchain_perps/aster_adapter.py

- **Priority**: P2-medium
- **Gap ID**: `COD-REQUESTS-instruments-service-aster_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File imports requests library and has async functions. Should use aiohttp for async HTTP operations.

**Affected Files**:
- `instruments-service/instruments_service/app/venues/onchain_perps/aster_adapter.py`

**Codex Reference**: `06-coding-standards/PERFORMANCE_STANDARDS.md#async-http`

---

#### 9. Using requests in async code in instruments-service/instruments_service/app/venues/defi/morpho_adapter.py

- **Priority**: P2-medium
- **Gap ID**: `COD-REQUESTS-instruments-service-morpho_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File imports requests library and has async functions. Should use aiohttp for async HTTP operations.

**Affected Files**:
- `instruments-service/instruments_service/app/venues/defi/morpho_adapter.py`

**Codex Reference**: `06-coding-standards/PERFORMANCE_STANDARDS.md#async-http`

---

#### 10. Import inside function in instruments-service/instruments_service/app/venues/defi/the_graph_client.py:77

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-the_graph_client-77`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 77. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/app/venues/defi/the_graph_client.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 11. Import inside function in instruments-service/instruments_service/utils/ccxt_service.py:66

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-ccxt_service-66`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 66. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/utils/ccxt_service.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 12. Print statement in instruments-service/instruments_service/cli/parser.py

- **Priority**: P3-low
- **Gap ID**: `COD-PRINT-instruments-service-parser`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
File contains print() statement. Should use logger.info() instead.

**Affected Files**:
- `instruments-service/instruments_service/cli/parser.py`

**Codex Reference**: `06-coding-standards/README.md`

---

#### 13. os.getenv() usage in instruments-service/instruments_service/cli/handlers/instrument_handler.py

- **Priority**: P2-medium
- **Gap ID**: `COD-GETENV-instruments-service-instrument_handler`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File uses os.getenv() which violates coding standards. Should extend UnifiedCloudServicesConfig instead.

**Affected Files**:
- `instruments-service/instruments_service/cli/handlers/instrument_handler.py`

**Codex Reference**: `06-coding-standards/README.md#configuration`

---

#### 14. Import inside function in instruments-service/instruments_service/cli/handlers/instrument_handler.py:44

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-instrument_handler-44`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 44. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/cli/handlers/instrument_handler.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 15. Import inside function in instruments-service/instruments_service/cli/handlers/corporate_actions_production_handler.py:127

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-corporate_actions_production_handler-127`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 127. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/cli/handlers/corporate_actions_production_handler.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 16. Import inside function in instruments-service/instruments_service/cli/handlers/corporate_actions_handler.py:137

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-corporate_actions_handler-137`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 137. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/cli/handlers/corporate_actions_handler.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 17. Import inside function in instruments-service/instruments_service/cli/handlers/corporate_actions_backfill_handler.py:92

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-corporate_actions_backfill_handler-92`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 92. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/cli/handlers/corporate_actions_backfill_handler.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 18. Import inside function in instruments-service/instruments_service/corporate_actions/models.py:70

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-models-70`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 70. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/corporate_actions/models.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 19. Import inside function in instruments-service/instruments_service/corporate_actions/adapter.py:74

- **Priority**: P3-low
- **Gap ID**: `COD-IMPORT-instruments-service-adapter-74`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
Import statement found inside function at line 74. All imports should be at top of file.

**Affected Files**:
- `instruments-service/instruments_service/corporate_actions/adapter.py`

**Codex Reference**: `06-coding-standards/README.md#imports`

---

#### 20. Print statement in instruments-service/examples/query_instruments.py

- **Priority**: P3-low
- **Gap ID**: `COD-PRINT-instruments-service-query_instruments`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
File contains print() statement. Should use logger.info() instead.

**Affected Files**:
- `instruments-service/examples/query_instruments.py`

**Codex Reference**: `06-coding-standards/README.md`

---

#### 21. Bare except clause in instruments-service/build/lib/instruments_service/app/venues/defi/curve_rpc_adapter.py

- **Priority**: P2-medium
- **Gap ID**: `COD-BARE-instruments-service-curve_rpc_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File contains bare `except:` clause which violates coding standards. Should use `@handle_api_errors` decorator or specific exception types.

**Affected Files**:
- `instruments-service/build/lib/instruments_service/app/venues/defi/curve_rpc_adapter.py`

**Codex Reference**: `06-coding-standards/README.md#error-handling`

---

#### 22. Using requests in async code in instruments-service/build/lib/instruments_service/app/venues/defi/aster_adapter.py

- **Priority**: P2-medium
- **Gap ID**: `COD-REQUESTS-instruments-service-aster_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File imports requests library and has async functions. Should use aiohttp for async HTTP operations.

**Affected Files**:
- `instruments-service/build/lib/instruments_service/app/venues/defi/aster_adapter.py`

**Codex Reference**: `06-coding-standards/PERFORMANCE_STANDARDS.md#async-http`

---

#### 23. Using requests in async code in instruments-service/build/lib/instruments_service/app/venues/defi/hyperliquid_adapter.py

- **Priority**: P2-medium
- **Gap ID**: `COD-REQUESTS-instruments-service-hyperliquid_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ❌ No

**Description**:
File imports requests library and has async functions. Should use aiohttp for async HTTP operations.

**Affected Files**:
- `instruments-service/build/lib/instruments_service/app/venues/defi/hyperliquid_adapter.py`

**Codex Reference**: `06-coding-standards/PERFORMANCE_STANDARDS.md#async-http`

---

#### 24. datetime.now() without UTC in instruments-service/build/lib/instruments_service/app/venues/defi/uniswapv3_adapter.py

- **Priority**: P1-high
- **Gap ID**: `COD-UTC-instruments-service-uniswapv3_adapter`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
File uses datetime.now() without timezone.utc. Should use datetime.now(timezone.utc).

**Affected Files**:
- `instruments-service/build/lib/instruments_service/app/venues/defi/uniswapv3_adapter.py`

**Codex Reference**: `06-coding-standards/README.md#utc`

---

#### 25. Print statement in instruments-service/build/lib/instruments_service/cli/parser.py

- **Priority**: P3-low
- **Gap ID**: `COD-PRINT-instruments-service-parser`
- **Type**: standards_violation
- **Auto-fixable**: ✅ Yes

**Description**:
File contains print() statement. Should use logger.info() instead.

**Affected Files**:
- `instruments-service/build/lib/instruments_service/cli/parser.py`

**Codex Reference**: `06-coding-standards/README.md`

---

## CI Verification

**Last verified**: 2026-02-15
**Status**: FAILED (local quality gates)

### Issues (2026-02-15)

| Issue | Location | Description |
|-------|----------|-------------|
| os.getenv | pytest_load_env.py | GOOGLE_APPLICATION_CREDENTIALS |
| datetime.now() | cloud_instrument_storage.py | Use datetime.now(timezone.utc) |
| requests in async | check_envio_config.py, get_clickup_user_ids.py, clickup_import.py | Use aiohttp |
| Codex compliance | multiple | 4 violations |

---
