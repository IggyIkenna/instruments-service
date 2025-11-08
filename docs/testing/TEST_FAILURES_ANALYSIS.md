# Test Failures Analysis

**Date**: 2025-01-27  
**Total Tests**: 273  
**Passing**: 238  
**Failing**: 21  
**Errors**: 14  
**Coverage**: 75.82% ✅ (target: 75%+)

## Summary

The test suite has achieved the 75% coverage target, but there are 35 test failures/errors that need to be addressed. These fall into several categories:

1. **Mock/Import Issues** (14 errors, 3 failures)
2. **Test Logic Issues** (3 failures)
3. **Missing Test Data** (1 failure)

---

## 1. Unit Test Failures - Mock/Import Issues

### 1.1 `test_instrument_handler.py` (12 errors)

**Files Affected**: `tests/unit/test_instrument_handler.py`

**Root Cause**: Tests are trying to patch `CloudDataProvider` from the wrong module path.

**Error**:
```
AttributeError: <module 'instruments_service.cli.handlers.instrument_handler'> 
does not have the attribute 'CloudDataProvider'
```

**Affected Tests**:
- `test_run_delegates_to_execute`
- `test_execute_instrument_generation_success`
- `test_execute_instrument_generation_skip_future_date`
- `test_execute_instrument_generation_skip_existing`
- `test_execute_instrument_generation_force_mode`
- `test_execute_instrument_generation_no_instruments`
- `test_execute_instrument_generation_storage_failure`
- `test_execute_instrument_generation_exception_handling`
- `test_generate_instruments_for_date`
- `test_generate_instruments_for_date_all_exchanges`
- `test_generate_instruments_for_date_exchange_error`
- `test_cleanup`

**Fix Required**: 
- Check if `CloudDataProvider` is actually imported in `instrument_handler.py`
- Update patch paths to use the correct import location (likely `instruments_service.app.core.cloud_data_provider.CloudDataProvider`)

---

### 1.2 `test_cli_main.py` (10 failures)

**Files Affected**: `tests/unit/test_cli_main.py`

**Root Cause**: Tests are trying to patch attributes that don't exist on the `main` function.

**Error**:
```
AttributeError: <function main at 0x...> does not have the attribute 'main'
AttributeError: <function main at 0x...> does not have the attribute 'run_cli'
```

**Affected Tests**:
- `test_main_success`
- `test_main_with_query_mode`
- `test_main_with_all_query_args`
- `test_main_failure_status`
- `test_main_exception_handling`
- `test_run_cli_success`
- `test_run_cli_keyboard_interrupt`
- `test_run_cli_exception`
- `test_main_entry_point_success`
- `test_main_entry_point_failure`

**Fix Required**:
- Review `instruments_service/cli/main.py` to understand the actual structure
- Update tests to patch the correct functions/modules
- May need to patch `sys.argv` or use `subprocess` for CLI testing instead of direct function patching

---

### 1.3 `test_cli_handlers_init.py` (3 failures)

**Files Affected**: `tests/unit/test_cli_handlers_init.py`

**Root Cause**: Tests are running in isolation, so the handler registry is empty (only contains test-mode from previous test).

**Error**:
```
ValueError: Unsupported mode: instruments. Supported modes: ['test-mode']
```

**Affected Tests**:
- `test_get_handler_for_mode_instruments`
- `test_get_handler_for_mode_instruments_query`
- `test_get_handler_for_mode_unsupported`

**Fix Required**:
- Ensure handler registry is properly initialized before tests
- May need to import handlers explicitly or use fixtures to register handlers
- Check if handlers are registered lazily and need to be triggered

---

### 1.4 `test_cli_handlers.py` (Integration) (2 errors, 1 failure)

**Files Affected**: `tests/integration/test_cli_handlers.py`

**Root Cause**: Tests are trying to patch `InstrumentsClient` from the wrong module path.

**Error**:
```
AttributeError: <module 'instruments_service.cli.handlers.instruments_query_handler'> 
does not have the attribute 'InstrumentsClient'
```

**Affected Tests**:
- `test_query_handler_initialization` (FAILED)
- `test_query_handler_list_query` (ERROR)
- `test_query_handler_summary_query` (ERROR)

**Fix Required**:
- `InstrumentsClient` is imported inside `InstrumentsQueryHandler.__init__` (lazy import)
- Update patch path to: `instruments_service.clients.instruments_client.InstrumentsClient`
- Or patch it before the handler is instantiated

---

## 2. Integration Test Failures - Test Logic Issues

### 2.1 `test_instrument_processing.py` (1 failure)

**Files Affected**: `tests/integration/test_instrument_processing.py`

**Root Cause**: Test expects `fetch_exchange_instruments` to return a `dict`, but it actually returns a `tuple` `(dict, int)`.

**Error**:
```
AssertionError: assert False
+ where False = isinstance(({'aaveusdt': {...}, ...}, 530), dict)
```

**Affected Test**:
- `test_fetch_exchange_instruments`

**Fix Required**:
- Update test to handle tuple return: `instruments, date_filtered_count = await service.fetch_exchange_instruments(...)`
- Or update assertion to check for tuple: `assert isinstance(result, tuple)`

---

### 2.2 `test_instruments_client.py` (1 failure)

**Files Affected**: `tests/integration/test_instruments_client.py`

**Root Cause**: Test data (mocked DataFrame) is missing the `ccxt_symbol` column that `get_summary_stats` expects.

**Error**:
```
KeyError: 'ccxt_symbol'
```

**Affected Test**:
- `test_get_summary_stats`

**Fix Required**:
- Update test fixture to include `ccxt_symbol` column in the mocked DataFrame
- Ensure all required columns are present: `['instrument_key', 'venue', 'instrument_type', 'ccxt_symbol', ...]`

---

## 3. E2E Test Failures - Missing Test Data

### 3.1 `test_instrument_generation_e2e.py` (1 failure)

**Files Affected**: `tests/e2e/test_instrument_generation_e2e.py`

**Root Cause**: `query_instruments` method returns empty DataFrame because BigQuery queries were removed (instruments are now GCS-only).

**Error**:
```
AssertionError: Should be able to query instruments back from test bucket
assert 0 > 0
```

**Affected Test**:
- `test_instrument_generation_e2e`

**Fix Required**:
- Update test to use GCS download methods instead of `query_instruments`
- Use `CloudDataProvider.get_instruments_from_gcs()` or `InstrumentsClient._download_from_gcs()`
- Or update `query_instruments` to actually query from GCS (if that's the intended behavior)

---

## Summary by Category

| Category | Count | Severity | Priority |
|----------|-------|----------|----------|
| Mock/Import Path Issues | 17 | Medium | High |
| Test Logic Issues | 2 | Low | Medium |
| Missing Test Data | 1 | Low | Medium |
| **Total** | **20** | - | - |

## Recommended Fix Order

1. **High Priority**: Fix mock/import path issues (17 tests)
   - These are likely quick fixes once the correct import paths are identified
   - Will significantly improve test reliability

2. **Medium Priority**: Fix test logic issues (2 tests)
   - Update assertions to match actual return types
   - Add missing columns to test fixtures

3. **Low Priority**: Fix E2E test (1 test)
   - Update to use GCS download methods
   - May require understanding the intended query behavior

## Notes

- **Coverage is not affected**: These failures don't impact the 75.82% coverage achievement
- **Most failures are test infrastructure issues**: Not actual code bugs
- **Integration tests may require GCP setup**: Some failures might be environment-related
- **Mock patching needs review**: Many failures stem from incorrect patch paths

## Next Steps

1. Review actual import structure in handler files
2. Fix patch paths in unit tests
3. Update test assertions to match actual return types
4. Add missing columns to test fixtures
5. Update E2E test to use GCS download methods

