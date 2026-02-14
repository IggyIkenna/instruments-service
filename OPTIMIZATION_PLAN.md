# Instruments-Service Quality Gates Optimization Plan

## Current Performance

- **Local Quality Gates**: ~4 minutes
- **Cloud Build**: ~30 minutes
- **Total Tests**: 686 (639 unit tests)

## Optimization Opportunities

### 1. Parallel Test Execution ⚡ (High Impact)

**Current**: Tests run sequentially
**Solution**: Add `pytest-xdist` for parallel execution
**Expected Speedup**: 2-4x faster (depending on CPU cores)
**Implementation**:

- Add `pytest-xdist>=3.6.0` to dev dependencies
- Update quality gates to use `-n auto` (auto-detect CPU cores)
- Update Cloud Build to use `-n 4` (4 parallel workers)

### 2. Consolidate Duplicate Path Format Tests 🔄 (Medium Impact)

**Current**: Multiple tests checking similar path format patterns:

- `test_cloud_agnostic_paths.py::test_cloud_instrument_storage_path_format`
- `test_cloud_agnostic.py::test_path_format_uses_key_equals_value`
- `test_cloud_agnostic.py::test_instrument_availability_path_format`
- `test_cloud_instrument_storage.py::test_store_instruments_venue_path_format`

**Solution**: Consolidate into single comprehensive test suite
**Expected Speedup**: ~5-10 seconds (fewer test setup/teardown cycles)

### 3. Optimize Cloud Build ⚙️ (High Impact)

**Current**: Sequential steps, no caching, slow Docker build
**Solutions**:

- **Dependency Caching**: Cache pip dependencies between builds
- **Parallel Steps**: Run linting and test preparation in parallel
- **Docker Layer Caching**: Use Cloud Build's built-in Docker caching
- **Skip Docker Build in PRs**: Only build Docker image on main branch

**Expected Speedup**: 10-15 minutes reduction

### 4. Enhanced Quick Mode 🚀 (Medium Impact)

**Current**: `--quick` runs unit tests only
**Enhancement**:

- Skip slow unit tests (marked with `@pytest.mark.slow`)
- Skip integration/e2e/smoke tests
- Use parallel execution even in quick mode
- Skip expensive fixture setup

**Expected Speedup**: 50-70% faster quick mode

### 5. Test Categorization 🏷️ (Low Impact, High Value)

**Current**: No test markers for slow/expensive tests
**Solution**: Add markers:

- `@pytest.mark.slow` - Tests taking >1 second
- `@pytest.mark.integration` - Integration tests (already exists)
- `@pytest.mark.e2e` - E2E tests (already exists)
- `@pytest.mark.smoke` - Smoke tests (already exists)

**Benefit**: Better test organization and selective execution

### 6. Fixture Optimization 🔧 (Low-Medium Impact)

**Current**: Some fixtures recreated per test
**Solution**:

- Review fixture scopes (session > module > class > function)
- Cache expensive operations (API clients, config loading)
- Use `pytest.fixture(scope="session")` for immutable fixtures

**Expected Speedup**: 10-20 seconds

## Implementation Priority

1. **Phase 1 (Immediate - High ROI)**:
   - Add pytest-xdist for parallel execution
   - Optimize Cloud Build caching
   - Enhance quick mode

2. **Phase 2 (Short-term - Medium ROI)**:
   - Consolidate duplicate tests
   - Add test markers for slow tests
   - Optimize fixture scopes

3. **Phase 3 (Long-term - Low ROI but High Value)**:
   - Refactor test organization
   - Add test performance monitoring
   - Implement test sharding for CI

## Expected Results

After Phase 1:

- **Local Quality Gates**: ~1-2 minutes (50-75% faster)
- **Cloud Build**: ~15-20 minutes (33-50% faster)

After Phase 2:

- **Local Quality Gates**: ~45-90 seconds (75-85% faster)
- **Cloud Build**: ~12-15 minutes (50-60% faster)
