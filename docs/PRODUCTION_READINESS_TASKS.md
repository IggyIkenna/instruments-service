# instruments-service - Production Readiness Tasks

**Epic:** DATA-IO-PROD-001
**GitHub Project:** [#9](https://github.com/users/IggyIkenna/projects/9)
**Status:** 68 granular tasks identified
**Current Completion:** ~40/68 tasks complete (59%)

---

## 🚨 P0-Critical Tasks (Must Complete First)

### Task 1.1: Split config.py (1,929 lines → <1,500)

**Current State:**
- File: `instruments_service/config.py` - 1,929 lines (violates 1,500 line limit)
- Contains: SP500_TICKERS (lines 50-550, 500 lines), TRADFI_VENUE_MAP (lines 600-750, 150 lines), DEFI_PROTOCOL_CONFIGS (lines 800-950, 150 lines), duplicate InstrumentsServiceConfig class (lines 1000-1100)

**Target State:**
- New file: `instruments_service/config/instrument_definitions.py` with all constants
- Keep only: `InstrumentsServiceConfig` in `config/service_config.py`
- All imports updated across codebase

**Broken into 7 bite-sized tasks:**
1. Extract SP500_TICKERS → instrument_definitions.py (30min)
2. Extract TRADFI_VENUE_MAPPINGS → instrument_definitions.py (20min)
3. Extract DEFI_PROTOCOL_CONFIGS → instrument_definitions.py (20min)
4. Remove duplicate class from config.py (15min)
5. Update imports in instrument_handler.py (10min)
6. Update imports in cli/main.py (10min)
7. Verify quality gates pass (15min)

**Total:** 2h

---

### Task 1.3: Add Test Coverage Thresholds

**Current State:**
- File: `scripts/quality-gates.sh` line 45: `pytest tests/`
- No coverage enforcement, unknown actual coverage %

**Target State:**
- Command: `pytest --cov=instruments_service --cov-report=term --cov-report=html --cov-fail-under=35 tests/`
- Coverage report generated on every run
- Quality gates fail if coverage <35%

**Tasks:**
1. Update quality-gates.sh with --cov flags (20min)
2. Update GitHub Actions workflow with coverage (15min)

**Total:** 35min

---

### Task 1.4: Increase Test Coverage to 50%+

**Current State:**
- Coverage unknown (no reports generated)
- Missing tests for:
  - `app/core/instrument_generation_engine.py`
  - `app/adapters/ccxt_adapter.py`
  - `app/adapters/databento_adapter.py`
  - `app/adapters/the_graph_adapter.py`

**Target State:**
- Overall coverage >50% for `instruments_service/` package
- Each adapter has unit test with 80%+ coverage
- All core modules tested

**Tasks (11 total):**
1. Run coverage report, identify gaps (10min)
2. Add unit tests for InstrumentGenerationEngine (1h)
   - `test_generate_instruments_cefi()`
   - `test_generate_instruments_tradfi()`
   - `test_generate_instruments_defi()`
3. Add unit tests for CCXTInstrumentAdapter (1h)
   - Mock CCXT calls
   - Test normalization logic
4. Add unit tests for DatabentoCategoryAdapter (45min)
5. Add unit tests for TheGraphAdapter (45min)
6. Verify coverage >50% (10min)

**Total:** ~4h

---

### Task 5.1: Create Per-Category Terraform Configs

**Current State:**
- Single deployment: `terraform/services/instruments-service/main.tf`
- Deploys one instance, no category isolation

**Target State:**
- 3 separate deployments:
  - `terraform/services/instruments-service/cefi/main.tf`
  - `terraform/services/instruments-service/tradfi/main.tf`
  - `terraform/services/instruments-service/defi/main.tf`
- Each writes to category-specific bucket: `instruments-store-{cefi|tradfi|defi}-{project}`
- Machine specs: 2 core, 4GB per deployment

**Tasks (7 total):**
1. Create cefi/main.tf (1h)
2. Create tradfi/main.tf (45min)
3. Create defi/main.tf (45min)
4. Update orchestrator in deploy.py for per-category (1h)
5. Test instruments-cefi deployment (30min)
6. Test instruments-tradfi deployment (30min)
7. Test instruments-defi deployment (30min)

**Total:** 5h

---

## 🟡 P1-High Tasks (Complete After P0)

### Task 1.5: Standardize Event Logging

**Current State:**
- Most files use direct `unified-events-interface` imports ✅
- Some fallback patterns may exist (audit flagged download_handler.py in MTDH, not this service)

**Target State:**
- 100% direct imports from `unified-events-interface`
- No try-except fallbacks
- All 11 lifecycle events present

**Tasks (2 total):**
1. Grep for any fallback patterns (10min)
2. Remove fallbacks if found (10min)

**Total:** 20min

---

### Task 1.6: Add Timing Metadata to Lifecycle Events

**Current State:**
- Events have timestamp only: `log_event("STARTED")`
- No duration tracking, no nested spans

**Target State:**
- All events include timing metadata:
  ```python
  log_event("STARTED", timing={"start_ts": "2026-02-19T14:30:00Z"})
  log_event("COMPLETED", timing={"end_ts": "...", "duration_ms": 1234})
  ```
- Performance context: `performance={"trace_id": "...", "phase": "validation"}`

**Tasks (10 total):**
1. Update STARTED event with timing (15min)
2. Update VALIDATION_STARTED event (15min)
3. Update VALIDATION_COMPLETED event with duration (15min)
4. Update INSTRUMENT_LOADING_STARTED event (15min)
5. Update INSTRUMENT_LOADING_COMPLETED event (15min)
6. Update PROCESSING_STARTED event (15min)
7. Update PROCESSING_COMPLETED event (15min)
8. Update UPLOAD_STARTED event (15min)
9. Update UPLOAD_COMPLETED event (15min)
10. Update COMPLETED event with total duration (15min)

**Total:** 2.5h

---

### Task 1.7: Integrate PerformanceTracer

**Current State:**
- No performance tracing
- No detailed timing breakdown by phase

**Target State:**
- `PerformanceTracer` integrated in cli/main.py
- All phases wrapped with start_span()/end_span()
- Traces exported to GCS for analytics

**Tasks (1 for this service):**
1. Add PerformanceTracer to entrypoint (45min)
   - Wrap validation phase
   - Wrap instrument loading phase
   - Wrap processing phase
   - Wrap upload phase
   - Export trace to GCS on completion

**Total:** 45min

---

### Task 4.2: Add Hot-Reload Metadata to Config Fields

**Current State:**
- Config fields have no metadata
- Unknown which fields require restart vs hot-reload

**Target State:**
- All fields tagged: `metadata={"hot_reloadable": bool, "requires_restart": bool}`
- Example:
  - `max_workers`: `hot_reloadable=False` (affects shard initialization)
  - `log_level`: `hot_reloadable=True`
  - `instruments_bucket`: `requires_restart=True`

**Tasks (1):**
1. Add metadata to all config fields in config/service_config.py (30min)

**Total:** 30min

---

### Task 4.5: Integrate ConfigUpdateListener

**Current State:**
- No config update listening
- Config loaded from .env only on startup

**Target State:**
- ConfigUpdateListener subscribed to `config-updates-{project}` Pub/Sub topic
- Hot-reloadable changes applied in-memory
- Restart-required changes flag service for restart

**Tasks (2):**
1. Add ConfigUpdateListener to cli/main.py (30min)
2. Add CONFIG_VERSION env var loading from GCS (45min)

**Total:** 1.25h

---

### Task 5.5: Add Mode-Aware Path Construction

**Current State:**
- GCS writer always writes to root: `gs://bucket/instrument_availability/by_date/day={date}/`
- No --mode flag in CLI

**Target State:**
- Batch mode: writes to root (existing behavior)
- Live mode: writes to `gs://bucket/live/instrument_availability/by_date/day={date}/hr={hr}/`
- CLI accepts `--mode batch|live`

**Tasks (2):**
1. Update gcs_writer.py to support mode parameter (30min)
2. Add --mode flag to CLI (20min)

**Total:** 50min

---

### Task 10.4: Create /health Endpoint for Live Mode

**Current State:**
- No /health endpoint
- No health monitoring for live deployments

**Target State:**
- Flask/FastAPI endpoint at `/health`
- Returns: `{status, mode, uptime_seconds, streaming_counters, persistence_counters}`

**Tasks (1):**
1. Create api/health.py with /health endpoint (1.5h)
   - Add streaming_counters (per venue)
   - Add persistence_counters
   - Add health status logic

**Total:** 1.5h

---

## 📊 Task Summary by Priority

| Priority | Count | Estimated Time | % of Total |
|----------|-------|----------------|------------|
| P0-Critical | 28 | 45h | 41% |
| P1-High | 32 | 35h | 47% |
| P2-Medium | 8 | 8h | 12% |
| **Total** | **68** | **88h** | **100%** |

---

## 📅 Recommended Execution Order

### Day 1: Foundational (P0)
1. Tasks 1.1.1-1.1.7: Split config.py (2h)
2. Tasks 1.3.1-1.3.2: Add coverage thresholds (35min)
3. Tasks 5.1.1-5.1.3: Create Terraform configs (2.5h)
4. Tasks 4.2.1: Add hot-reload metadata (30min)

**Total Day 1:** ~6h

### Day 2: Testing & Library Work
1. Tasks 1.4.1-1.4.6: Add test coverage (4h)
2. Tasks 1.7.3: Integrate PerformanceTracer (45min)
3. Tasks 1.6.1-1.6.5: Add timing to events (1.25h)

**Total Day 2:** ~6h

### Day 3: Config UI & Deployment
1. Tasks 4.5.1-4.5.2: Config UI integration (1.25h)
2. Tasks 5.1.4-5.1.7: Test per-category deployments (2.5h)
3. Tasks 5.5.1-5.5.2: Mode-aware paths (50min)

**Total Day 3:** ~5h

### Day 4: Live Mode Prep
1. Task 10.4.1: Create /health endpoint (1.5h)
2. Remaining P1-high tasks

**Total Day 4:** ~4h

---

## ✅ Already Complete (from Audit)

- ✅ Event logging infrastructure (uses unified-events-interface)
- ✅ Batch mode fully functional
- ✅ Category-specific GCS buckets exist
- ✅ CCXT/Databento/The Graph adapters implemented
- ✅ Quality gates infrastructure (just needs coverage added)
- ✅ Terraform deployment configs exist (just need per-category split)

---

## References

- **Granular Breakdown:** `unified-trading-codex/11-project-management/epic-breakdowns/data-io-granular-breakdown.md`
- **Epic Document:** `unified-trading-codex/11-project-management/epics/data-io-production-readiness-epic.md`
- **GitHub Project:** https://github.com/users/IggyIkenna/projects/9
- **Architecture:** `unified-trading-codex/04-architecture/deployment-grouping.md`
