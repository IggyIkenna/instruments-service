# Quality Gate Bypass Audit — instruments-service

Inventory of all exceptions, exclusions, and handling that bypass or relax quality gate checks. Use this to decide which to keep, fix, or remove.

**Status:** Section 7 classifies valid (keep) vs hardening (fix). Aligns with `.cursor/plans/quality-gates-audit-factors-propagation.plan.md` Phase 7 — fix root causes, no shortcuts. Propagate audit template to all Python repos.

---

## 1. Quality Gate Script Exclusions (quality-gates.sh)

### 1.1 Path/Glob Exclusions (checks never run on these paths)

| Check | Excluded Paths | Rationale |
|-------|----------------|-----------|
| **print()** | `tests/**`, `scripts/**`, `examples/**`, `pytest_load_env.py` | Tests/scripts may use print |
| **os.getenv()** | `tests/**`, `scripts/**`, `pytest_load_env.py` | Tests/scripts may use env directly |
| **datetime.now()** | `docs/**`, `*.md` | Docs only |
| **bare except** | `tests/**` | Tests may catch broadly |
| **google.cloud** | `tests/**` | Tests may mock |
| **empty fallbacks** | `tests/**`, `scripts/**` | Tests/scripts exempt |
| **imports inside functions** | `tests/**`, `scripts/**` + **14 whitelisted files** (see 1.2) | Lazy imports allowed |
| **Any/object** | `tests/**`, `scripts/**` | Tests exempt |
| **project ID** | `tests/**` | Tests exempt |
| **requests in async** | `scripts/**`, `**/defi/morpho_adapter.py`, `**/onchain_perps/aster_adapter.py` | Known exceptions |
| **asyncio.run() in loops** | `examples/**`, `scripts/**`, `**/venues/defi/*`, `**/cli/**` | Entry points / CLI |
| **file size** | `scripts/*`, `.venv/*`, `deps/*`, `.git/*`, `build/*` | Scripts exempt per codex |
| **pip-audit** | (skip if pip_audit not installed) | Optional tool |

### 1.2 Import Check Whitelist (14 files exempt from “imports inside functions”)

These files are **excluded** from the import-inside-functions check:

| File | Reason |
|------|--------|
| `**/adapter_loader.py` | Lazy adapter loading by design |
| `**/__init__.py` | Lazy submodule loading |
| `**/dependency_checker.py` | Circular import |
| `**/instruments_service.py` | Circular import |
| `**/symbol_parser.py` | TYPE_CHECKING block |
| `**/canonical_key_generator.py` | TYPE_CHECKING block |
| `**/live_mode_handler.py` | Circular import |
| `**/cloud_instrument_storage.py` | Optional deps |
| `**/parser.py` | Optional deps |
| `**/main.py` | dotenv before imports |
| `**/ccxt_service.py` | Optional VenueMapping |
| `**/corporate_actions_handler.py` | Circular import |
| `**/corporate_actions_backfill_handler.py` | Circular import |
| `**/corporate_actions_production_handler.py` | Circular import |
| `**/corporate_actions_update_handler.py` | Circular import |

### 1.3 grep -v Exclusions (lines matching these patterns are ignored)

| Check | Excluded Pattern | Effect |
|-------|------------------|--------|
| **Any/object** | `dict[str, Any]` | Allows dict[str, Any] anywhere |
| **Any/object** | `type: ignore` | Any line with type: ignore bypasses the check |
| **project ID** | `GCP_PROJECT_ID` | GOOGLE_CLOUD_PROJECT allowed if GCP_PROJECT_ID also present |

---

## 2. Inline Code Bypasses (instruments_service/)

### 2.1 type: ignore[reportAny] — Any/object quality gate bypass

| File | Line | Code | Purpose |
|------|------|------|---------|
| `corporate_actions/adapter.py` | 277 | `calendar: object = stock.calendar  # type: ignore[reportAny]` | yfinance calendar type |
| `corporate_actions/models.py` | 70 | `def validate_amount(cls, v: object) -> float:  # type: ignore[reportAny]` | Pydantic validator |
| `app/core/instruments_service.py` | 339 | `adapter: object \| None = None  # type: ignore[reportAny]` | Dynamic adapter type |
| `app/core/processors/defi_processor.py` | 41–43 | `venue_mapping: object` etc. | Protocol stubs |
| `app/core/processors/derived_fields_populator.py` | 17–20 | `venue_mapping: object` etc. | Protocol stubs |
| `cli/base_handler.py` | 68 | `def run(self, **kwargs: object)` | Handler interface |
| `cli/handlers/instrument_handler.py` | 111, 127 | `**kwargs: object` | Handler args |
| `cli/handlers/live_mode_handler.py` | 69 | `def run(self, **kwargs: object)` | Handler interface |
| `cli/handlers/corporate_actions_*.py` | 4 files | `**kwargs: object` | Handler args |
| `cli/handlers/generate_date_views_handler.py` | 153 | `**kwargs: object` | Handler args |
| `events.py` | 12 | `def log_event(..., **kwargs: Any)` | Event logging |
| `utils/ccxt_service.py` | 119 | `def get_ccxt_exchange(...) -> object \| None` | CCXT exchange type |
| `__init__.py` | 20 | `def __getattr__(name: str) -> Any` | Lazy module loading |

### 2.2 type: ignore[assignment] — Type checker only

| File | Line | Code | Purpose |
|------|------|------|---------|
| `corporate_actions/adapter.py` | 189 | `splits_df: pd.Series = stock.splits` | yfinance return type |
| `corporate_actions/adapter.py` | 290 | `earnings_df: pd.DataFrame = stock.earnings_dates` | yfinance return type |

### 2.3 pyright: ignore — Pyright/Pylance only (not in quality gates)

| File | Line | Code | Purpose |
|------|------|------|---------|
| `app/core/processors/defi_processor.py` | 273 | `service._get_manual_ccxt_fallback(...)` | Private method access |
| `utils/ccxt_service.py` | 184 | `cast(dict[str, Any], exchange.load_markets())` | CCXT untyped |
| `utils/__init__.py` | 15–28 | `get_http_session`, `clear_pool`, etc. | Optional UCS imports |

### 2.4 noqa — Ruff linter only

| File | Line | Code | Purpose |
|------|------|------|---------|
| `corporate_actions/adapter.py` | 281 | `_ = calendar.loc["Earnings Date"]  # noqa: F841` | Intentional unused |
| `__init__.py` | 23 | `import instruments_service.cli  # noqa: F401` | Side-effect import |

---

## 3. Ruff Config Bypasses (pyproject.toml)

| Rule | Scope | Effect |
|------|--------|--------|
| **E501** (line length) | Global | Ignored; formatter handles |
| **E722** (bare except) | Global | Bare `except:` allowed |
| **E402** (module level import) | `cli/main.py`, `tests/conftest.py` | Imports not at top allowed |
| **E722** (bare except) | `scripts/*` | Bare except allowed in scripts |

---

## 4. Docstring / Pattern Bypasses

| File | Change | Effect |
|------|--------|--------|
| `schemas/output_schemas.py` | Docstring example imports commented: `# from ... import` | Avoids “imports inside functions” match (docstring was indented) |

---

## 5. Test Bypasses (pytest.skip / skipif)

| File | Usage | Reason |
|------|-------|--------|
| `conftest.py` | 6× `pytest.skip()` | GCP creds, bucket access, Secret Manager, Tardis key |
| `test_event_logging.py` | 2× `pytest.skip()` | No service-specific events |
| `test_cloud_agnostic_paths.py` | 1× `pytest.skip()` | Package dir not found |
| `test_adapter_loader.py` | 1× `pytest.skip()` | HyperliquidBaseClient removed |
| `test_corporate_actions.py` | 1× `@pytest.mark.skipif` | Condition-based |
| `test_instrument_generation_e2e.py` | 1× `pytest.skip()` | E2E condition |
| `test_shard_combinatorics.py` | 3× `pytest.skip()` | Config / env |
| `test_performance.py` | 4× `@pytest.mark.skipif` | Env/feature flags |
| `test_cli_handlers.py` | 4× `@pytest.mark.skipif` | Env/feature flags |

---

## 6. Summary Counts

| Category | Count |
|----------|-------|
| **type: ignore[reportAny]** (Any/object bypass) | 18 |
| **type: ignore[assignment]** | 2 |
| **pyright: ignore** | 6 |
| **noqa** | 2 |
| **Ruff per-file-ignores** | 3 files |
| **Ruff global ignores** | 2 rules |
| **Import whitelist files** | 14 |
| **Path exclusions** (per check) | 3–6 paths each |
| **pytest.skip / skipif** | ~24 |

---

## 7. Valid vs Hardening — Classification

### 7.1 ✅ Valid (Acceptable — No Action)

| Item | Rationale |
|------|-----------|
| **Path exclusions: tests/** | Tests are exempt from production rules (print, os.getenv, bare except, etc.). Standard practice. |
| **Path exclusions: scripts/** | Codex exempts scripts from line count; scripts often need different patterns. |
| **Path exclusions: examples/** | Examples/documentation only. |
| **pytest_load_env.py** | Test env setup. |
| **Import whitelist: adapter_loader.py** | Lazy loading by design for optional adapters. |
| **Import whitelist: __init__.py** | `__getattr__` lazy loading is standard Python. |
| **Import whitelist: symbol_parser, canonical_key_generator** | `TYPE_CHECKING` blocks — standard pattern for circular imports. |
| **Import whitelist: main.py** | `load_dotenv()` must run before config imports. |
| **dict[str, Any] exclusion** | Codex explicitly allows for non-finite nested dicts. |
| **Ruff E501** | Formatter handles line length; no conflict. |
| **Ruff E722 in scripts/** | Codex exempts scripts; bare except acceptable there. |
| **noqa F401** (side-effect import) | Standard pattern for `import x` when `x` is used for side effects. |
| **noqa F841** (intentional unused) | Acceptable when variable is intentionally unused. |
| **pytest.skip: GCP creds / bucket / Secret Manager** | Cannot run without credentials; skip is correct. |
| **pytest.skip: HyperliquidBaseClient removed** | Feature removed; skip is correct. |
| **pytest.skip: Tardis API key** | Cannot run without key; skip is correct. |
| **pip-audit skip when not installed** | Optional tool; skip is acceptable. |
| **file size: scripts exempt** | Per codex file-splitting-guide. |
| **instrument_handler date loop: except (..., Exception)** | In-flight validation — catch any service error, log, continue with other dates. Per codex validation-patterns. |

### 7.2 🚩 Hardening Flags (Audit Concerns — Consider Fixing)

| Item | Priority | Action |
|------|----------|--------|
| **type: ignore[reportAny]** (18 instances) | **High** | Replace with `TypedDict`, `Protocol`, or concrete types. Reduces type safety. |
| **Import whitelist: circular imports** (dependency_checker, instruments_service, live_mode_handler, corporate_actions handlers) | **High** | Refactor to reduce circular imports; consider dependency injection. |
| **Ruff E722 (bare except) — global** | **High** | Bare `except:` hides errors. Use `except Exception` or specific exceptions. |
| **Ruff E402** (main.py, conftest) | **Medium** | Restructure so imports can be at top. |
| **morpho_adapter.py, aster_adapter.py** (requests in async) | **Medium** | If they use `requests` in async code, migrate to `aiohttp`. |
| **asyncio.run exclusion: cli/** entire dir** | **Medium** | Broad exclusion may hide real violations. Narrow to specific entry points. |
| **pyright: ignore — private method access** (defi_processor) | **Medium** | Prefer public API or proper Protocol. |
| **pyright: ignore — utils/__init__.py** (6 instances) | **Medium** | Optional UCS fallback; consider explicit feature flag or fail-fast. |
| **type: ignore[assignment]** (yfinance) | **Low** | Third-party untyped; add py.typed stub or vendor types if needed. |
| **docstring workaround** (output_schemas) | **Low** | Hacky; fix docstring format or add to whitelist explicitly. |
| **pytest.skip: "No service-specific events"** | **Low** | May indicate missing config; verify event markers exist. |
| **pytest.skip: "package directory not found"** | **Low** | Environment setup; document or fix test discovery. |
| **pytest.skip: shard combinatorics** | **Low** | Config-dependent; ensure config is present in CI. |

### 7.3 Summary

| Category | Valid | Hardening |
|----------|-------|-----------|
| Path/glob exclusions | 8 | 1 |
| Import whitelist | 8 | 6 |
| grep -v / pattern exclusions | 1 | 2 |
| Inline type: ignore | 0 | 18 |
| Ruff config | 2 | 2 |
| Test skips | 4 | 5 |
| **Total** | **23** | **34** |

---

## 8. Decision Checklist

**See also:** `.cursor/plans/quality-gates-audit-factors-propagation.plan.md` Phase 7 — bypass hardening tasks.

Use this to decide what to change:

- [ ] **type: ignore[reportAny]** — Replace with proper types (TypedDict, Protocol, etc.)?
- [ ] **Circular import whitelist** — Refactor to reduce?
- [ ] **Ruff E722 (bare except)** — Disallow globally; keep only in scripts?
- [ ] **Ruff E402** — Move imports to top in main.py / conftest?
- [ ] **morpho/aster adapters** — Migrate requests → aiohttp?
- [ ] **pyright: ignore** — Fix underlying type issues?
- [ ] **Test skips** — Replace with fixtures or env setup where possible?
