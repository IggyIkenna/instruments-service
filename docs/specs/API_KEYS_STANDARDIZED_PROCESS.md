<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Standardized API Key Process — instruments-service

Single source of truth for how API keys are resolved, when they are required, and how to add new ones.

> **Source of truth:** `unified-trading-codex/02-data/instruments-and-api-keys-standard.md` and `.cursor/rules/instruments-domain-and-api-keys.mdc`. This doc is service-specific context; use the canonical docs for implementation.

---

## 1. Who Reads Instruments?

**Other services do NOT import instruments-service as a package.** They use shared libraries:

| Consumer                           | How They Read Instruments                                                          |
| ---------------------------------- | ---------------------------------------------------------------------------------- |
| **market-tick-data-service**       | `InstrumentsDomainClient` from unified-trading-services                            |
| **market-data-processing-service** | `InstrumentsDomainClient` from unified-trading-services                            |
| **features-\*** services           | `InstrumentsDomainClient` from unified-trading-services                            |
| **UTDv3 data-status**              | `InstrumentsDomainClient.get_aggregated_instruments()` (aggregated cache)          |
| **deployment scripts**             | `InstrumentsDomainClient` or instruments-service CLI                               |
| **instruments-service itself**     | `CloudInstrumentStorage.query_instruments`, `InstrumentsService.query_instruments` |

**Standard:** All consumers use **InstrumentsDomainClient** (unified-trading-services). No duplicate `_load_instruments_by_venue` or direct GCS reads. Aggregated instruments via `get_aggregated_instruments(category)`.

**Conclusion:** instruments-service is a **producer** (writes to GCS). Consumers use **unified-trading-services** `InstrumentsDomainClient` exclusively. No need for instruments-service to expose a reader library.

---

## 2. API Keys by Mode / Venue

Not all modes need all API keys. Use **selective validation** — only fetch keys for requested venues.

| Mode / Category               | Required API Keys | Optional          |
| ----------------------------- | ----------------- | ----------------- |
| **CEFI** (Tardis venues)      | Tardis            | —                 |
| **TRADFI** (Databento venues) | Databento         | —                 |
| **DEFI** (DeFi venues)        | The Graph         | Alchemy, AaveScan |
| **Corporate actions only**    | None              | —                 |
| **TRADFI-only (no CEFI)**     | Databento         | Tardis not needed |
| **DEFI-only (no CEFI)**       | The Graph         | Tardis not needed |

**DataSourceMapping** (unified-market-interface) maps venues → data sources → required secrets. Use `DataSourceMapping.get_required_secrets(venues)` before validating.

---

## 3. Single Source of Truth: Secret Manager

**All API keys live in Secret Manager (GCP).** No credentials in code or env vars in production.

| Layer                        | Responsibility                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| **Secret Manager**           | Store all API keys                                                                     |
| **unified-config-interface** | Define secret names in `UnifiedCloudConfig`                                            |
| **unified-trading-services** | `get_secret_client` — **only** way to resolve API keys                                 |
| **Services**                 | Call `get_secret_client` via config secret names. Never `os.environ.get` for API keys. |

**Resolution order** (never `os.getenv` alone for API keys):

1. **Config / kwargs** — e.g. `config.get("tardis_api_key")` (for programmatic callers)
2. **Secret Manager** — `get_secret_client(secret_name=config.tardis_secret_name, project_id=..., fallback_env_var="TARDIS_API_KEY")`
3. **Env var fallback** — Only for local dev; production uses Secret Manager.

**Single function:** `get_secret_client` from unified-trading-services.

```python
from unified_trading_services import get_secret_client

api_key = get_secret_client(
    project_id=instruments_config.gcp_project_id,
    secret_name=instruments_config.tardis_secret_name,
    fallback_env_var="TARDIS_API_KEY",
)
```

---

## 4. Config Fields (InstrumentsServiceConfig / UnifiedCloudConfig)

All secret names come from config (UnifiedCloudConfig). Never hardcode secret names.

| Config Field            | Env Var                 | Default               | Used For           |
| ----------------------- | ----------------------- | --------------------- | ------------------ |
| `tardis_secret_name`    | `TARDIS_SECRET_NAME`    | `tardis-api-key-full` | CEFI (Tardis)      |
| `databento_secret_name` | `DATABENTO_SECRET_NAME` | `databento-api-key`   | TRADFI (Databento) |
| `graph_secret_name`     | `GRAPH_SECRET_NAME`     | `graph-api-key`       | DEFI (The Graph)   |
| `alchemy_secret_name`   | `ALCHEMY_SECRET_NAME`   | `alchemy-api-key`     | DEFI (Alchemy RPC) |
| `aavescan_secret_name`  | `AAVESCAN_SECRET_NAME`  | `aavescan-api-key`    | DEFI (AaveScan)    |

Instruments-service inherits these from `UnifiedCloudConfig` (unified-config-interface). Override in `InstrumentsServiceConfig` only if needed.

---

## 5. Current Implementation Map

| Location                                | Pattern                                                  | Compliant?               |
| --------------------------------------- | -------------------------------------------------------- | ------------------------ |
| `instrument_processing_service`         | config → Secret Manager → env                            | ✅                       |
| `selective_validation`                  | `validate_required_api_keys(venues)` → get_secret_client | ✅                       |
| `dependency_checker`                    | get_secret_client                                        | ✅                       |
| `conftest`                              | get_secret_client for Tardis                             | ✅                       |
| `scripts/test_batch_cost_comparison.py` | `os.environ.get("DATABENTO_API_KEY")`                    | ❌ Use get_secret_client |
| `scripts/find_subgraph_ids.py`          | `os.environ.get("THEGRAPH_API_KEY", "test-key")`         | ❌ Use get_secret_client |

---

## 6. Adding a New API Key

1. Add to `UnifiedCloudConfig` (unified-config-interface): `{service}_secret_name` with env alias.
2. Add to `.env.example`: `{SERVICE}_SECRET_NAME=...`
3. Add to `selective_validation` data-source mapping if venue-specific.
4. Use `get_secret_client(secret_name=config.{service}_secret_name, fallback_env_var="{SERVICE}_API_KEY")` in code.
5. Document in this file.

---

## 7. Mode-Specific Behavior

- **Pre-flight:** `validate_required_api_keys(venues)` — only for venues being processed.
- **Lazy load:** InstrumentProcessingService loads Tardis key only when CeFi is requested; Graph key only when DeFi is requested.
- **Corporate actions:** No API keys (yfinance, exchange-calendars are used; no key required for basic usage).
- **Scripts:** Use get_secret_client, not os.environ.get, for consistency.

---

## 8. Flagging Missing API Keys

When adding a new data source, verify we have a secret for it:

1. Add `{service}_secret_name` to `UnifiedCloudConfig` if not present.
2. Document in this file and in `.cursor/plans/INSTRUMENT_AGGREGATION_AND_API_KEYS_PLAN.md`.
3. Use context7 ("use context7") when integrating new market data APIs (Tardis, Databento, The Graph, etc.) to confirm required credentials.

**Known data sources and keys:**

| Data Source | Secret                | Status                                |
| ----------- | --------------------- | ------------------------------------- |
| Tardis      | tardis_secret_name    | ✅                                    |
| Databento   | databento_secret_name | ✅                                    |
| The Graph   | graph_secret_name     | ✅                                    |
| Alchemy     | alchemy_secret_name   | ✅                                    |
| AaveScan    | aavescan_secret_name  | ✅                                    |
| Envio       | envio_secret_name     | ✅ Used (UniswapV4, features-onchain) |
