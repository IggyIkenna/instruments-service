# instruments-service

Fetches canonical instrument records from venue reference-data adapters (the internal URDI
reference provider), validates them, applies the canonical instrument-id form, and writes to GCS.
It IS the reference-data fetcher — it calls venue APIs via its adapters and resolves their
credentials through UTL. See `docs/ADAPTER_ARCHITECTURE.md` for the full adapter model and
`docs/SETUP_GUIDE.md` for credential/secret handling.

## Setup

```bash
source scripts/setup.sh
```

See `docs/SETUP_GUIDE.md` for the authoritative environment setup (editable path deps, Python
pin, secrets, quality gates).

## What it does

For each configured venue and date:

1. `urdi_reference_provider.fetch_instruments_for_all_venues(venues, ...)` → `VenueFetchResult` wrapping canonical `list[InstrumentRecord]` (already typed). URDI here is the internal load-bearing module `engine/urdi_reference_provider.py`, not an external repo.
2. Optional CCXT metadata enrichment (`engine/processors/cefi_metadata.py`) for leverage/margin fields
3. `DomainValidationService("instruments").validate(df)` — flags anomalies via event log
4. `ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA).validate_dataframe(df)` — blocks bad writes
5. Write per-venue parquet to GCS via `get_data_sink()` with catalogue entry

Mode (batch/live), scheduling, credential injection, and data availability checks are handled by
UTL `ServiceBootstrap`. `--mode` selects infrastructure (batch vs live), never the operation —
the operation is `--operation instruments`.

## CLI — service-specific args

```
--operation instruments        the operation to run (see docs/ADAPTER_ARCHITECTURE.md for others)
--mode batch | live            infrastructure trigger (UTL BatchIO vs ScheduledIO) — NOT an operation
--asset-group CEFI|DEFI|TRADFI|SPORTS|PREDICTION|ALL   asset group to process (default: ALL)
--force                        reprocess dates already present in storage (skip the skip-if-fresh check)
--venues [LIST]                restrict to a subset of URDI_SUPPORTED_VENUES
--start-date / --end-date      batch date range
```

`--operation` / `--mode` / `--asset-group` follow the workspace CLI convention SSOT
(`codex/06-coding-standards/cli-convention.md`). `--mode` is never an operation name.

Cross-cutting mode variables (`RUNTIME_MODE`, `DATA_MODE`, `TESTNET_MODE`, `ENVIRONMENT`, etc.):
→ UAC `unified_api_contracts/internal/modes.py`
→ `codex/09-strategy/architecture-v2/cross-cutting/operational-modes-matrix.md`

## Config — 4 fields only

| Env var                      | Default | Purpose                                              |
| ---------------------------- | ------- | ---------------------------------------------------- |
| `ENABLE_CCXT_INTEGRATION`    | `true`  | Post-URDI CCXT metadata enrichment (leverage/margin) |
| `CONFIG_STORE_BUCKET`        | `""`    | Hot-reload config bucket                             |
| `INSTRUMENTS_CATALOGUE_PATH` | `""`    | CI test override for catalogue path                  |

Bucket names, API URLs, and deployment state are resolved by UTL `cloud_constants`, UCI provider
manifest, and UTL `ServiceBootstrap` respectively — not service config.

## Output schema

`InstrumentRecord` — UAC `unified_api_contracts/internal/reference/instrument.py`. Output path:
`instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet`

## Live mode cadence

`RUNTIME_MODE=live`: UTL `ScheduledIO` triggers every 15 minutes at wall-clock-aligned intervals
(`:00`, `:15`, `:30`, `:45` UTC). The handler code is identical to batch — UTL selects the trigger.

## Mock mode (`DATA_MODE=mock`)

Mock instrument generation is UAC-owned — there is no service-local seed script. The generator
`InstrumentGenerator` (UAC `unified_api_contracts/internal/testing/instrument_generator.py`) reads
the instrument universe from `representative_sample.py` (the SSOT) and produces schema-identical
`InstrumentRecord`s; downstream mock-data-chain tests consume this output. This service is
**Layer 1** of the mock data chain.

| Aspect                | Mock                                                               | Real                  |
| --------------------- | ------------------------------------------------------------------ | --------------------- |
| Data source           | `InstrumentGenerator(seed)` from UAC                               | venue adapter fetches |
| Instrument universe   | UAC `representative_sample.py` (the SSOT)                          | Live exchange APIs    |
| Differentials         | Expiry events, new listings, delistings (driven by `MockScenario`) | As-is from exchanges  |
| Schema, paths, format | Identical to real                                                  | —                     |

Scenario definitions (`NORMAL`, `STRESS`, `FLASH_CRASH`, `MISSING_DATA`, etc.):
→ UAC `unified_api_contracts/internal/modes.py` (`MockScenario`)

## Sharding

| Mode  | Dimensions                 | Notes                                           |
| ----- | -------------------------- | ----------------------------------------------- |
| batch | asset_group × venue × date | Each shard = one independent VM / Cloud Run job |
| live  | venue                      | One instance per venue, 15-min polling          |

Full compute spec (VM type, memory, disk, timeout):
→ `unified-trading-pm/configs/sharding.instruments-service.yaml`
→ `unified-trading-pm/configs/sharding_config.yaml` (cross-service sharding SSOT)

## Resource profile

**Memory:** Instruments are API response objects, not files. One shard = one venue × one day
= hundreds to a few thousand `InstrumentRecord` objects (≈1–2 MB). No chunking or streaming
required. Everything fits in a single DataFrame per shard.

**Disk:** All output goes to GCS via API (`gcsfuse` disabled). No local parquet staging.
Local disk = OS swap only. VM is self-deleted after each shard completes (`self_delete: true`).

**CPU:** URDI venue fetches run concurrently (`asyncio.gather`). Processing and writes are
sequential. Cross-shard parallelism (many shards running as independent containers) is the
primary scaling mechanism — no intra-shard thread pool.

**Reads:** This service never reads from storage. Streaming-read patterns do not apply here.
(See `market-data-processing-service` for a service with large-file streaming reads.)

## Extending coverage

Add a venue: register its adapter KEY in UAC
(`unified_api_contracts/registry/venue_adapter_keys.py` → `VENUE_TO_ADAPTER_KEY`) and its
adapter CLASS here (`reference_data/factory.py` → `_ADAPTERS`). `URDI_SUPPORTED_VENUES` is
UAC-derived — never edit a venue list in this repo.

## Dependencies

`instruments-service` depends on exactly two editable-path sibling repos (declared in
`pyproject.toml`): **UTL** (`unified-trading-library`) and **UAC** (`unified-api-contracts`).
Every import in `instruments_service/` is `from unified_trading_library import ...` or
`from unified_api_contracts import ...`. There are no other workspace repo dependencies.

## Where the real logic lives

| Concern                                | Location                                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Instrument key spec / canonical schema | `codex/02-data/` · UAC `unified_api_contracts/registry/`                                                |
| Parquet schema (`INSTRUMENTS_SCHEMA`)  | UAC `unified_api_contracts/internal/reference/`                                                         |
| Venue adapters (fetch + normalise)     | `reference_data/adapters/` (this repo) · adapter KEYS in UAC `registry/venue_adapter_keys.py`           |
| Sports reference + team mappings       | `reference_data/adapters/sports/` (this repo)                                                           |
| Mock instrument universe               | UAC `unified_api_contracts/registry/representative_sample.py`                                           |
| Mock scenarios + mode enums            | UAC `unified_api_contracts/internal/modes.py`                                                           |
| Batch vs live / scheduling             | UTL `ScheduledIO`/`BatchIO` · runtime topology DAG (`unified-trading-pm/configs/runtime-topology.yaml`) |
| Config patterns · service framework    | `codex/06-coding-standards/`                                                                            |
| DeFi protocol details                  | `codex/04-architecture/defi-execution-overview.md` · `codex/02-data/defi-canonical-naming-ssot.md`      |
