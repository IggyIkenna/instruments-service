# instruments-service

Fetches canonical instrument records from URDI, validates, and writes to GCS.
No external API calls. No credential management. No canonicalization logic.

## Setup

```bash
source scripts/setup.sh
```

## What it does

For each configured venue and date:

1. `urdi_reference_provider.fetch(venue, date)` → `list[InstrumentRecord]` (canonical, already typed)
2. Optional CCXT metadata enrichment (`engine/processors/cefi_metadata.py`) for leverage/margin fields
3. `DomainValidationService("instruments").validate(df)` — flags anomalies via event log
4. `ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA).validate_dataframe(df)` — blocks bad writes
5. Write per-venue parquet to GCS via `get_data_sink()` with catalogue entry

Mode (batch/live), scheduling, credential injection, and data availability checks are handled by
UTL `ServiceBootstrap`. The service does not inspect `--mode`.

## CLI — service-specific args

```
--CEFI / --TRADFI / --DEFI / --SPORTS   categories to process (default: all)
--redo-all                               reprocess dates already present in storage
--venues [LIST]                          restrict to a subset of URDI_SUPPORTED_VENUES
```

Cross-cutting mode variables (`RUNTIME_MODE`, `DATA_MODE`, `TESTNET_MODE`, `ENVIRONMENT`, etc.):
→ `unified-internal-contracts/unified_internal_contracts/modes.py`
→ `unified-trading-codex/09-strategy/cross-cutting/operational-modes-matrix.md`

## Config — 4 fields only

| Env var                      | Default | Purpose                                              |
| ---------------------------- | ------- | ---------------------------------------------------- |
| `ENABLE_CCXT_INTEGRATION`    | `true`  | Post-URDI CCXT metadata enrichment (leverage/margin) |
| `CONFIG_STORE_BUCKET`        | `""`    | Hot-reload config bucket                             |
| `INSTRUMENTS_CATALOGUE_PATH` | `""`    | CI test override for catalogue path                  |

Bucket names, API URLs, and deployment state are resolved by UTL `cloud_constants`, UCI provider
manifest, and UTL `ServiceBootstrap` respectively — not service config.

## Output schema

`InstrumentRecord` — `unified-internal-contracts/` (UIC). Output path:
`instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet`

## Live mode cadence

`RUNTIME_MODE=live`: UTL `ScheduledIO` triggers every 15 minutes at wall-clock-aligned intervals
(`:00`, `:15`, `:30`, `:45` UTC). The handler code is identical to batch — UTL selects the trigger.

## Mock mode (`DATA_MODE=mock`)

```bash
python scripts/seed_mock_data.py --scenario normal --seed 42 --env local
```

Substitutes URDI with `InstrumentGenerator` from `unified-internal-contracts`. This service is
**Layer 1** of the mock data chain — all downstream services depend on this output.

| Aspect                | Mock                                                               | Real                 |
| --------------------- | ------------------------------------------------------------------ | -------------------- |
| Data source           | `InstrumentGenerator(seed)` from UIC                               | URDI API calls       |
| Instrument universe   | `UAC representative_sample.py` (the SSOT)                          | Live exchange APIs   |
| Differentials         | Expiry events, new listings, delistings (driven by `MockScenario`) | As-is from exchanges |
| Schema, paths, format | Identical to real                                                  | —                    |

Scenario definitions (`NORMAL`, `STRESS`, `FLASH_CRASH`, `MISSING_DATA`, etc.):
→ `unified-internal-contracts/unified_internal_contracts/modes.py` (`MockScenario`)
→ `unified-internal-contracts/unified_internal_contracts/testing/scenarios/*.yaml`

## Sharding

| Mode  | Dimensions              | Notes                                           |
| ----- | ----------------------- | ----------------------------------------------- |
| batch | category × venue × date | Each shard = one independent VM / Cloud Run job |
| live  | venue                   | One instance per venue, 15-min polling          |

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

Add a venue: edit `adapters/urdi_reference_provider.py` → `URDI_SUPPORTED_VENUES`.
URDI must have an adapter for the venue first — add it there, not here.

## Where the real logic lives

| Concern                                | Location                                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Instrument key spec / canonical schema | `unified-trading-codex/02-data/` · UAC `unified_api_contracts/registry/`                                |
| Parquet schema (`INSTRUMENTS_SCHEMA`)  | `unified-internal-contracts/` (UIC)                                                                     |
| Venue adapters (fetch + normalise)     | `unified-reference-data-interface/` (URDI)                                                              |
| Sports reference + team mappings       | `unified-sports-reference-interface/`                                                                   |
| Mock instrument universe               | UAC `unified_api_contracts/registry/representative_sample.py`                                           |
| Mock scenarios + mode enums            | `unified-internal-contracts/unified_internal_contracts/modes.py`                                        |
| Batch vs live / scheduling             | UTL `ScheduledIO`/`BatchIO` · runtime topology DAG (`unified-trading-pm/configs/runtime-topology.yaml`) |
| Config patterns · service framework    | `unified-trading-codex/06-coding-standards/`                                                            |
| DeFi protocol details                  | `unified-trading-codex/09-strategy/defi/`                                                               |
