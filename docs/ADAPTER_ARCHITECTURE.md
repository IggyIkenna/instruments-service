# Adapter Architecture

> **Consolidated doc** — replaces `ARCHITECTURE.md`, `specs/COMMAND_FLOW_ANALYSIS.md`, `specs/COMMAND_FLOW_DIAGRAM.md`,
> `specs/VENUE_ADAPTERS.md`, and the general/cross-cutting parts of `specs/INSTRUMENT_SPECIFICATION.md` (the
> canonical-id grammar + `canonical_id_builder.py` explanation). Per-asset-group instrument type catalogs and worked
> examples live in the 5 asset-group docs, not here.
>
> **Live mockup**: the instrument-definitions drilldown mockup shows real captured samples per asset group/venue,
> including current-vs-target-canonical comparisons for every open divergence described below —
> [artifact e2824e52-3a51-43e0-b4b1-933bee469f9d](https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d),
> DeFi / CeFi tabs. Not browsable from this doc — open it directly and navigate to the relevant tab.
>
> **This doc replaces stale architecture claims found in the docs it merges.** The previous `ARCHITECTURE.md` /
> `COMMAND_FLOW_*` docs described an `app/core/` + `app/venues/` module layout (`InstrumentProcessingService`,
> `CloudInstrumentStorage`, `InstrumentBatchProcessor`, `TardisAdapter`) and a `--mode instruments` CLI invocation.
> **None of that exists in the current codebase.** The real module layout, CLI convention, and orchestration pipeline
> are described below, verified directly against `instruments-service/instruments_service/` as of 2026-07-08.

---

## Purpose

The Instruments Service generates canonical instrument definitions (metadata for centralized/normalized instrument
identity and lookup) from exchange APIs, DeFi protocol SDKs/subgraphs, TradFi data vendors, sports data providers, and
prediction-market APIs, and stores them to GCS. It is the **first service in the data pipeline** and the authoritative
source for instrument/reference-data metadata across the trading system.

**Key responsibilities**:

- Discover available instruments/entities per asset group (what instruments/fixtures/markets exist)
- Generate canonical instrument IDs (see [Canonical Instrument ID Specification](#canonical-instrument-id-specification) below)
- Enrich with metadata (contract addresses, fee tiers, tick sizes, risk parameters, etc.)
- Track availability windows and instrument lifecycle
- Store instrument definitions + sports/prediction reference data to GCS for downstream consumption

**Role in the pipeline**:

```
instruments-service (this service)
    ↓
market-tick-data-service (downloads market data using instrument IDs)
    ↓
market-data-processing-service (processes ticks into candles)
    ↓
features-* services (generate features from processed data)
    ↓
strategy-service (uses instruments for trading decisions)
    ↓
execution-service (uses instruments for order execution)
```

**Downstream clients should use the domain client library to query instruments, not import instruments-service
directly.** See the Usage doc for client patterns.

**instruments-service owns reference data; venue lists and adapter KEYS are UAC data** — the service is a thin
resolver over `unified_api_contracts.registry` (`VENUE_TO_ADAPTER_KEY`, `VENUES_BY_ASSET_GROUP`), not the source of
truth for which venues exist. See `codex/04-architecture/instruments-service-as-ssot-for-mtds.md` and
`…/instrument-universe-registry-consolidation.md`.

**"URDI"**: internal shorthand used in code comments for the service's own reference-data abstraction layer
(`instruments_service.reference_data` — the adapter base class + factory + per-venue adapters described below). It is
not a separate repo or package; the name is a phantom/legacy label for "the one external-API path" inside this
service. `engine/urdi_reference_provider.py`'s own docstring states it plainly: _"the ONLY external API path for
instruments-service."_

---

## Real module map

```
instruments_service/
├── __main__.py                        # python -m instruments_service entry point
├── cli/
│   ├── main.py                        # main_service_cli() — ServiceBootstrap wiring, --operation=status
│   │                                   #   / --operation=refresh-league-entity-coverage short-circuits
│   └── instruments_handler.py         # InstrumentsHandler(UnifiedServiceHandler) — preflight/process/cleanup
├── engine/
│   ├── urdi_reference_provider.py     # fetch_instruments_for_all_venues() — the sole external-API dispatch point
│   ├── validation_utils.py
│   ├── data_utils.py
│   └── orchestrator/                  # process_instruments() split into cohesion modules (see below)
│       ├── process.py                 # process_instruments() — the 8-stage per-date/per-venue entrypoint
│       ├── venue_core.py              # adapter epoch table, venue availability, get_venues_for_asset_groups()
│       ├── catalogue.py               # bucket resolution, availability-index / catalogue record writes
│       ├── process_preflight.py       # stage 1b — freshness/skip-if-exists preflight
│       ├── process_fetch.py           # stage 2 — URDI fetch + date filtering
│       ├── process_zero_records.py    # stage 4 — honest-absence handling per asset group
│       ├── process_write.py           # stage 5-6 — schema validation + per-venue parquet/manifest writes
│       ├── process_enrichment.py      # stage 7 — sports reference + enrichment providers
│       ├── process_completeness.py    # stage 8 — completeness check + automatic retry
│       ├── defi.py                    # DeFi-specific orchestration (universe assembly, chain fan-out)
│       ├── writers.py, sink.py        # low-level parquet/CSV-sample writers
│       └── failure.py                 # shared failure classification helpers
├── reference_data/                    # the "URDI" adapter layer
│   ├── base_adapter.py                # BaseReferenceDataAdapter (ABC) — every venue adapter subclasses this
│   ├── factory.py                     # get_adapter_for_canonical_venue() — key → adapter CLASS table
│   ├── schemas.py                     # CanonicalOptionsChain / CanonicalExpiryCalendar / OHLCVRef / FundingRateRef
│   ├── sports_dependency.py           # enrichment-adapter dependency-order enforcement (api-football first)
│   ├── catalogue/                     # catalogue-build helpers
│   ├── utils/
│   └── adapters/
│       ├── cefi/                      # ccxt_adapter.py, tardis/, aster.py, hyperliquid.py, lighter.py, ...
│       ├── defi/                      # uniswap_v2/v3/v4.py, aave_v3.py, morpho.py, curve.py, + ~35 more
│       ├── tradfi/                    # databento/, ibkr.py, massive.py, futures_factory.py, tradfi_live.py
│       ├── prediction/                # kalshi.py, polymarket/
│       └── sports/                    # adapters/ (api_football.py, betfair.py, ...), factory.py, _normalizer.py
├── config/                            # instruments_config, settings
├── data/                              # externalized static data (sp500_tickers.json, tradfi_instruments.json)
├── api/                                # health/admin HTTP surface (ServiceBootstrap-provided)
├── sports/                            # sports-domain support code (separate from reference_data/adapters/sports)
└── triggers/                          # live-mode trigger dispatch (--trigger flag, in-progress per instruments_master)
```

There is **no `app/` directory, no `InstrumentProcessingService`, no `CloudInstrumentStorage`, no
`InstrumentBatchProcessor`, no `TardisAdapter` class, no `venues/` directory** — these were real at some point but
have since been replaced wholesale by the `reference_data/adapters/` + `engine/orchestrator/` split described here.
Any other document, runbook, or onboarding note still citing the old names/paths is stale.

---

## Command flow

### Real CLI invocation

```bash
python -m instruments_service --operation instruments --mode batch \
  --asset-group CEFI --start-date 2026-07-01 --end-date 2026-07-01
```

- `--operation` selects the ServiceBootstrap handler (`instruments` → `InstrumentsHandler`; today the only
  registered operation). `--operation=status` and `--operation=refresh-league-entity-coverage` are read-only
  diagnostics that bypass the date-loop framework entirely (handled directly in `cli/main.py` before
  `ServiceBootstrap` is constructed).
- `--mode` selects **infrastructure**, not domain logic: `batch` (UTL `BatchIO`, date-range iteration) or `live` (UTL
  `ScheduledIO`, wall-clock aligned). This is the CLI convention SSOT
  (`codex/06-coding-standards/cli-convention.md`) — `--mode` is never an operation name.
- `--asset-group` (`CEFI` / `DEFI` / `TRADFI` / `SPORTS` / `PREDICTION` / `ALL`) selects which venue set
  `get_venues_for_asset_groups()` resolves.
- Additional flags: `--venues` (shard override), `--force`, `--sports-provider` / `--sports-entity` / `--league` /
  `--season` (sports-domain scoping), `--source` (`massive` routes TradFi to the Massive/Polygon.io-compatible
  adapter instead of Databento), `--trigger` (live-mode entity-subset selector, in-progress rollout), `--run-tag`
  (GCS output prefix; `t1-recon` self-defaults the date window to today).
- The `--mode instruments` form documented previously is **stale** — `--mode` now means batch/live only; the older
  docs conflated it with today's `--operation`.

### Orchestration: `process_instruments()` (8 stages)

`InstrumentsHandler.process()` (in `cli/instruments_handler.py`) calls `engine.orchestrator.process_instruments()`
once per date in the requested range. The function was split (behaviour-preserving extraction) from a former
~1,931-line monolith into cohesion modules that share mutable state through a package-level `_orch` namespace —
`unittest.mock.patch("instruments_service.engine.orchestrator.<name>")` still targets correctly across the split.

```
InstrumentsHandler.preflight()
  ├─▶ start ApiKeyReloader (fail-fast on missing keys, periodic refresh from Secret Manager)
  ├─▶ start a 60s background PipelineHeartbeatTimer (mid-date stall detection)
  └─▶ wire CLI filters (--venues, --sports-*, --league, --season, --source, --trigger, ...)

InstrumentsHandler.process(payload)  — per date in the batch/live loop
  └─▶ engine.orchestrator.process_instruments(date, asset_groups, ...)
        │
        ├─ 1. Resolve active venues: get_venues_for_asset_groups(asset_groups),
        │      filtered by is_venue_available(venue, date) (adapter launch-date gating)
        │
        ├─ 1b. _freshness_preflight() — skip-if-exists check against the manifest
        │      (per-asset-group buckets; bypassed by --force)
        │
        ├─ 2. _fetch_urdi_records() — the SOLE external-API path, via
        │      engine.urdi_reference_provider.fetch_instruments_for_all_venues(),
        │      which resolves each venue to an adapter class via
        │      reference_data.factory.get_adapter_for_canonical_venue()
        │
        ├─ 3. _filter_and_enrich_records() — reduce the full historical
        │      universe each adapter returns down to instruments active on
        │      the requested date (available_from/to window filtering),
        │      plus reject_junk_instruments() (non-ASCII / known test symbols)
        │
        ├─ 4. _handle_zero_records() — honest-absence handling per asset
        │      group when nothing survives step 3 (distinguishes a clean-empty
        │      day from an upstream fetch failure)
        │
        ├─ 5. _validate_records() — schema validation with per-record failure
        │      isolation (a bad record does not fail the whole shard)
        │
        ├─ 6. _write_all_venues() — per-venue parquet + catalogue record +
        │      CSV sample + manifest write (ManifestWriter, buffered, flushed
        │      in InstrumentsHandler.cleanup())
        │
        ├─ 7. _run_sports_enrichment() — sports-only: fetches + writes
        │      reference-data providers (FootyStats/Understat/Transfermarkt/
        │      SoccerFootball.info/Open-Meteo) alongside fixtures, subject to
        │      the api-football-first dependency order (sports_dependency.py)
        │
        └─ 8. _completeness_and_retry() — shard completeness check against
               expected venues; automatic retry for venues that failed
               retryably in step 2
```

Every stage classifies and emits errors per-venue (`classify_and_emit_error` / UAC `VenueErrorClassification`) rather
than raising out of a per-shard loop — see `codex/04-architecture/shard-level-failure-isolation.md`. A
`PIPELINE_HEARTBEAT` is emitted after every completed date so a hung backfill (e.g. an unbounded scrape) trips
`DP_VM_STALL` instead of a VM silently sitting `RUNNING` with a flat progress metric.

### Storage / bucket resolution

Every write goes through `engine.orchestrator.catalogue._get_instruments_bucket()` →
`resolve_bucket_name(cloud="gcp", kind=..., asset_group=...)` — never an inline `gs://` literal. Bucket naming is
`instruments-store-{asset_group}-{env}-{project_id}` for `cefi`/`defi`/`tradfi`/`sports`, **except prediction**, which
resolves via a dedicated flat kind (`instruments-store-prediction` → `instruments-store-pred-{env}-{project_id}`) —
not a `PREDICTION` entry in the per-asset-group dict. Every consumer of the prediction store must resolve the flat
kind or it 404s; this bucket-naming split was confirmed still real and live as of the 2026-07-08 audit
(`instruments-store-pred-prd-central-element-323112` exists; `instruments-store-prediction-prd-central-element-323112`
404s).

Each write also updates the consolidated `_index/availability_index.parquet` roll-up (`catalogue._write_catalogue_record()`)
so downstream services can check completeness without listing thousands of GCS blobs, and flushes to the
per-asset-group MTDS-visible manifest (`ManifestWriter`) at date-loop `cleanup()`.

---

## Venue Adapter Architecture

### Pattern

Every venue/protocol integration subclasses `reference_data.base_adapter.BaseReferenceDataAdapter` (an ABC). The base
class provides: bounded-timeout `aiohttp` session management (a mandatory `ClientTimeout` — an unbounded session was
the root cause of a 2026-06-19 silent-stall incident), retry with backoff on `{429,500,502,503,504}`, and the shared
canonical schemas (`CanonicalOptionsChain`, `CanonicalExpiryCalendar`, `OHLCVRef`, `FundingRateRef`). Adapters are
API-keyless by convention — the service fetches credentials from Secret Manager (via `ApiKeyReloader`) and injects
them at call time as the `api_key` constructor argument; no adapter reads `os.getenv()` or a secret name directly.

`reference_data/factory.py` is the **key → adapter class** table (`get_adapter_for_canonical_venue()`). It explicitly
does **not** own venue truth: the **venue → adapter-key** mapping (`VENUE_TO_ADAPTER_KEY`) and the per-asset-group
venue lists (`VENUES_BY_ASSET_GROUP`) are UAC registry data
(`codex/04-architecture/instrument-universe-registry-consolidation.md`) — instruments-service is a thin resolver over
both. `engine.orchestrator.venue_core.get_venues_for_asset_groups()` is the function that turns a requested
asset-group list into the concrete venue list to fetch, including asset-group-specific fan-out rules (e.g. bare `OKX`
expands to `OKX-SPOT` / `OKX-SWAP` / `OKX-FUTURES` because Tardis exposes those as three separate endpoints; bare
`COINBASE` maps to the `COINBASE-SPOT` Tardis endpoint).

Sports has its **own**, separate factory (`reference_data/adapters/sports/factory.py`,
`create_sports_reference_adapter()`) for enrichment-provider adapters, with an explicit dependency-order check
(`sports_dependency.check_api_football_dependency`) — enrichment providers (FootyStats/Understat/Transfermarkt/
SoccerFootball.info/Open-Meteo/Betfair) require api-football's fixtures to already exist for the target date, and
raise an actionable `DependencyError` naming the exact backfill command to run first if it doesn't.

### Real venue adapters by asset group

**CeFi** (`reference_data/adapters/cefi/`):

- `tardis/` (`adapter.py`, `combos.py`, `parsing.py`) — historical/batch instrument enumeration via Tardis, covering
  Binance, Bybit, OKX, Deribit, Upbit, Coinbase and others; owns Deribit-style combo/calendar-spread parsing.
- `ccxt_adapter.py` (`CCXTReferenceDataAdapter`) — generic CCXT-backed fallback for live-mode enumeration on venues
  without a dedicated adapter (real-time public endpoints, no historical data).
- `deribit_options_adapter.py`, `deribit_combo_adapter.py` — Deribit-specific options-chain and combo/strategy
  builders.
- `aster.py`, `hyperliquid.py`, `lighter.py`, `pacifica.py`, `extended.py` — on-chain/DEX perpetual venues
  (Aster, Hyperliquid, Lighter (zkSync), Pacifica (Solana), Extended (Starknet)).
- `kalshi_perp.py`, `polymarket_perp.py` — prediction-venue perpetual wrappers (cross-listed under CeFi for the
  perpetual instrument type, distinct from the `prediction/` adapters below which handle the market/event side).

**DeFi** (`reference_data/adapters/defi/`, ~40 adapters) — DEX pools: `uniswap_v2.py`, `uniswap_v3.py`,
`uniswap_v4.py`, `curve.py`, `balancer.py`, `raydium.py`, `orca.py`, `phoenix.py`, `meteora.py`, `jupiter.py`,
`lifinity.py`; lending: `aave_v3.py`, `morpho.py`, `compound_v3.py`, `spark.py`, `euler_v2.py`, `fluid.py`,
`radiant.py`, `venus.py`, `benqi.py`, `kamino.py`, `drift.py`; LST/yield/staking: `lido.py`, `etherfi.py`, `ethfi.py`,
`rocket_pool.py`, `ethena.py`, `renzo.py`, `puffer.py`, `pendle.py`, `yearn.py`, `convex.py`, `idle.py`, `beefy.py`,
`sanctum.py`, `solblaze.py`, `marinade.py`, `jito.py`, `jito_restaking.py`, `karak.py`, `symbiotic.py`,
`kelpdao.py`, `eigenlayer.py`, `mango.py`, `zeta.py`, `flash_trade.py`, `solana_native_staking.py`. Every DeFi adapter
builds its venue token as `f"{protocol}-{chain}"` (dash-separated — e.g. `AAVE_V3-ARBITRUM`, `UNISWAP_V3-ETHEREUM`;
confirmed directly in `aave_v3.py`/`uniswap_v3.py` source), not the underscore-joined form (`AAVE_V3_ETH`) some older
docs and a handful of misspelled real rows (`AAVEV3-OPTIMISM`, `AAVE_V3-OPTIMISM` duplication) still show.

**TradFi** (`reference_data/adapters/tradfi/`): `databento/` (`adapter.py`, `symbology.py`, `sessions.py` — CME/NASDAQ/
NYSE/CBOE/ICE via Databento, including static VIX/KRW-USD/Bitcoin-ETF definitions), `ibkr.py` (Interactive Brokers),
`massive.py` (Polygon.io-compatible alternate source, opt-in via `--source massive`), `futures_factory.py`,
`tradfi_live.py`.

**Prediction** (`reference_data/adapters/prediction/`): `kalshi.py`, `polymarket/` (event-contract markets; routes
through its own domain builder, `canonical/domain/prediction/prediction_mapping.py`, rather than the ad hoc CeFi/DeFi
construction pattern below).

**Sports** (`reference_data/adapters/sports/`): `adapters/api_football.py` (fixtures — the primary, dependency-order
root for every enrichment provider), `adapters/betfair.py`, plus the enrichment-only providers under the sports
`factory.py` table (`footystats`, `open_meteo`, `soccer_football_info`, `transfermarkt`, `understat`).

### Current implementation status

All of the above are real, wired-up adapters as of this writing (registered in `reference_data/factory.py`'s import
list and `_ADAPTERS` table). There is no meaningful "pending implementation" venue set left over from the old docs —
Euler/Fluid/Hyperliquid/Aster all shipped since. Check `factory.py` directly for the authoritative, current adapter
registration table rather than trusting a static list in any doc (this one included) to stay perfectly current.

---

## Canonical Instrument ID Specification

> **Corrections applied in this section relative to the old `INSTRUMENT_SPECIFICATION.md`** — see
> `unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md` (the operator-decided
> target-state doc) and `unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md` (the
> full 7-layer compliance audit) for full evidence. **None of the "target" forms below are live in production today**
> — this section shows current-real vs target-canonical explicitly, same pattern as the live mockup.

### The architecture is now decided: ONE shared builder for every asset group — adoption is still in progress

**RESOLVED 2026-07-08** (operator, `instrument_id_format_canonicalization_2026_07_08.md`): _"one builder for
everything would make more sense... every asset group, every instrument type, can get its canonical instrument IDs,
same with fixtures [sports], just by filling in the right inputs."_ Per-domain builders that each independently
canonicalize are explicitly **rejected**. This section previously described a real gap ("there is no single enforced
builder") found by the 2026-07-08 audit, which sampled instruments-service (39+ adapter files), MTDS, deployment-api,
and strategy-service and found that virtually every adapter built its own `instrument_key` ad hoc (direct f-string
construction, as shown in the real `aave_v3.py` / `uniswap_v3.py` / `morpho.py` snippets referenced above). That gap
is now a decided target-state with a real, shipped implementation — but **adoption across the ~40+ existing ad hoc
call sites is NOT yet complete**; most adapters still build `instrument_key` the old way until retrofitted. Treat this
section as "the one builder now exists and is the required target for every NEW call site," not "every adapter
already uses it."

**The one entry point**: `unified_api_contracts.build_canonical_instrument_id(asset_group, venue, instrument_type,
**kwargs)` (`unified-api-contracts/unified_api_contracts/internal/reference/canonical_id_builder.py`). It dispatches
to the one real construction implementation per asset group — callers never need to know which internal helper to
call:

```python
from unified_api_contracts import AssetGroup, InstrumentType, build_canonical_instrument_id

# CeFi
build_canonical_instrument_id(AssetGroup.CEFI, "bybit", InstrumentType.PERPETUAL, "BTCUSDT")
# → "BYBIT:PERPETUAL:BTCUSDT"

# DeFi (VENUE-CHAIN composition)
build_canonical_instrument_id(AssetGroup.DEFI, "aave_v3", InstrumentType.LENDING, "USDC", chain="arbitrum")
# → "AAVE_V3-ARBITRUM:LENDING:USDC"

# TradFi (dated derivative, structured expiry)
build_canonical_instrument_id(AssetGroup.TRADFI, "cme", InstrumentType.FUTURE, "ES", expiry_date=date(2026, 6, 20))
# → "CME:FUTURE:ES-20260620"

# Sports — dispatches to the fixture-id domain builder, NOT VENUE:TYPE:SYMBOL
# (an intentional, separately operator-confirmed design decision — see "What's
# explicitly out of scope for this canonicalization" below)
build_canonical_instrument_id(
    AssetGroup.SPORTS, league="ENG_PREMIER_LEAGUE", home_team="ARSENAL",
    away_team="CHELSEA", fixture_date="2026-03-22",
)
# → "ENG_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322"
```

Two supporting additions ship alongside the entry point, both extending — not duplicating — existing infrastructure:

- **`passthrough=True`** on `build_instrument_id()` — wraps an already-fully-formed raw/native symbol verbatim as
  `VENUE[-CHAIN]:TYPE:SYMBOL` instead of reconstructing it from `expiry_date`/`strike`/`option_right`. This closes the
  real gap that made the CCXT live-mode fix (`instruments-service@8544273d`,
  `canonical_id_p0_ccxt_live_batch_divergence_2026_07_08.md`) deliberately NOT route through this module: dated
  FUTURE/OPTION ids need to pass through the raw exchange-native id (matching Tardis), not get reconstructed from
  parts. `passthrough=True` is exactly that pass-through path, now available in the shared builder.
- **`build_leg(venue, instrument_type, symbol, *, side, ratio=1, ...)`** — builds one
  `unified_api_contracts.internal.InstrumentLeg` via the same shared construction, for multi-leg combos. Extends the
  real existing `InstrumentLeg`/`InstrumentType.COMBO` infrastructure already used by `databento/symbology.py`'s
  `_parse_cme_calendar_spread_legs` and both Deribit combo builders, each of which today builds a leg's
  `instrument_key` with its own ad hoc f-string (e.g. `f"{venue}:FUTURE:{front}"`, `f"DERIBIT:{leg_name}"` — the
  latter missing its `:TYPE:` segment entirely).

**Retrofit status (2026-07-08)**: `deribit_options_adapter.py` now calls `build_instrument_id(..., passthrough=True)`
for its OPTION `instrument_key` (behavior-preserving — same real output, `DERIBIT:OPTION:<raw_name>`, verified by the
adapter's existing regression test). The remaining ~40 ad hoc call sites (every DeFi pool/lending/LST adapter, the
on-chain-perp adapters, Deribit's combo-leg builder, TradFi's Databento/CME path, sports/prediction adapters, and
MTDS's `canonical_write.py`/`tardis_shared.py`) are tracked as a follow-up retrofit checklist — see
`unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md`.

### Top-level grammar

```
<instrument-id> ::= <venue> ":" <type> ":" <payload>

<venue>  ::= UPPER_ALNUM_DASH                      # e.g. BINANCE-FUTURES, KRAKEN-FUTURES
           | UPPER_ALNUM_DASH "-" CHAIN            # DeFi only — VENUE-CHAIN, e.g. AAVE_V3-ARBITRUM, UNISWAP_V3-ETHEREUM
<type>   ::= SPOT_ASSET | SPOT_PAIR | PERPETUAL | FUTURE | OPTION | POOL | LST
           | A_TOKEN | DEBT_TOKEN | STAKING | YIELD_BEARING | EQUITY | INDEX
           | COMBO | ...                            # full per-type catalog lives in the 5 asset-group docs
<payload>::= type-specific — see below
```

**"Instrument ID" and "instrument key" are used interchangeably** — "instrument ID" is preferred in documentation,
`instrument_key` is the code/field name.

**An `<asset-class>` prefix** (`CEFI:` / `DEFI:` / `COMMODITIES:` / etc.) appears in some older documentation as an
optional leading segment. **This is aspirational and unimplemented** — no real production row in any asset group
carries it. Do not present it as real or in-use; the real, live top-level shape is always exactly `VENUE:TYPE:PAYLOAD`.

### Venue token: dash-separated, always

DeFi protocol-on-chain venue tokens are `PROTOCOL-CHAIN`, dash-separated throughout — `AAVE_V3-ARBITRUM`,
`UNISWAP_V3-ETHEREUM`, `MORPHO-BASE`. (The underscore inside `AAVE_V3` / `UNISWAP_V3` is part of the protocol name
itself, not a venue/chain joiner.) Real adapter source confirms this — both `aave_v3.py` and `uniswap_v3.py` build
`venue_tag = f"{self._venue_prefix}-{self._chain}"`. Some older docs and a real, live, misspelled duplicate
(`AAVEV3-OPTIMISM` alongside the correct `AAVE_V3-OPTIMISM`) show the underscore-joined form
(`AAVE_V3_ETH`/`AAVE_V3_OPTIMISM`) — that form is not the target and, where it appears as a distinct real venue token
from the correctly-dashed one, is a bug (a fragmenting duplicate-spelling issue), not an accepted alternate.

### Margin marker: `@LIN` / `@INV` suffix, no trailing `@VENUE`

**Decided target**: `@LIN` (linear — quote asset is the margin currency) or `@INV` (inverse — base asset is the
margin currency), as a suffix on the instrument payload. Matches strategy-service's existing position-ID convention
(e.g. `HYPERLIQUID:PERPETUAL:ETH-USDC@LIN@HYPERLIQUID` was the old provisional form) — **explicitly decided NOT to
also append `@VENUE`**, since venue is already the first colon-segment and repeating it there is redundant (operator:
_"I don't see why you would append the venue suffix to something that already has venue in its canonical name"_).
`canonical_id_builder.py`'s current real code does **not** yet implement this — it still emits the older
`-inverse-`/`-linear-` lowercase word form embedded between underlying and expiry (e.g.
`DERIBIT:FUTURE:BTC-USD-inverse-20261226`), which the operator explicitly rejected in favor of the `@LIN`/`@INV`
suffix form for the reasons above. Both forms coexist as "not the current target" today; treat the code's own
docstring examples as stale relative to this decided target.

### Date format: `YYYYMMDD` (8-digit, sortable) — not `YYMMDD`

**Decided target**: `YYYYMMDD`, e.g. `20260731`. String-sortable (chronological order = alphabetical order).
`canonical_id_builder.py`'s real `_build_future`/`_build_option` code already emits this form
(`expiry_date.strftime('%Y%m%d')`). What is **not** yet consistent is what real venue adapters do before that point:
Bybit uses a real `DDMMMYY` format with no quote segment at all (`BYBIT:FUTURE:BTC-01DEC23`), and Deribit's real
`DDMMMYY` (`BTC-10JUL26`) — while internally clean — does not match `YYYYMMDD` either. (Kraken's raw `FF_`/`FI_`
contract-type prefix causing a 5-instrument dated-future symbol collision was a real, structural bug here too — it is
now **FIXED**, `market-tick-data-service@3d7491b1bcbebc17af0aa31219e90f38478d57cd`, confirmed in
`CEFI_INSTRUMENTS.md`'s Known-bugs table; the fix restored per-instrument distinctness but did not migrate the format
to the `@INV`/`YYYYMMDD` target — that remains a separate, still-pending migration.) An older 6-digit `YYMMDD` grammar
rule that appeared in prior docs was simply wrong; the real decided target has always been the 8-digit sortable form.

### DeFi pool format: dash-separated fee tier, not colon-separated

**Decided target**: `VENUE-CHAIN:POOL:TOKEN0-TOKEN1-FEE_TIER` (dash before the fee tier), e.g.
`UNISWAP_V3-ETHEREUM:POOL:USDC-WETH-500`. **Not** `VENUE:POOL:BASE-QUOTE:FEE_TIER@CHAIN` (colon before fee tier) as
shown in some older docs — colon is the reserved top-level `VENUE:TYPE:SYMBOL` delimiter, so a second colon inside
the payload is ambiguous to any naive `split(":")` parser. The fee tier (basis points — 100/500/3000/10000 for
Uniswap V3) must be part of the canonical symbol per operator decision, not dropped. Real production data today is
further from this than the format question alone suggests: DEX-pool `instrument_id` is, across 6,180+ real rows and
13 protocols, a **bare on-chain pool address** with zero `VENUE:TYPE:SYMBOL` structure at all (venue/chain/assets
live in separate columns) — the dash-fee-tier format above is the target once that gap is closed, not a description
of what exists today.

### Known real, live P0 bugs affecting instrument_id correctness (not just formatting)

The 2026-07-08 audit found several **live, currently-wrong** bugs distinct from the format-convention questions
above, each with its own fix plan (`instruments_master` epic):

- **Kraken-Futures dated-future symbol collision**: the MTDS Tardis adapter's underlying-extraction regex assumes a
  `TICKER-QUOTE` shape; Kraken's real raw format is `{TYPE_PREFIX}_{PAIR}_{DATE}` (e.g. `FI_XBTUSD_220325`), so BCH/
  ETH/LTC/XBT/XRP quarterly futures with the same expiry all collide onto the byte-identical
  `KRAKEN-FUTURES:FUTURE:FI-USD-inverse-20220325`. Structural — every Kraken dated future hits this.
- **23 DeFi adapters silently return empty on canonical-form type filters** — 7 lending + 16 yield/LST adapters guard
  `get_instruments()` against lowercase snake_case literals that never match the real uppercase `InstrumentType`
  enum values.
- **Live≠batch instrument_id divergence for 13 major CeFi venues** — the live-mode CCXT adapter path stores the bare
  unmodified ccxt-native symbol as `instrument_key`, never passed through any canonicalizer, while batch (Tardis)
  produces a differently-shaped id for the same real instrument.

These are tracked as their own immediately-actionable fix plans, not deferred behind the broader canonicalization
decision — see the audit doc's `resulting_plan:` list for the current plan filenames.

### What's explicitly out of scope for this canonicalization

Sports keeps its own `LEAGUE:MATCHUP:DATE`-style scheme (operator-confirmed reasonable — sports doesn't have a clean
TYPE/SYMBOL concept). The 31 shared `canonical_question_group` keys between Polymarket/Kalshi rows are **not** a
collision — venue is tracked as a separate column and sharing the thematic label across venues is the intended
cross-venue-arb comparison mechanism. TradFi's single-leg dated-derivative codes (e.g. `CME:FUTURE:6AF0`) are real
industry-standard terse contract codes, not an uncleaned internal prefix — no divergence to canonicalize there.

---

## Related documentation

- Instrument type catalogs and worked examples per asset group — see the 5 asset-group docs (CeFi / DeFi / TradFi /
  Sports / Prediction).
- `codex/04-architecture/instruments-service-as-ssot-for-mtds.md`, `…/instrument-universe-registry-consolidation.md`
  — UAC ownership of venue lists and adapter keys.
- `codex/04-architecture/shard-level-failure-isolation.md` — the per-venue error-classification pattern used
  throughout `process_instruments()`.
- `codex/06-coding-standards/cli-convention.md` — `--operation`/`--mode`/`--asset-group` CLI convention SSOT.
- `unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md` — the full operator
  decision doc for every canonical-id divergence described above, including migration-mechanics todos still open.
- `unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md` — the full 7-layer compliance
  audit (instruments-service / MTDS / deployment-api / deployment-ui / strategy-service / GCS parquet / manifest).
