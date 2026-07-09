# CeFi Instruments

> **Cross-links**: [instruments-definitions drilldown mockup — CeFi tab](https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d)
> (real per-venue instrument_id samples, live-verified 2026-07-08) ·
> [`instrument_id_format_canonicalization_2026_07_08.md`](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> (the decided target canonical format for dated derivatives, not shipped yet) ·
> [`canonical_instrument_id_audit_2026_07_08.md`](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md)
> (the full cross-repo compliance audit this doc's "Known limitations" section draws from) ·
> [`ADAPTER_ARCHITECTURE.md`](../ADAPTER_ARCHITECTURE.md) (adapter code structure, `canonical_id_builder.py` caveats) ·
> [`DEFI_INSTRUMENTS.md`](./DEFI_INSTRUMENTS.md) (on-chain/DeFi perp venues — Hyperliquid, Aster, and the rest of the
> on-chain CLOB cluster live there, not here — see "Scope" below).

## Scope

This doc covers **genuinely centralized exchanges**: Binance, Bybit, OKX, Deribit, Coinbase, Upbit, Kraken /
Kraken-Futures, Bitfinex, Bitget.

The on-chain perp CLOBs (Hyperliquid, Aster, Pacifica, Extended, Lighter) and the prediction-platform crypto-perps
(Kalshi-Perp, Polymarket-Perp) are **order-book venues, not AMM pools**, so they're classified `asset_group=cefi`
(`VENUES_BY_ASSET_GROUP["cefi"]`, `market_data_categories.py:226-274`) and share the CeFi capture/adapter path — but
their instrument specs are documented in [`DEFI_INSTRUMENTS.md`](./DEFI_INSTRUMENTS.md) and
[`PREDICTION_INSTRUMENTS.md`](./PREDICTION_INSTRUMENTS.md), not here.

## Venues

Two independent modes exist per venue family:

- **Batch mode** — Tardis (historical data). `unified_api_contracts.registry.venue_mapping.VenueMapping.tardis_to_venue`
  is the reverse Tardis-exchange-slug → canonical-venue mapping.
- **Live mode** — CCXT (real-time, public endpoints, no API key needed for instrument discovery) for a 13-canonical-venue
  subset. `instruments_service/reference_data/factory.py:95-114` (`_CANONICAL_VENUE_TO_CCXT_EXCHANGE`).

Both modes construct the same canonical `instrument_key` for the same real instrument: `VENUE:TYPE:BASE-QUOTE` for
SPOT_PAIR/PERPETUAL (reconstructed from base/quote), `VENUE:TYPE:RAW_SYMBOL` for FUTURE/OPTION (the exchange-native id
verbatim, upper-cased) — `ccxt_adapter.py::_build_instrument_key` mirrors the construction
`tardis/adapter.py::_parse_tardis_instrument` uses in batch mode, so live and batch converge on one id per instrument.

| Canonical venue                         | Batch (Tardis slug)                   | Live (CCXT id)  | Instrument types                                                                                                                                                 |
| --------------------------------------- | ------------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BINANCE-SPOT`                          | `binance`                             | `binance`       | Spot pairs                                                                                                                                                       |
| `BINANCE-FUTURES`                       | `binance-futures`                     | `binanceusdm`   | Perpetuals + dated futures, USDT-margined (linear)                                                                                                               |
| `BINANCE-DELIVERY`                      | (via binance-futures data)            | —               | COIN-M perps + dated futures, coin-margined (inverse); distinct endpoint from `BINANCE-FUTURES`                                                                  |
| `BYBIT`                                 | `bybit`                               | `bybit`         | Perpetuals + dated futures + options                                                                                                                             |
| `BYBIT-SPOT`                            | `bybit-spot`                          | `bybit`         | Spot pairs (distinct canonical venue so the perp-gate pairs BYBIT-SPOT↔BYBIT)                                                                                    |
| `OKX-SPOT` / `OKX-SWAP` / `OKX-FUTURES` | `okex` / `okex-swap` / `okex-futures` | `okx` (unified) | Spot / perpetuals ("swap") / dated futures — bare `OKX` has no instruments itself; the real data lives on these 3 suffixed venues                                |
| `DERIBIT`                               | `deribit`                             | `deribit`       | **Both** linear (USDC-settled) and inverse (USD-quoted, BTC/ETH-settled) options + futures + perps + spot — see "Deribit margin types" below                     |
| `DERIBIT-COMBO`                         | `deribit` (own adapter)               | —               | Multi-leg combo/spread instruments (33 structure codes — vertical/calendar/butterfly/condor/box/jelly-roll); own manifest shard, own `deribit_combo` adapter key |
| `COINBASE-SPOT`                         | `coinbase`                            | `coinbase`      | Spot pairs (USD quote — for coinbase premium)                                                                                                                    |
| `COINBASE-FUTURES`                      | `coinbase-international`              | —               | Coinbase Derivatives (perps); distinct canonical venue from COINBASE-SPOT                                                                                        |
| `UPBIT`                                 | `upbit`                               | `upbit`         | Spot pairs, KRW/BTC/USDT quotes (QUOTE-BASE format, inverted by the adapter — see below)                                                                         |
| `KRAKEN-SPOT`                           | `kraken`                              | `kraken`        | Spot pairs                                                                                                                                                       |
| `KRAKEN-FUTURES`                        | `cryptofacilities`                    | `krakenfutures` | Perpetuals + dated futures                                                                                                                                       |
| `BITFINEX-SPOT`                         | `bitfinex`                            | —               | Spot pairs (Tardis Tier-3)                                                                                                                                       |
| `BITFINEX-FUTURES`                      | `bitfinex-derivatives`                | —               | Perpetuals (linear USDT-margined **and** inverse BTC-margined — see "Accepted quote assets" below)                                                               |
| `BITGET-SPOT` / `BITGET-FUTURES`        | `bitget` / `bitget-futures`           | —               | Spot / perpetuals (Tardis Tier-3)                                                                                                                                |

`bitstamp`, `huobi`, `huobi-dm` remain declared in `VenueMapping.all_tardis_exchanges`
(`unified_api_contracts/registry/venue_mapping.py`, plus matching `venue_to_ccxt`/`tardis_to_venue` entries) but are
not members of `VENUES_BY_ASSET_GROUP["cefi"]` (`unified_api_contracts/registry/market_data_categories.py`) — stale
declarations excluded from the active CeFi venue set, with no row in the Venues table above. `GEMINI-SPOT` and
`PHEMEX-SPOT` have been fully removed from `VenueMapping` (no declaration remains).

### Deribit margin types

Deribit is **not** single-margin-type — it runs both linear and inverse instruments side by side, and the adapter code
distinguishes them by quote currency: `instruments_service/reference_data/adapters/cefi/tardis/parsing.py:303,410,423`
(`_resolve_base_quote` / `_infer_margin_type`) — inverse instruments are USD-quoted but BTC/ETH-settled
(`BTC-PERPETUAL`, coin-margined), linear instruments are USDC-quoted and USDC-settled (`BTC_USDC-PERPETUAL`). Both are
captured; `margin_type` (`LINEAR`/`INVERSE`) and `quote_asset` are populated per-instrument in the v6 schema.

### Deribit combo leg `instrument_key` format (fixed 2026-07-09)

`DERIBIT-COMBO` (`deribit_combo_adapter.py::_build_legs`) previously built each leg's `instrument_key` with an ad hoc
`f"DERIBIT:{leg_name}"` — missing the `:TYPE:` segment entirely (2 colon-parts instead of the workspace's
`VENUE:TYPE:SYMBOL` grammar). Fixed by routing through the shared UAC `build_leg()` builder
(`unified_api_contracts.internal.reference.canonical_id_builder`), with a per-leg `InstrumentType` classified from
Deribit's own `instrument_name` shape — `get_combos` returns only `{amount, instrument_name}` per leg, no per-leg type
field. Classification (`_classify_deribit_leg_instrument_type`, verified against real live `get_combos` responses for
BTC/ETH, 2026-07-09 — every leg name observed across both currencies' active combos was either 2 or 4 dash-parts, no
other shape): `{BASE}-PERPETUAL` → `PERPETUAL`; `{BASE}-{DDMMMYY}` (2 dash-parts) → `FUTURE`;
`{BASE}-{DDMMMYY}-{STRIKE}-{C|P}` (4 dash-parts) → `OPTION`. A leg whose name matches none of these shapes is dropped
(logged), not raised.

Real before/after (from a live `BTC-FS-10JUL26_PERP` futures-spread combo and a live `BTC-CCAL-24JUL26_10JUL26-75000`
call-calendar combo, `get_combos?currency=BTC`, 2026-07-09):

| Leg (real `instrument_name`) | Before (bug)                  | After (fixed)                        |
| ---------------------------- | ----------------------------- | ------------------------------------ |
| `BTC-PERPETUAL`              | `DERIBIT:BTC-PERPETUAL`       | `DERIBIT:PERPETUAL:BTC-PERPETUAL`    |
| `BTC-10JUL26`                | `DERIBIT:BTC-10JUL26`         | `DERIBIT:FUTURE:BTC-10JUL26`         |
| `BTC-17JUL26-65000-C`        | `DERIBIT:BTC-17JUL26-65000-C` | `DERIBIT:OPTION:BTC-17JUL26-65000-C` |

The combo's own top-level `instrument_key` (e.g. `DERIBIT:COMBO:BTC-CCAL-24JUL26_10JUL26-75000`) was already correct
and unaffected — the bug was leg-scoped only.

### UPBIT symbol inversion

UPBIT uses QUOTE-BASE format (`KRW-BTC` = buy BTC with KRW). The Tardis adapter detects this and inverts base/quote
before filtering (`parsing.py:350-352`).

### Expiry parsing

Deribit symbols encode expiry: `BTC-27MAR26-190000-C` → `2026-03-27`. The adapter parses `DDMMMYY` from the second
segment when Tardis's own `expiry` field isn't populated (common for active options).

## MVP Universe

The MVP-scoping mechanism is the UAC registry `unified_api_contracts/registry/cefi_instrument_universe.py` — the SSOT
for which base assets instruments-service tracks (and MTDS captures) across every CeFi venue.

### `CEFI_BASE_ASSET_UNIVERSE` — the real curated set (~540 base assets)

Per operator decision 2026-06-23, this is the **union of three tranches** (survivorship-bias-free by design — it
deliberately keeps delisted/declined coins, not just today's survivors):

1. **Legacy 44** — the prior MVP subset (top-cap majors + EigenLayer-dust + FTT/LUNA delisting-test coins). All kept.
2. **Top-100-by-market-cap aggregated across time since 2019** — the union of coins that were top-100 at each
   year-end/cycle-peak snapshot since 2019. Because top-100 membership churns hard across a cycle, this is a few
   hundred unique base assets and deliberately includes coins that later declined or delisted (LUNA, LUNC, UST, FTT,
   SRM, CEL, WAVES, HT, OKB, OMG, …) — no live market-cap API exists, so this tranche is a checked-in curated frozenset,
   not fetched live.
3. **All HYPERLIQUID + ASTER perp base assets** — every base asset with a perp listed on HL/ASTER is folded in, so a
   coin tradable there is also captured on the CEX venues for cross-venue price/funding dispersion (HL/ASTER themselves
   are out of this doc's scope — see "Scope" above — but their listed-base-asset set still feeds this CeFi universe).

Quote assets are **not** filtered by this set — any canonical accepted quote is fine as long as the base asset is a
member.

### Accepted quote assets

Fleet default: `USDT`, `USDC`, `USD` (`CEFI_ACCEPTED_QUOTE_ASSETS`, `cefi_instrument_universe.py:131-133`). Two
per-venue extensions exist (`_CEFI_VENUE_QUOTE_EXTENSIONS`): **UPBIT** additionally accepts `KRW` (keyed on the
ENTITY prefix, so `UPBIT`/`UPBIT-SPOT` both resolve) — KRW is deliberately not added fleet-wide (it would admit
thousands of cross pairs on other venues). **BITFINEX-FUTURES** additionally accepts `BTC` (keyed on the FULL
canonical venue string, NOT the bare `BITFINEX` entity, so the sibling `BITFINEX-SPOT` does **not** get the
extension).

### Options underlyings

Deribit options stay restricted to **BTC and ETH only** (`CEFI_OPTIONS_UNDERLYINGS`, `cefi_instrument_universe.py:183-186`)
— a genuine data-volume constraint (Deribit already has ~213K historical option rows per the adapter's own code
comment; a per-coin option-chain expansion would multiply that further for little added value).

### Equity/commodity-basis perp universe

`CEFI_EQUITY_PERP_BASE_UNIVERSE` (`cefi_instrument_universe.py`) is a separate curated set covering
single-stock/commodity/index perps listed on crypto venues (**124 entries**: US and Korean equities, the Binance
tradfi-perp-symmetry set, pre-IPO/premarket share perps, commodities, and index/sector/leveraged ETFs) — this exists
specifically so each crypto-venue tradfi-underlying perp has a captured basis-arb counterpart in the TradFi MVP
universe. Crypto majors (BTC/ETH/SOL/…) are **not** in this set — only the tradfi-underlying perps.

**Six tickers kept despite an open cross-venue question**: `CFG`, `DIA`, `INX`, `ROBO`, `SLX`, `SPX` no longer appear
under `contractType=TRADIFI_PERPETUAL` on Binance; Binance now tags them `underlyingType=COIN` (subtypes
Meme/Infrastructure/RWA/Alpha) — the ticker has been reused by an unrelated crypto token (e.g. `SPX` is now the
"SPX6900" meme coin, not the S&P 500), not a simple delisting. All 6 still have live symmetric entries in
`TRADFI_EQUITY_PERP_BASIS_UNIVERSE` (real ETFs/stocks) and in `crypto_equity_link.py`. Resolving this needs a
dedicated cross-venue audit (does OKX/Bybit's same-ticker perp reuse the same crypto token, or a genuine surviving
equity-basis product?) rather than a unilateral removal.

### Two separate layers — raw capture vs. MVP classification (don't conflate them)

There are two distinct, independently-gated mechanisms here — a doc-reader (or agent) can easily conflate them since
both involve "is this base asset in scope":

1. **Raw capture gate** (what actually gets fetched from the venue and written to GCS) — per
   `instruments_service/reference_data/adapters/cefi/tardis/parsing.py:430-466` (`_passes_asset_filter`), the curated
   base-asset whitelist is **no longer a gate** for SPOT/PERP/FUTURE (operator 2026-06-23 — the reference universe
   must equal each venue's real listed universe, not a curated subset, so small/illiquid-coin funding/price history
   isn't lost). The two gates that remain here are venue-volume-safe, not coin-curation:
   1. **Accepted-quote gate** — USDT/USDC/USD fleet-wide + the per-venue KRW/UPBIT and BTC/BITFINEX-FUTURES
      extensions. Drops exotic cross pairs (`BASE/EUR`, `BASE/BTC`) on other venues (incl. `BITFINEX-SPOT`, which
      does NOT get the BTC extension); most derivatives carry no quote and pass trivially — Bitfinex derivatives is
      the documented exception (it DOES resolve a real quote).
   2. **Options-underlying gate** — BTC/ETH only, as above.

2. **MVP classification** (a separate, later-stage question — "does this already-captured row count as MVP for
   downstream reporting/scope purposes") — real function:
   `unified_api_contracts/canonical/crosscutting/mvp_scope.py::is_in_mvp_capture_universe`. This one DOES still
   apply a real spot-vs-perp rule, fully active, not superseded by the 2026-06-23 raw-capture change above:
   - **PERP / EQUITY_PERP**: in-universe on base-membership alone — "the perp IS the gate," no spot required.
   - **SPOT** (`SPOT_PAIR`/`SPOT_ASSET`): HARD-gated — requires the _same venue_ to also list a perp for that base —
     **except**: (a) the base is in `STAKING_SPOT_EXCEPTION` (`cefi_instrument_universe.py:249-254` — 28
     liquid-staking/restaking tokens: STETH, WSTETH, RETH, WEETH, EETH, EIGEN, ETHFI, the Solana LSTs
     MSOL/JITOSOL/JSOL/BSOL/SCNSOL/INF, and more — captured on ANY venue regardless of perp existence, the
     `carry_staked_basis`/DeFi-seasonal-reward legs the strategy layer needs spot liquidity for), or (b) the venue
     is in `_CEFI_SPOT_PERP_GATE_EXEMPT_VENUES = {UPBIT}` (Upbit lists no perps at all — spot-only Korean exchange —
     so its spot is captured unconditionally rather than vacuously failing the gate).
   - **Quote assets** (USDT/USDC/USD/BTC/ETH) sidestep this rule entirely — they're validated by the separate
     accepted-quote gate above, never evaluated as a "base" needing a perp.
   - **DATED FUTURE**: base-membership + venue only, not perp-gated (shares the base with the futures complex).

## Instrument ID format: current vs. decided target (dated derivatives)

**This section shows real current output alongside the operator-decided target, same current-vs-target framing as the
A_TOKEN/DEBT_TOKEN lending-instrument decision.** Full detail:
[`instrument_id_format_canonicalization_2026_07_08.md`](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md).

Dated-derivative (FUTURE/OPTION) instrument_ids are **not consistent with each other across venues today**, and don't
get the same cleanup the same venue's own PERPETUAL gets:

| Venue           | Real current instrument_id                       | Note                                                                                                                                           |
| --------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| KRAKEN-FUTURES  | `KRAKEN-FUTURES:FUTURE:XBT-USD-inverse-20260731` | Raw `FI_`/`FF_`/`PI_`/`PF_` prefix stripped, still `-inverse-` word-form margin marker + raw expiry digits, not yet the target `@INV-YYYYMMDD` |
| BINANCE-FUTURES | `BINANCE-FUTURES:FUTURE:BTCUSDT_260925`          | Raw concatenated base+quote, underscore-date                                                                                                   |
| BYBIT           | `BYBIT:FUTURE:BTC-01DEC23`                       | No quote segment at all; `DDMMMYY` date                                                                                                        |
| DERIBIT         | `DERIBIT:OPTION:BTC-10JUL26-48000-C`             | `DDMMMYY` date — looks clean in isolation, but does not match the other 3 venues, and `DDMMMYY` does not sort chronologically as a string      |

**Decided target (operator, 2026-07-08)**: `VENUE:TYPE:BASE[_QUOTE]@LIN|@INV-YYYYMMDD[-STRIKE-C|P]` — uniform across
every CeFi venue and both dated-derivative types. Examples: `KRAKEN-FUTURES:FUTURE:XBT-USD@INV-20260731`,
`BYBIT:FUTURE:BTC-USDT@LIN-20231201`, `DERIBIT:OPTION:BTC@INV-20260710-48000-C`. Two settled sub-decisions:

- **Margin marker = `@LIN`/`@INV` suffix**, not `canonical_id_builder.py`'s already-written-but-unused `-linear-`/
  `-inverse-` word form — chosen to match strategy-service's existing `@LIN`/`@INV` position-id convention (e.g.
  `HYPERLIQUID:PERPETUAL:ETH-USDC@LIN@HYPERLIQUID`) rather than add a 3rd convention. **No trailing `@VENUE`** — venue
  is already the first colon-segment, a trailing venue suffix is redundant.
- **Date format = `YYYYMMDD`**, not Deribit's `DDMMMYY` — string-sortable (chronological order = alphabetical order),
  where `DDMMMYY` is not (`"10APR26"` sorts after `"10JAN27"` despite being earlier). This means Deribit — previously
  assessed elsewhere as "already canonical, no fix needed" — is **also** in scope for this migration.

The convention must be enforced via real, callable builder functions everywhere it applies, not docstring-only
assertions (the current state of both `canonical_id_builder.py` and strategy-service's `@LIN`/`@INV` handling). Migration
mechanics (backfill vs. go-forward-only) are an open todo in the canonicalization decision doc, not yet scoped.

## Known limitations

Full audit trail: [canonical instrument_id audit](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).

| Limitation                                                                              | Detail                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`CEFI_EQUITY_PERP_BASE_UNIVERSE` ticker reuse**                                       | Six tickers (`CFG`/`DIA`/`INX`/`ROBO`/`SLX`/`SPX`) are kept pending a dedicated cross-venue audit — see "Equity/commodity-basis perp universe" above.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| **Kraken-Futures dated-future `instrument_id` — margin-marker format not yet migrated** | Per-(ticker, expiry) distinctness is correctly resolved (`_KRAKEN_FUTURES_RE` in `market-tick-data-service/.../adapters/cefi/tardis_shared.py` extracts the real ticker from the `{TYPE_PREFIX}_{PAIR}_{DATE}` raw symbol shape), but the format is still the v6 word-form margin marker (`KRAKEN-FUTURES:FUTURE:XBT-USD-inverse-20260731`), not yet the operator-decided `@INV-YYYYMMDD` target — see "Instrument ID format" above.                                                                                                                                                                                                                                                                            |
| **Kraken-Futures `FI_` vs `FF_` same-(ticker, expiry) ambiguity**                       | 13 real (ticker, expiry) combinations (`ETH`/`XBT`, 2024-2026 range) have both a real `FI_` and a real `FF_` raw Tardis symbol with different row counts (e.g. `FI_ETHUSD_240329` vs `FF_ETHUSD_240329`) that collapse onto the same `instrument_id` today — `derive_row_instrument_id`'s FUTURE branch (`tardis_shared.py`) has no field to encode the `FI`/`FF` contract-subtype. The adjacent code comment describing `FI*` as pre-2020/"no longer active" is contradicted by real 2024-2026 capture data (a data-state claim, not independently re-verified here). Needs an operator decision on what `FI*` vs `FF*` represents for KRAKEN-FUTURES before it can be encoded in the canonical instrument_id. |

## Caching strategy

Three layers:

1. **Adapter TTL cache** — `get_instruments_cached()` stores results for 1hr; first call hits the API, subsequent calls
   return instantly.
2. **Factory adapter pool** — all Tardis venues share one adapter instance with one shared cache.
3. **Concurrency cap** — `asyncio.Semaphore(4)` limits concurrent API calls.

Tardis returns ALL instruments ever listed with `availableSince`/`availableTo` timestamps, so the cache means this call
happens once per run, not once per venue.

## Schema

CeFi instruments carry `asset_group="crypto"` always. Key CeFi-relevant fields on the canonical `InstrumentRecord`
(`unified_api_contracts.canonical.domain.instruments_catalog`): `margin_type` (`LINEAR`/`INVERSE`, set when relevant —
see "Deribit margin types" above), `quote_asset`, `combo_type` + `leg_weights` (Deribit combos/spreads). Session
metadata (`is_trading_day`, `regular_open_utc`, etc.) is always `None` for CeFi — crypto markets are 24/7.

Per-shard bundle validation (options/futures chains) is mandatory at `record_captured` via `expected_root_clusters` +
`cluster_extractor` kwargs; the three-category empty-output decision (A/B/C — see
[`POST_PLAN_REALITY_2026_05_06.md`](../../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md)) applies to
every CeFi adapter; a bare `_create_empty_output()` placeholder method is banned.

## Related documentation

- [`ADAPTER_ARCHITECTURE.md`](../ADAPTER_ARCHITECTURE.md) — adapter code structure, the general
  `VENUE:TYPE:PAYLOAD[@CHAIN]` grammar, and `canonical_id_builder.py`'s real (limited) usage.
- [`DEFI_INSTRUMENTS.md`](./DEFI_INSTRUMENTS.md) — on-chain perp CLOBs (Hyperliquid, Aster, Pacifica, Extended,
  Lighter) and the rest of the DeFi universe.
- [`TRADFI_INSTRUMENTS.md`](./TRADFI_INSTRUMENTS.md) — the TradFi-underlying side of the equity/commodity-basis perp
  arc referenced above.
- [`PREDICTION_INSTRUMENTS.md`](./PREDICTION_INSTRUMENTS.md) — Kalshi-Perp / Polymarket-Perp crypto perps (distinct
  from those venues' YES/NO prediction markets).
