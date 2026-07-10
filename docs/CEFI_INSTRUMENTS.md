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

`BITSTAMP-SPOT`, `HUOBI-SPOT`/`HUOBI-FUTURES` (Tardis `bitstamp`/`huobi`/`huobi-dm`), `GEMINI-SPOT`, and `PHEMEX-SPOT`
are no longer part of the CeFi venue universe — fully removed from `VenueMapping` (no declaration remains), with no
row in the Venues table above.

### Deribit margin types

Deribit is **not** single-margin-type — it runs both linear and inverse instruments side by side, and the adapter code
distinguishes them by quote currency: `instruments_service/reference_data/adapters/cefi/tardis/parsing.py`
(`_resolve_base_quote` / `_infer_margin_type`) — inverse instruments are USD-quoted but BTC/ETH-settled
(`BTC-PERPETUAL`, coin-margined), linear instruments are USDC-quoted and USDC-settled (`BTC_USDC-PERPETUAL`). Both are
captured; `margin_type` (`LINEAR`/`INVERSE`) and `quote_asset` are populated per-instrument in the v6 schema. The live
options-chain adapter (`deribit_options_adapter.py`) resolves the same signal from Deribit's own `quote_currency` field
(`USD`=inverse, `USDC`=linear), falling back to `settlement_currency` for older/partial payloads — Deribit's
`_infer_margin_type` branch (Tardis batch path) was already correct before this pass (unlike the Bybit/Kraken-Futures/
OKX bugs below).

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

| Venue           | Real current instrument_id                                                   | Note                                                                                                                                                                                                                                                                                                                                                                                                                             |
| --------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| KRAKEN-FUTURES  | `KRAKEN-FUTURES:FUTURE:ETH-USD@LIN-20230728`                                 | **MIGRATED 2026-07-09** (`prod/catalog.parquet` + all 4,752 real per-day snapshots) — see "BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration" below. Raw Tardis form was `KRAKEN-FUTURES:FUTURE:FF_ETHUSD_230728` (untransformed `FI_`/`FF_`/`PI_`/`PF_`-prefixed passthrough, not the `-inverse-` word-form previously documented here — that shape was never actually what `prod/catalog.parquet` carried; corrected 2026-07-09). |
| BINANCE-FUTURES | `BINANCE-FUTURES:FUTURE:BTCUSDT_260925`                                      | Raw concatenated base+quote, underscore-date                                                                                                                                                                                                                                                                                                                                                                                     |
| BYBIT           | `BYBIT:FUTURE:BTC-USD@INV-20231201`                                          | **MIGRATED 2026-07-09** (`prod/catalog.parquet` + all 4,788 real per-day snapshots) — see "BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration" below. Raw Tardis form was `BYBIT:FUTURE:BTC-01DEC23` (no quote segment, `DDMMMYY` date).                                                                                                                                                                                             |
| DERIBIT         | `DERIBIT:OPTION:BTC-10JUL26-48000-C`                                         | `DDMMMYY` date — looks clean in isolation, but does not match the other 3 venues, and `DDMMMYY` does not sort chronologically as a string. **The live options-chain adapter now emits the target format** (`DERIBIT:OPTION:BTC@INV-20260710-48000-C`) — see "Deribit `@LIN`/`@INV` migration" below; Tardis-batch/CCXT-live still emit the raw form shown here.                                                                  |
| OKX-FUTURES     | `OKX-FUTURES:FUTURE:BTC-USD_UM-260710` / `OKX-FUTURES:FUTURE:BTC-USD-260710` | Raw Tardis id passthrough (`YYMMDD`, no separate margin marker) — the two real siblings above are the SAME underlying + expiry but genuinely opposite margin types (`_UM` = linear, bare = inverse; see margin-type bug entry below), currently indistinguishable without reading the literal `_UM` substring                                                                                                                    |

**Decided target (operator, 2026-07-08)**: `VENUE:TYPE:BASE[_QUOTE]@LIN|@INV-YYYYMMDD[-STRIKE-C|P]` — uniform across
every CeFi venue and both dated-derivative types. Examples: `KRAKEN-FUTURES:FUTURE:XBT-USD@INV-20260731`,
`BYBIT:FUTURE:BTC-USD@INV-20231201`, `DERIBIT:OPTION:BTC@INV-20260710-48000-C`. Two settled sub-decisions:

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

**PERPETUAL scope-expanded 2026-07-09** — the `@LIN`/`@INV` marker also applies to PERPETUAL (no date suffix; see the
issue doc's finding 1 sub-decision). Real proof case: `KRAKEN-FUTURES:PERPETUAL:AAVE-USD` (raw `PF_AAVEUSD`, genuinely
linear) vs `KRAKEN-FUTURES:PERPETUAL:BTC-USD` (raw `PI_XBTUSD`, genuinely inverse) — same quote, indistinguishable
without the marker, and previously BOTH mislabeled `linear` in `prod/catalog.parquet` (see next paragraph).

**BYBIT + KRAKEN-FUTURES margin-type bug — FIXED 2026-07-09 (`instruments-service`,
`reference_data/adapters/cefi/tardis/parsing.py::_infer_margin_type`)**. Neither venue had a venue-specific branch in
`_infer_margin_type` before this fix, so every real inverse (coin-margined) instrument on both venues silently fell
through to the `linear` default. Real, live-verified impact (`prod/catalog.parquet`, 2026-07-09):

| Venue          | Real rows | Previously mislabeled `linear` (now correctly `inverse`) |
| -------------- | --------- | -------------------------------------------------------- |
| BYBIT          | 1,543     | 279 (255 FUTURE + 24 PERPETUAL)                          |
| KRAKEN-FUTURES | 1,103     | 382 (378 FUTURE + 4 PERPETUAL)                           |

Kraken-Futures' margin type is **not quote-inferable at all** — `PI_XBTUSD` (inverse) and `PF_XBTUSD` (linear) both
resolve quote `USD` via `_split_kraken_symbol`; the fix reads the real `PI_`/`FI_` (inverse) vs `PF_`/`FF_` (linear)
raw instrument-type prefix instead. Bybit's fix also closed a real, separate base/quote-resolution gap
(`_split_bybit_symbol`, new): the legacy no-dash CME-style quarterly shape (`BTCUSDH22` — 46 real symbols on the live
Tardis venue listing, BTC/ETH only) failed quote resolution entirely under the old generic splitter and was **silently
dropped** by `adapter.py`'s empty-quote per-instrument skip — confirmed absent from `prod/catalog.parquet` today (0
rows), i.e. these 46 real instruments were never captured at all, not just mislabeled.

A real, standalone `@LIN`/`@INV`-`YYYYMMDD` canonical-symbol builder (`_build_perpetual_canonical_symbol`,
`_build_dated_derivative_canonical_symbol`, `_build_canonical_perpetual_key`, `_build_canonical_future_key` —
routed through the shared UAC `build_instrument_id(..., passthrough=True)` builder) now exists in the same module for
both venues, unit-tested against real Tardis-verified fixtures
(`tests/unit/test_bybit_kraken_futures_canonical_id.py`). **The `prod/catalog.parquet` + per-day backfill described as
deferred here previously is now DONE, 2026-07-09** — see "BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration" below for
the real before/after counts. **Still not yet wired into `adapter.py`'s live `instrument_key` construction**
(`adapter.py` still emits `VENUE:TYPE:BASE-QUOTE` for PERPETUAL / `VENUE:TYPE:RAW_ID` for FUTURE, no marker embedded)
— that go-forward wiring is deliberately deferred (`adapter.py` had a concurrent sibling edit in flight this session,
same reasoning as the Deribit/Binance/OKX entries elsewhere in this doc).

**OKX-SWAP/OKX-FUTURES margin-type bug — FIXED 2026-07-09, real and BACKWARDS (not just unlabeled)**
(`instruments-service`, `reference_data/adapters/cefi/tardis/parsing.py::_infer_margin_type`). Unlike Bybit/Kraken
(a missing branch that silently defaulted to `linear`), OKX had one unconditional, exchange-unscoped check —
`if "USD_UM" in upper_id or "USD_CM" in upper_id: return INVERSE` — that was **directly backwards**, plus the same
missing-branch gap as Bybit/Kraken for the bare-quote case. Verified directly against the LIVE OKX public
`/api/v5/public/instruments` REST endpoint (no auth, `instType=SWAP` — 416 real rows — and `instType=FUTURES` — 105
real rows), 2026-07-09: every real dated-future `instId` carrying the literal `_UM` infix (e.g.
`BTC-USD_UM-260710`) is `ctType=linear` (`settleCcy="USD"` is a synthetic cross-margin unit, NOT the base asset;
`ctValCcy=BTC`) — the old code mapped this to `INVERSE`, the opposite of the real value. The bare sibling with no
`_UM` (e.g. `BTC-USD-260710`) is `ctType=inverse` (`settleCcy=BTC`, the real base asset) — the old code had no OKX
branch for this case at all, so it fell through to the generic `linear` default, again the opposite of the real
value. OKX-SWAP perpetuals never carry `_UM`/`_CM` at all (0 of 416 real rows) and follow the same bare-USD=inverse
rule. Real, live-verified impact (`prod/catalog.parquet`, 2026-07-09):

| Venue       | Real rows | Wrong before fix                                                                                                        |
| ----------- | --------- | ----------------------------------------------------------------------------------------------------------------------- |
| OKX-SWAP    | 623       | 45 (bare-`USD`-quoted PERPETUAL, mislabeled `linear`, should be `inverse`)                                              |
| OKX-FUTURES | 5,430     | 2,715 (2,582 bare-`USD` FUTURE mislabeled `linear` + 133 `_UM`/`_UM_XPERP` FUTURE mislabeled `inverse`, both backwards) |

**2,760 of 6,053 real OKX-SWAP/OKX-FUTURES rows (45.6%) carried the opposite of their true margin type** — the
largest real blast radius found across the whole `_infer_margin_type` fix pass (Bybit 279/1,543, Kraken-Futures
382/1,103). A real, bounded, backed-up smoke-test migration (21 rows: 7 OKX-SWAP + 7 bare-`USD` OKX-FUTURES + 7
`_UM` OKX-FUTURES, spanning all 3 wrong buckets) ran directly against the live `prod/catalog.parquet`
(`instruments-store-cefi-prd-central-element-323112`) 2026-07-09 — backup
`prod/catalog.okxmarginfix-smoketest.20260709-091832.bak.parquet`, all 21 corrections verified correct by
re-downloading the written blob. Real before/after examples: `OKX-SWAP:PERPETUAL:1INCH-USD` `linear`→`inverse`;
`OKX-FUTURES:FUTURE:ADA-USD-210416` `linear`→`inverse`; `OKX-FUTURES:FUTURE:BTC-USD_UM-260220` `inverse`→`linear`.
Measured: recompute cost is negligible (~0.1ms/row); the dominant cost is the single full-catalog
download+patch+upload+verify round trip (~13s observed, independent of row count since the whole parquet is
rewritten in one shot) — the full sweep was NOT a long-running job, it was the same ~13-15s single-file rewrite as
this smoke test.

**`prod/catalog.parquet` full sweep — FIXED 2026-07-09 (real, complete, not just smoke-tested)**
(`scripts/canonicalize_okx_margin_type_2026_07_09.py --apply --full-sweep`). Real measured before/after: 2,753 of
6,053 real OKX-SWAP/OKX-FUTURES rows (2,701 OKX-FUTURES + 52 OKX-SWAP) still carried the backwards `margin_type` at
sweep time (2,739 was this pass's own pre-sweep estimate before the exact recount; the 14-row gap vs. the earlier
`~2,739` estimate reflects live re-measurement against the exact current catalog, not a new regression) — all 2,753
rewritten in ~12s and verified correct by re-download; a fresh 0-writes dry-run immediately after confirms **0
remaining mismatches** across all 6,053 real rows. Backup (full-sweep, pre-write):
`prod/catalog.okxmarginfix-fullsweep.20260709-113203.bak.parquet` (the earlier 21-row smoke-test backup
`…-091832.bak.parquet` is superseded).

**`instrument_availability/by_date/` OKX-SWAP/OKX-FUTURES per-day snapshot corpus — PARTIALLY fixed 2026-07-09, then
found INCOMPLETE by a same-day legacy-GCS-naming audit (see "CeFi legacy GCS path-shape audit" below) — corrected
here, do not trust the "FIXED"/"0 remaining" claims this paragraph originally made.** Real scoping (concurrent
**delimited** GCS listing — `day=D/` prefixes, then a per-day venue-subfolder direct-child check, NOT a full
recursive corpus walk): **4,762 real files** (2,381 OKX-SWAP + 2,381 OKX-FUTURES day-partitions, 2020-01-22 to
present). Direct sampling before the fix confirmed the identical inversion bug in this corpus (e.g. real
`OKX-FUTURES` 2023-06-15 row `BTC-USD-230616`, `quote_asset=USD`, mislabeled `linear`, should be `inverse`) — this
corpus is what `catalog.parquet` is rolled up FROM (`instruments_service/engine/orchestrator/writers.py`), so the
catalog-only fix above would have been silently reverted by the next scheduled catalogue rebuild had this corpus
been left unmigrated. Ran with real concurrency (`--by-day --apply --by-day-full-sweep --workers 40`,
ThreadPoolExecutor) across all 4,762 files it found, backing up each touched file individually in place
(`instruments.okxmarginfix.<ts>.bak.parquet` alongside the original) before rewriting it. **~155,889 real rows
corrected** (measured by summing mismatches in each per-file backup against the corrected value; a small number of
files were touched twice due to one run being interrupted mid-sweep by a shell timeout — each interrupted file was
safely re-processed to completion on the next pass, so this can very slightly inflate the row count via a handful of
duplicate backups, not under- or mis-correct any row). A "final, complete" idempotent full-sweep re-run confirmed
**0/4,762 files touched** and was read at the time as proof the entire corpus was fixed — **that conclusion was
WRONG**: the listing this script (and its own re-verification pass) used can structurally only ever see ONE of 4
real coexisting path shapes for this exact same per-day corpus (see the audit below) — the other 3 shapes, 4,814
more real files carrying the SAME unfixed bug, were never in scope for either the fix or its self-verification.
Those 4,814 files are migrated by a separate follow-up script — see "CeFi legacy GCS path-shape audit (2026-07-09)"
below for the full real finding and fix.

**OKX does NOT need `@LIN`/`@INV` instrument_key marker wiring (checked 2026-07-09, unlike Bybit/Kraken-Futures/
Deribit/Binance-Futures/Binance-Delivery above)** — `_build_canonical_perpetual_key`/`_build_canonical_future_key`
exist in `parsing.py` (shared, venue-agnostic) but are correctly never called for OKX: PERPETUAL's
`BASE-QUOTE` reconstruction is already unambiguous (margin type is a deterministic function of the quote token itself
— `USD`→inverse, `USDT`/`USDC`→linear — so no two real OKX-SWAP instruments can share a `BASE-QUOTE` string with
different margin types), and FUTURE's raw-`instId`-upper-cased passthrough already preserves the real `_UM`/`_CM`
infix (or its absence) verbatim, so a linear and an inverse dated future for the same `(base, expiry)` never collide
either (`BTC-USD_UM-260220` vs `BTC-USD-260220` are already distinct strings). No `adapter.py`/`ccxt_adapter.py`
change is needed or was made for OKX — those files remain untouched by this pass (a concurrent sibling edit was in
flight on `adapter.py` this session, so they were correctly not touched regardless).

### CeFi legacy GCS path-shape audit (2026-07-09)

Operator decision (`instrument_id_format_canonicalization_2026_07_08.md`, "generalized finding" 2026-07-09): a
sibling on-chain-perp workflow found ~99% of "captured" HL/ASTER historical objects sat under an even-older
bare-symbol filename shape no migration script recognized — audit CeFi for the same class of problem. Scope: real
GCS listing (not the manifest summary — one flat `gsutil ls -r` over `instrument_availability/by_date/`, 110,636
real objects, single walk) across BINANCE-FUTURES, BINANCE-DELIVERY, BYBIT, KRAKEN-FUTURES, DERIBIT, OKX-SWAP,
OKX-FUTURES.

**Result: 4 distinct real path shapes coexist for the per-day snapshot corpus, all under the SAME fixed leaf
filename (`instruments.parquet`) — CeFi has NO bare-symbol-per-instrument-file shape anywhere, unlike the HL/ASTER
on-chain-perp case.** The divergence here is partition-path depth, not filename identity ambiguity — venue is always
still recoverable from the path's `venue=` segment in every shape, and every other field (`raw_symbol`,
`quote_asset`, `instrument_type`, …) needed to re-derive a correction already lives in the row data itself, not the
filename:

| Shape                      | Real path template                                                                           | Real object count (7 target venues) |
| -------------------------- | -------------------------------------------------------------------------------------------- | ----------------------------------- |
| A (bare)                   | `day=D/venue=V/instruments.parquet`                                                          | 33,602                              |
| B (pipelined)              | `day=D/pipeline_mode=batch_instruments_service/asset_group=cefi/venue=V/instruments.parquet` | 28,710                              |
| C (doubled-day, pipelined) | `day=D/pipeline_mode=.../asset_group=cefi/day=D/venue=V/instruments.parquet`                 | 144                                 |
| D (doubled-day, bare)      | `day=D/day=D/venue=V/instruments.parquet`                                                    | 144                                 |

Shape B is not a legacy relic superseded by shape A (or vice versa) — both span the FULL real corpus in parallel for
the same `(day, venue)`, 2019-current (e.g. real DERIBIT: shape A 2019-03-30..2026-07-08, shape B
2019-03-30..2026-06-28). Shapes C/D are a real doubled-`day=`-partition-key bug bounded to 18 real dates
(2026-05-05..2026-05-22) across 12 venues.

**Coverage check against the 5 real 2026-07-09 canonicalization scripts** (verified by exact reconciliation — real
total objects vs. real `.bak` backup count already written per venue, not just reading the code):

| Script                                                        | Listing method                                                                                                                                                      | Real coverage                                                                                                                                                                         |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `canonicalize_bybit_kraken_futures_catalog_2026_07_09.py`     | Flat substring scan (`"venue=X/" in blob.name`) over the whole `by_date/` prefix — depth-agnostic                                                                   | **Full** — 9,540 real objects / 9,560 real `.bak` (the +20 is a separate, already-documented double-backup-of-a-backup artifact, not a coverage gap)                                  |
| `canonicalize_binance_futures_delivery_catalog_2026_07_09.py` | Same flat substring pattern                                                                                                                                         | **Full** — 9,234/9,234 exact                                                                                                                                                          |
| `canonicalize_deribit_id_markers_2026_07_09.py`               | Same flat substring pattern (docstring explicitly notes "both the legacy and `pipeline_mode=`-prefixed path shapes are real, distinct blobs and both are in scope") | **Full** — 5,342/5,342 exact                                                                                                                                                          |
| `canonicalize_okx_margin_type_2026_07_09.py` `--by-day`       | **Two-level DELIMITED (non-recursive) listing** — `day=D/` prefixes, then checks only whether `venue=V/` is a DIRECT CHILD of each `day=D/` prefix                  | **Gap — only shape A** (4,762/9,576 real files, 49.7%). Structurally can never discover shape B (nested one level deeper, under `pipeline_mode=.../asset_group=cefi/`) or shapes C/D. |

Confirmed with real content, not just path inspection: sampled `day=2023-06-15` OKX-FUTURES — the already-migrated
shape-A copy correctly shows `BTC-USD-230616` as `inverse`; the shape-B copy of the SAME (day, venue, instrument)
still showed `linear` for all 60 real rows in that file (the original, pre-fix bug) before this audit's fix ran.

**Fix — `scripts/legacy_naming_audit_okx_2026_07_09.py`, real full sweep, RAN 2026-07-09.** Same
`_expected_margin_type` correction rule as `canonicalize_okx_margin_type_2026_07_09.py` (byte-for-byte identical
formula), applied via a flat, depth-agnostic listing (same proven pattern as the Bybit/Kraken/Binance/Deribit
scripts) so no path shape — known or yet-undiscovered — can hide a target file again. Real results (`--apply
--confirm --full-sweep --workers 30`, all 9,576 real target files scanned — shape A included, idempotently, to
prove nothing was missed): **files_scanned=9,576, files_written=4,798, rows_fixed=155,614, errors=0**, elapsed 927s
(15.5 min, 0.097s/file avg — the listing itself reused an already-fresh real full `by_date/` GCS listing captured
minutes earlier for this same audit, rather than re-walking the bucket a second time). `files_written` (4,798) is
4,814 (shapes B+C+D) minus 16 (already fixed by this same script's own pre-sweep 30-file smoke test, correctly
reporting 0 further changes on re-scan — idempotent). A full-corpus re-verification dry-run immediately after
(`--full-sweep`, no `--apply`, re-download + re-check of all 9,576 real files) confirmed **files_written=0,
rows_fixed=0, errors=0** — 0 remaining legacy-shape-hidden mismatches across the ENTIRE real corpus, all 4 path
shapes, not just the one the original script's listing could see. Each touched file backed up individually in place
(`instruments.legacynamingauditokx.<ts>.bak.parquet`) before rewriting.

No equivalent gap exists for BINANCE-FUTURES/DELIVERY, BYBIT, KRAKEN-FUTURES, or DERIBIT — their scripts' flat
listings already covered every shape found (confirmed above), so no new script was needed for those venues. DeFi
(13 DEX-pool protocols + lending/staking) was audited in parallel by a separate in-flight sibling workflow this
session (`legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py`) — not restated here, out of this doc's CeFi
scope.

### Deribit `@LIN`/`@INV` migration (2026-07-09)

Deribit's dated-derivative + perpetual instrument_id now has a real, callable canonical builder and a shipped
consumer, per the doc's own worked examples above (`DERIBIT:FUTURE:BTC@INV-20260710`,
`DERIBIT:OPTION:BTC@INV-20260710-48000-C`, `DERIBIT:PERPETUAL:BTC-USDC@LIN`) — Deribit's target intentionally **drops
the quote segment for FUTURE/OPTION** (`BASE@MARKER-YYYYMMDD[-STRIKE-C|P]`, no `-QUOTE-`) while **keeping it for
PERPETUAL** (`BASE-QUOTE@MARKER`), unlike Kraken-Futures/Bybit above, which keep the quote for dated derivatives too —
matches the doc's own literal Deribit examples, not a new decision.

**Shipped**: `deribit_options_adapter.py` (the live, real-time options-chain adapter — used for the VOL\_\* mark-IV
family, independent of the Tardis-batch/CCXT-live universe-enumeration paths) now builds this format directly,
resolving margin type from Deribit's own `quote_currency` field (`USD`=inverse, `USDC`=linear; falls back to
`settlement_currency`). `tardis/parsing.py` gained `_build_canonical_option_key` — the OPTION analog of the shared
Bybit/Kraken-Futures/OKX `_build_canonical_future_key`/`_build_canonical_perpetual_key` builder set above (no prior
venue in this migration lists options), unit-tested (`tests/unit/test_deribit_canonical_id.py`).

**NOT yet wired into `tardis/adapter.py`'s Tardis-batch instrument_key construction or `ccxt_adapter.py`'s live-mode
mirror** — same deferral as the Bybit/Kraken-Futures/OKX fixes above (a concurrent sibling edit was in flight on
`adapter.py` this session); both paths must move together (they're deliberately kept identical today — Deribit's raw
`DDMMMYY` passthrough — for the `paper(W) == batch-rerun(W)` live/batch determinism invariant
`canonical_id_p0_ccxt_live_batch_divergence_2026_07_08.md` protects), so this is a coordinated follow-up, not
independently shippable per-file.

**Real GCS scoping (2026-07-09, `prod/catalog.parquet`, `instruments-store-cefi-prd-central-element-323112`, 4.4
MiB)**: 263,979 real DERIBIT PERPETUAL/FUTURE/OPTION rows (26 PERPETUAL + 1,507 FUTURE + 262,446 OPTION) — the target
transform (re-derive from already-captured `instrument_type`/`base_asset`/`margin_type` + the `DDMMMYY` embedded in
`raw_symbol`/`instrument_id` itself, no re-download) is **100% re-derivable, 0 collisions** against every one of these
263,979 real rows. A real, bounded, backed-up smoke test (28 rows — every real PERPETUAL row plus a FUTURE/OPTION
sample spanning both margin types, `scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply --sample-size 30`)
ran directly against the live catalog 2026-07-09 — backup
`prod/catalog.deribit-marker-migration.20260709-092610.bak.parquet`, all 28 corrections verified correct by
re-downloading the written blob, 0 unrelated rows touched (352,132 total rows unchanged). Real before/after examples:
`DERIBIT:PERPETUAL:BTC-USD` → `DERIBIT:PERPETUAL:BTC-USD@INV`; `DERIBIT:FUTURE:AVAX_USDC-10APR26` →
`DERIBIT:FUTURE:AVAX@LIN-20260410`; `DERIBIT:OPTION:BTC-10APR20-4750-C` → `DERIBIT:OPTION:BTC@INV-20200410-4750-C`.
Measured: the local re-derivation of all 263,979 rows takes ~8-14s (negligible, string parsing only); the dominant
cost is the fixed single-file download+backup+write+verify round trip against `prod/catalog.parquet` (~23s observed,
independent of sample size, same shape as the OKX smoke test above) — so migrating the **remaining ~263,951
catalog.parquet rows is NOT a long-running job**, it is the same ~20-25s single-file rewrite as this smoke test.

**`prod/catalog.parquet` full sweep — ACTUALLY RAN 2026-07-09** (real production data, not a smoke test): all
263,951 remaining real DERIBIT PERPETUAL/FUTURE/OPTION rows migrated
(`scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply --full-sweep`), 352,132 total rows unchanged, 0
duplicate `instrument_id` introduced. Backup: `prod/catalog.deribit-marker-migration.20260709-112920.bak.parquet`.
Verified by re-downloading the written blob: all 263,979 real target rows (100%) now carry the `@LIN`/`@INV` marker,
0 remaining. Real elapsed time: ~17s end-to-end (the initial full-sweep attempt hung on a per-row `DataFrame.loc[idx,
col] = val` assignment loop over 264K rows — O(n²)-ish pandas block-consistency overhead — fixed to a single
vectorized indexed assignment before the real run below).

**The much larger surface is the per-day historical snapshot corpus** —
`instrument_availability/by_date/day=*/venue=DERIBIT/instruments.parquet` (both the legacy and
`pipeline_mode=batch_instruments_service`-prefixed path shapes) — **5,342 real files**, 2019-03-30 to present. Each
per-day row's `instrument_key` carries the exact same raw `VENUE:TYPE:BASE[-QUOTE|_QUOTE][-DDMMMYY[-STRIKE-C|P]]`
shape as `catalog.parquet`'s `instrument_id` (verified against real 2019 and 2026 samples, incl. USDC-linear
futures/options) — the catalog transform (`build_target_instrument_id`) is reused unchanged, no separate
per-day-column re-derivation needed. **Full sweep — ACTUALLY RAN 2026-07-09**
(`scripts/canonicalize_deribit_id_markers_2026_07_09.py --by-date-all --apply --workers 32`, real 32-way
`ThreadPoolExecutor` concurrency, one file = one isolated shard): see "Deribit per-day snapshot corpus migration"
below for the real before/after counts and measured throughput. `prod/catalog.parquet` is itself a
derived/regenerated rollup of this per-day corpus (`scripts/build_instrument_catalogue.py`) — migrating both surfaces
in the same pass means a future catalogue regeneration will NOT silently revert the marker back to the old format.

**Live-vs-batch classification bug — FIXED 2026-07-09 (cross-repo)**:
`market-tick-data-service/market_tick_data_service/live/connectors/deribit_ws.py:100`
(`DeribitWSFeedConnector`) classified a live trade's instrument_type via
`"PERPETUAL" if "PERPETUAL" in inst_obj else "FUTURE" if inst_obj.count("-") == 2 else "OPTION"` — the `count("-") ==
2` branch was dead code: real Deribit FUTURE ids are 1 dash (`BTC-10JUL26`, `AVAX_USDC-10JUL26` — the quote is
underscore-joined, not dash-joined) and real OPTION ids are 3 dashes (`BTC-10JUL26-48000-C`), so no real symbol ever
had exactly 2 dashes. Every real live FUTURE trade was misclassified as `OPTION`. Fixed to
`"OPTION" if inst_obj.count("-") == 3 else "FUTURE"` (check OPTION's real 3-dash shape explicitly; FUTURE is the only
remaining real shape once PERPETUAL and OPTION are excluded) — real regression test added
(`test_real_future_instrument_one_dash_classified_as_future`, `market-tick-data-service/tests/unit/
test_deribit_ws_connector.py`) asserting a real 1-dash FUTURE id (`BTC-27SEP19`) now classifies as `FUTURE`, not
`OPTION`.

### BINANCE-FUTURES / BINANCE-DELIVERY `@LIN`/`@INV` migration (2026-07-09)

Target: `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN`, `BINANCE-FUTURES:FUTURE:BTC-USDT@LIN-20260925`,
`BINANCE-DELIVERY:PERPETUAL:BTC-USD@INV`, `BINANCE-DELIVERY:FUTURE:BTC-USD@INV-20200925` — same worked examples as the
doc's own target-format section above. Unlike Bybit/Kraken-Futures/OKX, **margin type needed no bug fix here** — these
are two separate Tardis exchange ids (`binance-futures` = linear, `binance-delivery` = inverse), already
unambiguous, so `_infer_margin_type` was already 100% correct for both venues (verified against all 1,107 real
`prod/catalog.parquet` rows below — 0 mislabeled).

**Shared UAC builder — additive `margin_marker` kwarg** (`unified-api-contracts`,
`unified_api_contracts/internal/reference/canonical_id_builder.py`): `build_instrument_id(...,
margin_marker="LIN"|"INV")` embeds the marker directly on `PERPETUAL`/`FUTURE`/`OPTION`, independent of and
byte-identical in output to the Bybit/Kraken/Deribit siblings' `passthrough=True` + pre-formatted-symbol convention
above — a second, converging implementation of the same operator decision, not a conflict (kept, since it's a
strictly additive change with its own tests; existing `quote_asset`/`margin_type` legacy-word-form callers are
byte-for-byte unchanged). Unit-tested (`tests/internal/unit/test_canonical_id_builder.py::TestMarginMarker`).

**Real GCS migration — actually RAN, not just smoke-tested** (`prod/catalog.parquet`,
`instruments-store-cefi-prd-central-element-323112`, 352,132 total rows, 2026-07-09): 1,107 real BINANCE-FUTURES/
BINANCE-DELIVERY rows (BINANCE-FUTURES 837 PERPETUAL + 48 FUTURE, 100% `linear`; BINANCE-DELIVERY 45 PERPETUAL + 177
FUTURE, 100% `inverse`) — the full 1,107-row catalog migration **was applied**, not deferred: backup
`prod/catalog.20260709-091147.binancefix.bak.parquet`, all 1,107 rows verified correct by re-downloading the written
blob (0 rows missing the `@` marker, 0 duplicate `instrument_id` introduced, 352,132 total rows unchanged). Real
before/after: `BINANCE-FUTURES:PERPETUAL:BTC-USDT` → `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN`;
`BINANCE-DELIVERY:FUTURE:LTCUSD_231229` → `BINANCE-DELIVERY:FUTURE:LTC-USD@INV-20231229`.

**Same derived-rollup caveat as Deribit's section above applies** — `prod/catalog.parquet` is regenerated from
`instrument_availability/by_date/day=*/[.../]venue=BINANCE-{FUTURES,DELIVERY}/instruments.parquet` per-day snapshots
(`scripts/build_instrument_catalogue.py`); a future catalogue regeneration would silently revert the catalog fix
above unless the per-day source rows are ALSO migrated. Real full-corpus scan (2026-07-09): **9,234 real per-day
files** (4,878 BINANCE-FUTURES + 4,356 BINANCE-DELIVERY, since 2019-11-17 — both the legacy and
`pipeline_mode=batch_instruments_service`-prefixed path shapes). Unlike the catalog table, each snapshot row already
carries clean `base_asset`/`quote_asset`/`margin_type`/`expiry` columns (no string re-parsing needed). A real,
bounded, backed-up smoke test (10 real files, `scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py
--by-date-sample 10 --apply --confirm`) ran directly against the live per-day corpus — all 10 corrections verified
correct by re-downloading the written blob against its backup. Measured real apply throughput (download + backup
upload + rewritten upload, serial, single Python storage client): 2.259s/file → full 9,234-file sweep ETA ≈ 5.8
hours serial (not measured with concurrency at smoke-test time).

**Full by_date sweep — ACTUALLY RAN 2026-07-09, real 16-way concurrency** (real production data, not a smoke test):
`scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py --by-date-all --apply --confirm --workers 16`
(thread-pool `ThreadPoolExecutor`, one file = one isolated shard — a per-file exception is caught + logged, never
aborts the sweep) ran against all 9,234 real target files. Real results: **files=9,234, files_changed=9,224 (the
other 10 were already migrated by the earlier smoke test — correctly reported 0 changes, proving idempotency),
rows_changed=1,343,238, errors=0**. Real elapsed time: 894.8s (14.9 min) for the write phase (0.097s/file avg — a
~23x speedup over the 2.259s/file serial baseline) + ~147s for the initial full-corpus blob listing = **~17.4 minutes
end-to-end**, matching the doc's own "~15-25 minutes with 8-16x concurrency" estimate. Each file got its own
timestamped `.binancefix.bak.parquet` backup before being overwritten (per-shard backup-then-write, not one
whole-corpus backup). Spot-checked post-write (5 real files spanning 2019-11-17 through 2026-07-08, both venues):
100% of PERPETUAL/FUTURE rows carry the correct `@LIN`/`@INV` marker matching each venue's real margin type. **Full
re-verification**: a second full-corpus dry-run pass (`--by-date-all --workers 16`, no `--apply`, real re-download +
re-check of every one of the 9,234 files) reported `files_changed=0, rows_changed=0, errors=0` — **0 remaining
legacy-shape rows across the entire real corpus**, confirming the migration is complete and idempotent, not just
smoke-tested. `prod/catalog.parquet`'s fix above is now durable — a future catalogue regeneration will re-derive from
already-migrated per-day source rows, not revert it.

**Real bug found + fixed during this sweep**: `_list_by_date_targets`'s original substring match
(`"venue=BINANCE-FUTURES/" in blob_name`) also matched this script's own `.binancefix.bak.parquet` backup blobs
(e.g. `venue=BINANCE-FUTURES/instruments.<ts>.binancefix.bak.parquet`), which would have caused a full sweep to
re-migrate + re-backup its own backups — corrupting the backup chain and inflating file/row counts run over run.
Caught by a pre-full-sweep validation pass (a 20-file concurrent apply test picked up 10 pre-existing backup blobs
from the earlier smoke test instead of the real primary files) before the real 9,234-file sweep launched. Fixed to
require the blob name end exactly `/instruments.parquet`. The 10 backup blobs mutated during validation were restored
to their true pre-fix byte content (recovered from the backup-of-backup `_write_blob` created before the accidental
overwrite, verified byte-for-byte identical after restore) before the real sweep ran — 0 real production data lost,
0 primary `instruments.parquet` files were ever touched by the bug (only the safety-backup layer was briefly
affected).

**NOT wired into `adapter.py`'s live `instrument_key` construction** — same deferral as every other venue in this
section (a concurrent sibling edit was in flight on `adapter.py` this session; the wiring is a shared, venue-agnostic
change to `_parse_tardis_instrument`'s `symbol`/`instrument_key` construction lines that applies uniformly across
every CeFi Tardis venue, not something to half-apply per-venue).

**Both the UAC `margin_marker` addition and the new migration script are currently UNCOMMITTED** — implemented,
unit-tested, and (for the migration script) already proven correct against real production data, but blocked from
`quickmerge.sh` by pre-existing, unrelated quality-gate regressions independently discovered while shipping this fix:
`unified-api-contracts` fails the `MAX_FILE_LINES=900` check on 4 files this change never touched
([[uac_qg_900line_regression_blocking_pushes_2026_07_09]]), and `instruments-service` fails STEP 5.101's
empty-string-fallback ratchet (377 live vs. 369 baseline, same pre-existing class already tracked in
[[mtds_empty_string_fallback_codex_gate_blocking_pushes_2026_07_08]]). Both issue docs filed/updated 2026-07-09.

### BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration + margin_type backfill (2026-07-09)

Closes the deferred backfill flagged in "Instrument ID format" above and the two BYBIT/KRAKEN-FUTURES "Known
limitations" rows this doc previously carried for the 2026-07-09 `_infer_margin_type` fix (`176d4610`). Target:
`BYBIT:PERPETUAL:BTC-USD@INV`, `BYBIT:FUTURE:BTC-USD@INV-20231201`, `KRAKEN-FUTURES:PERPETUAL:BTC-USD@INV`,
`KRAKEN-FUTURES:FUTURE:ETH-USD@LIN-20230728` — same worked examples as the doc's own target-format section above.

**New script** `scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py` — unlike the Binance/Deribit sibling
scripts, this one imports the real `tardis/parsing.py` helpers directly (`_split_bybit_symbol`,
`_split_kraken_symbol`, `_infer_margin_type`, `_build_canonical_perpetual_key`, `_build_canonical_future_key`) rather
than duplicating quote-currency logic — `parsing.py` had no concurrent sibling edit in flight this session (unlike
`adapter.py`), so importing it directly is safe and keeps the change surface minimal. One transform re-derives BOTH
the corrected `margin_type` value AND the target canonical id from each row's own `raw_symbol` — never a
margin-type-only or id-only half-fix.

**Real GCS migration — both `prod/catalog.parquet` and the full per-day corpus, both ACTUALLY RAN** (bucket
`instruments-store-cefi-prd-central-element-323112`, 2026-07-09):

| Surface                                                                               | Real scope                                                                           | Real result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prod/catalog.parquet`                                                                | 1,543 BYBIT + 1,103 KRAKEN-FUTURES = 2,646 rows (of 352,132 total)                   | **2,646/2,646 (100%) now carry the `@LIN`/`@INV` marker; 661/661 wrong `margin_type` values corrected (279 BYBIT: 255 FUTURE + 24 PERPETUAL; 382 KRAKEN-FUTURES: 378 FUTURE + 4 PERPETUAL) — exact match to the `176d4610` audit counts.** 0 unresolved rows, 0 new duplicate `instrument_id`, 352,132 total rows unchanged. Backup: `prod/catalog.20260709-120452.bybitkrakenfix.bak.parquet`.                                                                                                                        |
| `instrument_availability/by_date/**/venue={BYBIT,KRAKEN-FUTURES}/instruments.parquet` | 9,540 real per-day files (4,788 BYBIT + 4,752 KRAKEN-FUTURES), 2020-01-01 to present | **9,540/9,540 (100%) migrated**: 1,978,228 real row-level id relabels, 150,948 real `margin_type` corrections, 0 unresolved. Real 20-way `ThreadPoolExecutor` concurrency, 983.6s (16.4 min) elapsed, 0.103s/file. Re-verified by a full-corpus dry-run re-scan immediately after (`--by-date-all`, no `--apply`): `files_changed=0, id_changed=0, margin_fixed=0` across all 9,540 files — confirmed complete and idempotent, not just smoke-tested. Per-file backups: `instruments.<ts>.bybitkrakenfix.bak.parquet`. |

**Real durability bug hit + resolved during this migration**: the first `prod/catalog.parquet` apply run
(`--catalog --apply --confirm`) logged a successful write, but re-downloading it minutes later showed the OLD,
unmigrated content — re-serialized through `pandas`/`pyarrow` (byte-identical to a fresh no-op re-serialization of
the pre-fix data) but with generation/update-time metadata consistent with MY OWN write, not a visibly later
overwrite. Root cause: `prod/catalog.parquet` is a derived rollup regenerated from the by_date corpus
(`scripts/build_instrument_catalogue.py`) — a scheduled/concurrent catalogue rebuild fired in the same window and
re-derived the still-unmigrated by_date rows, silently reverting the catalog-only fix (same class of issue already
documented for the Deribit/finding-7 migration earlier this session, 2026-07-09 01:03 UTC). **Resolution**: migrated
the by_date corpus FIRST (the durable source), then re-applied the catalog fix LAST — re-verified stable (unchanged
generation, correct content) for 11+ minutes after re-apply with no revert, confirming durability now that any future
catalogue regeneration re-derives from already-correct source rows.

**Real bug found + fixed in this script's own `_list_by_date_targets`, same class already caught in the sibling
Binance migration script**: the original substring match (`"venue=BYBIT/" in blob_name`) also matched this script's
own `.bybitkrakenfix.bak.parquet` backup blobs from an earlier 20-file smoke test, so the first full sweep
incidentally re-processed + re-backed-up 20 backup blobs (harmless — backups only, 0 primary `instruments.parquet`
files affected, the true original content remains recoverable via the backup-of-backup chain) alongside the real
9,540 targets. Fixed to require the blob name end exactly `/instruments.parquet` (matching the sibling script's own
fix for the identical class of bug); the subsequent dry-run re-verification pass (`files=9,540` exactly, no stray
backups) confirms the fix is correct.

**Real Bybit legacy coin-margined quarterly recapture (the 46 `BTCUSDH22`-shaped symbols) — ATTEMPTED, BLOCKED by a
newly-discovered, real, separate adapter.py bug — NOT the catalog/by_date backfill above.** Ran the normal capture
path (`python -m instruments_service --operation instruments --mode batch --asset-group cefi --venues BYBIT
--start-date 2026-07-09 --end-date 2026-07-09 --force`) exactly as this migration's todo specifies. Result: **the
entire BYBIT venue fetch fails with 0 records** — `pydantic.ValidationError: InstrumentRecord(instrument_type=FUTURE)
requires non-null expiry`. Root-caused (read-only diagnosis, no `adapter.py` edit — see below): real, live Tardis
data (`GET https://api.tardis.dev/v1/exchanges/bybit`, no-auth, 2026-07-09) confirms all 46 real
`BTCUSDH22`-shaped symbols exist and are `type: future`; **4 of the 46 are currently still-active contracts with no
`availableTo` field** (`BTCUSDU26`, `BTCUSDZ26`, `ETHUSDU26`, `ETHUSDZ26`). `adapter.py::_parse_tardis_instrument`'s
expiry-resolution fallback chain (lines ~736-747) tries, in order: `item.expiry` (absent for this shape — Tardis's
free no-auth endpoint carries no `expiry` field at all) → `available_to` (only populated for delisted/expired
symbols — these 4 are still trading, so this is `None` too) → a dash-based Deribit/OKX-style parser (`raw_id` has no
dash) → an underscore-based Kraken-Futures-style parser (`raw_id` has no underscore either). **No branch handles
Bybit's no-dash CME-style month-code shape at all** — `expiry` resolves to `None`, and `InstrumentRecord(...)`
construction raises uncaught inside `_fetch_exchange_instruments`'s bare per-item loop (`adapter.py:657`, no
try/except around the per-item construction call — unlike the quote-empty guard a few lines earlier in the same
function, which correctly `return None`-skips instead of raising). One bad row kills the WHOLE venue fetch (shard =
venue for CeFi Tardis, so this is correctly-isolated venue-level failure classification working as designed — but
the shard itself is now permanently red for ALL 1,543+ real BYBIT instruments, not just the 4 triggering rows, until
fixed). The other 42 of 46 delisted/historical symbols DO have a real `availableTo` and would likely resolve via that
existing fallback (not independently verified row-by-row, since the venue-level abort happens before any of them are
reached in practice). **This is a real regression exposed BY `176d4610`'s own fix** — before it, `_split_bybit_symbol`
didn't exist, so these 46 symbols' quote resolution failed and they were silently dropped by the (existing,
correctly-guarded) empty-quote skip; now that quote resolves correctly, they proceed further and hit this separate,
previously-latent expiry gap instead. **Not fixed in this pass** — the fix belongs in `adapter.py`
(`instruments_service/reference_data/adapters/cefi/tardis/adapter.py`), which this pass's dispatch explicitly
excluded from editing (a separate, actively-iterating concurrent workflow owns that file this session — 3 more
commits landed on it during this pass alone). Real before/after for this item: **0/46 before, 0/46 after** (fully
diagnosed and blocked, not silently skipped) — see "Known limitations" below. Minimal fix (not applied): add one more
fallback branch mirroring the Kraken-Futures pattern, resolving expiry from the raw month-code + year (needs a real
quarterly-expiry-day convention — e.g. Bybit's real last-Friday-of-contract-month rule — not a trivial string parse,
which is why this is scoped as its own follow-up fix rather than folded into this pass).

## Known limitations

Full audit trail: [canonical instrument_id audit](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).

| Limitation                                                                                                                                                                                                                                  | Detail                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`CEFI_EQUITY_PERP_BASE_UNIVERSE` ticker reuse**                                                                                                                                                                                           | Six tickers (`CFG`/`DIA`/`INX`/`ROBO`/`SLX`/`SPX`) are kept pending a dedicated cross-venue audit — see "Equity/commodity-basis perp universe" above.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **Kraken-Futures dated-future `instrument_id` — margin-marker format not yet migrated**                                                                                                                                                     | Per-(ticker, expiry) distinctness is correctly resolved (`_KRAKEN_FUTURES_RE` in `market-tick-data-service/.../adapters/cefi/tardis_shared.py` extracts the real ticker from the `{TYPE_PREFIX}_{PAIR}_{DATE}` raw symbol shape), but the format is still the v6 word-form margin marker (`KRAKEN-FUTURES:FUTURE:XBT-USD-inverse-20260731`), not yet the operator-decided `@INV-YYYYMMDD` target — see "Instrument ID format" above.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| **Kraken-Futures `FI_` vs `FF_` same-(ticker, expiry) ambiguity**                                                                                                                                                                           | 13 real (ticker, expiry) combinations (`ETH`/`XBT`, 2024-2026 range) have both a real `FI_` and a real `FF_` raw Tardis symbol with different row counts (e.g. `FI_ETHUSD_240329` vs `FF_ETHUSD_240329`) that collapse onto the same `instrument_id` today — `derive_row_instrument_id`'s FUTURE branch (`tardis_shared.py`) has no field to encode the `FI`/`FF` contract-subtype. The adjacent code comment describing `FI*` as pre-2020/"no longer active" is contradicted by real 2024-2026 capture data (a data-state claim, not independently re-verified here). Needs an operator decision on what `FI*` vs `FF*` represents for KRAKEN-FUTURES before it can be encoded in the canonical instrument_id.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **`prod/catalog.parquet` + per-day `margin_type`/instrument_id for BYBIT/KRAKEN-FUTURES — FIXED 2026-07-09 (real full sweep, both surfaces, not just smoke-tested)**                                                                        | Both the catalog (2,646/2,646 rows) and the full 9,540-file per-day corpus now carry the corrected `margin_type` (661 + 150,948 real corrections respectively) and the `@LIN`/`@INV`-`YYYYMMDD` canonical id. See "BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration" above for the real before/after counts, the durability bug hit + resolved (catalog-only fix was silently reverted by a concurrent catalogue rebuild until the by_date source was also migrated), and measured throughput.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **46 real Bybit legacy inverse quarterly futures (`BTCUSDH22`-shaped) — STILL entirely un-captured; 0/46, blocked by a newly-discovered real `adapter.py` bug (separate from the margin/id fix above)**                                     | Real, live-verified against `api.tardis.dev/v1/exchanges/bybit` (BTC/ETH only, 46 symbols) — `_split_bybit_symbol` (2026-07-09) now resolves base/quote/margin correctly for all 46, but a real live re-capture attempt (2026-07-09, this pass) found the entire BYBIT venue fetch now fails with 0 records: `adapter.py`'s expiry-resolution chain has no fallback for this no-dash CME-style shape, so the 4 currently-still-active symbols (`BTCUSDU26`, `BTCUSDZ26`, `ETHUSDU26`, `ETHUSDZ26` — no `item.expiry`, no `availableTo` since they're not yet delisted) resolve `expiry=None`, and the uncaught `InstrumentRecord` `ValidationError` aborts the WHOLE venue (not just these 4 rows) — a real regression exposed BY the `176d4610` fix itself (previously silently dropped by the now-fixed empty-quote guard before ever reaching this deeper validation). See "BYBIT / KRAKEN-FUTURES `@LIN`/`@INV` migration" above for full root-cause diagnosis. Not fixed in this pass — the fix belongs in `adapter.py`, explicitly out of scope this session (separate, actively-iterating concurrent workflow owns that file). |
| **`prod/catalog.parquet` OKX-SWAP/OKX-FUTURES `margin_type` — FIXED 2026-07-09 (real full sweep, not just smoke-tested)**                                                                                                                   | The full sweep (`scripts/canonicalize_okx_margin_type_2026_07_09.py --apply --full-sweep`) ran — all 2,753 real remaining mismatched rows (2,701 OKX-FUTURES + 52 OKX-SWAP) now carry the corrected `margin_type`, 0 remaining across all 6,053 real rows, verified by re-download. Backup: `prod/catalog.okxmarginfix-fullsweep.20260709-113203.bak.parquet` (the earlier 21-row smoke-test backup `…-091832.bak.parquet` is superseded).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| **`instrument_availability/by_date/` OKX-SWAP/OKX-FUTURES per-day snapshot corpus — PARTIALLY fixed 2026-07-09 (4,762/9,576 real files, 49.7%); the remaining 4,814 (shapes B+C+D) FIXED by a follow-up legacy-GCS-naming audit, same day** | The original `--by-day --apply --by-day-full-sweep --workers 40` pass only ever discovered shape-A (`day=D/venue=V/`) files via a two-level delimited listing that cannot see nested path shapes — see "CeFi legacy GCS path-shape audit (2026-07-09)" below for the full real finding (4 coexisting real path shapes, only 1 covered) and the real fix (`scripts/legacy_naming_audit_okx_2026_07_09.py`, all 9,576 real files now covered and correct). `prod/catalog.parquet`'s fix is durable now that ALL real per-day source-of-truth files are corrected, not just the shape the original script's listing happened to see.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **OKX-FUTURES has no live WS connector** (venue-key fix, `market-tick-data-service/live/connectors/okx_ws.py`)                                                                                                                              | This connector only ever parses `-SWAP` (perpetual) trade frames — real dated-futures instIds (`BTC-USD-260710`) were never handled by it. It was previously registered under the wrong key (`OKX-FUTURES`), which both mislabeled every real trade as `OKX-FUTURES:PERP:...` AND silently occupied the venue key a real dated-futures connector would need. Fixed 2026-07-09: now registered under `OKX-SWAP` (correct); `OKX-FUTURES` is intentionally left unregistered — a real dated-futures live connector does not exist yet (separate, unscoped future work).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **`prod/catalog.parquet` DERIBIT instrument_id — FIXED 2026-07-09 (real full sweep, not just smoke-tested)**                                                                                                                                | The Deribit `@LIN`/`@INV` migration (see "Deribit `@LIN`/`@INV` migration" above) ran its real full sweep — all 263,979 real target rows now carry the marker, 0 remaining, 352,132 total rows unchanged, verified by re-download. Backup: `prod/catalog.deribit-marker-migration.20260709-112920.bak.parquet` (full-sweep backup; the earlier 28-row smoke-test backup `…-092610.bak.parquet` is superseded).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **`instrument_availability/by_date/` DERIBIT per-day snapshot corpus — FIXED 2026-07-09 (real full sweep, concurrent)**                                                                                                                     | All 5,342 real per-day files migrated (`--by-date-all --apply --workers 32`) — see "Deribit per-day snapshot corpus migration" above for the real before/after counts, per-file backups, and measured throughput. `prod/catalog.parquet`'s fix above is now durable — a future catalogue regeneration will re-derive from already-migrated per-day source rows, not revert it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **DERIBIT Tardis-batch/CCXT-live instrument_key construction not yet wired to `@LIN`/`@INV`**                                                                                                                                               | `tardis/adapter.py`'s `_parse_tardis_instrument` and `ccxt_adapter.py`'s `_build_instrument_key` both still emit the raw `DDMMMYY`-passthrough format for DERIBIT FUTURE/OPTION and unmarked `BASE-QUOTE` for PERPETUAL. `tardis/parsing.py`'s `_build_canonical_perpetual_key`/`_build_canonical_future_key`/`_build_canonical_option_key` builder set is complete and unit-tested (`tests/unit/test_deribit_canonical_id.py`, 12/12 passing) and ready to be called from `adapter.py` — re-checked 2026-07-09, `adapter.py` is STILL dirty/uncommitted from a concurrent sibling edit this session, so the call-site wiring remains correctly deferred (same reasoning as the Bybit/Kraken-Futures/OKX entries above). The two paths are deliberately kept identical for the `paper(W) == batch-rerun(W)` live/batch determinism invariant, so they must be migrated together, not independently, once `adapter.py` frees up — see "Deribit `@LIN`/`@INV` migration" above.                                                                                                                                                         |
| **`deribit_ws.py` (MTDS) live-classification bug — FIXED 2026-07-09 (cross-repo)**                                                                                                                                                          | `market-tick-data-service/market_tick_data_service/live/connectors/deribit_ws.py:100` had a dead `inst_obj.count("-") == 2` branch — real Deribit FUTURE ids are 1 dash, real OPTION ids are 3 dashes, so no real symbol ever hit the 2-dash case; every real live FUTURE trade was misclassified as OPTION. Fixed to `"OPTION" if count == 3 else "FUTURE"`; real regression test added (`test_real_future_instrument_one_dash_classified_as_future`) asserting a real 1-dash id (`BTC-27SEP19`) now classifies as `FUTURE`. 34/34 tests passing in `tests/unit/test_deribit_ws_connector.py`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **BINANCE-FUTURES/BINANCE-DELIVERY `adapter.py` instrument_key construction not yet wired to `@LIN`/`@INV`**                                                                                                                                | Same deferral as the Bybit/Kraken-Futures/OKX/Deribit entries above (`adapter.py` had a concurrent sibling edit in flight this session) — see "BINANCE-FUTURES / BINANCE-DELIVERY `@LIN`/`@INV` migration" above.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **`instrument_availability/by_date/` per-day snapshot corpus for BINANCE-FUTURES/BINANCE-DELIVERY — FIXED 2026-07-09 (real full sweep, concurrent)**                                                                                        | All 9,234 real per-day files migrated (`--by-date-all --apply --confirm --workers 16`, real 16-way `ThreadPoolExecutor` concurrency): files_changed=9,224, rows_changed=1,343,238, errors=0, real elapsed 894.8s (14.9 min) write phase + ~147s corpus listing ≈ 17.4 min end-to-end (0.097s/file avg, ~23x the 2.259s/file serial baseline). Re-verified by a full-corpus dry-run re-scan: `files_changed=0, rows_changed=0` across all 9,234 files — 0 remaining legacy-shape rows. See "BINANCE-FUTURES / BINANCE-DELIVERY `@LIN`/`@INV` migration" above for the real bug found + fixed in `_list_by_date_targets` during pre-sweep validation. `prod/catalog.parquet`'s fix above is now durable.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| **UAC `canonical_id_builder.py` `margin_marker` addition + the new `scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py` are uncommitted (pre-existing, unrelated QG blockers)**                                            | See "BINANCE-FUTURES / BINANCE-DELIVERY `@LIN`/`@INV` migration" above and the 2 linked issue docs — `unified-api-contracts` and `instruments-service` are both independently red for pre-existing reasons unrelated to this change, so quickmerge is blocked for both files until those clear.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

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
