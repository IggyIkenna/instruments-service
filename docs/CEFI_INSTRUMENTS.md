# CeFi Instruments

> **Cross-links**: [instruments-definitions drilldown mockup — CeFi tab](https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d)
> (real per-venue instrument_id samples, live-verified 2026-07-08) ·
> [`instrument_id_format_canonicalization_2026_07_08.md`](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> (the decided target canonical format for dated derivatives, not shipped yet) ·
> [`canonical_instrument_id_audit_2026_07_08.md`](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md)
> (the full cross-repo compliance audit this doc's "known bugs" section summarizes) ·
> [`ADAPTER_ARCHITECTURE.md`](../ADAPTER_ARCHITECTURE.md) (adapter code structure, `canonical_id_builder.py` caveats) ·
> [`DEFI_INSTRUMENTS.md`](./DEFI_INSTRUMENTS.md) (on-chain/DeFi perp venues — Hyperliquid, Aster, and the rest of the
> on-chain CLOB cluster live there, not here — see "Scope" below).

## Scope

This doc covers **genuinely centralized exchanges**: Binance, Bybit, OKX, Deribit, Coinbase, Upbit, Kraken /
Kraken-Futures, Bitfinex, Bitget, HTX/Huobi, Bitstamp.

**Not covered here** (documented in `DEFI_INSTRUMENTS.md` instead): the on-chain perp CLOBs — Hyperliquid, Aster,
Pacifica (Solana), Extended (Starknet), Lighter (zkSync) — and the prediction-platform crypto-perp CLOBs — Kalshi-Perp,
Polymarket-Perp (documented in `PREDICTION_INSTRUMENTS.md`, distinct from those venues' actual YES/NO prediction
markets). **Important nuance**: at the data-pipeline/UAC level, all 7 of those venues are technically registered inside
the same `"cefi"` asset-group bucket as the exchanges in this doc —
`unified_api_contracts/registry/market_data_categories.py:226-274` (`VENUES_BY_ASSET_GROUP["cefi"]`) lists HYPERLIQUID,
ASTER, PACIFICA-SOLANA, EXTENDED-STARKNET, LIGHTER-ZKSYNC, KALSHI-PERP, and POLYMARKET-PERP alongside BINANCE-SPOT,
DERIBIT, etc. — because they're CLOB-style order-book data like a real exchange, not AMM-pool data. This doc's
narrower "centralized exchange" scope is a docs-organization choice for the 7-doc consolidation, not a different
technical asset-group.

## Venues

Two independent modes exist per venue family, and — per a real, still-open bug (see "Known bugs" below) — they
currently produce **structurally different `instrument_id` values for the same real instrument**:

- **Batch mode** — Tardis (historical data). `unified_api_contracts.registry.venue_mapping.VenueMapping.tardis_to_venue`
  is the reverse Tardis-exchange-slug → canonical-venue mapping.
- **Live mode** — CCXT (real-time, public endpoints, no API key needed for instrument discovery) for a 13-canonical-venue
  subset. `instruments_service/reference_data/factory.py:95-114` (`_CANONICAL_VENUE_TO_CCXT_EXCHANGE`).

| Canonical venue                         | Batch (Tardis slug)                   | Live (CCXT id)  | Instrument types                                                                                                                                                 |
| --------------------------------------- | ------------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BINANCE-SPOT`                          | `binance`                             | `binance`       | Spot pairs                                                                                                                                                       |
| `BINANCE-FUTURES`                       | `binance-futures`                     | `binanceusdm`   | Perpetuals + dated futures, USDT-margined (linear)                                                                                                               |
| `BINANCE-DELIVERY`                      | (via binance-futures data)            | —               | COIN-M perps + dated futures, coin-margined (inverse); distinct endpoint from `BINANCE-FUTURES` (2026-06-24)                                                     |
| `BYBIT`                                 | `bybit`                               | `bybit`         | Perpetuals + dated futures + options                                                                                                                             |
| `BYBIT-SPOT`                            | `bybit-spot`                          | `bybit`         | Spot pairs (distinct canonical venue since 2026-06-23 so the perp-gate pairs BYBIT-SPOT↔BYBIT)                                                                   |
| `OKX-SPOT` / `OKX-SWAP` / `OKX-FUTURES` | `okex` / `okex-swap` / `okex-futures` | `okx` (unified) | Spot / perpetuals ("swap") / dated futures — bare `OKX` has no instruments itself; the real data lives on these 3 suffixed venues                                |
| `DERIBIT`                               | `deribit`                             | `deribit`       | **Both** linear (USDC-settled) and inverse (USD-quoted, BTC/ETH-settled) options + futures + perps + spot — see "Deribit margin types" below                     |
| `DERIBIT-COMBO`                         | `deribit` (own adapter)               | —               | Multi-leg combo/spread instruments (33 structure codes — vertical/calendar/butterfly/condor/box/jelly-roll); own manifest shard, own `deribit_combo` adapter key |
| `COINBASE-SPOT`                         | `coinbase`                            | `coinbase`      | Spot pairs (USD quote — for coinbase premium)                                                                                                                    |
| `COINBASE-FUTURES`                      | `coinbase-international`              | —               | Coinbase Derivatives (perps); distinct canonical venue since 2026-06-23                                                                                          |
| `UPBIT`                                 | `upbit`                               | `upbit`         | Spot pairs, KRW/BTC/USDT quotes (QUOTE-BASE format, inverted by the adapter — see below)                                                                         |
| `KRAKEN-SPOT`                           | `kraken`                              | `kraken`        | Spot pairs                                                                                                                                                       |
| `KRAKEN-FUTURES`                        | `cryptofacilities`                    | `krakenfutures` | Perpetuals + dated futures                                                                                                                                       |
| `BITFINEX-SPOT`                         | `bitfinex`                            | —               | Spot pairs (Tardis Tier-3, added 2026-05-01)                                                                                                                     |
| `BITFINEX-FUTURES`                      | `bitfinex-derivatives`                | —               | Perpetuals (linear USDT-margined **and** inverse BTC-margined — see the open Bitfinex bug below)                                                                 |
| `BITGET-SPOT` / `BITGET-FUTURES`        | `bitget` / `bitget-futures`           | —               | Spot / perpetuals (Tardis Tier-3, added 2026-05-01)                                                                                                              |

**Removed** (2026-07-08, operator-confirmed): `bitstamp`, `huobi`, `huobi-dm` — previously declared in
`VenueMapping.all_tardis_exchanges` (`unified_api_contracts/registry/venue_mapping.py`, plus matching
`venue_to_ccxt`/`tardis_to_venue`/`tardis_exchange_instrument_types` entries) but never active in
`VENUES_BY_ASSET_GROUP["cefi"]` — a stale declaration, not a deliberate deprioritization. Removed from all registries
and mappings, same treatment as the earlier GEMINI-SPOT/PHEMEX-SPOT removal below. The deeper `external/bitstamp/`
and `external/huobi/` normalize/schema submodules were left in place (matching the Gemini/Phemex precedent — only the
registry/mapping declarations that drive the active venue list get pruned, not the adapter-level code underneath).

**Removed** (from an earlier iteration of this doc, still true): GEMINI-SPOT, PHEMEX-SPOT — low volume, removed from
all registries.

### Deribit margin types

Deribit is **not** single-margin-type — it runs both linear and inverse instruments side by side, and the adapter code
distinguishes them by quote currency: `instruments_service/reference_data/adapters/cefi/tardis/parsing.py:303,410,423`
(`_resolve_base_quote` / `_infer_margin_type`) — inverse instruments are USD-quoted but BTC/ETH-settled
(`BTC-PERPETUAL`, coin-margined), linear instruments are USDC-quoted and USDC-settled (`BTC_USDC-PERPETUAL`). Both are
captured; `margin_type` (`LINEAR`/`INVERSE`) and `quote_asset` are populated per-instrument in the v6 schema. An
earlier version of this doc's Venues table didn't call this out explicitly even though the schema already tracked it —
corrected here.

### UPBIT symbol inversion

UPBIT uses QUOTE-BASE format (`KRW-BTC` = buy BTC with KRW). The Tardis adapter detects this and inverts base/quote
before filtering (`parsing.py:350-352`).

### Expiry parsing

Deribit symbols encode expiry: `BTC-27MAR26-190000-C` → `2026-03-27`. The adapter parses `DDMMMYY` from the second
segment when Tardis's own `expiry` field isn't populated (common for active options).

## MVP Universe

**The old claim in this doc's predecessor spec (`specs/MVP_INSTRUMENTS.md`) — 21 base assets × 5 CeFi exchanges = 168
instruments — is stale and should not be used.** That list (SOL, BTC, ETH, AVAX, ADA, SUSHI, CAKE, XRP, DOGE, XLM, LTC,
ALGO, FIL, TRX, BNB, LINK, MATIC, APT, VET, ATOM, NEAR across Binance/OKX/Bybit/Upbit/Coinbase) describes a much
earlier, narrower phase of the project. The real, current MVP-scoping mechanism is the UAC registry
`unified_api_contracts/registry/cefi_instrument_universe.py` — the actual SSOT for which base assets instruments-service
tracks (and MTDS captures) across every CeFi venue.

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
extension) — fixed 2026-07-08, see "Known bugs" below for the prior gap.

### Options underlyings

Deribit options stay restricted to **BTC and ETH only** (`CEFI_OPTIONS_UNDERLYINGS`, `cefi_instrument_universe.py:166-169`)
— a genuine data-volume constraint (Deribit already has ~213K historical option rows per the adapter's own code
comment; a per-coin option-chain expansion would multiply that further for little added value).

### Equity/commodity-basis perp universe

`CEFI_EQUITY_PERP_BASE_UNIVERSE` (`cefi_instrument_universe.py`) is a separate curated set covering
single-stock/commodity/index perps listed on crypto venues (17 OKX US-equity perps + a much larger Binance
tradfi-perp-symmetry set added 2026-06-24, re-synced 2026-07-08 — now **124 entries**) — this exists specifically so
each crypto-venue tradfi-underlying perp has a captured basis-arb counterpart in the TradFi MVP universe. Crypto
majors (BTC/ETH/SOL/…) are **not** in this set — only the tradfi-underlying perps.

**2026-07-08 re-sync** against the live Binance `fapi/v1/exchangeInfo` (`contractType=TRADIFI_PERPETUAL`, 118 symbols
live that day, all `status=TRADING`) versus the 105-entry 2026-06-24 snapshot:

- **+21 added**: ANTHROPIC / OPENAI (pre-IPO share perps, `underlyingType=PREMARKET`), BBX, BSP, BX, BZ
  (`underlyingType=COMMODITY`), CAT, CBRS, DRAM, FLEX, KORU, KSTR, MVLL, QNTX, SQQQ, STRC, STXX, TER, TQQQ, TTWO, TXN.
- **3 renamed**: the Korean single-stock entries moved from the KRX numeric code to the actual live `baseAsset`
  string, confirmed identical on Binance, OKX, **and** Bybit — `005930`→`SAMSUNG`, `000660`→`SKHYNIX`,
  `005380`→`HYUNDAI`. The numeric KRX code never matched any live venue's real base_ccy for _this_ (cefi-perp)
  universe; it remains correct and unchanged for the separate TradFi-side identifier space
  (`TRADFI_EQUITY_PERP_BASIS_UNIVERSE` / `KRX_EQUITIES`, keyed by the Yahoo/Databento ticker root).
- **2 removed**: `AMC`, `MARA` — verified genuinely delisted (absent from Binance, OKX across
  SWAP/FUTURES/SPOT/MARGIN, and Bybit; `AMC-USDT-SWAP`/`MARA-USDT-SWAP` return OKX error 51001 "doesn't exist"). The
  matching dead cross-reference in `crypto_equity_link.CRYPTO_EQUITY_PERP_TO_REAL_EQUITY` was removed in the same
  commit.
- **6 kept despite an open question** — `CFG`, `DIA`, `INX`, `ROBO`, `SLX`, `SPX` no longer appear under
  `contractType=TRADIFI_PERPETUAL` on Binance; Binance now tags them `underlyingType=COIN` (subtypes
  Meme/Infrastructure/RWA/Alpha) — the ticker has been reused by an unrelated crypto token (e.g. `SPX` is now the
  "SPX6900" meme coin, not the S&P 500), not a simple delisting. All 6 still have live symmetric entries in
  `TRADFI_EQUITY_PERP_BASIS_UNIVERSE` (real ETFs/stocks) and in `crypto_equity_link.py`. Resolving this needs a
  dedicated cross-venue audit (does OKX/Bybit's same-ticker perp reuse the same crypto token, or a genuine surviving
  equity-basis product?) rather than a unilateral removal — see the new row in "Known bugs" below.

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
      the documented exception (it DOES resolve a real quote), fixed 2026-07-08, see "Known bugs" below.
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

**Not fixed yet — this section shows real current output alongside the operator-decided target, same
current-vs-target framing as the A_TOKEN/DEBT_TOKEN lending-instrument decision.** Full detail:
[`instrument_id_format_canonicalization_2026_07_08.md`](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md).

Dated-derivative (FUTURE/OPTION) instrument_ids are **not consistent with each other across venues today**, and don't
get the same cleanup the same venue's own PERPETUAL gets:

| Venue           | Real current instrument_id (as of 2026-07-08)    | Note                                                                                                                                                                                                            |
| --------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| KRAKEN-FUTURES  | `KRAKEN-FUTURES:FUTURE:XBT-USD-inverse-20260731` | Raw `FI_`/`FF_`/`PI_`/`PF_` prefix now correctly stripped (collision bug fixed — see "Known bugs" below), but still `-inverse-` word-form margin marker + raw expiry digits, not yet the target `@INV-YYYYMMDD` |
| BINANCE-FUTURES | `BINANCE-FUTURES:FUTURE:BTCUSDT_260925`          | Raw concatenated base+quote, underscore-date                                                                                                                                                                    |
| BYBIT           | `BYBIT:FUTURE:BTC-01DEC23`                       | No quote segment at all; `DDMMMYY` date                                                                                                                                                                         |
| DERIBIT         | `DERIBIT:OPTION:BTC-10JUL26-48000-C`             | `DDMMMYY` date — looks clean in isolation, but does not match the other 3 venues, and `DDMMMYY` does not sort chronologically as a string                                                                       |

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

## Known bugs / open findings (as of 2026-07-08)

Summarized from the full [canonical instrument_id audit](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).
Status reflects each finding's own fix-plan as of this writing — **do not assume anything below is fixed just because
it's old; check the cited plan's `status:` before relying on this table.**

| Finding                                                                         | Status                                        | Detail                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bitfinex BTC-margined-perp silently dropped**                                 | **FIXED** (2026-07-08)                        | The accepted-quote gate (`parsing.py:463`, docstring: "derivatives carry no quote and pass") assumed derivatives never carry a quote — but Bitfinex's own derivative symbol parser (`parsing.py:325-339`) _does_ extract a real quote for Bitfinex inverse perps (`ETHF0:BTCF0` → base=ETH, quote=BTC). Since `BTC` wasn't in the accepted-quote set for Bitfinex, real Bitfinex BTC-margined perps were rejected as if they were an exotic cross-pair. Confirmed live via `api-pub.bitfinex.com/v2/tickers`: `ETHF0:BTCF0` trades ~~2,000+ ETH/day (~~$6-7M/day) — not a negligible edge case; same family includes `LTCF0:BTCF0`, `XRPF0:BTCF0`, `XAUTF0:BTCF0`. **Fix shipped**: `BTC` added as a `BITFINEX-FUTURES`-keyed accepted-quote extension (`_CEFI_VENUE_QUOTE_EXTENSIONS`, `cefi_instrument_universe.py`) — keyed on the FULL canonical venue string rather than the bare entity (unlike the UPBIT/KRW extension) so the sibling `BITFINEX-SPOT` does not also start accepting `BASE/BTC` cross-pairs. Verified end-to-end: `_passes_asset_filter("ETH", "BTC", "PERPETUAL", venue="BITFINEX-FUTURES")` now returns `True`; `BITFINEX-SPOT` `ETH/BTC` still correctly returns `False`. `unified-api-contracts@<PENDING-SHA>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **Equity-perp base universe stale vs. Binance's live listing (105→124)**        | **PARTIALLY OPEN**                            | `CEFI_EQUITY_PERP_BASE_UNIVERSE` was re-synced 2026-07-08 against live Binance `fapi/v1/exchangeInfo` — see "Equity/commodity-basis perp universe" above for the full +21/-2/3-renamed diff. **Still open**: `CFG`, `DIA`, `INX`, `ROBO`, `SLX`, `SPX` no longer appear as `TRADIFI_PERPETUAL` on Binance (Binance now tags them `underlyingType=COIN` — the ticker was reused by an unrelated crypto token, e.g. `SPX` = the "SPX6900" meme coin) but were **not removed** pending a dedicated cross-venue audit, since all 6 still have live symmetric entries in `TRADFI_EQUITY_PERP_BASIS_UNIVERSE` and `crypto_equity_link.py`, and OKX/Bybit list a same-ticker perp too (not yet confirmed whether that's the same crypto token or a surviving equity product). `unified-api-contracts@<PENDING-SHA>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **CCXT live-mode instrument_id ≠ batch-mode (Tardis) instrument_id, 13 venues** | **OPEN — P0**                                 | `instruments_service/reference_data/adapters/cefi/ccxt_adapter.py:156` (`_parse_ccxt_market`) stores `instrument_key=symbol` — the bare, unmodified ccxt-native market symbol (`"BTC/USDT"`, `"BTC/USDT:USDT"`) — with zero canonicalization. This is the live-mode route for all 13 CCXT-backed venues in the Venues table above. Batch mode (Tardis) produces a differently-shaped, properly dash-cleaned id for the same real instrument. Same instrument, structurally different ids depending on capture mode — a direct live=batch determinism violation. Not yet fixed (fix plan `canonical_id_p0_ccxt_live_batch_divergence_2026_07_08.md`, 0/4 todos done as of this writing). This is also a **blocking prerequisite** for the strategy-service position-reconciliation fix (below) to actually work end-to-end.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **Live position reconciliation silently defeated for every CCXT venue**         | **OPEN — P0**                                 | Downstream consequence of the divergence above: strategy-service's `reconciliation_engine.py::_find_exchange_qty` compares the internal canonical `instrument_id` against the raw exchange symbol coming back from these same CCXT-backed adapters — they never match, so the check that's supposed to catch a real position mismatch always falls through to "no exchange position." Not yet fixed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Kraken-Futures dated-future symbol collision**                                | **FIXED** (core bug + historical remediation) | 5 genuinely distinct real instruments (BCH/ETH/LTC/XBT/XRP quarterly futures, same expiry) previously collapsed onto one identical `instrument_id` because the underlying-extraction regex assumed a `TICKER-QUOTE` shape and Kraken's real dated-future format is `{TYPE_PREFIX}_{PAIR}_{DATE}`. Fixed and shipped: `market-tick-data-service@3d7491b1bcbebc17af0aa31219e90f38478d57cd` (added `_KRAKEN_DATED_PREFIX_RE`, verified against all 5 original colliding files + 2 more real symbols — all now distinct). **Historical remediation done (2026-07-08)**: full-corpus GCS scope found real Kraken-Futures dated-future captures on exactly 5 real days (`2022-03-01`, `2022-03-04`, `2024-02-01`, `2025-01-10`, `2026-01-10`), 125 real parquet files, 37,559,524 rows, 6 real tickers (BCH/ETH/LTC/SOL/XBT/XRP) — every file was already single-real-instrument by filename (the bug never physically merged files, only corrupted the `underlying`/`instrument_id` COLUMN VALUES), so all 125 files were fixed in place (server-side backup first, then column recompute from the untouched raw `symbol`), 0 errors; independent re-verification confirms 0 remaining collisions between different real tickers. **New, separate finding from this verification (not fixed, not this bug)**: 13 (ticker, expiry) pairs — ETH/XBT only — have both a real `FI_` and a real `FF_` raw symbol with genuinely different row counts that now collapse onto the same corrected `instrument_id` (`derive_row_instrument_id` has no field for the `FI`/`FF` contract-subtype) — see the plan's new P2 todo. Note the core fix + remediation restored per-instrument (ticker,expiry) distinctness only; it did **not** migrate the format to the `@INV`/`YYYYMMDD` target above — that's a separate, still-pending migration. |
| **Kraken-Futures `FI_` vs `FF_` same-(ticker,expiry) ambiguity**                | **OPEN — P2 (new, found 2026-07-08)**         | Discovered while verifying the collision fix above: 13 real (ticker, expiry) combinations (`ETH`/`XBT` only, 2024-2026 range) have BOTH a real `FI_` and a real `FF_` raw Tardis symbol with different row counts (e.g. `FI_ETHUSD_240329` = 129,010 book_snapshot_5 rows + 447 trades vs `FF_ETHUSD_240329` = 107,156 book_snapshot_5 rows + 330 trades) — not duplicates, two genuinely different real data series — that both derive `KRAKEN-FUTURES:FUTURE:ETH-USD-inverse-20240329` today. `derive_row_instrument_id`'s FUTURE branch has no field to encode the `FI`/`FF` contract-subtype at all, so this isn't an extraction bug like the row above — it's a schema gap. The existing code comment in `tardis_shared.py` describing `FI_` as "old index, pre-2020, no longer active" is contradicted by this real 2024-2026 data. Needs an operator decision on what `FI_` vs `FF_` actually represents for KRAKEN-FUTURES and how (or whether) to encode it in the canonical instrument_id before any further Kraken-Futures work. Tracked in `canonical_id_p0_kraken_futures_collision_2026_07_08.md`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **23 DeFi adapters silently return empty on canonical-form type filters**       | Fixed (DeFi-scope, not CeFi)                  | Out of this doc's scope — see `DEFI_INSTRUMENTS.md`. Noted here only because the audit found it in the same pass; its fix plan (`canonical_id_p0_defi_adapter_type_filter_bug_2026_07_08.md`) is complete (4/4 todos).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

## Caching strategy

Three layers, unchanged from the prior version of this doc and still accurate:

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
