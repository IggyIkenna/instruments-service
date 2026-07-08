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

**Declared but not currently active**: `VenueMapping.all_tardis_exchanges`
(`unified_api_contracts/registry/venue_mapping.py:32-60`) also declares `bitstamp`, `huobi`, and `huobi-dm` as Tardis
endpoints (and `venue_to_ccxt`/`venue_instrument_type_to_tardis` have matching `BITSTAMP-SPOT`/`HUOBI-SPOT`/
`HUOBI-FUTURES` entries) — but none of the three appear in `VENUES_BY_ASSET_GROUP["cefi"]`
(`unified_api_contracts/registry/market_data_categories.py:226-274`), which is the actual enumerated active-venue list
driving the download/manifest universe. Adapter plumbing exists; these 3 are not part of the current capture universe.
**Open question, not resolved by this doc**: whether this is a deliberate deprioritization (lower liquidity than the
Tier-3 additions that did make the cut) or a stale declaration nobody's pruned — worth a quick check before treating
either Bitstamp or Huobi/HTX as "coming soon."

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

Fleet default: `USDT`, `USDC`, `USD` (`CEFI_ACCEPTED_QUOTE_ASSETS`, `cefi_instrument_universe.py:131-133`). Per-venue
extension: **UPBIT only** additionally accepts `KRW` (`_CEFI_VENUE_QUOTE_EXTENSIONS`, `cefi_instrument_universe.py:142-144`)
— KRW is deliberately not added fleet-wide (it would admit thousands of cross pairs on other venues). This is the only
venue-specific quote extension that currently exists — see the open Bitfinex bug below for a real gap in this same
mechanism.

### Options underlyings

Deribit options stay restricted to **BTC and ETH only** (`CEFI_OPTIONS_UNDERLYINGS`, `cefi_instrument_universe.py:166-169`)
— a genuine data-volume constraint (Deribit already has ~213K historical option rows per the adapter's own code
comment; a per-coin option-chain expansion would multiply that further for little added value).

### Equity/commodity-basis perp universe

`CEFI_EQUITY_PERP_BASE_UNIVERSE` (`cefi_instrument_universe.py:191-227`) is a separate curated set covering
single-stock/commodity/index perps listed on crypto venues (17 OKX US-equity perps + a much larger Binance
tradfi-perp-symmetry set added 2026-06-24: AAPL, TSLA, NVDA, … plus XAU/XAG/NATGAS/COPPER commodities and
SPX/SPY/QQQ/… index & sector-ETF perps) — this exists specifically so each crypto-venue tradfi-underlying perp has a
captured basis-arb counterpart in the TradFi MVP universe. Crypto majors (BTC/ETH/SOL/…) are **not** in this set —
only the tradfi-underlying perps.

### Staking/LST spot exception

`STAKING_SPOT_EXCEPTION` (`cefi_instrument_universe.py:249-254`) is the one carve-out from the ordinary "spot only
where a perp also exists" rule: liquid-staking/restaking tokens (STETH, WSTETH, RETH, WEETH, EETH, EIGEN, ETHFI, the
Solana LSTs MSOL/JITOSOL/JSOL/BSOL/SCNSOL/INF, and more) get their spot captured on any venue that lists them, perp or
no perp — these are the `carry_staked_basis`/DeFi-seasonal-reward legs the strategy layer needs spot liquidity for.

### The two surviving gates

Per `instruments_service/reference_data/adapters/cefi/tardis/parsing.py:430-466` (`_passes_asset_filter`), the curated
base-asset whitelist is **no longer a gate** for SPOT/PERP/FUTURE (operator 2026-06-23 — the reference universe must
equal each venue's real listed universe, not a curated subset, so small/illiquid-coin funding/price history isn't
lost). The two gates that remain are venue-volume-safe, not coin-curation:

1. **Accepted-quote gate** — USDT/USDC/USD fleet-wide + the per-venue KRW/UPBIT extension. Drops exotic cross pairs
   (`BASE/EUR`, `BASE/BTC`) on other venues; derivatives are documented as "carry no quote and pass" — **this
   documented behavior is exactly where the open Bitfinex bug below diverges from reality.**
2. **Options-underlying gate** — BTC/ETH only, as above.

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

| Finding                                                                         | Status                       | Detail                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bitfinex BTC-margined-perp silently dropped**                                 | **OPEN — P1**                | The accepted-quote gate (`parsing.py:463`, docstring: "derivatives carry no quote and pass") assumes derivatives never carry a quote — but Bitfinex's own derivative symbol parser (`parsing.py:325-339`) _does_ extract a real quote for Bitfinex inverse perps (`ETHF0:BTCF0` → base=ETH, quote=BTC). Since `BTC` isn't in the accepted-quote set for Bitfinex (only `UPBIT` has a per-venue quote extension, for `KRW`), real Bitfinex BTC-margined perps get rejected as if they were an exotic cross-pair. Confirmed live via `api-pub.bitfinex.com/v2/tickers`: `ETHF0:BTCF0` trades ~~2,034 ETH/day (~~$6-7M/day) — not a negligible edge case. **Fix**: add `BTC` to a per-venue accepted-quote extension for Bitfinex derivatives, the same mechanism already used for UPBIT's `KRW` extension.                                                                                    |
| **CCXT live-mode instrument_id ≠ batch-mode (Tardis) instrument_id, 13 venues** | **OPEN — P0**                | `instruments_service/reference_data/adapters/cefi/ccxt_adapter.py:156` (`_parse_ccxt_market`) stores `instrument_key=symbol` — the bare, unmodified ccxt-native market symbol (`"BTC/USDT"`, `"BTC/USDT:USDT"`) — with zero canonicalization. This is the live-mode route for all 13 CCXT-backed venues in the Venues table above. Batch mode (Tardis) produces a differently-shaped, properly dash-cleaned id for the same real instrument. Same instrument, structurally different ids depending on capture mode — a direct live=batch determinism violation. Not yet fixed (fix plan `canonical_id_p0_ccxt_live_batch_divergence_2026_07_08.md`, 0/4 todos done as of this writing). This is also a **blocking prerequisite** for the strategy-service position-reconciliation fix (below) to actually work end-to-end.                                                                  |
| **Live position reconciliation silently defeated for every CCXT venue**         | **OPEN — P0**                | Downstream consequence of the divergence above: strategy-service's `reconciliation_engine.py::_find_exchange_qty` compares the internal canonical `instrument_id` against the raw exchange symbol coming back from these same CCXT-backed adapters — they never match, so the check that's supposed to catch a real position mismatch always falls through to "no exchange position." Not yet fixed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Kraken-Futures dated-future symbol collision**                                | **FIXED** (core bug)         | 5 genuinely distinct real instruments (BCH/ETH/LTC/XBT/XRP quarterly futures, same expiry) previously collapsed onto one identical `instrument_id` because the underlying-extraction regex assumed a `TICKER-QUOTE` shape and Kraken's real dated-future format is `{TYPE_PREFIX}_{PAIR}_{DATE}`. Fixed and shipped: `market-tick-data-service@3d7491b1bcbebc17af0aa31219e90f38478d57cd` (added `_KRAKEN_DATED_PREFIX_RE`, verified against all 5 original colliding files + 2 more real symbols — all now distinct). **Still open**: scoping how much historical GCS data was already silently affected by the collision, and deciding whether to backfill it — both operator-decision-gated, not yet attempted. Note this fix restored per-instrument distinctness; it did **not** migrate the format to the `@INV`/`YYYYMMDD` target above — that's a separate, still-pending migration. |
| **23 DeFi adapters silently return empty on canonical-form type filters**       | Fixed (DeFi-scope, not CeFi) | Out of this doc's scope — see `DEFI_INSTRUMENTS.md`. Noted here only because the audit found it in the same pass; its fix plan (`canonical_id_p0_defi_adapter_type_filter_bug_2026_07_08.md`) is complete (4/4 todos).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

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
