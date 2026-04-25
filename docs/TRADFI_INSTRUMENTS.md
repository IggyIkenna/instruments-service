# TradFi Instrument Pipeline

## Overview

TradFi instruments are fetched from **Databento** (CME, ICE, NYSE, NASDAQ) and **Yahoo Finance** (FX spot). The URDI Databento adapter uses the curated `TRADFI_DATABENTO_INSTRUMENTS` registry in UAC to fetch only the instruments we care about — not the entire dataset (which returns millions of rows).

## Venues (5 active)

| Venue  | Source        | Dataset     | Instrument Types                           |
| ------ | ------------- | ----------- | ------------------------------------------ |
| CME    | Databento     | GLBX.MDP3   | Index/commodity/FX/treasury/crypto futures |
| ICE    | Databento     | IFEU.IMPACT | Energy futures (Brent, Gasoil)             |
| NYSE   | Databento     | DBEQ.BASIC  | S&P 500 equities (NYSE-listed)             |
| NASDAQ | Databento     | DBEQ.BASIC  | S&P 500 equities + ETFs (NASDAQ-listed)    |
| FX     | Yahoo Finance | N/A         | Static FX spot pairs                       |

**Disabled:** CBOE/CFE (`XCBF.PITCH` — dataset not in Databento subscription).

## Curated Instrument Registry (UAC `tradfi_instrument_universe.py`)

50 curated futures symbols across 5 product groups, each with explicit `asset_group`:

| Group             | Symbols                                                    | asset_group  | Count |
| ----------------- | ---------------------------------------------------------- | ------------ | ----- |
| Index futures     | ES, NQ, RTY, YM, NKD                                       | equity       | 5     |
| Sector futures    | XAF, XAK, XAY, XAP, XAV, XAI, XAB, XAU                     | equity       | 8     |
| Treasury futures  | ZT, ZF, ZN, ZB                                             | fixed_income | 4     |
| Commodity futures | GC, CL, NG, HO, RB, SI, HG, CT, ZS, ZC, ZW, ZL, ZM, LE, HE | commodity    | 15    |
| FX futures        | 6E, 6B, 6J, 6A, 6C, 6N, 6S, 6M, 6Z, 6L                     | fx           | 10    |
| Crypto futures    | BTC, ETH                                                   | crypto       | 2     |
| ICE energy        | BRN, G                                                     | commodity    | 2     |

**Options:** Disabled (ES.OPT produces 5,990 instruments/day). Revisit when options strategy is implemented.

## Filtering

### Expiry Cap (1 year)

Futures with expiry > 1 year from the target date are filtered out. This prevents fetching 5-year quarterly contracts for products like ES that list them.

### Spread Filtering

Databento `instrument_class = "S"` (calendar spreads, cracks, etc.) are excluded. Only outright futures (`"F"`), equities (`"E"`), and options (`"O"`) pass through.

### Equity Deduplication

`DBEQ.BASIC` returns multiple rows per ticker (one per exchange listing — XNYS, XNAS, ARCX, BATS). The adapter deduplicates by `raw_symbol`, keeping the first occurrence.

### Equity Venue Routing

NYSE-listed tickers route to venue `NYSE`, NASDAQ-listed to `NASDAQ`. The split is driven by `TRADFI_TICKER_UNIVERSE["nasdaq_tickers"]` in UAC.

## Per-Instrument asset_group Resolution

`asset_group` is set per-instrument, not per-venue. CME hosts equity index futures (ES), commodity futures (CL), FX futures (6E), treasury futures (ZN), and crypto futures (BTC) — each gets its correct domain category. Resolution order:

1. Match `underlying` field from Databento against UAC registry exchange codes
2. Match 3-char then 2-char prefix of `raw_symbol` against registry
3. Fall back to dataset-level mapping

## Session Metadata (exchange_calendars)

TradFi instruments carry trading session metadata:

- `is_trading_day` — holiday-aware (exchange_calendars library)
- `regular_open_utc` / `regular_close_utc` — DST-aware UTC times
- `early_close_utc` — set on shortened trading days only

CME Sunday-evening session is handled (open Sunday 5pm CT = Monday 22:00 UTC previous day).

## Instrument Counts (2026-03-23)

| Venue     | Count   | asset_group breakdown                                            |
| --------- | ------- | ---------------------------------------------------------------- |
| CME       | 304     | commodity: 143, fx: 79, equity: 52, crypto: 16, fixed_income: 14 |
| NYSE      | 212     | equity                                                           |
| ICE       | 65      | commodity                                                        |
| NASDAQ    | 41      | equity                                                           |
| FX        | 1       | fx                                                               |
| **Total** | **623** |                                                                  |

## Caching Strategy (Implemented)

Adapter TTL cache (1hr) + factory adapter pool. Databento adapter is reused across CME/ICE/NYSE/NASDAQ venues. First call fetches definitions, subsequent calls within TTL return cached results. For batch runs spanning < 30 days, a single fetch covers all dates since the instrument set changes slowly.

## Databento API Usage

We use the **Streaming API** (`timeseries.get_range`) with `schema="definition"` and `stype_in="parent"`. This fetches all contracts under a parent symbol (e.g. `ES.FUT` returns all ES quarterly futures). Monthly subscription — unlimited requests.

## FX Spot (Yahoo Finance)

Static definitions — `KRW/USD` via `KRWUSD=X` ticker. No API call needed; the adapter creates InstrumentRecords from the `FX_SPOT_PAIRS` registry. FX spot is a category, not an exchange — `venue="FX"`, `asset_group="fx"`.
