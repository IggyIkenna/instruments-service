# TradFi Instruments

Covers CME/CBOE(CFE)/NASDAQ/NYSE/ICE(Yahoo-only)/KRX-style traditional-finance futures, options, equities,
event contracts, multi-leg spreads/combos, FX spot, and benchmark indices — the full Databento + Yahoo Finance +
yfinance TradFi pipeline in instruments-service.

**Live mockup**: the TradFi tab in the instruments-definitions mockup —
[claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d](https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d).

---

## 1. Overview

TradFi instruments come from three independent sources:

- **Databento** (CME, NASDAQ, NYSE, CBOE/CFE) — the curated `TRADFI_DATABENTO_INSTRUMENTS` registry in UAC
  (`unified_api_contracts/registry/tradfi_instrument_universe.py`) tells the adapter exactly which parent
  symbols/datasets to fetch, so it pulls only the instruments we care about instead of a full dataset dump (which
  returns millions of rows).
- **Yahoo Finance** — FX spot pairs, KRX (Korea Exchange) single-stock equities, and benchmark indices (DXY, US
  treasury yield tenors, KOSPI/KOSPI200) are static definitions, no per-day API discovery needed.
- **yfinance** (a separate path from the two above) — corporate actions (dividends, splits, earnings) for the S&P
  500 universe. See §10.

**3-dataset subscription lockdown (operator, 2026-06-18)**: instruments-service pays for exactly 3 Databento
datasets — `GLBX.MDP3` (CME), `DBEQ.BASIC` (NASDAQ/NYSE consolidated US equities feed), `XCBF.PITCH` (CBOE Futures
Exchange — VX/VIX futures). Every other Databento dataset is off the allowlist and `assert_dataset_allowed`/
`assert_databento_request_allowed` raise if anything tries to query one — this is a billing guard, not just a
scope choice.

**Corrections to the previous TradFi doc** (this doc supersedes it): the old doc listed ICE as one of "5 active
venues" and CBOE/CFE as "Disabled." Both are stale as of the 2026-06-18 lockdown:

- **ICE is DROPPED**, not active. Brent (BRN), Gasoil (G), the ICE Dollar-Index future (DX), and the US softs
  (CT/CC/KC/SB/OJ) are all off the paid subscription (`IFEU.IMPACT`/`IFUS.IMPACT` datasets removed). The one
  surviving ICE-labeled series is the Yahoo-sourced DXY **cash** index (`DX-Y.NYB`) — that's a Yahoo series, not a
  Databento ICE dataset, so it is unaffected by the purge and is the sole retained ICE exception.
- **CBOE/CFE is ACTIVE**, not disabled. `XCBF.PITCH` (Cboe Futures Exchange) now feeds VX (VIX) **futures**
  outright contracts. The bare "CFE" dataset code is rejected by the Databento API (400 `validation_failed`) — the
  real dataset id is `XCBF.PITCH`; the canonical venue token stays `CBOE`. This gives VIX _futures_, not the VIX
  _cash index_ — the old Barchart-sourced 15-minute cash index was **retired 2026-06-25**; the 15m VIX series is
  now aggregated from the VX futures front contract instead.

---

## 2. Venues (current, real)

| Venue      | Source             | Dataset      | Role                                                                                     |
| ---------- | ------------------ | ------------ | ---------------------------------------------------------------------------------------- |
| CME        | Databento          | `GLBX.MDP3`  | Index/sector/commodity/FX/crypto futures + options + CME event contracts (binary yes/no) |
| NASDAQ     | Databento          | `DBEQ.BASIC` | Tech-heavy equities, BTC/ETH spot ETFs (IBIT/ETHA), select single-stock hedge legs       |
| NYSE       | Databento          | `DBEQ.BASIC` | Remaining S&P 500 equities, additional single-stock hedge legs                           |
| CBOE (CFE) | Databento          | `XCBF.PITCH` | VX (VIX) futures — outright contracts only (class "S" calendar spreads dropped, see §7)  |
| FX         | Yahoo Finance      | —            | G10 FX majors + KRW/USD static spot pairs                                                |
| KRX        | Yahoo Finance      | —            | Korea Exchange single-stock equities + KOSPI/KOSPI200 indices                            |
| ICE        | Yahoo Finance ONLY | —            | DXY (US Dollar Index) cash series only — every ICE Databento dataset is dropped          |

**Dropped / retired** (do not reference as active):

- ICE Databento datasets — Brent (BRN), Gasoil (G) on `IFEU.IMPACT`; US softs + Dollar-Index future (DX) on
  `IFUS.IMPACT`. Billing-lockdown, 2026-06-18. Re-adding any of these requires a real ICE subscription plus an
  explicit dataset-allowlist change.
- Barchart-sourced VIX **cash** index — retired 2026-06-25, superseded by VX-futures aggregation.
- Direct per-venue crypto-ETF feeds (`XNAS.ITCH`, `ARCX.PILLAR`, `BATS.PITCH`) — dropped in favor of `DBEQ.BASIC`
  only, same 2026-06-18 lockdown. MVP crypto-ETF scope was independently reduced (2026-05-05) to IBIT + ETHA only
  (NASDAQ) — FBTC/ARKB (BATS), GBTC/ETHE/BITO (NYSE Arca) are out of MVP scope; re-add only if a strategy
  archetype needs them.

---

## 3. Curated Databento Instrument Registry

**SSOT**: `unified-api-contracts/unified_api_contracts/registry/tradfi_instrument_universe.py` —
`TRADFI_DATABENTO_INSTRUMENTS`, **93 curated definitions** (counted directly from the registry, 2026-07-08). UAC
owns this registry; the URDI Databento adapter reads it at import time (config-reloader override possible via cloud
ConfigStore, hot-reloadable, but this file is the default).

| Group                               | Count | Symbols / roots                                                                                                       |
| ----------------------------------- | ----- | --------------------------------------------------------------------------------------------------------------------- |
| CME index futures                   | 5     | ES, NQ, RTY, YM, NKD                                                                                                  |
| CME sector futures (SPDR-style)     | 8     | XAF, XAK, XAY, XAP, XAV, XAI, XAB, XAU                                                                                |
| CME treasury futures                | 4     | ZT, ZF, ZN, ZB                                                                                                        |
| CME commodity futures               | 16    | GC, CL, NG, HO, RB, SI, HG, PL, PA, ZS, ZC, ZW, ZL, ZM, LE, HE                                                        |
| CME FX futures                      | 10    | 6E, 6B, 6J, 6A, 6C, 6N, 6S, 6M, 6Z, 6L                                                                                |
| CME crypto futures                  | 5     | BTC, ETH, MBT (micro BTC), MET (micro ETH), MES (micro E-mini S&P)                                                    |
| CME ES options surfaces             | 11    | ES.OPT (quarterly) + EW/EW1/EW2/EW4 (weekly) + E1A-E5A (0DTE daily) + EOM                                             |
| CME commodity options-on-futures    | 9     | OG(gold), LO(crude), ON(natgas), HXE(copper), SO(silver), PO(platinum), PAO(palladium), OH(heating oil), OB(gasoline) |
| CME index options-on-futures        | 1     | NQ.OPT (Nasdaq-100)                                                                                                   |
| CME event contracts (binary yes/no) | 9     | ECES, ECNQ, ECRTY, ECYM, ECGC, ECCL, ECNG, EC6E, ECBTC                                                                |
| CFE (VX/VIX) futures                | 1     | VX.FUT                                                                                                                |
| BTC spot ETF                        | 1     | IBIT (NASDAQ)                                                                                                         |
| ETH spot ETF                        | 1     | ETHA (NASDAQ)                                                                                                         |

That's **81 genuinely-TradFi curated definitions**. The registry file also carries a 12th group that is **not**
conceptually part of the TradFi pipeline even though it lives in the same file and uses the same `DBEQ.BASIC`
dataset:

| Group                                                                     | Count | Symbols                                                                |
| ------------------------------------------------------------------------- | ----- | ---------------------------------------------------------------------- |
| Net-profitable crypto-venue equity-perp hedge legs (`asset_group="cefi"`) | 12    | NVDA, MSFT, CRCL, INTC, GOOGL, AMD, TSLA, AMZN, META, HOOD, AAPL, BABA |

These are real US-listed stocks (Databento `DBEQ.BASIC`, `raw_symbol` stype) used as IBKR stock-borrow hedge legs
for crypto-venue single-stock perps — added 2026-06-20 after a NET-basis backtest (`NET = perp_funding_ann -
futures_roll_carry_ann`, threshold >5% annualized). They're tagged `asset_group="cefi"` specifically **to keep them
out of the TradFi data pipeline** even though they're fetched via the TradFi adapter's dataset — don't confuse them
with the TradFi equity universe in §5.

**Not disabled — options ARE live** (the old doc's "Options: Disabled (ES.OPT produces 5,990 instruments/day).
Revisit when options strategy is implemented" line is stale): ES options, weekly/daily/EOM surfaces, and 9
commodity + 1 index options-on-futures root are all in the live curated registry today.

---

## 4. Yahoo-Sourced Static Registries

Also defined in `tradfi_instrument_universe.py`, fetched via Yahoo Finance (not Databento, no billing exposure):

| Registry        | Count | Contents                                                                                                                                  |
| --------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `FX_SPOT_PAIRS` | 11    | G10 FX majors (EUR/USD, GBP/USD, USD/JPY, AUD/USD, USD/CAD, USD/CHF, NZD/USD, EUR/GBP, EUR/JPY, USD/MXN) + KRW/USD (kimchi-premium basis) |
| `KRX_EQUITIES`  | 3     | Hyundai Motor (005380), Samsung Electronics (005930), SK Hynix (000660) — `.KS` Yahoo tickers, daily history confirmed back to 2019-01-02 |
| `YAHOO_INDICES` | 8     | DXY (ICE-labeled, Yahoo-sourced), US3M/US2Y/US5Y/US10Y/US30Y (CBOE-labeled treasury yield tenors), KOSPI/KOSPI200 (KRX-labeled)           |

Notes:

- G10 FX majors added 2026-06-26 for cefi features (DXY context, Polymarket EUR/USD arb, cross-asset macro
  signals); history confirmed back to 2003+ on Yahoo, backfill floor is the operator target 2019-01-01.
- US2Y is sourced from the CME yield-future `2YY=F` (genesis 2018-08-13, best-estimate, VERIFY at backfill) — the
  only Yahoo 2-year series available; the operator directed including it despite previously-noted
  stale/zero-volume concerns, since honest-absence handling surfaces freshness downstream.
- KOSPI/KOSPI200 added 2026-06-27 (operator: "daily KOSPI prices from Yahoo Finance"). These are `INDEX`-type
  (non-tradeable references), distinct from the 3 KRX single-stock `EQUITY` entries above.
- **Open question**: the exact `instrument_id` construction for `YAHOO_INDICES` / `KRX_EQUITIES` entries (which
  Yahoo-specific adapter builds the final `instrument_key` string) was not read in this pass — only the UAC
  registry dataclasses were confirmed. Don't assume a specific string format for these without checking the actual
  Yahoo TradFi adapter.

---

## 5. Equity / ETF Ticker Universe

Equities and ETFs are **not** in the Databento registry above — they come from a separate file,
`unified_api_contracts/registry/tradfi_ticker_universe.py`, per that file's own docstring ("Equities/ETFs come from
the tradfi_ticker_universe.py ... instead"). Real counts (2026-07-08):

| List                       | Count |
| -------------------------- | ----- |
| `SP500_TICKERS`            | 200   |
| `NASDAQ_TICKERS`           | 101   |
| `ETF_TICKERS`              | 78    |
| `NYSE_TRADFI_PERP_TICKERS` | 18    |

`NASDAQ_TICKERS` (a subset of the broader universe) drives equity venue routing — see §7.

**Open question / stale-content flag**: the old `MVP_INSTRUMENTS.md` claimed **~603 total S&P 500 tickers**
(current + historical 2020-2025 constituents including delisted names like ATVI/CERN/XLNX/FRC). The current
`SP500_TICKERS` list has only 200 entries — a real discrepancy this pass could not resolve (didn't have time to
check whether historical constituents live in a different/larger list, a config-reloader override, or whether the
universe was genuinely pruned since MVP_INSTRUMENTS.md was written). Don't repeat the ~603 number as current fact
without checking the live list first.

---

## 6. MVP Universe (real, from code)

`MVP_CME_EXCHANGE_CODES` (33 exchange codes, `tradfi_instrument_universe.py`) gates
`get_mvp_databento_symbols_for_venue()` — **for CME only**. Every other venue (NASDAQ, NYSE, CBOE, FX, KRX) returns
its **full** curated list unfiltered even in MVP mode. Note also: the main adapter fetch path uses
`TRADFI_DATABENTO_INSTRUMENTS` directly with **no MVP filter at all** — the MVP gate only applies to the
`get_mvp_databento_symbols_for_venue()` helper function, not to what the adapter actually downloads day-to-day.

**MVP CME scope** (operator 2026-06-27 — "CME roots whose underlying futures have a Binance perp leg"):

- **SP500 complex**: ES + MES futures, plus the full ES options surface (ES.OPT quarterly + EW/EW1/EW2/EW4 weekly +
  E1A-E5A daily 0DTE + EOM).
- **NQ**: NQ.FUT + NQ.OPT (Nasdaq-100 options-on-futures).
- **Commodity futures + options-on-futures** backing Binance perps: GC+OG (gold), CL+LO (crude), NG+ON (natgas),
  HG+HXE (copper), SI+SO (silver), PL+PO (platinum), PA+PAO (palladium).
- **CME event contracts** for the above underlyings: ECES, ECNQ, ECGC, ECCL, ECNG (+ECBTC, standalone Bitcoin
  event contract).

**Stale content in the old `MVP_INSTRUMENTS.md`** (do not carry forward):

- CBOE VIX "via Barchart" — Barchart is retired (§1); VIX is VX-futures-aggregated now.
- GBTC/BITO/ETHE/FBTC/ARKB Bitcoin/Ether ETFs — dropped from MVP scope 2026-05-05 (only IBIT/ETHA remain, §2).
- ICE energy futures (BRN/G) as a "default TradFi exchange" — ICE is dropped entirely (§1-2).
- Missing entirely from that doc: PL/PA metals futures, MBT/MET/MES micro futures, the 9 CME event contracts, and
  all 10 commodity/index options-on-futures roots — all added 2026-06-20 through 2026-06-27, after that doc was
  last updated.
- The doc's "~10,891 instruments per day" TradFi total and its per-venue instrument counts predate all of the
  above changes and should not be treated as current.

---

## 7. Filtering & Resolution Logic

- **Expiry cap**: futures with expiry beyond 1 year from the target date are filtered out
  (`_is_filtered_out`) — prevents fetching multi-year quarterly contracts for products (like ES) that list them.
- **Equity dedup**: `DBEQ.BASIC` returns one row per exchange listing (XNYS, XNAS, ARCX, BATS) for the same
  ticker; the adapter dedups by `raw_symbol`, keeping the first occurrence.
- **Equity venue routing**: tickers in `NASDAQ_TICKERS` (via `TRADFI_TICKER_UNIVERSE["nasdaq_tickers"]`) route to
  venue `NASDAQ`; everything else on `DBEQ.BASIC` defaults to `NYSE`.
- **Per-instrument `asset_group` resolution** (not per-venue — CME alone spans equity/commodity/FX/fixed_income/
  crypto): (1) match the Databento `underlying` field against the UAC registry, (2) match the 3-char then 2-char
  prefix of `raw_symbol`, (3) fall back to a dataset-level default (`_DATASET_TO_asset_group`).
- **Class "S" (spread) handling is venue-dependent — not a blanket exclusion.** The previous doc's claim
  ("`instrument_class = "S"` are excluded. Only outright futures/equities/options pass through") is stale. Real
  behavior, per instrument dataset:
  - **`GLBX.MDP3` (CME)**: class "S" → reclassified `InstrumentType.COMBO`, legs parsed via
    `_parse_cme_calendar_spread_legs` — decomposed, not dropped. See §11.
  - **`XCBF.PITCH` (CBOE/VX)**: class "S" is **dropped entirely** (`inst_class not in ("F","M") → return None`) —
    a 2026-06-25 fix (tag `G1.c`) explicitly targeting a prior bug where these rows polluted the catalog as
    mis-typed `SPOT_PAIR` (comment cites "4,216 mis-typed SPOT_PAIR rows"). See §11 for why real historical rows
    in this shape still exist in production.
  - **`DBEQ.BASIC` (equities)**: class "S" → reclassified `EQUITY` (not `SPOT_PAIR`) — a separate 2026-06-25 fix
    (tag `G1.d`) for 318 previously mis-typed NASDAQ/NYSE equity rows.
- **User-defined combos** (e.g. Databento symbols like `"UD:1V:CXT ..."`) come through parent symbology as
  FUTURE/OPTION but have no derivable underlying — these get reclassified `COMBO` too.
- **Databento API usage**: Streaming API (`timeseries.get_range`), `schema="definition"`, `stype_in="parent"` for
  futures/options (returns every contract under a parent symbol in one call, e.g. `ES.FUT` → all ES quarterlies) or
  `stype_in="raw_symbol"` for equities/ETFs/single-stock hedge legs (one ticker per fetch).
- **Caching**: adapter TTL cache (1hr) + factory adapter pool, reused across CME/NASDAQ/NYSE/CBOE. For batch runs
  spanning <30 days a single fetch covers all dates (instrument sets change slowly).

---

## 8. Instrument ID Format (TradFi)

Canonical grammar (full spec lives in the shared instrument-ID doc): `VENUE:TYPE:PAYLOAD[@CHAIN]`. TradFi's
`chain` is always `"off-chain"`.

| Type     | Format                                            | Example                                                                             |
| -------- | ------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `FUTURE` | `VENUE:FUTURE:BASE-QUOTE-YYMMDD`                  | `CME:FUTURE:ES-USD-241225`                                                          |
| `OPTION` | `VENUE:OPTION:BASE-QUOTE-YYMMDD-STRIKE-CALL\|PUT` | `CME:OPTION:ES-USD-241225-4500-CALL`                                                |
| `EQUITY` | `VENUE:EQUITY:SYMBOL`                             | `NASDAQ:EQUITY:AAPL`, `NYSE:EQUITY:SPY`                                             |
| `INDEX`  | `VENUE:INDEX:SYMBOL`                              | illustrative only — see the open question in §4 for the Yahoo-sourced index entries |

**Single-leg dated-derivative codes are already fine, real, industry-standard, and terse — this is NOT a
canonicalization gap.** Confirmed 2026-07-08 while auditing instrument_id divergences workspace-wide: codes like
`CME:FUTURE:6AF0` are real exchange contract codes, not an uncleaned internal prefix (unlike, say, Kraken's raw
`FF_XBTUSD_260731` on the CeFi side) — no fix needed here.

**Multi-leg spreads/combos ARE a real divergence, not yet fixed.** See §11 — this is the one place TradFi's
instrument_id format genuinely needs work, and it is explicitly **pending confirmation**, not settled.

---

## 9. Session Metadata & Trading Hours

TradFi instruments carry trading-session metadata via the `exchange_calendars` library (holiday-aware):

- `is_trading_day`, `regular_open_utc` / `regular_close_utc` (DST-aware), `early_close_utc` (set only on
  shortened trading days).
- **CME**: near-24-hour trading, Sunday 5:00 PM CT (= Monday 22:00 UTC the previous day) through Friday 4:00 PM CT,
  with a daily 4:00-5:00 PM CT maintenance break.
- **CBOE VX futures**: regular US trading hours.
- **NASDAQ/NYSE**: 9:30 AM - 4:00 PM ET, DST-aware UTC conversion (14:30-21:00 UTC winter, 13:30-20:00 UTC summer).
- **KRX**: holiday calendar `XKRX` (Korea).
- **Yahoo FX**: continuous (24/7 forex market), daily OHLCV.
- **Yahoo indices**: daily close.

---

## 10. Corporate Actions Pipeline (yfinance, S&P 500)

A separate, production pipeline from the Databento/Yahoo-spot path above — fetches dividends, stock splits, and
earnings for the S&P 500 ticker universe via `yfinance`. Status: production, last tested 2026-02-07 (10 tickers,
100% success).

**Run it**:

```bash
# Full backfill (~17 min for 503 tickers)
python -m instruments_service.cli.main \
  --mode corporate_actions_production \
  --parallel-workers 2 --max-retries 3 --upload-to-gcs

# Test with specific tickers, no upload
python -m instruments_service.cli.main \
  --mode corporate_actions_production \
  --tickers AAPL MSFT GOOGL AMZN TSLA --parallel-workers 2
```

| Arg                  | Default         | Notes                                          |
| -------------------- | --------------- | ---------------------------------------------- |
| `--mode`             | required        | `corporate_actions_production`                 |
| `--tickers`          | `SP500_TICKERS` | space-separated override                       |
| `--parallel-workers` | 2               | raise → faster, but risks yfinance rate limits |
| `--max-retries`      | 3               | retry attempts per ticker                      |
| `--upload-to-gcs`    | `False`         | upload after processing                        |

**Data flow**: load/create metadata → get ticker list (`SP500_TICKERS`) → parallel fetch with a 100ms inter-request
delay (rate-limit guard) → save each ticker's data immediately (no data loss on a mid-run failure) → combine into
`by_date` partitions → update `ticker_registry.json` → generate `coverage_report.json` → optional GCS upload.

**Storage** (local staging under `data/temp/corporate_actions/`, mirrored to GCS on upload):

```
by_ticker/<TICKER>/{dividends,splits,earnings}.parquet   # raw, per-ticker
by_date/day=YYYY-MM-DD/{dividends,earnings}.parquet      # query-optimized partitions
metadata/{ticker_registry.json, coverage_report.json}
```

The original doc's GCS examples used a literal `gs://instruments-store-tradfi-{env}-{project_id}/...` string and
direct `gsutil`/`google.cloud.storage` calls — those predate this workspace's current storage rules (every bucket
must resolve via `resolve_bucket_name(...)`, GCS object ops via UTL `gcs_copy_object`/`gcs_describe_object`, never
inline `gs://` or subprocess `gsutil`). Treat the original examples as historical/illustrative, not a template for
new code.

**Data schema** (all three share `source="yfinance"`, `fetched_at`, optional `instrument_key`):

- **Dividends**: `ticker`, `ex_date`, `pay_date?`, `record_date?`, `declaration_date?`, `amount`, `dividend_type`,
  `currency`.
- **Splits**: `ticker`, `effective_date`, `split_ratio` (string, e.g. `"2:1"`), `split_factor` (float).
- **Earnings**: `ticker`, `earnings_date`, `eps_estimate?`, `reported_eps?`, `surprise_percent?`, `revenue?`,
  `fiscal_quarter?`, `fiscal_year?`.

**Benchmarks** (tested): 5 tickers/185 events ~12s; 10 tickers/460 events ~21s; full 503 tickers/~23,000
events/~17 min/~21,000 files created.

**Known gotchas**:

- yfinance rate limiting (`"Invalid Crumb"` or HTTP 401) → drop `--parallel-workers` to 1.
- A ticker missing `dividends.parquet` is normal — not every company pays dividends (e.g. META).
- `corporate_actions_backfill_handler.py`, `corporate_actions_update_handler.py`, and
  `generate_date_views_handler.py` are marked **legacy/deprecated** in-repo — `corporate_actions_production_handler.py`
  is the one maintained path; don't build on the deprecated handlers.

---

## 11. Known Non-Canonical Today: Multi-Leg Spreads & Combos

> **Status: current-state documented, target-state proposed, NOT implemented, NOT operator-confirmed as final.**
> This mirrors the same current-vs-target-state framing already used for the DeFi A_TOKEN/DEBT_TOKEN split
> decision elsewhere in this consolidation — do not treat anything in this section as shipped.

### Current real state (confirmed against `prod/catalog.parquet`, 2026-07-08)

**CBOE/VX calendar spreads bypass structured decomposition entirely.** Real rows reuse the single-leg `SPOT_PAIR`
type and separate legs with a whitespace-padded dash of raw exchange tickers:

```
CBOE:SPOT_PAIR:VX/F1:1:S - VX/G1:1:B          (2-leg — 34,017 real rows)
```

Confirmed volumes: 34,017 rows at 2 legs, 4,211 at 3 legs, 5 at 4 legs — **38,233 total**, with up to 9
colon-segments for the worst 3-leg butterfly-spread cases (a naive `split(":")` parser cannot make sense of these).

Likely mechanism (inferred from code in this pass, **not independently re-walked against GCS history** — flagging
as the honest limit of this investigation): `databento/adapter.py:767-773` (fix tag `G1.c`, 2026-06-25) now drops
every non-outright `XCBF.PITCH` row outright — `if dataset == "XCBF.PITCH" and inst_class not in ("F", "M"): return
None`. The same code comment cites this fix as targeting "4,216 mis-typed SPOT_PAIR rows," and shows the actual raw
symbol shape that triggered it: `"VX/F1:1:S - VX/G1:1:B"`. That fix stops **new** pollution at the
definition-fetch layer going forward, but it is a drop, not a retroactive backfill — it does not fix rows already
captured under the pre-fix behavior. The 38,233-row population the 2026-07-08 audit found in the production catalog
is plausibly that historical residue, though this session did not confirm the exact timeline row-by-row.

**CME calendar spreads DO get real structured decomposition** — this part already works:
`symbology.py::_parse_cme_calendar_spread_legs` (wired at `adapter.py:799-802`) is real, tested, live for
`GLBX.MDP3`. The combo's own `instrument_key` is `VENUE:COMBO:raw_symbol` (e.g. `CME:COMBO:ESM6-ESU6` — correct
type, no delimiter ambiguity), and its legs are a real structured `list[InstrumentLeg]` (`instrument_key`/`side`
`"BUY"`/`"SELL"`/`ratio` fields — a proper `BaseModel`, not a string), serialized to a separate JSON column at
write time (`process_write.py:184`), not encoded into the instrument_id.

Two real gaps remain even in this working CME path:

- **(b) Legs use the raw ticker, not the human product name.** `InstrumentLeg(instrument_key=f"{venue}:FUTURE:
{front}", ...)` yields `CME:FUTURE:ESM6`, not `CME:FUTURE:SP500` — even though the human-name registry
  (`_resolve_product_root()` / `unified_api_contracts.registry.tradfi_symbology`, e.g. `ES→SP500`, `GC→GOLD`,
  `VX→VIX`) already exists and is already used elsewhere for single-leg products.
- **(c) Legs redundantly repeat the venue.** Each leg's `instrument_key` (`CME:FUTURE:ESM6`) re-states `CME` even
  though the combo is already scoped to one venue at the top level (`CME:COMBO:...`) — the same "no redundant venue
  suffix" objection already settled elsewhere in the broader canonicalization decision (against a trailing
  `@VENUE` margin-marker).

### Proposed fix — pending operator confirmation, NOT implemented

An initial flat-string proposal (`VENUE:SPREAD:LEG-RATIO-SIDE;...` using raw exchange tickers) was **rejected by
the operator** for using raw exchange jargon instead of real human-readable names. The revised proposal reuses
real, already-existing infrastructure instead of inventing a new grammar:

1. Route CBOE/VX calendar spreads through the **same** `InstrumentLeg`/`InstrumentType.COMBO` pathway already
   proven for CME — decompose instead of dropping.
2. Apply `_resolve_product_root()` to every leg's symbol on **both** venues, so legs read as human names
   (`FUTURE:VIX`, not `FUTURE:VX/F1` or `FUTURE:VXF1`).
3. Drop the redundant per-leg `VENUE:` prefix in the existing CME builder too — a leg's `instrument_key` becomes
   `TYPE:SYMBOL` only (venue is implied by the combo's own top-level `VENUE:COMBO:...`).

This is **not implemented** and **not operator-confirmed as final** — it's recommended to become its own dedicated
fix plan under the `instruments_master` epic, independently shippable ahead of (and in parallel with) the broader
instrument_id canonicalization migration and this docs-consolidation effort.

### Adjacent, separately-tracked TradFi canonicalization findings (same 2026-07-08 audit — also not fixed today)

- **~62,650 combo rows workspace-wide with zero leg decomposition** — broader than just the CBOE/VX case above.
- **224 securities double-keyed** as both `EQUITY` and `SPOT_PAIR`.
- **92.7% of TradFi rows carry literal whitespace** as an uncontrolled sub-delimiter — operator-flagged as never
  acceptable, anywhere in the workspace.
- **IBKR's TradFi adapter builds `SYMBOL:RAW_CODE:CCY`** — wrong segment order (no venue token at all), and
  collapses stocks/bonds/FX-cash into one generic `SPOT_PAIR` type despite `canonical_id_builder.py` already
  defining distinct `EQUITY`/`BOND`/`CURRENCY` types for exactly this.

All of the above feed the same pending decision doc
(`unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`), tracked under the
`instruments_master` epic — none are fixed today, and none should be described as settled.

### Betfair — not a TradFi finding

The 2026-07-08 audit also flagged "Betfair stores raw `marketId`/`selectionId` with `/` instead of `:`." **That
finding belongs in the Sports doc, not here.** Confirmed by reading
`instruments_service/reference_data/adapters/sports/adapters/betfair.py` directly: its module docstring reads
"Betfair reference data adapter — sports event market catalogue," and it's registered as a sports-only venue key
(`"betfair"`) throughout `reference_data/router.py` and `reference_data/factory.py`'s sports adapter tables.
Betfair has no TradFi-adapter presence anywhere in this codebase.

---

## 12. Related Documentation

- Sports doc — the Betfair `marketId`/`selectionId` delimiter finding belongs there, not here (§11).
- CeFi doc, DeFi doc — sibling asset-group docs in this consolidation; share the same `VENUE:TYPE:PAYLOAD[@CHAIN]`
  canonical grammar and the same pending instrument_id canonicalization decision.
- Live mockup TradFi tab:
  [claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d](https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d).
