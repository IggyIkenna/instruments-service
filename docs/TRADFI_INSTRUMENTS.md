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
| CBOE (CFE) | Databento          | `XCBF.PITCH` | VX (VIX) futures — outrights + calendar spreads/butterflies (decomposed, see §11)        |
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
- **Open question RESOLVED 2026-07-08**: there is no separate "Yahoo adapter" file — all 3 Yahoo-sourced static
  registries are built as static `InstrumentRecord`s directly inside the same
  `instruments_service/reference_data/adapters/tradfi/databento/adapter.py`
  (`DatabentoReferenceDataAdapter`), merged into the same `get_instruments()` result list as the real
  Databento-fetched instruments (steps 3/3b/3c). Confirmed real `instrument_key` shapes, read directly from code:
  - `FX_SPOT_PAIRS` (`_create_fx_spot_records`): `FX:SPOT_PAIR:{BASE}-{QUOTE}`, e.g. `FX:SPOT_PAIR:EUR-USD`.
  - `KRX_EQUITIES` (`_create_krx_equity_records`): `KRX:EQUITY:{symbol}` where `symbol` is the bare KRX numeric
    code (not the `.KS` Yahoo ticker), e.g. `KRX:EQUITY:005930`.
  - `YAHOO_INDICES` (`_create_yahoo_index_records`): `{venue}:INDEX:{base_asset}-USD`, e.g. `CBOE:INDEX:VIX-USD`
    (quote is hardcoded `"USD"` regardless of the index's real denomination).

---

## 5. Equity / ETF Ticker Universe

Equities and ETFs are **not** in the Databento registry above — they come from a separate file,
`unified_api_contracts/registry/tradfi_ticker_universe.py`, per that file's own docstring ("Equities/ETFs come from
the tradfi_ticker_universe.py ... instead"). Real counts (2026-07-08):

| List                       | Count                                                                                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SP500_TICKERS`            | 200 as of 2026-07-08 morning — expanded to the full current S&P 500 membership same day (see below); check the live registry for the current count, don't repeat 200 as current fact |
| `NASDAQ_TICKERS`           | 101                                                                                                                                                                                  |
| `ETF_TICKERS`              | 78                                                                                                                                                                                   |
| `NYSE_TRADFI_PERP_TICKERS` | 18                                                                                                                                                                                   |

`NASDAQ_TICKERS` (a subset of the broader universe) drives equity venue routing — see §7.

**RESOLVED 2026-07-08**: the old `MVP_INSTRUMENTS.md`'s claimed **~603 total S&P 500 tickers** (current +
historical 2020-2025 constituents including delisted names like ATVI/CERN/XLNX/FRC) is explicitly NOT the target —
operator decision was to restore the full CURRENT S&P 500 membership only (not the historical/delisted angle),
since the workspace only needs OHLCV for these tickers and a bigger current universe also widens the pool of real
cash-equity candidates for a future equity-basis-arb strategy against Binance/OKX equity-perps. `SP500_TICKERS`
was expanded from 200 to the full current membership in `unified-api-contracts` the same day — see that repo's
shipped commit for the exact before/after count and source citation.

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
  - **`XCBF.PITCH` (CBOE/VX)**: class "S" → reclassified `InstrumentType.COMBO`, legs parsed via
    `_parse_cboe_spread_legs` (2026-07-08 fix, superseding the 2026-06-25 G1.c drop — see §11). Real Databento
    `instrument_class` semantics, confirmed via the `databento` SDK's own `InstrumentClass` enum, are ALSO now
    documented accurately: `"S"` is `FUTURE_SPREAD`, never "FX Spot / Equity spot" as an earlier internal comment
    claimed.
  - **`DBEQ.BASIC` (equities)**: class "S" → reclassified `EQUITY` (not `SPOT_PAIR`) — a 2026-06-25 fix (tag
    `G1.d`) for 318 previously mis-typed NASDAQ/NYSE equity rows. **Separately, class "K" (the real Databento
    `STOCK` code — confirmed via a live definition-schema call for AAPL/SPY/IBIT, 2026-07-08) was ALSO fixed**:
    it previously fell through to the default `SPOT_PAIR` (mislabeled "Forex spot" in the class-map comment — FX
    spot pairs actually come from a wholly separate static builder, never this class map), which was the real,
    live, ongoing root cause of 100% of fresh NASDAQ/NYSE single-stock captures landing as `SPOT_PAIR`. See §11.
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

| Type     | Format                                               | Example                                                            |
| -------- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| `FUTURE` | `VENUE:FUTURE:PRODUCT_ROOT@LIN-YYYYMMDD`             | `CME:FUTURE:GOLD@LIN-20260821`                                     |
| `OPTION` | `VENUE:OPTION:PRODUCT_ROOT@LIN-YYYYMMDD-STRIKE-C\|P` | `CME:OPTION:NASDAQ100@LIN-20260918-32000-C`                        |
| `EQUITY` | `VENUE:EQUITY:SYMBOL`                                | `NASDAQ:EQUITY:AAPL`, `NYSE:EQUITY:SPY`                            |
| `INDEX`  | `VENUE:INDEX:SYMBOL`                                 | see §4 for the resolved Yahoo-sourced index `instrument_key` shape |

**Single-leg dated-derivative `@LIN`-`YYYYMMDD` extension — SHIPPED 2026-07-09 (write path + historical
migration, MTDS repo).** The 2026-07-08 assessment above (real exchange contract codes like `CME:FUTURE:6AF0`
are already fine, no fix needed) was the original call; the operator reversed it 2026-07-09
(`instrument_id_format_canonicalization_2026_07_08.md` finding 1, "What this is NOT"): "I'd rather adjust
tradfi... that's the whole point of cross-AG normalisation" — TradFi single-leg dated derivatives (`FUTURE`/
`OPTION`) are now IN SCOPE for the same target already shipped for CeFi. **Implemented** (this is the
market-tick-data-service raw-tick write path this doc's §8 table describes — a separate surface from the
instruments-service reference-data catalog §11 covers):

- **Write-path fix** (`market-tick-data-service/market_tick_data_service/market_interface/adapters/tradfi/
databento_enrichment.py::_classify_row` for Databento CME/CBOE/DBEQ; `tradfi_shared.py::
derive_tradfi_row_instrument_id` for IBKR, symmetric code-only fix — IBKR has zero real historical rows to
  migrate, never wired into a live/backfill entrypoint): the raw exchange product root (e.g. `GC`, `NQ`, `VX`)
  is translated to its human-canonical name (`GOLD`, `NASDAQ100`, `VIX`) via the UAC registry
  `unified_api_contracts.registry.EXCHANGE_CODE_TO_NAME` (the SAME registry `instruments-service`'s
  `_resolve_product_root()` already uses for §11's COMBO legs — one shared human-name mapping, not two), and
  `build_instrument_id(..., margin_marker="LIN")` stamps the settlement suffix. Every real TradFi future/option
  in this system is USD-settled — no inverse-margined TradFi product exists — so the marker is always `@LIN`,
  never `@INV` (confirmed, not assumed: the CME/CBOE/ICE product universe in `tradfi_symbology.py` has no
  crypto/BTC-margined instrument). A companion UAC fix
  (`unified_api_contracts/external/databento/databento_classifier.py::_derive_cme_option_underlying`) added the
  6 missing CME commodity-option root mappings (silver/palladium/platinum/copper/heating-oil/gasoline options —
  `SO`/`PAO`/`PO`/`HXE`/`OH`/`OB`) that previously fell through to the raw option code unresolved. The
  `underlying=` GCS partition segment (`futures_chain`/`options_chain` bundle files) follows the same DataFrame
  column automatically — no separate path-builder fix needed, per `partitioned_writer.py`'s existing
  per-underlying grouping.
- **Historical migration**: `market-tick-data-service/scripts/migrate_tradfi_single_leg_product_root_lin_2026_07_09.py`
  (backup-first — server-side copy to `_migration_backup_2026_07_09/` before every rewrite — real GCS
  concurrency via `ThreadPoolExecutor`, dry-run-by-default, idempotent/resumable). Real scope, enumerated from
  the existing `availability_index.parquet` manifest (single-walk discipline — no fresh corpus walk, real
  `row_count>0` captured entries only): **158,812 real shard objects / ~1.19B rows** across CME
  `futures_chain`+`options_chain` and CBOE `futures_chain` (`ohlcv_1s`/`ohlcv_1m` data_types only — see the
  excluded-scope note below; ICE dropped out entirely, 0 real captured rows, consistent with §1's ICE-purge).
  **Status as of 2026-07-09 14:31 UTC (real, in-progress — not yet complete)**: launched at real production
  concurrency (48 workers); verified correct end-to-end on live samples across CME (6S→CHF, ZS→SOYBEAN,
  6N→NZD, 6M→MXN, 6B→GBP, GC→GOLD, NQ→NASDAQ100 — real before/after GCS reads, backup+new-path+content all
  confirmed) and CBOE (VX→VIX, real GCS listing confirmed post-migration); 9,000+ real objects processed (real
  moves + in-place rewrites + genuinely-absent-shard skips), running continuously in the background at
  ~6-13 real objects/sec (throughput varies with real shared-host GCS contention from concurrent workspace
  agents) — real ETA at last measurement ~6-7 hours from launch. The script is idempotent (a shard already in
  the target `@LIN` shape is a no-op), so it can be safely re-run/resumed to reach full completion:
  `python scripts/migrate_tradfi_single_leg_product_root_lin_2026_07_09.py --worklist <worklist.parquet> --workers 48 --apply --stamp <new-stamp>`
  (worklist = the same manifest-derived scope query documented in the script's own module docstring), followed
  by `--skip-gcs` (manifest-only rewrite) once the GCS pass reaches 100%.
  **Real gap found and deliberately excluded, not silently dropped**: CME's `data_type=options_chain` axis
  (120,946 manifest entries, ~187.5M rows) uses a DIFFERENT, unverified legacy per-contract/per-spread flat file
  layout (confirmed live via GCS listing: filenames like `CC__FMH0025!.parquet` directly under
  `data_type=options_chain/`, no `underlying=X/` subdirectory at all; the manifest's own `underlying` column for
  these rows holds per-contract keys like `ESU4_C5675`, not a product root) — this is NOT the
  `underlying={ROOT}/ticks.parquet` bundled scheme the write-path fix and this migration target. Touching it
  without first understanding its real physical layout risked silent data loss at ~187M-row scale, so it was
  excluded from this pass rather than guessed at; flagged here as a real, open follow-up (a dedicated
  investigation + fix plan, scope TBD) rather than deferred silently.

**Multi-leg spreads/combos — the CODE fix is FIXED 2026-07-08/09; the historical catalog migration is real but
NOT yet durable.** See §11 for the full picture, including why the historical migration needs re-running after
every catalog rollup cycle until the upstream by_date corpus is also migrated.

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

## 11. Multi-Leg Spreads & Combos — code FIXED 2026-07-08/09; historical catalog migration NOT durable yet

> **Status: the CODE fix is real, tested, and shipped — every future capture is correct going forward.**
> CBOE/VX spreads now decompose through the same `InstrumentLeg`/`InstrumentType.COMBO` pathway already
> proven for CME; the CME path's 2 pre-existing gaps (raw-ticker legs, redundant per-leg venue) are also
> closed; a 1-4 leg hard cap was added (operator spec, 2026-07-09 — 5+-leg combos are dropped and logged,
> never captured). **The historical `prod/catalog.parquet` in-place migration is a separate, NOT-yet-durable
> story — see "Historical migration: real, but NOT durable yet" below before treating either affected row
> count as current.** Plan:
> `unified-trading-pm/plans/active/canonical_id_p1_tradfi_combo_leg_canonicalization_2026_07_08.md`.

### Real pre-fix state (confirmed against `prod/catalog.parquet`, 2026-07-08)

CBOE/VX calendar spreads bypassed structured decomposition entirely — real rows reused the single-leg `SPOT_PAIR`
type and separated legs with a whitespace-padded dash of raw exchange tickers, e.g.
`CBOE:SPOT_PAIR:VX/F1:1:S - VX/G1:1:B`.

**Real, measured volume — corrected**: the originating finding cited "34,017 (2-leg) + 4,211 (3-leg) + 5 (4-leg) =
38,233" rows; a direct read of the real catalog found this was wrong. The actual population is **4,211 rows at 2
legs + 5 rows at 3 legs = 4,216 total** (0 four-leg rows exist) — exactly matching the `databento/adapter.py` G1.c
fix comment's own "4,216 mis-typed SPOT_PAIR rows" count. The larger number was not reproducible against the real
data; treat the corrected 4,216 figure as authoritative.

Mechanism: `databento/adapter.py`'s G1.c fix (2026-06-25) dropped every non-outright `XCBF.PITCH` row
(`inst_class not in ("F", "M") → return None`) instead of decomposing it — a workaround for the mis-typed-SPOT_PAIR
pollution, not the real target state.

### The fix (shipped)

1. **`symbology.py::_parse_cboe_spread_legs`** (new) parses CBOE's real raw_symbol shape — `TICKER:RATIO:SIDE` per
   leg, joined by `" - "` (e.g. `VX/F1:1:S - VX/G1:1:B`, or a 3-leg butterfly `VX/H1:1:B - VX/J1:2:S - VX/K1:1:B`)
   — into real `list[InstrumentLeg]` objects. Unlike CME's format, CBOE's raw_symbol carries an explicit side/ratio
   per leg, so no leg-count special-casing is needed.
2. **`symbology.py::_build_leg_key`** (new, shared by both the CME and CBOE parsers) builds every leg's
   `instrument_key` as `TYPE:SYMBOL` — human product name via `_resolve_product_root()` (`FUTURE:VIX`,
   `FUTURE:SP500`, not `FUTURE:VX/F1` or `FUTURE:ESM6`), no redundant per-leg `VENUE:` prefix (the combo's own
   top-level `VENUE:COMBO:...` already carries it once). This also fixes the 2 pre-existing gaps in the CME path.
   **Deliberate deviation from "route through `unified_api_contracts...canonical_id_builder.build_leg()`"**: the
   plan's "minimize the change surface" instruction asked for this, but UAC's real `build_leg()` unconditionally
   embeds `venue` (via `build_instrument_id` → `_venue_token`, which raises if venue is empty) — there is no way to
   call it and get a venue-less `TYPE:SYMBOL` leg key, which directly conflicts with the just-settled "drop the
   redundant per-leg venue" decision this same fix implements. Extending UAC's `build_leg()` with an opt-in
   venue-omission mode would be the fully-DRY answer, but that is a cross-repo change to `unified-api-contracts`
   (its own quality gates + version bump + dependency re-sync) outside this fix's repo scope — `_build_leg_key`
   stays a small, real, single, shared local helper instead (still "one implementation, not two independently-
   evolving ones" within this repo) pending that as a separate, explicitly-tracked follow-up.
3. **`symbology.py::_sanitize_symbol_for_key`** (new) replaces whitespace in the top-level combo's raw-symbol
   payload with a single `-` (e.g. `CBOE:COMBO:VX/F1:1:S-VX/G1:1:B`, not `...S - VX/G1...` with the whitespace-
   padded dash) — applied to EVERY Databento-sourced `instrument_key`, not just combos (see the whitespace finding
   below).
4. **`_FUTURES_DATASETS`** now includes `XCBF.PITCH` alongside `GLBX.MDP3`; `_SPREAD_LEG_PARSERS` dispatches the
   right per-dataset parser. The top-level `instrument_type` is `COMBO` (not `SPOT_PAIR`) for both venues.
   4b. **1-4 leg hard cap** (operator spec, 2026-07-09): `_parse_cboe_spread_legs` drops (does not truncate) any
   combo with 5+ real legs, logging the real leg count — no real 5-leg row exists in production today (see the
   real leg-count distribution below), but the parser must never silently truncate one if it ever appears.
   `_parse_cme_calendar_spread_legs` is inherently always exactly 2 legs, already within the cap.
   4c. **Signed weight, no new stored field** (operator spec, 2026-07-09: "the weights tell us that anyway" — no
   parallel strategy-name taxonomy): a leg's `side` (`"BUY"`/`"SELL"`) + `ratio` (positive int) together already
   give a consumer a directly-usable signed weight via the documented convention
   `signed_ratio = ratio if side == "BUY" else -ratio` — no new field was added to `InstrumentLeg` since the
   operator explicitly allowed "a documented convention derived from side+ratio" as a valid implementation choice.
5. **Historical migration: real, but NOT durable yet** (`scripts/canonicalize_cboe_vx_combo_catalog_2026_07_08.py`).
   `--apply` WAS run 2026-07-08 (real: the pre-migration snapshot blob
   `prod/snapshots/pre_cboe_vx_combo_canon_2026_07_08.parquet` exists in GCS, created 2026-07-08 18:50:17 UTC) and
   correctly rewrote the then-real 4,216 `SPOT_PAIR`→`COMBO` rows in place. **But `prod/catalog.parquet` is a
   self-refreshing roll-up** (`scripts/build_instrument_catalogue.py`, "derive the lifecycle instrument catalogue
   from the per-date definitions... makes the catalogue a derivative of the maintained per-date definitions... on a
   recurring basis") — it was regenerated FROM SCRATCH from the `instrument_availability/by_date/day=*/venue=CBOE/`
   per-day corpus at **2026-07-09 01:03:00 UTC** (confirmed via `gsutil stat`, ~6h after the migration), and that
   per-day corpus was never itself rewritten (explicitly out of scope, see below) — so the regenerated catalog
   re-derived `SPOT_PAIR` for every historical date the rollup still carries. **Re-verified 2026-07-09 (this fix's
   completion pass)**: a fresh dry-run (stable across 2 back-to-back runs) shows **91 real CBOE `SPOT_PAIR`
   spread rows again** — a direct row-level diff against the 2026-07-08 pre-migration snapshot confirms these are
   NOT new captures (0 raw_symbols not already in the snapshot; all 91 are a subset of the original 4,216 that the
   rollup's own retention/window logic still carries). **The durable fix is the CODE change above, not the
   catalog-level migration on its own**: once deployed, every FRESH by_date capture is correct, so future rollup
   regenerations stop reintroducing new pollution — but any already-captured historical date keeps re-deriving
   `SPOT_PAIR` on every rollup cycle until that historical per-day corpus is also rewritten (see the honest scope
   limit below; still deferred). Practically: re-running `--apply` is safe (small, gated, sub-5-second single-file
   operation against the current 91-row population) but should be treated as **routine tidying after each rollup
   cycle, not a one-time fix**, until the per-day corpus itself is migrated.
   **Honest scope limit**: `prod/catalog.parquet`'s schema (`InstrumentCatalogEntry`) carries no `legs` column at
   all, so this migration only corrects `instrument_type`/`instrument_id` there — the full per-leg structured data
   (with human names, no venue prefix) only exists for instruments captured by the FIXED code going forward. A full
   rewrite of the historical `instrument_availability/by_date/day=*/venue=CBOE/instruments.parquet` per-day
   snapshot corpus (thousands of files, 2015-present) was explicitly NOT attempted in this pass — gated by this
   workspace's single-walk discipline (a new whole-corpus GCS walk is review-blocking) and left as a separate,
   larger follow-up; this is now the real blocker to a durable historical fix, not a nice-to-have.

### Adjacent findings from the same 2026-07-08 audit — also FIXED

- **92.7% of TradFi rows carried literal whitespace as an uncontrolled sub-delimiter.** Root-caused: NOT primarily
  the combo/spread code above (only ~2% of the affected rows) — the dominant source (~98%, 994,808 of 1,015,929
  affected rows) was CME/ICE `OPTION` raw_symbols, which Databento natively space-separates (e.g. `"OBZ4 C15500"`).
  A smaller population of `FUTURE`/`SPOT_PAIR` rows also carried embedded whitespace (e.g. a `CL:SA 02M M6`
  strategy-future raw_symbol, `BRK B` for Berkshire's Class B shares). Fixed via `_sanitize_symbol_for_key()`
  (symbology.py), applied at every `instrument_key` construction site in the Databento adapter — `raw_symbol`
  itself is untouched (stays the verbatim vendor code); only the canonical key is sanitized. Verified against
  every one of the 1,095,837 real unique `raw_symbol` values in `prod/catalog.parquet`: 0 remain whitespace-bearing
  after sanitization.
- **224 securities double-keyed as both `EQUITY` and `SPOT_PAIR` — real root cause found, much larger than
  reported.** The real Databento `instrument_class` for a plain stock is `"K"` (`InstrumentClass.STOCK`, confirmed
  via the `databento` SDK's own enum AND a live definition-schema call for AAPL/SPY/IBIT on `DBEQ.BASIC`,
  2026-07-08). `_CLASS_TO_TYPE["K"]` mapped to `SPOT_PAIR` (mislabeled "Forex spot" in the comment — FX spot pairs
  come from a wholly separate static builder, never this class map). This was a real, LIVE, ONGOING bug: the
  2026-07-08 `instrument_availability/by_date/day=2026-07-08/venue=NASDAQ/instruments.parquet` snapshot showed
  100/100 rows typed `SPOT_PAIR`, zero `EQUITY` — far broader than the "224 double-keyed" framing suggested (that
  number undercounted by only measuring the historical EQUITY/SPOT_PAIR overlap, not the full affected
  population). Fixed: `_CLASS_TO_TYPE["K"] = InstrumentType.EQUITY`; ETFs are unaffected (the downstream
  `raw_symbol in KNOWN_ETFS` override still reclassifies them `ETF`). **Historical migration — same
  not-yet-durable caveat as the CBOE/VX combo migration above**
  (`scripts/canonicalize_dbeq_stock_class_catalog_2026_07_08.py`): `--apply` WAS run 2026-07-08, correctly
  rewriting the then-real 318 NASDAQ/NYSE `SPOT_PAIR` rows in `prod/catalog.parquet` in place (317 → `EQUITY`, 1
  `IBIT` → `ETF`). Re-verified 2026-07-09 (this fix's completion pass, after the same
  `build_instrument_catalogue.py` rollup regeneration described above): a fresh dry-run shows **312 real
  NASDAQ/NYSE `SPOT_PAIR` rows again** (stable across 2 back-to-back runs), 0 of which currently match
  `KNOWN_ETFS` — same root cause (the rollup re-derives from the not-yet-migrated per-day corpus on every
  regeneration). The 11 genuine FX `SPOT_PAIR` rows (`venue=FX`, static Yahoo builder) are untouched by this
  migration's `classify()` predicate (venue-filtered to NASDAQ/NYSE only) regardless. Honest gap noted, not
  fixed: `ETHA` (the other curated BTC/ETH spot-ETF ticker) is not currently in the `KNOWN_ETFS` registry, so it
  reclassifies to `EQUITY` rather than `ETF` — a separate, smaller, non-blocking registry-completeness gap.
- **IBKR's adapter built `SYMBOL:RAW_CODE:CCY`** (wrong segment order, no venue token, and collapsed
  stocks/bonds/FX-cash into one generic `SPOT_PAIR` type). Fixed in
  `instruments_service/reference_data/adapters/tradfi/ibkr.py`: `instrument_key` is now
  `VENUE:TYPE:SYMBOL` (`IBKR:EQUITY:AAPL`, `IBKR:FUTURE:ES`), using the canonical `InstrumentType` (not IBKR's raw
  `secType` code); `_SEC_TYPE_MAP` now maps `STK→EQUITY`, `BOND→BOND`, `CASH→CURRENCY` (previously all 3 collapsed
  to `SPOT_PAIR`, despite `canonical_id_builder.py` already defining distinct `EQUITY`/`BOND`/`CURRENCY` types for
  exactly this). `CASH` (FX) contracts fold the quote currency into the payload (`IBKR:CURRENCY:EUR-USD`, matching
  this workspace's FX convention) rather than losing pair identity to a bare base-currency symbol.

All of the above are tracked from
`unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md` (finding 7, and this
doc's prior "adjacent findings" list), under the `instruments_master` epic.

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
