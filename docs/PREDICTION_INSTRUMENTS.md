# Prediction Instruments — Polymarket & Kalshi

> Renamed from `POLYMARKET_PREDICTION.md` (2026-07-08 docs consolidation — see
> [`instruments_service_docs_consolidation_2026_07_08.md`](../../unified-trading-pm/plans/active/instruments_service_docs_consolidation_2026_07_08.md)).
> Cross-links: the instrument-definitions drilldown mockup's **Prediction tab**
> (https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d) and the workspace's
> [instrument_id canonicalization decision doc](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> (finding 8) + the [canonical instrument_id audit](../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).
> `docs/specs/MVP_INSTRUMENTS.md` was checked for Prediction content while writing this doc and has **zero** real
> Polymarket/Kalshi coverage (confirmed stale for this asset group) — this doc does not inherit anything from it.

## Overview

Prediction markets are the one asset group where the MVP target isn't "capture everything available" — it's a
**cross-venue arbitrage overlap**. Polymarket and Kalshi both run structured markets on the same real-world questions
(will BTC close above $X today, will the Fed cut rates, will Arsenal beat Chelsea), and the trading thesis is spread
between the two venues' pricing of the _same_ question, not either venue standalone. That shapes almost everything
else in this doc: which venues are in scope, how instruments are classified, and why one particular label —
`canonical_question_group` — is deliberately shared verbatim across venues instead of being made venue-unique.

| Venue      | Access                                                                   | Auth                                                                               | Role                                          |
| ---------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | --------------------------------------------- |
| Polymarket | Gamma API (metadata) + CLOB API (order book/history) + Data API (trades) | None (all three are public reads)                                                  | Primary volume; on-chain (Polygon) settlement |
| Kalshi     | REST `/trade-api/v2`                                                     | Unauthenticated for the live snapshot; RSA-PSS request signing for `/historical/*` | CFTC-regulated counterpart                    |

## MVP universe (real, as of 2026-07-08)

Per `unified_api_contracts.canonical.crosscutting.mvp_scope.MVP_RULES["prediction"]` (`PredictionMvpRule`), the real,
current MVP definition is:

- **Venues**: `POLYMARKET` + `KALSHI` — both required. The prediction MVP is explicitly the **Kalshi↔Polymarket
  arbitrage overlap** (`arbitrage_price_dispersion`); a cross-venue spread cannot be quoted with only one leg. Kalshi
  was a "post-MVP TODO" until an operator decision (2026-06-27, decision #5) flipped it in-MVP.
- **market_groups**: `crypto`, `politics`, `sports` (the `PredictionMarketCategory` values the arb-overlap spans).
  `financial` is explicitly **excluded** from MVP scope.
- **data_types**: `trades` (CLOB fills), `market_lifecycle` (per-market lifecycle events), and
  `prediction_canonical_question_group` (the cluster/group-grain rollup — see "Cross-venue arbitrage mechanism"
  below).
- **sources**: unrestricted today (all Polymarket sources in scope) — there's an open `TODO(mvp-scope)` in the source
  to narrow this to `{polymarket_clob, polymarket_gamma_api}` once source tagging is confirmed downstream; not done
  yet.

**Real coverage, verified against a live `prod/catalog.parquet` pull this session** (bucket
`instruments-store-pred-prd-central-element-323112`, 2,486,092 rows):

|            | Rows          | Distinct real markets |
| ---------- | ------------- | --------------------- |
| POLYMARKET | 2,440,607     | 1,220,340             |
| KALSHI     | 45,485        | 22,760                |
| **Total**  | **2,486,092** | **1,242,992**         |

The row count is roughly double the distinct-market count because `trades` and `market_lifecycle` each contribute one
row per real market (1,242,992 rows apiece) — that 2x factor, not cross-venue duplication, is the majority of the gap
between "rows" and "distinct instrument_ids" reported in the canonicalization audit (see "Known gap" below for the
full reconciliation). A further 108 rows (`data_type=prediction_canonical_question_group`) are cluster/group-grain,
not per-market, rows.

Reminder (per the docs-consolidation operator ask): **MVP universe is not the same as everything captured**. The
adapters below can enumerate far more than the 3 in-scope `market_groups` — `financial`-category markets, and any
category outside crypto/politics/sports, are fetched and written but sit outside the arb-overlap MVP tag.

## Polymarket — three-API architecture

| API       | Base URL                   | Auth         | Purpose                                                                         |
| --------- | -------------------------- | ------------ | ------------------------------------------------------------------------------- |
| Gamma API | `gamma-api.polymarket.com` | None         | Market metadata, events, series (the reference-data adapter uses **only** this) |
| CLOB API  | `clob.polymarket.com`      | None (reads) | Order book, price history, and the historical-market enumeration path           |
| Data API  | `data-api.polymarket.com`  | None (reads) | Historical trades (market-data side, not reference data)                        |

Code: `instruments_service/reference_data/adapters/prediction/polymarket/` — split into `adapter.py` (entrypoints +
Gamma live listing), `parsing.py` (market → `InstrumentRecord`/`MarketLifecycle` mapping), `clob.py` (CLOB historical
enumeration + token-id registration), `markets.py`.

- **Live mode** (`date=None`, or `date==today`): paginates Gamma `/markets?closed=false&active=true`, ordered by
  `volume24hr` descending. Gamma doesn't reliably return `clobTokenIds`, so a same-date CLOB supplement run
  side-effects the per-outcome token-id side table used to populate the `clob_token_ids` column (needed for the
  Polymarket CLOB websocket subscription key) — its own return value is discarded, Gamma's universe is authoritative.
- **Historical mode** (`date < today`): uses the CLOB API directly — Gamma prunes old resolved markets, so
  `end_date_min`/`end_date_max` returns 0 rows for most past dates; CLOB has the full 863K+ market history
  unpruned.
- Both paths funnel through the **same** `_parse_market()` (in `parsing.py`) — there is one canonicalization code
  path for Polymarket, not a live/historical fork.

## Kalshi — REST + RSA-PSS signing, live/historical split

Code: `instruments_service/reference_data/adapters/prediction/kalshi.py`.

- **Live snapshot** (`/markets?status=open`) is reachable **unauthenticated**. The `/historical/*` tier (pre-cutoff
  markets) requires RSA-PSS request signing — credentials come from the `kalshi-api-credentials` Secret Manager JSON
  blob (`{"api_key_id"|"key_id", "private_key"}`), parsed in the constructor; a legacy/missing credential silently
  drops to unauthenticated (live-only) mode.
- The live↔historical boundary is resolved once via `/historical/cutoff` and cached; on any resolution failure the
  cutoff defaults to `date.min` so every date routes LIVE (a safe default — live is the unauthenticated path).
- **Series-scoped capture**: the plain `/markets?status=open` snapshot is dominated by `KXMVE*` multivariate parlay
  markets that consume the entire 2000-row page cap, starving out `KXBTCD`/`KXETHD`/`KXCPI`/`KX{LEAGUE}GAME`/etc. To
  compensate, the adapter separately walks `/series?category={Crypto,Economics,Financials,Sports,Politics}` →
  `/markets?series_ticker=...` for every series that classifies to a non-`OTHER` `CanonicalQuestionGroup`, merging
  in any markets missed by the flooded snapshot. Rate-limited to ~3 req/s with bounded 429 backoff-retry (a series is
  shard-skipped, not fatal, after 4 retries).
- `venue` **must** return `"KALSHI"` (uppercase) — a 2026-06-22 bug shipped the lowercase source name `"kalshi"`
  instead, which silently wrote the universe to `venue=kalshi` while the live MTDS reader searched `venue=KALSHI`,
  making the entire Kalshi universe invisible to live trading with no error. Fixed; documented in the adapter's
  `venue` property docstring as a durable warning against reintroducing it.
- A separate `available_from_datetime` flooring bug (fixed 2026-06-22): Kalshi's live snapshot stamps `open_time` as
  an intraday timestamp on the current day; the day-grain universe filter compares against midnight, so an
  afternoon-opening market failed the `available_from <= date_dt` check and the entire day's universe was dropped.
  Fixed by flooring `available_from_datetime` to the open date at day-grain (the precise timestamp is still carried
  on `MarketLifecycle` for tick-gating).

## Category routing & per-market instrument_id construction

Both adapters classify a market into a canonical shape, but **only Polymarket's classification result ever reaches a
canonical-looking field** — and even there, not the one that matters most. See "Known gap" below before assuming
either venue's `instrument_id` is structured.

| Category (Polymarket) | `base_asset` field shape (NOT `instrument_id` — see below)                                                                | Example                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Crypto up/down        | `POLYMARKET:UP_DOWN:{ASSET}:{TF}:{DATE}` (via `build_crypto_prediction_id`)                                               | `BTC:UP_DOWN:2026-03-22` (1D timeframe)                  |
| Macro up/down         | same shape, macro index instead of crypto asset                                                                           | `SPX:UP_DOWN:2026-03-22`                                 |
| Football sports       | `{LEAGUE}:{HOME}-v-{AWAY}:{DATE}:{MARKET_TYPE}` (via `build_prediction_instrument_id`, same builder Betfair/Odds API use) | `EPL:ARSENAL-CHELSEA:2025-26::HOME` (selection appended) |
| Everything else       | first 50 chars of the raw `question` text                                                                                 | —                                                        |

Kalshi has **no per-category id builder at all** — `instrument_key` is always the raw Kalshi ticker
(`KXBTC-21MAR26-T95000`, `KXMLBSPREAD-26JUN231940LADMIN-LAD3`), full stop. The adapter does run
`classify_kalshi_to_canonical_group()` per market, but that result feeds only the separate `MarketLifecycle` row
(`canonical_group` + `settlement_lag`), never the `instrument_id`.

### Polymarket sports — team/league resolution

Sports markets on Polymarket have generic `outcomes: ["Yes", "No"]`, so team names come from (priority order):
`event_title` (100% hit rate, "Arsenal vs. Chelsea") → `outcomes` (works for spreads where outcomes ARE team names) →
`question` text parsing. League resolution is via `series.slug` → `POLYMARKET_SERIES_TO_LEAGUE` (UAC
`external/polymarket/sports_mappings.py`). Only leagues in `POLYMARKET_PREDICTION_LEAGUES` (23 leagues, verified
2026-03-18 → 2026-03-26, re-checked unchanged this session) are kept — everything else (esports, cricket, rugby, F1,
UFC, NBA) is dropped or reclassified `prediction::other`:

EPL, ENG_CHAMPIONSHIP, LA_LIGA, SEGUNDA_DIVISION, BUNDESLIGA, BUNDESLIGA_2, SERIE_A, SERIE_B, LIGUE_1, LIGUE_2,
EREDIVISIE, PRIMEIRA_LIGA, SCOTTISH_PREMIERSHIP, SUPER_LIG, DANISH_SUPERLIGA, ELITESERIEN, MLS, LIGA_MX,
ARGENTINA_PRIMERA, BRASILEIRAO, A_LEAGUE, J1_LEAGUE, K_LEAGUE_1.

**Kalshi's sports coverage is a smaller, different set** — `external/kalshi/sports_mappings.py`'s
`KALSHI_SPORTS_TICKER_PREFIXES` covers only 6 football leagues (EPL, Bundesliga, La Liga, Serie A/`SEA`, Ligue 1/`FL1`,
Champions League) **plus NBA/NFL/MLB** — the 3 US leagues Polymarket's registry explicitly excludes. The two venues'
"sports" prediction coverage is not the same 23-league set on both sides; the real cross-venue overlap is narrower
than either venue's own list. (Kalshi's sports tickers are still raw passthrough, same as every other Kalshi
`instrument_id` — this mapping only affects `MarketLifecycle` classification, not the `instrument_id` shape.)

## Cross-venue arbitrage mechanism — `canonical_question_group`

The mechanism that actually lets a strategy compare Polymarket vs. Kalshi pricing for "the same" question is
`canonical_question_group` (a `CanonicalQuestionGroup` `StrEnum`,
`unified_api_contracts/canonical/domain/predictions/canonical_groups.py`), classified per-market by
`classify_polymarket_to_canonical_group()` / `classify_kalshi_to_canonical_group()` and surfaced via the dedicated
`data_type=prediction_canonical_question_group` cluster rows (part of the MVP `data_types` set above). Values look
like `BTC_UP_DOWN_DAILY`, `BNB_PRICE_RANGE_DAILY`, `CPI_PRINT_PER_MONTH`, `FED_RATE_DECISION_PER_FOMC` — venue-agnostic
labels for a recurring _class_ of question, not a specific dated market instance.

> **This is correct-as-designed, not a bug.** An earlier pass this session initially flagged 31 `canonical_question_group`
> values appearing verbatim on both venues as a "collision." That's wrong: `venue` is tracked as its own column
> (`CatalogRow.venue`, `unified_api_contracts/canonical/domain/instruments_catalog.py`), `canonical_question_group` is
> a _thematic cross-venue label_ by design (same docstring: `"canonical_question_group": e.g. "US_ELECTION_2024",
"CRYPTO_BTC_DAILY"`), and sharing the label across venues is the **intended mechanism** for cross-venue arb
> comparison — the same pattern sports fixtures use (a fixture_id is venue/bookmaker-independent; `venue` is the
> separate axis that varies). Nothing downstream should ever need `canonical_question_group` to be globally unique
> without also keying on `venue` — that combination is what identifies "this venue's instance of this recurring
> question."

This session additionally **confirmed against a live `prod/catalog.parquet` pull** exactly how few rows this
mechanism touches: only **108 of the 2,486,092 catalog rows** carry `data_type=prediction_canonical_question_group`
(46 Polymarket-only + 31 shared labels × 2 venues = 62 shared-label rows). The other 2,485,984 rows are ordinary
per-market `trades`/`market_lifecycle` rows with venue-opaque `instrument_id`s (see next section) — the shared-label
mechanism is a small, deliberate cluster-grain overlay on top of the per-market catalog, not something that touches
most rows.

### Per-instrument join — `cross_venue_mapping.build_cross_venue_mapping()`

`canonical_question_group` answers "do these two venues trade the same recurring _family_ of question?" It does
**not** answer "is THIS Kalshi market the same individual contract as THAT Polymarket market — same strike, same
settlement date?" — the join the `arbitrage_price_dispersion` strategy actually needs to quote a live spread. That
per-instrument join already exists:
`unified_api_contracts/canonical/domain/predictions/cross_venue_mapping.py::build_cross_venue_mapping()`. Per
family, sorted by how it derives the join key (all fields come from the real `InstrumentRecord`, never a title field
the schema doesn't carry):

- **Crypto / index / commodity price markets** (`UP_DOWN` / `PRICE_RANGE` / `PRICE_LEVEL`) join on
  `(underlying, bet_type, settlement_date, strike)` — `underlying` + `bet_type` come from the SAME
  `classify_*_to_canonical_group()` → `underlying_for_group()` / `bet_type_for_group()` pipeline that produces
  `canonical_question_group` (`unified_api_contracts/canonical/domain/predictions/two_axis.py`); `strike` is parsed
  from the Kalshi market ticker's `-T<n>`/`-B<n>` suffix or the Polymarket slug's numeric token (never the
  always-`None` `InstrumentRecord.strike` field).
- **Macro** (`PER_MONTH`: CPI / Fed / NFP …) joins on `(underlying, bet_type, release_month, threshold)` — month
  grain, not day, because the exact print day/time differs by venue.
- **Sports** reuses `SportsFixtureKey.pairing_key()` (`fixture_parsing.py`) — an order-independent
  `(league, sorted(teams), date)` key parsed from the Kalshi event ticker / title and the Polymarket event_title —
  requiring the human title, which the canonical `InstrumentRecord` does not carry (see the matcher's own docstring);
  callers supply it via an optional `titles: instrument_key -> title` map.
- **Everything else** (politics / geo / weather / culture) has no clean per-instrument cross-venue strike or fixture
  join → **no key, no row** (honest absence, never a false pair) — the `canonical_question_group` family label is
  the closest thing to a "canonical identity" these have.

The output, `PredictionMarketCrossVenueMapping`, carries both venues' native ids (`polymarket_condition_id` +
`kalshi_market_ticker`) plus the shared `underlying` / `strike` / `expiry_utc` / `canonical_event_id` for a matched
pair — this is the real, working "something we already have" for cross-venue Prediction matching. It runs on demand
over two full venue universes (not wired into the per-day write path or persisted onto the catalog today) — see
"Canonical identity model" below for how this session proposes closing that gap.

## Canonical identity model — root cause diagnosed + fixed, target scheme decided (2026-07-08)

> Supersedes this doc's prior "Known gap — per-market `instrument_id` is genuinely opaque" framing. Per the
> [canonicalization decision doc](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> finding 8 — the null-field question is now **answered** (not "genuinely opaque"), the conceptual-fit question the
> operator raised is **answered**, and a canonical scheme is **decided**. What's left is a scoped migration, tracked
> in a dedicated plan (see below), not an open investigation.

### 1. Root cause of the 100%-NULL `base_asset`/`raw_symbol` — a catalog-rollup bug, now fixed

Both adapters' `_parse_market()` genuinely populate `base_asset` and `raw_symbol` at `InstrumentRecord` construction
(Polymarket: `base_asset=<category id>, raw_symbol=slug`; Kalshi: `base_asset=series_ticker[:50],
raw_symbol=event_ticker`), and `process_write.py::_records_to_dataframe()` correctly serializes every
`InstrumentRecord` field (via `model_dump()`) into the per-day `instrument_availability/by_date/.../instruments.parquet`
snapshots — so the values genuinely exist in GCS. The bug was one level up:
`scripts/build_instrument_catalogue.py::build_prediction_catalogue_dataframe()` — Prediction's dedicated
multi-grain roll-up (cqg bundle + per-conditionId, distinct from the generic `build_catalogue_dataframe()` every
other asset group uses) — reads each per-day row into a `_PredLifecycle` accumulator that only ever tracked
`instrument_type` / `created` / `settled`; `raw_symbol` and `base_asset` were never read off the row, so the
`_emit()` helper that builds each `prod/catalog.parquet` row never included those keys, and `pd.DataFrame(rows,
columns=CATALOG_COLUMNS)` silently filled them with `NaN` for all 2,486,092 rows. The generic roll-up's
`_extract_meta()` (used by cefi/defi/tradfi) reads and carries these fields correctly — Prediction's dedicated
roll-up simply never mirrored that. **Fixed 2026-07-08**: `_PredLifecycle` / `_merge_lifecycle` / `_emit()` now
thread `raw_symbol` + `base_asset` through at the per-conditionId grain (a `canonical_question_group` bundle row
still leaves them `""` — honest absence, a family has no single per-market value). Next full catalogue regen
picks up real values for every `trades`/`market_lifecycle` row.

`underlying` is a **different** case — no adapter ever passes `underlying=` to `InstrumentRecord` at all (it's not
dropped downstream; it's genuinely never computed upstream). See §3 for why, and what's proposed.

### 2. Are `base_asset` / `underlying` / `raw_symbol` conceptually sensible for Prediction? — direct answer

- **`raw_symbol`: yes, and not vestigial.** It's real venue-native data (Kalshi's `event_ticker`, Polymarket's
  `slug`) — the same fields `cross_venue_mapping.py` already parses strikes/fixtures from, and the field the
  generic catalogue's UTL reader prefers for unique venue+id matching. This was purely a rollup bug (§1).
- **`base_asset`: real values, but a misleading field name for Prediction.** For Kalshi it's a genuine venue-native
  grouping key (`series_ticker`). For Polymarket it is **not** a base asset in the CeFi/DeFi sense — it's a
  synthesized display label whose shape varies by category: `BTC:UP_DOWN:2026-03-25` for crypto (asset-like),
  `EPL:ARSENAL-CHELSEA:...` for sports (instrument-id-like, not asset-like), and the **raw truncated question text**
  for everything else (`"other"` category) — not an asset at all. The values aren't garbage; the field is just
  reused for something the CeFi/DeFi/TradFi schema didn't design it to hold. Left as-is here (already correctly
  populated once §1 ships); not renamed, to avoid a schema-wide breaking change over a naming nitpick.
- **`underlying`: conceptually sensible for a REAL SUBSET, honestly absent for the rest — and there's already a
  closed-form way to compute it correctly.** `unified_api_contracts/canonical/domain/predictions/two_axis.py`'s
  `PredictionUnderlying` enum is a **comprehensive** decomposition — every `CanonicalQuestionGroup` maps to exactly
  one `PredictionUnderlying` member (`BTC`, `SPX`, `CPI`, `TRUMP`, `GEO_ISRAEL_IRAN`, `SPORTS_MLB`, …, `OTHER`).
  `cross_venue_mapping.py::_build_mapping()` already applies the right convention for the matched-pair schema:
  `underlying=None if is_sports else underlying.value` — sports fixtures don't have a single scalar "underlying" any
  more sensible than the fixture itself; crypto/macro/commodity price markets do (BTC, CPI, GOLD, …); politics/geo/
  entertainment don't (no natural subject asset) and correctly fall to `PredictionUnderlying.OTHER`. So: `underlying`
  is the right field, `None`/`OTHER` is the right honest value for the markets that don't have one, and the bug is
  simply that **no adapter ever calls this already-existing classification pipeline to populate it.**

### 3. The canonical scheme — one mechanism, reusing what exists (operator: "pick one … some things just can't be the same")

Three real market shapes, three real examples, ONE mechanism (not three parallel ones):

| Shape                           | Example                                                                     | Family axis                                           | Per-instance axis                                                                                                                                                            |
| ------------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pure prediction (no underlying) | "Will the Fed cut rates in September?"                                      | `canonical_question_group=FED_RATE_DECISION_PER_FOMC` | No cross-venue strike/fixture join exists → `canonical_event_id` honestly absent; `underlying=None` (`OTHER`)                                                                |
| Crypto/macro arb pair           | "BTC above $95k by June 24" (Polymarket) ↔ `KXBTCD-26JUN24-T95000` (Kalshi) | `canonical_question_group=BTC_UP_DOWN_DAILY`          | `underlying=BTC` (`PredictionUnderlying.BTC`, from the SAME classifier); `canonical_event_id` from `build_cross_venue_mapping()`'s `(BTC, UP_DOWN, 2026-06-24, strike)` join |
| Sports arb pair                 | Arsenal vs. Chelsea, EPL, 2026-03-22 (Polymarket ↔ Kalshi `KXEPLGAME-...`)  | `canonical_question_group=SPORTS_EPL_MATCH`           | `canonical_event_id` from `SportsFixtureKey.pairing_key()` (league + sorted teams + date); `underlying=None` (fixture identity, not a scalar asset)                          |

Decision — extend the **existing** mechanism, not a new one:

1. **`canonical_question_group`** stays the family/theme axis exactly as-is (no change).
2. **`underlying`**: populate at adapter-construction time (Polymarket `_parse_market()` / Kalshi `_parse_market()`)
   by calling the same `classify_*_to_canonical_group()` → `underlying_for_group()` pipeline already used for
   `MarketLifecycle.canonical_group`, applying `cross_venue_mapping._build_mapping()`'s existing
   `None if sports else value` rule. Zero new classification logic — this reuses code that already runs per-market
   today for the `MARKET_LIFECYCLE` data_type, just doesn't write its result onto `InstrumentRecord.underlying`.
3. **`canonical_instrument_id`** (an `InstrumentRecord` field that already exists — currently populated only by
   TradFi/Databento adapters, always `None` for Prediction) is the right home for the per-instance cross-venue join
   key: populate it with `PredictionMarketCrossVenueMapping.canonical_event_id` when `build_cross_venue_mapping()`
   finds a same-market pair, else leave it `None` (honest absence — no false pairs, matching the matcher's own
   design). This requires running the matcher over both venues' universes and merging results back — a real
   migration (adapters only see their own venue in isolation; the join needs both), NOT an adapter-local field
   population like `underlying`.
4. **Sports ↔ Sports-asset-group alignment (the operator's "sports arbitrage… canonical for sports that matches"
   ask)**: Prediction's sports fixture key (`SportsFixtureKey.pairing_key()`, team-name based) and the Sports asset
   group's own canonical fixture id (`build_fixture_id()` → `{LEAGUE}:{HOME}_v_{AWAY}:{YYYYMMDD}`,
   [`SPORTS_INSTRUMENTS.md`](./SPORTS_INSTRUMENTS.md)) carry the **same information** (league + two teams + date)
   but are **two independent implementations today** — they are not guaranteed to normalize team names identically.
   Polymarket's adapter already has an **unused** `_cross_reference_fixture()` method
   (`reference_data/adapters/prediction/polymarket/parsing.py`) that resolves a real API-Football `fixture_id` for a
   Polymarket sports market — it is defined but never called from `_parse_market()`/`_build_sports_id()`. Wiring
   that up (or reusing `build_fixture_id()`'s own team registry inside `fixture_parsing.py`) is the concrete way to
   make a Prediction sports market's identity byte-identical to the Sports asset group's fixture_id for the same
   real event, not just conceptually similar.

Items 3 and 4 are real migrations (cross-venue join wiring, adapter changes, tests) — tracked in
[`prediction_canonical_identity_migration_2026_07_08.md`](../../unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md),
not implemented in this pass. Item 2 (`underlying`) is scoped in the same plan since it touches the same two adapter
files and should ship together with the sports-null convention rather than piecemeal.

## GCS output & the bucket-naming split

```
gs://instruments-store-pred-{env}-{project}/
  prod/catalog.parquet                     # rolled-up per-market + per-cluster catalog (this doc's analysis above)
  instrument_availability/
    by_date/
      day={YYYY-MM-DD}/
        venue=POLYMARKET/
          instruments.json
        venue=KALSHI/
          instruments.json
```

**FIXED 2026-07-08** (was: confirmed, real, still-live infra bug — `instruments-store-pred-prd-central-element-323112`
exists with 33,122 blobs; `instruments-store-prediction-prd-central-element-323112` returned a 404). Two independent
code paths resolved "the prediction instruments bucket" to two different real strings:

1. **The live, correct path (unchanged)** — instruments-service's own special-cased flat-kind resolver
   (`instruments_service/engine/orchestrator/catalogue.py`, `scripts/build_instrument_catalogue.py`): the string kind
   `"instruments-store-prediction"` is passed to `resolve_bucket_name(kind="instruments-store-prediction")`, which
   resolves through the cloud-providers.yaml SSOT to the real, abbreviated bucket
   `instruments-store-pred-{env}-{project_id}`. This is the bucket that actually has data.
2. **The dead, broken path — fixed.** `unified_api_contracts/canonical/gcs_paths.py`'s
   `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` table, keyed by `(AssetGroup, BucketKind)`, templated
   `(AssetGroup.PREDICTION, BucketKind.INSTRUMENTS)` as the **unabbreviated**
   `instruments-store-prediction-{env}-{project_id}` — a bucket that has never existed. No consumer reached this
   facade for `PREDICTION` + `BucketKind.INSTRUMENTS` at the time this was found (so it was a dormant landmine, not
   an active 404), but it now resolves to the correct abbreviated `instruments-store-pred-{env}-{project_id}`.
   **`BucketKind.MARKET_DATA` was deliberately left AS THE UNABBREVIATED long form** —
   `market-data-tick-prediction-{env}-{project_id}` is a real, still-live legacy bucket mid-migration to the
   canonical `market-data-tick-pred-prd-{pid}` (`market-tick-data-service/scripts/migrate_prediction_to_pred_prd_v9.py`,
   `prediction_manifest_canonicalisation_2026_06_01.md` §C), and it IS actively read by
   `market-data-processing-service`'s `DependencyChecker.UPSTREAM_DEPS_BY_ASSET_GROUP["PREDICTION"]` — flipping it
   before that migration's `--drop-stale` step completes would point that dependency check at the less-complete
   bucket. Follow-up: `market-data-processing-service/tests/unit/test_dependency_checker_sports_prediction.py`
   (lines ~149, ~155) asserts the OLD unabbreviated `instruments-store-prediction-` INSTRUMENTS value — that repo is
   outside this fix's scope (owned separately this round) and will need its assertions updated once it bumps its
   `unified-api-contracts` pin.

## Key files

| File                                                       | Repo                  | Purpose                                                                                                                                 |
| ---------------------------------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `reference_data/adapters/prediction/polymarket/adapter.py` | instruments-service   | Gamma live listing + `get_instruments()` entrypoint                                                                                     |
| `reference_data/adapters/prediction/polymarket/parsing.py` | instruments-service   | Market → `InstrumentRecord`/`MarketLifecycle`, category/id builders                                                                     |
| `reference_data/adapters/prediction/polymarket/clob.py`    | instruments-service   | CLOB historical enumeration + clob_token_id registration                                                                                |
| `reference_data/adapters/prediction/kalshi.py`             | instruments-service   | Kalshi adapter — live/historical routing, RSA-PSS signing, series-scoped capture                                                        |
| `canonical/domain/prediction/prediction_mapping.py`        | unified-api-contracts | Legacy keyword classifier (`PredictionMarketMapper`) — category only reaches adapters; its own `canonical_id` is unused                 |
| `canonical/domain/predictions/canonical_groups.py`         | unified-api-contracts | `CanonicalQuestionGroup` enum + `CANONICAL_GROUP_METADATA` (settlement lags)                                                            |
| `canonical/domain/predictions/classifiers.py`              | unified-api-contracts | `classify_polymarket_to_canonical_group` / `classify_kalshi_to_canonical_group`                                                         |
| `canonical/domain/predictions/two_axis.py`                 | unified-api-contracts | `PredictionUnderlying` (Axis-1, comprehensive) / `PredictionBetType` (Axis-2) + `underlying_for_group()` / `bet_type_for_group()`       |
| `canonical/domain/predictions/cross_venue_mapping.py`      | unified-api-contracts | `build_cross_venue_mapping()` — the real per-instrument Kalshi↔Polymarket same-market matcher                                           |
| `canonical/domain/predictions/fixture_parsing.py`          | unified-api-contracts | `SportsFixtureKey` + `parse_kalshi_sports_fixture` / `parse_polymarket_sports_fixture` — the sports per-fixture join                    |
| `scripts/build_instrument_catalogue.py`                    | instruments-service   | `build_prediction_catalogue_dataframe()` — the prediction multi-grain catalogue roll-up (the `raw_symbol`/`base_asset` fix lives here)  |
| `canonical/domain/instruments_catalog.py`                  | unified-api-contracts | `CatalogRow` — the shared per-instrument catalog shape across all 5 asset groups                                                        |
| `canonical/crosscutting/mvp_scope.py`                      | unified-api-contracts | `PredictionMvpRule` — the real MVP venues/market_groups/data_types definition                                                           |
| `canonical/gcs_paths.py`                                   | unified-api-contracts | `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` — the INSTRUMENTS-kind template FIXED 2026-07-08 (was the dead `instruments-store-prediction-*`) |
| `external/polymarket/sports_mappings.py`                   | unified-api-contracts | Series→league, team→canonical, 23-league `POLYMARKET_PREDICTION_LEAGUES`                                                                |
| `external/kalshi/sports_mappings.py`                       | unified-api-contracts | 6-football-league + NBA/NFL/MLB Kalshi ticker-prefix registry                                                                           |
| `canonical/domain/sports/canonical_ids.py`                 | unified-api-contracts | `build_prediction_instrument_id()` (shared with Betfair/Odds API for sports)                                                            |
| `engine/orchestrator/catalogue.py`                         | instruments-service   | The live, correct prediction bucket-kind resolver                                                                                       |

## See also

- Mockup Prediction tab: https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d
- [`instrument_id_format_canonicalization_2026_07_08.md`](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md) — finding 8, now updated with the resolved root cause + canonical scheme decision documented above
- [`prediction_canonical_identity_migration_2026_07_08.md`](../../unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md) — the tracked plan for the remaining migration (adapter-level `underlying` population + `canonical_instrument_id` cross-venue wiring + sports fixture_id alignment)
- [`canonical_instrument_id_audit_2026_07_08.md`](../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md) — the full 7-layer audit this doc's findings are drawn from
- [`ADAPTER_ARCHITECTURE.md`](./ADAPTER_ARCHITECTURE.md) — general adapter code-structure conventions (not Prediction-specific)
- [`SPORTS_INSTRUMENTS.md`](./SPORTS_INSTRUMENTS.md) — the full 94-league sports MVP (a different, larger registry than Polymarket/Kalshi's prediction-market football coverage above)
