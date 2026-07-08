# Prediction Instruments — Polymarket & Kalshi

> Renamed from `POLYMARKET_PREDICTION.md` (2026-07-08 docs consolidation — see
> [`instruments_service_docs_consolidation_2026_07_08.md`](../../../unified-trading-pm/plans/active/instruments_service_docs_consolidation_2026_07_08.md)).
> Cross-links: the instrument-definitions drilldown mockup's **Prediction tab**
> (https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d) and the workspace's
> [instrument_id canonicalization decision doc](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> (finding 8) + the [canonical instrument_id audit](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).
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

## Known gap — per-market `instrument_id` is genuinely opaque (open investigation, not yet a target format)

> Per the [canonicalization decision doc](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> finding 8: **unlike every other divergence in that doc, this is not a "wrong delimiter" problem — the fields a
> canonical format would need are never populated in the production catalog in the first place.** No target format
> is proposed here; this section documents what's confirmed and what's still open.

Confirmed, both from the PM audit and re-verified directly against the same cached `prod/catalog.parquet` this
session:

- `base_asset`, `underlying`, and `raw_symbol` are **100% NULL** across all 2,486,092 rows, both venues — despite
  both adapters' Python code populating these fields at `InstrumentRecord` construction time (Polymarket's
  `_parse_market()` sets `base_asset=base_asset, raw_symbol=slug`; Kalshi's sets `base_asset=series_ticker[:50],
raw_symbol=event_ticker`). **Why the populated values never survive into the production catalog is not yet
  understood** — whether they're dropped during catalog aggregation, overwritten, or never actually reach the write
  path for this asset group is an open question, not something this doc resolves.
- The bulk of individual Polymarket markets (2,440,534 of 2,440,607 rows, 99.997%) carry the bare on-chain
  `condition_id` as `instrument_id` — a hash like
  `0x69c5f86ba99a9a19933a698a36809d17c9d0dae990aae72b84bf1df545fe0793`. This is not incidental: Polymarket's
  `_parse_market()` always sets `instrument_key=condition_id`. The nicer, category-shaped id it _does_ compute (the
  `base_asset` column in the table above) is a **different** field — it is never what ends up as `instrument_id`.
- Kalshi's individual markets (45,454 of 45,485 rows) carry the raw Kalshi ticker as `instrument_id`
  (`KXBTCD-26JUN2711-T53999.99`) — never a hash, but equally venue-native/opaque; Kalshi's adapter has no
  canonical-id builder at all (see above).
- **Newly confirmed this session** (resolves part of finding 8's open question): the ~50% "duplication rate"
  finding 8 flagged (1,243,069 unique of 2,486,092 rows) is mostly an artifact of this catalog file carrying **two
  rows per real market** (one `trades`, one `market_lifecycle` — both data*types share the same `instrument_id`) —
  not evidence of real cross-market or cross-venue id collision. The exact reconciliation: 1,242,992 real markets ×
  2 data_types = 2,485,984 rows, plus 108 `prediction_canonical_question_group` cluster rows (of which 31 labels are
  shared across venues, so they contribute only 77 \_additional* unique ids) = 1,242,992 + 77 = **1,243,069 unique
  ids** — an exact match to the real `nunique()` count. So the "short readable label shared verbatim across venues"
  behavior finding 8 flagged as unconfirmed is the `canonical_question_group` cluster mechanism described above, not
  a separate phenomenon — but this still doesn't explain the NULL-field question above, which remains genuinely open.
- **Correction to the PM doc's own file reference**: the todo asking to "investigate `prediction_mapping.py`'s real
  extraction logic" (finding 8) points at the wrong module. `unified_api_contracts/canonical/domain/prediction/prediction_mapping.py`
  (`PredictionMarketMapper`, `PRED:{category}:{hash12}` scheme) is a separate, largely-vestigial keyword classifier —
  Polymarket's `_parse_market()` does call it (`_MAPPER.map_market(...)`), but only reads `.category` off the
  result; the `canonical_id` field that module computes is discarded and never reaches any InstrumentRecord field.
  The actual `canonical_question_group` classification logic lives in
  `unified_api_contracts/canonical/domain/predictions/{canonical_groups,classifiers}.py`
  (`classify_polymarket_to_canonical_group` / `classify_kalshi_to_canonical_group`) — that's the module a future
  investigation into per-market `instrument_id` structure should actually start from.

**Still open / not decided** (per the operator's explicit instruction not to invent a settled fix here): what a
canonical per-market Prediction `instrument_id` should even contain once `base_asset`/`underlying`/`raw_symbol` are
understood; why those fields are dropped between adapter and catalog; whether any real downstream consumer treats
prediction `instrument_id` as globally unique without also keying on `venue`.

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

**Confirmed, real, still-live infra bug**: `instruments-store-pred-prd-central-element-323112` exists (33,122 blobs);
`instruments-store-prediction-prd-central-element-323112` returns a 404. This is a genuine bucket-naming split for
the `prediction` asset group specifically, not a naming-convention nitpick — two independent code paths resolve "the
prediction instruments bucket" to two different real strings:

1. **The live, correct path** — instruments-service's own special-cased flat-kind resolver
   (`instruments_service/engine/orchestrator/catalogue.py`, `scripts/build_instrument_catalogue.py`): the string kind
   `"instruments-store-prediction"` is passed to `resolve_bucket_name(kind="instruments-store-prediction")`, which
   resolves through the cloud-providers.yaml SSOT to the real, abbreviated bucket
   `instruments-store-pred-{env}-{project_id}`. This is the bucket that actually has data.
2. **A dead, broken path** — `unified_api_contracts/canonical/gcs_paths.py`'s
   `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` table, keyed by `(AssetGroup, BucketKind)`, templates
   `(AssetGroup.PREDICTION, BucketKind.INSTRUMENTS)` as the **unabbreviated**
   `instruments-store-prediction-{env}-{project_id}` — a bucket that has never existed. As of this session, no MTDS
   consumer actually calls this facade for `PREDICTION` + `BucketKind.INSTRUMENTS` (only the `MARKET_DATA` kind for
   prediction is reached from `gcs_paths.py` today), so this broken template is latent rather than actively serving
   a 404 in production right now — but it is a live landmine for the next consumer that reaches for the "obvious"
   per-asset-group facade instead of instruments-service's special-cased flat kind, and the audit's direct GCS check
   (confirming one bucket is real and the other 404s) is the authoritative signal here, independent of which code
   path is reached today.

## Key files

| File                                                       | Repo                  | Purpose                                                                                                                 |
| ---------------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `reference_data/adapters/prediction/polymarket/adapter.py` | instruments-service   | Gamma live listing + `get_instruments()` entrypoint                                                                     |
| `reference_data/adapters/prediction/polymarket/parsing.py` | instruments-service   | Market → `InstrumentRecord`/`MarketLifecycle`, category/id builders                                                     |
| `reference_data/adapters/prediction/polymarket/clob.py`    | instruments-service   | CLOB historical enumeration + clob_token_id registration                                                                |
| `reference_data/adapters/prediction/kalshi.py`             | instruments-service   | Kalshi adapter — live/historical routing, RSA-PSS signing, series-scoped capture                                        |
| `canonical/domain/prediction/prediction_mapping.py`        | unified-api-contracts | Legacy keyword classifier (`PredictionMarketMapper`) — category only reaches adapters; its own `canonical_id` is unused |
| `canonical/domain/predictions/canonical_groups.py`         | unified-api-contracts | `CanonicalQuestionGroup` enum + `CANONICAL_GROUP_METADATA` (settlement lags)                                            |
| `canonical/domain/predictions/classifiers.py`              | unified-api-contracts | `classify_polymarket_to_canonical_group` / `classify_kalshi_to_canonical_group`                                         |
| `canonical/domain/instruments_catalog.py`                  | unified-api-contracts | `CatalogRow` — the shared per-instrument catalog shape across all 5 asset groups                                        |
| `canonical/crosscutting/mvp_scope.py`                      | unified-api-contracts | `PredictionMvpRule` — the real MVP venues/market_groups/data_types definition                                           |
| `canonical/gcs_paths.py`                                   | unified-api-contracts | `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` — contains the broken `instruments-store-prediction-*` template                  |
| `external/polymarket/sports_mappings.py`                   | unified-api-contracts | Series→league, team→canonical, 23-league `POLYMARKET_PREDICTION_LEAGUES`                                                |
| `external/kalshi/sports_mappings.py`                       | unified-api-contracts | 6-football-league + NBA/NFL/MLB Kalshi ticker-prefix registry                                                           |
| `canonical/domain/sports/canonical_ids.py`                 | unified-api-contracts | `build_prediction_instrument_id()` (shared with Betfair/Odds API for sports)                                            |
| `engine/orchestrator/catalogue.py`                         | instruments-service   | The live, correct prediction bucket-kind resolver                                                                       |
| `scripts/build_instrument_catalogue.py`                    | instruments-service   | Rolls per-instrument metadata into `prod/catalog.parquet`                                                               |

## See also

- Mockup Prediction tab: https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d
- [`instrument_id_format_canonicalization_2026_07_08.md`](../../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md) — finding 8, the operator-decided scope for every OTHER asset group's canonicalization (Prediction is explicitly carved out pending the investigation above)
- [`canonical_instrument_id_audit_2026_07_08.md`](../../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md) — the full 7-layer audit this doc's findings are drawn from
- [`ADAPTER_ARCHITECTURE.md`](./ADAPTER_ARCHITECTURE.md) — general adapter code-structure conventions (not Prediction-specific)
- [`SPORTS_INSTRUMENTS.md`](./SPORTS_INSTRUMENTS.md) — the full 94-league sports MVP (a different, larger registry than Polymarket/Kalshi's prediction-market football coverage above)
