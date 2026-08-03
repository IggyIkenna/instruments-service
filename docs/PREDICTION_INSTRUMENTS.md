# Prediction Instruments — Polymarket & Kalshi

> Cross-links: the instrument-definitions drilldown mockup's **Prediction tab**
> (https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d) and the workspace's
> [instrument_id canonicalization decision doc](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
> (finding 8) + the [canonical instrument_id audit](../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md).
> `docs/specs/MVP_INSTRUMENTS.md` has no Polymarket/Kalshi coverage — this is the sole spec for the Prediction asset
> group.

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
  arbitrage overlap** (`arbitrage_price_dispersion`); a cross-venue spread cannot be quoted with only one leg.
- **market_groups**: `crypto`, `politics`, `sports` (the `PredictionMarketCategory` values the arb-overlap spans).
  `financial` is explicitly **excluded** from MVP scope.
- **data_types**: `trades` (CLOB fills), `market_lifecycle` (per-market lifecycle events), and
  `prediction_canonical_question_group` (the cluster/group-grain rollup — see "Cross-venue arbitrage mechanism"
  below).
- **sources**: unrestricted today (all Polymarket sources in scope) — there's an open `TODO(mvp-scope)` in the source
  to narrow this to `{polymarket_clob, polymarket_gamma_api}` once source tagging is confirmed downstream; not done
  yet.

**Real coverage** — measured 2026-07-17 against `prod/catalog.parquet` (bucket
`instruments-store-pred-prd-central-element-323112`, 184,462,581 bytes / ~184.5 MB, 24 columns, object updated
2026-07-17T01:02:27Z), **2,673,230 rows**:

|            | Rows          | Distinct real markets |
| ---------- | ------------- | --------------------- |
| POLYMARKET | 2,590,757     | 1,295,342             |
| KALSHI     | 82,473        | 41,217                |
| **Total**  | **2,673,230** | **1,336,559**         |

> **Numbers are date-stamped, not eternal.** These are a real measurement of the live object as of **2026-07-17**; the
> catalogue is regenerated, so re-measure rather than trusting this table verbatim. (It supersedes an earlier
> 2,486,092-row / 1,242,992-market reading — that was a real measurement of an older object, not an error.)

The row count is roughly double the distinct-market count because `trades` and `market_lifecycle` each contribute one
row per real market (1,336,559 rows apiece — measured `data_type` distribution) — that 2x factor, not cross-venue
duplication, is the majority of the gap between "rows" and "distinct instrument_ids" reported in the canonicalization
audit (see "Canonical identity model" below for the full reconciliation). A further **112 rows**
(`data_type=prediction_canonical_question_group`) are cluster/group-grain, not per-market, rows.

Note: **MVP universe is not the same as everything captured**. The adapters below can enumerate far more than the 3
in-scope `market_groups` — `financial`-category markets, and any category outside crypto/politics/sports, are fetched
and written but sit outside the arb-overlap MVP tag.

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
- `venue` **must** return `"KALSHI"` (uppercase, not the lowercase source name `"kalshi"`) — the live MTDS reader
  looks up the instrument-parquet partition by `venue=KALSHI`; `venue` and `source` are distinct axes, and a
  lowercase `venue` would silently make the entire Kalshi universe invisible to live trading with no error.
- Kalshi's live snapshot stamps `open_time` as an intraday timestamp on the current day, but the day-grain universe
  filter compares against midnight — so `available_from_datetime` is floored to the open date at day-grain before
  that comparison (the precise timestamp is still carried on `MarketLifecycle` for tick-gating).

## Category routing & per-market instrument_id construction

### `instrument_key` wrap — `VENUE:TYPE:SYMBOL` (2026-07-09 fix)

Until 2026-07-09, both adapters stored the bare raw provider id as `instrument_key` with **zero structure** — not
even the `VENUE:TYPE:` prefix every other asset group carries (Polymarket: `market.condition_id` verbatim; Kalshi:
`market.ticker` verbatim — see finding B5 /
[`canonical_id_builder_retrofit_checklist_2026_07_08.md`](../../unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md)
todo 7). Both adapters now route through the shared
`unified_api_contracts.build_canonical_instrument_id(AssetGroup.PREDICTION, venue, InstrumentType.PREDICTION_MARKET, raw_id)`
builder:

| Venue      | Before (bare)                | After (wrapped)                                 |
| ---------- | ---------------------------- | ----------------------------------------------- |
| Kalshi     | `KXBTC-21MAR26-T95000`       | `KALSHI:PREDICTION_MARKET:KXBTC-21MAR26-T95000` |
| Polymarket | `0xabc123…` (`condition_id`) | `POLYMARKET:PREDICTION_MARKET:0xabc123…`        |

**Deliberately NOT `passthrough=True`**: that mode upper-cases the symbol for every non-DeFi type
(`canonical_id_builder.py::_build_passthrough`), which would corrupt Polymarket's `condition_id` — a real, lowercase
`0x…64hex` hash (confirmed via real fixture values, e.g. `0xdeadbeef`, `0xabc123`) — into a non-matching id. Calling
the builder WITHOUT `passthrough` for `InstrumentType.PREDICTION_MARKET` dispatches to
`_build_sports_or_prediction()`, which wraps `VENUE:TYPE:{symbol}` with the symbol's case preserved verbatim —
exactly what's needed here (Kalshi tickers are already upper-case by convention, so the two code paths are
byte-identical for Kalshi; only Polymarket's case preservation is load-bearing).

**Downstream consumers checked before the wrap landed** (all real, all confirmed compatible or already assuming the
wrapped shape):

- `instruments_service/engine/orchestrator/process_write.py::_records_to_dataframe()` joins the CLOB
  `clob_token_ids` side-table by `instrument_key` — fixed in the same change by registering the side-table entry
  under the final wrapped `instrument_key` (not the bare `condition_id`) at the point of construction, so the
  `key == instrument_key` invariant the join relies on still holds.
- `market-tick-data-service/.../live/_is_universe.py::prediction_instrument_ids_from_df()` already rebuilds a bare
  Kalshi `instrument_key` to `KALSHI:PREDICTION_MARKET:{ticker}` and passes an already-prefixed key through
  untouched — this fix makes that rebuild a no-op for fresh data going forward (forward-compatible). Polymarket
  resolves solely via the separate `clob_token_ids` column, untouched by this fix.
- `market-data-processing-service/.../adapters/prediction/trades_adapter.py::preprocess()` already independently
  synthesizes `"POLYMARKET:PREDICTION_MARKET:" + condition_id` from the RAW tick stream's own `condition_id` column
  (not from this catalog's `instrument_key`) — untouched by this fix, and now produces the SAME literal string as
  the catalog's `instrument_key` for the same market (previously the two diverged in shape).
- `features-service`'s cross-venue-dispatch calculators and MTDS's live Kalshi WS connectors already
  assume/require the `KALSHI:PREDICTION_MARKET:{ticker}` wrapped form — this fix removes the gap between what the
  catalog wrote and what these consumers already expected.
- `polymarket/adapter.py::get_instrument(symbol)` matches by exact `instrument_key == symbol` (bare-`condition_id`
  convention) — has no real (non-test) caller today (workspace-wide grep, 2026-07-09); left as a follow-up rather
  than bundled into this fix — a future caller passing a bare `condition_id` would need `get_instrument()` updated
  to also match the wrapped suffix.

### MTDS raw-tick layer — batch writer fix + historical migration (2026-07-09)

The wrap above is instruments-service's own reference-data catalog (`instrument_key`). A SEPARATE, real gap existed
one layer down in `market-tick-data-service`'s raw trade-tick storage (the parquet files under
`market-data-tick-pred-prd-{project_id}`): the **live** Kalshi/Polymarket WS connectors already wrote the wrapped
`VENUE:PREDICTION_MARKET:{raw_id}` shape as the on-disk filename (verified via real 2026-06-28 GCS listing), but
**batch** writes did not — a real, previously-undocumented batch/live divergence found by a dedicated discovery
pass over this exact venue family:

| Write path                           | Filename (before)                                          | Filename (after fix)                                   | `instrument_type` (before) | `instrument_type` (after)       |
| ------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------ | -------------------------- | ------------------------------- |
| Kalshi batch (`trades`)              | `{ticker}.parquet` (no `instrument_id` col)                | `KALSHI:PREDICTION_MARKET:{ticker}.parquet`            | `prediction`               | `prediction_market`             |
| Polymarket batch (`trades`)          | `{condition_id}.parquet`                                   | `POLYMARKET:PREDICTION_MARKET:{condition_id}.parquet`  | `prediction_market`        | `prediction_market` (unchanged) |
| Both venues, live (WS)               | already `VENUE:PREDICTION_MARKET:{id}.parquet`             | unchanged — already compliant                          | `prediction_market`        | `prediction_market` (unchanged) |
| Both venues, batch `book_snapshot_5` | shared `ticks.parquet` fan-in (no per-instrument filename) | unchanged — out of scope (no per-row `symbol` to wrap) | `prediction_market`        | `prediction_market` (unchanged) |

Fix (stops new drift): `kalshi_adapter.py::_annotate_kalshi_ticker()` now stamps a wrapped `instrument_id` column
and unifies `instrument_type` to `prediction_market` (matching the UAC SSOT builder's own default,
`build_prediction_partition_path(instrument_type="prediction_market")`, and Polymarket batch + both live
connectors — Kalshi batch's `prediction` value was drift, not a deliberate distinct value).
`partitioned_writer.py::PartitionedTickWriter._get_writer()` gained a `file_symbol` param
(`_resolve_file_symbol()`) that substitutes the wrapped `instrument_id` for the on-disk filename ONLY — verbatim,
not run through MTDS's own colon-stripping `_sanitize_symbol()`, so batch now matches live's real GCS-verified
shape byte-for-byte. The writer KEY / row-count / manifest market_id tracking stay on the bare ticker/condition_id
(untouched) so Polymarket `book_snapshot_5`'s pre-existing shared-`ticks.parquet` fan-in (no per-row `symbol`) is
unaffected. `ingest_kalshi_bulk_to_canonical.py` (the Jon-Becker deep-history seed script, which reuses
`_annotate_kalshi_ticker` directly) updated in lockstep. `rebuild_prediction_manifest.py`'s `compute_object_atom`
now also prefers a real `ticker` column over the parquet filename stem as the Kalshi market id (previously the
ONLY fallback for Kalshi, since Kalshi objects carry no `conditionId`/`condition_id` column) — decouples the
manifest rebuild from whatever the raw-tick filename happens to be.

**Historical migration** (`market-tick-data-service/market_tick_data_service/scripts/`
`migrate_prediction_instrument_id_wrap_2026_07_09.py`) — real, GCS-verified scope from a full day-sharded scan of
the live bucket `market-data-tick-pred-prd-central-element-323112` (1,836 `day=` partitions, 2021-06-30 through
2026-07-09, 2,730,659 total prediction objects listed):

| Venue      | Legacy `trades` objects needing migration | Transform                                                                                                                        |
| ---------- | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| KALSHI     | 1,552,991                                 | Real content rewrite (download, add/fix `instrument_id` + `instrument_type` columns, re-upload) + directory move + filename wrap |
| POLYMARKET | 1,140,056                                 | Real content rewrite (download, add/fix `instrument_id` column, re-upload) + filename wrap                                       |

A real smoke-apply on 2025-08-15 (both venues) found Polymarket's assumed "already has a correct `instrument_id`
column, pure server-side rename suffices" premise was WRONG for ~53% of a real day's objects (pre-dating the
`instrument_id` stamp landing in `_annotate_cid_dataframe`) — real post-write sample verification caught this
(14/30 failed), so Polymarket's migration was corrected to also content-rewrite (derive `instrument_id` from the
always-present `condition_id`/`conditionId` column), matching Kalshi's approach, not left as a partial fix.
Backup-safe by construction: every object is COPIED to its new canonical key, never overwritten in place — the
pre-migration original at its OLD key is the de-facto backup until a separate, explicit, irreversible
`--drop-legacy` pass (never bundled with the first apply, mirroring `migrate_prediction_to_pred_prd_v9.py`'s own
`--drop-stale` precedent). Real day-sharded, 5-way parallel apply launched 2026-07-09 (`--workers 64` per shard,
GCS storage-client HTTP connection pool boosted to match — the default `requests` pool size of 10 caused severe
thread contention/thrashing at high worker counts, a real measured finding: workers=128 was SLOWER than workers=32
before the fix); real measured throughput ~15-30 objects/sec per shard once warmed up. Given the real corpus size,
full completion is a multi-hour (not multi-minute) operation — see the migration script's own docstring and the
tracked plan's Progress Log for the real elapsed time and final counts once complete.

Both adapters still classify a market into a canonical _shape_ for other fields, but **only Polymarket's
classification result ever reaches a canonical-looking field** (`base_asset`) — and even there, not the one that
matters most. See "Canonical identity model" below before assuming either venue's `base_asset` is structured.

| Category (Polymarket) | `base_asset` field shape (NOT `instrument_id` — see below)                                                                | Example                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Crypto up/down        | `POLYMARKET:UP_DOWN:{ASSET}:{TF}:{DATE}` (via `build_crypto_prediction_id`)                                               | `BTC:UP_DOWN:2026-03-22` (1D timeframe)                  |
| Macro up/down         | same shape, macro index instead of crypto asset                                                                           | `SPX:UP_DOWN:2026-03-22`                                 |
| Football sports       | `{LEAGUE}:{HOME}-v-{AWAY}:{DATE}:{MARKET_TYPE}` (via `build_prediction_instrument_id`, same builder Betfair/Odds API use) | `EPL:ARSENAL-CHELSEA:2025-26::HOME` (selection appended) |
| Everything else       | first 50 chars of the raw `question` text                                                                                 | —                                                        |

Kalshi has **no per-category id builder for `base_asset`** — unlike Polymarket's category-shaped `base_asset` above,
Kalshi's `base_asset` is just `series_ticker[:50]` (see "Canonical identity model" § field semantics below).
`instrument_key` itself is now the wrapped `KALSHI:PREDICTION_MARKET:{ticker}` (see "instrument_key wrap" above,
2026-07-09 fix) — the raw ticker (`KXBTC-21MAR26-T95000`, `KXMLBSPREAD-26JUN231940LADMIN-LAD3`) is the SYMBOL
segment, not the whole id. The adapter does run `classify_kalshi_to_canonical_group()` per market, but that result
feeds only the separate `MarketLifecycle` row (`canonical_group` + `settlement_lag`), never `base_asset`/
`instrument_key`.

### Polymarket sports — team/league resolution

Sports markets on Polymarket have generic `outcomes: ["Yes", "No"]`, so team names come from (priority order):
`event_title` (100% hit rate, "Arsenal vs. Chelsea") → `outcomes` (works for spreads where outcomes ARE team names) →
`question` text parsing. League resolution is via `series.slug` → `POLYMARKET_SERIES_TO_LEAGUE` (UAC
`external/polymarket/sports_mappings.py`). Only leagues in `POLYMARKET_PREDICTION_LEAGUES` (23 leagues) are kept —
everything else (esports, cricket, rugby, F1, UFC, NBA) is dropped or reclassified `prediction::other`:

EPL, ENG_CHAMPIONSHIP, LA_LIGA, SEGUNDA_DIVISION, BUNDESLIGA, BUNDESLIGA_2, SERIE_A, SERIE_B, LIGUE_1, LIGUE_2,
EREDIVISIE, PRIMEIRA_LIGA, SCOTTISH_PREMIERSHIP, SUPER_LIG, DANISH_SUPERLIGA, ELITESERIEN, MLS, LIGA_MX,
ARGENTINA_PRIMERA, BRASILEIRAO, A_LEAGUE, J1_LEAGUE, K_LEAGUE_1.

**Kalshi's sports coverage is a smaller, different set** — `external/kalshi/sports_mappings.py`'s
`KALSHI_SPORTS_TICKER_PREFIXES` covers only 6 football leagues (EPL, Bundesliga, La Liga, Serie A/`SEA`, Ligue 1/`FL1`,
Champions League) **plus NBA/NFL/MLB** — the 3 US leagues Polymarket's registry explicitly excludes. The two venues'
"sports" prediction coverage is not the same 23-league set on both sides; the real cross-venue overlap is narrower
than either venue's own list. (Kalshi's sports tickers are still the raw ticker as the SYMBOL segment of
`instrument_key` — see "instrument_key wrap" above — this mapping only affects `MarketLifecycle` classification,
not the ticker content itself.)

## Cross-venue arbitrage mechanism — `canonical_question_group`

The mechanism that actually lets a strategy compare Polymarket vs. Kalshi pricing for "the same" question is
`canonical_question_group` (a `CanonicalQuestionGroup` `StrEnum`,
`unified_api_contracts/canonical/domain/predictions/canonical_groups.py`), classified per-market by
`classify_polymarket_to_canonical_group()` / `classify_kalshi_to_canonical_group()` and surfaced via the dedicated
`data_type=prediction_canonical_question_group` cluster rows (part of the MVP `data_types` set above). Values look
like `BTC_UP_DOWN_DAILY`, `BNB_PRICE_RANGE_DAILY`, `CPI_PRINT_PER_MONTH`, `FED_RATE_DECISION_PER_FOMC` — venue-agnostic
labels for a recurring _class_ of question, not a specific dated market instance.

> **35** `canonical_question_group` values appear verbatim on both venues (measured 2026-07-17) — this is **by design,
> not a collision**.
> `venue` is tracked as its own column (`CatalogRow.venue`, `unified_api_contracts/canonical/domain/instruments_catalog.py`),
> `canonical_question_group` is a _thematic cross-venue label_ by design (same docstring: `"canonical_question_group":
e.g. "US_ELECTION_2024", "CRYPTO_BTC_DAILY"`), and sharing the label across venues is the **intended mechanism** for
> cross-venue arb comparison — the same pattern sports fixtures use (a fixture_id is venue/bookmaker-independent;
> `venue` is the separate axis that varies). Nothing downstream should ever need `canonical_question_group` to be
> globally unique without also keying on `venue` — that combination is what identifies "this venue's instance of this
> recurring question."

Only **112 of the 2,673,230 catalog rows** carry `data_type=prediction_canonical_question_group` (measured 2026-07-17):
**35 shared labels × 2 venues = 70 rows**, + **38 Polymarket-only**, + **4 Kalshi-only** = 112. The other **2,673,118**
rows are ordinary per-market `trades`/`market_lifecycle` rows with venue-opaque `instrument_id`s (see next section) —
the shared-label mechanism is a small, deliberate cluster-grain overlay on top of the per-market catalog, not something
that touches most rows.

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
over two full venue universes today — **not wired into the per-day write path or persisted onto the catalog** (see
"Canonical identity model" below for the target scheme that closes this gap).

## Canonical identity model

Per the [canonicalization decision doc](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md)
finding 8: the null-field behavior below is understood, the field semantics are settled, and the canonical scheme for
per-instance cross-venue identity (§3) is **implemented** as of 2026-07-09 —
[`prediction_canonical_identity_migration_2026_07_08.md`](../../unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md)
todos 1, 2, 4, 5 shipped (todo 3's regen/backfill and todo 6's downstream-uniqueness check remain, see that plan's
Progress Log for real evidence numbers).

### 1. `base_asset` / `raw_symbol` / `underlying` — current population state

Both adapters' `_parse_market()` populate `base_asset` and `raw_symbol` at `InstrumentRecord` construction
(Polymarket: `base_asset=<category id>, raw_symbol=slug`; Kalshi: `base_asset=series_ticker[:50],
raw_symbol=event_ticker`), and `process_write.py::_records_to_dataframe()` serializes every `InstrumentRecord`
field (via `model_dump()`) into the per-day `instrument_availability/by_date/.../instruments.parquet` snapshots —
so both fields exist in GCS at the per-day grain. `scripts/build_instrument_catalogue.py`'s Prediction-specific
multi-grain roll-up (`build_prediction_catalogue_dataframe()`, distinct from the generic
`build_catalogue_dataframe()` every other asset group uses) threads `raw_symbol` + `base_asset` through its
`_PredLifecycle` / `_merge_lifecycle` / `_emit()` accumulator at the per-conditionId grain (a
`canonical_question_group` bundle row leaves them `""` — honest absence, a family has no single per-market value).

> **✅ RESOLVED 2026-07-17 (history kept deliberately).** This section previously carried a **Known gap**: _"this rollup
> logic has not yet had a full catalogue regen run against it — `prod/catalog.parquet` may still reflect the pre-fix
> `NaN` values for `raw_symbol`/`base_asset` until that regen runs."_ **That regen has since run.** Measured directly
> against the live `prod/catalog.parquet` (object updated 2026-07-17T01:02:27Z), over per-market rows only
> (`data_type != prediction_canonical_question_group`):
>
> | Field        | POLYMARKET                        | KALSHI                      |
> | ------------ | --------------------------------- | --------------------------- |
> | `raw_symbol` | **100.00%** (2,590,684/2,590,684) | **100.00%** (82,434/82,434) |
> | `base_asset` | **100.00%** (2,590,684/2,590,684) | **100.00%** (82,434/82,434) |
>
> No `NaN`/blank `raw_symbol` or `base_asset` survives on any per-market row, on either venue. The gap is closed.
>
> **On the "KALSHI 99.95%" figure**: a fill rate computed over ALL Kalshi rows gives 82,434/82,473 = **99.9527%**. That
> 0.05% is **not** a data gap — it is exactly the 39 Kalshi `prediction_canonical_question_group` cluster rows, which
> are blank **by design** (a family has no single per-market `raw_symbol`; see the honest-absence rule in
> `codex/02-data/honest-absence-downstream-handling.md`). The same arithmetic on Polymarket gives 2,590,684/2,590,757 =
> 99.9972%, which merely rounds to "100.00%". The apparent venue asymmetry is an artifact of each venue's cqg-row
> share, not a real difference — **measure fill over per-market rows only, or the honest-absence rows will masquerade
> as a defect.**

`underlying` (2026-07-09 fix): both adapters' `_parse_market()` now call `underlying_for_group()` on the SAME
`classify_*_to_canonical_group()` result already computed for `MarketLifecycle.canonical_group` (one classification
call, reused — not a second reclassification), and pass the result to `InstrumentRecord(underlying=...)`. Real,
verified examples (`_parse_market()` invoked directly, both adapters):

| Market                                           | Venue      | `underlying` |
| ------------------------------------------------ | ---------- | ------------ |
| `bitcoin-up-or-down-june-24-2026` (BTC daily)    | Polymarket | `"BTC"`      |
| `cpi-inflation-above-3-percent-june-2026`        | Polymarket | `"CPI"`      |
| `trump-approval-rating-above-45-june-2026`       | Polymarket | `"TRUMP"`    |
| `will-something-strange-happen` (unclassifiable) | Polymarket | `"OTHER"`    |
| `epl-arsenal-vs-chelsea-2026-03-22` (sports)     | Polymarket | `None`       |
| `KXBTCD-26JUN24-T95000`                          | Kalshi     | `"BTC"`      |
| `KXCPIYOY-26JUL-T3`                              | Kalshi     | `"CPI"`      |
| `KXFEDDECISION-26JUL-C`                          | Kalshi     | `"FED"`      |
| `KXMLBGAME-26JUN261910SEACLE` (sports)           | Kalshi     | `None`       |
| `KXWEIRDTHING-26JUL` (unclassifiable)            | Kalshi     | `"OTHER"`    |

`OTHER` is the honest catch-all for a **genuinely-unclassified** market (`CanonicalQuestionGroup.OTHER` /
`MISC_NOVELTY`) — most politics/geo/culture markets that DO classify get their OWN named underlying (`TRUMP`,
`ELECTION`, `GEO_ISRAEL_IRAN`, `OSCARS`, …), not a blanket politics/geo bucket (§2 below corrects an earlier,
imprecise framing of this point). Sports markets get `None` (not `OTHER`) — a fixture has no single scalar
underlying, which is a different "not applicable" case from "genuinely unclassified".

### 2. Field semantics: `base_asset` / `underlying` / `raw_symbol`

- **`raw_symbol`**: real venue-native data (Kalshi's `event_ticker`, Polymarket's `slug`) — the same fields
  `cross_venue_mapping.py` parses strikes/fixtures from, and the field the generic catalogue's UTL reader prefers
  for unique venue+id matching. Not vestigial.
- **`base_asset`**: real values, but a misleading field name for Prediction. For Kalshi it's a genuine venue-native
  grouping key (`series_ticker`). For Polymarket it is **not** a base asset in the CeFi/DeFi sense — it's a
  synthesized display label whose shape varies by category: `BTC:UP_DOWN:2026-03-25` for crypto (asset-like),
  `EPL:ARSENAL-CHELSEA:...` for sports (instrument-id-like, not asset-like), and the **raw truncated question text**
  for everything else (`"other"` category) — not an asset at all. The values aren't garbage; the field is reused
  for something the CeFi/DeFi/TradFi schema didn't design it to hold. Not renamed, to avoid a schema-wide breaking
  change over a naming nitpick.
- **`underlying`**: conceptually sensible for a real subset, honestly absent for the rest — **populated at adapter
  time as of 2026-07-09** (§1 table above has real examples). `unified_api_contracts/canonical/domain/predictions/two_axis.py`'s
  `PredictionUnderlying` enum is a **comprehensive** decomposition — every `CanonicalQuestionGroup` maps to exactly
  one `PredictionUnderlying` member (`BTC`, `SPX`, `CPI`, `TRUMP`, `GEO_ISRAEL_IRAN`, `SPORTS_MLB`, …, `OTHER`).
  `cross_venue_mapping.py::_build_mapping()`'s convention (`underlying=None if is_sports else underlying.value`) is
  now mirrored at adapter-construction time by both `_parse_market()` methods. **Correction to an earlier framing of
  this point**: `OTHER` is NOT a blanket bucket for "politics/geo/entertainment" — the `CANONICAL_GROUP_TO_UNDERLYING`
  map assigns most classified politics/geo/culture groups their OWN named underlying (`TRUMP`, `ELECTION`, `ELON`,
  `GEO_ISRAEL_IRAN`, `GEO_RUSSIA_UKRAINE`, `OSCARS`, `BOX_OFFICE`, …); `OTHER` is reserved for a market the
  classifier genuinely cannot route (`CanonicalQuestionGroup.OTHER` / `MISC_NOVELTY`) — a real, verified example is
  `will-something-strange-happen` → `underlying="OTHER"` (§1 table). Sports fixtures get `None` (not `OTHER` — a
  distinct "not applicable" case, since a fixture has no single scalar subject asset at all).

### 3. Canonical scheme for cross-venue identity — decided AND implemented (2026-07-09)

Three real market shapes, three real examples, ONE mechanism (not three parallel ones):

| Shape                                                              | Example                                                                     | Family axis                                           | Per-instance axis                                                                                                                                                                                                                  |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pure prediction (named non-sports underlying, no cross-venue join) | "Will the Fed cut rates in September?"                                      | `canonical_question_group=FED_RATE_DECISION_PER_FOMC` | `underlying="FED"` (a NAMED subject — corrects an earlier draft of this doc that claimed `None`/`OTHER` here); no cross-venue strike/fixture join exists → `canonical_instrument_id` honestly absent (`""`)                        |
| Crypto/macro arb pair                                              | "BTC above $95k by June 24" (Polymarket) ↔ `KXBTCD-26JUN24-T95000` (Kalshi) | `canonical_question_group=BTC_UP_DOWN_DAILY`          | `underlying="BTC"`; `canonical_instrument_id="PRICE::BTC::UP_DOWN::2026-06-24::DIR"` — REAL output of `build_cross_venue_mapping()`'s `(BTC, UP_DOWN, 2026-06-24, strike)` join, verified identical on both venues' catalogue rows |
| Sports (Polymarket, fixture_id)                                    | Arsenal vs. Chelsea, EPL, 2026-03-22 (Polymarket)                           | `canonical_question_group=SPORTS_EPL_MATCH`           | `underlying=None`; `canonical_instrument_id="EPL:CHELSEA_v_ARSENAL:20260322"` — REAL output of `build_fixture_id()` (Sports-asset-group-aligned, adapter-time, see item 4 below)                                                   |

Implementation — extends the **existing** mechanism, not a new one:

1. **`canonical_question_group`** stays the family/theme axis exactly as-is (no change).
2. **`underlying`** (todo 1 — SHIPPED): both `_parse_market()` methods now call
   `underlying_for_group()` on the SAME `classify_*_to_canonical_group()` result already computed for
   `MarketLifecycle.canonical_group` (`classify_lifecycle(market, group=<precomputed>)` — a new optional `group`
   parameter lets the caller pass its own classification through instead of the method reclassifying), applying
   `cross_venue_mapping._build_mapping()`'s existing `None if sports else value` rule. Zero new classification
   logic. Real verified examples in §1's table above.
3. **`canonical_instrument_id`** for a matched crypto/macro/index pair (todo 2 — SHIPPED): wired into
   `scripts/build_instrument_catalogue.py::build_prediction_catalogue_dataframe()` — a real, scheduled step that
   runs on every catalogue regen (not left as a pure function with no caller). After accumulating the per-conditionId
   lifecycle for the full run, it builds minimal `InstrumentRecord` views (`instrument_key` / `venue` / `raw_symbol` /
   `base_asset` / `expiry` — all `build_cross_venue_mapping()` needs, per that module's own docstring on what a
   prediction `InstrumentRecord` carries) split by venue, runs `build_cross_venue_mapping(kalshi_recs, poly_recs)`
   (no `titles=` — see item 4 below), and indexes the result by both venues' `instrument_key` so `_emit()` can look a
   matched conditionId's `canonical_event_id` up. Unmatched instruments keep `canonical_instrument_id=""` — honest
   absence, never a guessed or false pair (verified: a Kalshi-only instrument with no Polymarket counterpart present
   gets `""`, not a fabricated id). `CATALOG_COLUMNS` gained a `canonical_instrument_id` column so this now persists
   into `prod/catalog.parquet` on every regen.
4. **`titles` map source for sports cross-venue matching (todo 4 — DECIDED: not built this migration)**: the
   canonical `InstrumentRecord` schema dropped the `symbol` field (see `cross_venue_mapping.py`'s own module
   docstring), so **no per-instrument human title survives anywhere the offline catalogue roll-up can reach** — not
   the per-day parquet snapshot (`model_dump()` of a schema with no title field), not `prod/catalog.parquet`.
   **⚠️ Partially superseded 2026-07-17 — see "Prediction titles" below**: the legacy Shape-B objects DO still carry
   `question` at 100% fill (47-column family), so `question` is recoverable by re-reading data at rest — **no
   re-capture needed**. This does NOT rescue sports: `event_title` measures 0.00% fill and sports objects lack the
   column entirely, so the `titles=` decision below stands unchanged. Real
   options considered: (a) re-add a title field to `InstrumentRecord` — a schema change out of this migration's
   scope; (b) a live re-fetch from both venues' APIs at roll-up time — defeats the point of an offline, parquet-driven
   rollup and adds a real network dependency to a batch job; (c) a persisted title side-table (mirroring the
   `clob_token_ids` side-table pattern `_register_clob_token_ids()` already uses) — the real, buildable follow-up,
   NOT done this migration (tracked as a deferred item in the plan). **Decision**: ship without a titles map for now
   — `build_cross_venue_mapping(kalshi_recs, poly_recs)` is called with no `titles=` kwarg, which is the matcher's
   own documented honest-absence default (a sports instrument with no title yields no join key, never a false pair)
   — so Kalshi↔Polymarket sports cross-venue pairing is honestly absent in the catalogue today, exactly as
   documented in "Per-instrument join" above. This does NOT block item 5 below, which achieves sports identity
   alignment through a completely different, title-free mechanism.
5. **Sports ↔ Sports-asset-group alignment for Polymarket (todo 5 — SHIPPED, Polymarket only)**: rather than wire the
   unused, network-dependent `_cross_reference_fixture()` method (a per-market API-Football call — unsuitable for the
   hot per-market adapter-parsing loop; a full-universe capture run enumerates 1M+ markets), `_build_sports_id()`
   (`polymarket/parsing.py`) now calls `parse_polymarket_sports_fixture()` (the SAME "Away vs Home" title parser
   `cross_venue_mapping.py`'s own sports branch uses) to get the fixture's `(league, home, away, date)`, then computes
   `build_fixture_id(league, build_team_id(home), build_team_id(away), date)` — the **exact same call shape**
   `scripts/build_instrument_catalogue.py::build_sports_fixture_team_player_catalogue()` uses to build the Sports
   asset group's own fixture rows off raw API-Football team names (no crosswalk, no network call — verified by
   reading that function). `normalize_participant()` (case/whitespace-only) and `build_team_id()`'s `_slug()`
   (case/whitespace-insensitive) are algebraically compatible, so this is byte-identical to calling `build_team_id()`
   on the raw split. Confirmed Prediction's league short-code space (`"EPL"`, `POLYMARKET_PREDICTION_LEAGUES`) IS the
   Sports asset group's own `LEAGUE_REGISTRY` canonical `league_id` space (`league_data_prediction.py`'s
   `LeagueDefinition(league_id="EPL", ...)`) — not a separate namespace needing its own translation. Result surfaces
   on `InstrumentRecord.canonical_instrument_id` for Polymarket sports rows specifically (real example: EPL Arsenal
   vs. Chelsea → `"EPL:CHELSEA_v_ARSENAL:20260322"`, §1 table). **Kalshi sports fixture_id alignment is NOT done** —
   Kalshi's sports team names in the title are city-level ("Seattle vs Cleveland"), and there is no Kalshi-specific
   team-name-to-canonical registry (unlike Polymarket's `POLYMARKET_TEAM_TO_CANONICAL`) to bridge them to the Sports
   asset group's team_id space without risking a wrong mapping — left as a real, tracked follow-up rather than a
   fabricated guess.
6. **Downstream consumer uniqueness check (todo 6 — NOT done this session)**: whether any real consumer treats
   Prediction `instrument_id` as globally unique without also keying on `venue` remains unchecked — carried forward.

Todo 3 (`prod/catalog.parquet` regen/backfill against real GCS) is the remaining implementation step — real scoping /
smoke-test / ETA numbers are in
[`prediction_canonical_identity_migration_2026_07_08.md`](../../unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md)'s
Progress Log (the full backfill itself is intentionally NOT run from this session per the workspace's staged-rollout
rule — smoke-tested + measured, not executed at full scale unsupervised).

## GCS output & the bucket-naming split

**Measured 2026-07-17**: a full recursive listing of `instrument_availability/` returns **22,111 objects, 100% of them
`.parquet` — there is not a single `.json` object under that prefix.** (An earlier revision of this doc drew the tree
below as `instruments.json`; that was wrong on the extension AND on the layout.) The corpus is **two live shapes**, and
the legacy one is NOT dead — see "Prediction titles" below.

```
gs://instruments-store-pred-{env}-{project}/
  prod/catalog.parquet                     # rolled-up per-market + per-cluster catalog (this doc's analysis above)
  instrument_availability/
    by_date/

      # SHAPE A — canonical_question_group-partitioned: 12,451 objects (56.3%)
      canonical_question_group={CQG}/      # 78 distinct values, e.g. BTC_UP_DOWN_DAILY
        day={YYYY-MM-DD}/
          venue={POLYMARKET|KALSHI}/       # 11,013 POLYMARKET + 1,438 KALSHI
            instruments.parquet

      # SHAPE B — legacy: 9,660 objects (43.7%), POLYMARKET-only (zero KALSHI)
      day={YYYY-MM-DD}/
        [pipeline_mode=batch_instruments_service/asset_group=prediction/]   # present on 4,729; absent on 4,931
          [market={BTC|ETH|OTHER|FOOTBALL|…}/] [venue=POLYMARKET/]          # market= / venue= order VARIES
            instruments.parquet | prediction_market_metadata.parquet
```

Shape B is not one layout but a **family of 7 real skeletons** (measured counts, `instrument_availability/by_date/`
prefix elided):

| Skeleton                                                                         | Objects |
| -------------------------------------------------------------------------------- | ------: |
| `day=*/venue=*/market=*/instruments.parquet`                                     |   3,142 |
| `day=*/pipeline_mode=*/asset_group=*/venue=*/market=*/instruments.parquet`       |   3,041 |
| `day=*/pipeline_mode=*/asset_group=*/market=*/venue=*/instruments.parquet`       |     826 |
| `day=*/market=*/venue=*/instruments.parquet`                                     |     826 |
| `day=*/venue=*/instruments.parquet`                                              |     574 |
| `day=*/pipeline_mode=*/asset_group=*/venue=*/instruments.parquet`                |     473 |
| `day=*/venue=*/prediction_market_metadata.parquet`                               |     389 |
| `day=*/pipeline_mode=*/asset_group=*/venue=*/prediction_market_metadata.parquet` |     389 |

Measured facts a reader/consumer must not assume away:

- **All 22,111 objects are `.parquet`** (21,333 `instruments.parquet` + 778 `prediction_market_metadata.parquet`) — a
  second filename exists under `by_date/`, so a glob assuming `instruments.parquet` misses 778 objects.
- **`pipeline_mode` is not universal**: only 4,729 of the 9,660 legacy objects carry
  `pipeline_mode=batch_instruments_service/asset_group=prediction/`; the other 4,931 have no `pipeline_mode`/
  `asset_group` level at all. Both values are single-valued where present. Per the SOURCE-AWARE prefix-match rule
  (`codex/02-data/pipeline-mode-partition.md`), a reader must PREFIX-MATCH, not assume the segment exists.
- **`market=` and `venue=` appear in BOTH orders** (826 objects each way) — never parse these positionally; parse by
  `key=` name.
- **KALSHI exists only in Shape A.** All 9,660 legacy objects are `venue=POLYMARKET`.
- **`day=` spans 2025-03-13 … 2029-01-20** — far-future days are expiry-dated markets, not capture days.

The real, live bucket is the abbreviated `instruments-store-pred-{env}-{project_id}`
(`instruments-store-pred-prd-central-element-323112` exists with 33,122 blobs; the unabbreviated
`instruments-store-prediction-{env}-{project_id}` has never existed — 404). Two independent code paths resolve "the
prediction instruments bucket," and both now agree on the abbreviated form:

1. **instruments-service's own special-cased flat-kind resolver** (`instruments_service/engine/orchestrator/catalogue.py`,
   `scripts/build_instrument_catalogue.py`): the string kind `"instruments-store-prediction"` is passed to
   `resolve_bucket_name(kind="instruments-store-prediction")`, which resolves through the cloud-providers.yaml SSOT
   to `instruments-store-pred-{env}-{project_id}`. This is the bucket that actually has data.
2. **`unified_api_contracts/canonical/gcs_paths.py`'s `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` table**, keyed by
   `(AssetGroup, BucketKind)`: `(AssetGroup.PREDICTION, BucketKind.INSTRUMENTS)` resolves to the same abbreviated
   `instruments-store-pred-{env}-{project_id}`. `(AssetGroup.PREDICTION, BucketKind.MARKET_DATA)` is deliberately
   kept as the **unabbreviated long form** — `market-data-tick-prediction-{env}-{project_id}` is a real,
   still-live legacy bucket mid-migration to the canonical `market-data-tick-pred-prd-{pid}`
   (`market-tick-data-service/scripts/migrate_prediction_to_pred_prd_v9.py`,
   `prediction_manifest_canonicalisation_2026_06_01.md` §C), and it IS actively read by
   `market-data-processing-service`'s `DependencyChecker.UPSTREAM_DEPS_BY_ASSET_GROUP["PREDICTION"]` — flipping it
   before that migration's `--drop-stale` step completes would point that dependency check at the less-complete
   bucket (data-state, not code-verifiable from this repo).

**Known limitation (cross-repo)**: `market-data-processing-service/tests/unit/test_dependency_checker_sports_prediction.py`
(lines ~149, ~155) still asserts the OLD unabbreviated `instruments-store-prediction-` value for the INSTRUMENTS
bucket_template — that repo's `unified-api-contracts` pin has not picked up the abbreviated-form fix yet, so its
assertions need updating once it bumps that pin.

## Prediction titles — a REGRESSION, not an eternal gap (READ BEFORE PLANNING A RE-CAPTURE)

> **🟡 If you are about to plan a multi-day re-capture to recover prediction market titles: don't — at least not for
> `question`. The human question text is ALREADY in GCS today.** Measured 2026-07-17.

The legacy Shape-B objects predate a stored-field reduction (~36 → 22 fields) and therefore carry a **much wider raw
Gamma payload than the current per-day snapshot does** — including the human-readable `question`. The roll-up already
reads these objects. So a missing title downstream is a **regression introduced by the field reduction, NOT an
absence that requires re-fetching from the venue**. Recovering `question` is a re-read of data already at rest.

**Measured evidence** — 40 randomly-sampled legacy objects (16,360 rows), read individually with `pyarrow.parquet`:

| Schema family | Objects |   Rows | Has `question` col | `question` fill | Has `event_title` col | `event_title` fill |
| ------------- | ------: | -----: | ------------------ | --------------: | --------------------- | -----------------: |
| **47-column** |      35 | 15,665 | yes (35/35)        |     **100.00%** | yes (35/35)           |          **0.00%** |
| 41-column     |       3 |    693 | no                 |           0.00% | no                    |              0.00% |
| 30-column     |       2 |      2 | no                 |           0.00% | no                    |              0.00% |
| **Overall**   |      40 | 16,360 | —                  |      **95.75%** | —                     |          **0.00%** |

Real sampled text (day span 2025-03-15 … 2029-01-20): _"Will J.B. Pritzker win the 2028 Democratic presidential
nomination?"_, _"Will Kamala Harris win the 2028 Democratic presidential nomination?"_ — 6,775 distinct values across
15,665 rows (**43.2% distinct**; recurring dated markets legitimately repeat the same question text across days).

**Three measured caveats that bound the claim — do not over-read it:**

1. **`event_title` is NOT recoverable this way.** The 47-column schema _has_ an `event_title` column, but it is
   **0.00% filled across all 15,665 sampled rows**. "Legacy objects carry `question` AND `event_title` at ~100%" is
   **false on `event_title`** — only `question` is populated.
2. **Sports objects carry NEITHER field.** Every `market=FOOTBALL` object sampled (20 objects, 2 independent samples)
   is **30-column with no `question` and no `event_title`**. Since Polymarket sports team resolution depends on
   `event_title` (see "Polymarket sports — team/league resolution"), **the sports `titles=` map in "Canonical identity
   model" §3 item 4 is NOT unblocked by this finding** and remains honestly absent.
3. **The legacy corpus is not uniformly 47-column** (30/41/47 families observed), so `question` covers ~95.75% of
   legacy rows sampled — not 100% of the corpus.

**Net**: the re-capture assumption is retired for **`question` on non-sports markets** (already at rest → re-read, no
venue fetch). It is **not** retired for `event_title` / sports titles, where the field is empty or the column is
absent — that still needs a real source, and the persisted title side-table (§3 item 4 option (c)) remains the
tracked follow-up.

## Key files

| File                                                       | Repo                  | Purpose                                                                                                                                       |
| ---------------------------------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `reference_data/adapters/prediction/polymarket/adapter.py` | instruments-service   | Gamma live listing + `get_instruments()` entrypoint                                                                                           |
| `reference_data/adapters/prediction/polymarket/parsing.py` | instruments-service   | Market → `InstrumentRecord`/`MarketLifecycle`, category/id builders                                                                           |
| `reference_data/adapters/prediction/polymarket/clob.py`    | instruments-service   | CLOB historical enumeration + clob_token_id registration                                                                                      |
| `reference_data/adapters/prediction/kalshi.py`             | instruments-service   | Kalshi adapter — live/historical routing, RSA-PSS signing, series-scoped capture                                                              |
| `canonical/domain/prediction/prediction_mapping.py`        | unified-api-contracts | Legacy keyword classifier (`PredictionMarketMapper`) — category only reaches adapters; its own `canonical_id` is unused                       |
| `canonical/domain/predictions/canonical_groups.py`         | unified-api-contracts | `CanonicalQuestionGroup` enum + `CANONICAL_GROUP_METADATA` (settlement lags)                                                                  |
| `canonical/domain/predictions/classifiers.py`              | unified-api-contracts | `classify_polymarket_to_canonical_group` / `classify_kalshi_to_canonical_group`                                                               |
| `canonical/domain/predictions/two_axis.py`                 | unified-api-contracts | `PredictionUnderlying` (Axis-1, comprehensive) / `PredictionBetType` (Axis-2) + `underlying_for_group()` / `bet_type_for_group()`             |
| `canonical/domain/predictions/cross_venue_mapping.py`      | unified-api-contracts | `build_cross_venue_mapping()` — the real per-instrument Kalshi↔Polymarket same-market matcher                                                 |
| `canonical/domain/predictions/fixture_parsing.py`          | unified-api-contracts | `SportsFixtureKey` + `parse_kalshi_sports_fixture` / `parse_polymarket_sports_fixture` — the sports per-fixture join                          |
| `scripts/build_instrument_catalogue.py`                    | instruments-service   | `build_prediction_catalogue_dataframe()` — the prediction multi-grain catalogue roll-up (populates `raw_symbol`/`base_asset` per-conditionId) |
| `canonical/domain/instruments_catalog.py`                  | unified-api-contracts | `CatalogRow` — the shared per-instrument catalog shape across all 5 asset groups                                                              |
| `canonical/crosscutting/mvp_scope.py`                      | unified-api-contracts | `PredictionMvpRule` — the real MVP venues/market_groups/data_types definition                                                                 |
| `canonical/gcs_paths.py`                                   | unified-api-contracts | `BUCKET_TEMPLATES_BY_ASSET_GROUP_KIND` — the per-asset-group INSTRUMENTS/MARKET_DATA bucket templates                                         |
| `external/polymarket/sports_mappings.py`                   | unified-api-contracts | Series→league, team→canonical, 23-league `POLYMARKET_PREDICTION_LEAGUES`                                                                      |
| `external/kalshi/sports_mappings.py`                       | unified-api-contracts | 6-football-league + NBA/NFL/MLB Kalshi ticker-prefix registry                                                                                 |
| `canonical/domain/sports/canonical_ids.py`                 | unified-api-contracts | `build_prediction_instrument_id()` (shared with Betfair/Odds API for sports)                                                                  |
| `engine/orchestrator/catalogue.py`                         | instruments-service   | The live, correct prediction bucket-kind resolver                                                                                             |

## See also

- Mockup Prediction tab: https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d
- [`instrument_id_format_canonicalization_2026_07_08.md`](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md) — finding 8: the canonical instrument_id scheme for Prediction
- [`prediction_canonical_identity_migration_2026_07_08.md`](../../unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md) — the tracked plan for the remaining migration (adapter-level `underlying` population + `canonical_instrument_id` cross-venue wiring + sports fixture_id alignment)
- [`canonical_instrument_id_audit_2026_07_08.md`](../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md) — the full 7-layer audit backing this doc's canonical-identity analysis
- [`ADAPTER_ARCHITECTURE.md`](./ADAPTER_ARCHITECTURE.md) — general adapter code-structure conventions (not Prediction-specific)
- [`SPORTS_INSTRUMENTS.md`](./SPORTS_INSTRUMENTS.md) — the full 94-league sports MVP (a different, larger registry than Polymarket/Kalshi's prediction-market football coverage above)
