# Sports Instruments

> One of 7 consolidated instruments-service docs (docs-consolidation Phase 3,
> `unified-trading-pm/plans/active/instruments_service_docs_consolidation_2026_07_08.md`). Covers sports-betting
> fixtures/odds instruments: leagues, matchups, and bookmakers-as-venues.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10
cross-cutting principles), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md),
[shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md).
**Live mockup**: the Sports tab of the instruments-definitions mockup
(https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d) renders this model directly — a fixture IS the
instrument, and a bookmaker is treated as a venue, exactly as described below.

---

## Overview

Sports instruments follow an 11-step pipeline from static config (UAC) through reference data
(instruments-service) to market data (MTDS) to features (FSS) — the same **instruments-service -> MTDS -> FSS**
shape as CeFi/DeFi. Key difference: MTDS owns both instrument discovery AND tick data for sports, because the Odds
API returns markets + prices in a single response, so there is no separate "sports instrument discovery" step at the
market-data layer the way there is for CeFi order books.

**Sole Source Rule**: API-Football is the sole source of truth for reference data. If a league, team, fixture,
player, venue, or referee does not exist in API-Football, it does not exist in our universe. All other providers
(FootyStats, Understat, SoccerFootball, Transfermarkt, Open-Meteo, Odds API) are enrichment or market-data only.

## The fixture-is-the-instrument model

Unlike CeFi/DeFi, where an _instrument_ is a standing tradeable object (a spot pair, a perpetual) that exists
independently of any one trade, in Sports the **fixture itself is the instrument** — a specific match between two
specific teams on a specific date. Markets (who wins?) and selections (Arsenal to win) are attached to that fixture,
and a bookmaker fills the venue role.

| Term               | Definition                               | CeFi Analogy                             |
| ------------------ | ---------------------------------------- | ---------------------------------------- |
| Fixture            | Sporting event (two teams, date, venue)  | Trading pair (BTC-USDT)                  |
| Market             | Question about a fixture (who wins?)     | Spot, Perp, Option                       |
| Selection          | Position on a market (Arsenal to win)    | Long / Short                             |
| Betting instrument | Fixture + market + selection + bookmaker | Specific instrument on specific exchange |
| Odds               | Price at a point in time                 | Bid/ask price                            |

## Instrument identity: Sports has its own ID scheme, by design

Sports does **not** use the general CeFi/DeFi `VENUE:TYPE:SYMBOL` convention, and that is an intentional,
operator-confirmed design decision — not a gap or a bug.

The 2026-07-08 canonical-instrument-id audit
([`canonical_instrument_id_audit_2026_07_08.md`](../../unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md))
and the follow-on decision doc
([`instrument_id_format_canonicalization_2026_07_08.md`](../../unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md))
reviewed whether Sports should be forced into `VENUE:TYPE:SYMBOL` like every other asset group, and the operator
explicitly decided **no**: _"sports doesn't have a clean TYPE/SYMBOL concept."_ A fixture is not a `TYPE` with a
`SYMBOL` in the way a spot pair or perpetual is — it's an event between two named participants on a date, so a
`LEAGUE:MATCHUP:DATE`-shaped identity fits the domain better than shoehorning it into the venue-first convention. This
sits alongside a matching, separately-confirmed decision for Prediction: the 31 `canonical_question_group` keys
shared between Polymarket/Kalshi are **not** a collision — the label is a deliberate cross-venue-arb mechanism, the
same pattern as Sports fixtures being venue/bookmaker-independent at the fixture level even though bookmaker odds are
venue-scoped underneath.

In code, `unified_api_contracts.canonical.domain.sports.canonical_ids.build_fixture_id()` builds exactly this shape:

```
{LEAGUE}:{HOME}_v_{AWAY}:{YYYYMMDD}[_{HHMM}]
```

e.g. `ENG_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322`. This is the real, current canonical fixture id builder, used
by the API-Football and FootyStats normalizers.

**One real documentation bug worth flagging while we're here**: `canonical_id_builder.py`
(`unified-api-contracts/unified_api_contracts/internal/reference/canonical_id_builder.py`) — the file that other docs
treat as the workspace's general instrument-id SSOT — cites this same sports builder family in its own docstring as
"the correct integrated example of the `VENUE:TYPE:SYMBOL` convention." It isn't: `build_fixture_id()` produces
`LEAGUE:MATCHUP:DATE`, a structurally different scheme, with its own separate fallback path. That's now understood as
_reasonable_ per the operator decision above (Sports legitimately needs its own scheme) — but the docstring's framing
of it as a `VENUE:TYPE:SYMBOL` example is simply incorrect and should be corrected wherever `canonical_id_builder.py`
is next touched.

## MVP universe today (real, from the current adapter registry)

### Prediction Leagues (33 active)

Defined in UAC `LEAGUE_REGISTRY` (`unified_api_contracts/canonical/domain/sports/league_registry.py`). A league is
"Prediction" only if the Odds API covers it (no odds = can't trade).

| Country     | Leagues                                   | AF IDs         |
| ----------- | ----------------------------------------- | -------------- |
| England     | EPL, Championship, League One, League Two | 39, 40, 41, 42 |
| Germany     | Bundesliga, Bundesliga 2, Liga 3          | 78, 79, 80     |
| Spain       | La Liga, Segunda Division                 | 140, 141       |
| Italy       | Serie A, Serie B                          | 135, 136       |
| France      | Ligue 1, Ligue 2                          | 61, 62         |
| Netherlands | Eredivisie                                | 88             |
| Portugal    | Primeira Liga                             | 94             |
| Belgium     | Jupiler Pro                               | 144            |
| Turkey      | Super Lig                                 | 203            |
| Scotland    | Premiership                               | 179            |
| Austria     | Bundesliga                                | 218            |
| Denmark     | Superliga                                 | 119            |
| Greece      | Super League                              | 197            |
| Poland      | Ekstraklasa                               | 106            |
| Switzerland | Super League                              | 207            |
| Sweden      | Allsvenskan                               | 113            |
| Norway      | Eliteserien                               | 103            |
| Japan       | J1 League                                 | 98             |
| South Korea | K League 1                                | 292            |
| Australia   | A-League                                  | 188            |
| Brazil      | Brasileirao                               | 71             |
| Argentina   | Primera                                   | 128            |
| Chile       | Primera                                   | 265            |
| Mexico      | Liga MX                                   | 262            |
| USA         | MLS                                       | 253            |

### Reference-data providers (7)

Adapters live in `instruments-service/instruments_service/reference_data/adapters/sports/adapters/`, registered in
`sports/factory.py`'s `_ADAPTERS` map (`api_football`, `footystats`, `open_meteo`, `soccer_football_info` /
`soccerfootball_info`, `transfermarkt`, `understat`). Enrichment adapters depend on API-Football having already been
fetched for the target date — the factory pre-flight checks this and raises `DependencyError` if not.

| Provider       | Role                                                       | API Key         | Coverage              |
| -------------- | ---------------------------------------------------------- | --------------- | --------------------- |
| API-Football   | Reference data SSOT (fixtures, teams, standings, injuries) | Required        | 100% of leagues       |
| Odds API       | Market data (odds, betting instruments) — via MTDS, not IS | Required        | 33 prediction leagues |
| FootyStats     | Enrichment (advanced shooting/passing stats)               | Required        | ~73% of fixtures      |
| Understat      | Enrichment (xG, shot data)                                 | None (scraping) | 5 leagues only        |
| SoccerFootball | Enrichment (progressive stats, standings)                  | Required        | ~38%                  |
| Transfermarkt  | Enrichment (player valuations, transfers)                  | Required        | ~41%                  |
| Open-Meteo     | Enrichment (weather at venue)                              | None (free)     | 100% (needs lat/lon)  |

**Betfair** (`sports/adapters/betfair.py`) is a distinct, separately-registered reference-data adapter — it goes
through the general `reference_data/factory.py` (as a `BaseReferenceDataAdapter`), not the sports-domain
`sports/factory.py` (whose adapters extend `BaseSportsReferenceAdapter`). It surfaces Betfair's `listMarketCatalogue`
runners as `InstrumentRecord`s with `instrument_type=EXCHANGE_ODDS`. See "Known gaps" below for a real format bug in
its `instrument_key`.

### Bookmakers (20, via Odds API — MTDS market data, not instruments-service reference data)

`pinnacle, betfair_ex_uk, matchbook, betonlineag, lowvig, onexbet, marathonbet, bovada, betsson, unibet, unibet_uk,
livescorebet, skybet, paddypower, betway, coral, boylesports, leovegas, casumo, virginbet`

**Markets**: h2h (match odds), totals (over/under), spreads (handicap). **Time buckets (14 per fixture-day)**: T-24h,
T-12h, T-6h, T-90m, T-80m, T-70m, T-60m, T-50m, T-40m, T-30m, T-20m, T-10m, T-0, HT.

Note: the MVP-crypto-instruments spec (`docs/specs/MVP_INSTRUMENTS.md`) is a CeFi/DeFi/TradFi-only document with zero
sports content — it does not describe the sports universe at all, and should not be treated as a sports reference.
This doc, and the real adapter registry it's sourced from, are the sports MVP-universe reference.

## 11-step pipeline

### Steps 1-2: Config (UAC, no runtime)

| Step | What                                      | Where                        | Refresh                  |
| ---- | ----------------------------------------- | ---------------------------- | ------------------------ |
| 1    | Prediction leagues (33)                   | UAC `LEAGUE_REGISTRY`        | Manual                   |
| 2    | League mappings (AF->FT/US/SF/TM/OddsAPI) | UAC `provider_league_ids.py` | Yearly (FootyStats only) |

### Steps 3-8: Reference data (instruments-service -> GCS)

| Step | What                                      | Source                     | Refresh           | GCS Path                                               |
| ---- | ----------------------------------------- | -------------------------- | ----------------- | ------------------------------------------------------ |
| 3    | Teams (~600/season)                       | AF `/teams`                | Seasonal          | `sports_reference/by_date/day={date}/entity=teams/`    |
| 4    | Team mappings (6,245 teams x 5 providers) | UAC static + AF            | Seasonal          | `sports_reference/mappings/team_mapping.parquet`       |
| 5    | Prediction fixtures (~30-60/day)          | AF `/fixtures`             | Daily             | `sports_reference/by_date/day={date}/entity=fixtures/` |
| 6    | Reference fixtures (cups, continental)    | AF `/fixtures`             | Daily             | `sports_reference/by_date/day={date}/entity=fixtures/` |
| 7    | Venues (3,445, 95% geocoded)              | AF `/venues` + Nominatim   | Yearly            | `sports_reference/venues/venues.parquet`               |
| 8    | Players, referees, injuries               | AF `/injuries`, `/lineups` | Daily/per-fixture | `sports_reference/by_date/day={date}/entity=injuries/` |

**CLI**: `python -m instruments_service.cli.main --operation instruments --mode batch --asset-group SPORTS
--start-date {date} --end-date {date}`. **Timing**: ~42 seconds per day (33 leagues, ~180 fixtures, ~900 injuries,
~690 standings).

### Step 9: Market data — odds (MTDS -> GCS)

| What                       | Source                 | Refresh                         | GCS Path                                                        |
| -------------------------- | ---------------------- | ------------------------------- | --------------------------------------------------------------- |
| Odds + betting instruments | Odds API v4 historical | 14 time buckets per fixture day | `raw_tick_data/by_date/day={date}/venue=ODDS_API/ticks.parquet` |

**API cost**: `bookmakers=` param (not `regions=`) for 4x lower credit usage. Historical: `10 × 3 markets × 1 = 30
credits/call`. Live: `3 markets × 1 = 3 credits/call`. Per day (batch): `30 × 14 buckets × 33 leagues = 13,860
credits`. 80-day backfill = ~1.1M credits.

**Schema**:

```
instrument_id: FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}
venue: {bookmaker_key}
event_id, sport_key, home_team, away_team, commence_time
market_key, outcome_name, price, point
time_bucket, timestamp_utc, kickoff_utc, bm_time, m_time, source, date
```

**CLI**: `python -m market_tick_data_service.cli.main --operation download --mode batch --asset-group SPORTS
--start-date {date} --end-date {date}`

### Steps 10-11: Features (FSS -> GCS)

| Step | What                                               | Source                    | Output                                                                        |
| ---- | -------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| 10   | Derived stable (form, standings, goals)            | instruments-service GCS   | `features-sports-*/sports_features/by_date/day={date}/feature_group={group}/` |
| 11   | Derived complex (xG, weather, odds microstructure) | Multi-provider APIs + GCS | Same path                                                                     |

**23 calculators, 672 features** across: odds microstructure, team goals, h2h, league context, advanced stats,
player lineup, halftime, xG, venue context, weather, steam detection, referee, season context, poisson xG, team
form, team xG, team derived.

**CLI**: `python -m features_sports_service.cli.main --operation compute --mode batch --start-date {date} --end-date
{date}`

## GCS bucket layout

```
gs://instruments-store-sports-{env}-{project}/
  sports_reference/
    by_date/day={YYYY-MM-DD}/entity={type}/{type}.parquet    (instruments-service live output)
    fixtures/day={YYYY-MM-DD}/fixtures.parquet                (backfill)
    fixture_stats/day={YYYY-MM-DD}/stats.parquet              (backfill)
    fixture_events/day={YYYY-MM-DD}/events.parquet            (backfill)
    venues/venues.parquet                                      (with lat/lon)
    teams_in_league/season={YYYY}/teams.parquet               (backfill)
    footystats_league_ids/season={YYYY}/ids.parquet           (backfill)
    standings/season={YYYY}/standings.parquet                  (backfill)
    mappings/team_mapping.parquet                              (session 2)
  instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet

gs://market-data-tick-sports-{env}-{project}/
  raw_tick_data/by_date/day={YYYY-MM-DD}/venue=ODDS_API/ticks.parquet

gs://features-sports-{project}/
  sports_features/by_date/day={YYYY-MM-DD}/feature_group={group}/features.parquet
```

All paths are hive-partitioned, BigQuery-compatible. Timestamps coerced to microseconds.

## Data counts (as of 2026-03-27 — a snapshot, not live-verified in this pass)

| Entity                  | Source               | Count                       | Date Range         |
| ----------------------- | -------------------- | --------------------------- | ------------------ |
| Fixtures (backfill)     | Old dump             | 143,568 across 3,438 days   | 2019-01 to 2026-05 |
| Fixture stats           | Old dump             | ~163K across 2,380 days     | 2019-01 to 2026-01 |
| Fixture events          | Old dump             | ~1.87M across 2,462 days    | 2019-01 to 2026-01 |
| Teams-in-league         | Old dump             | ~30K across 7 seasons       | 2019-2025          |
| Venues                  | Old dump + Nominatim | 3,445 (95% geocoded)        | Static             |
| Standings               | Old dump             | ~4.7K across 7 seasons      | 2019-2025          |
| Odds (migrated from v3) | Old system           | 288M rows across 1,825 days | 2020-06 to 2025-12 |
| Odds (MTDS live)        | Odds API             | ~35K rows                   | 2026-03-22         |

**Gap**: 2025-12-31 to 2026-03-21 (~80 days of odds missing). MTDS can backfill via `download_batch()`.

## Known gaps and open findings

These are real, currently-open findings surfaced by the 2026-07-08 canonical-instrument-id audit — genuine
data-completeness and format bugs, distinct from the by-design ID-scheme decision above.

### The real reference catalog is bare (data-completeness gap, not a format issue)

Per real `prod/catalog.parquet` reads (confirmed 2026-07-08): Sports' catalog is currently very thin relative to the
pipeline described above —

- `venue` is an **empty string for all 116 real rows** in the catalog today.
- One row's key is the literal sentinel string `"UNKNOWN"`.
- **Only league-level entities exist** in the real catalog — there are no team-level, match/fixture-level, or
  player-level `instrument_id`s in production today, despite the 11-step pipeline above being designed to carry
  fixtures, teams, players, and referees all the way through.

This is a genuine, still-open data-completeness gap: the pipeline/adapters described in this doc are real and wired
up, but the real catalog output doesn't yet reflect fixture-level (let alone team/player-level) granularity. It is
not a symptom of the ID-format decision above — even a correctly-shaped `LEAGUE:MATCHUP:DATE` id would still be
missing for anything below league level, because those rows simply aren't being written yet.

### Betfair: real `/` delimiter bug in `instrument_key`

Confirmed via `instruments-service/instruments_service/reference_data/adapters/sports/adapters/betfair.py:279`
(`_build_runner_record`): Betfair's `instrument_key` is built as `f"{market_id}/{selection_id}"` — a raw
`marketId/selectionId` pair joined with a forward slash, not the workspace's `:`-delimited convention used
everywhere else (including Sports' own `LEAGUE:MATCHUP:DATE` scheme). This is confirmed genuinely sports-scoped (the
adapter lives under `reference_data/adapters/sports/`, sets `venue="betfair"` and
`instrument_type=InstrumentType.EXCHANGE_ODDS`) — this is the audit's "most degenerate raw-passthrough found" finding,
and it lives in this doc's domain, not TradFi's.

### `canonical_id_builder.py` docstring inaccuracy

See "Instrument identity" above — the file's own docstring mis-cites the sports fixture-id builder as a
`VENUE:TYPE:SYMBOL` example when it's actually a structurally different `LEAGUE:MATCHUP:DATE` scheme. Not a behavior
bug (the builder itself works correctly for its actual, by-design scheme), just a documentation correction still
outstanding in that file.

## Seasonal refresh (Phase B — not yet implemented)

instruments-service should run a daily no-op check:

1. Call AF `/leagues` for the 33 prediction leagues.
2. Check `seasons[].current` — has a new season started?
3. If yes: fetch new teams, update FT league IDs, fetch venues.
4. If no: do nothing.

## Batch -> Live: minimal delta

| Concern             | Batch                         | Live                        | Change         |
| ------------------- | ----------------------------- | --------------------------- | -------------- |
| Trigger             | Daily cron                    | Pub/Sub on fixture schedule | Trigger only   |
| instruments-service | Full 33-league refresh        | No change (daily)           | Nothing        |
| MTDS odds           | 14 buckets via historical API | WebSocket stream            | Adapter swap   |
| FSS fetch           | Providers called once/day     | Per-fixture (~60min pre-KO) | Frequency only |
| GCS paths           | Same                          | Same                        | Nothing        |
| Schema              | Same                          | Same                        | Nothing        |

## BigQuery external table

```sql
-- Already created
SELECT * FROM `sports_analytics.odds_ticks_hive`
WHERE day = "2025-12-20" AND time_bucket = "T-24h" AND sport_key = "Premier League"
```

Note: the hive partition `venue=ODDS_API` shadows the in-file `venue` column (bookmaker). Use the `instrument_id` to
extract the bookmaker, or query the parquet directly via pandas.
