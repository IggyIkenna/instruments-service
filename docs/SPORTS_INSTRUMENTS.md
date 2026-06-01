# Sports Instrument Pipeline

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**Post-2026-05-06 additions** — sports per-fixture sharding clarified per multi-axis plan: manifest shard atom is `(asset_group=sports, source, data_type, league_id, day)`; fixture_id is a row-level column in the parquet for drill-down; per-fixture detail comes from parquet read at drill-down time, NOT from a separate manifest row. Avoids 10× manifest inflation. League stays as a higher-level rollup grouping for filtering.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Overview

Sports instruments follow an 11-step pipeline from static config (UAC) through reference data (instruments-service) to
market data (MTDS) to features (FSS). The pipeline mirrors CeFi/DeFi: **instruments-service -> MTDS -> FSS**.

Key difference: MTDS owns both instrument discovery AND tick data for sports, because the Odds API returns markets +
prices in a single response.

## Sole Source Rule

**API-Football is the sole source of truth for all reference data.** If a league, team, fixture, player, venue, or
referee does not exist in API-Football, it does not exist in our universe. All other providers are enrichment only.

## Terminology

| Term               | Definition                               | CeFi Analogy                             |
| ------------------ | ---------------------------------------- | ---------------------------------------- |
| Fixture            | Sporting event (two teams, date, venue)  | Trading pair (BTC-USDT)                  |
| Market             | Question about a fixture (who wins?)     | Spot, Perp, Option                       |
| Selection          | Position on a market (Arsenal to win)    | Long / Short                             |
| Betting instrument | Fixture + market + selection + bookmaker | Specific instrument on specific exchange |
| Odds               | Price at a point in time                 | Bid/ask price                            |

## Prediction Leagues (33 active)

Defined in UAC `LEAGUE_REGISTRY`. A league is "Prediction" only if Odds API covers it (no odds = can't trade).

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

## Data Providers (7)

| Provider       | Role                                                       | API Key         | Secret Name                    | Coverage              |
| -------------- | ---------------------------------------------------------- | --------------- | ------------------------------ | --------------------- |
| API-Football   | Reference data SSOT (fixtures, teams, standings, injuries) | Required        | `api-football-api-key`         | 100% of leagues       |
| Odds API       | Market data (odds, betting instruments)                    | Required        | `odds-api-key`                 | 33 prediction leagues |
| FootyStats     | Enrichment (advanced shooting/passing stats)               | Required        | `footystats-api-key`           | ~73% of fixtures      |
| Understat      | Enrichment (xG, shot data)                                 | None (scraping) | N/A                            | 5 leagues only        |
| SoccerFootball | Enrichment (progressive stats, standings)                  | Required        | `soccer-football-info-api-key` | ~38%                  |
| Transfermarkt  | Enrichment (player valuations, transfers)                  | Required        | `transfermarkt-api-key`        | ~41%                  |
| Open-Meteo     | Enrichment (weather at venue)                              | None (free)     | N/A                            | 100% (needs lat/lon)  |

## 11-Step Pipeline

### Steps 1-2: Config (UAC, no runtime)

| Step | What                                      | Where                        | Refresh                  |
| ---- | ----------------------------------------- | ---------------------------- | ------------------------ |
| 1    | Prediction leagues (33)                   | UAC `LEAGUE_REGISTRY`        | Manual                   |
| 2    | League mappings (AF->FT/US/SF/TM/OddsAPI) | UAC `provider_league_ids.py` | Yearly (FootyStats only) |

### Steps 3-8: Reference Data (instruments-service -> GCS)

| Step | What                                      | Source                     | Refresh           | GCS Path                                               |
| ---- | ----------------------------------------- | -------------------------- | ----------------- | ------------------------------------------------------ |
| 3    | Teams (~600/season)                       | AF `/teams`                | Seasonal          | `sports_reference/by_date/day={date}/entity=teams/`    |
| 4    | Team mappings (6,245 teams x 5 providers) | UAC static + AF            | Seasonal          | `sports_reference/mappings/team_mapping.parquet`       |
| 5    | Prediction fixtures (~30-60/day)          | AF `/fixtures`             | Daily             | `sports_reference/by_date/day={date}/entity=fixtures/` |
| 6    | Reference fixtures (cups, continental)    | AF `/fixtures`             | Daily             | `sports_reference/by_date/day={date}/entity=fixtures/` |
| 7    | Venues (3,445, 95% geocoded)              | AF `/venues` + Nominatim   | Yearly            | `sports_reference/venues/venues.parquet`               |
| 8    | Players, referees, injuries               | AF `/injuries`, `/lineups` | Daily/per-fixture | `sports_reference/by_date/day={date}/entity=injuries/` |

**CLI**: `python -m instruments_service.cli.main --operation instruments --mode batch --asset-group SPORTS --start-date {date} --end-date {date}`

**Timing**: ~42 seconds per day (33 leagues, ~180 fixtures, ~900 injuries, ~690 standings).

### Step 9: Market Data — Odds (MTDS -> GCS)

| What                       | Source                 | Refresh                         | GCS Path                                                        |
| -------------------------- | ---------------------- | ------------------------------- | --------------------------------------------------------------- |
| Odds + betting instruments | Odds API v4 historical | 14 time buckets per fixture day | `raw_tick_data/by_date/day={date}/venue=ODDS_API/ticks.parquet` |

**Time buckets (14)**: T-24h, T-12h, T-6h, T-90m, T-80m, T-70m, T-60m, T-50m, T-40m, T-30m, T-20m, T-10m, T-0, HT

**Bookmakers (20)**: pinnacle, betfair_ex_uk, matchbook, betonlineag, lowvig, onexbet, marathonbet, bovada, betsson, unibet, unibet_uk, livescorebet, skybet, paddypower, betway, coral, boylesports, leovegas, casumo, virginbet

**Markets**: h2h (match odds), totals (over/under), spreads (handicap)

**API cost**: Uses `bookmakers=` parameter (not `regions=`) for 4x lower credit usage.
Historical: `10 × 3 markets × 1 = 30 credits/call`. Live: `3 markets × 1 = 3 credits/call`.
Per day (batch): `30 × 14 buckets × 33 leagues = 13,860 credits`. 80-day backfill = ~1.1M credits.

**Schema**:

```
instrument_id: FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}
venue: {bookmaker_key}
event_id, sport_key, home_team, away_team, commence_time
market_key, outcome_name, price, point
time_bucket, timestamp_utc, kickoff_utc, bm_time, m_time, source, date
```

**CLI**: `python -m market_tick_data_service.cli.main --operation download --mode batch --asset-group SPORTS --start-date {date} --end-date {date}`

### Steps 10-11: Features (FSS -> GCS)

| Step | What                                               | Source                    | Output                                                                        |
| ---- | -------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| 10   | Derived stable (form, standings, goals)            | instruments-service GCS   | `features-sports-*/sports_features/by_date/day={date}/feature_group={group}/` |
| 11   | Derived complex (xG, weather, odds microstructure) | Multi-provider APIs + GCS | Same path                                                                     |

**23 calculators, 672 features** across: odds microstructure, team goals, h2h, league context, advanced stats, player lineup, halftime, xG, venue context, weather, steam detection, referee, season context, poisson xG, team form, team xG, team derived.

**CLI**: `python -m features_sports_service.cli.main --operation compute --mode batch --start-date {date} --end-date {date}`

## GCS Bucket Layout

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

gs://market-data-tick-sports-{project}/
  raw_tick_data/by_date/day={YYYY-MM-DD}/venue=ODDS_API/ticks.parquet

gs://features-sports-{project}/
  sports_features/by_date/day={YYYY-MM-DD}/feature_group={group}/features.parquet
```

All paths are hive-partitioned, BigQuery-compatible. Timestamps coerced to microseconds.

## Data Counts (as of 2026-03-27)

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

## BigQuery External Table

```sql
-- Already created
SELECT * FROM `sports_analytics.odds_ticks_hive`
WHERE day = "2025-12-20" AND time_bucket = "T-24h" AND sport_key = "Premier League"
```

Note: The hive partition `venue=ODDS_API` shadows the in-file `venue` column (bookmaker). Use the
`instrument_id` to extract the bookmaker, or query the parquet directly via pandas.

## Seasonal Refresh (Phase B — not yet implemented)

instruments-service should run a daily no-op check:

1. Call AF `/leagues` for 33 prediction leagues
2. Check `seasons[].current` — has a new season started?
3. If yes: fetch new teams, update FT league IDs, fetch venues
4. If no: do nothing

## Batch -> Live: Minimal Delta

| Concern             | Batch                         | Live                        | Change         |
| ------------------- | ----------------------------- | --------------------------- | -------------- |
| Trigger             | Daily cron                    | Pub/Sub on fixture schedule | Trigger only   |
| instruments-service | Full 33-league refresh        | No change (daily)           | Nothing        |
| MTDS odds           | 14 buckets via historical API | WebSocket stream            | Adapter swap   |
| FSS fetch           | Providers called once/day     | Per-fixture (~60min pre-KO) | Frequency only |
| GCS paths           | Same                          | Same                        | Nothing        |
| Schema              | Same                          | Same                        | Nothing        |
