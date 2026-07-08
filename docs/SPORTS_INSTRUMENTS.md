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
(instruments-service) to market data (MTDS) to features (FSS — **F**eatures **S**ports **S**ervice, per
`features_service/sports/config.py`'s `FeaturesSportsServiceConfig` and the service's own `README.md`) — the same
**instruments-service -> MTDS -> FSS** shape as CeFi/DeFi. Key difference: MTDS owns both instrument discovery AND
tick data for sports, because the Odds API returns markets + prices in a single response, so there is no separate
"sports instrument discovery" step at the market-data layer the way there is for CeFi order books.

**Fixture and market-data provenance**: fixture discovery is instruments-service's job — it fetches API-Football
fixtures for the 33 Prediction leagues and writes the canonical fixture reference data to
`sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures/league={LEAGUE}/
fixtures.parquet` (per-league partitioned, canonical `league_id` values). MTDS reads that same fixture data back (via
`SportsCatalogReader`, `market-tick-data-service/market_tick_data_service/engine/sports_catalog_reader.py`) to know
which fixtures exist and build its own manifest/expected-universe rows, then independently calls the Odds API to get
markets + bookmaker odds for those fixtures and writes ticks to `raw_tick_data/by_date/day={date}/
pipeline_mode=batch_odds_api/asset_group=sports/venue={BOOKMAKER}/league_id={LEAGUE}/fixture_id={FIXTURE}/
instrument_type=odds/data_type=trades/ticks.parquet` (per-bookmaker-per-fixture; `venue=` here is the BOOKMAKER, e.g.
`PINNACLE`, not the literal string `ODDS_API`).

**Known limitation — odds↔instruments row-level join-key gap**: the odds-tick row's own `instrument_id`
(`FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}`, built by UAC's `build_instrument_id()`)
never embeds instruments-service's canonical fixture id (`af_fixture_id` or the `LEAGUE:MATCHUP:DATE` fixture id) —
MTDS derives `home_id`/`away_id` purely from the Odds API's own team-name strings via
`validate_team_resolution()`/`build_team_id()`, independent of the fixture parquet it separately reads for manifest
purposes. So there is no row-level join key from an odds tick back to instruments-service's fixture record — both
sides write to the same GCS-partition scheme and both partition per-league/per-date, but they are not linked at the
individual-row grain by a shared id. **Open architecture question for the operator**: is a row-level fixture-id join
needed (e.g. threading `af_fixture_id` through the odds tick schema), or is manifest-level linkage (both reading and
writing the same `(league_id, date)` partitions) sufficient, given fixtures are already uniquely identified by
`(league, home, away, date)` in both places? (See the Step 9 schema section below for the same gap in the odds-tick
schema.) Separately, `docs/SPORTS_ODDS.md` (MTDS's own doc, out of this repo's ownership) is stale on both the GCS
path (shows an older `venue=ODDS_API` shape) and schema (lists `time_bucket`/`m_time` columns the current writer
does not emit).

**Sole Source Rule**: API-Football is the sole source of truth for reference data. If a league, team, fixture,
player, venue, or referee does not exist in API-Football, it does not exist in our universe. All other providers
(FootyStats, Understat, SoccerFootball, Transfermarkt, Open-Meteo, Odds API) are enrichment or market-data only. The
pre-flight gate lives in
`instruments-service/instruments_service/reference_data/sports_dependency.py::check_api_football_dependency()`,
called from `sports/factory.py::create_sports_reference_adapter()` for every venue in
`_API_FOOTBALL_DEPENDENT_VENUES` (`footystats`, `understat`, `transfermarkt`, `soccer_football_info`, `open_meteo`,
`betfair`) — it raises `DependencyError` (with an actionable remediation CLI command) if API-Football's fixtures
parquet is missing for the target date, checked before any enrichment adapter call runs. There is no separate "live"
code path: Sports "live" is the same batch CLI invoked with `--start-date {today} --end-date {today}` at
fixture-proximate times (see Batch -> Live below), so this same pre-flight gate runs for every invocation, batch or
live-triggered.

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

e.g. `ENG_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322`. This is the canonical fixture id builder, used by the
API-Football and FootyStats normalizers.

A single shared entry point, `unified_api_contracts/internal/reference/canonical_id_builder.py`'s
`build_canonical_instrument_id()`, dispatches every asset group's canonical-id construction; for
`asset_group="sports"` it routes straight to `build_fixture_id()`. Its docstring documents the `LEAGUE:MATCHUP:DATE`
shape and explicitly notes it is **not** `VENUE:TYPE:SYMBOL`, by design. Remaining Sports-specific retrofit items are
tracked in `unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md` (see "Known gaps"
below for the Betfair `/` delimiter item that applies to Sports).

## MVP universe today (from the current adapter registry)

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

### Reference / Features tier leagues — the other 61 of the 94-league universe

The 33 Prediction leagues above are only the tradeable slice. UAC `LEAGUE_REGISTRY`
(`unified_api_contracts/canonical/domain/sports/league_classification_data_a.py` + `_b.py`) carries 94 total
football leagues, each tagged with a `LeagueClassificationType` (`Prediction` / `Features` / `Reference` — the only 3
values, `league_registry.py:221-226`): 33 Prediction + 22 Features + 39 Reference = 94. `_mvp_football_league_ids()`
(`unified_api_contracts/canonical/crosscutting/mvp_scope.py:317`) unions all three tiers into the full captured
universe.

- **Features tier (22 leagues)**: not tradeable (no Odds API coverage), but real, in-scope leagues API-Football
  covers and instruments-service captures — used for cross-league context (e.g. second divisions of the 33
  Prediction countries), feeding the same team-form/history calculators.
- **Reference tier (39 leagues)**: lower divisions, cups, continental competitions, and youth/reserve leagues (e.g.
  MLS Next Pro, Copa Chile, Brazil Serie B, various U20 competitions). **Known limitation**: several Reference-tier
  leagues remain numeric-`league_id`-keyed rather than canonicalized to a human-readable code —
  `_canonical_league_id()`'s numeric-resolution pass (`instruments_service/engine/orchestrator/sports.py:57`) only
  resolves leagues UAC has a named mapping for; unmapped numerics pass through unchanged by design. This is distinct
  from the `"UNKNOWN"` sentinel bug documented under "Known gaps" below.
- **Capture scope**: Reference-tier fixtures are fetched and written to the same `sports_reference/by_date/.../
entity=fixtures/` path as Prediction-tier fixtures — the write-universe gate (`_is_in_canonical_write_universe()`)
  scopes to `get_expected_leagues_for_source("api_football")`, which returns all 94 leagues, not just the 33.

**Non-tradeable-data usage**: the Reference/Features tiers feed four use cases: (a) a promoted/relegated team's
prior-season form from its previous division, (b) fixture-congestion / schedule-density context, (c) a historical
first-season-back baseline for promoted/relegated teams, and (d) injury-data carryover for starting-lineup
predictions. All four are wired into the batch/live compute path (`export_derived_features()`, invoked from
`features_service/sports/cli/handlers/batch_handler.py` — the same entrypoint batch and live use).

- **(a) Promotion/relegation cross-league historical form**: `promoted_team_handler.blend_promoted_features()` (a
  `LEAGUE_STRENGTH`-ratio decay-weighted blend of a team's prior-division form) is invoked by
  `features-service/features_service/sports/calculators/promoted_team_features_calculator.py`
  (`compute_promoted_team_batch`), registered in `feature_catalog.py`'s `DERIVED_CALCULATOR_GROUPS["promoted_team"]`
  and run from `derived_features_exporter.py::_run_history_based_calculators` alongside team_form/team_xg/h2h,
  reusing the same multi-league, multi-season `history` DataFrame (`_build_team_history`, spanning all 94
  UAC-classified leagues) those calculators already consume. For each fixture's home/away team, it detects
  promoted/relegated status via `is_promoted_team()` (fewer than `MIN_MATCHES_FOR_STABLE`=10 matches played in the
  current league+season), finds that team's most recent season in a different league in the same `history`, and
  blends current-vs-previous-division form (`promoted_blend_ppg`/`_win_rate`/`_goals_scored_std`/
  `_goals_conceded_std`/`_clean_sheet_rate`/`_conversion_rate`, home*/away_ prefixed) via
  `blend_promoted_features()`. `season_context.py`'s `is_promotion_relegation` (current-season relegation-battle
  flag) and `team_form.py`'s same-league `prev_season_ppg` are separate, distinct concepts.
- **(c) Promoted-team first-season historical baseline**: the same calculator also computes
  `promoted_cohort_avg_ppg` / `promoted_cohort_sample_size` (home*/away* prefixed) — the historical average
  points-per-game other teams making the same cross-league transition (e.g. `ENG_CHAMPIONSHIP` -> `EPL`) achieved in
  their own first captured season at the new level, computed from match-level rows in `history`; an honest-absence
  `0.0`/`0` when the lookback window has no other team on record making that transition. See
  `features-service/tests/sports/unit/calculators/test_promoted_team_features_calculator.py`.
- **(d) Injury-data carryover**: `features-service/features_service/sports/calculators/injury_impact_calculator.py`
  (`compute_injury_impact_batch`, consuming API-Football's `injuries` reference entity — home/away injury counts,
  severity score, key-player-injured flag, crisis indicator) is registered in
  `DERIVED_CALCULATOR_GROUPS["injury_impact"]`, run from `derived_new_calculators.py::run_new_calculators`. Its
  Phase-0 output feeds a Phase-1 `replacement_model` calculator (`depends_on=["player_lineup", "injury_impact"]`)
  that scores expected quality-drop/tactical-distortion from missing players.
- **(b) Fixture congestion / schedule density**: `team_form.py` computes `days_rest`, `games_last_7d`,
  `games_last_14d`, `games_per_week`; `venue_context.py` computes home/away `days_since_last_match`;
  `h2h_calculator.py` computes `h2h_days_since_last`; `bucketed_features_calculator.py` buckets rest days into
  bands; `european_fatigue_calculator.py` adds European-competition-specific congestion (`days_since_european`,
  `european_matches_season`, `double_fixture_week`).

### Reference-data providers (7)

Adapters live in `instruments-service/instruments_service/reference_data/adapters/sports/adapters/`, registered in
`sports/factory.py`'s `_ADAPTERS` map (`api_football`, `footystats`, `open_meteo`, `soccer_football_info` /
`soccerfootball_info`, `transfermarkt`, `understat`). Enrichment adapters depend on API-Football having already been
fetched for the target date — the factory pre-flight checks this and raises `DependencyError` if not.

| Provider       | Role                                                       | API Key         | Coverage                                          |
| -------------- | ---------------------------------------------------------- | --------------- | ------------------------------------------------- |
| API-Football   | Reference data SSOT (fixtures, teams, standings, injuries) | Required        | 100% of leagues                                   |
| Odds API       | Market data (odds, betting instruments) — via MTDS, not IS | Required        | 33 prediction leagues                             |
| FootyStats     | Enrichment (advanced shooting/passing stats)               | Required        | ~73% of fixtures                                  |
| Understat      | Enrichment (xG, shot data)                                 | None (scraping) | 5 leagues only                                    |
| SoccerFootball | Enrichment (progressive stats, standings)                  | Required        | ~99% (current era) / 99.9% (all-time) — see below |
| Transfermarkt  | Enrichment (player valuations, transfers)                  | Required        | ~99% (current era) / 75.4% (all-time) — see below |
| Open-Meteo     | Enrichment (weather at venue)                              | None (free)     | 100% (needs lat/lon)                              |

**Coverage root cause (per-provider):**

- **FootyStats (~73%)**: FootyStats' `/league-list?chosen_leagues_only=true` endpoint reports our account's
  chosen-leagues subscription as 49 leagues total; cross-referencing by name against our 33 Prediction leagues finds
  3 entirely absent — Austria Bundesliga, Greece Super League, and Australia A-League are not in our FootyStats plan
  at all (provider/plan-side, not a bug on our end). That accounts for ~9% of the 33 leagues being structurally
  uncoverable regardless of fetch logic. **Data-state gap (not code-verifiable)**: the remaining shortfall down to
  ~73% (from the ~91% the 30-covered-leagues ceiling would otherwise imply) is not root-caused — would need
  per-fixture live spot-checks across many leagues/dates. The fetch code (`FOOTYSTATS_HISTORICAL_SEASON_IDS`,
  consumed in `instruments_service/engine/orchestrator/footystats.py`) resolves leagues by FootyStats' own numeric
  season/competition id, not by name, so a name-mismatch between our league labels and FootyStats' display names
  (e.g. `JUPILER_PRO` vs "Belgium Pro League") is not the cause of the residual gap.
- **SFI / Soccer Football Info**: `SOCCER_FOOTBALL_INFO_IDS`
  (`unified-api-contracts/unified_api_contracts/canonical/domain/sports/provider_league_ids.py:30-64`) has a static
  hex-id entry for all 33 of 33 Prediction leagues. Coverage is computed by
  `deployment-api/deployment_api/services/data_status/breakdowns_domain.py::_build_sports_entity_entry()` ->
  `sports_helpers.py::sports_honest_coverage()`, whose `SPORTS_DATA_TYPE_META["SFI_PROGRESSIVE_STATS"]` entry uses
  axis `per_league_per_fixture_date` — denominated on the per-fixture calendar across the 33 SFI-expected leagues,
  the correct grain for this data_type (one row per fixture). **Data-state snapshot (2026-07-08 production
  manifest pull, not re-verifiable from code alone)**: SFI_PROGRESSIVE_STATS = 63,798 / 63,862 expected
  (league, fixture-date) shards = 99.9% over the full 2014-2026 history, and 14,627 / 14,691 = 99.56% over the
  current era (2025-01-01 to 2026-07-08).
- **Transfermarkt**: `TRANSFERMARKT_IDS` (`provider_league_ids.py:67-100`) covers 32 of 33 Prediction leagues (only
  Greek Super League lacks a Transfermarkt code). Transfermarkt's captured entity is `player_values` (a team/league
  valuation snapshot, not a per-fixture artifact); `SPORTS_DATA_TYPE_META["PLAYER_VALUES"]` in `sports_helpers.py`
  uses axis `per_league_trigger_date` (season-start + transfer-window-open + transfer-window-close, via UAC
  `get_reference_refresh_dates`) — the correct team/league-season grain, not a fixture count. **Data-state snapshot
  (2026-07-08 production manifest pull)**: PLAYER_VALUES = 2,564 / 3,400 expected (league, trigger-date) shards =
  75.41% over the full 2014-2026 history (reflects historical-backfill incompleteness — older, mostly pre-2025
  seasons only partially backfilled with player-value snapshots), and 439 / 441 = 99.55% over the current era.
  Transfermarkt has no official public API — access is via an unofficial RapidAPI wrapper or an Apify scraper, less
  complete/stable than FootyStats' documented official API.
  **Known limitation (dead-code cleanup)**: `deployment-api/deployment_api/services/data_status/
venue_resolution.py::_resolve_expected_dates`'s own docstring claims a "Transfermarkt → transfer-window dates"
  priority branch, backed by helper functions (`sports.py::_is_transfer_window_venue`,
  `venue_resolution.py::_resolve_transfer_window_dates`, `_is_understat_venue`, `_resolve_understat_fixture_dates`)
  — but none of the four are called from `_resolve_expected_dates`'s body; they are dead code and the docstring is
  inaccurate. This does not affect the coverage numbers above: the only manifest rows with
  `venue=TRANSFERMARKT`/`transfermarkt` (92 rows, all `capture_status=attempted_failed`, plus 2,991 unrelated
  2018-dated rows tagged with prediction-market `data_type`s like `arbitrage_opportunity`) are legacy/orphaned
  artifacts — the real `PLAYER_VALUES` rows carry a blank `venue` column and are keyed by `data_type`, going through
  `sports_honest_coverage()` instead.

**Betfair** (`sports/adapters/betfair.py`) is a distinct, separately-registered reference-data adapter — it goes
through the general `reference_data/factory.py` (as a `BaseReferenceDataAdapter`), not the sports-domain
`sports/factory.py` (whose adapters extend `BaseSportsReferenceAdapter`). It surfaces Betfair's `listMarketCatalogue`
runners as `InstrumentRecord`s with `instrument_type=EXCHANGE_ODDS`. See "Known gaps" below for a real format bug in
its `instrument_key`.

**Betfair's current state, and the 4 live execution adapters:**

- **Betfair reference data**: the adapter code is real and live-capable — it hits
  `https://api.betfair.com/exchange/betting/json-rpc/v1` (`listMarketCatalogue`) with a session token + app key from
  Secret Manager, not a static downloaded file. It is not currently scheduled anywhere in production: no Cloud
  Scheduler cron references it, no launcher script in `deployment-service/scripts/vm/` fetches it, and there is no
  `entity=betfair*` output under `sports_reference/by_date/` in the prod bucket. Dormant: code exists, not currently
  running.
- **The 4 live execution adapters**: `execution-service/execution_service/sports_execution/adapters/exchanges/`
  contains 4 implemented (non-stub) modules — `betfair.py` (via `betfairlightweight`, order placement/cancel/list),
  `matchbook.py` (REST API), `polymarket_clob.py` (EIP-712 + HMAC-signed CLOB API), `kalshi.py` (RSA-signed REST
  API). `adapters/bookmaker_api/` holds `onexbet.py` (a real, non-scraping API integration for an actively-used
  bookmaker — one of the 20 Odds-API-sourced bookmakers, see below) and `api_football.py`. `adapters/
aggregator/odds_api.py` is the same Odds API aggregator MTDS uses. `adapters/unity/` is a scaffold for **Unity**, a
  prime broker that exposes a single TCP connection multiplexed across 10 child sportsbooks (VX/SharpBet, Pinnacle,
  Bet365, …) per its own docstring — commercial turnover/rollover-gate tracking ($260k/mo subscription-waiver gate)
  is implemented, but concrete I/O is intentionally stubbed (`adapters/unity/__init__.py`'s own docstring:
  "production requires the Unity-issued binary + real TCP framing spec") — a known limitation, not yet live.
- **UK/EU bookmaker-scraping modules removed**: the 14 Playwright-based scraper modules that previously lived at
  `execution-service/execution_service/sports_execution/adapters/scrapers/` (`bet365`, `bet888sport`, `betfred`,
  `betvictor`, `betway`, `boylesports`, `bwin`, `coral`, `ladbrokes`, `paddypower`, `sbobet`, `skybet`, `unibet`,
  `williamhill`, plus shared `base_scraper.py`/`version_registry.py` infra) have been deleted, along with their
  dedicated tests. `bookmaker_api/onexbet.py` was retained — a real, non-scraping API adapter, not part of that
  removal.

### Bookmakers (20, via Odds API — MTDS market data, not instruments-service reference data)

`pinnacle, betfair_ex_uk, matchbook, betonlineag, lowvig, onexbet, marathonbet, bovada, betsson, unibet, unibet_uk,
livescorebet, skybet, paddypower, betway, coral, boylesports, leovegas, casumo, virginbet`

**Markets**: h2h (match odds), totals (over/under), spreads (handicap). **Time buckets (14 per fixture-day)**: T-24h,
T-12h, T-6h, T-90m, T-80m, T-70m, T-60m, T-50m, T-40m, T-30m, T-20m, T-10m, T-0, HT.

This doc, and the real adapter registry it's sourced from, are the sports MVP-universe reference.

## 11-step pipeline

### Steps 1-2: Config (UAC, no runtime)

| Step | What                                      | Where                        | Refresh                  |
| ---- | ----------------------------------------- | ---------------------------- | ------------------------ |
| 1    | Prediction leagues (33)                   | UAC `LEAGUE_REGISTRY`        | Manual                   |
| 2    | League mappings (AF->FT/US/SF/TM/OddsAPI) | UAC `provider_league_ids.py` | Yearly (FootyStats only) |

### Steps 3-8: Reference data (instruments-service -> GCS)

| Step | What                                      | Source                     | Refresh (as-designed)             | GCS Path                                                                                                |
| ---- | ----------------------------------------- | -------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 3    | Teams (~600/season)                       | AF `/teams`                | Season-boundary (real, see below) | `sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=teams/league={LEAGUE}/`    |
| 4    | Team mappings (6,245 teams x 5 providers) | UAC static + AF            | Append-only, unpartitioned        | `sports_reference/mappings/team_mapping_v2.parquet`                                                     |
| 5    | Prediction fixtures (~30-60/day)          | AF `/fixtures`             | Daily                             | `sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity=fixtures/league={LEAGUE}/` |
| 6    | Reference fixtures (cups, continental)    | AF `/fixtures`             | Daily                             | Same path as #5 — write-universe gate covers all 94 leagues, not just the 33                            |
| 7    | Venues (3,445, 95% geocoded)              | AF `/venues` + Nominatim   | Yearly                            | `sports_reference/venues/venues.parquet`                                                                |
| 8    | Players, referees, injuries               | AF `/injuries`, `/lineups` | Daily/per-fixture                 | `sports_reference/by_date/day={date}/entity=injuries/`                                                  |

**CLI**: `python -m instruments_service.cli.main --operation instruments --mode batch --asset-group SPORTS
--start-date {date} --end-date {date}`. **Timing**: ~42 seconds per day (33 leagues, ~180 fixtures, ~900 injuries,
~690 standings).

**Teams/standings refresh cadence**: Teams and standings are fetched fresh daily in production via the
`is-daily-enum-sports` Cloud Scheduler cron (`30 13 * * *`), which invokes
`instruments-service/scripts/daily_is_enumeration.py` -> `--operation instruments --mode batch --asset-group sports
--start-date {D-2} --end-date {D} --force` with no `--sports-entity` scoping — so TEAMS/STANDINGS refetch
unconditionally alongside every other entity, every day (roughly 33 AF `/teams` + 33 `/standings` calls per
invocation; the in-process `_cached_teams_df` cache means one CLI invocation covering a multi-day rolling window
pays this cost once, not once per day in that window). A season-boundary-gated alternative exists
(`deployment-service`'s `SportsTriggerScheduler` Tier-2 `reference` tier, see "Seasonal refresh" below) that fetches
TEAMS/LEAGUES only near real season boundaries. **Open question**: narrow `is-daily-enum-sports`'s unconditional
TEAMS/STANDINGS scope now that the season-boundary path is dispatching, or keep both running until the
season-boundary path has proven reliable across a real season boundary.

**Team mappings**: the current, complete file is `sports_reference/mappings/team_mapping_v2.parquet` — one flat,
unpartitioned parquet, 6,245 teams x 5 providers, 18 columns including a per-provider id+name pair for all 5
enrichment providers (`api_football_id/name`, `footystats_id/name`, `understat_id/name`, `sfi_id/name`,
`transfermarkt_id/name`) plus `odds_api_name`, `league`, and Transfermarkt squad/market-value snapshot columns.
`team_mapping.parquet` (76 rows, EPL/Bundesliga only, `odds_api_name`/`understat_name` only) is a smaller, legacy
file, still present but superseded. Other mapping files: `odds_api_team_mapping.parquet` (658 rows, AF-id-keyed,
Odds-API-name-only), `league_mapping.parquet` (605 rows, one row per `(league, season)`), `sfi_league_mapping.parquet`
(50 rows), and Transfermarkt's own per-season structure at
`sports_reference/mappings/season={YYYY}/transfermarkt_league_teams=/teams.parquet` (years 2014, 2017-2026).

### Step 9: Market data — odds (MTDS -> GCS)

| What                       | Source                 | Refresh                         | GCS Path                                                                                                                                                                                         |
| -------------------------- | ---------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Odds + betting instruments | Odds API v4 historical | 14 time buckets per fixture day | `raw_tick_data/by_date/day={date}/pipeline_mode=batch_odds_api/asset_group=sports/venue={BOOKMAKER}/league_id={LEAGUE}/fixture_id={FIXTURE}/instrument_type=odds/data_type=trades/ticks.parquet` |

**API cost**: `bookmakers=` param (not `regions=`) for 4x lower credit usage. Historical: `10 × 3 markets × 1 = 30
credits/call`. Live: `3 markets × 1 = 3 credits/call`. Per day (batch): `30 × 14 buckets × 33 leagues = 13,860
credits`. 80-day backfill = ~1.1M credits.

Note: the path above (`venue={BOOKMAKER}`, e.g. `PINNACLE`, not the literal string `ODDS_API`) is the current writer
output (`market_tick_data_service/engine/orchestrator/venue_fetch.py`'s `_build_sports_shard_path()`) — it differs
from what MTDS's own `docs/SPORTS_ODDS.md` still documents (an older `venue=ODDS_API` shape, plus a schema table
listing `time_bucket`/`m_time` columns the current writer no longer emits). That doc is MTDS-owned, out of scope to
fix here.

**Schema**:

```
instrument_id: FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}
venue: {bookmaker_key}
event_id, sport_key, home_team, away_team, commence_time
market_key, outcome_name, price, point
fetch_utc, kickoff_utc, minutes_to_kickoff, bm_minutes_to_kickoff, staleness_seconds, source, data_type, league_id, date
```

The `instrument_id` shape (`FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}`) is built by
UAC's `build_instrument_id()`. The real columns are as shown above; `time_bucket`/`bm_time`/`m_time` are not columns
in the current writer (`odds_api_adapter.py::_build_fixture_rows()`).

The same odds↔instruments row-level join-key gap described under Overview above applies to this schema: this
`instrument_id` never embeds instruments-service's canonical fixture id (neither `af_fixture_id` nor the
`LEAGUE:MATCHUP:DATE` form) — MTDS derives `{HOME}`/`{AWAY}` from the Odds API's own team-name strings independently.

**CLI**: `python -m market_tick_data_service.cli.main --operation download --mode batch --asset-group SPORTS
--start-date {date} --end-date {date}`

### Steps 10-11: Features (FSS -> GCS)

| Step | What                                               | Source                    | Output                                                                        |
| ---- | -------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| 10   | Derived stable (form, standings, goals)            | instruments-service GCS   | `features-sports-*/sports_features/by_date/day={date}/feature_group={group}/` |
| 11   | Derived complex (xG, weather, odds microstructure) | Multi-provider APIs + GCS | Same path                                                                     |

**Feature counts**: the SSOT, `features-service/features_service/sports/schemas/feature_catalog.py` (its own
docstring: "the SSOT for what features the pipeline produces"), wires 32 calculator groups into
`DERIVED_CALCULATOR_GROUPS` (its own docstring undercounts this as "22 calculators"). Computed totals: 970 derived
features + 140 odds features + 28 fixture features = 1,138 total. **Known doc-drift**: a stale `672` figure remains
hardcoded as an example value in `features_service/sports/api/sse_stream.py`'s docstring, not yet corrected.
Coverage areas: odds microstructure, team goals/form/derived, h2h, league context, advanced stats, player lineup,
halftime, xG (incl. Poisson xG), venue context, weather, steam detection, referee, season context,
European-competition fatigue, bucketed rest-day features.

**FSS cadence**: "FSS" = Features Sports Service. Live-mode compute is event-driven: FSS subscribes to a PubSub
subscription (`persist-sports-odds-features-reader`) via `features_service/sports/app/pubsub/subscriber.py`;
`cli/handlers/live_handler.py` is the live trigger entrypoint. Batch mode (backfill / T+1 reconciliation) is a
date-range CLI loop on its own cron (`30 2 * * *` per `deployment-service/terraform/gcp/t1_batch_scheduler.tf`, plus
a cluster-level `0 6 * * *` batch cron) — there is no fixed intra-day poll loop inside FSS itself. A separate fixed
60-second poll exists one service upstream, in MTDS's own Odds API ingestion
(`market_tick_data_service/live/connectors/odds_api_ws.py`, `_DEFAULT_POLL_INTERVAL_S = 60.0` — no native WebSocket,
polling loop) — that is the Odds API ingestion cadence, not the feature-compute cadence.

**Fixed 2026-07-09 (was: Known gap)**: FSS previously defaulted to subscribing on `sports-odds-ready`, a
terraform-provisioned topic (`deployment-service/terraform/gcp/main.tf`, `terraform/aws/main.tf`) with a real,
working subscriber but **no shipped publisher anywhere in the fleet** — a dead trigger, latent only because live
sports odds ingestion is itself `BLOCKED-CREDENTIALS`. MTDS's generic live persistence sink
(`market_tick_data_service/live/event_facade_sink.py`) publishes via the UTL `EventTransport` facade to
`persist-{asset_group}-{data_type}`, which for sports odds resolves to `persist-sports-odds-features` — the
`("sports", "odds_features")` entry in `unified_api_contracts.events.sink_matrix.SINK_MATRIX`, matching the
terraform-provisioned topic `google_pubsub_topic.persist_sports_odds_features`
(`deployment-service/terraform/gcp/live_event_log/main.tf`). FSS now defaults its subscriber to
`persist-sports-odds-features-reader` (the `{topic}-reader` pull-subscription convention from UTL
`PubSubTransport._pubsub_subscription`), repointing it at the topic MTDS's live sink actually publishes to. Files
changed: `features_service/sports/app/pubsub/subscriber.py`, `cli/handlers/live_handler.py`, `cli/main.py`,
`cli/parser.py`, `docs/ARCHITECTURE.md` (all `features-service`). **Residual follow-up (deployment-service, not done
here)**: the `persist-sports-odds-features-reader` pull subscription itself is not yet terraform-provisioned (only
the push subscription to the GCS warm sink exists); provisioning it, and deprecating the now-unused
`sports-odds-ready` topic, are deployment-service terraform changes tracked in
`unified-trading-pm/plans/active/issues/instruments_docs_audit_outstanding_items_2026_07_08.md` D7.

**CLI**: `python -m features_sports_service.cli.main --operation compute --mode batch --start-date {date} --end-date
{date}`

## GCS bucket layout

```
gs://instruments-store-sports-{env}-{project}/
  sports_reference/
    by_date/day={YYYY-MM-DD}/pipeline_mode={mode}_{source}/entity={type}/league={LEAGUE}/{type}.parquet
                                                               ({mode}_{source} e.g. batch_api_football,
                                                                batch_footystats, batch_soccer_football_info,
                                                                batch_transfermarkt)
    fixtures/day={YYYY-MM-DD}/fixtures.parquet                (backfill, legacy pre-pipeline_mode layout)
    fixture_stats/day={YYYY-MM-DD}/stats.parquet              (backfill, legacy pre-pipeline_mode layout)
    fixture_events/day={YYYY-MM-DD}/events.parquet            (backfill, legacy pre-pipeline_mode layout)
    venues/venues.parquet                                      (with lat/lon)
    teams_in_league/season={YYYY}/teams.parquet               (backfill, legacy)
    footystats_league_ids/season={YYYY}/ids.parquet           (backfill, legacy)
    standings/season={YYYY}/standings.parquet                  (backfill, legacy)
    mappings/team_mapping_v2.parquet                           (current file — 6,245 teams x 5 providers, one flat
                                                                unpartitioned parquet; team_mapping.parquet still
                                                                exists but is a legacy 76-row/2-provider file)
    mappings/season={YYYY}/transfermarkt_league_teams=/teams.parquet   (Transfermarkt-specific, per-season)
  instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet

gs://market-data-tick-sports-{env}-{project}/
  raw_tick_data/by_date/day={YYYY-MM-DD}/pipeline_mode=batch_odds_api/asset_group=sports/venue={BOOKMAKER}/
    league_id={LEAGUE}/fixture_id={FIXTURE}/instrument_type=odds/data_type=trades/ticks.parquet
                                                               (venue= is the bookmaker, not the literal ODDS_API)

gs://features-sports-{project}/
  sports_features/by_date/day={YYYY-MM-DD}/feature_group={group}/features.parquet
```

All paths are hive-partitioned, BigQuery-compatible. Timestamps coerced to microseconds. Sports carries
`pipeline_mode=` in its GCS paths on both the instruments-service side (`sports_dependency.py`'s
`batch_api_football`, and the `batch_footystats`/`batch_transfermarkt`/`batch_soccer_football_info` variants) and
the MTDS side (`pipeline_mode_for_source("odds_api")` -> `PipelineMode.BATCH_ODDS_API`), source-scoped per the
workspace's `{mode}_{source}` convention.

## Data counts

**Flagged gap (data-state, not code-verifiable)**: snapshot as of 2026-03-27; this backfill-row table has not been
re-derived against a fresh GCS aggregation pass, so current real row counts may differ from what's shown below.

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

**Flagged gap (data-state)**: 2025-12-31 to 2026-03-21 (~80 days of odds missing). MTDS can backfill via
`download_batch()`.

## Known gaps and open findings

Distinct from the by-design ID-scheme decision described above — see the linked plan/issue docs for next steps on
each item below.

### Sports reference catalog is league-grain only, by design

`prod/catalog.parquet` rows for Sports: `venue` is an empty string for all rows (correct, see below), and only
league-level entities exist (the former phantom `"UNKNOWN"` sentinel row is fixed — see below).
`scripts/build_instrument_catalogue.py`: this is not a silently-broken write path —
`asset_group == "sports"` dispatches to `build_sports_catalogue_from_manifest()`, which is a documented, deliberate
2026-06-07 design decision to scope the sports "could-exist" catalog to league grain only, because the captured
manifest atom itself is per-`(league_id, data_type, date)` with no fixture/team/player grain (a fixture-grain
catalogue would inflate `expected_unattempted` against a manifest that can never match it). The 11-step pipeline
above does write fixture/team/player reference data to GCS — that part is real — it just never gets rolled into
catalog/coverage rows. **Known limitation**: fixture/team/player-grain catalog + coverage tracking for Sports has
never been implemented, not silently broken; an operator decision is needed on whether it's wanted, plus the
manifest-schema work it would require. Scoped in
`unified-trading-pm/plans/active/sports_catalog_league_grain_only_scope_2026_07_08.md`.

**`venue` vs `source`**: the empty `venue` string above is correct, by design, not a bug — `venue` is a bookmaker
concept, and sports reference-data rows (fixtures/teams/leagues) have no bookmaker association, so an empty `venue`
is the honest value. `source` is a separate column that does need to be populated (identifies the upstream vendor —
`api_football`, `footystats`, etc.); the catalog builder (`build_instrument_catalogue.py`'s `CATALOG_COLUMNS`) does
not carry a `source` column for the catalog artifact at all — that field lives on the separate manifest
(`AvailabilityRecord`, populated via `record_captured(..., source=...)`), not on the catalog.

### `league_id="UNKNOWN"` sentinel — RESOLVED (2026-07-09)

**Was a real, currently-active data-correctness bug; fixed at the code level and backfilled in prod on 2026-07-09.**
Root cause (confirmed 2026-07-08, fixed 2026-07-09): a self-sustaining catalogue↔enumerator feedback loop.
`scripts/build_instrument_catalogue.py`'s `build_sports_catalogue_from_manifest()` rolled the manifest into one
catalogue row per distinct `league_id`, filtering only `league_id != ""` — it did not exclude the `"UNKNOWN"`
sentinel, so it minted a real, persisted catalogue row `instrument_id="UNKNOWN"/league_id="UNKNOWN"`.
`scripts/enumerate_expected_universe.py`'s `_enumerate_v2_sports()` then read that phantom row back
(`league_id = instr.league_id or instr.instrument_id`, no sentinel guard) and emitted one row per sports data_type ×
every alive day for it — the amplifier that grew 2,373 manifest rows across all 17 sports data_types (all
`capture_status ∈ {expected_unattempted, empty_confirmed, captured}`, the 10 `captured` rows all
`instrument_count=0` — a phantom pseudo-league polluting the honest-absence/gap-fill bookkeeping, not real fetched
data mislabeled under the wrong league), recurring daily via the `enum-universe-sports-*` cron. The literal
`"UNKNOWN"` seed traces to
`instruments_service/reference_data/adapters/sports/adapters/api_football_reference.py:165`'s fallback for a
fixture with an empty `league.name` — frozen since the 2026-06-24 `_is_in_canonical_write_universe` write-universe
gate (no new captures land under it).

**Fix (both layers, shipped together):** (1) `build_sports_catalogue_from_manifest()` now excludes
`SPORTS_LEAGUE_ID_SENTINELS` (`{"UNKNOWN"}`, case-insensitive) before the roll-up — this is a narrow sentinel check,
deliberately NOT a full `LEAGUE_REGISTRY` membership check, because 22 real leagues currently in prod (raw numeric
long-tail ids plus `LA_LIGA_2`/`RFPL`/`SCOTTISH_LEAGUE_CUP_185`) are not in `LEAGUE_REGISTRY` and would have been
wrongly dropped by a membership-based filter. (2) `_enumerate_v2_sports()` carries a matching defense-in-depth guard
so it can never re-amplify a phantom league into expected/empty rows even if one somehow re-enters the catalogue.
**Backfill (prod, 2026-07-09):** removed the 1 phantom catalogue row (`prod/catalog.parquet`, 116 → 115 rows) and
the 2,373 manifest rows (`_index/availability_index.parquet`) carrying `league_id="UNKNOWN"`; both objects backed up
first (`*.20260708-234112.unknown_league_backfill.bak.parquet`). Verified post-backfill: 0 sentinel rows remain in
the catalog, manifest index, or per-VM shards; rebuilding the catalogue from the live post-backfill manifest through
the patched roll-up still mints 0 `"UNKNOWN"` rows. Was tracked in
`unified-trading-pm/plans/active/issues/sports_manifest_unknown_league_id_2026_07_08.md` (now marked resolved) and
`instruments_docs_audit_outstanding_items_2026_07_08.md` item A1.

### Betfair `/` delimiter in `instrument_key` — known limitation, tracked

Betfair's `instrument_key` (`f"{market_id}/{selection_id}"`,
`instruments-service/instruments_service/reference_data/adapters/sports/adapters/betfair.py:279`,
`_build_runner_record`) uses a `/` delimiter, inconsistent with every other Sports id in this workspace
(`:`-delimited) — not a `VENUE:TYPE:SYMBOL` violation, since Sports intentionally does not follow that convention
(see "Instrument identity" above). Fixing this does not route through `build_canonical_instrument_id` (Sports keeps
its own scheme) — it needs its own delimiter fix (`f"{market_id}:{selection_id}"`), plus updates to the two
downstream consumers that currently parse `/`: strategy-service's `position/core/fill_event_consumer.py`
(`inst.rsplit("/", 1)`) and execution-service's `sports_execution/adapters/exchanges/betfair_order_mapping.py`
(builds `f"{market_id}/{selection_id}"` in two places) — a 3-repo coordinated change, still open. Low priority given
Betfair reference-data fetching is not currently scheduled/live in production. Tracked in
`unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md`.

## Seasonal refresh (Phase B)

`SportsTriggerScheduler` + `PeriodicTierDispatcher`
(`deployment-service/deployment_service/sports_trigger_scheduler.py` / `sports_trigger_periodic.py`), configured by
`configs/sports-trigger-tiers.yaml`'s Tier-2 `reference` section — `TEAMS` and `LEAGUES` are gated on
`window_condition: season_boundary` (`_gate_by_season_boundary()`, tolerance ±3 days around each expected league's
season start/end dates). `pipeline_mode` for this path is the `batch_api_football`/`batch_*` family, matching the
"batch on live, since it's slow-moving" design. Because this refresh writes through the same instruments-service
batch CLI/orchestrator code as every other invocation, it lands in the same GCS structure documented above.

Dispatch runs via Cloud Run Jobs: the CLI (`cli/commands/sports_trigger.py::sports_trigger_run`) passes
`--backend`/`--workspace-root`/`--cloud-run-*` options into `SportsTriggerScheduler(...)`; the terraform Cloud Run
Job (`terraform/gcp/sports_scheduler_cron.tf`) runs with `--backend cloud` + the project/region/service-account.
`configs/sports-trigger-tiers.yaml`'s `cloud_run_job_name` fields point at the already-provisioned per-service Cloud
Run Jobs the T+1 batch reconciliation cron also dispatches into (`uts-prod-instruments-service-t1-recon`,
`uts-prod-market-tick-data-service-fast-t1-recon`, `features-sports-service-job`). **Known gap**: no dedicated
ml-service Cloud Run Job exists yet, so the `inference_pre_match` tier's `ml-service` entry ships with an empty
`cloud_run_job_name` (`configs/sports-trigger-tiers.yaml`) — this fires a warning and skips cloud dispatch for that
one service rather than pointing at a non-existent job name.

The blunter, unconditional daily `is-daily-enum-sports` cron (see "Team mappings" above) also keeps Sports reference
data flowing independent of this scheduler; see the open question above on whether to narrow its scope now that the
season-boundary path is dispatching.

## Batch -> Live: minimal delta

Sports "live" is the same batch CLI, fired at fixture-proximate times instead of a fixed daily cron — stated
directly in `sports_trigger_scheduler.py`'s own module docstring ("Sports 'live' = batch with `--date today`, fired
at fixture-proximate times. Same CLI, same service, just triggered by fixture proximity instead of daily cron"). The
trigger tiers (`configs/sports-trigger-tiers.yaml`, deployed as `uts-prod-sports-scheduler`; see "Seasonal refresh"
above):

| Tier           | What                                                                                 | Cadence                                                                           | Change from batch                                                       |
| -------------- | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| 1 — Discovery  | Fixture calendar + standings refresh                                                 | Rolling window (today-1..today+7), every 6h                                       | Trigger only                                                            |
| 2 — Reference  | INJURIES (daily) / TRANSFERS / LEAGUES / TEAMS (season-boundary-gated)               | Daily cadence check; season-boundary items fire only near a real season start/end | Trigger + real gating (see Seasonal refresh)                            |
| 3 — Pre-match  | Odds snapshots (T-24h/T-6h/T-1h), lineups, weather, pre-match features, ML inference | Fixture-proximate, offset from real `kickoff_utc`                                 | Trigger + frequency (ML inference has no Cloud Run Job yet — see above) |
| 4 — Post-match | Final stats (T+30m), delayed xG (T+24h), post-match features (T+25h)                 | Fixture-proximate, offset from real match-end estimate                            | Trigger + frequency                                                     |

instruments-service makes no code distinction between batch and live — it is always the same
`--operation instruments --mode batch --asset-group SPORTS --start-date X --end-date Y` CLI contract; only the
caller (the trigger scheduler vs. a plain daily cron) and the date arguments differ. GCS paths and schema are
identical either way.

## BigQuery external table

```sql
-- Already created
SELECT * FROM `sports_analytics.odds_ticks_hive`
WHERE day = "2025-12-20" AND sport_key = "Premier League"
```

The hive `venue=` partition segment holds the bookmaker (e.g. `venue=PINNACLE`), not the literal `ODDS_API` string —
see the Step 9 note above. **Not independently re-verified (data-state)**: this example predates the current
writer's schema (see the Step 9 schema note on `time_bucket`) and should be checked against the real current
external-table DDL/columns before relying on it.
