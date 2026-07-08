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
(instruments-service) to market data (MTDS) to features (FSS — **F**eatures **S**ports **S**ervice, confirmed
2026-07-08 via `features_service/sports/config.py:16`'s `FeaturesSportsServiceConfig` and the service's own
`README.md`) — the same **instruments-service -> MTDS -> FSS** shape as CeFi/DeFi. Key difference: MTDS owns both
instrument discovery AND tick data for sports, because the Odds API returns markets + prices in a single response,
so there is no separate "sports instrument discovery" step at the market-data layer the way there is for CeFi order
books.

**Fixture and market-data provenance (confirmed 2026-07-08, both sides read directly)**: fixture discovery is
100% instruments-service's job — it fetches API-Football fixtures for the 33 prediction leagues and writes the
canonical fixture reference data to `sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/
entity=fixtures/league={LEAGUE}/fixtures.parquet` (per-league partitioned, canonical `league_id` values). MTDS reads
that SAME fixture data back (via `SportsCatalogReader`,
`market-tick-data-service/market_tick_data_service/engine/sports_catalog_reader.py`) to know which fixtures exist and
build its own manifest/expected-universe rows, then independently calls the Odds API to get markets + bookmaker odds
for those fixtures and writes ticks to `raw_tick_data/by_date/day={date}/pipeline_mode=batch_odds_api/
asset_group=sports/venue={BOOKMAKER}/league_id={LEAGUE}/fixture_id={FIXTURE}/instrument_type=odds/
data_type=trades/ticks.parquet` (per-bookmaker-per-fixture; `venue=` here is the BOOKMAKER, e.g. `PINNACLE`, not the
literal string `ODDS_API` — that string only appears in the manifest/`CatalogRow` context, not the real per-bookmaker
tick path). **A real, confirmed gap between the two sides, not yet resolved**: the odds-tick row's own
`instrument_id` (`FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}`, built by UAC's
`build_instrument_id()`) never embeds instruments-service's canonical fixture id (`af_fixture_id` or the
`LEAGUE:MATCHUP:DATE` fixture id) — MTDS derives `home_id`/`away_id` purely from the Odds API's own team-name
strings via `validate_team_resolution()`/`build_team_id()`, independent of the fixture parquet it separately reads
for manifest purposes. So there is no ROW-LEVEL join key from an odds tick back to instruments-service's fixture
record — the two sides use the SAME real GCS-partition scheme and both correctly write per-league/per-date, but they
are not linked at the individual-row grain by a shared id. **This is a real architectural question for the
operator**: is a row-level fixture-id join needed (e.g. threading `af_fixture_id` through the odds tick schema), or
is manifest-level linkage (both reading/writing the same `(league_id, date)` partitions) sufficient given fixtures
are already uniquely identified by `(league, home, away, date)` in both places? Flagging rather than guessing.
`docs/SPORTS_ODDS.md` (MTDS's own doc) is additionally stale on both the GCS path (still shows the older
`raw_tick_data/by_date/day={date}/venue=ODDS_API/ticks.parquet` shape) and the schema (lists `time_bucket`/`m_time`
columns the real writer no longer emits) — that doc needs its own refresh, out of scope for this pass since it's an
MTDS-owned doc.

**Sole Source Rule**: API-Football is the sole source of truth for reference data. If a league, team, fixture,
player, venue, or referee does not exist in API-Football, it does not exist in our universe. All other providers
(FootyStats, Understat, SoccerFootball, Transfermarkt, Open-Meteo, Odds API) are enrichment or market-data only.
**Real enforcement, confirmed both in code and in the batch/live split**: the pre-flight gate lives in
`instruments-service/instruments_service/reference_data/sports_dependency.py::check_api_football_dependency()`,
called from `sports/factory.py::create_sports_reference_adapter()` for every venue in
`_API_FOOTBALL_DEPENDENT_VENUES` (`footystats`, `understat`, `transfermarkt`, `soccer_football_info`, `open_meteo`,
`betfair`) — it raises `DependencyError` (with an actionable remediation CLI command) if API-Football's fixtures
parquet is missing for the target date, checked BEFORE any enrichment adapter call runs. There is no separate "live"
code path to separately re-check: Sports "live" is literally the same batch CLI invoked with `--start-date
{today} --end-date {today}` at fixture-proximate times (see the trigger-scheduler discussion under Batch → Live
below), so this same pre-flight gate runs for every real invocation, batch or live-triggered, with zero duplication
or drift risk between the two.

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

**RESOLVED 2026-07-08 — the one-builder architecture landed today and Sports is already wired in correctly.** The
operator's decision to build one shared entry point ("one builder for everything... every asset group, every
instrument type, can get its canonical instrument IDs, same with fixtures, just by filling in the right inputs";
see `unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`) shipped in
`unified-api-contracts` this session (commit `7c0f45dd`, "add `build_canonical_instrument_id` one-entry-point
builder"). The new dispatcher (`unified_api_contracts/internal/reference/canonical_id_builder.py`) routes
`asset_group="sports"` straight to this same `build_fixture_id()` and its own docstring now correctly documents the
shape as `LEAGUE:MATCHUP:DATE`, explicitly calling out that it is **not** `VENUE:TYPE:SYMBOL` and that this is "by
design... sports doesn't have a clean TYPE/SYMBOL concept," not a gap. The mis-citation flagged in an earlier pass of
this doc is gone — verified by reading the current file directly (no further edit needed here). Remaining, still-open,
smaller retrofit items for Sports are tracked in
`unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md` (see "Known gaps" below for
the one that applies to Sports — the Betfair `/` delimiter).

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

### Reference / Features tier leagues — the other 61 of the real 94-league universe (verified 2026-07-08)

The 33 Prediction leagues above are only the TRADEABLE slice. UAC `LEAGUE_REGISTRY`
(`unified_api_contracts/canonical/domain/sports/league_classification_data_a.py` + `_b.py`) carries **94 total
football leagues**, each tagged with a real `LeagueClassificationType` (`Prediction` / `Features` / `Reference` —
confirmed these are the only 3 values, `league_registry.py:221-226`): **33 Prediction + 22 Features + 39 Reference =
94**, counted directly from the classification field (not estimated). `_mvp_football_league_ids()`
(`unified_api_contracts/canonical/crosscutting/mvp_scope.py:317`) is the real helper that unions all three tiers into
the full captured universe.

- **Features tier (22 leagues)**: not tradeable (no Odds API coverage), but still real, in-scope leagues API-Football
  covers and instruments-service captures — used for cross-league context (e.g. second divisions of the 33 Prediction
  countries not already listed above, feeding the same team-form/history calculators).
- **Reference tier (39 leagues)**: lower divisions, cups, continental competitions, and youth/reserve leagues (real
  examples captured live this session: MLS Next Pro, Copa Chile, Brazil Serie B, various U20 competitions — seen
  directly in a real `sports_reference/by_date/day=2026-07-06/pipeline_mode=batch_api_football/entity=fixtures/`
  read, several leagues still numeric-`league_id`-keyed rather than canonicalized to a human-readable code, because
  `_canonical_league_id()`'s numeric-resolution pass only covers leagues UAC has named — a real, minor gap, not the
  same bug as the `"UNKNOWN"` sentinel documented below).
- **Real capture status**: confirmed via GCS reads (2026-07-06) that Reference-tier fixtures ARE genuinely fetched
  and written to the same `sports_reference/by_date/.../entity=fixtures/` path as Prediction-tier fixtures — the
  write-universe gate (`_is_in_canonical_write_universe()`) scopes to `get_expected_leagues_for_source("api_football")`,
  which returns all 94 leagues, not just the 33. So the raw fixture/team/standings DATA for the other 61 leagues is
  real and present.
- **Operator's stated purpose — now implemented end-to-end (updated 2026-07-08, features-service sub-agent pass)**:
  the intent (per the operator) is using this non-tradeable data for (a) a promoted/relegated team's prior-season
  form from its previous (lower) division, (b) fixture-congestion / schedule-density context, (c) a historical
  first-season-back baseline for promoted/relegated teams, and (d) injury-data carryover for starting-lineup
  predictions. All four are now real and wired into the real batch/live compute path (`export_derived_features()`,
  invoked from `features_service/sports/cli/handlers/batch_handler.py` — the same entrypoint batch AND live use).
  - **(a) Promotion/relegation cross-league historical form — NOW WIRED.** The previously-orphaned
    `promoted_team_handler.blend_promoted_features()` (a `LEAGUE_STRENGTH`-ratio decay-weighted blend of a team's
    prior-division form, described above as real-but-unwired) is now invoked by a new calculator group,
    `features-service/features_service/sports/calculators/promoted_team_features_calculator.py`
    (`compute_promoted_team_batch`), registered in `feature_catalog.py`'s `DERIVED_CALCULATOR_GROUPS["promoted_team"]`
    and run from `derived_features_exporter.py::_run_history_based_calculators` alongside team*form/team_xg/h2h — the
    same multi-league, multi-season `history` DataFrame those calculators already consume (built by
    `_build_team_history`, which spans all 94 UAC-classified leagues, not just the 33 Prediction-tier ones) is reused
    directly, so no new data plumbing was needed. For each fixture's home/away team, it detects promoted/relegated
    status via the existing `is_promoted_team()` threshold (fewer than `MIN_MATCHES_FOR_STABLE`=10 matches played in
    the current league+season), finds that team's most recent season in a DIFFERENT league in the same `history`, and
    blends current-vs-previous-division form (`promoted_blend_ppg`/`_win_rate`/`_goals_scored_std`/
    `_goals_conceded_std`/`_clean_sheet_rate`/`_conversion_rate`, home*/away\_ prefixed) via the real
    `blend_promoted_features()` — unchanged, still the same tested decay/strength-ratio math, just finally called
    from a real compute path. `season_context.py`'s `is_promotion_relegation` (current-season relegation-battle flag)
    and `team_form.py`'s same-league `prev_season_ppg` remain untouched, distinct concepts.
  - **(c) Promoted-team first-season historical baseline — NEW feature, real data.** The same calculator also adds
    `promoted_cohort_avg_ppg` / `promoted_cohort_sample_size` (home*/away* prefixed): the real historical average
    points-per-game that OTHER teams making the SAME cross-league transition (e.g. `ENG_CHAMPIONSHIP` -> `EPL`)
    achieved in their own first captured season at the new level, computed directly from real match-level rows in
    `history` (wins/draws/losses from real `goals_for`/`goals_against`, not synthetic numbers) — an honest-absence
    `0.0`/`0` when the captured lookback window has no other team on record making that transition, never a
    fabricated baseline. Verified end-to-end against a real example: Luton Town's actual 2022-23 Championship
    promotion (3rd place, 80 points, 21W-17D-8L) into their real first 9 Premier League 2023-24 matches (1W-2D-6L,
    matches_played=9 < 10 so still in the blend window) — see
    `features-service/tests/sports/unit/calculators/test_promoted_team_features_calculator.py`.
  - **(d) Injury-data carryover — was ALREADY real and wired, not a gap.** Checked the real feature registry rather
    than assuming: `features-service/features_service/sports/calculators/injury_impact_calculator.py`
    (`compute_injury_impact_batch`, consuming API-Football's real `injuries` reference entity — home/away injury
    counts, severity score, key-player-injured flag, crisis indicator) is registered in
    `DERIVED_CALCULATOR_GROUPS["injury_impact"]`, run from `derived_new_calculators.py::run_new_calculators` (same
    real `export_derived_features()` path), and its Phase-0 output feeds a Phase-1 `replacement_model` calculator
    (`depends_on=["player_lineup", "injury_impact"]`) that scores expected quality-drop/tactical-distortion from
    missing players — i.e. real captured injury data already flows into a starting-lineup-strength feature today. No
    new work was needed here; this doc previously did not call this out explicitly.
  - **(b) Fixture congestion / schedule density — REAL, implemented, multiple wired-in calculators.** `team_form.py`
    computes `days_rest`, `games_last_7d`, `games_last_14d`, `games_per_week`; `venue_context.py` computes
    home/away `days_since_last_match`; `h2h_calculator.py` computes `h2h_days_since_last`;
    `bucketed_features_calculator.py` buckets rest days into bands; `european_fatigue_calculator.py` adds
    European-competition-specific congestion (`days_since_european`, `european_matches_season`,
    `double_fixture_week`). All confirmed present in the real `DERIVED_CALCULATOR_GROUPS` registry — this half of
    the operator's stated design intent is genuinely live today.

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

**Coverage root cause — real investigation, 2026-07-08 (live API calls + static-mapping reads, not re-guessed):**

- **FootyStats (~73%)**: live-queried FootyStats' own `/league-list?chosen_leagues_only=true` endpoint (real API
  call, our production `footystats-api-key`) — our account's chosen-leagues subscription has **49 leagues total**,
  and cross-referencing by name against our 33 Prediction leagues found **3 genuinely, entirely absent**: Austria
  Bundesliga, Greece Super League, and Australia A-League are simply not in our FootyStats plan at all — confirmed
  real, provider/plan-side, not a bug on our end (there is nothing to fetch — they aren't offered). That accounts
  for ~9% of the 33 leagues being structurally uncoverable regardless of fetch logic. The remaining shortfall (down
  to ~73% from the ~91% the 30-covered-leagues ceiling implies) must come from per-fixture matching within the other
  30 leagues — I could not fully root-cause that residual gap in this pass (would need per-fixture live spot-checks
  across many leagues/dates); flagging as a real, scoped follow-up. One genuine naming-mismatch risk worth noting:
  several of our league names differ from FootyStats' own display names for the same competition (our
  `JUPILER_PRO` is FootyStats' "Belgium Pro League"; our `PRIMEIRA_LIGA` is "Portugal Liga NOS"; our `SUPER_LIG` is
  "Turkey Süper Lig" with a different diacritic) — worth checking our fetch code resolves leagues by FootyStats'
  numeric `footystats_id` (which `team_mapping_v2.parquet`/`league_mapping.parquet` both carry) rather than by name,
  since a name-based match would silently miss these three.
- **SFI / Soccer Football Info — RESOLVED 2026-07-08, was a stale figure, not a live data gap.**
  `SOCCER_FOOTBALL_INFO_IDS` (`unified-api-contracts/unified_api_contracts/canonical/domain/sports/provider_league_ids.py:30-64`)
  has a real, static hex-id entry for **all 33 of 33** Prediction leagues (verified by direct read, zero missing), so
  "missing whole leagues" was already ruled out. This pass went further and traced the ACTUAL coverage-% code
  end-to-end: `deployment-api/deployment_api/services/data_status/breakdowns_domain.py::_build_sports_entity_entry()`
  calls `deployment-api/deployment_api/services/data_status/sports_helpers.py::sports_honest_coverage()`, whose
  `SPORTS_DATA_TYPE_META["SFI_PROGRESSIVE_STATS"]` entry uses axis `per_league_per_fixture_date` — i.e. it's
  denominated on the real per-fixture calendar across the 33 SFI-expected leagues, which is the RIGHT grain for this
  data_type (SFI progressive stats genuinely are one row per fixture). I downloaded the live production manifest
  (`gs://instruments-store-sports-prd-central-element-323112/_index/availability_index.parquet`, 78.9 MiB, 4,955,493
  rows, pulled 2026-07-08) and called this real function directly (not re-implemented, not guessed) against it:
  **SFI_PROGRESSIVE_STATS = 63,798 / 63,862 expected (league, fixture-date) shards = 99.9%** over the full 2014-2026
  history, and **14,627 / 14,691 = 99.56%** over the current era (2025-01-01 to 2026-07-08). There is no metric bug
  AND no meaningful live data gap — the previously-recorded "~38%" figure in earlier passes of this doc did not
  reflect the real production honest-coverage code or real manifest data; it was stale and is superseded by the
  numbers above.
- **Transfermarkt — RESOLVED 2026-07-08, the denominator-grain bug was real but was already fixed before this
  pass, on 2026-06-11 (`deployment-api@6b7aa696`), a full month before this investigation.** `TRANSFERMARKT_IDS`
  (`provider_league_ids.py:67-100`) covers **32 of 33** Prediction leagues (only Greek Super League lacks a
  Transfermarkt code), so missing-leagues was never the primary explanation. The real framing issue described in the
  previous pass of this doc — Transfermarkt's captured entity is `player_values`, a team/league VALUATION snapshot,
  not a per-fixture artifact — turns out to already be fixed in code: `SPORTS_DATA_TYPE_META["PLAYER_VALUES"]` in
  `sports_helpers.py` uses axis `per_league_trigger_date` (NOT `per_league_per_fixture_date`), denominated on the
  real count of trigger dates per league (season-start + transfer-window-open + transfer-window-close, via UAC
  `get_reference_refresh_dates`) — each trigger-date shard bundles the whole league's team valuations, so this
  axis is the correct team/league-season grain, not a fixture count. Verified by direct read of the code (the fix
  landed in the same commit that decomposed the 6,663-line `data_status_service.py` god-module) AND by running the
  real function against the real manifest downloaded above: **PLAYER_VALUES = 2,564 / 3,400 expected
  (league, trigger-date) shards = 75.41%** over the full 2014-2026 history (54 of the 55 UAC-declared leagues had
  ≥1 real trigger date in-window), and **439 / 441 = 99.55%** over the current era (2025-01-01 to 2026-07-08). The
  75.41% all-time figure reflects genuine historical-backfill incompleteness (older seasons, mostly pre-2025, only
  partially backfilled with player-value snapshots) — a real but already-tracked data-completeness gap, not a live
  pipeline defect and not a metric bug. **No code change was needed in this pass** — the denominator-correctness fix
  already existed; the previously-recorded "~41%" figure in earlier passes of this doc predates that fix (or was
  never recomputed against it) and is superseded by the numbers above. Also still true: Transfermarkt "has no
  official public API" (confirmed via the adapter's own docstring) — access is via an unofficial RapidAPI wrapper or
  an Apify scraper, less complete/stable than FootyStats' documented official API.
  **Separate, unrelated, NOT fixed this pass** (flagged, low-priority, no live impact): `deployment-api`'s
  older per-VENUE breakdown path (`deployment_api/services/data_status/venue_resolution.py::_resolve_expected_dates`)
  has a docstring claiming a "Transfermarkt → transfer-window dates" priority branch, backed by real helper functions
  (`sports.py::_is_transfer_window_venue`, `venue_resolution.py::_resolve_transfer_window_dates`,
  `_is_understat_venue`, `_resolve_understat_fixture_dates`) — but none of the four are ever called from
  `_resolve_expected_dates` (confirmed via grep across the repo); they're dead code and the docstring is inaccurate.
  Confirmed this does NOT affect the numbers above: the only manifest rows with `venue=TRANSFERMARKT`/`transfermarkt`
  (92 rows, all `capture_status=attempted_failed`, and 2,991 unrelated rows from 2018 tagged with prediction-market
  `data_type`s like `arbitrage_opportunity`) are legacy/orphaned artifacts, not real Transfermarkt data — the real
  `PLAYER_VALUES` rows (272,212 of them) carry a blank `venue` column and are keyed by `data_type`, going through
  `sports_honest_coverage()` above instead. Left as dead-code cleanup for a future pass.

**Betfair** (`sports/adapters/betfair.py`) is a distinct, separately-registered reference-data adapter — it goes
through the general `reference_data/factory.py` (as a `BaseReferenceDataAdapter`), not the sports-domain
`sports/factory.py` (whose adapters extend `BaseSportsReferenceAdapter`). It surfaces Betfair's `listMarketCatalogue`
runners as `InstrumentRecord`s with `instrument_type=EXCHANGE_ODDS`. See "Known gaps" below for a real format bug in
its `instrument_key`.

**Betfair's real current state, and the real "4 live adapters" (verified 2026-07-08):**

- **Is Betfair reference data live, static, or nothing?** The adapter code itself is real and live-capable — it
  hits `https://api.betfair.com/exchange/betting/json-rpc/v1` (`listMarketCatalogue`) with a session token +
  app key from Secret Manager, not a static downloaded file. But it is **not currently scheduled anywhere**: no
  Cloud Scheduler cron references it (`gcloud scheduler jobs list` — zero Betfair hits), no launcher script in
  `deployment-service/scripts/vm/` fetches it (the only "betfair" hits there are unrelated `betfairlightweight`
  dependency-install comments), and there is zero real `entity=betfair*` output anywhere under
  `sports_reference/by_date/` in the prod bucket. So: real, live-capable code: yes. Actually running in production
  today: no — it is dormant.
- **The real "4 live adapters"**: `execution-service/execution_service/sports_execution/adapters/exchanges/`
  contains exactly 4 modules — `betfair.py` (via `betfairlightweight`, real order placement/cancel/list), `matchbook.py`
  (real REST API), `polymarket_clob.py` (real EIP-712 + HMAC-signed CLOB API), `kalshi.py` (real RSA-signed REST
  API) — all 4 are genuinely implemented (not stubs), matching the operator's "4 live adapters." `adapters/bookmaker_api/`
  (`onexbet.py`, `api_football.py`) is a smaller, real-API (non-scraping) bookmaker access path — kept: `onexbet` is
  one of the 20 active Odds-API-sourced bookmakers (see below) and `OneXBetAdapter` is a genuine, tested, non-stub
  HTTP integration, categorically different from the Playwright scrapers below. `adapters/aggregator/odds_api.py` is
  the same Odds API aggregator MTDS uses. `adapters/unity/` is a real, substantial scaffold for **Unity**, "a prime
  broker for sports books that exposes a single TCP connection multiplexed across 10 child books (VX/SharpBet,
  Pinnacle, Bet365, …)" (per its own docstring) — commercial turnover/rollover-gate tracking ($260k/mo
  subscription-waiver gate) is real and implemented, but "concrete I/O is intentionally stubbed — production requires
  the Unity-issued binary + real TCP framing spec," i.e. this is scaffolded-but-not-live, exactly matching "still
  deciding which broker."
- **Scrapers RETIRED 2026-07-08 per operator decision** (superseding the 2026-05-12 DEFERRED-INDEFINITELY
  scaffolding-retention call): the 14 UK/EU Playwright bookmaker-scraping modules that used to live at
  `execution-service/execution_service/sports_execution/adapters/scrapers/` (`bet365`, `bet888sport`, `betfred`,
  `betvictor`, `betway`, `boylesports`, `bwin`, `coral`, `ladbrokes`, `paddypower`, `sbobet`, `skybet`, `unibet`,
  `williamhill` — plus their shared `base_scraper.py` + `version_registry.py` infra and package `__init__.py`) have
  been **deleted outright** (source + their 14 dedicated test files + the version-registry test + the
  scraper-specific cases in `test_adapter_stubs.py`), not just left dormant. Confirmed zero real callers before
  deletion (grep across the workspace — the only references were the package's own re-export in
  `adapters/__init__.py` and their dedicated tests; already 0 rows in production and already stripped from UAC
  `VENUES_BY_ASSET_GROUP["sports"]` / `VENUE_DATA_TYPE_CAPABILITIES` and MTDS `_ADAPTER_PATHS` back in 2026-05-12).
  `bookmaker_api/onexbet.py` was explicitly evaluated and **kept** — it is a real, non-scraping API adapter for an
  actively-used bookmaker, not part of this retirement. See `unified-trading-pm/plans/epics/sports_master.md` §
  "Scrapers retired 2026-07-08 per operator" for the canonical provenance record.

### Bookmakers (20, via Odds API — MTDS market data, not instruments-service reference data)

`pinnacle, betfair_ex_uk, matchbook, betonlineag, lowvig, onexbet, marathonbet, bovada, betsson, unibet, unibet_uk,
livescorebet, skybet, paddypower, betway, coral, boylesports, leovegas, casumo, virginbet`

**Markets**: h2h (match odds), totals (over/under), spreads (handicap). **Time buckets (14 per fixture-day)**: T-24h,
T-12h, T-6h, T-90m, T-80m, T-70m, T-60m, T-50m, T-40m, T-30m, T-20m, T-10m, T-0, HT.

Note: `docs/specs/MVP_INSTRUMENTS.md` no longer exists (the `docs/specs/` directory itself was removed in the
2026-07-08 docs consolidation) — confirmed via a direct filesystem check. Its predecessor content covered
CeFi/DeFi/TradFi only and had zero sports content even while it existed, so nothing is lost for Sports. This doc,
and the real adapter registry it's sourced from, are the sports MVP-universe reference.

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

**Step 3/4 corrections, real and verified 2026-07-08 (this was a stale claim in an earlier pass of this doc):**

- **Teams IS genuinely fetched fresh every real day in production today** — NOT a cheap no-op most days. Confirmed
  via real GCS reads: `entity=teams/league={L}/teams.parquet` exists, freshly written, for all 33 leagues on
  2026-07-01 through 2026-07-06 (each date's file has a distinct `available_at` stamp). This is a genuine daily
  refetch; real cost is roughly 33 AF `/teams` + 33 `/standings` calls per real invocation (the in-process
  `_cached_teams_df` cache means one CLI invocation covering a multi-day rolling window only pays this cost ONCE,
  not once per day in that window — confirmed by file-creation timestamps clustering within seconds of each other
  across a 3-day window written by one process run). **Root cause, traced to the real production cron
  (`is-daily-enum-sports`, Cloud Scheduler `30 13 * * *`, confirmed executing daily via `gcloud run jobs executions
list`)**: it invokes `instruments-service/scripts/daily_is_enumeration.py` → `--operation instruments --mode batch
--asset-group sports --start-date {D-2} --end-date {D} --force` with NO `--sports-entity` scoping, so
  TEAMS/STANDINGS get fetched unconditionally alongside everything else, every day. **There IS a season-boundary-gated
  design already built for exactly this** (`deployment-service`'s `SportsTriggerScheduler` Tier-2 `reference` tier,
  `configs/sports-trigger-tiers.yaml`) — it WAS silently non-functional in production from 2026-06-24 to 2026-07-08
  due to a CLI/deployment wiring gap, filed and then fixed the same day as
  `unified-trading-pm/plans/active/issues/sports_trigger_scheduler_cloud_dispatch_broken_2026_07_08.md` (see the
  Phase B section below for the fix + real-infra verification). **Real cost**: modest (roughly 66 AF calls per
  invocation, not per day of range), so this was not an urgent API-budget emergency — but it was genuinely wasteful
  relative to the season-boundary design, which is now dispatching for real again; re-evaluate narrowing
  `is-daily-enum-sports`'s unconditional TEAMS/STANDINGS scope only after the season-boundary path proves reliable
  over a real season boundary (don't create a coverage gap in between).
- **Team mappings — the doc's "6,245 teams x 5 providers" claim was pointing at the WRONG (stale) file.**
  `sports_reference/mappings/team_mapping.parquet` (the path this doc previously cited) is a small, legacy,
  incomplete file — **76 rows, 2 leagues (EPL/Bundesliga only), 2 of 5 providers** (`odds_api_name`/`understat_name`
  only; no FootyStats/SFI/Transfermarkt columns). The REAL, current 6,245-row file is
  `sports_reference/mappings/team_mapping_v2.parquet` — **one single flat parquet, not partitioned by season, year,
  or day at all** (directly answers the operator's question: it's one file with everyone in it, not split up), 18
  columns including a real per-provider id+name pair for all 5 enrichment providers (`api_football_id/name`,
  `footystats_id/name`, `understat_id/name`, `sfi_id/name`, `transfermarkt_id/name`) plus `odds_api_name`, `league`,
  and Transfermarkt-sourced squad/market-value snapshot columns. The doc's number was directionally correct, just
  citing the wrong (superseded) filename — corrected above. Separately, real, additional mapping files exist that
  this doc did not previously mention: `odds_api_team_mapping.parquet` (658 rows, AF-id-keyed, Odds-API-name-only),
  `league_mapping.parquet` (605 rows, one row per `(league, season)` — season-partitioned as ROWS in one file, not
  separate files), and `sfi_league_mapping.parquet` (50 rows). Transfermarkt ALSO separately writes a genuinely
  per-season file structure at `sports_reference/mappings/season={YYYY}/transfermarkt_league_teams=/teams.parquet`
  (real years present: 2014, 2017-2026) — this is a different artifact from `team_mapping_v2.parquet`, not a
  duplicate.

### Step 9: Market data — odds (MTDS -> GCS)

| What                       | Source                 | Refresh                         | GCS Path (real, current — confirmed 2026-07-08 by direct read of the writer)                                                                                                                     |
| -------------------------- | ---------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Odds + betting instruments | Odds API v4 historical | 14 time buckets per fixture day | `raw_tick_data/by_date/day={date}/pipeline_mode=batch_odds_api/asset_group=sports/venue={BOOKMAKER}/league_id={LEAGUE}/fixture_id={FIXTURE}/instrument_type=odds/data_type=trades/ticks.parquet` |

**API cost**: `bookmakers=` param (not `regions=`) for 4x lower credit usage. Historical: `10 × 3 markets × 1 = 30
credits/call`. Live: `3 markets × 1 = 3 credits/call`. Per day (batch): `30 × 14 buckets × 33 leagues = 13,860
credits`. 80-day backfill = ~1.1M credits.

Note: the path above (`venue={BOOKMAKER}`, real per-bookmaker value like `PINNACLE`, not the literal string
`ODDS_API`) is the REAL current writer output (`market_tick_data_service/engine/orchestrator/venue_fetch.py`'s
`_build_sports_shard_path()`), verified this session — it differs from what MTDS's own `docs/SPORTS_ODDS.md` still
documents (an older `raw_tick_data/by_date/day={date}/venue=ODDS_API/ticks.parquet` shape, plus a schema table
listing `time_bucket`/`m_time` columns the real writer no longer emits). That's a real doc-drift issue in a doc this
repo doesn't own; flagging here since it's directly relevant to Sports, not fixed in this pass.

**Schema**:

```
instrument_id: FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}
venue: {bookmaker_key}
event_id, sport_key, home_team, away_team, commence_time
market_key, outcome_name, price, point
fetch_utc, kickoff_utc, minutes_to_kickoff, bm_minutes_to_kickoff, staleness_seconds, source, data_type, league_id, date
```

The `instrument_id` shape (`FOOTBALL:{BOOKMAKER}:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}`) is real and
current — confirmed by direct read of UAC's `build_instrument_id()`. The column list above is corrected from an
earlier stale pass of this doc, which listed `time_bucket`/`bm_time`/`m_time` — those are not real columns in the
current writer (`odds_api_adapter.py::_build_fixture_rows()`); the real columns are as shown, confirmed by direct
read of the row-construction code.

**A real, currently-unresolved identity gap (surfaced 2026-07-08, flagging per instruction rather than resolving
unilaterally)**: this `instrument_id` never embeds instruments-service's canonical fixture id (neither the raw
`af_fixture_id` nor the `LEAGUE:MATCHUP:DATE` form) — MTDS derives `{HOME}`/`{AWAY}` from the Odds API's own
team-name strings independently, even though it separately reads instruments-service's fixture parquet for manifest
purposes (see the Overview section above). There is no row-level join key from an odds tick back to a specific
instruments-service fixture record today. Is this an acceptable gap (fixtures are already uniquely identified by
`(league, home, away, date)` in both places, so a join can be done by those fields without a shared id) or does it
need a real fix (thread `af_fixture_id` through the odds schema)? This is a real architecture question for the
operator, not resolved in this pass.

**CLI**: `python -m market_tick_data_service.cli.main --operation download --mode batch --asset-group SPORTS
--start-date {date} --end-date {date}`

### Steps 10-11: Features (FSS -> GCS)

| Step | What                                               | Source                    | Output                                                                        |
| ---- | -------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| 10   | Derived stable (form, standings, goals)            | instruments-service GCS   | `features-sports-*/sports_features/by_date/day={date}/feature_group={group}/` |
| 11   | Derived complex (xG, weather, odds microstructure) | Multi-provider APIs + GCS | Same path                                                                     |

**Real, current counts (corrected 2026-07-08 — the previous "23 calculators, 672 features" figure was stale, from a
2026-03-27 snapshot)**: the real SSOT, `features-service/features_service/sports/schemas/feature_catalog.py` (its own
docstring: "the SSOT for what features the pipeline produces"), has **32 calculator groups** wired into
`DERIVED_CALCULATOR_GROUPS` (its own docstring undercounts this as "22 calculators" — also stale, confirmed by
directly importing and counting the dict). Live-computed real totals: **970 derived features + 140 odds features +
28 fixture features = 1,138 total** — genuinely past "about a thousand" per the operator's own estimate, and
includes our own xG variant among the derived calculators. The literal `672` figure survives as a stale hardcoded
example value in `features_service/sports/api/sse_stream.py`'s docstring — likely the actual source of the number
this doc previously carried forward. Coverage areas: odds microstructure, team goals/form/derived, h2h, league
context, advanced stats, player lineup, halftime, xG (incl. Poisson xG), venue context, weather, steam detection,
referee, season context, European-competition fatigue, bucketed rest-day features.

**Resolved 2026-07-08 (`features-service@4d57c766`)**: the legacy tracking system flagged above —
`features_service/sports/tracking/registry.py` (+ its `_registry_data_*` modules, 1,057 named entries, 10 marked
`FeatureStatus.COMPLETE`) — was **dead scaffolding, not a real feature backlog, and has been deleted**. Evidence: (1)
zero real consumers workspace-wide — only its own unit test (`tests/sports/unit/test_registry.py`) imported it; (2)
`git log --follow` shows it entered the repo in one shot via the `features-sports-service` git-subtree merge
(`b144552d`) and was never touched again except mechanical refactors (type-ignore sweeps, file-size splits) — never
a content update; (3) even its strongest claim to accuracy, the 10 "complete" entries, was mostly wrong: only 4
(`steam_detected_home/away`, `steam_magnitude_home/away`) correspond to a real computed feature (in
`exporters/odds_features_exporter.py::_compute_steam_features`), and all 10 were mis-attributed to
`calculators/steam_detector.py` — a real, live module, but an unrelated real-time execution-signal detector
(`SteamDetector`/`SteamMoveSignal`), not a feature calculator; the other 6 entries (`steam_flag_*`,
`steam_timing_*`, `league_steam_frequency`) don't exist anywhere in the real calculators. Deleted rather than
migrated (no real backlog content to preserve); stale doc references to its `FeatureRegistryEntry` taxonomy were
also cleaned up in the same commit. `feature_builder_registry.py` (the live calculator-group dependency DAG used by
the pipeline exporter) is a separate, actively-used file and was untouched.

**FSS fetch/compute cadence — real, current design (answers "what is FSS fetch" and the 60-minute-poll concern):**
"FSS" = **Features Sports Service**. Live-mode compute is genuinely **event-driven**, not a fixed poll: FSS
subscribes to a PubSub topic (`sports-odds-ready`) via `features_service/sports/app/pubsub/subscriber.py`, and
`cli/handlers/live_handler.py` documents this explicitly as the live trigger. Batch mode (backfill / T+1
reconciliation) is a date-range CLI loop on its own cron (`30 2 * * *` per
`deployment-service/terraform/gcp/t1_batch_scheduler.tf`, plus a cluster-level `0 6 * * *` batch cron) — there is no
fixed intra-day poll loop inside FSS itself. **The real fixed-60-second poll the operator may be recalling lives one
service upstream**, in MTDS's own Odds API ingestion
(`market_tick_data_service/live/connectors/odds_api_ws.py`, `_DEFAULT_POLL_INTERVAL_S = 60.0`, "no native
WebSocket... polling loop with a 60-second interval") — 60 seconds, not 60 minutes, and it's the Odds API ingestion
cadence, not the feature-compute cadence. **Not fully resolved**: I could not find, in the time available, the real
production code that PUBLISHES to `sports-odds-ready` (only FSS-side consumers and tests reference the topic) — so
the exact hop from MTDS's 60s Odds API poll to FSS's event-driven trigger is real and wired on the consumer side, but
I couldn't confirm the publisher side this pass. Flagging as a real open question rather than asserting it's fully
verified end-to-end.

**CLI**: `python -m features_sports_service.cli.main --operation compute --mode batch --start-date {date} --end-date
{date}`

## GCS bucket layout

```
gs://instruments-store-sports-{env}-{project}/
  sports_reference/
    by_date/day={YYYY-MM-DD}/pipeline_mode={mode}_{source}/entity={type}/league={LEAGUE}/{type}.parquet
                                                               (real, current instruments-service output —
                                                                confirmed live 2026-07-08; {mode}_{source} e.g.
                                                                batch_api_football, batch_footystats,
                                                                batch_soccer_football_info, batch_transfermarkt)
    fixtures/day={YYYY-MM-DD}/fixtures.parquet                (backfill, legacy pre-pipeline_mode layout)
    fixture_stats/day={YYYY-MM-DD}/stats.parquet              (backfill, legacy pre-pipeline_mode layout)
    fixture_events/day={YYYY-MM-DD}/events.parquet            (backfill, legacy pre-pipeline_mode layout)
    venues/venues.parquet                                      (with lat/lon)
    teams_in_league/season={YYYY}/teams.parquet               (backfill, legacy)
    footystats_league_ids/season={YYYY}/ids.parquet           (backfill, legacy)
    standings/season={YYYY}/standings.parquet                  (backfill, legacy)
    mappings/team_mapping_v2.parquet                           (real current file — 6,245 teams x 5 providers,
                                                                one flat unpartitioned parquet; team_mapping.parquet
                                                                still exists but is a stale 76-row/2-provider file)
    mappings/season={YYYY}/transfermarkt_league_teams=/teams.parquet   (Transfermarkt-specific, real per-season)
  instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet

gs://market-data-tick-sports-{env}-{project}/
  raw_tick_data/by_date/day={YYYY-MM-DD}/pipeline_mode=batch_odds_api/asset_group=sports/venue={BOOKMAKER}/
    league_id={LEAGUE}/fixture_id={FIXTURE}/instrument_type=odds/data_type=trades/ticks.parquet
                                                               (real, current MTDS writer — confirmed 2026-07-08;
                                                                venue= is the bookmaker, not the literal ODDS_API)

gs://features-sports-{project}/
  sports_features/by_date/day={YYYY-MM-DD}/feature_group={group}/features.parquet
```

All paths are hive-partitioned, BigQuery-compatible. Timestamps coerced to microseconds. **Sports DOES carry
`pipeline_mode=` in its real GCS paths, confirmed on both the instruments-service side
(`sports_dependency.py`'s `batch_api_football`, and the real `batch_footystats`/`batch_transfermarkt`/
`batch_soccer_football_info` variants seen in production) and the MTDS side (`pipeline_mode_for_source("odds_api")`
→ `PipelineMode.BATCH_ODDS_API`) — this directly answers the "do we even need pipeline\*mode for sports, since it's
slow-moving" question: yes, it's already there and in active use, source-scoped per the workspace's
`{mode}*{source}` convention, not a design gap.**

## Data counts (as of 2026-03-27 — still a stale snapshot; the feature/calculator counts above WERE re-verified live

this pass, but this backfill-row table was not re-derived — real row counts here would need a fresh GCS aggregation
pass, out of scope for this round; flagging the staleness explicitly rather than silently repeating it as current)

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

These are real findings surfaced by the 2026-07-08 canonical-instrument-id audit and re-investigated in a same-day
follow-up session with real GCS reads and live API calls (not just static code reading) — genuine
data-completeness/format issues, distinct from the by-design ID-scheme decision above. Two items below (the
`canonical_id_builder.py` docstring, and the Betfair delimiter's framing) have since resolved or been correctly
re-scoped by a concurrent sibling agent's 2026-07-08 work; the reference-catalog and `"UNKNOWN"`-league-id findings
remain genuinely open — see the linked plan/issue docs for the concrete next steps on those.

### The real reference catalog is bare — CONFIRMED genuinely bigger than a single bug, now scoped into a plan

Per real `prod/catalog.parquet` reads (confirmed 2026-07-08, re-confirmed in the 2026-07-08 follow-up): `venue` is an
empty string for all 116 real rows, one row's key is the literal sentinel `"UNKNOWN"`, and only league-level entities
exist. The follow-up traced the real root cause by reading the actual builder code
(`scripts/build_instrument_catalogue.py`): this is **not** a silently-broken write path — `asset_group == "sports"`
dispatches to `build_sports_catalogue_from_manifest()`, which is a **documented, deliberate 2026-06-07 design
decision** to scope the sports "could-exist" catalog to league grain only, because the captured manifest atom itself
is per-`(league_id, data_type, date)` with no fixture/team/player grain (a fixture-grain catalogue would inflate
`expected_unattempted` against a manifest that can never match it). The 11-step pipeline above genuinely does write
fixture/team/player reference DATA to GCS — that part is real — it just never gets rolled into catalog/coverage rows.
Fixture/team/player-grain catalog + coverage tracking for Sports was **never implemented**, not silently broken.
Scoped into `unified-trading-pm/plans/active/sports_catalog_league_grain_only_scope_2026_07_08.md` (operator decision
needed on whether fixture-grain coverage tracking is even wanted, plus the manifest-schema work it would require).

**`venue` vs `source` — corrected terminology (2026-07-08 — an earlier pass of this doc conflated the two).** The
empty `venue` string above is CORRECT, by design, not a bug: `venue` is a bookmaker concept, and sports reference-data
rows (fixtures/teams/leagues) genuinely have no bookmaker association, so an empty `venue` is the honest value — not
evidence of a missing/broken field. `source`, by contrast, is a real, SEPARATE column that DOES need to be populated
(it identifies the upstream vendor — `api_football`, `footystats`, etc.). The catalog builder
(`build_instrument_catalogue.py`'s `CATALOG_COLUMNS`) does not even carry a `source` column for the catalog artifact
at all — the real `source` field lives on the separate manifest (`AvailabilityRecord`, populated via
`record_captured(..., source=...)`), not on the catalog. No real bug here — just a naming mix-up in an earlier pass
of this doc, now corrected.

Pulling on the `"UNKNOWN"` sentinel row surfaced a separate, real, **currently-active** data-correctness bug: the
underlying manifest (`_index/availability_index.parquet`) has **2,373 rows** with `league_id="UNKNOWN"` across all 17
sports data_types, dated 2025-12-15 through **2026-07-08 (today)** — not a historical artifact, still recurring.
**2026-07-08 follow-up, re-verified with real data**: sampling the actual 2,373 rows found ALL of them are
`capture_status ∈ {expected_unattempted, empty_confirmed}` — ZERO are `captured`. This re-characterizes the bug: it
is a phantom "UNKNOWN" pseudo-league polluting the honest-absence/gap-fill BOOKKEEPING side (the denominator of
coverage tracking), not real fetched data being silently mislabeled under the wrong league. Two more root-cause
candidates were checked and ruled out this session (an HTTP-error-classification `"UNKNOWN"` in the shared sports
adapter base class, and a bad static seed row in `LEAGUE_REGISTRY` — neither is the source). Root cause is still not
pinned to an exact write call site; a same-session data migration was deliberately NOT attempted since the correct
per-row substitution value isn't known yet (rewriting to a guess would risk new, differently-wrong data). Updated
with all of this new evidence:
`unified-trading-pm/plans/active/issues/sports_manifest_unknown_league_id_2026_07_08.md`.

### Betfair: real `/` delimiter in `instrument_key` — correctly NOT a `VENUE:TYPE:SYMBOL` violation; still open, already tracked

**Framing correction (2026-07-08)**: an earlier pass of this doc flagged Betfair's `/`-delimited `instrument_key`
(`f"{market_id}/{selection_id}"`, `instruments-service/instruments_service/reference_data/adapters/sports/
adapters/betfair.py:279`, `_build_runner_record`) as if it violated the workspace's `VENUE:TYPE:SYMBOL` convention —
but this doc _itself_ documents, in "Instrument identity" above, that Sports intentionally does NOT follow that
convention at all. The real, correctly-scoped question is whether Betfair's `/` is internally consistent with
Sports' OWN provider-native id schemes (it isn't — every other Sports id in this workspace is `:`-delimited), not
whether it matches CeFi's convention (irrelevant to Sports by design). This is now tracked as a real, still-open,
already-scoped todo in `unified-trading-pm/plans/active/canonical_id_builder_retrofit_checklist_2026_07_08.md` (filed
by the same 2026-07-08 session that shipped the one-builder architecture), which correctly states: fixing this does
NOT route through `build_canonical_instrument_id` (Sports keeps its own scheme) — it just needs its own internal
delimiter fix (`f"{market_id}:{selection_id}"`), plus updates to the two real downstream consumers that currently
parse `/` (strategy-service's `position/core/fill_event_consumer.py` `rsplit("/", 1)`, and execution-service's
`sports_execution/adapters/exchanges/betfair_order_mapping.py`) — a 3-repo coordinated change, not a same-repo fix,
hence still open. Given item 5's finding that Betfair reference-data fetching is not currently scheduled/live in
production (0 real rows), this is genuinely **low priority** pending the operator's venue-scoping decision above —
no need for a duplicate issue doc; the existing retrofit-checklist todo already covers it correctly.

### `canonical_id_builder.py` docstring — RESOLVED 2026-07-08

An earlier pass of this doc flagged the file's docstring as mis-citing the sports fixture-id builder as a
`VENUE:TYPE:SYMBOL` example. Re-checked directly against the current file this session: it now correctly documents
`build_fixture_id()`'s `LEAGUE:MATCHUP:DATE` shape as explicitly NOT `VENUE:TYPE:SYMBOL`, "by design... sports
doesn't have a clean TYPE/SYMBOL concept" — fixed as part of the one-builder architecture landing today (see
"Instrument identity" above). No further action needed.

## Seasonal refresh (Phase B) — FIXED 2026-07-08: real dispatch confirmed working against production

An earlier pass of this doc described Phase B as "not yet implemented," with a 4-step spec (daily no-op check; call
AF `/leagues`; if a new season started, fetch teams/league IDs/venues; else no-op). **That's incorrect** — this
exact design already exists, in real code, in `deployment-service` (not instruments-service):
`SportsTriggerScheduler` + `PeriodicTierDispatcher`
(`deployment-service/deployment_service/sports_trigger_scheduler.py` / `sports_trigger_periodic.py`), configured by
`configs/sports-trigger-tiers.yaml`'s Tier-2 `reference` section — `TEAMS` and `LEAGUES` are gated on
`window_condition: season_boundary` (`_gate_by_season_boundary()`, tolerance ±3 days around each expected league's
real season start/end dates — real code, not a stub). `pipeline_mode` for this path is the `batch_api_football`/
`batch_*` family, matching the operator's own "batch on live, since it's slow-moving stuff" framing — confirmed
correct. Because this refresh writes through the exact same instruments-service batch CLI/orchestrator code as
every other invocation, it lands in the SAME real historical GCS structure documented above — genuine batch=live
symmetry already holds structurally, by construction, with no separate code path to keep in sync.

**It WAS non-functional in production for ~14 days (2026-06-24 -> 2026-07-08) — now fixed and verified live.** Root
cause (confirmed via real `gcloud` evidence): the CLI (`cli/commands/sports_trigger.py::sports_trigger_run`) never
passed a `backend`/`workspace_root`/`cloud_run_config` argument to `SportsTriggerScheduler(...)`, so it silently
defaulted to `backend="local"` inside the Cloud Run Job container that only ships `deployment-service` code
(`FROM api AS sports-scheduler`, Dockerfile) — every dispatch subprocess call failed immediately, invisibly (the job
still exited 0; `PeriodicTierDispatcher` only persists `last_run[tier]` when `dispatched > 0`, so the GCS state file
(`sports_scheduler_state/scheduler.json`) went stale — `last_run.reference = 2026-06-24` despite thousands of real
cron executions since then). Filed as
`unified-trading-pm/plans/active/issues/sports_trigger_scheduler_cloud_dispatch_broken_2026_07_08.md`, then fixed in
`deployment-service` the same day:

- Added `--backend`/`--workspace-root`/`--cloud-run-*` CLI options (`cli/commands/sports_trigger.py`), wired into
  `SportsTriggerScheduler(...)` for both `run` and `evaluate`; the terraform Cloud Run Job
  (`terraform/gcp/sports_scheduler_cron.tf`) now passes `--backend cloud` + the real project/region/service-account.
- `configs/sports-trigger-tiers.yaml`'s `cloud_run_job_name` fields were repointed at the REAL, already-provisioned
  per-service Cloud Run Jobs the T+1 batch reconciliation cron already dispatches into
  (`uts-prod-instruments-service-t1-recon`, `uts-prod-market-tick-data-service-fast-t1-recon`,
  `features-sports-service-job`) rather than a set of scheduler-specific jobs that were never provisioned (verified
  via `gcloud run jobs list` — zero matches for any of the old `sports-trigger-*` names). No dedicated ml-service
  Cloud Run Job exists yet, so that one entry (`inference_pre_match`) ships with an empty `cloud_run_job_name` — a
  real, separate infra gap, not silently papered over (fires a loud warning + skips instead).
- Fixed a second latent bug found in the same code path: `_dispatch_services`'s cloud branch passed the FULL
  `"python -m <module> ..."` command as the Cloud Run Jobs execution-override `args` — but Cloud Run Jobs V2 can only
  override `args`, never `command`/entrypoint, and the real target jobs (verified via `gcloud run jobs describe
uts-prod-instruments-service-t1-recon`: `command: None`, `args: ['--operation=instruments', ...]`) already bake the
  module invocation into the image's own ENTRYPOINT. Would have broken every cloud dispatch even after the CLI fix.
- **Verified against real production infra, not just tests**: instantiated the fixed `SportsTriggerScheduler` with
  `backend="cloud"` and called `_check_reference()` directly against the real GCS state file and real Cloud Run Jobs
  API. Result: `INJURIES` (`run_always: true`) and `TRANSFERS` (transfer window open for 49 leagues that day) both
  dispatched successfully via real `uts-prod-instruments-service-t1-recon` executions, and the real state file's
  `last_run.reference` advanced from the stale `2026-06-24` to `2026-07-08` — the exact staleness this bug caused,
  now cured. Unit test regressions added for the CLI wiring and the args-stripping fix
  (`tests/unit/test_sports_trigger_cli.py`, `tests/unit/test_sports_trigger_scheduler_periodic.py`).

What was keeping Sports data flowing during the outage was a separate, blunter, unconditional daily job
(`is-daily-enum-sports` — see the Step 3/4 note above); that mechanism is unaffected by this fix and can be
re-evaluated for narrowing once the season-boundary path has proven reliable over a real season boundary.

## Batch -> Live: minimal delta (corrected 2026-07-08 — the real trigger mechanism, not a Pub/Sub design)

Sports "live" is literally the same batch CLI, fired at fixture-proximate times instead of a fixed daily cron — a
real design principle stated directly in `sports_trigger_scheduler.py`'s own module docstring ("Sports 'live' =
batch with `--date today`, fired at fixture-proximate times. Same CLI, same service, just triggered by fixture
proximity instead of daily cron"), not the Pub/Sub-based design an earlier pass of this doc implied. The real
trigger tiers (`configs/sports-trigger-tiers.yaml`, deployed as `uts-prod-sports-scheduler`; see the Phase B section
above — dispatch was broken 2026-06-24 -> 2026-07-08 and is now fixed + verified live):

| Tier           | What                                                                                 | Real cadence                                                                      | Change from batch                                                         |
| -------------- | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| 1 — Discovery  | Fixture calendar + standings refresh                                                 | Rolling window (today-1..today+7), every 6h                                       | Trigger only                                                              |
| 2 — Reference  | INJURIES (daily) / TRANSFERS / LEAGUES / TEAMS (season-boundary-gated)               | Daily cadence check; season-boundary items fire only near a real season start/end | Trigger + real gating (fixed + verified live 2026-07-08 — see Phase B)    |
| 3 — Pre-match  | Odds snapshots (T-24h/T-6h/T-1h), lineups, weather, pre-match features, ML inference | Fixture-proximate, offset from real `kickoff_utc`                                 | Trigger + frequency (ML inference has no Cloud Run Job yet — see Phase B) |
| 4 — Post-match | Final stats (T+30m), delayed xG (T+24h), post-match features (T+25h)                 | Fixture-proximate, offset from real match-end estimate                            | Trigger + frequency                                                       |

instruments-service itself makes NO code distinction between batch and live — it is always the same
`--operation instruments --mode batch --asset-group SPORTS --start-date X --end-date Y` CLI contract; only the
caller (the trigger scheduler vs. a plain daily cron) and the date arguments differ. GCS paths and schema are
identical either way (confirmed above).

## BigQuery external table

```sql
-- Already created
SELECT * FROM `sports_analytics.odds_ticks_hive`
WHERE day = "2025-12-20" AND sport_key = "Premier League"
```

Note (corrected 2026-07-08): the real hive `venue=` partition segment holds the BOOKMAKER (e.g. `venue=PINNACLE`),
not the literal `ODDS_API` string an earlier pass of this doc described — see the Step 9 real-path note above. The
in-file `venue` column carries the same bookmaker value, so there is no shadowing conflict to work around in the
current schema; the earlier `time_bucket` column referenced in the sample query above is also not a real column in
the current writer (see the Step 9 schema correction above) — this BigQuery example should be re-verified against
the real current external-table DDL/columns before relying on it, which was out of scope to re-generate in this
pass.
