<!-- POST_PLAN_BANNER_2026_05_06 -->

> **POST-PLAN REALITY (2026-05-06)** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) BEFORE making code or doc changes informed by this doc. This doc is partially stale: pre-canonical_question_group + pre-lifecycle (no `market_created_at` / `resolution_time` / `settlement_time` capture). The post-plan-reality doc lists the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision, cluster validation mandatory, per-row write-time `available_at`, prediction lifecycle timing, temporary state must have named successor, per-VM shard isolation, etc.) plus the active plans where the canonical post-plan reality is being implemented (`writegate_honest_coverage_endtoend_2026_05_06.plan.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.plan.md`). If this doc and the active plans disagree, the plans win. If you find a contradiction the plans don't address, flag to user — don't decide unilaterally.

# Polymarket Prediction Market Adapter

## Overview

The Polymarket adapter fetches prediction market instruments from the Polymarket Gamma API (public, no auth required).
It handles three market categories — **crypto up/down**, **macro up/down**, and **football sports** — and normalises
them into canonical instrument IDs that match the rest of the trading system.

For sports, Polymarket is treated as **just another bookmaker** alongside Odds API and Betfair. The same canonical
fixture IDs, team names, and league IDs are used across all venues, enabling cross-venue arbitrage detection.

## Three-API Architecture

| API       | Base URL                   | Auth         | Purpose                         |
| --------- | -------------------------- | ------------ | ------------------------------- |
| Gamma API | `gamma-api.polymarket.com` | None         | Market metadata, events, series |
| CLOB API  | `clob.polymarket.com`      | None (reads) | Order book, price history       |
| Data API  | `data-api.polymarket.com`  | None (reads) | Historical trades               |

The URDI adapter uses the **Gamma API** only. UMI adapters use CLOB and Data API for market data.

## Category Routing

Markets are classified by examining `sportsMarketType`, question text, and event metadata:

| Category        | Instrument ID Format                                                        | Example                                                            |
| --------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Crypto up/down  | `PREDICTION:POLYMARKET:UP_DOWN:{ASSET}:{TF}:{DATE}`                         | `PREDICTION:POLYMARKET:UP_DOWN:BTC:1D:2026-03-22`                  |
| Macro up/down   | `PREDICTION:POLYMARKET:UP_DOWN:{INDEX}:{TF}:{DATE}`                         | `PREDICTION:POLYMARKET:UP_DOWN:SPX:1D:2026-03-22`                  |
| Football sports | `FOOTBALL:POLYMARKET:{MARKET}:{LEAGUE}:{SEASON}:{HOME}-{AWAY}::{SELECTION}` | `FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME` |

## Sports Market Processing

### Team Name Extraction

Polymarket sports markets have `outcomes: ["Yes", "No"]` for moneyline (not team names). Team names are extracted
from event metadata in priority order:

1. **`event_title`** — e.g. "Arsenal vs. Chelsea" (most reliable, 100% hit rate)
2. **`outcomes`** — for spreads where outcomes ARE team names (e.g. `["Austin FC", "Real Salt Lake"]`)
3. **`question`** — e.g. "Will Arsenal win on 2026-03-22?" or "Arsenal vs. Chelsea: O/U 2.5"

The `event_title` is extracted from `events[0].title` in the raw Gamma API response before Pydantic validation.
Common suffixes like " - More Markets" and prefixes like "Super Rugby Pacific: " are stripped.

### League Resolution

Polymarket groups matches by `series.slug` (e.g. "premier-league-2025"). The mapping
`POLYMARKET_SERIES_TO_LEAGUE` in UAC resolves these to canonical league IDs (e.g. "EPL").

### Prediction League Filter

Only leagues in `POLYMARKET_PREDICTION_LEAGUES` (23 leagues, defined in UAC `sports_mappings.py`) are kept.
Everything else — esports, cricket, rugby, F1, UFC, NBA — is dropped for sports or reclassified as
`prediction::other`. This avoids wasted API calls and unstructured data in the pipeline.

**Covered leagues (verified 2026-03-18 to 2026-03-26):**

EPL, ENG_CHAMPIONSHIP, LA_LIGA, SEGUNDA_DIVISION, BUNDESLIGA, BUNDESLIGA_2, SERIE_A, SERIE_B, LIGUE_1, LIGUE_2,
EREDIVISIE, PRIMEIRA_LIGA, SCOTTISH_PREMIERSHIP, SUPER_LIG, DANISH_SUPERLIGA, ELITESERIEN, MLS, LIGA_MX,
ARGENTINA_PRIMERA, BRASILEIRAO, A_LEAGUE, J1_LEAGUE, K_LEAGUE_1

**Not covered by Polymarket:** ENG_LEAGUE_ONE, ENG_LEAGUE_TWO, LIGA_3, GREEK_SUPER_LEAGUE, ALLSVENSKAN,
AUSTRIAN_BUNDESLIGA, SWISS_SUPER_LEAGUE, EKSTRAKLASA, JUPILER_PRO, CHILE_PRIMERA

### Team Name Normalisation

Team names are normalised via `get_canonical_team_for_polymarket()` in UAC, which maps Polymarket's formal names
(e.g. "Olympique Lyonnais", "FC Barcelona", "Real Betis Balompie") to canonical IDs (LYON, BARCELONA, REAL_BETIS).
Unmapped teams fall back to `_slug()` which produces SCREAMING_SNAKE_CASE from the raw name.

## Cross-Venue Arbitrage

The fixture part of the instrument ID is identical across venues — only the venue token differs:

```
FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME
FOOTBALL:BETFAIR_EX_UK:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME
FOOTBALL:ODDS_API:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME
```

`GROUP BY` everything except venue → compare prices → arb detection.

For joining with V3 odds data (which uses display league names like "Premier League" in GCS paths),
`ODDS_API_DISPLAY_TO_CANONICAL` in `provider_league_ids.py` provides the mapping.

## Batch Mode

When `date` is passed to `get_instruments()`, the adapter fetches ALL markets that ended on that UTC date
(including closed/resolved) using `end_date_min`/`end_date_max` query params. Pagination uses raw API page
size (not filtered count) to ensure all pages are fetched even when most markets are filtered out.

## GCS Output

Instruments are written by `instruments-service` to:

```
gs://instruments-store-prediction-{project}/
  instrument_availability/
    by_date/
      day={YYYY-MM-DD}/
        venue=POLYMARKET/
          instruments.json
```

## Key Files

| File                                             | Repo | Purpose                                                   |
| ------------------------------------------------ | ---- | --------------------------------------------------------- |
| `adapters/polymarket.py`                         | URDI | Adapter implementation                                    |
| `external/polymarket/sports_mappings.py`         | UAC  | Series→league, team→canonical, prediction league registry |
| `external/polymarket/crypto_macro_mappings.py`   | UAC  | Crypto/macro tag slugs, timeframes                        |
| `external/polymarket/schemas.py`                 | UAC  | PolymarketGammaMarket schema                              |
| `canonical/domain/sports/provider_league_ids.py` | UAC  | Polymarket series slugs, Odds API display names           |
| `canonical/domain/sports/canonical_ids.py`       | UAC  | `build_prediction_instrument_id()`                        |
| `adapters/prediction/polymarket_adapter.py`      | UMI  | Market data (odds, trades) — crypto/macro                 |
| `adapters/sports/polymarket_adapter.py`          | UMI  | Market data (odds, trades) — football                     |
