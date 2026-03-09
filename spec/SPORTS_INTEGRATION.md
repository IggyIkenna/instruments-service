# Sports Integration Spec -- instruments-service

## Overview

The instruments-service is augmented with SPORTS asset class support, treating sports fixtures as
tradeable instruments alongside CRYPTO_CEFI, DEFI, and TRADFI instruments. This follows the unified
principle: **sports is an asset class, not a separate system.**

## What Was Added

- **Sports instrument parser:** Parses the sports instrument key format
  (`FOOTBALL:BETFAIR:MATCH_ODDS:ENG-PREMIER_LEAGUE:2018-2019:LIVERPOOL-C_PALACE::LIVERPOOL`)
- **Fixture matching:** Cross-provider fixture ID mapping using API-Football as canonical source.
  Maps Betfair market IDs, Odds API event IDs, and API-Football fixture IDs to a single canonical ID.
- **Team normalization:** Standardizes team names across providers (e.g., "Crystal Palace" / "C Palace"
  / "Crystal Pal" all map to `C_PALACE`).
- **Sports reference data ingestion:** Leagues, teams, venues, and seasons via API-Football.

## Category Pattern (CEFI / TRADFI / DEFI / SPORTS)

The instruments-service already processes instruments by `market_category`. SPORTS follows the same
pattern:

| Category    | Instrument Example                                   | Provider                      |
| ----------- | ---------------------------------------------------- | ----------------------------- |
| CRYPTO_CEFI | `BTC-USDT:BINANCE:SPOT`                              | Binance, Coinbase             |
| DEFI        | `WETH-USDC:UNISWAP_V3:SWAP`                          | Uniswap, Aave                 |
| TRADFI      | `AAPL:NASDAQ:EQUITY`                                 | Alpaca, Interactive Brokers   |
| SPORTS      | `FOOTBALL:BETFAIR:MATCH_ODDS:ENG-PREMIER_LEAGUE:...` | Betfair, Pinnacle, Polymarket |

The SPORTS category uses the same `InstrumentRecord` Pydantic model, with sports-specific fields
mapped into the unified schema.

## Dependencies

### unified-api-contracts (Sports Schemas)

- `SportsInstrumentKey` -- Pydantic model for parsing and validating sports instrument keys
- `FixtureRecord` -- Canonical fixture representation
- `TeamRecord`, `LeagueRecord` -- Reference data models
- `FixtureMappingRecord` -- Cross-provider fixture ID mapping

### Other Dependencies

- `unified-config-interface` -- `UnifiedCloudConfig` for configuration
- `unified-events-interface` -- `setup_events`, `log_event` for lifecycle event logging
- `unified-domain-client` -- Domain client for cross-service communication

## Reference

- Codex sports integration plan: `unified-trading-codex/04-architecture/sports-integration-plan.md`
- Sports instrument format: `unified-trading-codex/01-domain/sports-instruments.md`
- Asset classes: `unified-trading-codex/01-domain/asset-classes.md`
