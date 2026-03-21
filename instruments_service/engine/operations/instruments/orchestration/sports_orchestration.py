"""Sports Orchestration Module.

Handles processing of sports fixtures and market instruments.
Follows the same pattern as CeFi/TradFi/DeFi orchestration modules.

Data flow:
  1. USRI (API Football) → CanonicalFixture list for the date
  2. URDI (Betfair/Polymarket) → market instruments per fixture
  3. Join fixture context + market instruments → InstrumentDefinition
  4. Store via DataSink
"""

from __future__ import annotations

import logging
from datetime import datetime

from unified_api_contracts.sports import LEAGUE_REGISTRY, get_league
from unified_events_interface import log_event
from unified_trading_library import get_secret_client

from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class SportsOrchestrator:
    """Orchestrates sports instrument processing.

    Responsibilities:
    - Determine which leagues to process based on venue filter
    - Fetch fixtures from USRI (API Football)
    - Fetch market instruments from URDI (Betfair)
    - Join fixture context with market data
    - Convert to InstrumentDefinition records
    """

    def __init__(self, project_id: str = "") -> None:
        self._project_id = project_id

    async def process_sports(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """Process sports fixtures and markets for the given date."""
        all_instruments: dict[str, InstrumentDefinition] = {}
        leagues_to_process = self._determine_leagues(venues_filter)

        if not leagues_to_process:
            logger.info("Skipping SPORTS processing — no leagues to process")
            return all_instruments

        date_str = date.strftime("%Y-%m-%d")
        logger.info(
            "Processing %s sports leagues for %s: %s",
            len(leagues_to_process),
            date_str,
            leagues_to_process,
        )

        # Fetch fixtures from USRI
        fixtures = await self._fetch_fixtures(date_str, leagues_to_process)
        if not fixtures:
            log_event(
                "VENUE_ZERO_INSTRUMENTS",
                details={"venue": "SPORTS", "date": date_str, "reason": "no fixtures returned from USRI"},
            )
            return all_instruments

        logger.info("Fetched %s fixtures for %s", len(fixtures), date_str)

        # Build instruments from fixtures
        for fixture in fixtures:
            try:
                instruments = self._fixture_to_instruments(fixture, date)
                all_instruments.update(instruments)
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(
                    "%s",
                    f"Failed to process fixture {getattr(fixture, 'fixture_id', '?')}: {e}",
                )
                continue

        logger.info("Generated %s sports instruments for %s", len(all_instruments), date_str)
        return all_instruments

    async def _fetch_fixtures(self, date_str: str, league_ids: list[str]) -> list[object]:
        """Fetch fixtures from USRI (API Football)."""
        try:
            from unified_sports_reference_interface import create_sports_reference_adapter

            secret_client = get_secret_client(project_id=self._project_id)
            api_key = secret_client.get_secret("api-football-api-key")

            adapter = create_sports_reference_adapter("api_football", api_key=api_key)

            # Convert league string IDs to API Football integer IDs
            api_football_ids: list[int] = []
            for league_id in league_ids:
                league_def = get_league(league_id)
                if league_def and hasattr(league_def, "api_football_id") and league_def.api_football_id:
                    api_football_ids.append(league_def.api_football_id)

            if not api_football_ids:
                logger.warning("No API Football IDs found for leagues: %s", league_ids)
                return []

            fixtures = await adapter.get_fixtures(date=date_str, league_ids=api_football_ids)
            return fixtures

        except (ImportError, ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
            logger.error("Failed to fetch fixtures from USRI: %s", e)
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={"venue": "api_football", "error": str(e), "date": date_str},
            )
            return []

    def _fixture_to_instruments(self, fixture: object, date: datetime) -> dict[str, InstrumentDefinition]:
        """Convert a CanonicalFixture to InstrumentDefinition records.

        Each fixture produces multiple instruments — one per market type
        (match_winner, over_under_2_5, both_teams_to_score, asian_handicap, etc.).
        """
        instruments: dict[str, InstrumentDefinition] = {}

        fixture_id = str(getattr(fixture, "fixture_id", getattr(fixture, "id", "")))
        home_team = str(getattr(fixture, "home_team", getattr(fixture, "home", {}))).upper()
        away_team = str(getattr(fixture, "away_team", getattr(fixture, "away", {}))).upper()
        league_id = str(getattr(fixture, "league_id", getattr(fixture, "league", "")))
        # league_name available via getattr(fixture, "league_name", league_id) if needed

        if not fixture_id or not home_team or not away_team:
            return instruments

        # Generate canonical fixture key
        date_str = date.strftime("%Y%m%d")
        fixture_key = f"{home_team}-v-{away_team}@{date_str}"

        # Standard market types for football
        market_types = [
            "MATCH_WINNER",
            "OVER_UNDER_2_5",
            "BOTH_TEAMS_TO_SCORE",
            "ASIAN_HANDICAP",
            "DOUBLE_CHANCE",
            "CORRECT_SCORE",
        ]

        for market_type in market_types:
            instrument_key = f"BETFAIR:{market_type}:{league_id}-{fixture_key}"
            instruments[instrument_key] = InstrumentDefinition(
                instrument_key=instrument_key,
                venue="BETFAIR",
                symbol=f"{home_team}/{away_team}",
                exchange_raw_symbol=fixture_id,
                instrument_type=market_type,
                base_asset=home_team,
                quote_asset=away_team,
                market_category="sports",
                data_provider="api_football",
                data_types="odds,fixture_stats",
                available_from_datetime=date.isoformat(),
                tardis_exchange="",
                tardis_symbol="",
            )

        return instruments

    def _determine_leagues(self, venues_filter: list[str]) -> list[str]:
        """Determine which leagues to process based on the venues filter."""
        if not venues_filter:
            return sorted(LEAGUE_REGISTRY.keys())

        matched: list[str] = [v.upper() for v in venues_filter if v.upper() in LEAGUE_REGISTRY]
        if matched:
            logger.info("Filtered SPORTS leagues by venues filter: %s", matched)
        else:
            logger.info("No SPORTS leagues matched venues filter: %s", venues_filter)
        return matched
