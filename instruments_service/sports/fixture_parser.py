"""
Fixture Parser — converts raw fixture/event data into instrument definitions.

A sports *fixture* (match/game/event) maps to a single instrument in the
instruments-service canonical model. The fixture parser:

1. Validates that the league is in the registry.
2. Normalizes home/away team names via :class:`TeamNormalizer`.
3. Builds an ``InstrumentDefinition``-compatible dict with the correct
   ``instrument_key``, ``venue``, ``instrument_type``, ``symbol``, etc.

The instrument key format for sports is::

    {LEAGUE}:FIXTURE:{HOME_ID}-v-{AWAY_ID}@{YYYYMMDD}

Example::

    EPL:FIXTURE:ARS-v-CHE@20260315

Also supports:
- Lookup by API-Football league ID when the canonical league key is unknown.
- ``data_provider`` population from the league's ``data_sources``.
- Batch parsing of multiple fixtures in one call.
"""

from __future__ import annotations

import logging
from datetime import datetime

from instruments_service.sports.league_registry import (
    LEAGUE_REGISTRY,
    LeagueDefinition,
    get_league_by_api_football_id,
)
from instruments_service.sports.team_normalizer import TeamNormalizer

logger = logging.getLogger(__name__)


class FixtureParser:
    """Parses raw fixture data into instrument-compatible dictionaries.

    Parameters
    ----------
    team_normalizer:
        Optional pre-built normalizer. A default one is created if ``None``.
    """

    def __init__(self, team_normalizer: TeamNormalizer | None = None) -> None:
        self._normalizer = team_normalizer or TeamNormalizer()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_fixture(
        self,
        league_id: str,
        home_team: str,
        away_team: str,
        kickoff_utc: datetime,
    ) -> dict[str, str | bool | float | None]:
        """Parse a single fixture into an instrument definition dict.

        Parameters
        ----------
        league_id:
            Canonical league identifier (e.g. ``EPL``) **or** a string
            representation of an API-Football numeric ID (e.g. ``"39"``).
        home_team:
            Home team name (any known variant).
        away_team:
            Away team name (any known variant).
        kickoff_utc:
            Kickoff time in UTC.

        Returns
        -------
        dict
            A dictionary compatible with ``InstrumentDefinition(**result)``.

        Raises
        ------
        ValueError
            If the league is not in the registry or teams cannot be resolved.
        """
        league = self._resolve_league(league_id)

        # Normalize teams
        home_result = self._normalizer.normalize(home_team)
        if home_result is None:
            msg = f"Cannot resolve home team: {home_team!r}"
            raise ValueError(msg)
        home_id, home_display = home_result

        away_result = self._normalizer.normalize(away_team)
        if away_result is None:
            msg = f"Cannot resolve away team: {away_team!r}"
            raise ValueError(msg)
        away_id, away_display = away_result

        # Build instrument key
        date_str = kickoff_utc.strftime("%Y%m%d")
        instrument_key = f"{league.league_id}:FIXTURE:{home_id}-v-{away_id}@{date_str}"
        symbol = f"{home_id}-v-{away_id}"
        available_from = kickoff_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Derive data_provider from the league's data_sources
        data_provider = ",".join(sorted(league.data_sources)) if league.data_sources else ""

        return {
            "instrument_key": instrument_key,
            "venue": league.league_id,
            "instrument_type": "FIXTURE",
            "symbol": symbol,
            "available_from_datetime": available_from,
            "available_to_datetime": None,
            "base_asset": home_display,
            "quote_asset": away_display,
            "data_types": "odds,scores",
            "market_category": "SPORTS",
            "asset_class": league.sport,
            "data_provider": data_provider,
            "venue_type": "SPORTS_LEAGUE",
            "chain": "",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "ccxt_exchange": "",
            "ccxt_symbol": "",
            "exchange_raw_symbol": "",
            "databento_symbol": "",
        }

    def parse_fixtures(
        self,
        fixtures: tuple[tuple[str, str, str, datetime], ...],
    ) -> list[dict[str, str | bool | float | None]]:
        """Parse multiple fixtures in one call.

        Parameters
        ----------
        fixtures:
            A tuple of ``(league_id, home_team, away_team, kickoff_utc)``
            tuples.

        Returns
        -------
        list[dict]
            Successfully parsed fixtures.  Fixtures that fail validation
            are logged at WARNING level and skipped.
        """
        results: list[dict[str, str | bool | float | None]] = []
        for league_id, home_team, away_team, kickoff_utc in fixtures:
            try:
                result = self.parse_fixture(league_id, home_team, away_team, kickoff_utc)
                results.append(result)
            except ValueError:
                logger.warning(
                    "Skipping fixture: league=%r home=%r away=%r",
                    league_id,
                    home_team,
                    away_team,
                )
        return results

    def resolve_league_for_api_football_id(self, api_football_id: int) -> str | None:
        """Return the canonical league_id for an API-Football numeric ID, or ``None``."""
        league = get_league_by_api_football_id(api_football_id)
        if league is None:
            return None
        return league.league_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_league(self, league_id: str) -> LeagueDefinition:
        """Resolve *league_id* to a :class:`LeagueDefinition`.

        Tries the canonical registry first, then falls back to API-Football
        numeric ID lookup.
        """
        league_upper = league_id.upper()

        # 1. Direct lookup
        if league_upper in LEAGUE_REGISTRY:
            return LEAGUE_REGISTRY[league_upper]

        # 2. Try interpreting as API-Football numeric ID
        try:
            api_id = int(league_id)
        except ValueError:
            pass
        else:
            league_def = get_league_by_api_football_id(api_id)
            if league_def is not None:
                return league_def

        msg = f"Unknown league: {league_id!r}. Known leagues: {sorted(LEAGUE_REGISTRY.keys())}"
        raise ValueError(msg)
