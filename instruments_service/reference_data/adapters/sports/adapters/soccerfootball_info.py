"""SoccerFootball.info adapter — league standings and tables.

Auth: service must fetch soccer-football-info-api-key from Secret Manager
and pass it via the ``api_key`` constructor parameter. Uses RapidAPI.
Base URL: https://soccer-football-info.p.rapidapi.com
Note: UAC does not have external/soccerfootball_info yet; raw dicts used.
"""

from __future__ import annotations

import logging

import aiohttp
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = "https://soccer-football-info.p.rapidapi.com"


class SoccerFootballInfoAdapter(BaseSportsReferenceAdapter):
    """SoccerFootball.info sports reference data adapter.

    Fetches league standings and tables from the SoccerFootball.info
    RapidAPI endpoint. Returns data as canonical types.

    Secret Manager key: ``soccer-football-info-api-key``
    """

    @property
    def venue(self) -> str:
        return "soccerfootball_info"

    def _headers(self) -> dict[str, str]:
        """Build request headers with RapidAPI key authentication."""
        if not self._api_key:
            raise ValueError(
                "SoccerFootball.info adapter requires an API key. "
                "Service must fetch 'soccer-football-info-api-key' from Secret Manager "
                "and pass it via api_key parameter."
            )
        return {
            "x-rapidapi-key": self._api_key,
            "x-rapidapi-host": "soccer-football-info.p.rapidapi.com",
            "Accept-Encoding": "gzip, deflate",
        }

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch league standings data from SoccerFootball.info.

        Returns:
            List of canonical leagues with standings data.
        """
        url = f"{_BASE_URL}/championships/list/"

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        league_list = _extract_data(raw_response)
        for item in league_list:
            if not isinstance(item, dict):
                continue
            try:
                league_id = str(item.get("id", "") or item.get("league_id", ""))
                name = str(item.get("name", "") or item.get("league_name", ""))
                country = str(item.get("country", "") or item.get("country_name", ""))
                leagues.append(
                    CanonicalLeague(
                        league_id=league_id,
                        name=name,
                        country=country,
                        league_type=str(item.get("type", "")) if item.get("type") else None,
                        logo_url=str(item.get("logo", "")) if item.get("logo") else None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse SoccerFootball.info league: %s", exc)
                continue

        logger.info("Fetched %d leagues from SoccerFootball.info", len(leagues))
        return leagues

    async def get_standings(self, league_id: str, season: str | None = None) -> list[dict[str, object]]:
        """Fetch standings/table for a specific league.

        Args:
            league_id: SoccerFootball.info league ID (UUID string).
            season: Optional season identifier.

        Returns:
            List of standings entries as raw dicts (no UAC schema yet).
        """
        url = f"{_BASE_URL}/leagues/{league_id}/standings"
        params: dict[str, str] = {}
        if season:
            params["season"] = season

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(
                    session,
                    url,
                    params=params if params else None,
                    headers=self._headers(),
                )
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        standings = _extract_data(raw_response)
        logger.info(
            "Fetched %d standings entries for league=%s",
            len(standings),
            league_id,
        )
        return standings

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """SoccerFootball.info does not provide detailed fixture data.

        Use ApiFootballAdapter for fixtures. This returns an empty list.
        """
        logger.info("get_fixtures not supported on SoccerFootball.info adapter — use ApiFootballAdapter")
        return []

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams from standings for a league.

        Args:
            league_id: SoccerFootball.info league ID (as int, converted to str).
            season: Optional season year.

        Returns:
            List of canonical teams extracted from standings data.
        """
        standings = await self.get_standings(str(league_id))
        teams: list[CanonicalTeam] = []
        for entry in standings:
            if not isinstance(entry, dict):
                continue
            try:
                team_id = str(entry.get("team_id", "") or entry.get("id", ""))
                name = str(entry.get("team_name", "") or entry.get("name", ""))
                if not name:
                    continue
                teams.append(
                    CanonicalTeam(
                        team_id=team_id,
                        name=name,
                        short_name=str(entry.get("short_name", "")) if entry.get("short_name") else None,
                        country=None,
                        founded=None,
                        logo_url=str(entry.get("logo", "")) if entry.get("logo") else None,
                        venue=None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse SoccerFootball.info team: %s", exc)
                continue

        logger.info("Fetched %d teams for league=%s", len(teams), league_id)
        return teams

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """SoccerFootball.info does not provide odds data.

        Use OddsApiAdapter for odds. This returns an empty list.
        """
        logger.info("get_odds not supported on SoccerFootball.info adapter — use OddsApiAdapter")
        return []


def _extract_data(raw: object) -> list[dict[str, object]]:
    """Extract data list from a SoccerFootball.info response envelope."""
    if isinstance(raw, dict):
        # Try common envelope keys
        for key in ("data", "result", "results", "standings", "leagues", "response"):
            data = raw.get(key)
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []
