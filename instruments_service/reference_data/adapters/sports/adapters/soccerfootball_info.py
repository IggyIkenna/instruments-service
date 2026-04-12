"""SoccerFootball.info adapter — league standings and tables.

Auth: service must fetch soccer-football-info-api-key from Secret Manager
and pass it via the ``api_key`` constructor parameter. Uses RapidAPI.
Base URL: https://soccer-football-info.p.rapidapi.com
"""

from __future__ import annotations

import logging

from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalStanding,
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
            async with self._make_session() as session:
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

    async def get_standings(self, league_id: str, season: str | None = None) -> list[CanonicalStanding]:
        """Fetch standings/table for a specific league.

        Args:
            league_id: SoccerFootball.info league ID (UUID string).
            season: Optional season identifier.

        Returns:
            List of validated CanonicalStanding models.
        """
        url = f"{_BASE_URL}/leagues/{league_id}/standings"
        params: dict[str, str] = {}
        if season:
            params["season"] = season

        try:
            async with self._make_session() as session:
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

        raw_rows = _extract_data(raw_response)
        results: list[CanonicalStanding] = []
        for item in raw_rows:
            try:
                results.append(_normalize_sfi_standing(item, league_id, season))
            except Exception as exc:
                logger.warning("Failed to normalize SFI standing: %s", exc)
                continue
        logger.info(
            "Fetched %d standings entries for league=%s",
            len(results),
            league_id,
        )
        return results

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

    async def get_teams(self, league_id: int | str, season: int | None = None) -> list[CanonicalTeam]:
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
            try:
                if not entry.team_name:
                    continue
                teams.append(
                    CanonicalTeam(
                        team_id=entry.team_id,
                        name=entry.team_name,
                        short_name=None,
                        country=None,
                        founded=None,
                        logo_url=None,
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

        This adapter is for reference data only. Returns an empty list.
        """
        logger.info("get_odds not supported on SoccerFootball.info adapter")
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


def _normalize_sfi_standing(
    item: dict[str, object],
    league_id: str,
    season: str | None,
) -> CanonicalStanding:
    """Map a SFI standings row to CanonicalStanding."""
    return CanonicalStanding(
        league_id=league_id,
        season=season,
        rank=_safe_int(item.get("position") or item.get("rank")) or 0,
        team_id=str(item.get("team_id", "") or item.get("id", "")),
        team_name=str(item.get("team_name", "") or item.get("name", "")),
        points=_safe_int(item.get("points")) or 0,
        goals_diff=_safe_int(item.get("goal_difference") or item.get("goals_diff")),
        group=str(item.get("group", "")) if item.get("group") else None,
        form=str(item.get("form", "")) if item.get("form") else None,
        played=_safe_int(item.get("played") or item.get("matches_played")),
        wins=_safe_int(item.get("wins") or item.get("won")),
        draws=_safe_int(item.get("draws") or item.get("drawn")),
        losses=_safe_int(item.get("losses") or item.get("lost")),
        goals_for=_safe_int(item.get("goals_for") or item.get("goals_scored")),
        goals_against=_safe_int(item.get("goals_against") or item.get("goals_conceded")),
    )


def _safe_int(val: object) -> int | None:
    """Safely convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(str(val))
    except (ValueError, TypeError):
        return None
