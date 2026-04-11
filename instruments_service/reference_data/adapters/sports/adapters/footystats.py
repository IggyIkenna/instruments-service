"""FootyStats adapter — detailed match statistics and team form data.

Auth: service must fetch footystats-api-key from Secret Manager and pass
it via the ``api_key`` constructor parameter.
Base URL: https://api.football-data-api.com
Ref: https://api.football-data-api.com
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import aiohttp
from unified_api_contracts.external.footystats import (
    FootyStatsMatch,
)
from unified_api_contracts.external.footystats.normalize import (
    normalize_footystats_match,
)
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = "https://api.football-data-api.com"


class FootystatsAdapter(BaseSportsReferenceAdapter):
    """FootyStats sports reference data adapter.

    Fetches detailed match statistics and team form data from the
    FootyStats (Football-Data-API) and normalizes them to canonical
    types via UAC normalize functions.

    Secret Manager key: ``footystats-api-key``
    """

    @property
    def venue(self) -> str:
        return "footystats"

    def _params_with_key(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """Build query params with API key authentication."""
        if not self._api_key:
            raise ValueError(
                "FootyStats adapter requires an API key. "
                "Service must fetch 'footystats-api-key' from Secret Manager "
                "and pass it via api_key parameter."
            )
        params: dict[str, str] = {"key": self._api_key}
        if extra:
            params.update(extra)
        return params

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Fetch match stats for a given date from FootyStats.

        Args:
            date: Date string in YYYY-MM-DD format.
            league_ids: Optional list of FootyStats league/competition IDs.

        Returns:
            List of canonical fixtures with detailed match statistics.
        """
        url = f"{_BASE_URL}/todays-matches"
        params = self._params_with_key({"date": date})

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        fixtures: list[CanonicalFixture] = []
        match_list = _extract_data(raw_response)
        for item in match_list:
            try:
                match = _parse_match(item)
                if match is None:
                    continue
                canonical = normalize_footystats_match(match)
                if league_ids and match.competition_id not in league_ids:
                    continue
                fixtures.append(canonical)
            except Exception as exc:
                logger.warning(
                    "Failed to normalize FootyStats match %s: %s",
                    item.get("match_id", "unknown") if isinstance(item, dict) else "unknown",
                    exc,
                )
                continue

        logger.info("Fetched %d fixtures for date=%s", len(fixtures), date)
        return fixtures

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch available leagues from FootyStats.

        Returns:
            List of canonical leagues.
        """
        url = f"{_BASE_URL}/league-list"
        params = self._params_with_key({"chosen_leagues_only": "true"})

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        data_list = _extract_data(raw_response)
        for item in data_list:
            if not isinstance(item, dict):
                continue
            try:
                leagues.append(
                    CanonicalLeague(
                        league_id=str(item.get("id", "")),
                        name=str(item.get("name", "")),
                        country=str(item.get("country", "")) if item.get("country") else "",
                        league_type=None,
                        logo_url=str(item.get("image", "")) if item.get("image") else None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse FootyStats league: %s", exc)
                continue

        logger.info("Fetched %d leagues from FootyStats", len(leagues))
        return leagues

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams for a league from FootyStats.

        Args:
            league_id: FootyStats competition ID.
            season: Optional season year. Defaults to current year.

        Returns:
            List of canonical teams.
        """
        url = f"{_BASE_URL}/league-teams"
        effective_season = season if season is not None else datetime.now(UTC).year
        params = self._params_with_key(
            {
                "season_id": str(league_id),
            }
        )

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        teams: list[CanonicalTeam] = []
        data_list = _extract_data(raw_response)
        for item in data_list:
            if not isinstance(item, dict):
                continue
            try:
                teams.append(
                    CanonicalTeam(
                        team_id=str(item.get("id", "")),
                        name=str(item.get("clean_name", "") or item.get("name", "")),
                        short_name=str(item.get("short_hand", "")) if item.get("short_hand") else None,
                        country=str(item.get("country", "")) if item.get("country") else None,
                        founded=None,
                        logo_url=str(item.get("image", "")) if item.get("image") else None,
                        venue=None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse FootyStats team: %s", exc)
                continue

        logger.info(
            "Fetched %d teams for league=%d season=%d",
            len(teams),
            league_id,
            effective_season,
        )
        return teams

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """FootyStats does not provide odds data.

        This adapter is for reference data only. Returns an empty list.
        """
        logger.info("get_odds not supported on FootyStats adapter")
        return []


def _extract_data(raw: object) -> list[dict[str, object]]:
    """Extract the data list from a FootyStats response envelope."""
    if isinstance(raw, dict):
        data = raw.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _parse_match(item: dict[str, object]) -> FootyStatsMatch | None:
    """Parse a single match item from the FootyStats response."""
    try:
        match_id = item.get("id") or item.get("match_id")
        if match_id is None:
            return None
        return FootyStatsMatch(
            match_id=int(str(match_id)),
            date_unix=int(str(item.get("date_unix", 0))),
            competition_id=int(str(item.get("competition_id", 0))),
            season=str(item.get("season", "")),
            home_id=int(str(item.get("homeID", 0) or item.get("home_id", 0))),
            away_id=int(str(item.get("awayID", 0) or item.get("away_id", 0))),
            home_name=str(item.get("home_name", "")),
            away_name=str(item.get("away_name", "")),
            home_goals=_safe_int(item.get("homeGoalCount") or item.get("home_goal_count")),
            away_goals=_safe_int(item.get("awayGoalCount") or item.get("away_goal_count")),
            total_goals=_safe_int(item.get("totalGoalCount") or item.get("total_goal_count")),
            home_xg=_safe_float(item.get("team_a_xg")),
            away_xg=_safe_float(item.get("team_b_xg")),
            status=str(item.get("status", "")) if item.get("status") else None,
            game_week=_safe_int(item.get("game_week")),
            home_possession=_safe_int(item.get("team_a_possession")),
            away_possession=_safe_int(item.get("team_b_possession")),
            home_shots=_safe_int(item.get("team_a_shots")),
            away_shots=_safe_int(item.get("team_b_shots")),
            home_corners=_safe_int(item.get("team_a_corners")),
            away_corners=_safe_int(item.get("team_b_corners")),
        )
    except Exception as exc:
        logger.warning("Failed to parse FootyStats match item: %s", exc)
        return None


def _safe_int(val: object) -> int | None:
    """Safely convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(str(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val: object) -> float | None:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return None
