"""API Football adapter — fixtures, leagues, teams from api-football.com.

Auth: service must fetch api-football-api-key from Secret Manager and pass
it via the ``api_key`` constructor parameter.
Base URL: https://v3.football.api-sports.io
Ref: https://www.api-football.com/documentation-v3
"""

from __future__ import annotations

import logging
from datetime import datetime

import aiohttp
from unified_api_contracts.external.api_football import (  # noqa: qg-deep-import
    ApiFootballFixture,
    ApiFootballLeague,
)
from unified_api_contracts.external.api_football.normalize import (  # noqa: qg-deep-import
    normalize_api_football_fixture,
)
from unified_api_contracts.registry.endpoints import BASE_URLS  # noqa: qg-deep-import
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = BASE_URLS["api_football"]


def _effective_season_for_league(api_football_id: int) -> int:
    """Calculate the API-Football season year for a league.

    API-Football ``season`` = the calendar year when the season started.
    For European Aug-start leagues, in March 2026 the active season started
    in Aug 2025, so season = 2025.  For South American Jan/Feb-start leagues,
    in March 2026 the active season started in 2026, so season = 2026.

    Uses ``LEAGUE_REGISTRY`` season_months to determine the start month.
    Falls back to the standard Aug-start heuristic for unknown leagues.
    """
    from unified_api_contracts.canonical.domain.sports.league_data import (  # noqa: qg-deep-import
        get_league_by_api_football_id,
    )

    now = datetime.now()
    league_def = get_league_by_api_football_id(api_football_id)
    start_month = (
        league_def.season_months[0] if league_def is not None else 8  # default: European Aug-start
    )

    return now.year if now.month >= start_month else now.year - 1


def _flatten_standings_groups(
    groups: list[object],
) -> list[dict[str, object]]:
    """Flatten nested standings groups into a flat row list."""
    rows: list[dict[str, object]] = []
    for group in groups:
        if not isinstance(group, list):
            continue
        for row in group:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _parse_team_item(
    team_data: dict[str, object],
    venue_data: object,
) -> CanonicalTeam:
    """Build a CanonicalTeam from an API-Football teams response item."""
    from unified_api_contracts.canonical.domain.sports import (  # noqa: qg-deep-import
        CanonicalVenue,
        build_team_id,
        build_venue_id,
    )

    venue_obj: CanonicalVenue | None = None
    if isinstance(venue_data, dict):
        venue_name = str(venue_data.get("name", "")) if venue_data.get("name") else None
        if venue_name:
            venue_obj = CanonicalVenue(
                venue_id=build_venue_id(venue_name),
                name=venue_name,
            )
    display_name = str(team_data.get("name", ""))
    return CanonicalTeam(
        team_id=build_team_id(display_name),
        name=display_name,
        short_name=str(team_data.get("code", "")) if team_data.get("code") else None,
        country=str(team_data.get("country", "")) if team_data.get("country") else None,
        founded=team_data.get("founded") if isinstance(team_data.get("founded"), int) else None,
        logo_url=str(team_data.get("logo", "")) if team_data.get("logo") else None,
        venue=venue_obj,
    )


class ApiFootballAdapter(BaseSportsReferenceAdapter):
    """API Football sports reference data adapter.

    Fetches fixtures, leagues, and teams from the API Football v3 API
    and normalizes them to canonical types via UAC normalize functions.

    Secret Manager key: ``api-football-api-key``
    """

    @property
    def venue(self) -> str:
        return "api_football"

    def _headers(self) -> dict[str, str]:
        """Build request headers with API key authentication."""
        if not self._api_key:
            raise ValueError(
                "API Football adapter requires an API key. "
                "Service must fetch 'api-football-api-key' from Secret Manager "
                "and pass it via api_key parameter."
            )
        return {"x-apisports-key": self._api_key}

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Fetch fixtures for a given date from API Football.

        Args:
            date: Date string in YYYY-MM-DD format.
            league_ids: Optional list of API Football league IDs to filter by.

        Returns:
            List of canonical fixtures normalized via UAC.
        """
        url = f"{_BASE_URL}/fixtures"
        # API Football season = start year of the season.
        # Use league-aware season calculation when a specific league is requested.
        params: dict[str, str] = {"date": date}
        if league_ids:
            season_year = _effective_season_for_league(league_ids[0])
            params["league"] = str(league_ids[0])
            params["season"] = str(season_year)

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        fixtures = _parse_fixture_list(_extract_response(raw_response))

        if league_ids and len(league_ids) > 1:
            for lid in league_ids[1:]:
                additional = await self._fetch_league_fixtures(date, lid)
                fixtures.extend(additional)

        logger.info("Fetched %d fixtures for date=%s", len(fixtures), date)
        return fixtures

    async def _fetch_league_fixtures(self, date: str, league_id: int) -> list[CanonicalFixture]:
        """Fetch fixtures for a single league (used for multi-league queries)."""
        url = f"{_BASE_URL}/fixtures"
        season_year = _effective_season_for_league(league_id)
        params: dict[str, str] = {
            "date": date,
            "league": str(league_id),
            "season": str(season_year),
        }
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            return []

        fixtures: list[CanonicalFixture] = []
        response_list = _extract_response(raw_response)
        for item in response_list:
            fixture_raw = _parse_fixture_response(item)
            if fixture_raw is None:
                continue
            try:
                canonical = normalize_api_football_fixture(fixture_raw)
                fixtures.append(canonical)
            except Exception as exc:
                logger.warning("Failed to normalize fixture: %s", exc)
                continue
        return fixtures

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch all available leagues from API Football.

        Returns:
            List of canonical leagues.
        """
        url = f"{_BASE_URL}/leagues"
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        response_list = _extract_response(raw_response)
        for item in response_list:
            if not isinstance(item, dict):
                continue
            league_data = item.get("league")
            country_data = item.get("country")
            if not isinstance(league_data, dict):
                continue
            country_name = ""
            if isinstance(country_data, dict):
                country_name = str(country_data.get("name", ""))
            try:
                leagues.append(
                    CanonicalLeague(
                        league_id=str(league_data.get("id", "")),
                        name=str(league_data.get("name", "")),
                        country=country_name,
                        league_type=str(league_data.get("type", "")) if league_data.get("type") else None,
                        logo_url=str(league_data.get("logo", "")) if league_data.get("logo") else None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse league: %s", exc)
                continue

        logger.info("Fetched %d leagues", len(leagues))
        return leagues

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams for a given league from API Football.

        Args:
            league_id: API Football league ID.
            season: Optional season year. If None, calculated from league's
                season_months: for Aug-start leagues (European) in March 2026,
                season = 2025 (the year the season started).

        Returns:
            List of canonical teams.
        """
        url = f"{_BASE_URL}/teams"
        effective_season = season if season is not None else _effective_season_for_league(league_id)
        params: dict[str, str] = {
            "league": str(league_id),
            "season": str(effective_season),
        }
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        teams: list[CanonicalTeam] = []
        response_list = _extract_response(raw_response)
        for item in response_list:
            if not isinstance(item, dict):
                continue
            team_data = item.get("team")
            if not isinstance(team_data, dict):
                continue
            try:
                team = _parse_team_item(team_data, item.get("venue"))
                teams.append(team)
            except Exception as exc:
                logger.warning("Failed to parse team: %s", exc)
                continue

        logger.info("Fetched %d teams for league=%d season=%d", len(teams), league_id, effective_season)
        return teams

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """API Football does not provide odds via OddsAPI format.

        Use OddsApiAdapter for odds. This returns an empty list.
        """
        logger.info("get_odds not supported on API Football adapter — use OddsApiAdapter")
        return []

    async def get_standings(self, league_id: int, season: int | None = None) -> list[dict[str, object]]:
        """Fetch league standings from API Football.

        API endpoint: GET /standings?league={id}&season={year}
        Returns league table with position, team, points, GD, form.
        """
        url = f"{_BASE_URL}/standings"
        effective_season = season if season is not None else _effective_season_for_league(league_id)
        params: dict[str, str] = {"league": str(league_id), "season": str(effective_season)}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results: list[dict[str, object]] = []
        response_list = _extract_response(raw_response)
        for item in response_list:
            league_data = item.get("league")
            if not isinstance(league_data, dict):
                continue
            standings_groups = league_data.get("standings")
            if not isinstance(standings_groups, list):
                continue
            results.extend(_flatten_standings_groups(standings_groups))

        logger.info(
            "Fetched %d standing rows for league=%d season=%d",
            len(results),
            league_id,
            effective_season,
        )
        return results

    async def get_injuries(self, date: str) -> list[dict[str, object]]:
        """Fetch injuries for a given date from API Football.

        API endpoint: GET /injuries?date={YYYY-MM-DD}
        """
        url = f"{_BASE_URL}/injuries"
        params: dict[str, str] = {"date": date}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results = _extract_response(raw_response)
        logger.info("Fetched %d injuries for date=%s", len(results), date)
        return results

    async def get_fixture_statistics(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch match statistics for a completed fixture.

        API endpoint: GET /fixtures/statistics?fixture={id}
        Returns shots, possession, corners, fouls, cards per team.
        """
        url = f"{_BASE_URL}/fixtures/statistics"
        params: dict[str, str] = {"fixture": str(fixture_id)}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results = _extract_response(raw_response)
        logger.info("Fetched %d stat groups for fixture=%d", len(results), fixture_id)
        return results

    async def get_fixture_events(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch match events (goals, cards, substitutions) for a fixture.

        API endpoint: GET /fixtures/events?fixture={id}
        """
        url = f"{_BASE_URL}/fixtures/events"
        params: dict[str, str] = {"fixture": str(fixture_id)}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results = _extract_response(raw_response)
        logger.info("Fetched %d events for fixture=%d", len(results), fixture_id)
        return results

    async def get_fixture_lineups(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch starting lineups for a fixture.

        API endpoint: GET /fixtures/lineups?fixture={id}
        Returns starting XI, formation, coach per team.
        """
        url = f"{_BASE_URL}/fixtures/lineups"
        params: dict[str, str] = {"fixture": str(fixture_id)}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results = _extract_response(raw_response)
        logger.info("Fetched %d lineup entries for fixture=%d", len(results), fixture_id)
        return results

    async def get_fixture_player_stats(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch per-player per-match statistics for a fixture.

        API endpoint: GET /fixtures/players?fixture={id}
        Returns 33+ stat fields per player.
        """
        url = f"{_BASE_URL}/fixtures/players"
        params: dict[str, str] = {"fixture": str(fixture_id)}
        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            self._emit_fetch_failed(self._classify_error(exc), exc)
            return []

        results = _extract_response(raw_response)
        logger.info("Fetched %d player stat groups for fixture=%d", len(results), fixture_id)
        return results


def _extract_response(raw: object) -> list[dict[str, object]]:
    """Extract the response list from an API Football response envelope."""
    if isinstance(raw, dict):
        response = raw.get("response")
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _parse_fixture_response(item: dict[str, object]) -> ApiFootballFixture | None:
    """Parse a single fixture item from the API Football response.

    API Football returns fixtures in a nested structure:
    {fixture: {...}, league: {...}, teams: {...}, goals: {...}, score: {...}}
    """
    fixture_data = item.get("fixture")
    league_data = item.get("league")
    teams_data = item.get("teams")
    goals_data = item.get("goals")
    score_data = item.get("score")

    if not isinstance(fixture_data, dict):
        return None

    try:
        return ApiFootballFixture(
            id=fixture_data.get("id") if isinstance(fixture_data.get("id"), int) else None,
            referee=str(fixture_data.get("referee", "")) if fixture_data.get("referee") else None,
            timezone=str(fixture_data.get("timezone", "")) if fixture_data.get("timezone") else None,
            date=str(fixture_data.get("date", "")) if fixture_data.get("date") else None,
            timestamp=fixture_data.get("timestamp") if isinstance(fixture_data.get("timestamp"), int) else None,
            venue=fixture_data.get("venue") if isinstance(fixture_data.get("venue"), dict) else None,
            status=fixture_data.get("status") if isinstance(fixture_data.get("status"), dict) else None,
            periods=None,
            league=ApiFootballLeague(**league_data) if isinstance(league_data, dict) else None,
            teams=_parse_teams(teams_data) if isinstance(teams_data, dict) else None,
            goals=goals_data if isinstance(goals_data, dict) else None,
            score=score_data if isinstance(score_data, dict) else None,
        )
    except Exception as exc:
        logger.warning("Failed to parse fixture response item: %s", exc)
        return None


def _parse_fixture_list(response_list: list[dict[str, object]]) -> list[CanonicalFixture]:
    """Parse and normalize a list of raw API Football fixture responses."""
    fixtures: list[CanonicalFixture] = []
    for item in response_list:
        fixture_raw = _parse_fixture_response(item)
        if fixture_raw is None:
            continue
        try:
            canonical = normalize_api_football_fixture(fixture_raw)
            fixtures.append(canonical)
        except Exception as exc:
            fixture_id = item.get("fixture", {}).get("id", "unknown") if isinstance(item, dict) else "unknown"
            logger.warning("Failed to normalize fixture %s: %s", fixture_id, exc)
    return fixtures


def _parse_teams(teams_data: dict[str, object]) -> dict[str, object] | None:
    """Parse teams dict preserving the home/away structure for normalization."""
    from unified_api_contracts.external.api_football import (  # noqa: qg-deep-import
        ApiFootballTeamWithWinner,
    )

    result: dict[str, ApiFootballTeamWithWinner] = {}
    for side in ("home", "away"):
        team_raw = teams_data.get(side)
        if isinstance(team_raw, dict):
            try:
                result[side] = ApiFootballTeamWithWinner(**team_raw)
            except Exception:
                continue
    return result if result else None
