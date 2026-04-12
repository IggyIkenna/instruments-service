"""Understat adapter — xG (expected goals), shot data, advanced stats.

Auth: None (public JSON API, browser-like headers).
Base URL: https://understat.com
Ref: https://understat.com
Leagues: EPL, La_Liga, Bundesliga, Serie_A, Ligue_1, RFPL

Data fetched via ``GET /getLeagueData/{league}/{season}`` which returns
JSON with ``{teams, players, dates}`` — the ``dates`` array has per-match
xG, goals, and team info.
"""

from __future__ import annotations

import logging

from unified_api_contracts.external.understat import (
    UnderstatMatch,
    UnderstatMatchTeam,
)
from unified_api_contracts.external.understat.normalize import (
    normalize_understat_fixture,
)
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = "https://understat.com"

# Understat league identifiers
_UNDERSTAT_LEAGUES: list[str] = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1", "RFPL"]


class UnderstatAdapter(BaseSportsReferenceAdapter):
    """Understat sports reference data adapter.

    Fetches xG (expected goals), shot data, and advanced stats from
    Understat's JSON API. No API key required.

    Data is fetched via ``/getLeagueData/{league}/{season}`` which returns
    JSON with match-level xG, goals, and team metadata.
    """

    @property
    def venue(self) -> str:
        return "understat"

    def _headers(self) -> dict[str, str]:
        """Build request headers — Understat AJAX requires XMLHttpRequest."""
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Fetch xG-enriched fixture data from Understat for a season.

        Understat does not support date-based queries directly. This fetches
        the current season's fixtures for each league and filters by date.

        Args:
            date: Date string in YYYY-MM-DD format.
            league_ids: Not used (Understat uses league name strings).

        Returns:
            List of canonical fixtures with xG data.
        """
        year_str = date[:4]
        try:
            season_year = int(year_str)
        except ValueError:
            season_year = 2025

        # Understat seasons are labelled by start year (e.g. 2025 for 2025/2026)
        month_str = date[5:7]
        try:
            month = int(month_str)
        except ValueError:
            month = 1
        season = season_year if month >= 8 else season_year - 1

        fixtures: list[CanonicalFixture] = []
        for league in _UNDERSTAT_LEAGUES:
            league_fixtures = await self._fetch_league_fixtures(league, season, date)
            fixtures.extend(league_fixtures)

        logger.info("Fetched %d xG-enriched fixtures for date=%s", len(fixtures), date)
        return fixtures

    async def _fetch_league_fixtures(
        self,
        league: str,
        season: int,
        target_date: str,
    ) -> list[CanonicalFixture]:
        """Fetch fixtures for a single league/season from Understat JSON API.

        Uses ``GET /getLeagueData/{league}/{season}`` which returns JSON
        with ``{dates: [...], teams: {...}, players: [...]}`` where each
        entry in ``dates`` is a match with xG, goals, and team info.
        """
        url = f"{_BASE_URL}/getLeagueData/{league}/{season}"
        headers = {**self._headers(), "Referer": f"{_BASE_URL}/league/{league}/{season}"}

        try:
            async with self._make_session() as session:
                raw_response = await self._get_with_retry(session, url, headers=headers)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            return []

        matches = _extract_dates_from_json(raw_response)
        return _filter_and_normalize_matches(matches, target_date, league, season)

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Return the fixed list of Understat-supported leagues.

        Returns:
            List of canonical leagues.
        """
        leagues: list[CanonicalLeague] = []
        league_countries: dict[str, str] = {
            "EPL": "England",
            "La_Liga": "Spain",
            "Bundesliga": "Germany",
            "Serie_A": "Italy",
            "Ligue_1": "France",
            "RFPL": "Russia",
        }
        for league_name in _UNDERSTAT_LEAGUES:
            leagues.append(
                CanonicalLeague(
                    league_id=league_name,
                    name=league_name.replace("_", " "),
                    country=league_countries.get(league_name, ""),
                    league_type=None,
                    logo_url=None,
                )
            )
        logger.info("Returned %d Understat leagues", len(leagues))
        return leagues

    async def get_teams(self, league_id: int | str, season: int | None = None) -> list[CanonicalTeam]:
        """Understat does not support team lookup by numeric league ID.

        Use ApiFootballAdapter for team reference data. This returns an empty list.
        """
        logger.info("get_teams not supported on Understat adapter — use ApiFootballAdapter")
        return []

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Understat does not provide odds data.

        This adapter is for xG/stats data only. Returns an empty list.
        """
        logger.info("get_odds not supported on Understat adapter")
        return []


def _filter_and_normalize_matches(
    matches: list[dict[str, object]],
    target_date: str,
    league: str,
    season: int,
) -> list[CanonicalFixture]:
    """Filter matches by date and normalize to canonical fixtures."""
    fixtures: list[CanonicalFixture] = []
    for match_data in matches:
        if not isinstance(match_data, dict):
            continue
        match_datetime = str(match_data.get("datetime", ""))
        if not match_datetime.startswith(target_date):
            continue
        try:
            match = _parse_understat_match(match_data)
            if match is None:
                continue
            canonical = normalize_understat_fixture(match, league_name=league, season=str(season))
            fixtures.append(canonical)
        except Exception as exc:
            logger.warning(
                "Failed to normalize Understat match %s: %s",
                match_data.get("id", "unknown"),
                exc,
            )
    return fixtures


def _extract_dates_from_json(raw: object) -> list[dict[str, object]]:
    """Extract match list from Understat ``/getLeagueData`` JSON response.

    Response shape: ``{dates: [...], teams: {...}, players: [...]}``
    Each entry in ``dates`` is a match dict with id, h, a, goals, xG, datetime.
    """
    if not isinstance(raw, dict):
        return []
    dates = raw.get("dates")
    if isinstance(dates, list):
        return [item for item in dates if isinstance(item, dict)]
    return []


def _parse_understat_match(item: dict[str, object]) -> UnderstatMatch | None:
    """Parse a single match from Understat datesData."""
    try:
        home_raw = item.get("h")
        away_raw = item.get("a")
        home_team: UnderstatMatchTeam | dict[str, object] | None = None
        away_team: UnderstatMatchTeam | dict[str, object] | None = None

        if isinstance(home_raw, dict):
            home_team = UnderstatMatchTeam(
                id=_safe_int(home_raw.get("id")),
                title=str(home_raw.get("title", "")),
                short_title=str(home_raw.get("short_title", "")) if home_raw.get("short_title") else None,
            )
        if isinstance(away_raw, dict):
            away_team = UnderstatMatchTeam(
                id=_safe_int(away_raw.get("id")),
                title=str(away_raw.get("title", "")),
                short_title=str(away_raw.get("short_title", "")) if away_raw.get("short_title") else None,
            )

        return UnderstatMatch(
            id=item.get("id"),
            h=home_team,
            a=away_team,
            goals_h=_safe_int(
                item.get("goals", {}).get("h") if isinstance(item.get("goals"), dict) else item.get("goals_h")
            ),
            goals_a=_safe_int(
                item.get("goals", {}).get("a") if isinstance(item.get("goals"), dict) else item.get("goals_a")
            ),
            xg_h=_safe_float(item.get("xG", {}).get("h") if isinstance(item.get("xG"), dict) else item.get("xG_h")),
            xg_a=_safe_float(item.get("xG", {}).get("a") if isinstance(item.get("xG"), dict) else item.get("xG_a")),
            datetime=str(item.get("datetime", "")),
            is_result=bool(item.get("isResult")),
        )
    except Exception as exc:
        logger.warning("Failed to parse Understat match item: %s", exc)
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
