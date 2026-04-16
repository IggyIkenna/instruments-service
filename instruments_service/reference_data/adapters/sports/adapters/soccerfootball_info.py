"""SoccerFootball.info adapter — league standings, tables, and progressive stats.

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
    CanonicalProgressiveStats,
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
        # SFI uses /championships/{id}/standings/ not /leagues/{id}/standings
        # (confirmed from archived new-sports-batting-services client)
        url = f"{_BASE_URL}/championships/standings/"
        params: dict[str, str] = {"i": league_id, "l": "en_US"}
        if season:
            params["s"] = season

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

    async def get_match_ids_for_date(
        self,
        date: str,
    ) -> list[str]:
        """Fetch SFI match IDs for a given date.

        API endpoint: GET /matches/date/{date}
        Returns a list of SFI match ID strings for completed matches.

        Args:
            date: Date string in YYYY-MM-DD format.

        Returns:
            List of SFI match ID strings.
        """
        # SFI uses /matches/day/basic/ with ?d= param, not /matches/date/{date}
        url = f"{_BASE_URL}/matches/day/basic/"

        params: dict[str, str] = {"d": date, "l": "en_US"}

        try:
            async with self._make_session() as session:
                raw_response = await self._get_with_retry(
                    session,
                    url,
                    params=params,
                    headers=self._headers(),
                )
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        raw_rows = _extract_data(raw_response)
        match_ids: list[str] = []
        for item in raw_rows:
            match_id = item.get("id") or item.get("match_id")
            status = str(item.get("status", "")).upper()
            # Only include completed matches (finished/full-time)
            if match_id and status in ("FT", "AET", "PEN", "FINISHED", "FULL_TIME"):
                match_ids.append(str(match_id))
        logger.info(
            "SFI match IDs for date=%s: %d completed matches",
            date,
            len(match_ids),
        )
        return match_ids

    async def get_progressive_stats(
        self,
        match_id: str,
    ) -> list[CanonicalProgressiveStats]:
        """Fetch progressive (30-second interval) match stats from SFI.

        API endpoint: GET /matches/{match_id}/progressive
        Returns minute-by-minute team-level stats for halftime features:
        goals, possession, shots, corners, fouls, cards, dangerous attacks.

        Args:
            match_id: SoccerFootball.info match ID.

        Returns:
            List of canonical progressive stats (one per team per 30s tick).
        """
        # SFI uses /matches/view/progressive/ with ?i= param
        url = f"{_BASE_URL}/matches/view/progressive/"

        params: dict[str, str] = {"i": match_id, "l": "en_US"}

        try:
            async with self._make_session() as session:
                raw_response = await self._get_with_retry(
                    session,
                    url,
                    params=params,
                    headers=self._headers(),
                )
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        raw_rows = _extract_data(raw_response)
        results: list[CanonicalProgressiveStats] = []
        for item in raw_rows:
            try:
                results.append(_normalize_sfi_progressive_stat(item, match_id))
            except Exception as exc:
                logger.warning(
                    "Failed to normalize SFI progressive stat for match=%s: %s",
                    match_id,
                    exc,
                )
                continue
        logger.info(
            "Fetched %d progressive stat rows for match=%s",
            len(results),
            match_id,
        )
        return results

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


def _parse_timer_to_seconds(timer: str) -> int:
    """Parse SFI timer string "MM:SS" to total seconds.

    Examples: "00:30" -> 30, "45:00" -> 2700, "90:00" -> 5400.
    Falls back to 0 on parse errors.
    """
    parts = timer.split(":")
    if len(parts) == 2:
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, TypeError):
            return 0
    return 0


def _normalize_sfi_progressive_stat(
    item: dict[str, object],
    match_id: str,
) -> CanonicalProgressiveStats:
    """Map an SFI progressive stats row to CanonicalProgressiveStats.

    SFI progressive data includes per-team stats at 30-second intervals:
    goals, possession, shots, corners, fouls, cards, dangerous attacks.
    """
    timer = str(item.get("timer", "00:00"))
    timer_seconds = _parse_timer_to_seconds(timer)
    team = str(item.get("team", ""))

    possession_raw = item.get("possession")
    possession_pct = float(str(possession_raw)) if possession_raw is not None else None

    return CanonicalProgressiveStats(
        fixture_id=match_id,
        timer_seconds=timer_seconds,
        team=team,
        goals=_safe_int(item.get("goals")),
        possession_pct=possession_pct,
        dangerous_attacks=_safe_int(item.get("dangerous_attacks")),
        attacks=_safe_int(item.get("attacks")),
        shots_on_target=_safe_int(item.get("shots_on_target")),
        shots_off_target=_safe_int(item.get("shots_off_target")),
        corners=_safe_int(item.get("corners")),
        fouls=_safe_int(item.get("fouls")),
        yellow_cards=_safe_int(item.get("yellow_cards")),
        red_cards=_safe_int(item.get("red_cards")),
        substitutions=_safe_int(item.get("substitutions")),
        dominance_pct=float(str(item.get("dominance"))) if item.get("dominance") is not None else None,
    )
