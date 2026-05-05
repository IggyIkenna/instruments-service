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

    Plan: RapidAPI Ultra — 4 req/sec, 99,999 req/day. Throttle to 3 req/sec
    (0.34s interval) to stay safely under the per-second cap and avoid 429s.
    """

    # Override base default (0.1s = 10 req/sec). SFI plan is 4 req/sec hard cap.
    _min_request_interval: float = 0.34

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
        """Fetch SFI match IDs for a given date (id-only, all leagues).

        Returns a list of SFI match ID strings for completed matches across
        every championship SFI tracks for that date. Use
        ``get_match_descriptors_for_date`` instead when callers want to
        filter by championship before paying per-match endpoint quota.

        API endpoint: GET /matches/day/basic/?d=YYYYMMDD

        Args:
            date: Date string in YYYY-MM-DD format.

        Returns:
            List of SFI match ID strings.
        """
        descriptors = await self.get_match_descriptors_for_date(date)
        return [d["match_id"] for d in descriptors]

    async def get_match_descriptors_for_date(
        self,
        date: str,
    ) -> list[dict[str, str]]:
        """Fetch SFI match descriptors (match_id + championship_id) for a date.

        Returns one dict per completed match with the SFI match id and the
        SFI championship (league) id. Lets the orchestrator filter the
        per-match progressive loop down to mapped prediction leagues
        BEFORE making the per-match API call — saves ~10x RapidAPI quota
        because SFI's day-list returns ~50 leagues' worth of matches and we
        only consume ~4.

        API endpoint: GET /matches/day/basic/?d=YYYYMMDD
        """
        sfi_date = date.replace("-", "")  # 2025-03-01 → 20250301
        url = f"{_BASE_URL}/matches/day/basic/"
        params: dict[str, str] = {"d": sfi_date, "l": "en_US"}

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
        descriptors: list[dict[str, str]] = []
        for item in raw_rows:
            match_id = item.get("id") or item.get("match_id")
            status = str(item.get("status", "")).upper()
            if not (match_id and status in ("FT", "AET", "PEN", "FINISHED", "FULL_TIME", "ENDED")):
                continue
            championship = item.get("championship") or {}
            championship_id = ""
            if isinstance(championship, dict):
                championship_id = str(championship.get("id") or "")
            descriptors.append({"match_id": str(match_id), "championship_id": championship_id})
        logger.info(
            "SFI match descriptors for date=%s: %d completed matches",
            date,
            len(descriptors),
        )
        return descriptors

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
        # Annotate halftime window on all rows
        results = detect_halftime_window(results)
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


def _safe_float(val: object) -> float | None:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return None


def _extract_team_stats(team_data: dict[str, object]) -> dict[str, object]:
    """Extract flat stats from an SFI nested team object (teamA/teamB)."""
    attacks_raw = team_data.get("attacks")
    shoots_raw = team_data.get("shoots")
    fouls_raw = team_data.get("fouls")
    dominance_raw = team_data.get("dominance")

    result: dict[str, object] = {
        "goals": team_data.get("goal"),
        "possession": team_data.get("possession"),
        "corners": team_data.get("corners"),
        "substitutions": team_data.get("substitutions"),
        "penalties": team_data.get("penalties"),
        "xg": team_data.get("xG"),
    }

    # Attacks: {"n": 45, "d": 17}
    if isinstance(attacks_raw, dict):
        result["attacks"] = attacks_raw.get("n")
        result["dangerous_attacks"] = attacks_raw.get("d")

    # Shoots: {"t": "6", "on": 4, "off": 2, "g_a": null}
    if isinstance(shoots_raw, dict):
        result["shoots_total"] = shoots_raw.get("t")
        result["shoots_on_target"] = shoots_raw.get("on")
        result["shoots_off_target"] = shoots_raw.get("off")

    # Fouls: {"t": null, "y_c": 1, "y_t_r_c": null, "r_c": 0}
    if isinstance(fouls_raw, dict):
        result["fouls"] = fouls_raw.get("t")
        result["yellow_cards"] = fouls_raw.get("y_c")
        result["red_cards"] = fouls_raw.get("r_c")

    # Dominance: {"index": "52.3", "avg_2_5": "48.1"}
    if isinstance(dominance_raw, dict):
        result["dominance_index"] = dominance_raw.get("index")
        result["dominance_avg"] = dominance_raw.get("avg_2_5")

    return result


def _extract_odds(odds_data: dict[str, object]) -> dict[str, float | None]:
    """Extract flat odds from an SFI nested odds object."""
    result: dict[str, float | None] = {}

    # 1X2: {"1": "1.181", "X": "6.000", "2": "13.000"}
    x1x2 = odds_data.get("1X2")
    if isinstance(x1x2, dict):
        result["odds_1x2_home"] = _safe_float(x1x2.get("1"))
        result["odds_1x2_draw"] = _safe_float(x1x2.get("X"))
        result["odds_1x2_away"] = _safe_float(x1x2.get("2"))

    # Over/Under: {"o": "1.850", "u": "1.950", "v": "4.5"}
    ou = odds_data.get("over_under")
    if isinstance(ou, dict):
        result["odds_ou_over"] = _safe_float(ou.get("o"))
        result["odds_ou_under"] = _safe_float(ou.get("u"))
        result["odds_ou_line"] = _safe_float(ou.get("v"))

    # Asian Handicap: {"1": "1.875", "2": "1.925", "v": "-0.5"}
    ah = odds_data.get("asian_handicap")
    if isinstance(ah, dict):
        result["odds_ah_home"] = _safe_float(ah.get("1"))
        result["odds_ah_away"] = _safe_float(ah.get("2"))
        result["odds_ah_line"] = _safe_float(ah.get("v"))

    # Asian Corner: {"o": "2.025", "u": "1.775", "v": "6.5"}
    ac = odds_data.get("asian_corner")
    if isinstance(ac, dict):
        result["odds_asian_corner_over"] = _safe_float(ac.get("o"))
        result["odds_asian_corner_under"] = _safe_float(ac.get("u"))
        result["odds_asian_corner_line"] = _safe_float(ac.get("v"))

    return result


def _normalize_sfi_progressive_stat(
    item: dict[str, object],
    match_id: str,
) -> CanonicalProgressiveStats:
    """Map an SFI progressive stats row to CanonicalProgressiveStats.

    Handles two formats:
    1. Nested SFI format with teamA/teamB/odds sub-objects
    2. Pre-flattened format with top-level stat fields

    SFI progressive data includes per-team stats at 30-second intervals:
    goals, possession, shots, corners, fouls, cards, dangerous attacks,
    xG, dominance index, and in-play odds.
    """
    timer = str(item.get("timer", "00:00"))
    timer_seconds = _parse_timer_to_seconds(timer)
    team = str(item.get("team", ""))

    # --- Extract nested team data if present (SFI live format) ---
    team_a_raw = item.get("teamA")
    team_b_raw = item.get("teamB")
    odds_raw = item.get("odds")

    home_stats: dict[str, object] = {}
    away_stats: dict[str, object] = {}
    odds_fields: dict[str, float | None] = {}

    if isinstance(team_a_raw, dict):
        home_stats = _extract_team_stats(team_a_raw)
    if isinstance(team_b_raw, dict):
        away_stats = _extract_team_stats(team_b_raw)
    if isinstance(odds_raw, dict):
        odds_fields = _extract_odds(odds_raw)

    # --- Flat fields (pre-flattened format or fallback) ---
    possession_raw = item.get("possession")
    possession_pct = _safe_float(possession_raw)

    # Build shots from nested shoots data or flat fields
    shots_on = _safe_int(item.get("shots_on_target"))
    shots_off = _safe_int(item.get("shots_off_target"))

    return CanonicalProgressiveStats(
        fixture_id=match_id,
        timer_seconds=timer_seconds,
        team=team,
        # --- Core stats (flat format fallback) ---
        goals=_safe_int(item.get("goals")),
        possession_pct=possession_pct,
        dangerous_attacks=_safe_int(item.get("dangerous_attacks")),
        attacks=_safe_int(item.get("attacks")),
        shots_on_target=shots_on,
        shots_off_target=shots_off,
        corners=_safe_int(item.get("corners")),
        fouls=_safe_int(item.get("fouls")),
        yellow_cards=_safe_int(item.get("yellow_cards")),
        red_cards=_safe_int(item.get("red_cards")),
        substitutions=_safe_int(item.get("substitutions")),
        dominance_pct=_safe_float(item.get("dominance")),
        # --- Enhanced: xG per team ---
        xg_home=_safe_float(home_stats.get("xg")),
        xg_away=_safe_float(away_stats.get("xg")),
        # --- Enhanced: Dominance per team ---
        dominance_index_home=_safe_float(home_stats.get("dominance_index")),
        dominance_index_away=_safe_float(away_stats.get("dominance_index")),
        dominance_avg_home=_safe_float(home_stats.get("dominance_avg")),
        dominance_avg_away=_safe_float(away_stats.get("dominance_avg")),
        # --- Enhanced: Attacks split ---
        attacks_normal=_safe_int(home_stats.get("attacks")),
        attacks_dangerous=_safe_int(home_stats.get("dangerous_attacks")),
        attacks_normal_away=_safe_int(away_stats.get("attacks")),
        attacks_dangerous_away=_safe_int(away_stats.get("dangerous_attacks")),
        # --- Enhanced: Shoots breakdown (from nested teamA) ---
        shoots_total=_safe_int(home_stats.get("shoots_total")),
        shoots_on_target=_safe_int(home_stats.get("shoots_on_target")),
        shoots_off_target=_safe_int(home_stats.get("shoots_off_target")),
        # --- Enhanced: In-play odds ---
        odds_1x2_home=odds_fields.get("odds_1x2_home"),
        odds_1x2_draw=odds_fields.get("odds_1x2_draw"),
        odds_1x2_away=odds_fields.get("odds_1x2_away"),
        odds_ou_over=odds_fields.get("odds_ou_over"),
        odds_ou_under=odds_fields.get("odds_ou_under"),
        odds_ou_line=odds_fields.get("odds_ou_line"),
        odds_ah_home=odds_fields.get("odds_ah_home"),
        odds_ah_away=odds_fields.get("odds_ah_away"),
        odds_ah_line=odds_fields.get("odds_ah_line"),
        odds_asian_corner_over=odds_fields.get("odds_asian_corner_over"),
        odds_asian_corner_under=odds_fields.get("odds_asian_corner_under"),
        odds_asian_corner_line=odds_fields.get("odds_asian_corner_line"),
        # ht_start_timer / ht_end_timer are set post-hoc by detect_halftime_window()
    )


def _stats_signature(row: CanonicalProgressiveStats) -> tuple[object, ...]:
    """Build a fingerprint from stats fields for halftime freeze detection."""
    return (
        row.goals,
        row.shots_on_target,
        row.shots_off_target,
        row.corners,
        row.attacks,
        row.dangerous_attacks,
    )


_MIN_HALFTIME_RUN = 5
_HALFTIME_SEARCH_START_SECONDS = 43 * 60  # 43:00


def detect_halftime_window(
    rows: list[CanonicalProgressiveStats],
) -> list[CanonicalProgressiveStats]:
    """Detect halftime freeze and annotate all rows with ht_start_timer / ht_end_timer.

    Algorithm:
    1. Sort rows by timer_seconds.
    2. Build a stats fingerprint per entry (goals + shots + corners + attacks).
    3. Find the first run of 5+ consecutive entries with identical fingerprints
       after timer >= 43*60 seconds (43:00).
    4. ht_start_timer = timer_seconds of the first entry in the run.
    5. ht_end_timer = timer_seconds of the first entry after the run that changes.
    6. Write these values on EVERY row (so downstream can filter easily).

    Since CanonicalProgressiveStats is frozen, returns new instances with
    ht_start_timer and ht_end_timer set.
    """
    if not rows:
        return rows

    sorted_rows = sorted(rows, key=lambda r: r.timer_seconds)

    # Find halftime freeze
    ht_start: int | None = None
    ht_end: int | None = None

    run_start_idx: int | None = None
    run_length = 1
    prev_fp: tuple[object, ...] | None = None

    for idx, row in enumerate(sorted_rows):
        if row.timer_seconds < _HALFTIME_SEARCH_START_SECONDS:
            prev_fp = _stats_signature(row)
            run_start_idx = idx
            run_length = 1
            continue

        fp = _stats_signature(row)
        if fp == prev_fp:
            run_length += 1
        else:
            # Check if the just-ended run was long enough
            if run_length >= _MIN_HALFTIME_RUN and run_start_idx is not None:
                ht_start = sorted_rows[run_start_idx].timer_seconds
                ht_end = row.timer_seconds
                break
            run_start_idx = idx
            run_length = 1

        prev_fp = fp

    # Edge case: run extends to end of data (no change after freeze)
    if ht_start is None and run_length >= _MIN_HALFTIME_RUN and run_start_idx is not None:
        ht_start = sorted_rows[run_start_idx].timer_seconds
        # ht_end remains None — halftime not yet ended in data

    if ht_start is None:
        return rows  # No halftime detected, return unchanged

    # Rebuild all rows with halftime annotations
    return [row.model_copy(update={"ht_start_timer": ht_start, "ht_end_timer": ht_end}) for row in rows]
