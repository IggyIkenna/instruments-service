"""SoccerFootball.info adapter tests — coverage for uncovered methods.

Tests: get_leagues, get_standings, get_fixtures, get_teams, get_match_ids_for_date,
get_progressive_stats, get_odds, helper functions, detect_halftime_window.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    SoccerFootballInfoAdapter,
    _extract_data,
    _extract_odds,
    _extract_team_stats,
    _normalize_sfi_progressive_stat,
    _normalize_sfi_standing,
    _parse_timer_to_seconds,
    _safe_float,
    _safe_int,
    _stats_signature,
    detect_halftime_window,
)


def _make_aiohttp_mock(
    json_response: object,
    status: int = 200,
) -> MagicMock:
    """Build a mock aiohttp.ClientSession."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    if status >= 400:
        mock_resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    mock_resp.json = AsyncMock(return_value=json_response)
    mock_resp.headers = {}

    resp_cm = MagicMock()
    resp_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=resp_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm


# ---------------------------------------------------------------------------
# Adapter property tests
# ---------------------------------------------------------------------------


class TestSFIAdapterProperties:
    def test_venue(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        assert adapter.venue == "soccerfootball_info"

    def test_headers_with_key(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        headers = adapter._headers()
        assert headers["x-rapidapi-key"] == "test-key"
        assert "soccer-football-info" in headers["x-rapidapi-host"]

    def test_headers_no_key_raises(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key=None)
        with pytest.raises(ValueError, match="API key"):
            adapter._headers()


# ---------------------------------------------------------------------------
# get_leagues
# ---------------------------------------------------------------------------


class TestSFIGetLeagues:
    @pytest.mark.asyncio
    async def test_get_leagues_success(self) -> None:
        raw = {
            "data": [
                {
                    "id": "123",
                    "name": "Premier League",
                    "country": "England",
                    "type": "league",
                    "logo": "http://logo.png",
                },
                {"id": "456", "name": "La Liga", "country": "Spain"},
            ]
        }
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].league_id == "123"
        assert leagues[0].name == "Premier League"
        assert leagues[0].country == "England"
        assert leagues[0].league_type == "league"
        assert leagues[0].logo_url == "http://logo.png"

    @pytest.mark.asyncio
    async def test_get_leagues_with_alternate_keys(self) -> None:
        raw = {"data": [{"league_id": "789", "league_name": "Bundesliga", "country_name": "Germany"}]}
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 1
        assert leagues[0].league_id == "789"
        assert leagues[0].name == "Bundesliga"
        assert leagues[0].country == "Germany"

    @pytest.mark.asyncio
    async def test_get_leagues_skips_non_dict(self) -> None:
        raw = {"data": ["not-a-dict", {"id": "1", "name": "Test", "country": "X"}]}
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 1

    @pytest.mark.asyncio
    async def test_get_leagues_empty(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert leagues == []


# ---------------------------------------------------------------------------
# get_standings
# ---------------------------------------------------------------------------


class TestSFIGetStandings:
    @pytest.mark.asyncio
    async def test_get_standings_returns_empty(self) -> None:
        # SFI has no standings endpoint — retired 2026-04-24 after 100% failure
        # (42/42 phantom rows on 2026-04-29). No HTTP call is made.
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        standings = await adapter.get_standings("league-1", season="2025")
        assert standings == []

    @pytest.mark.asyncio
    async def test_get_standings_returns_empty_no_season(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        standings = await adapter.get_standings("league-1")
        assert standings == []


# ---------------------------------------------------------------------------
# get_fixtures
# ---------------------------------------------------------------------------


class TestSFIGetFixtures:
    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        result = await adapter.get_fixtures("2026-04-01")
        assert result == []


# ---------------------------------------------------------------------------
# get_teams
# ---------------------------------------------------------------------------


class TestSFIGetTeams:
    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        # SFI team data was derived from standings, which SFI doesn't support.
        # No HTTP call is made; returns empty immediately.
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        teams = await adapter.get_teams("league-1")
        assert teams == []


# ---------------------------------------------------------------------------
# get_match_ids_for_date
# ---------------------------------------------------------------------------


class TestSFIGetMatchIds:
    @pytest.mark.asyncio
    async def test_get_match_ids_success(self) -> None:
        raw = {
            "data": [
                {"id": "M1", "status": "FT"},
                {"id": "M2", "status": "AET"},
                {"id": "M3", "status": "PEN"},
                {"match_id": "M4", "status": "FINISHED"},
                {"id": "M5", "status": "NS"},  # not started - excluded
                {"id": "M6", "status": "1H"},  # in play - excluded
            ]
        }
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            ids = await adapter.get_match_ids_for_date("2026-04-01")
        assert len(ids) == 4
        assert "M1" in ids
        assert "M2" in ids
        assert "M3" in ids
        assert "M4" in ids


# ---------------------------------------------------------------------------
# get_progressive_stats
# ---------------------------------------------------------------------------


class TestSFIGetProgressiveStats:
    @pytest.mark.asyncio
    async def test_get_progressive_stats_success(self) -> None:
        raw = {
            "data": [
                {
                    "timer": "45:00",
                    "team": "home",
                    "goals": 1,
                    "possession": 55.0,
                    "shots_on_target": 3,
                    "shots_off_target": 2,
                    "dangerous_attacks": 10,
                    "attacks": 20,
                    "corners": 4,
                    "fouls": 5,
                    "yellow_cards": 1,
                    "red_cards": 0,
                    "teamA": {
                        "goal": 1,
                        "possession": 55,
                        "corners": 4,
                        "xG": 0.8,
                        "attacks": {"n": 20, "d": 10},
                        "shoots": {"t": 5, "on": 3, "off": 2},
                        "fouls": {"t": 5, "y_c": 1, "r_c": 0},
                        "dominance": {"index": "52.3", "avg_2_5": "48.1"},
                    },
                    "teamB": {
                        "goal": 0,
                        "possession": 45,
                        "xG": 0.3,
                        "attacks": {"n": 15, "d": 5},
                        "shoots": {"t": 3, "on": 1, "off": 2},
                        "fouls": {"t": 3, "y_c": 0, "r_c": 0},
                        "dominance": {"index": "47.7", "avg_2_5": "51.9"},
                    },
                    "odds": {
                        "1X2": {"1": "1.181", "X": "6.000", "2": "13.000"},
                        "over_under": {"o": "1.850", "u": "1.950", "v": "4.5"},
                        "asian_handicap": {"1": "1.875", "2": "1.925", "v": "-0.5"},
                        "asian_corner": {"o": "2.025", "u": "1.775", "v": "6.5"},
                    },
                }
            ]
        }
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            stats = await adapter.get_progressive_stats("match-123")
        assert len(stats) == 1
        s = stats[0]
        assert s.fixture_id == "match-123"
        assert s.timer_seconds == 2700
        assert s.goals == 1
        assert s.xg_home == 0.8
        assert s.xg_away == 0.3
        assert s.odds_1x2_home == 1.181


# ---------------------------------------------------------------------------
# get_odds
# ---------------------------------------------------------------------------


class TestSFIGetOdds:
    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        result = await adapter.get_odds("soccer_epl")
        assert result == []


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestSFIHelpers:
    def test_extract_data_dict_data_key(self) -> None:
        assert len(_extract_data({"data": [{"id": 1}]})) == 1

    def test_extract_data_dict_result_key(self) -> None:
        assert len(_extract_data({"result": [{"id": 1}]})) == 1

    def test_extract_data_dict_standings_key(self) -> None:
        assert len(_extract_data({"standings": [{"id": 1}]})) == 1

    def test_extract_data_dict_leagues_key(self) -> None:
        assert len(_extract_data({"leagues": [{"id": 1}]})) == 1

    def test_extract_data_dict_response_key(self) -> None:
        assert len(_extract_data({"response": [{"id": 1}]})) == 1

    def test_extract_data_list(self) -> None:
        assert len(_extract_data([{"id": 1}, {"id": 2}])) == 2

    def test_extract_data_empty(self) -> None:
        assert _extract_data({}) == []
        assert _extract_data(None) == []
        assert _extract_data("string") == []

    def test_extract_data_filters_non_dicts(self) -> None:
        result = _extract_data({"data": [{"id": 1}, "not-dict", 42]})
        assert len(result) == 1

    def test_safe_int(self) -> None:
        assert _safe_int(42) == 42
        assert _safe_int("42") == 42
        assert _safe_int(None) is None
        assert _safe_int("abc") is None

    def test_safe_float(self) -> None:
        assert _safe_float(1.5) == 1.5
        assert _safe_float("1.5") == 1.5
        assert _safe_float(None) is None
        assert _safe_float("abc") is None

    def test_parse_timer_to_seconds(self) -> None:
        assert _parse_timer_to_seconds("00:00") == 0
        assert _parse_timer_to_seconds("00:30") == 30
        assert _parse_timer_to_seconds("45:00") == 2700
        assert _parse_timer_to_seconds("90:00") == 5400

    def test_parse_timer_to_seconds_stoppage_time(self) -> None:
        """SFI's "MM:SS+MM:SS" stoppage form must add, not collapse to 0.

        ~15% of a fixture's real snapshots use this form (measured 2026-07-16),
        including the run-up to the halftime whistle.
        """
        assert _parse_timer_to_seconds("45:00+00:00") == 2700
        assert _parse_timer_to_seconds("45:00+02:30") == 2850
        assert _parse_timer_to_seconds("90:00+07:30") == 5850

    def test_parse_timer_to_seconds_unparseable_is_none_not_zero(self) -> None:
        """None, never 0 — a 0 would masquerade as a genuine pre-kickoff row."""
        assert _parse_timer_to_seconds("bad") is None
        assert _parse_timer_to_seconds("ab:cd") is None
        assert _parse_timer_to_seconds("") is None
        assert _parse_timer_to_seconds("45:00+bad") is None

    def test_normalize_sfi_standing(self) -> None:
        item = {
            "position": 1,
            "team_id": "T1",
            "team_name": "Arsenal",
            "points": 72,
            "goal_difference": 35,
            "group": "A",
            "form": "WWWDW",
            "played": 30,
            "wins": 22,
            "draws": 6,
            "losses": 2,
            "goals_for": 65,
            "goals_against": 30,
        }
        result = _normalize_sfi_standing(item, "league-1", "2025")
        assert result.rank == 1
        assert result.team_name == "Arsenal"
        assert result.points == 72
        assert result.goals_diff == 35
        assert result.group == "A"
        assert result.form == "WWWDW"

    def test_extract_team_stats(self) -> None:
        team = {
            "goal": 2,
            "possession": 55,
            "corners": 4,
            "substitutions": 3,
            "penalties": 0,
            "xG": 1.5,
            "attacks": {"n": 20, "d": 10},
            "shoots": {"t": 6, "on": 3, "off": 3},
            "fouls": {"t": 8, "y_c": 2, "r_c": 0},
            "dominance": {"index": "55.0", "avg_2_5": "50.0"},
        }
        result = _extract_team_stats(team)
        assert result["goals"] == 2
        assert result["attacks"] == 20
        assert result["dangerous_attacks"] == 10
        assert result["shoots_total"] == 6
        assert result["shoots_on_target"] == 3
        assert result["fouls"] == 8
        assert result["yellow_cards"] == 2
        assert result["red_cards"] == 0
        assert result["dominance_index"] == "55.0"

    def test_extract_odds(self) -> None:
        odds = {
            "1X2": {"1": "1.5", "X": "4.0", "2": "6.0"},
            "over_under": {"o": "1.85", "u": "1.95", "v": "2.5"},
            "asian_handicap": {"1": "1.9", "2": "1.9", "v": "-0.5"},
            "asian_corner": {"o": "2.0", "u": "1.8", "v": "9.5"},
        }
        result = _extract_odds(odds)
        assert result["odds_1x2_home"] == 1.5
        assert result["odds_1x2_draw"] == 4.0
        assert result["odds_1x2_away"] == 6.0
        assert result["odds_ou_over"] == 1.85
        assert result["odds_ah_home"] == 1.9
        assert result["odds_asian_corner_over"] == 2.0

    def test_extract_odds_empty(self) -> None:
        result = _extract_odds({})
        assert result == {}

    def test_extract_odds_first_half_markets(self) -> None:
        """The ``1h_*`` family — verified against the live API 2026-07-16.

        Provider keys are ``1h_result`` / ``1h_asian_handicap`` / ``1h_goalline``
        / ``1h_asian_corner`` — NOT the flat ``h1_home_win`` spelling of the
        legacy bulk-dump table mirrored by ``SFMatchProgressiveOddsRaw``.
        """
        odds = {
            "1h_result": {"1": "2.600", "X": "2.250", "2": "4.000"},
            "1h_asian_handicap": {"1": "1.875", "2": "1.925", "v": "-0.5"},
            "1h_goalline": {"o": "1.850", "u": "1.950", "v": "1.5"},
            "1h_asian_corner": {"o": "2.025", "u": "1.775", "v": "3.5"},
        }
        result = _extract_odds(odds)
        assert result["odds_h1_result_home"] == 2.6
        assert result["odds_h1_result_draw"] == 2.25
        assert result["odds_h1_result_away"] == 4.0
        assert result["odds_h1_ah_home"] == 1.875
        assert result["odds_h1_ah_away"] == 1.925
        assert result["odds_h1_ah_line"] == -0.5
        assert result["odds_h1_goalline_over"] == 1.85
        assert result["odds_h1_goalline_under"] == 1.95
        assert result["odds_h1_goalline_line"] == 1.5
        assert result["odds_h1_ac_over"] == 2.025
        assert result["odds_h1_ac_under"] == 1.775
        assert result["odds_h1_ac_line"] == 3.5

    def test_extract_odds_first_half_distinct_from_full_time(self) -> None:
        """A first-half price must never be conflated with the full-time 1X2."""
        odds = {
            "1X2": {"1": "1.980", "X": "3.350", "2": "3.850"},
            "1h_result": {"1": "2.600", "X": "2.250", "2": "4.000"},
        }
        result = _extract_odds(odds)
        assert result["odds_1x2_home"] == 1.98
        assert result["odds_h1_result_home"] == 2.6
        assert result["odds_1x2_home"] != result["odds_h1_result_home"]

    def test_extract_odds_null_and_dash_sentinels(self) -> None:
        """SFI signals "no price" as null OR the string "-" — both mean None."""
        odds = {
            "1h_result": {"1": None, "X": "-", "2": "4.000"},
            "1h_asian_corner": {"o": None, "u": None, "v": None},
        }
        result = _extract_odds(odds)
        assert result["odds_h1_result_home"] is None
        assert result["odds_h1_result_draw"] is None
        assert result["odds_h1_result_away"] == 4.0
        assert result["odds_h1_ac_over"] is None

    def test_normalize_progressive_stat_carries_first_half_odds(self) -> None:
        """End-to-end: the nested 1h_* payload reaches the canonical fields."""
        item = {
            "timer": "40:00",
            "team": "home",
            "odds": {
                "1X2": {"1": "1.980", "X": "3.350", "2": "3.850"},
                "1h_result": {"1": "2.600", "X": "2.250", "2": "4.000"},
                "1h_goalline": {"o": "1.850", "u": "1.950", "v": "1.5"},
            },
        }
        row = _normalize_sfi_progressive_stat(item, "match-1")
        assert row.timer_seconds == 2400
        assert row.odds_1x2_home == 1.98
        assert row.odds_h1_result_home == 2.6
        assert row.odds_h1_result_draw == 2.25
        assert row.odds_h1_result_away == 4.0
        assert row.odds_h1_goalline_over == 1.85

    def test_normalize_progressive_stat_rejects_unparseable_timer(self) -> None:
        """Refuse to coerce a bad timer to 0 — raise so the caller drops the row."""
        with pytest.raises(ValueError, match="unparseable SFI timer"):
            _normalize_sfi_progressive_stat({"timer": "garbage", "team": "home"}, "match-1")

    def test_normalize_sfi_progressive_stat(self) -> None:
        item = {
            "timer": "30:00",
            "team": "home",
            "goals": 1,
            "possession": 60.0,
            "shots_on_target": 3,
            "shots_off_target": 2,
            "dangerous_attacks": 8,
            "attacks": 15,
            "corners": 3,
            "fouls": 4,
            "yellow_cards": 1,
            "red_cards": 0,
            "substitutions": 0,
            "dominance": 55.0,
        }
        result = _normalize_sfi_progressive_stat(item, "M1")
        assert result.fixture_id == "M1"
        assert result.timer_seconds == 1800
        assert result.goals == 1
        assert result.possession_pct == 60.0

    def test_stats_signature(self) -> None:
        from unified_api_contracts.sports import CanonicalProgressiveStats

        row = CanonicalProgressiveStats(
            fixture_id="M1",
            timer_seconds=2700,
            team="home",
            goals=1,
            shots_on_target=3,
            shots_off_target=2,
            corners=4,
            attacks=20,
            dangerous_attacks=10,
        )
        fp = _stats_signature(row)
        assert fp == (1, 3, 2, 4, 20, 10)


# ---------------------------------------------------------------------------
# detect_halftime_window
# ---------------------------------------------------------------------------


class TestDetectHalftimeWindow:
    def test_empty_rows(self) -> None:
        result = detect_halftime_window([])
        assert result == []

    def test_no_halftime_detected(self) -> None:
        from unified_api_contracts.sports import CanonicalProgressiveStats

        rows = [
            CanonicalProgressiveStats(
                fixture_id="M1", timer_seconds=i * 60, team="home", goals=i // 30, shots_on_target=i, corners=i
            )
            for i in range(90)
        ]
        # Each row has different stats so no freeze run detected
        result = detect_halftime_window(rows)
        # No halftime annotations since all stats change
        assert all(r.ht_start_timer is None for r in result)

    def test_halftime_freeze_detected(self) -> None:
        from unified_api_contracts.sports import CanonicalProgressiveStats

        rows = []
        # First half: stats change each minute
        for i in range(45):
            rows.append(
                CanonicalProgressiveStats(
                    fixture_id="M1",
                    timer_seconds=i * 60,
                    team="home",
                    goals=1 if i >= 20 else 0,
                    shots_on_target=min(i, 5),
                    shots_off_target=min(i, 3),
                    corners=min(i, 4),
                    attacks=i,
                    dangerous_attacks=i // 2,
                )
            )
        # Halftime freeze: 6 entries with identical stats (from 45:00 to 50:00)
        for i in range(6):
            rows.append(
                CanonicalProgressiveStats(
                    fixture_id="M1",
                    timer_seconds=(45 + i) * 60,
                    team="home",
                    goals=1,
                    shots_on_target=5,
                    shots_off_target=3,
                    corners=4,
                    attacks=44,
                    dangerous_attacks=22,
                )
            )
        # Second half: stats change again
        rows.append(
            CanonicalProgressiveStats(
                fixture_id="M1",
                timer_seconds=51 * 60,
                team="home",
                goals=2,
                shots_on_target=6,
                shots_off_target=3,
                corners=5,
                attacks=45,
                dangerous_attacks=23,
            )
        )
        result = detect_halftime_window(rows)
        # All rows should have halftime annotations
        assert result[0].ht_start_timer is not None

    def test_halftime_freeze_extends_to_end(self) -> None:
        from unified_api_contracts.sports import CanonicalProgressiveStats

        rows = []
        # Pre-halftime: changing stats
        for i in range(43):
            rows.append(
                CanonicalProgressiveStats(
                    fixture_id="M1",
                    timer_seconds=i * 60,
                    team="home",
                    goals=0,
                    shots_on_target=i,
                    corners=i,
                    attacks=i,
                    dangerous_attacks=i,
                )
            )
        # Freeze from 43:00 to end (6+ entries, identical stats)
        for i in range(8):
            rows.append(
                CanonicalProgressiveStats(
                    fixture_id="M1",
                    timer_seconds=(43 + i) * 60,
                    team="home",
                    goals=0,
                    shots_on_target=42,
                    corners=42,
                    attacks=42,
                    dangerous_attacks=42,
                )
            )
        result = detect_halftime_window(rows)
        assert result[0].ht_start_timer is not None
        # ht_end should be None since freeze extends to end
        assert result[0].ht_end_timer is None
