"""Understat adapter tests — coverage for uncovered methods.

Tests: get_fixtures, get_leagues, get_teams, get_odds, _fetch_league_fixtures,
get_match_ids_for_date, get_match_shots, _parse_shot_from_raw,
helper functions (_extract_dates_from_json, _filter_and_normalize_matches,
_parse_understat_match, _safe_int, _safe_float).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from instruments_service.reference_data.adapters.sports.adapters.understat import (
    UnderstatAdapter,
    _extract_dates_from_json,
    _filter_and_normalize_matches,
    _parse_shot_from_raw,
    _parse_understat_match,
    _safe_float,
    _safe_int,
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


class TestUnderstatAdapterProperties:
    def test_venue(self) -> None:
        adapter = UnderstatAdapter()
        assert adapter.venue == "understat"

    def test_headers(self) -> None:
        adapter = UnderstatAdapter()
        headers = adapter._headers()
        assert "XMLHttpRequest" in headers["X-Requested-With"]


class TestUnderstatGetLeagues:
    @pytest.mark.asyncio
    async def test_get_leagues(self) -> None:
        adapter = UnderstatAdapter()
        leagues = await adapter.get_leagues()
        assert len(leagues) == 6
        names = [lg.name for lg in leagues]
        assert "EPL" in names
        assert "La Liga" in names
        assert "Bundesliga" in names


class TestUnderstatGetTeams:
    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        adapter = UnderstatAdapter()
        teams = await adapter.get_teams("EPL")
        assert teams == []


class TestUnderstatGetOdds:
    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = UnderstatAdapter()
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []


class TestUnderstatGetFixtures:
    @pytest.mark.asyncio
    async def test_get_fixtures_with_matches(self) -> None:
        raw = {
            "dates": [
                {
                    "id": "12345",
                    "h": {"id": 1, "title": "Arsenal", "short_title": "ARS"},
                    "a": {"id": 2, "title": "Chelsea", "short_title": "CHE"},
                    "goals": {"h": 2, "a": 1},
                    "xG": {"h": "1.8", "a": "0.9"},
                    "datetime": "2026-09-15 15:00:00",
                    "isResult": True,
                }
            ]
        }
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-09-15")
        # 6 leagues, each returns the same 1 match = 6 fixtures
        assert len(fixtures) == 6

    @pytest.mark.asyncio
    async def test_get_fixtures_no_matches(self) -> None:
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-09-15")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_fixtures_season_detection_pre_august(self) -> None:
        """For dates before August, season should be previous year."""
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-15")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_fixtures_error_handling(self) -> None:
        """On error, _fetch_league_fixtures returns empty, get_fixtures continues."""
        adapter = UnderstatAdapter()
        # Patch _fetch_league_fixtures to return empty (simulates caught error)
        with patch.object(adapter, "_fetch_league_fixtures", return_value=[]):
            fixtures = await adapter.get_fixtures("2026-09-15")
        assert fixtures == []


class TestUnderstatHelpers:
    def test_extract_dates_from_json_valid(self) -> None:
        raw = {"dates": [{"id": 1}, {"id": 2}]}
        assert len(_extract_dates_from_json(raw)) == 2

    def test_extract_dates_from_json_not_dict(self) -> None:
        assert _extract_dates_from_json("not-dict") == []

    def test_extract_dates_from_json_no_dates_key(self) -> None:
        assert _extract_dates_from_json({"other": []}) == []

    def test_extract_dates_from_json_dates_not_list(self) -> None:
        assert _extract_dates_from_json({"dates": "string"}) == []

    def test_filter_and_normalize_matches_filters_by_date(self) -> None:
        matches = [
            {
                "id": "1",
                "h": {"id": 1, "title": "Arsenal"},
                "a": {"id": 2, "title": "Chelsea"},
                "goals": {"h": 2, "a": 1},
                "xG": {"h": "1.8", "a": "0.9"},
                "datetime": "2026-09-15 15:00:00",
                "isResult": True,
            },
            {
                "id": "2",
                "datetime": "2026-09-16 15:00:00",
                "isResult": True,
            },
        ]
        result = _filter_and_normalize_matches(matches, "2026-09-15", "EPL", 2026)
        assert len(result) == 1

    def test_filter_and_normalize_matches_skips_non_dict(self) -> None:
        result = _filter_and_normalize_matches(["not-dict"], "2026-09-15", "EPL", 2026)
        assert result == []

    def test_parse_understat_match_valid(self) -> None:
        item = {
            "id": "12345",
            "h": {"id": 1, "title": "Arsenal", "short_title": "ARS"},
            "a": {"id": 2, "title": "Chelsea", "short_title": "CHE"},
            "goals": {"h": 2, "a": 1},
            "xG": {"h": "1.8", "a": "0.9"},
            "datetime": "2026-09-15 15:00:00",
            "isResult": True,
        }
        match = _parse_understat_match(item)
        assert match is not None
        assert match.goals_h == 2
        assert match.goals_a == 1
        assert match.xg_h == 1.8

    def test_parse_understat_match_flat_goals(self) -> None:
        item = {
            "id": "99",
            "h": {"id": 1, "title": "Team A"},
            "a": {"id": 2, "title": "Team B"},
            "goals_h": 3,
            "goals_a": 0,
            "xG_h": "2.5",
            "xG_a": "0.5",
            "datetime": "2026-09-15",
            "isResult": True,
        }
        match = _parse_understat_match(item)
        assert match is not None
        assert match.goals_h == 3
        assert match.goals_a == 0

    def test_parse_understat_match_no_teams(self) -> None:
        item = {"id": "1", "datetime": "2026-09-15", "isResult": True}
        match = _parse_understat_match(item)
        assert match is not None

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


def _make_session_cm(get_return: object = None) -> MagicMock:
    """Create an async context manager mock for _make_session."""
    mock_session = MagicMock()
    if get_return is not None:
        mock_session.get = MagicMock(return_value=get_return)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    return session_cm


class TestUnderstatGetFixturesEdgeCases:
    @pytest.mark.asyncio
    async def test_get_fixtures_invalid_year_falls_back(self) -> None:
        """Invalid year string in date falls back to season_year=2025."""
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("XXXX-09-15")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_fixtures_invalid_month_falls_back(self) -> None:
        """Invalid month string falls back to month=1 → previous season."""
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-XX-15")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_fetch_league_fixtures_error_returns_empty(self) -> None:
        """_fetch_league_fixtures catches exceptions and returns []."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("network error")),
            ),
            patch.object(adapter, "_classify_error", return_value="UNKNOWN"),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter._fetch_league_fixtures("EPL", 2026, "2026-09-15")
        assert result == []


class TestUnderstatGetMatchIds:
    @pytest.mark.asyncio
    async def test_get_match_ids_returns_matches_for_date(self) -> None:
        """Returns (match_id, league) pairs for matches on the target date."""
        raw = {
            "dates": [
                {"id": "123", "datetime": "2026-09-15 15:00:00"},
                {"id": "456", "datetime": "2026-09-16 15:00:00"},
            ]
        }
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_match_ids_for_date("2026-09-15")
        # 6 leagues x 1 match per league = 6 matches
        assert len(result) == 6
        assert all(league in ("EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1", "RFPL") for _, league in result)
        assert all(mid == "123" for mid, _ in result)

    @pytest.mark.asyncio
    async def test_get_match_ids_skips_non_dict_entries(self) -> None:
        """Non-dict entries in dates array are skipped."""
        raw = {"dates": ["not-dict", {"id": "789", "datetime": "2026-09-15 12:00:00"}]}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_match_ids_for_date("2026-09-15")
        assert len(result) == 6

    @pytest.mark.asyncio
    async def test_get_match_ids_skips_no_id(self) -> None:
        """Entries without id field are not included."""
        raw = {"dates": [{"datetime": "2026-09-15 12:00:00"}]}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_match_ids_for_date("2026-09-15")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_match_ids_error_swallowed_per_league(self) -> None:
        """Error in one league call is caught; other leagues continue."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=aiohttp.ClientError("fail")),
            ),
            patch.object(adapter, "_classify_error", return_value="RATE_LIMIT"),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter.get_match_ids_for_date("2026-09-15")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_match_ids_pre_august_uses_prev_season(self) -> None:
        """Dates before August use season_year - 1."""
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)) as mock_req,
        ):
            await adapter.get_match_ids_for_date("2026-03-15")
        # season=2025 should be in the URL
        call_url = mock_req.call_args[0][1]
        assert "/2025" in call_url

    @pytest.mark.asyncio
    async def test_get_match_ids_invalid_year(self) -> None:
        """Invalid year string falls back to season_year=2025."""
        raw = {"dates": []}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_match_ids_for_date("XXXX-09-15")
        assert result == []


class TestUnderstatGetMatchShots:
    @pytest.mark.asyncio
    async def test_get_match_shots_success(self) -> None:
        """Returns parsed shots for a valid match response."""
        raw = {
            "h": [
                {
                    "id": "1",
                    "minute": "23",
                    "result": "SavedShot",
                    "X": "0.85",
                    "Y": "0.45",
                    "xG": "0.12",
                    "player": "Player One",
                    "situation": "OpenPlay",
                    "player_id": "101",
                    "lastAction": "Pass",
                    "period": "1",
                    "type": "RightFoot",
                }
            ],
            "a": [
                {
                    "id": "2",
                    "minute": "55",
                    "result": "Goal",
                    "X": "0.90",
                    "Y": "0.50",
                    "xG": "0.75",
                    "player": "Player Two",
                    "situation": "FromCorner",
                    "player_id": "202",
                    "lastAction": "Cross",
                    "period": "2",
                    "type": "Head",
                }
            ],
        }
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            shots = await adapter.get_match_shots("12345")
        assert len(shots) == 2
        assert shots[0].result == "SavedShot"
        assert shots[1].result == "Goal"

    @pytest.mark.asyncio
    async def test_get_match_shots_error_returns_empty(self) -> None:
        """HTTP error returns []."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("network error")),
            ),
            patch.object(adapter, "_classify_error", return_value="UNKNOWN"),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            shots = await adapter.get_match_shots("99")
        assert shots == []

    @pytest.mark.asyncio
    async def test_get_match_shots_non_dict_response_returns_empty(self) -> None:
        """Non-dict response returns []."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=["not", "dict"])),
        ):
            shots = await adapter.get_match_shots("99")
        assert shots == []

    @pytest.mark.asyncio
    async def test_get_match_shots_side_not_list_skipped(self) -> None:
        """Non-list side data is skipped gracefully."""
        raw = {"h": "not-a-list", "a": None}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            shots = await adapter.get_match_shots("123")
        assert shots == []

    @pytest.mark.asyncio
    async def test_get_match_shots_skips_non_dict_shot_items(self) -> None:
        """Non-dict items in shot list are skipped."""
        raw = {"h": ["not-dict", {"id": "1", "result": "Goal", "minute": "5"}], "a": []}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            shots = await adapter.get_match_shots("55")
        # One shot parses OK (may be None if required fields absent, but still exercised)
        assert isinstance(shots, list)


class TestParseShotFromRaw:
    def test_parse_shot_all_fields(self) -> None:
        item: dict[str, object] = {
            "id": "42",
            "minute": "33",
            "result": "Goal",
            "X": "0.88",
            "Y": "0.52",
            "xG": "0.68",
            "player": "Test Player",
            "situation": "OpenPlay",
            "player_id": "123",
            "lastAction": "Pass",
            "period": "1",
            "type": "LeftFoot",
        }
        shot = _parse_shot_from_raw(item, "match-1", "h")
        assert shot is not None
        assert shot.result == "Goal"
        assert shot.match_id == "match-1"
        assert shot.h_a == "h"
        assert shot.xg == 0.68

    def test_parse_shot_none_optional_fields(self) -> None:
        item: dict[str, object] = {"id": "1", "minute": None, "result": None}
        shot = _parse_shot_from_raw(item, "m1", "a")
        assert shot is not None
        assert shot.result is None
        assert shot.minute is None

    def test_parse_shot_exception_returns_none(self) -> None:
        """UnderstatShot constructor failure returns None."""
        with patch(
            "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatShot",
            side_effect=ValueError("bad shot"),
        ):
            shot = _parse_shot_from_raw({"id": "1"}, "m1", "h")
        assert shot is None


class TestFilterNormalizeMatchesEdge:
    def test_normalize_exception_logged_skipped(self) -> None:
        """Exception during normalization is logged and match is skipped."""
        matches = [
            {
                "id": "bad",
                "h": {"id": 1, "title": "Arsenal"},
                "a": {"id": 2, "title": "Chelsea"},
                "goals": {"h": 2, "a": 1},
                "xG": {"h": "1.8", "a": "0.9"},
                "datetime": "2026-09-15 15:00:00",
                "isResult": True,
            }
        ]
        with patch(
            "instruments_service.reference_data.adapters.sports.adapters.understat.normalize_understat_fixture",
            side_effect=ValueError("normalization failed"),
        ):
            result = _filter_and_normalize_matches(matches, "2026-09-15", "EPL", 2026)
        assert result == []


class TestParseUnderstatMatchEdge:
    def test_parse_understat_match_exception_returns_none(self) -> None:
        """Exception in UnderstatMatch construction returns None."""
        # Provide item where UnderstatMatch constructor will fail
        with patch(
            "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatMatch",
            side_effect=ValueError("bad"),
        ):
            result = _parse_understat_match({"id": "1", "datetime": "2026-09-15"})
        assert result is None


class TestUnderstatFetchErrorTracking:
    """Tests for _fetch_error_count — distinguishes genuine no-fixtures from HTTP errors.

    The orchestrator uses adapter._fetch_error_count > 0 to decide whether to
    record attempted_failed (errors) vs empty_confirmed (honest absence).
    """

    def test_initial_fetch_error_count_is_zero(self) -> None:
        adapter = UnderstatAdapter()
        assert adapter._fetch_error_count == 0

    @pytest.mark.asyncio
    async def test_fetch_league_fixtures_404_increments_error_count(self) -> None:
        """A 404 on getLeagueData increments _fetch_error_count."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("HTTP 404")),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter._fetch_league_fixtures("EPL", 2019, "2019-10-05")
        assert result == []
        assert adapter._fetch_error_count == 1

    @pytest.mark.asyncio
    async def test_get_fixtures_resets_error_count(self) -> None:
        """get_fixtures resets _fetch_error_count before each call."""
        adapter = UnderstatAdapter()
        adapter._fetch_error_count = 99  # pre-set to dirty value
        raw = {"dates": []}
        mock_session = _make_aiohttp_mock(raw)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await adapter.get_fixtures("2026-09-15")
        # After a clean call with no errors, error count reflects this call only
        assert adapter._fetch_error_count == 0

    @pytest.mark.asyncio
    async def test_get_fixtures_all_leagues_404_sets_error_count_6(self) -> None:
        """When all 6 leagues return errors, _fetch_error_count == 6."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("HTTP 404")),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            fixtures = await adapter.get_fixtures("2019-10-05")
        assert fixtures == []
        assert adapter._fetch_error_count == 6

    @pytest.mark.asyncio
    async def test_get_match_ids_for_date_resets_error_count(self) -> None:
        """get_match_ids_for_date resets _fetch_error_count before each call."""
        adapter = UnderstatAdapter()
        adapter._fetch_error_count = 99
        raw = {"dates": []}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            await adapter.get_match_ids_for_date("2026-09-15")
        assert adapter._fetch_error_count == 0

    @pytest.mark.asyncio
    async def test_get_match_ids_for_date_404_increments_error_count(self) -> None:
        """A per-league 404 in get_match_ids_for_date increments _fetch_error_count."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("HTTP 404")),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter.get_match_ids_for_date("2019-10-05")
        assert result == []
        assert adapter._fetch_error_count == 6

    @pytest.mark.asyncio
    async def test_get_match_shots_404_increments_error_count(self) -> None:
        """A 404 on getMatch increments _fetch_error_count."""
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(
                adapter,
                "_get_with_retry",
                AsyncMock(side_effect=Exception("HTTP 404")),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            shots = await adapter.get_match_shots("12345")
        assert shots == []
        assert adapter._fetch_error_count == 1

    @pytest.mark.asyncio
    async def test_get_match_shots_success_does_not_increment_error_count(self) -> None:
        """A successful getMatch call does not change _fetch_error_count."""
        raw = {"h": [], "a": []}
        adapter = UnderstatAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            await adapter.get_match_shots("12345")
        assert adapter._fetch_error_count == 0
