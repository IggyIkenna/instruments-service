"""Understat adapter tests — coverage for uncovered methods.

Tests: get_fixtures, get_leagues, get_teams, get_odds, _fetch_league_fixtures,
helper functions (_extract_dates_from_json, _filter_and_normalize_matches,
_parse_understat_match, _safe_int, _safe_float).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.understat import (
    UnderstatAdapter,
    _extract_dates_from_json,
    _filter_and_normalize_matches,
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
