"""Transfermarkt adapter tests — coverage for uncovered methods.

Tests: get_teams (RapidAPI + Apify paths), get_fixtures, get_leagues, get_odds,
helper functions (_extract_clubs, _parse_squad, _parse_market_value,
_group_apify_players_into_clubs, _safe_int, _safe_float).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    TransfermarktAdapter,
    _extract_clubs,
    _group_apify_players_into_clubs,
    _parse_market_value,
    _parse_squad,
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


class TestTransfermarktAdapterProperties:
    def test_venue(self) -> None:
        adapter = TransfermarktAdapter(api_key="test-key")
        assert adapter.venue == "transfermarkt"

    def test_is_apify_true(self) -> None:
        adapter = TransfermarktAdapter(api_key="apify_api_12345")
        assert adapter._is_apify is True

    def test_is_apify_false(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapidapi-key-123")
        assert adapter._is_apify is False

    def test_is_apify_none(self) -> None:
        adapter = TransfermarktAdapter(api_key=None)
        assert adapter._is_apify is False

    def test_headers_rapidapi(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapidapi-key")
        headers = adapter._headers()
        assert headers["x-rapidapi-key"] == "rapidapi-key"

    def test_headers_apify(self) -> None:
        adapter = TransfermarktAdapter(api_key="apify_api_key123")
        headers = adapter._headers()
        assert headers["Authorization"] == "Bearer apify_api_key123"

    def test_headers_no_key_raises(self) -> None:
        adapter = TransfermarktAdapter(api_key=None)
        with pytest.raises(ValueError, match="API key"):
            adapter._headers()


class TestTransfermarktGetFixtures:
    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = TransfermarktAdapter(api_key="test-key")
        result = await adapter.get_fixtures("2026-04-01")
        assert result == []


class TestTransfermarktGetLeagues:
    @pytest.mark.asyncio
    async def test_get_leagues(self) -> None:
        adapter = TransfermarktAdapter(api_key="test-key")
        leagues = await adapter.get_leagues()
        # Should return leagues from TRANSFERMARKT_IDS SSOT
        assert len(leagues) > 0
        # Each league should have a non-empty league_id
        for lg in leagues:
            assert lg.league_id


class TestTransfermarktGetOdds:
    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = TransfermarktAdapter(api_key="test-key")
        result = await adapter.get_odds("soccer_epl")
        assert result == []


class TestTransfermarktGetTeams:
    @pytest.mark.asyncio
    async def test_get_teams_rapidapi(self) -> None:
        standings_response = {
            "data": [
                {"clubId": 100, "club": "Arsenal"},
                {"clubId": 200, "club": "Chelsea"},
            ]
        }
        profile_response = {"data": {"squadSize": 25, "averageAge": "26.3", "totalMarketValue": "€1.23bn"}}
        # Build a session mock that returns different responses for standings vs profile
        mock_resp_standings = AsyncMock()
        mock_resp_standings.status = 200
        mock_resp_standings.raise_for_status = MagicMock()
        mock_resp_standings.json = AsyncMock(return_value=standings_response)
        mock_resp_standings.headers = {}

        mock_resp_profile = AsyncMock()
        mock_resp_profile.status = 200
        mock_resp_profile.raise_for_status = MagicMock()
        mock_resp_profile.json = AsyncMock(return_value=profile_response)
        mock_resp_profile.headers = {}

        # Simple mock: return standings first, then profiles
        call_count = 0

        def make_resp_cm(resp: AsyncMock) -> MagicMock:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session_obj = MagicMock()

        def get_side_effect(*_args: object, **_kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return make_resp_cm(mock_resp_standings)
            return make_resp_cm(mock_resp_profile)

        mock_session_obj.get = MagicMock(side_effect=get_side_effect)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        adapter = TransfermarktAdapter(api_key="rapidapi-key")
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            teams = await adapter.get_teams("GB1", season=2025)
        assert len(teams) == 2


class TestTransfermarktHelpers:
    def test_extract_clubs_from_dict_clubs_key(self) -> None:
        raw = {"clubs": [{"id": "1", "name": "Arsenal"}]}
        result = _extract_clubs(raw)
        assert len(result) == 1

    def test_extract_clubs_from_dict_results_key(self) -> None:
        raw = {"results": [{"id": "1", "name": "Arsenal"}]}
        result = _extract_clubs(raw)
        assert len(result) == 1

    def test_extract_clubs_from_dict_data_key(self) -> None:
        raw = {"data": [{"id": "1", "name": "Arsenal"}]}
        result = _extract_clubs(raw)
        assert len(result) == 1

    def test_extract_clubs_from_list(self) -> None:
        raw = [{"id": "1"}, {"id": "2"}]
        result = _extract_clubs(raw)
        assert len(result) == 2

    def test_extract_clubs_empty(self) -> None:
        assert _extract_clubs({}) == []
        assert _extract_clubs(None) == []

    def test_extract_clubs_filters_non_dicts(self) -> None:
        result = _extract_clubs({"clubs": [{"id": "1"}, "not-dict"]})
        assert len(result) == 1

    def test_parse_squad_valid(self) -> None:
        item = {
            "id": "100",
            "name": "Arsenal",
            "squadSize": 25,
            "averageAge": "26.3",
            "players": [
                {
                    "id": 1,
                    "name": "Saka",
                    "position": "RW",
                    "nationality": "England",
                    "marketValue": "€120m",
                    "age": 22,
                    "contractUntil": "2028",
                    "image": "http://img.png",
                },
            ],
        }
        result = _parse_squad(item)
        assert result is not None
        assert result.team_name == "Arsenal"
        assert result.squad_size == 25
        assert result.players is not None
        assert len(result.players) == 1
        assert result.players[0].name == "Saka"

    def test_parse_squad_no_id(self) -> None:
        result = _parse_squad({"name": "Arsenal"})
        assert result is None

    def test_parse_squad_no_players(self) -> None:
        result = _parse_squad({"id": "100", "name": "Arsenal"})
        assert result is not None
        assert result.players is None

    def test_parse_squad_alternate_keys(self) -> None:
        item = {
            "id": "100",
            "name": "Arsenal",
            "squad_size": 25,
            "average_age": "26.3",
            "foreigners_number": 15,
            "total_market_value_eur": 800000000,
            "squad": [{"playerName": "Saka", "position": "RW"}],
        }
        result = _parse_squad(item)
        assert result is not None
        assert result.squad_size == 25
        assert result.foreigners_number == 15

    def test_parse_market_value_none(self) -> None:
        assert _parse_market_value(None) is None

    def test_parse_market_value_numeric(self) -> None:
        assert _parse_market_value(1000000) == 1000000.0
        assert _parse_market_value(1.5) == 1.5

    def test_parse_market_value_billions(self) -> None:
        result = _parse_market_value("€1.23bn")
        assert result is not None
        assert abs(result - 1_230_000_000) < 1

    def test_parse_market_value_millions(self) -> None:
        result = _parse_market_value("€120m")
        assert result is not None
        assert abs(result - 120_000_000) < 1

    def test_parse_market_value_thousands(self) -> None:
        result = _parse_market_value("€500k")
        assert result is not None
        assert abs(result - 500_000) < 1

    def test_parse_market_value_empty_string(self) -> None:
        assert _parse_market_value("") is None

    def test_parse_market_value_descriptive_text(self) -> None:
        result = _parse_market_value("€35.00m total market value")
        assert result is not None
        assert abs(result - 35_000_000) < 1

    def test_parse_market_value_invalid(self) -> None:
        assert _parse_market_value("not-a-number") is None

    def test_group_apify_players_into_clubs(self) -> None:
        players = [
            {"currentClubId": 100, "currentClub": "Arsenal", "playerId": 1, "playerName": "Saka"},
            {"currentClubId": 100, "currentClub": "Arsenal", "playerId": 2, "playerName": "Rice"},
            {"currentClubId": 200, "currentClub": "Chelsea", "playerId": 3, "playerName": "Palmer"},
            {"currentClubId": None, "playerName": "Free Agent"},  # No club - skipped
        ]
        result = _group_apify_players_into_clubs(players)
        clubs = result["clubs"]
        assert len(clubs) == 2
        arsenal = next(c for c in clubs if c["name"] == "Arsenal")
        assert len(arsenal["players"]) == 2

    def test_group_apify_players_empty(self) -> None:
        result = _group_apify_players_into_clubs([])
        assert result == {"clubs": []}

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
