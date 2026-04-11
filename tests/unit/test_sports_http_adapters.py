"""HTTP adapter mock tests for sports reference data adapters.

Covers: api_football and base adapter error classification.

Each test mocks ``aiohttp.ClientSession`` to simulate HTTP responses,
exercising parsing, normalization, error handling, and edge cases.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
    _extract_response,
    _flatten_standings_groups,
    _parse_fixture_list,
    _parse_fixture_response,
    _parse_team_item,
    _parse_teams,
)
from instruments_service.reference_data.adapters.sports.adapters.base import (
    BaseSportsReferenceAdapter,
)

# ---------------------------------------------------------------------------
# Shared aiohttp mock helper
# ---------------------------------------------------------------------------


def _make_aiohttp_mock(
    json_response: object,
    status: int = 200,
    headers: dict[str, str] | None = None,
    text_response: str | None = None,
) -> MagicMock:
    """Build a mock that replaces ``aiohttp.ClientSession`` context manager.

    Returns a MagicMock suitable for ``patch("aiohttp.ClientSession", return_value=...)``.
    The mock session object supports ``session.get(url, ...)`` returning an async CM
    whose ``__aenter__`` yields a response with the given status, JSON, and headers.
    """
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    if status >= 400:
        mock_resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    mock_resp.json = AsyncMock(return_value=json_response)
    mock_resp.headers = headers or {}
    if text_response is not None:
        mock_resp.text = AsyncMock(return_value=text_response)

    # resp context manager (async with session.get(...) as resp)
    resp_cm = MagicMock()
    resp_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_cm.__aexit__ = AsyncMock(return_value=None)

    # session object
    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=resp_cm)

    # POST (needed for Apify-style requests)
    mock_session_obj.post = MagicMock(return_value=resp_cm)

    # session context manager (async with aiohttp.ClientSession() as session)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm


# ===========================================================================
# API Football adapter tests
# ===========================================================================


class TestApiFootballHelpers:
    """Unit tests for API Football helper functions."""

    def test_extract_response_dict_with_response_list(self) -> None:
        raw = {"response": [{"id": 1}, {"id": 2}]}
        result = _extract_response(raw)
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_extract_response_dict_without_response_key(self) -> None:
        raw = {"data": [{"id": 1}]}
        result = _extract_response(raw)
        assert result == []

    def test_extract_response_raw_list(self) -> None:
        raw = [{"id": 1}, {"id": 2}, "not a dict"]
        result = _extract_response(raw)
        assert len(result) == 2

    def test_extract_response_non_list_non_dict(self) -> None:
        assert _extract_response("string") == []
        assert _extract_response(42) == []
        assert _extract_response(None) == []

    def test_flatten_standings_groups_nested(self) -> None:
        groups = [
            [{"team": "A", "points": 30}, {"team": "B", "points": 25}],
            [{"team": "C", "points": 20}],
        ]
        rows = _flatten_standings_groups(groups)
        assert len(rows) == 3
        assert rows[0]["team"] == "A"

    def test_flatten_standings_groups_non_list_items_skipped(self) -> None:
        groups = [
            "not a list",
            [{"team": "D", "points": 15}],
            42,
        ]
        rows = _flatten_standings_groups(groups)
        assert len(rows) == 1

    def test_flatten_standings_groups_non_dict_rows_skipped(self) -> None:
        groups = [[{"team": "E"}, "not a dict", 99]]
        rows = _flatten_standings_groups(groups)
        assert len(rows) == 1

    def test_parse_fixture_response_valid(self) -> None:
        item = {
            "fixture": {
                "id": 123456,
                "date": "2026-03-22T15:00:00+00:00",
                "timestamp": 1774296000,
                "timezone": "UTC",
                "referee": "Mike Dean",
                "venue": {"name": "Anfield"},
                "status": {"long": "Not Started", "short": "NS"},
            },
            "league": {"id": 39, "name": "Premier League", "season": 2025},
            "teams": {
                "home": {"id": 40, "name": "Liverpool"},
                "away": {"id": 33, "name": "Manchester United"},
            },
            "goals": {"home": 0, "away": 0},
            "score": {"halftime": {"home": 0, "away": 0}},
        }
        result = _parse_fixture_response(item)
        assert result is not None
        assert result.id == 123456

    def test_parse_fixture_response_missing_fixture_data(self) -> None:
        item = {"league": {"id": 39}, "teams": {}}
        result = _parse_fixture_response(item)
        assert result is None

    def test_parse_fixture_response_fixture_not_dict(self) -> None:
        item = {"fixture": "not a dict", "league": {}, "teams": {}}
        result = _parse_fixture_response(item)
        assert result is None

    def test_parse_teams_valid(self) -> None:
        teams_data = {
            "home": {"id": 40, "name": "Liverpool"},
            "away": {"id": 33, "name": "Manchester United"},
        }
        result = _parse_teams(teams_data)
        assert result is not None
        assert "home" in result
        assert "away" in result

    def test_parse_teams_non_dict_side_skipped(self) -> None:
        teams_data = {"home": "not a dict", "away": {"id": 33, "name": "Man Utd"}}
        result = _parse_teams(teams_data)
        assert result is not None
        assert "away" in result
        assert "home" not in result

    def test_parse_teams_empty_returns_none(self) -> None:
        teams_data = {"home": "bad", "away": "bad"}
        result = _parse_teams(teams_data)
        assert result is None

    def test_parse_team_item_with_venue(self) -> None:
        team_data = {
            "name": "Liverpool",
            "code": "LIV",
            "country": "England",
            "founded": 1892,
            "logo": "https://example.com/logo.png",
        }
        venue_data = {"name": "Anfield", "city": "Liverpool", "capacity": 54074}
        team = _parse_team_item(team_data, venue_data)
        assert team.name == "Liverpool"
        assert team.short_name == "LIV"
        assert team.country == "England"
        assert team.founded == 1892
        assert team.venue is not None
        assert team.venue.name == "Anfield"

    def test_parse_team_item_without_venue(self) -> None:
        team_data = {"name": "Chelsea", "code": "CHE"}
        team = _parse_team_item(team_data, None)
        assert team.name == "Chelsea"
        assert team.venue is None

    def test_parse_team_item_venue_not_dict(self) -> None:
        team_data = {"name": "Arsenal"}
        team = _parse_team_item(team_data, "not a dict")
        assert team.venue is None

    def test_parse_team_item_venue_no_name(self) -> None:
        team_data = {"name": "Tottenham"}
        venue_data = {"city": "London"}
        team = _parse_team_item(team_data, venue_data)
        assert team.venue is None

    def test_parse_fixture_list_normalizes(self) -> None:
        """Test that _parse_fixture_list handles items with missing fixture data gracefully."""
        response_list = [
            {"not_fixture": "bad"},
            {
                "fixture": {
                    "id": 1,
                    "date": "2026-03-22T15:00:00+00:00",
                    "status": {"long": "Not Started", "short": "NS"},
                },
                "league": {"id": 39, "name": "Premier League", "season": 2025},
                "teams": {
                    "home": {"id": 40, "name": "Liverpool"},
                    "away": {"id": 33, "name": "Manchester United"},
                },
                "goals": None,
                "score": None,
            },
        ]
        # Will skip items that fail normalization, but should not crash
        result = _parse_fixture_list(response_list)
        assert isinstance(result, list)


class TestApiFootballAdapterHttp:
    """Async HTTP tests for ApiFootballAdapter."""

    @pytest.fixture(autouse=True)
    def _no_retry_backoff(self):
        """Patch asyncio.sleep so adapter retry backoff is instant in tests."""
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_get_fixtures_single_league(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {
                        "fixture": {
                            "id": 100,
                            "date": "2026-03-22T15:00:00+00:00",
                            "status": {"long": "Not Started", "short": "NS"},
                        },
                        "league": {"id": 39, "name": "Premier League", "season": 2025},
                        "teams": {
                            "home": {"id": 40, "name": "Liverpool"},
                            "away": {"id": 33, "name": "Manchester United"},
                        },
                        "goals": None,
                        "score": None,
                    }
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22", league_ids=[39])
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_fixtures_no_league_ids(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"response": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_fixtures_multi_league(self) -> None:
        """Multiple league IDs triggers additional fetches per league."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"response": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22", league_ids=[39, 140])
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_fixtures_http_error_raises(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(Exception, match="HTTP 500"),
        ):
            await adapter.get_fixtures("2026-03-22")

    @pytest.mark.asyncio
    async def test_get_leagues_parses_correctly(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {
                        "league": {
                            "id": 39,
                            "name": "Premier League",
                            "type": "League",
                            "logo": "https://example.com/logo.png",
                        },
                        "country": {"name": "England"},
                    },
                    {
                        "league": {
                            "id": 140,
                            "name": "La Liga",
                            "type": "League",
                        },
                        "country": {"name": "Spain"},
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].name == "Premier League"
        assert leagues[0].country == "England"
        assert leagues[1].name == "La Liga"

    @pytest.mark.asyncio
    async def test_get_leagues_skips_non_dict_items(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {"response": ["not_dict", {"league": "not_dict"}, {"league": {"id": 1, "name": "A"}, "country": {}}]}
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 1

    @pytest.mark.asyncio
    async def test_get_teams_parses_correctly(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {
                        "team": {
                            "id": 40,
                            "name": "Liverpool",
                            "code": "LIV",
                            "country": "England",
                            "founded": 1892,
                            "logo": "https://example.com/logo.png",
                        },
                        "venue": {"name": "Anfield"},
                    },
                    {
                        "team": {
                            "id": 33,
                            "name": "Manchester United",
                            "code": "MUN",
                        },
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(39, season=2025)
        assert len(teams) == 2
        assert teams[0].name == "Liverpool"
        assert teams[1].name == "Manchester United"

    @pytest.mark.asyncio
    async def test_get_teams_skips_non_dict_team(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"response": [{"team": "not_a_dict"}, "not_a_dict"]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(39)
        assert teams == []

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    @pytest.mark.asyncio
    async def test_get_standings_parses(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {
                        "league": {
                            "id": 39,
                            "standings": [
                                [
                                    {"rank": 1, "team": {"name": "Liverpool"}, "points": 80},
                                    {"rank": 2, "team": {"name": "Arsenal"}, "points": 75},
                                ]
                            ],
                        }
                    }
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            standings = await adapter.get_standings(39, season=2025)
        assert len(standings) == 2
        assert standings[0]["rank"] == 1

    @pytest.mark.asyncio
    async def test_get_standings_empty_on_error(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            standings = await adapter.get_standings(39)
        assert standings == []

    @pytest.mark.asyncio
    async def test_get_injuries(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"player": {"name": "Mo Salah"}, "type": "Missing Fixtures"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            injuries = await adapter.get_injuries("2026-03-22")
        assert len(injuries) == 1

    @pytest.mark.asyncio
    async def test_get_injuries_error_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            injuries = await adapter.get_injuries("2026-03-22")
        assert injuries == []

    @pytest.mark.asyncio
    async def test_get_fixture_statistics(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"team": {"id": 40, "name": "Liverpool"}, "statistics": [{"type": "Shots on Goal", "value": 7}]},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            stats = await adapter.get_fixture_statistics(123456)
        assert len(stats) == 1

    @pytest.mark.asyncio
    async def test_get_fixture_statistics_error_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            stats = await adapter.get_fixture_statistics(123456)
        assert stats == []

    @pytest.mark.asyncio
    async def test_get_fixture_events(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"time": {"elapsed": 23}, "type": "Goal", "team": {"name": "Liverpool"}},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            events = await adapter.get_fixture_events(123456)
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_get_fixture_events_error_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            events = await adapter.get_fixture_events(123456)
        assert events == []

    @pytest.mark.asyncio
    async def test_get_fixture_lineups(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"team": {"name": "Liverpool"}, "formation": "4-3-3", "startXI": []},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            lineups = await adapter.get_fixture_lineups(123456)
        assert len(lineups) == 1

    @pytest.mark.asyncio
    async def test_get_fixture_lineups_error_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            lineups = await adapter.get_fixture_lineups(123456)
        assert lineups == []

    @pytest.mark.asyncio
    async def test_get_fixture_player_stats(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"team": {"name": "Liverpool"}, "players": [{"player": {"name": "Salah"}}]},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            stats = await adapter.get_fixture_player_stats(123456)
        assert len(stats) == 1

    @pytest.mark.asyncio
    async def test_get_fixture_player_stats_error_returns_empty(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            stats = await adapter.get_fixture_player_stats(123456)
        assert stats == []

    def test_headers_raises_without_key(self) -> None:
        adapter = ApiFootballAdapter()
        with pytest.raises(ValueError, match="API Football adapter requires an API key"):
            adapter._headers()

    def test_headers_returns_key(self) -> None:
        adapter = ApiFootballAdapter(api_key="my-key")
        h = adapter._headers()
        assert h["x-apisports-key"] == "my-key"


# ===========================================================================
# Base adapter tests (error classification + emit)
# ===========================================================================


class _ConcreteAdapter(BaseSportsReferenceAdapter):
    """Concrete implementation for testing base class methods."""

    @property
    def venue(self) -> str:
        return "test_venue"

    async def get_fixtures(self, date: str, league_ids: list[int] | None = None) -> list[object]:
        return []

    async def get_leagues(self) -> list[object]:
        return []

    async def get_teams(self, league_id: int, season: int | None = None) -> list[object]:
        return []

    async def get_odds(self, sport: str, regions: str = "uk", markets: str = "h2h") -> list[object]:
        return []


class TestBaseSportsAdapterErrorClassification:
    """Tests for base adapter error classification and event emission."""

    def test_classify_error_401(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("401 Unauthorized"), status=401) == "INVALID_API_KEY"

    def test_classify_error_authentication_message(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("authentication failed")) == "INVALID_API_KEY"

    def test_classify_error_429(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("429 Too Many Requests"), status=429) == "RATE_LIMIT_EXCEEDED"

    def test_classify_error_rate_message(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("rate limit exceeded")) == "RATE_LIMIT_EXCEEDED"

    def test_classify_error_timeout(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("connection timeout")) == "TIMEOUT_ERROR"

    def test_classify_error_403(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("403 Forbidden"), status=403) == "FORBIDDEN"

    def test_classify_error_forbidden_message(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("forbidden access")) == "FORBIDDEN"

    def test_classify_error_unknown(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("some other error")) == "UNKNOWN"

    def test_emit_fetch_failed(self) -> None:
        adapter = _ConcreteAdapter()
        # Should not raise — just log and emit
        adapter._emit_fetch_failed("RATE_LIMIT_EXCEEDED", Exception("rate limited"))

    @pytest.mark.asyncio
    async def test_base_standings_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_standings(39)
        assert result == []

    @pytest.mark.asyncio
    async def test_base_injuries_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_injuries("2026-03-22")
        assert result == []

    @pytest.mark.asyncio
    async def test_base_fixture_statistics_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_statistics(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_base_fixture_events_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_events(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_base_fixture_lineups_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_lineups(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_base_fixture_player_stats_returns_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_player_stats(123)
        assert result == []
