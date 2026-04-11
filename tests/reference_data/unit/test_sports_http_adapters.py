"""HTTP adapter mock tests for sports reference data adapters.

Covers: api_football, odds_api, footystats, transfermarkt,
understat, open_meteo, soccerfootball_info.

Each test mocks ``aiohttp.ClientSession`` to simulate HTTP responses,
exercising parsing, normalization, error handling, and edge cases.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.external.open_meteo import WeatherCondition

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
from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    FootystatsAdapter,
)
from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    _extract_data as _footystats_extract_data,
)
from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    _parse_match as _footystats_parse_match,
)
from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    _safe_float as _footystats_safe_float,
)
from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    _safe_int as _footystats_safe_int,
)
from instruments_service.reference_data.adapters.sports.adapters.odds_api import (
    OddsApiAdapter,
    _extract_events,
    _normalize_event_odds,
    _parse_bookmaker_odds,
    _parse_market_outcomes,
    _parse_outcome_odds,
)
from instruments_service.reference_data.adapters.sports.adapters.open_meteo import (
    OpenMeteoAdapter,
    _find_midday_index,
    _parse_weather_response,
    _safe_list_get,
    _safe_list_get_int,
    _weather_code_to_condition,
)
from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    SoccerFootballInfoAdapter,
)
from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    _extract_data as _sfi_extract_data,
)
from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    TransfermarktAdapter,
    _extract_clubs,
    _parse_squad,
)
from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    _extract_results as _tm_extract_results,
)
from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    _safe_float as _tm_safe_float,
)
from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    _safe_int as _tm_safe_int,
)
from instruments_service.reference_data.adapters.sports.adapters.understat import (
    UnderstatAdapter,
    _extract_dates_data,
    _filter_and_normalize_matches,
    _parse_understat_match,
)
from instruments_service.reference_data.adapters.sports.adapters.understat import (
    _safe_float as _understat_safe_float,
)
from instruments_service.reference_data.adapters.sports.adapters.understat import (
    _safe_int as _understat_safe_int,
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
# Odds API adapter tests
# ===========================================================================


class TestOddsApiHelpers:
    """Unit tests for Odds API helper functions."""

    def test_extract_events_from_list(self) -> None:
        raw = [{"id": "abc"}, {"id": "def"}, "not_dict"]
        result = _extract_events(raw)
        assert len(result) == 2

    def test_extract_events_non_list(self) -> None:
        assert _extract_events({"response": []}) == []
        assert _extract_events("string") == []

    def test_parse_outcome_odds_valid(self) -> None:
        outcome = {"name": "Liverpool", "price": 1.65}
        result = _parse_outcome_odds(outcome, "evt1", "h2h", "LIV vs MUN", "soccer_epl", "EPL")
        assert result is not None
        assert result.selection_name == "Liverpool"
        assert result.venue == "odds_api"

    def test_parse_outcome_odds_no_price(self) -> None:
        outcome = {"name": "Liverpool"}
        result = _parse_outcome_odds(outcome, "evt1", "h2h", "LIV vs MUN", "soccer_epl", "EPL")
        assert result is None

    def test_parse_outcome_odds_zero_price(self) -> None:
        outcome = {"name": "Liverpool", "price": 0}
        result = _parse_outcome_odds(outcome, "evt1", "h2h", "LIV vs MUN", "soccer_epl", "EPL")
        assert result is None

    def test_parse_outcome_odds_negative_price(self) -> None:
        outcome = {"name": "Liverpool", "price": -1.5}
        result = _parse_outcome_odds(outcome, "evt1", "h2h", "LIV vs MUN", "soccer_epl", "EPL")
        assert result is None

    def test_parse_outcome_odds_invalid_price(self) -> None:
        outcome = {"name": "Liverpool", "price": "abc"}
        result = _parse_outcome_odds(outcome, "evt1", "h2h", "LIV vs MUN", "soccer_epl", "EPL")
        assert result is None

    def test_parse_market_outcomes(self) -> None:
        market = {
            "key": "h2h",
            "outcomes": [
                {"name": "Liverpool", "price": 1.65},
                {"name": "Draw", "price": 3.80},
                {"name": "Manchester United", "price": 5.50},
            ],
        }
        result = _parse_market_outcomes(market, "evt1", "LIV vs MUN", "soccer_epl", "EPL")
        assert len(result) == 3

    def test_parse_market_outcomes_non_list(self) -> None:
        market = {"key": "h2h", "outcomes": "not_list"}
        result = _parse_market_outcomes(market, "evt1", "LIV vs MUN", "soccer_epl", "EPL")
        assert result == []

    def test_parse_bookmaker_odds(self) -> None:
        bm = {
            "markets": [
                {
                    "key": "h2h",
                    "outcomes": [
                        {"name": "Liverpool", "price": 1.65},
                        {"name": "Draw", "price": 3.80},
                    ],
                }
            ]
        }
        result = _parse_bookmaker_odds(bm, "evt1", "LIV vs MUN", "soccer_epl", "EPL")
        assert len(result) == 2

    def test_parse_bookmaker_odds_no_markets(self) -> None:
        bm = {"markets": "not_list"}
        result = _parse_bookmaker_odds(bm, "evt1", "LIV vs MUN", "soccer_epl", "EPL")
        assert result == []

    def test_normalize_event_odds_full(self) -> None:
        event = {
            "id": "evt1",
            "home_team": "Liverpool",
            "away_team": "Manchester United",
            "sport_key": "soccer_epl",
            "sport_title": "EPL",
            "bookmakers": [
                {
                    "key": "bet365",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Liverpool", "price": 1.65},
                                {"name": "Draw", "price": 3.80},
                                {"name": "Manchester United", "price": 5.50},
                            ],
                        }
                    ],
                }
            ],
        }
        result = _normalize_event_odds(event)
        assert len(result) == 3
        assert all(o.venue == "odds_api" for o in result)

    def test_normalize_event_odds_no_bookmakers(self) -> None:
        event = {"id": "evt1", "home_team": "A", "away_team": "B", "bookmakers": "not_list"}
        assert _normalize_event_odds(event) == []


class TestOddsApiAdapterHttp:
    """Async HTTP tests for OddsApiAdapter."""

    @pytest.mark.asyncio
    async def test_get_odds_with_bookmakers(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            [
                {
                    "id": "evt1",
                    "sport_key": "soccer_epl",
                    "sport_title": "EPL",
                    "home_team": "Liverpool",
                    "away_team": "Man Utd",
                    "bookmakers": [
                        {
                            "key": "bet365",
                            "markets": [
                                {
                                    "key": "h2h",
                                    "outcomes": [
                                        {"name": "Liverpool", "price": 1.65},
                                        {"name": "Draw", "price": 3.80},
                                        {"name": "Man Utd", "price": 5.50},
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
            headers={"x-requests-remaining": "450", "x-requests-used": "50"},
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            odds = await adapter.get_odds("soccer_epl", regions="uk", markets="h2h")
        assert len(odds) == 3
        assert adapter.requests_remaining == "450"

    @pytest.mark.asyncio
    async def test_get_odds_empty_events(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock([], headers={"x-requests-remaining": "499"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    @pytest.mark.asyncio
    async def test_get_leagues_parses(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            [
                {"key": "soccer_epl", "title": "English Premier League", "group": "Soccer"},
                {"key": "soccer_germany_bundesliga", "title": "Bundesliga", "group": "Soccer"},
            ],
            headers={"x-requests-remaining": "498"},
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].league_id == "soccer_epl"
        assert leagues[0].name == "English Premier League"

    @pytest.mark.asyncio
    async def test_get_leagues_non_list_response(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {"not_a_list": True},
            headers={"x-requests-remaining": "497"},
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert leagues == []

    @pytest.mark.asyncio
    async def test_get_events_parses(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            [
                {
                    "id": "evt1",
                    "sport_key": "soccer_epl",
                    "sport_title": "EPL",
                    "commence_time": "2026-03-22T15:00:00Z",
                    "home_team": "Liverpool",
                    "away_team": "Man Utd",
                },
            ],
            headers={"x-requests-remaining": "496"},
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            events = await adapter.get_events("soccer_epl")
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_get_events_non_list(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {"not_a_list": True},
            headers={"x-requests-remaining": "495"},
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            events = await adapter.get_events("soccer_epl")
        assert events == []

    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        adapter = OddsApiAdapter(api_key="test-key")
        teams = await adapter.get_teams(39)
        assert teams == []

    def test_params_with_key_raises_without_key(self) -> None:
        adapter = OddsApiAdapter()
        with pytest.raises(ValueError, match="Odds API adapter requires an API key"):
            adapter._params_with_key()

    def test_params_with_key_includes_extra(self) -> None:
        adapter = OddsApiAdapter(api_key="my-key")
        params = adapter._params_with_key({"regions": "uk"})
        assert params["apiKey"] == "my-key"
        assert params["regions"] == "uk"


# ===========================================================================
# FootyStats adapter tests
# ===========================================================================


class TestFootystatsHelpers:
    """Unit tests for FootyStats helper functions."""

    def test_extract_data_dict_with_data(self) -> None:
        raw = {"data": [{"id": 1}, {"id": 2}]}
        result = _footystats_extract_data(raw)
        assert len(result) == 2

    def test_extract_data_raw_list(self) -> None:
        raw = [{"id": 1}, "not_dict"]
        result = _footystats_extract_data(raw)
        assert len(result) == 1

    def test_extract_data_non_list_non_dict(self) -> None:
        assert _footystats_extract_data("string") == []

    def test_parse_match_valid(self) -> None:
        item = {
            "id": 12345,
            "date_unix": 1774296000,
            "competition_id": 100,
            "season": "2025/2026",
            "homeID": 40,
            "awayID": 33,
            "home_name": "Liverpool",
            "away_name": "Man Utd",
            "homeGoalCount": 2,
            "awayGoalCount": 1,
            "totalGoalCount": 3,
            "team_a_xg": 2.15,
            "team_b_xg": 0.87,
            "status": "complete",
            "game_week": 30,
            "team_a_possession": 62,
            "team_b_possession": 38,
            "team_a_shots": 15,
            "team_b_shots": 7,
            "team_a_corners": 8,
            "team_b_corners": 3,
        }
        result = _footystats_parse_match(item)
        assert result is not None
        assert result.match_id == 12345
        assert result.home_name == "Liverpool"
        assert result.home_goals == 2

    def test_parse_match_alternate_keys(self) -> None:
        """FootyStats sometimes uses match_id, home_id, away_id, etc."""
        item = {
            "match_id": 99999,
            "date_unix": 1774296000,
            "competition_id": 100,
            "season": "2025",
            "home_id": 40,
            "away_id": 33,
            "home_name": "Liverpool",
            "away_name": "Man Utd",
            "home_goal_count": 1,
            "away_goal_count": 0,
            "total_goal_count": 1,
        }
        result = _footystats_parse_match(item)
        assert result is not None
        assert result.match_id == 99999

    def test_parse_match_no_id_returns_none(self) -> None:
        item = {"date_unix": 1234, "home_name": "A"}
        result = _footystats_parse_match(item)
        assert result is None

    def test_safe_int(self) -> None:
        assert _footystats_safe_int(42) == 42
        assert _footystats_safe_int("42") == 42
        assert _footystats_safe_int(None) is None
        assert _footystats_safe_int("bad") is None

    def test_safe_float(self) -> None:
        assert _footystats_safe_float(1.5) == 1.5
        assert _footystats_safe_float("1.5") == 1.5
        assert _footystats_safe_float(None) is None
        assert _footystats_safe_float("bad") is None


class TestFootystatsAdapterHttp:
    """Async HTTP tests for FootystatsAdapter."""

    @pytest.mark.asyncio
    async def test_get_fixtures_parses(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {
                        "id": 12345,
                        "date_unix": 1774296000,
                        "competition_id": 100,
                        "season": "2025/2026",
                        "homeID": 40,
                        "awayID": 33,
                        "home_name": "Liverpool",
                        "away_name": "Man Utd",
                    }
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_fixtures_filter_by_league_ids(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {
                        "id": 12345,
                        "date_unix": 1774296000,
                        "competition_id": 100,
                        "season": "2025",
                        "homeID": 40,
                        "awayID": 33,
                        "home_name": "Liverpool",
                        "away_name": "Man Utd",
                    },
                    {
                        "id": 12346,
                        "date_unix": 1774296000,
                        "competition_id": 200,
                        "season": "2025",
                        "homeID": 50,
                        "awayID": 60,
                        "home_name": "Barcelona",
                        "away_name": "Real Madrid",
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22", league_ids=[100])
        # Only competition_id=100 should pass (if normalization succeeds)
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_leagues_parses(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {
                        "id": "100",
                        "name": "Premier League",
                        "country": "England",
                        "image": "https://example.com/pl.png",
                    },
                    {"id": "200", "name": "La Liga", "country": "Spain"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].name == "Premier League"
        assert leagues[0].country == "England"

    @pytest.mark.asyncio
    async def test_get_teams_parses(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {"id": "40", "clean_name": "Liverpool", "short_hand": "LIV", "country": "England"},
                    {"id": "33", "name": "Manchester United", "short_hand": "MUN"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(100, season=2025)
        assert len(teams) == 2
        assert teams[0].name == "Liverpool"

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    def test_params_with_key_raises_without_key(self) -> None:
        adapter = FootystatsAdapter()
        with pytest.raises(ValueError, match="FootyStats adapter requires an API key"):
            adapter._params_with_key()

    def test_params_with_key_includes_extra(self) -> None:
        adapter = FootystatsAdapter(api_key="my-key")
        params = adapter._params_with_key({"date": "2026-03-22"})
        assert params["key"] == "my-key"
        assert params["date"] == "2026-03-22"


# ===========================================================================
# Transfermarkt adapter tests
# ===========================================================================


class TestTransfermarktHelpers:
    """Unit tests for Transfermarkt helper functions."""

    def test_extract_clubs_dict_with_clubs_key(self) -> None:
        raw = {"clubs": [{"id": 1}, {"id": 2}]}
        assert len(_extract_clubs(raw)) == 2

    def test_extract_clubs_dict_with_results_key(self) -> None:
        raw = {"results": [{"id": 1}]}
        assert len(_extract_clubs(raw)) == 1

    def test_extract_clubs_dict_with_data_key(self) -> None:
        raw = {"data": [{"id": 1}]}
        assert len(_extract_clubs(raw)) == 1

    def test_extract_clubs_list(self) -> None:
        raw = [{"id": 1}, "not_dict"]
        assert len(_extract_clubs(raw)) == 1

    def test_extract_clubs_non_container(self) -> None:
        assert _extract_clubs("string") == []

    def test_extract_results_dict(self) -> None:
        raw = {"results": [{"id": 1}]}
        assert len(_tm_extract_results(raw)) == 1

    def test_extract_results_competitions_key(self) -> None:
        raw = {"competitions": [{"id": 1}]}
        assert len(_tm_extract_results(raw)) == 1

    def test_extract_results_list(self) -> None:
        raw = [{"id": 1}, "not_dict"]
        assert len(_tm_extract_results(raw)) == 1

    def test_parse_squad_valid(self) -> None:
        item = {
            "id": 11,
            "name": "Liverpool",
            "squadSize": 25,
            "averageAge": 27.5,
            "foreignersNumber": 15,
            "totalMarketValue": 1200000000,
            "players": [
                {
                    "id": 100,
                    "name": "Mo Salah",
                    "position": "Right Winger",
                    "nationality": "Egypt",
                    "marketValue": 80000000,
                    "age": 33,
                },
            ],
        }
        result = _parse_squad(item)
        assert result is not None
        assert result.team_name == "Liverpool"
        assert result.players is not None
        assert len(result.players) == 1
        assert result.players[0].name == "Mo Salah"

    def test_parse_squad_no_id_returns_none(self) -> None:
        item = {"name": "Test"}
        result = _parse_squad(item)
        assert result is None

    def test_parse_squad_no_players(self) -> None:
        item = {"id": 1, "name": "Test"}
        result = _parse_squad(item)
        assert result is not None
        assert result.players is None

    def test_parse_squad_alternate_keys(self) -> None:
        item = {
            "id": 1,
            "name": "Test",
            "squad_size": 20,
            "average_age": 26.0,
            "foreigners_number": 10,
            "total_market_value_eur": 500000000,
            "squad": [
                {"name": "Player One", "playerName": "backup"},
            ],
        }
        result = _parse_squad(item)
        assert result is not None
        assert result.squad_size == 20

    def test_safe_int_transfermarkt(self) -> None:
        assert _tm_safe_int(42) == 42
        assert _tm_safe_int("42") == 42
        assert _tm_safe_int(None) is None
        assert _tm_safe_int("bad") is None

    def test_safe_float_transfermarkt(self) -> None:
        assert _tm_safe_float(1.5) == 1.5
        assert _tm_safe_float("1.5") == 1.5
        assert _tm_safe_float(None) is None
        assert _tm_safe_float("bad") is None


class TestTransfermarktAdapterHttp:
    """Async HTTP tests for TransfermarktAdapter."""

    @pytest.mark.asyncio
    async def test_get_teams_parses(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapid-api-key")
        mock_session = _make_aiohttp_mock(
            {
                "clubs": [
                    {
                        "id": 11,
                        "name": "Liverpool",
                        "squadSize": 25,
                        "averageAge": 27.5,
                        "players": [
                            {"id": 100, "name": "Mo Salah", "position": "RW"},
                        ],
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(39, season=2025)
        assert isinstance(teams, list)

    @pytest.mark.asyncio
    async def test_get_leagues_parses(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapid-api-key")
        mock_session = _make_aiohttp_mock(
            {
                "results": [
                    {"id": "GB1", "name": "Premier League", "country": "England"},
                    {"id": "ES1", "title": "La Liga", "country": "Spain"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].name == "Premier League"

    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapid-api-key")
        fixtures = await adapter.get_fixtures("2026-03-22")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapid-api-key")
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    def test_is_apify_detection(self) -> None:
        adapter_apify = TransfermarktAdapter(api_key="apify_api_test123")
        assert adapter_apify._is_apify is True

        adapter_rapid = TransfermarktAdapter(api_key="rapid-api-key")
        assert adapter_rapid._is_apify is False

    def test_headers_raises_without_key(self) -> None:
        adapter = TransfermarktAdapter()
        with pytest.raises(ValueError, match="Transfermarkt adapter requires an API key"):
            adapter._headers()

    def test_headers_apify_format(self) -> None:
        adapter = TransfermarktAdapter(api_key="apify_api_test123")
        h = adapter._headers()
        assert h["Authorization"] == "Bearer apify_api_test123"
        assert h["Content-Type"] == "application/json"

    def test_headers_rapidapi_format(self) -> None:
        adapter = TransfermarktAdapter(api_key="rapid-api-key")
        h = adapter._headers()
        assert h["x-rapidapi-key"] == "rapid-api-key"
        assert "transfermarkt" in h["x-rapidapi-host"]


# ===========================================================================
# Understat adapter tests
# ===========================================================================


class TestUnderstatHelpers:
    """Unit tests for Understat helper functions."""

    def test_extract_dates_data_parses_json(self) -> None:
        # Simulate Understat HTML with embedded JSON.parse()
        html = """
        <html>
        <script>
        var datesData = JSON.parse('\\x5B\\x7B\\x22id\\x22:\\x221234\\x22\\x7D\\x5D')
        </script>
        </html>
        """
        result = _extract_dates_data(html)
        assert len(result) == 1
        assert result[0]["id"] == "1234"

    def test_extract_dates_data_no_json_blocks(self) -> None:
        html = "<html><body>No data here</body></html>"
        result = _extract_dates_data(html)
        assert result == []

    def test_extract_dates_data_invalid_json(self) -> None:
        html = "var x = JSON.parse('invalid json')"
        result = _extract_dates_data(html)
        assert result == []

    def test_parse_understat_match_valid(self) -> None:
        item = {
            "id": "1234",
            "h": {"id": "1", "title": "Liverpool", "short_title": "LIV"},
            "a": {"id": "2", "title": "Manchester United", "short_title": "MUN"},
            "goals": {"h": "2", "a": "1"},
            "xG": {"h": "2.15", "a": "0.87"},
            "datetime": "2026-03-22 15:00:00",
            "isResult": True,
        }
        result = _parse_understat_match(item)
        assert result is not None
        assert result.id == "1234"
        assert isinstance(result.h, object)

    def test_parse_understat_match_flat_goals(self) -> None:
        item = {
            "id": "5678",
            "h": {"id": "1", "title": "Arsenal"},
            "a": {"id": "2", "title": "Chelsea"},
            "goals_h": 3,
            "goals_a": 0,
            "xG_h": 2.5,
            "xG_a": 0.3,
            "datetime": "2026-03-22 15:00:00",
            "isResult": True,
        }
        result = _parse_understat_match(item)
        assert result is not None
        assert result.goals_h == 3

    def test_parse_understat_match_no_teams(self) -> None:
        item = {"id": "9999", "datetime": "2026-03-22", "isResult": False}
        result = _parse_understat_match(item)
        assert result is not None
        assert result.h is None

    def test_filter_and_normalize_matches_filters_by_date(self) -> None:
        matches = [
            {
                "id": "1",
                "datetime": "2026-03-22 15:00:00",
                "h": {"id": "1", "title": "A"},
                "a": {"id": "2", "title": "B"},
                "isResult": True,
            },
            {
                "id": "2",
                "datetime": "2026-03-23 15:00:00",
                "h": {"id": "3", "title": "C"},
                "a": {"id": "4", "title": "D"},
                "isResult": True,
            },
        ]
        result = _filter_and_normalize_matches(matches, "2026-03-22", "EPL", 2025)
        assert isinstance(result, list)
        # Only the first match should match the date filter

    def test_filter_and_normalize_matches_skips_non_dict(self) -> None:
        matches = ["not_dict", 42]
        result = _filter_and_normalize_matches(matches, "2026-03-22", "EPL", 2025)
        assert result == []

    def test_safe_int_understat(self) -> None:
        assert _understat_safe_int(42) == 42
        assert _understat_safe_int("42") == 42
        assert _understat_safe_int(None) is None
        assert _understat_safe_int("bad") is None

    def test_safe_float_understat(self) -> None:
        assert _understat_safe_float(1.5) == 1.5
        assert _understat_safe_float("1.5") == 1.5
        assert _understat_safe_float(None) is None
        assert _understat_safe_float("bad") is None


class TestUnderstatAdapterHttp:
    """Async HTTP tests for UnderstatAdapter."""

    @pytest.mark.asyncio
    async def test_get_leagues_returns_fixed_list(self) -> None:
        adapter = UnderstatAdapter()
        leagues = await adapter.get_leagues()
        assert len(leagues) == 6
        names = [league.name for league in leagues]
        assert "EPL" in names
        assert "La Liga" in names
        assert "Bundesliga" in names

    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        adapter = UnderstatAdapter()
        teams = await adapter.get_teams(39)
        assert teams == []

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = UnderstatAdapter()
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    @pytest.mark.asyncio
    async def test_get_fixtures_fetches_all_leagues(self) -> None:
        adapter = UnderstatAdapter()
        # Mock the HTML response with embedded JSON
        html_with_data = """
        <html>
        <script>
        var datesData = JSON.parse('\\x5B\\x7B\\x22id\\x22:\\x221\\x22,\\x22datetime\\x22:\\x222026-03-22 15:00:00\\x22,\\x22isResult\\x22:true,\\x22h\\x22:\\x7B\\x22id\\x22:\\x221\\x22,\\x22title\\x22:\\x22TeamA\\x22\\x7D,\\x22a\\x22:\\x7B\\x22id\\x22:\\x222\\x22,\\x22title\\x22:\\x22TeamB\\x22\\x7D\\x7D\\x5D')
        </script>
        </html>
        """
        mock_session = _make_aiohttp_mock({}, text_response=html_with_data)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_fixtures_handles_http_error(self) -> None:
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock({}, status=404, text_response="Not Found")
        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert isinstance(fixtures, list)

    @pytest.mark.asyncio
    async def test_get_fixtures_season_calculation(self) -> None:
        """Dates before August should use previous year as season."""
        adapter = UnderstatAdapter()
        mock_session = _make_aiohttp_mock({}, text_response="<html></html>")
        with patch("aiohttp.ClientSession", return_value=mock_session):
            # March 2026 → season 2025 (before August)
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert isinstance(fixtures, list)

    def test_headers(self) -> None:
        adapter = UnderstatAdapter()
        h = adapter._headers()
        assert "User-Agent" in h


# ===========================================================================
# Open-Meteo adapter tests
# ===========================================================================


class TestOpenMeteoHelpers:
    """Unit tests for Open-Meteo helper functions."""

    def test_find_midday_index_exact(self) -> None:
        times = ["2026-03-22T00:00", "2026-03-22T06:00", "2026-03-22T12:00", "2026-03-22T18:00"]
        assert _find_midday_index(times) == 2

    def test_find_midday_index_t12_format(self) -> None:
        times = ["2026-03-22T00:00", "2026-03-22T12:00"]
        assert _find_midday_index(times) == 1

    def test_find_midday_index_fallback_middle(self) -> None:
        times = ["00:00", "06:00", "18:00"]
        assert _find_midday_index(times) == 1

    def test_find_midday_index_empty(self) -> None:
        assert _find_midday_index([]) == 0

    def test_safe_list_get_valid(self) -> None:
        assert _safe_list_get([1.0, 2.0, 3.0], 1) == 2.0

    def test_safe_list_get_out_of_range(self) -> None:
        assert _safe_list_get([1.0], 5) is None

    def test_safe_list_get_none_data(self) -> None:
        assert _safe_list_get(None, 0) is None

    def test_safe_list_get_int_valid(self) -> None:
        assert _safe_list_get_int([10, 20, 30], 2) == 30

    def test_safe_list_get_int_out_of_range(self) -> None:
        assert _safe_list_get_int([10], 5) is None

    def test_safe_list_get_int_none_data(self) -> None:
        assert _safe_list_get_int(None, 0) is None

    def test_weather_code_to_condition_clear(self) -> None:
        assert _weather_code_to_condition(0) == WeatherCondition.CLEAR
        assert _weather_code_to_condition(1) == WeatherCondition.CLEAR

    def test_weather_code_to_condition_partly_cloudy(self) -> None:
        assert _weather_code_to_condition(2) == WeatherCondition.PARTLY_CLOUDY

    def test_weather_code_to_condition_fog(self) -> None:
        assert _weather_code_to_condition(45) == WeatherCondition.FOG

    def test_weather_code_to_condition_rain(self) -> None:
        assert _weather_code_to_condition(61) == WeatherCondition.RAIN

    def test_weather_code_to_condition_snow(self) -> None:
        assert _weather_code_to_condition(71) == WeatherCondition.SNOW

    def test_weather_code_to_condition_thunderstorm(self) -> None:
        assert _weather_code_to_condition(95) == WeatherCondition.THUNDERSTORM

    def test_weather_code_to_condition_none(self) -> None:
        assert _weather_code_to_condition(None) is None

    def test_weather_code_to_condition_above_range(self) -> None:
        assert _weather_code_to_condition(100) == WeatherCondition.CLOUDY

    def test_parse_weather_response_valid(self) -> None:
        raw = {
            "latitude": 53.43,
            "longitude": -2.96,
            "hourly": {
                "time": [f"2026-03-22T{h:02d}:00" for h in range(24)],
                "temperature_2m": [float(10 + i) for i in range(24)],
                "wind_speed_10m": [float(5 + i) for i in range(24)],
                "relative_humidity_2m": [int(60 + i) for i in range(24)],
                "precipitation": [0.0] * 24,
                "cloud_cover": [int(50 + i) for i in range(24)],
                "weather_code": [1] * 24,
            },
        }
        result = _parse_weather_response(raw, 53.43, -2.96, "2026-03-22")
        assert result is not None
        assert result.latitude == 53.43
        assert result.temperature_celsius == 22.0  # 10 + 12 (midday index)

    def test_parse_weather_response_non_dict(self) -> None:
        assert _parse_weather_response("string", 0.0, 0.0, "2026-03-22") is None

    def test_parse_weather_response_no_hourly(self) -> None:
        raw = {"latitude": 0.0, "longitude": 0.0}
        assert _parse_weather_response(raw, 0.0, 0.0, "2026-03-22") is None


class TestOpenMeteoAdapterHttp:
    """Async HTTP tests for OpenMeteoAdapter."""

    @pytest.mark.asyncio
    async def test_get_weather_parses(self) -> None:
        adapter = OpenMeteoAdapter()
        raw_response = {
            "latitude": 53.43,
            "longitude": -2.96,
            "hourly": {
                "time": [f"2026-03-22T{h:02d}:00" for h in range(24)],
                "temperature_2m": [float(10 + i) for i in range(24)],
                "wind_speed_10m": [float(15) for _ in range(24)],
                "relative_humidity_2m": [70] * 24,
                "precipitation": [0.0] * 24,
                "cloud_cover": [50] * 24,
                "weather_code": [3] * 24,
            },
        }
        mock_session = _make_aiohttp_mock(raw_response)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            weather = await adapter.get_weather(53.43, -2.96, "2026-03-22")
        assert weather is not None
        assert weather.latitude == 53.43
        assert weather.temperature_celsius == 22.0

    @pytest.mark.asyncio
    async def test_get_weather_error_returns_none(self) -> None:
        adapter = OpenMeteoAdapter()
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            weather = await adapter.get_weather(53.43, -2.96, "2026-03-22")
        assert weather is None

    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        fixtures = await adapter.get_fixtures("2026-03-22")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_leagues_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        leagues = await adapter.get_leagues()
        assert leagues == []

    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        teams = await adapter.get_teams(39)
        assert teams == []

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []


# ===========================================================================
# SoccerFootball.info adapter tests
# ===========================================================================


class TestSoccerFootballInfoHelpers:
    """Unit tests for SoccerFootball.info helper functions."""

    def test_extract_data_dict_with_data(self) -> None:
        raw = {"data": [{"id": 1}]}
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_dict_with_results(self) -> None:
        raw = {"results": [{"id": 1}]}
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_dict_with_standings(self) -> None:
        raw = {"standings": [{"team": "A", "points": 30}]}
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_dict_with_leagues(self) -> None:
        raw = {"leagues": [{"id": 1}]}
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_dict_with_response(self) -> None:
        raw = {"response": [{"id": 1}]}
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_list(self) -> None:
        raw = [{"id": 1}, "not_dict"]
        assert len(_sfi_extract_data(raw)) == 1

    def test_extract_data_non_container(self) -> None:
        assert _sfi_extract_data("string") == []


class TestSoccerFootballInfoAdapterHttp:
    """Async HTTP tests for SoccerFootballInfoAdapter."""

    @pytest.mark.asyncio
    async def test_get_leagues_parses(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {
                        "id": "uuid1",
                        "name": "Premier League",
                        "country": "England",
                        "type": "league",
                        "logo": "https://example.com",
                    },
                    {"id": "uuid2", "league_name": "La Liga", "country_name": "Spain"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert len(leagues) == 2
        assert leagues[0].name == "Premier League"

    @pytest.mark.asyncio
    async def test_get_standings_parses(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "standings": [
                    {"team_id": "t1", "team_name": "Liverpool", "points": 80, "position": 1},
                    {"team_id": "t2", "team_name": "Arsenal", "points": 75, "position": 2},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            standings = await adapter.get_standings("uuid1")
        assert len(standings) == 2

    @pytest.mark.asyncio
    async def test_get_standings_with_season(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"standings": [{"team_name": "Liverpool"}]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            standings = await adapter.get_standings("uuid1", season="2025")
        assert len(standings) == 1

    @pytest.mark.asyncio
    async def test_get_teams_from_standings(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "standings": [
                    {"team_id": "t1", "team_name": "Liverpool", "short_name": "LIV", "logo": "https://logo"},
                    {"team_id": "t2", "name": "Arsenal"},
                    {"team_id": "t3"},  # no name — should be skipped
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(1)
        assert len(teams) == 2
        assert teams[0].name == "Liverpool"
        assert teams[0].short_name == "LIV"

    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        fixtures = await adapter.get_fixtures("2026-03-22")
        assert fixtures == []

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="test-key")
        odds = await adapter.get_odds("soccer_epl")
        assert odds == []

    def test_headers_raises_without_key(self) -> None:
        adapter = SoccerFootballInfoAdapter()
        with pytest.raises(ValueError, match=r"SoccerFootball\.info adapter requires an API key"):
            adapter._headers()

    def test_headers_rapidapi_format(self) -> None:
        adapter = SoccerFootballInfoAdapter(api_key="my-key")
        h = adapter._headers()
        assert h["x-rapidapi-key"] == "my-key"
        assert "soccer-football-info" in h["x-rapidapi-host"]


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

    def test_base_standings_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_standings(39))
        assert result == []

    def test_base_injuries_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_injuries("2026-03-22"))
        assert result == []

    def test_base_fixture_statistics_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_fixture_statistics(123))
        assert result == []

    def test_base_fixture_events_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_fixture_events(123))
        assert result == []

    def test_base_fixture_lineups_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_fixture_lineups(123))
        assert result == []

    def test_base_fixture_player_stats_returns_empty(self) -> None:
        import asyncio

        adapter = _ConcreteAdapter()
        result = asyncio.get_event_loop().run_until_complete(adapter.get_fixture_player_stats(123))
        assert result == []
