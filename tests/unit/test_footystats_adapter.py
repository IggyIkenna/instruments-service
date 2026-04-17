"""FootyStats adapter tests — factual fixtures and predictive data split.

Covers: get_fixtures, get_fixture_predictions, _parse_match predictive fields,
get_leagues, get_teams, get_odds stub.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.footystats import (
    FootystatsAdapter,
    _extract_data,
    _parse_match,
    _safe_float,
    _safe_int,
)

# ---------------------------------------------------------------------------
# Shared aiohttp mock helper (same pattern as test_sports_http_adapters.py)
# ---------------------------------------------------------------------------


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


# Sample raw API response with both factual and predictive fields
_SAMPLE_MATCH_RAW: dict[str, object] = {
    "id": 12345,
    "date_unix": 1711900800,
    "competition_id": 1625,
    "season": "2024/2025",
    "homeID": 100,
    "awayID": 200,
    "home_name": "Arsenal",
    "away_name": "Chelsea",
    "homeGoalCount": 2,
    "awayGoalCount": 1,
    "totalGoalCount": 3,
    "team_a_xg": 1.8,
    "team_b_xg": 0.9,
    "status": "complete",
    "game_week": 28,
    "team_a_possession": 62,
    "team_b_possession": 38,
    "team_a_shots": 15,
    "team_b_shots": 8,
    "team_a_corners": 7,
    "team_b_corners": 3,
    # Predictive fields — core
    "btts_potential": 65,
    "btts_fhg_potential": 30,
    "btts_2hg_potential": 45,
    "o05_potential": 95,
    "o15_potential": 80,
    "o25_potential": 72,
    "ov35_potential": 40,
    "o45_potential": 18,
    "u05_potential": 5,
    "u15_potential": 20,
    "u25_potential": 28,
    "u35_potential": 60,
    "u45_potential": 82,
    "team_a_xg_prematch": 1.6,
    "team_b_xg_prematch": 0.8,
    "total_xg_prematch": 2.4,
    # Predictive fields — sparse + PPG
    "corners_potential": 55,
    "corners_o85_potential": 60,
    "corners_o95_potential": 45,
    "corners_o105_potential": 30,
    "cards_potential": 48,
    "offsides_potential": 35,
    "avg_potential": 52,
    "pre_match_home_ppg": 2.1,
    "pre_match_away_ppg": 1.5,
    "pre_match_team_a_overall_ppg": 1.9,
    "pre_match_team_b_overall_ppg": 1.4,
    # Odds fields (subset — real API has 68)
    "odds_ft_1": 2.10,
    "odds_ft_x": 3.40,
    "odds_ft_2": 3.50,
    "odds_ft_over25": 1.85,
    "odds_ft_under25": 1.95,
    "odds_btts_yes": 1.75,
    "odds_btts_no": 2.05,
    "odds_corners_over_95": 2.10,
}


class TestParseMatch:
    """Test _parse_match extracts both factual and predictive fields."""

    def test_parse_match_factual_fields(self) -> None:
        match = _parse_match(_SAMPLE_MATCH_RAW)
        assert match is not None
        assert match.match_id == 12345
        assert match.home_name == "Arsenal"
        assert match.away_name == "Chelsea"
        assert match.home_goals == 2
        assert match.away_goals == 1
        assert match.home_xg == 1.8
        assert match.home_possession == 62
        assert match.home_shots == 15
        assert match.home_corners == 7

    def test_parse_match_predictive_fields(self) -> None:
        match = _parse_match(_SAMPLE_MATCH_RAW)
        assert match is not None
        # Core potentials (fields that exist on FootyStatsMatch model)
        assert match.btts_potential == 65
        assert match.o25_potential == 72
        assert match.o35_potential == 40
        assert match.o45_potential == 18
        assert match.xg_prematch_home == 1.6
        assert match.xg_prematch_away == 0.8
        # Sparse
        assert match.corners_potential == 55
        assert match.cards_potential == 48
        assert match.avg_potential == 52

    def test_parse_match_no_id(self) -> None:
        result = _parse_match({"status": "complete"})
        assert result is None

    def test_parse_match_predictive_none_when_missing(self) -> None:
        minimal = {"id": 99, "date_unix": 0, "competition_id": 1, "season": "2025", "home_name": "A", "away_name": "B"}
        match = _parse_match(minimal)
        assert match is not None
        assert match.btts_potential is None
        assert match.o25_potential is None
        assert match.xg_prematch_home is None


class TestExtractData:
    """Test _extract_data response parsing."""

    def test_dict_with_data_list(self) -> None:
        result = _extract_data({"data": [{"id": 1}, {"id": 2}]})
        assert len(result) == 2

    def test_raw_list(self) -> None:
        result = _extract_data([{"id": 1}])
        assert len(result) == 1

    def test_empty_response(self) -> None:
        assert _extract_data({}) == []
        assert _extract_data(None) == []


class TestSafeConversions:
    """Test _safe_int and _safe_float."""

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


class TestFootystatsAdapterGetFixtures:
    """Test get_fixtures returns factual CanonicalFixture objects."""

    @pytest.mark.asyncio
    async def test_get_fixtures_success(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [_SAMPLE_MATCH_RAW]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-04-01")

        assert len(fixtures) == 1
        fx = fixtures[0]
        assert fx.home_goals == 2
        assert fx.away_goals == 1

    @pytest.mark.asyncio
    async def test_get_fixtures_empty(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": []})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            fixtures = await adapter.get_fixtures("2026-04-01")

        assert fixtures == []


class TestFootystatsAdapterGetPredictions:
    """Test get_fixture_predictions returns predictive data dicts."""

    @pytest.mark.asyncio
    async def test_get_predictions_success(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [_SAMPLE_MATCH_RAW]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            preds = await adapter.get_fixture_predictions("2026-04-01")

        assert len(preds) == 1
        pred = preds[0]
        assert pred.btts_potential == 65
        assert pred.o25_potential == 72
        assert pred.xg_prematch_home == 1.6
        assert pred.xg_prematch_away == 0.8
        assert pred.corners_potential == 55
        assert pred.cards_potential == 48
        assert pred.avg_potential == 52
        assert pred.fixture_id
        assert pred.source == "footystats"

    @pytest.mark.asyncio
    async def test_get_predictions_skips_no_data(self) -> None:
        """Matches with no predictive fields are excluded."""
        raw_no_preds = {
            "id": 99,
            "date_unix": 1711900800,
            "competition_id": 1,
            "season": "2025",
            "homeID": 1,
            "awayID": 2,
            "home_name": "TeamA",
            "away_name": "TeamB",
        }
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [raw_no_preds]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            preds = await adapter.get_fixture_predictions("2026-04-01")

        assert preds == []

    @pytest.mark.asyncio
    async def test_get_predictions_empty_response(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": []})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            preds = await adapter.get_fixture_predictions("2026-04-01")

        assert preds == []

    @pytest.mark.asyncio
    async def test_get_predictions_league_filter(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [_SAMPLE_MATCH_RAW]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            preds = await adapter.get_fixture_predictions("2026-04-01", league_ids=[9999])

        assert preds == []


class TestFootystatsAdapterOddsSnapshot:
    """Test get_fixture_odds_snapshot method."""

    @pytest.mark.asyncio
    async def test_get_odds_snapshot_success(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [_SAMPLE_MATCH_RAW]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            odds = await adapter.get_fixture_odds_snapshot("2026-04-01")

        assert len(odds) == 1
        row = odds[0]
        assert row["odds_ft_1"] == 2.10
        assert row["odds_ft_x"] == 3.40
        assert row["odds_ft_2"] == 3.50
        assert row["odds_btts_yes"] == 1.75
        assert row["odds_ft_over25"] == 1.85
        assert row["fixture_id"]
        assert row["source"] == "footystats"
        assert row["home_team"] == "Arsenal"
        assert row["away_team"] == "Chelsea"

    @pytest.mark.asyncio
    async def test_get_odds_snapshot_empty_when_no_odds(self) -> None:
        raw_no_odds = {
            "id": 99,
            "date_unix": 1711900800,
            "competition_id": 1,
            "season": "2025",
            "homeID": 1,
            "awayID": 2,
            "home_name": "TeamA",
            "away_name": "TeamB",
        }
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [raw_no_odds]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            odds = await adapter.get_fixture_odds_snapshot("2026-04-01")

        assert odds == []

    @pytest.mark.asyncio
    async def test_get_odds_snapshot_league_filter(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({"data": [_SAMPLE_MATCH_RAW]})

        with patch("aiohttp.ClientSession", return_value=mock_session):
            odds = await adapter.get_fixture_odds_snapshot("2026-04-01", league_ids=[9999])

        assert odds == []


class TestFootystatsAdapterOther:
    """Test other adapter methods."""

    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        result = await adapter.get_odds("soccer_epl")
        assert result == []

    def test_venue_property(self) -> None:
        adapter = FootystatsAdapter(api_key="test-key")
        assert adapter.venue == "footystats"

    def test_no_api_key_raises(self) -> None:
        adapter = FootystatsAdapter(api_key=None)
        with pytest.raises(ValueError, match="API key"):
            adapter._params_with_key()

    @pytest.mark.asyncio
    async def test_get_leagues_success(self) -> None:
        raw_leagues = {"data": [{"id": 1, "name": "Premier League", "country": "England"}]}
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw_leagues)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()

        assert len(leagues) == 1
        assert leagues[0].name == "Premier League"

    @pytest.mark.asyncio
    async def test_get_teams_success(self) -> None:
        raw_teams = {"data": [{"id": 10, "clean_name": "Arsenal", "country": "England"}]}
        adapter = FootystatsAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(raw_teams)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(1625)

        assert len(teams) == 1
        assert teams[0].name == "Arsenal"
