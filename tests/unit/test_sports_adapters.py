"""Extended tests for sports adapters — constructors, error classification, and basic API patterns."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
)

# ---------------------------------------------------------------------------
# API Football adapter
# ---------------------------------------------------------------------------


class TestApiFootballAdapter:
    def test_init_with_api_key(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        assert adapter.venue == "api_football"

    def test_init_without_key(self) -> None:
        adapter = ApiFootballAdapter()
        assert adapter.venue == "api_football"

    @pytest.mark.asyncio
    async def test_get_leagues_mocked(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(
            return_value={
                "response": [
                    {
                        "league": {
                            "id": 39,
                            "name": "Premier League",
                            "type": "League",
                            "logo": "https://example.com/logo.png",
                        },
                        "country": {"name": "England", "code": "GB"},
                        "seasons": [{"year": 2025, "start": "2025-08-16", "end": "2026-05-25", "current": True}],
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            leagues = await adapter.get_leagues()
        assert len(leagues) >= 1

    @pytest.mark.asyncio
    async def test_get_teams_mocked(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(
            return_value={
                "response": [
                    {
                        "team": {"id": 40, "name": "Liverpool", "code": "LIV", "logo": ""},
                        "venue": {"name": "Anfield", "city": "Liverpool", "capacity": 54074},
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            teams = await adapter.get_teams(39)
        assert len(teams) >= 1

    @pytest.mark.asyncio
    async def test_get_fixtures_mocked(self) -> None:
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.headers = {}
        mock_resp.json = AsyncMock(
            return_value={
                "response": [
                    {
                        "fixture": {
                            "id": 123456,
                            "date": "2026-03-22T15:00:00+00:00",
                            "status": {"long": "Not Started", "short": "NS"},
                            "venue": {"name": "Anfield", "city": "Liverpool"},
                        },
                        "league": {"id": 39, "name": "Premier League", "season": 2025, "round": "Regular Season - 30"},
                        "teams": {
                            "home": {"id": 40, "name": "Liverpool"},
                            "away": {"id": 33, "name": "Manchester United"},
                        },
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            fixtures = await adapter.get_fixtures("2026-03-22")
        assert len(fixtures) >= 1
