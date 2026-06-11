"""Coverage boost for soccerfootball_info.py uncovered exception/continue branches.

Targets:
- get_leagues: lines 74-77 (exception in HTTP), 83 (non-dict continue), 97-99 (parse error)
- get_standings: lines 129-132 (exception), 139-141 (parse error continue)
- get_match_descriptors_for_date: lines 244-247 (exception)
- get_progressive_stats: lines 297-300 (exception), 307-313 (parse error continue)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_adapter() -> object:
    from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
        SoccerFootballInfoAdapter,
    )

    return SoccerFootballInfoAdapter(api_key="test-key")


# ---------------------------------------------------------------------------
# get_leagues
# ---------------------------------------------------------------------------


class TestGetLeagues:
    """Lines 74-77, 83, 97-99."""

    @pytest.mark.asyncio
    async def test_http_exception_propagates(self) -> None:
        """Lines 74-77: exception in _get_with_retry → classify + emit + re-raise."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=ConnectionError("network error"),
            ),
            patch.object(adapter, "_classify_error", return_value="NETWORK_ERROR"),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_leagues()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_non_dict_item_skipped(self) -> None:
        """Line 83: non-dict item in league list → continue (skip)."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        raw_response = {"data": ["not_a_dict", {"id": "123", "name": "La Liga", "country": "Spain"}]}

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value=raw_response,
            ),
        ):
            leagues = await adapter.get_leagues()  # type: ignore[attr-defined]

        # Only the dict item should produce a league
        assert isinstance(leagues, list)
        assert len(leagues) == 1
        assert leagues[0].name == "La Liga"

    @pytest.mark.asyncio
    async def test_parse_error_in_league_continues(self) -> None:
        """Lines 97-99: CanonicalLeague construction fails → warning + continue."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        raw_response = {
            "data": [
                {"id": "123", "name": "Valid League", "country": "Spain"},
                {"id": None, "name": None, "country": None},  # might fail
            ]
        }

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value=raw_response,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info.CanonicalLeague",
                side_effect=[MagicMock(), Exception("parse error")],
            ),
        ):
            leagues = await adapter.get_leagues()  # type: ignore[attr-defined]

        # First item succeeded, second failed but was swallowed
        assert len(leagues) == 1


# ---------------------------------------------------------------------------
# get_standings
# ---------------------------------------------------------------------------


class TestGetStandings:
    """Lines 129-132, 139-141."""

    @pytest.mark.asyncio
    async def test_http_exception_propagates(self) -> None:
        """Lines 129-132: exception in _get_with_retry → classify + emit + re-raise."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=RuntimeError("timeout"),
            ),
            patch.object(adapter, "_classify_error", return_value="TIMEOUT"),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError),
        ):
            await adapter.get_standings("league-123")  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_standing_parse_error_continues(self) -> None:
        """Lines 139-141: _normalize_sfi_standing raises → warning + continue."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        raw_response = {"data": [{"team": "Arsenal", "rank": 1}, {"team": "Chelsea", "rank": 2}]}

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value=raw_response,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info._normalize_sfi_standing",
                side_effect=[MagicMock(), Exception("bad standing")],
            ),
        ):
            results = await adapter.get_standings("league-123")  # type: ignore[attr-defined]

        # First row succeeded, second failed
        assert len(results) == 1


# ---------------------------------------------------------------------------
# get_match_descriptors_for_date
# ---------------------------------------------------------------------------


class TestGetMatchDescriptors:
    """Lines 244-247."""

    @pytest.mark.asyncio
    async def test_http_exception_propagates(self) -> None:
        """Lines 244-247: exception during HTTP call → classify + emit + re-raise."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=ConnectionError("503"),
            ),
            patch.object(adapter, "_classify_error", return_value="SERVER_ERROR"),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_match_descriptors_for_date("2026-03-20")  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# get_progressive_stats
# ---------------------------------------------------------------------------


class TestGetProgressiveStats:
    """Lines 297-300, 307-313."""

    @pytest.mark.asyncio
    async def test_http_exception_propagates(self) -> None:
        """Lines 297-300: exception in HTTP call → classify + emit + re-raise."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=ConnectionError("network error"),
            ),
            patch.object(adapter, "_classify_error", return_value="NETWORK_ERROR"),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_progressive_stats("match-123")  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_stat_parse_error_continues(self) -> None:
        """Lines 307-313: _normalize_sfi_progressive_stat raises → warning + continue."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        raw_response = {"data": [{"team": "home", "minute": 30}, {"team": "away", "minute": 30}]}

        mock_stat = MagicMock()

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value=raw_response,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info._normalize_sfi_progressive_stat",
                side_effect=[mock_stat, Exception("bad stat")],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info.detect_halftime_window",
                side_effect=lambda x: x,
            ),
        ):
            results = await adapter.get_progressive_stats("match-123")  # type: ignore[attr-defined]

        # First item succeeded, second failed
        assert len(results) == 1
