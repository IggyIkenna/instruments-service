"""Coverage boost for open_meteo.py uncovered branches.

Targets:
- get_weather_match_window: lines 180, 201-202, 213, 227, 243-254 (api_key + exception paths)
- _parse_weather_response: lines 335-337, 346 (exception + None temp)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_adapter(api_key: str | None = None) -> object:
    from instruments_service.reference_data.adapters.sports.adapters.open_meteo import OpenMeteoAdapter

    return OpenMeteoAdapter(api_key=api_key)


# ---------------------------------------------------------------------------
# get_weather_match_window — api_key + exception paths
# ---------------------------------------------------------------------------


class TestGetWeatherMatchWindow:
    """Lines 180, 201-202, 213, 227, 243-254: api_key paths and exception handlers."""

    @pytest.mark.asyncio
    async def test_with_api_key_sets_params(self) -> None:
        """Lines 180, 213, 227: api_key → customer URLs + apikey in both requests."""
        adapter = _make_adapter(api_key="my-key")

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            result = await adapter.get_weather_match_window(  # type: ignore[attr-defined]
                venue_lat=51.5, venue_lon=-0.1, date="2023-01-01"
            )

        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_previous_runs_api_exception_does_not_abort_fetch(self) -> None:
        """Previous Runs API raises → warning logged, code continues to actual-weather fetch.

        The prior test asserted call_count==1 (exception propagated, actual weather never
        attempted). That was the bug: the inner except had a spurious `raise` that aborted the
        whole fetch for 16,241 shards (2024-01-03→2026-08-07). Fix: remove the re-raise so the
        code falls through to the archive/forecast endpoint, as the comment at line 136 always
        described. When actual weather ALSO fails (as in this test's mock), the failure propagates
        via the outer except — but only after attempting actual weather (call_count==2).
        """
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        call_count = 0

        async def retry_side_effect(session: object, url: str, **kwargs: object) -> object:
            nonlocal call_count
            call_count += 1
            # Both Previous Runs API and actual weather fail in this test
            raise ConnectionError("previous runs down")

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(adapter, "_get_with_retry", side_effect=retry_side_effect),
            patch.object(adapter, "_classify_error", return_value="NETWORK_ERROR"),
            patch.object(adapter, "_emit_fetch_failed") as mock_emit,
            pytest.raises(ConnectionError, match="previous runs down"),
        ):
            await adapter.get_weather_match_window(  # type: ignore[attr-defined]
                venue_lat=51.5, venue_lon=-0.1, date="2026-01-01"
            )

        # call_count==2: Previous Runs API tried first (raises, no abort), then actual weather tried
        assert call_count == 2
        mock_emit.assert_called_once()

    @pytest.mark.asyncio
    async def test_actual_weather_exception_propagates(self) -> None:
        """Lines 243-276: actual weather fetch raises → warning + re-raise (attempted_failed, not a
        silent partial result — see adapter-dead-code-and-fallback-ban.md rule 2)."""
        adapter = _make_adapter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        call_count = 0

        async def retry_side_effect(session: object, url: str, **kwargs: object) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Previous Runs API returns empty
                return {}
            # Actual weather fails
            raise ConnectionError("actual weather down")

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(adapter, "_get_with_retry", side_effect=retry_side_effect),
            patch.object(adapter, "_classify_error", return_value="NETWORK_ERROR"),
            patch.object(adapter, "_emit_fetch_failed") as mock_emit,
            pytest.raises(ConnectionError, match="actual weather down"),
        ):
            await adapter.get_weather_match_window(  # type: ignore[attr-defined]
                venue_lat=51.5, venue_lon=-0.1, date="2026-01-01"
            )

        mock_emit.assert_called_once()

    @pytest.mark.asyncio
    async def test_outer_exception_calls_emit_and_reraises(self) -> None:
        """Lines 287-290: outer exception → _classify_error + _emit_fetch_failed + re-raise
        (matches api_football.py's raise-after-_emit_fetch_failed pattern)."""
        adapter = _make_adapter()

        # Make _make_session raise directly so we hit the outer try/except
        with (
            patch.object(
                adapter,
                "_make_session",
                side_effect=RuntimeError("session failed"),
            ),
            patch.object(adapter, "_classify_error", return_value="SESSION_ERROR") as mock_classify,
            patch.object(adapter, "_emit_fetch_failed") as mock_emit,
            pytest.raises(RuntimeError, match="session failed"),
        ):
            await adapter.get_weather_match_window(  # type: ignore[attr-defined]
                venue_lat=51.5, venue_lon=-0.1, date="2026-01-01"
            )

        mock_classify.assert_called_once()
        mock_emit.assert_called_once_with("SESSION_ERROR", mock_classify.call_args[0][0])


# ---------------------------------------------------------------------------
# _parse_weather_response — exception + None temp
# ---------------------------------------------------------------------------


class TestParseWeatherResponse:
    """Lines 335-337, 346: validation exception + None temp."""

    def test_invalid_response_returns_none(self) -> None:
        """Lines 335-337: model_validate raises → warning + return None."""
        from instruments_service.reference_data.adapters.sports.adapters.open_meteo import _parse_weather_response

        with patch(
            "instruments_service.reference_data.adapters.sports.adapters.open_meteo.OpenMeteoResponse.model_validate",
            side_effect=ValueError("invalid"),
        ):
            result = _parse_weather_response({"bad": "data"}, 51.5, -0.1, "2026-01-01")

        assert result is None

    def test_non_dict_returns_none(self) -> None:
        """Line 331-332: not a dict → return None."""
        from instruments_service.reference_data.adapters.sports.adapters.open_meteo import _parse_weather_response

        result = _parse_weather_response("not a dict", 51.5, -0.1, "2026-01-01")
        assert result is None

    def test_none_temperature_returns_none(self) -> None:
        """Line 346: temp is None → return None."""
        from instruments_service.reference_data.adapters.sports.adapters.open_meteo import _parse_weather_response

        # Build a response where temp_2m[idx] is None
        raw = {
            "latitude": 51.5,
            "longitude": -0.1,
            "timezone": "UTC",
            "hourly": {
                "time": ["2026-01-01T12:00"],
                "temperature_2m": [None],
                "precipitation": [0.0],
                "wind_speed_10m": [5.0],
                "relative_humidity_2m": [70],
                "cloud_cover": [30],
                "weather_code": [0],
            },
        }
        result = _parse_weather_response(raw, 51.5, -0.1, "2026-01-01")
        assert result is None
