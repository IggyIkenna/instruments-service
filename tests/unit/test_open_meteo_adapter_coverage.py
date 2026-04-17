"""Open-Meteo adapter tests — coverage for uncovered methods.

Tests: get_weather, get_weather_match_window, get_fixtures, get_leagues,
get_teams, get_odds, helper functions (_parse_weather_response,
_find_midday_index, _safe_list_get, _safe_list_get_int,
_weather_code_to_condition, _find_hour_index, _build_weather_from_hourly).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.sports.adapters.open_meteo import (
    OpenMeteoAdapter,
    _build_weather_from_hourly,
    _find_hour_index,
    _find_midday_index,
    _parse_weather_response,
    _safe_list_get,
    _safe_list_get_int,
    _weather_code_to_condition,
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


_SAMPLE_WEATHER_RESPONSE: dict[str, object] = {
    "hourly": {
        "time": [
            "2026-04-01T00:00",
            "2026-04-01T06:00",
            "2026-04-01T12:00",
            "2026-04-01T18:00",
        ],
        "temperature_2m": [8.0, 10.0, 15.0, 12.0],
        "precipitation": [0.0, 0.0, 0.5, 1.0],
        "wind_speed_10m": [10.0, 15.0, 20.0, 12.0],
        "relative_humidity_2m": [80, 75, 60, 70],
        "cloud_cover": [90, 70, 50, 80],
        "weather_code": [3, 2, 61, 80],
    }
}


class TestOpenMeteoAdapterProperties:
    def test_venue(self) -> None:
        adapter = OpenMeteoAdapter()
        assert adapter.venue == "open_meteo"


class TestOpenMeteoGetFixtures:
    @pytest.mark.asyncio
    async def test_get_fixtures_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        result = await adapter.get_fixtures("2026-04-01")
        assert result == []


class TestOpenMeteoGetLeagues:
    @pytest.mark.asyncio
    async def test_get_leagues_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        result = await adapter.get_leagues()
        assert result == []


class TestOpenMeteoGetTeams:
    @pytest.mark.asyncio
    async def test_get_teams_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        result = await adapter.get_teams("EPL")
        assert result == []


class TestOpenMeteoGetOdds:
    @pytest.mark.asyncio
    async def test_get_odds_returns_empty(self) -> None:
        adapter = OpenMeteoAdapter()
        result = await adapter.get_odds("soccer_epl")
        assert result == []


class TestOpenMeteoGetWeather:
    @pytest.mark.asyncio
    async def test_get_weather_success(self) -> None:
        adapter = OpenMeteoAdapter()
        mock_session = _make_aiohttp_mock(_SAMPLE_WEATHER_RESPONSE)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_weather(51.5, -0.1, "2026-04-01")
        assert result is not None
        assert result.temperature_celsius == 15.0
        assert result.latitude == 51.5
        assert result.longitude == -0.1

    @pytest.mark.asyncio
    async def test_get_weather_error_returns_none(self) -> None:
        adapter = OpenMeteoAdapter()
        # Mock _get_with_retry to raise (simulates network failure)
        with patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("Network error")):
            result = await adapter.get_weather(51.5, -0.1, "2026-04-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_weather_invalid_response(self) -> None:
        adapter = OpenMeteoAdapter()
        mock_session = _make_aiohttp_mock({"error": True})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_weather(51.5, -0.1, "2026-04-01")
        assert result is None


class TestOpenMeteoGetWeatherMatchWindow:
    @pytest.mark.asyncio
    async def test_get_weather_match_window_success(self) -> None:
        hourly_data = {
            "time": [f"2026-04-01T{h:02d}:00" for h in range(24)],
            "temperature_2m": [10.0 + i * 0.5 for i in range(24)],
            "precipitation": [0.0] * 24,
            "wind_speed_10m": [15.0] * 24,
            "relative_humidity_2m": [70] * 24,
            "cloud_cover": [50] * 24,
            "weather_code": [3] * 24,
        }
        # Add _previous_day1 variants
        for key in [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m",
            "cloud_cover",
            "weather_code",
        ]:
            hourly_data[f"{key}_previous_day1"] = hourly_data[key]

        prev_response = {"hourly": hourly_data}
        actual_response = {"hourly": hourly_data}

        call_count = 0

        def mock_get(*_args: object, **_kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            resp = AsyncMock()
            resp.status = 200
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(return_value=prev_response if call_count == 1 else actual_response)
            resp.headers = {}
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=mock_get)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        adapter = OpenMeteoAdapter()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_weather_match_window(51.5, -0.1, "2026-04-01", kickoff_hour=15)
        assert isinstance(result, dict)
        # Should have keys for forecast_t24h, forecast_t0, and actual
        assert "forecast_t24h_ko_temp" in result or "actual_ko_temp" in result


class TestOpenMeteoHelpers:
    def test_parse_weather_response_valid(self) -> None:
        result = _parse_weather_response(_SAMPLE_WEATHER_RESPONSE, 51.5, -0.1, "2026-04-01")
        assert result is not None
        assert result.temperature_celsius == 15.0  # index 2 = midday
        assert result.precipitation_mm == 0.5

    def test_parse_weather_response_not_dict(self) -> None:
        assert _parse_weather_response("not-dict", 0, 0, "2026-01-01") is None

    def test_parse_weather_response_invalid_schema(self) -> None:
        result = _parse_weather_response({"hourly": {"time": [], "temperature_2m": []}}, 0, 0, "2026-01-01")
        assert result is None  # empty time list

    def test_parse_weather_response_no_hourly(self) -> None:
        result = _parse_weather_response({"latitude": 51.5}, 0, 0, "2026-01-01")
        assert result is None

    def test_find_midday_index(self) -> None:
        times = ["2026-04-01T06:00", "2026-04-01T12:00", "2026-04-01T18:00"]
        assert _find_midday_index(times) == 1

    def test_find_midday_index_no_match(self) -> None:
        times = ["2026-04-01T06:00", "2026-04-01T09:00", "2026-04-01T18:00"]
        # Fallback: middle of list
        assert _find_midday_index(times) == 1

    def test_find_midday_index_empty(self) -> None:
        assert _find_midday_index([]) == 0

    def test_safe_list_get(self) -> None:
        assert _safe_list_get([1.0, 2.0, 3.0], 1) == 2.0
        assert _safe_list_get(None, 0) is None
        assert _safe_list_get([1.0], 5) is None
        assert _safe_list_get([None], 0) is None

    def test_safe_list_get_int(self) -> None:
        assert _safe_list_get_int([10, 20, 30], 1) == 20
        assert _safe_list_get_int(None, 0) is None
        assert _safe_list_get_int([10], 5) is None
        assert _safe_list_get_int([None], 0) is None

    def test_weather_code_to_condition_clear(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        assert _weather_code_to_condition(0) == WeatherCondition.CLEAR
        assert _weather_code_to_condition(1) == WeatherCondition.CLEAR

    def test_weather_code_to_condition_partly_cloudy(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        assert _weather_code_to_condition(2) == WeatherCondition.PARTLY_CLOUDY
        assert _weather_code_to_condition(3) == WeatherCondition.PARTLY_CLOUDY

    def test_weather_code_to_condition_rain(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        assert _weather_code_to_condition(61) == WeatherCondition.RAIN

    def test_weather_code_to_condition_snow(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        assert _weather_code_to_condition(71) == WeatherCondition.SNOW

    def test_weather_code_to_condition_thunderstorm(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        assert _weather_code_to_condition(95) == WeatherCondition.THUNDERSTORM

    def test_weather_code_to_condition_none(self) -> None:
        assert _weather_code_to_condition(None) is None

    def test_weather_code_to_condition_unknown(self) -> None:
        from unified_api_contracts.external.open_meteo import WeatherCondition

        result = _weather_code_to_condition(999)
        assert result == WeatherCondition.CLOUDY

    def test_find_hour_index(self) -> None:
        times = [f"2026-04-01T{h:02d}:00" for h in range(24)]
        assert _find_hour_index(times, 15) == 15
        assert _find_hour_index(times, 0) == 0

    def test_find_hour_index_not_found(self) -> None:
        times = ["2026-04-01T00:00", "2026-04-01T06:00"]
        idx = _find_hour_index(times, 15)
        assert 0 <= idx < len(times)

    def test_find_hour_index_empty(self) -> None:
        assert _find_hour_index([], 15) == 0

    def test_build_weather_from_hourly_valid(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
            "temperature_2m": [18.5],
            "wind_speed_10m": [25.0],
            "relative_humidity_2m": [65],
            "precipitation": [0.2],
            "cloud_cover": [40],
            "weather_code": [3],
        }
        result = _build_weather_from_hourly(hourly, 0, 51.5, -0.1, "2026-04-01")
        assert result is not None
        assert result.temperature_celsius == 18.5
        assert result.precipitation_mm == 0.2

    def test_build_weather_from_hourly_with_prefix(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
            "temperature_2m_previous_day1": [17.0],
            "wind_speed_10m_previous_day1": [20.0],
            "relative_humidity_2m_previous_day1": [70],
            "precipitation_previous_day1": [0.0],
            "cloud_cover_previous_day1": [50],
            "weather_code_previous_day1": [2],
        }
        result = _build_weather_from_hourly(hourly, 0, 51.5, -0.1, "2026-04-01", var_prefix="_previous_day1")
        assert result is not None
        assert result.temperature_celsius == 17.0

    def test_build_weather_from_hourly_no_temp(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
        }
        result = _build_weather_from_hourly(hourly, 0, 51.5, -0.1, "2026-04-01")
        assert result is None

    def test_build_weather_from_hourly_null_temp(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
            "temperature_2m": [None],
        }
        result = _build_weather_from_hourly(hourly, 0, 51.5, -0.1, "2026-04-01")
        assert result is None

    def test_build_weather_from_hourly_idx_out_of_range(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
            "temperature_2m": [18.5],
        }
        result = _build_weather_from_hourly(hourly, 10, 51.5, -0.1, "2026-04-01")
        assert result is None

    def test_build_weather_from_hourly_missing_optional_fields(self) -> None:
        hourly: dict[str, list[float | int | str | None]] = {
            "time": ["2026-04-01T15:00"],
            "temperature_2m": [18.5],
            # No wind, humidity, etc.
        }
        result = _build_weather_from_hourly(hourly, 0, 51.5, -0.1, "2026-04-01")
        assert result is not None
        assert result.wind_speed_ms is None
        assert result.humidity_pct is None
