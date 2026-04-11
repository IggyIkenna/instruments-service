"""Open-Meteo adapter — weather data for match venues.

Auth: None (Open-Meteo is free, no API key needed for standard tier).
Base URL: https://api.open-meteo.com/v1
Ref: https://open-meteo.com/en/docs
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import aiohttp
from unified_api_contracts.external.open_meteo import (
    OpenMeteoResponse,
    WeatherCondition,
)
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = "https://api.open-meteo.com/v1"


@dataclass(frozen=True)
class WeatherData:
    """Weather observation for a venue at a specific time."""

    latitude: float
    longitude: float
    temperature_celsius: float
    wind_speed_ms: float | None
    humidity_pct: int | None
    precipitation_mm: float | None
    cloud_cover_pct: int | None
    condition: WeatherCondition | None
    observation_time: str


class OpenMeteoAdapter(BaseSportsReferenceAdapter):
    """Open-Meteo weather data adapter.

    Fetches weather data for match venues (temperature, rain, wind)
    from the Open-Meteo free API. No API key required.

    Note: Open-Meteo is free and does not need an API key.
    OpenWeather is the paid alternative.
    """

    @property
    def venue(self) -> str:
        return "open_meteo"

    async def get_weather(
        self,
        venue_lat: float,
        venue_lon: float,
        date: str,
    ) -> WeatherData | None:
        """Fetch weather data for a venue location and date.

        Args:
            venue_lat: Venue latitude.
            venue_lon: Venue longitude.
            date: Date string in YYYY-MM-DD format.

        Returns:
            WeatherData with temperature, wind, rain, humidity, or None on failure.
        """
        # Use archive-api for historical dates (>3 months ago), forecast for recent/future
        cutoff = (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")
        url = "https://archive-api.open-meteo.com/v1/archive" if date < cutoff else f"{_BASE_URL}/forecast"
        params: dict[str, str] = {
            "latitude": str(venue_lat),
            "longitude": str(venue_lon),
            "hourly": ("temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,cloud_cover,weather_code"),
            "start_date": date,
            "end_date": date,
            "timezone": "UTC",
        }

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            return None

        result = _parse_weather_response(raw_response, venue_lat, venue_lon, date)
        if result is not None:
            logger.info(
                "Fetched weather for (%.2f, %.2f) on %s: %.1f°C",
                venue_lat,
                venue_lon,
                date,
                result.temperature_celsius,
            )
        return result

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Open-Meteo does not provide fixture data.

        Use ApiFootballAdapter for fixtures. This returns an empty list.
        """
        logger.info("get_fixtures not supported on Open-Meteo adapter — use ApiFootballAdapter")
        return []

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Open-Meteo does not provide league data.

        Use ApiFootballAdapter for leagues. This returns an empty list.
        """
        logger.info("get_leagues not supported on Open-Meteo adapter — use ApiFootballAdapter")
        return []

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Open-Meteo does not provide team data.

        Use ApiFootballAdapter for teams. This returns an empty list.
        """
        logger.info("get_teams not supported on Open-Meteo adapter — use ApiFootballAdapter")
        return []

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Open-Meteo does not provide odds data.

        This adapter is for weather data only. Returns an empty list.
        """
        logger.info("get_odds not supported on Open-Meteo adapter")
        return []


def _parse_weather_response(
    raw_response: object,
    venue_lat: float,
    venue_lon: float,
    date: str,
) -> WeatherData | None:
    """Parse an Open-Meteo API response into WeatherData."""
    if not isinstance(raw_response, dict):
        return None
    try:
        response = OpenMeteoResponse.model_validate(raw_response)
    except Exception as exc:
        logger.warning("Failed to validate Open-Meteo response: %s", exc)
        return None

    hourly = response.hourly
    if hourly is None or not hourly.time:
        return None

    target_idx = _find_midday_index(hourly.time)
    temp = _safe_list_get(hourly.temperature_2m, target_idx)
    if temp is None:
        return None

    wind = _safe_list_get(hourly.wind_speed_10m, target_idx)
    wind_ms = wind / 3.6 if wind is not None else None
    obs_time = hourly.time[target_idx] if target_idx < len(hourly.time) else date
    return WeatherData(
        latitude=venue_lat,
        longitude=venue_lon,
        temperature_celsius=temp,
        wind_speed_ms=wind_ms,
        humidity_pct=_safe_list_get_int(hourly.relative_humidity_2m, target_idx),
        precipitation_mm=_safe_list_get(hourly.precipitation, target_idx),
        cloud_cover_pct=_safe_list_get_int(hourly.cloud_cover, target_idx),
        condition=_weather_code_to_condition(_safe_list_get_int(hourly.weather_code, target_idx)),
        observation_time=obs_time,
    )


def _find_midday_index(times: list[str]) -> int:
    """Find the index closest to 12:00 (midday) in the time list."""
    for i, t in enumerate(times):
        if "12:00" in t or "T12:" in t:
            return i
    # Fallback: middle of the list
    return len(times) // 2 if times else 0


def _safe_list_get(data: list[float] | None, idx: int) -> float | None:
    """Safely get a float from a list by index."""
    if data is None or idx >= len(data):
        return None
    val = data[idx]
    return float(val) if val is not None else None


def _safe_list_get_int(data: list[int] | None, idx: int) -> int | None:
    """Safely get an int from a list by index."""
    if data is None or idx >= len(data):
        return None
    val = data[idx]
    return int(val) if val is not None else None


_WMO_CODE_RANGES: list[tuple[int, WeatherCondition]] = [
    (1, WeatherCondition.CLEAR),
    (3, WeatherCondition.PARTLY_CLOUDY),
    (49, WeatherCondition.FOG),
    (59, WeatherCondition.DRIZZLE),
    (65, WeatherCondition.RAIN),
    (69, WeatherCondition.HEAVY_RAIN),
    (79, WeatherCondition.SNOW),
    (84, WeatherCondition.RAIN),
    (89, WeatherCondition.SNOW),
    (99, WeatherCondition.THUNDERSTORM),
]


def _weather_code_to_condition(code: int | None) -> WeatherCondition | None:
    """Map WMO weather code to WeatherCondition enum.

    WMO codes: https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM
    """
    if code is None:
        return None
    for upper_bound, condition in _WMO_CODE_RANGES:
        if code <= upper_bound:
            return condition
    return WeatherCondition.CLOUDY
