"""Open-Meteo adapter — weather data for match venues.

Auth: None (Open-Meteo is free, no API key needed for standard tier).
Base URL: https://api.open-meteo.com/v1
Ref: https://open-meteo.com/en/docs

Weather data for sports fixtures uses THREE endpoints:
  1. Archive API  — actual observed weather for historical dates (>90 days ago)
  2. Forecast API — current/recent weather + future forecasts
  3. Previous Runs API — historical forecasts (what was predicted N days before)
     Endpoint: https://previous-runs-api.open-meteo.com/v1/forecast
     Provides _previous_day1, _previous_day2 suffixed variables.
     Available from Jan 2024+ (GFS from Apr 2021).

For sports betting features we need point-in-time forecasts:
  T-24h: what was the forecast 24h before kickoff? (information available to bettors)
  T-12h: refined forecast closer to match
  T-0:   latest forecast at kickoff time (best pre-match estimate)
  HT:    actual weather during first half (known at halftime)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from unified_api_contracts.external.open_meteo import (
    OpenMeteoResponse,
    WeatherCondition,
)
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
    CanonicalWeather,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = "https://api.open-meteo.com/v1"
_PREV_RUNS_URL: str = "https://previous-runs-api.open-meteo.com/v1/forecast"
# Pro-tier customer endpoints — used when api_key is set
_CUSTOMER_BASE_URL: str = "https://customer-api.open-meteo.com/v1"
_CUSTOMER_ARCHIVE_URL: str = "https://customer-archive-api.open-meteo.com/v1/archive"
_CUSTOMER_PREV_RUNS_URL: str = "https://customer-previous-runs-api.open-meteo.com/v1/forecast"
_HOURLY_VARS: str = "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,cloud_cover,weather_code"


class OpenMeteoAdapter(BaseSportsReferenceAdapter):
    """Open-Meteo weather data adapter.

    Fetches weather data for match venues (temperature, rain, wind)
    from the Open-Meteo free API. No API key required.

    Note: Open-Meteo is free and does not need an API key.
    OpenWeather is the paid alternative.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "open_meteo"

    async def get_weather(
        self,
        venue_lat: float,
        venue_lon: float,
        date: str,
    ) -> CanonicalWeather | None:
        """Fetch weather data for a venue location and date.

        Args:
            venue_lat: Venue latitude.
            venue_lon: Venue longitude.
            date: Date string in YYYY-MM-DD format.

        Returns:
            WeatherData with temperature, wind, rain, humidity, or None on failure.
        """
        # Use archive-api for historical dates (>3 months ago), forecast for recent/future.
        # If api_key is set, use Pro-tier customer-* endpoints (unlimited + faster).
        cutoff = (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")
        if self._api_key:
            url = _CUSTOMER_ARCHIVE_URL if date < cutoff else f"{_CUSTOMER_BASE_URL}/forecast"
        else:
            url = "https://archive-api.open-meteo.com/v1/archive" if date < cutoff else f"{_BASE_URL}/forecast"
        params: dict[str, str] = {
            "latitude": str(venue_lat),
            "longitude": str(venue_lon),
            "hourly": ("temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,cloud_cover,weather_code"),
            "start_date": date,
            "end_date": date,
            "timezone": "UTC",
        }
        if self._api_key:
            params["apikey"] = self._api_key

        try:
            async with self._make_session() as session:
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

    async def get_weather_match_window(
        self,
        venue_lat: float,
        venue_lon: float,
        date: str,
        kickoff_hour: int = 15,
    ) -> dict[str, float | int | str | None]:
        """Fetch weather across the full match window at multiple lead times.

        Captures a 3-hour window (KO, KO+1h, KO+2h) for each lead time,
        covering the full ~105 minutes of a fixture including extra time.

        Uses Open-Meteo Previous Runs API for historical forecasts and
        archive/forecast API for actuals.

        Args:
            venue_lat: Venue latitude.
            venue_lon: Venue longitude.
            date: Match date (YYYY-MM-DD).
            kickoff_hour: Kickoff hour in UTC (0-23). Default 15 (3pm).

        Returns:
            Flat dict with prefixed columns for each lead_time x match_hour:
              forecast_t24h_ko_temp, forecast_t24h_1h_temp, forecast_t24h_2h_temp,
              forecast_t0_ko_temp, ...,
              actual_ko_temp, actual_1h_temp, actual_2h_temp,
              plus aggregates: total_precip, rain_hours, wind_max, temp_range
        """
        results: dict[str, float | int | str | None] = {}

        # Match window: 3 consecutive hours from kickoff
        match_hours = [kickoff_hour, kickoff_hour + 1, kickoff_hour + 2]
        hour_labels = ["ko", "1h", "2h"]  # kickoff, ~halftime, ~fulltime

        # Weather variables to extract per hour
        weather_keys = [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m",
            "cloud_cover",
            "weather_code",
        ]
        # Short column names for output
        col_names = ["temp", "precip_mm", "wind_kmh", "humidity_pct", "cloud_pct", "weather_code"]

        prev_day1_vars = ",".join(f"{v}_previous_day1" for v in _HOURLY_VARS.split(","))

        try:
            async with self._make_session() as session:
                # 1. Previous Runs API: T-24h forecast + T-0 forecast for full match window
                prev_params: dict[str, str] = {
                    "latitude": str(venue_lat),
                    "longitude": str(venue_lon),
                    "hourly": f"{_HOURLY_VARS},{prev_day1_vars}",
                    "start_date": date,
                    "end_date": date,
                    "timezone": "UTC",
                }
                if self._api_key:
                    prev_params["apikey"] = self._api_key
                prev_runs_url = _CUSTOMER_PREV_RUNS_URL if self._api_key else _PREV_RUNS_URL
                try:
                    # 2 retries max — if Previous Runs API is down, skip forecasts
                    # and still get actuals. Don't block 3+ min per venue on 500s.
                    prev_response = await self._get_with_retry(
                        session,
                        prev_runs_url,
                        params=prev_params,
                        max_retries=2,
                    )
                    if isinstance(prev_response, dict) and "hourly" in prev_response:
                        hourly = prev_response["hourly"]
                        times = hourly.get("time", [])
                        for lead, prefix_suffix in [("forecast_t24h", "_previous_day1"), ("forecast_t0", "")]:
                            for hour, hlabel in zip(match_hours, hour_labels, strict=True):
                                idx = _find_hour_index(times, hour)
                                for wkey, cname in zip(weather_keys, col_names, strict=True):
                                    data = hourly.get(f"{wkey}{prefix_suffix}")
                                    val = data[idx] if data and idx < len(data) and data[idx] is not None else None
                                    results[f"{lead}_{hlabel}_{cname}"] = val
                except Exception as exc:
                    logger.warning(
                        "Previous Runs API failed for (%.2f, %.2f) on %s: %s",
                        venue_lat,
                        venue_lon,
                        date,
                        exc,
                    )

                # 2. Actual weather across match window (archive for >90d, forecast for recent)
                cutoff = (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")
                if self._api_key:
                    actual_url = _CUSTOMER_ARCHIVE_URL if date < cutoff else f"{_CUSTOMER_BASE_URL}/forecast"
                else:
                    actual_url = (
                        "https://archive-api.open-meteo.com/v1/archive" if date < cutoff else f"{_BASE_URL}/forecast"
                    )
                actual_params: dict[str, str] = {
                    "latitude": str(venue_lat),
                    "longitude": str(venue_lon),
                    "hourly": _HOURLY_VARS,
                    "start_date": date,
                    "end_date": date,
                    "timezone": "UTC",
                }
                if self._api_key:
                    actual_params["apikey"] = self._api_key
                try:
                    actual_response = await self._get_with_retry(
                        session,
                        actual_url,
                        params=actual_params,
                    )
                    if isinstance(actual_response, dict) and "hourly" in actual_response:
                        hourly = actual_response["hourly"]
                        times = hourly.get("time", [])
                        for hour, hlabel in zip(match_hours, hour_labels, strict=True):
                            idx = _find_hour_index(times, hour)
                            for wkey, cname in zip(weather_keys, col_names, strict=True):
                                data = hourly.get(wkey)
                                val = data[idx] if data and idx < len(data) and data[idx] is not None else None
                                results[f"actual_{hlabel}_{cname}"] = val
                except Exception as exc:
                    logger.warning(
                        "Actual weather fetch failed for (%.2f, %.2f) on %s: %s",
                        venue_lat,
                        venue_lon,
                        date,
                        exc,
                    )

        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)

        # Compute aggregates across the 3-hour match window for each lead time
        for lead in ["forecast_t24h", "forecast_t0", "actual"]:
            precips = [results.get(f"{lead}_{h}_precip_mm") for h in hour_labels]
            temps = [results.get(f"{lead}_{h}_temp") for h in hour_labels]
            winds = [results.get(f"{lead}_{h}_wind_kmh") for h in hour_labels]

            valid_precips = [p for p in precips if p is not None]
            valid_temps = [t for t in temps if t is not None]
            valid_winds = [w for w in winds if w is not None]

            results[f"{lead}_total_precip_mm"] = sum(valid_precips) if valid_precips else None
            results[f"{lead}_rain_hours"] = sum(1 for p in valid_precips if p > 0.5) if valid_precips else None
            results[f"{lead}_wind_max_kmh"] = max(valid_winds) if valid_winds else None
            results[f"{lead}_temp_range"] = max(valid_temps) - min(valid_temps) if len(valid_temps) >= 2 else None

        fetched = sum(1 for v in results.values() if v is not None)
        logger.info(
            "Weather match window for (%.2f, %.2f) on %s KO=%02d:00: %d columns populated",
            venue_lat,
            venue_lon,
            date,
            kickoff_hour,
            fetched,
        )
        return results

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

    async def get_teams(self, league_id: int | str, season: int | None = None) -> list[CanonicalTeam]:
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
) -> CanonicalWeather | None:
    """Parse an Open-Meteo API response into CanonicalWeather."""
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
    condition = _weather_code_to_condition(_safe_list_get_int(hourly.weather_code, target_idx))
    return CanonicalWeather(
        latitude=venue_lat,
        longitude=venue_lon,
        temperature_celsius=temp,
        wind_speed_ms=wind_ms,
        humidity_pct=_safe_list_get_int(hourly.relative_humidity_2m, target_idx),
        precipitation_mm=_safe_list_get(hourly.precipitation, target_idx),
        cloud_cover_pct=_safe_list_get_int(hourly.cloud_cover, target_idx),
        condition=condition.value if condition is not None else None,
        observation_time=obs_time,
        date=date,
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


def _find_hour_index(times: list[str], target_hour: int) -> int:
    """Find the index for a specific hour in the time list.

    Times are formatted as "YYYY-MM-DDThh:mm". Returns the index
    closest to the target hour, clamped to valid range.
    """
    target = f"T{target_hour:02d}:"
    for i, t in enumerate(times):
        if target in t:
            return i
    # Fallback: clamp to valid range
    return min(max(target_hour, 0), len(times) - 1) if times else 0


def _build_weather_from_hourly(
    hourly: dict[str, list[float | int | str | None]],
    idx: int,
    venue_lat: float,
    venue_lon: float,
    date: str,
    var_prefix: str = "",
) -> CanonicalWeather | None:
    """Build CanonicalWeather from hourly dict at a specific index.

    var_prefix: "" for current/actual data, "_previous_day1" for T-24h forecast,
    "_previous_day2" for T-48h forecast, etc.
    """
    temp_key = f"temperature_2m{var_prefix}"
    temp_data = hourly.get(temp_key)
    if not temp_data or idx >= len(temp_data) or temp_data[idx] is None:
        return None

    wind_key = f"wind_speed_10m{var_prefix}"
    wind_data = hourly.get(wind_key)
    wind_val = wind_data[idx] if wind_data and idx < len(wind_data) else None
    wind_ms = float(wind_val) / 3.6 if wind_val is not None else None

    humidity_key = f"relative_humidity_2m{var_prefix}"
    humidity_data = hourly.get(humidity_key)
    humidity_val = (
        int(humidity_data[idx])
        if humidity_data and idx < len(humidity_data) and humidity_data[idx] is not None
        else None
    )

    precip_key = f"precipitation{var_prefix}"
    precip_data = hourly.get(precip_key)
    precip_val = (
        float(precip_data[idx]) if precip_data and idx < len(precip_data) and precip_data[idx] is not None else None
    )

    cloud_key = f"cloud_cover{var_prefix}"
    cloud_data = hourly.get(cloud_key)
    cloud_val = int(cloud_data[idx]) if cloud_data and idx < len(cloud_data) and cloud_data[idx] is not None else None

    code_key = f"weather_code{var_prefix}"
    code_data = hourly.get(code_key)
    code_val = int(code_data[idx]) if code_data and idx < len(code_data) and code_data[idx] is not None else None
    condition = _weather_code_to_condition(code_val)

    times = hourly.get("time", [])
    obs_time = times[idx] if idx < len(times) else date

    return CanonicalWeather(
        latitude=venue_lat,
        longitude=venue_lon,
        temperature_celsius=float(temp_data[idx]),
        wind_speed_ms=wind_ms,
        humidity_pct=humidity_val,
        precipitation_mm=precip_val,
        cloud_cover_pct=cloud_val,
        condition=condition.value if condition is not None else None,
        observation_time=obs_time,
        date=date,
    )


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
