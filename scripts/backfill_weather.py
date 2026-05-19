#!/usr/bin/env python3
"""Backfill weather data for all fixture venues across all historical dates.

Strategy: bulk-download entire year of hourly data per venue (1 API call per
venue per year), then slice by fixture kickoff times to extract 3-hour match
windows. Much faster than per-day fetching (~650 calls total vs ~180K).

Open-Meteo free tier: 600 calls/min, 10K/day, 300K/month — more than enough.

Two data sources:
  - Archive API: actual observed weather (available from 1940+)
  - Previous Runs API: historical forecasts / T-24h predictions (from Jan 2024+)

Output: per-date weather.parquet in GCS with 66 columns per fixture venue:
  forecast_t24h_ko_temp, forecast_t24h_1h_precip_mm, ..., actual_2h_wind_kmh,
  plus aggregates (total_precip, rain_hours, wind_max, temp_range)

Usage:
    python scripts/backfill_weather.py --start-date 2024-01-01 --end-date 2026-04-15
    python scripts/backfill_weather.py --start-date 2024-01-01 --end-date 2026-04-15 --dry-run
    python scripts/backfill_weather.py --start-date 2019-01-01 --end-date 2023-12-31 --actuals-only
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import storage
from unified_api_contracts.registry.sports_venue_coordinates import (
    VENUE_COORDINATES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_PREV_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
_HOURLY_VARS = "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,cloud_cover,weather_code"
_COL_MAP = {
    "temperature_2m": "temp",
    "precipitation": "precip_mm",
    "wind_speed_10m": "wind_kmh",
    "relative_humidity_2m": "humidity_pct",
    "cloud_cover": "cloud_pct",
    "weather_code": "weather_code",
}
_HOUR_LABELS = ["ko", "1h", "2h"]

# Previous Runs API data availability
_PREV_RUNS_START = date_type(2024, 1, 1)


def _fetch_hourly_bulk(
    lat: float,
    lon: float,
    start: str,
    end: str,
    url: str,
    extra_vars: str = "",
) -> dict[str, list[float | int | str | None]]:
    """Fetch bulk hourly weather data for a venue across a date range."""
    hourly = _HOURLY_VARS
    if extra_vars:
        hourly = f"{hourly},{extra_vars}"
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "hourly": hourly,
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get("hourly", {})


def _extract_match_window(
    hourly: dict[str, list[float | int | str | None]],
    times: list[str],
    kickoff_hour: int,
    date_str: str,
    lead_prefix: str,
    var_suffix: str = "",
) -> dict[str, float | int | str | None]:
    """Extract 3-hour match window from bulk hourly data for a specific date."""
    result: dict[str, float | int | str | None] = {}
    target_prefix = f"{date_str}T"

    for hour_offset, hlabel in enumerate(_HOUR_LABELS):
        target_time = f"{target_prefix}{kickoff_hour + hour_offset:02d}:00"
        idx = None
        for i, t in enumerate(times):
            if t == target_time:
                idx = i
                break
        if idx is None:
            continue

        for api_key, col_name in _COL_MAP.items():
            data_key = f"{api_key}{var_suffix}"
            data = hourly.get(data_key)
            if data and idx < len(data) and data[idx] is not None:
                result[f"{lead_prefix}_{hlabel}_{col_name}"] = data[idx]

    # Aggregates
    precips = [result.get(f"{lead_prefix}_{h}_precip_mm") for h in _HOUR_LABELS]
    temps = [result.get(f"{lead_prefix}_{h}_temp") for h in _HOUR_LABELS]
    winds = [result.get(f"{lead_prefix}_{h}_wind_kmh") for h in _HOUR_LABELS]
    vp = [p for p in precips if p is not None]
    vt = [t for t in temps if t is not None]
    vw = [w for w in winds if w is not None]
    result[f"{lead_prefix}_total_precip_mm"] = sum(vp) if vp else None
    result[f"{lead_prefix}_rain_hours"] = sum(1 for p in vp if p > 0.5) if vp else None
    result[f"{lead_prefix}_wind_max_kmh"] = max(vw) if vw else None
    result[f"{lead_prefix}_temp_range"] = (max(vt) - min(vt)) if len(vt) >= 2 else None

    return result


def _load_fixtures_for_dates(
    bucket: storage.Bucket,
    start: str,
    end: str,
) -> dict[str, list[dict[str, str | int]]]:
    """Load fixture dates and kickoff hours from GCS.

    Returns: {date_str: [{venue_id, kickoff_hour}, ...]}
    """
    fixtures_by_date: dict[str, list[dict[str, str | int]]] = defaultdict(list)

    # List all date partitions in range
    start_d = date_type.fromisoformat(start)
    end_d = date_type.fromisoformat(end)
    current = start_d
    while current <= end_d:
        date_str = current.isoformat()
        prefix = f"sports_reference/by_date/day={date_str}/entity=fixtures/"
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=20))

        for blob in blobs:
            if not blob.name.endswith(".parquet"):
                continue
            data = blob.download_as_bytes()
            df = pq.read_table(io.BytesIO(data)).to_pandas()
            if "venue_id" not in df.columns and "home_team" in df.columns:
                # No venue_id — skip (will use all venues fallback)
                continue
            if "venue_id" in df.columns:
                for vid in df["venue_id"].dropna().unique():
                    ko_hour = 15  # default
                    if "kickoff_utc" in df.columns:
                        ko_rows = df[df["venue_id"] == vid]["kickoff_utc"].dropna()
                        if not ko_rows.empty:
                            try:
                                ko_str = str(ko_rows.iloc[0])
                                ko_hour = int(ko_str[11:13]) if len(ko_str) > 13 else 15
                            except (ValueError, IndexError):
                                pass
                    fixtures_by_date[date_str].append({"venue_id": str(vid), "kickoff_hour": ko_hour})

        current += timedelta(days=1)

    return dict(fixtures_by_date)


def run(args: argparse.Namespace) -> None:
    """Run the weather backfill."""
    venues = VENUE_COORDINATES
    logger.info("Venues with coordinates: %d", len(venues))

    client = storage.Client()
    bucket_name = "instruments-store-sports-central-element-323112"
    bucket = client.bucket(bucket_name)

    # Load fixture data to know which venues played on which dates
    logger.info("Loading fixture data from GCS for %s to %s...", args.start_date, args.end_date)
    fixtures_by_date = _load_fixtures_for_dates(bucket, args.start_date, args.end_date)
    logger.info("Found fixtures on %d dates", len(fixtures_by_date))

    # Determine which venues we need weather for
    all_venue_ids = set()
    for date_fixtures in fixtures_by_date.values():
        for f in date_fixtures:
            all_venue_ids.add(f["venue_id"])

    # If no fixture data found, use all venues
    if not all_venue_ids:
        logger.info("No fixture venue_ids found — using all %d venues", len(venues))
        all_venue_ids = set(venues.keys())

    # Filter to venues with coordinates
    venues_to_fetch = {vid: venues[vid] for vid in all_venue_ids if vid in venues}
    logger.info("Venues to fetch weather for: %d (of %d with fixtures)", len(venues_to_fetch), len(all_venue_ids))

    if args.dry_run:
        logger.info(
            "[DRY RUN] Would fetch weather for %d venues across %s to %s",
            len(venues_to_fetch),
            args.start_date,
            args.end_date,
        )
        return

    # Chunk date range into yearly segments for API calls
    start_d = date_type.fromisoformat(args.start_date)
    end_d = date_type.fromisoformat(args.end_date)
    year_chunks: list[tuple[str, str]] = []
    chunk_start = start_d
    while chunk_start <= end_d:
        chunk_end = min(date_type(chunk_start.year, 12, 31), end_d)
        year_chunks.append((chunk_start.isoformat(), chunk_end.isoformat()))
        chunk_start = date_type(chunk_start.year + 1, 1, 1)

    logger.info("Year chunks: %d (%s)", len(year_chunks), [f"{s[:4]}" for s, _ in year_chunks])

    # Per-venue: bulk download all years, then slice by fixture dates
    total_api_calls = 0
    total_rows_written = 0

    for vid, coords in venues_to_fetch.items():
        lat, lon = coords.latitude, coords.longitude
        logger.info("Processing venue %s (%.2f, %.2f)...", vid, lat, lon)

        # Accumulate all hourly data across years
        all_actual_hourly: dict[str, list[float | int | str | None]] = {}
        all_actual_times: list[str] = []
        all_prev_hourly: dict[str, list[float | int | str | None]] = {}
        all_prev_times: list[str] = []

        for chunk_start_str, chunk_end_str in year_chunks:
            # Actuals
            try:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
                url = _ARCHIVE_URL if chunk_end_str < cutoff else _FORECAST_URL
                hourly = _fetch_hourly_bulk(lat, lon, chunk_start_str, chunk_end_str, url)
                times = hourly.get("time", [])
                if not all_actual_times:
                    all_actual_hourly = hourly
                    all_actual_times = times
                else:
                    for key in hourly:
                        if key == "time":
                            all_actual_times.extend(hourly["time"])
                        elif key in all_actual_hourly:
                            all_actual_hourly[key].extend(hourly[key])
                        else:
                            all_actual_hourly[key] = hourly[key]
                total_api_calls += 1
            except Exception as exc:
                logger.warning("Actual fetch failed for %s %s-%s: %s", vid, chunk_start_str, chunk_end_str, exc)

            # Previous Runs (T-24h forecasts) — only available from 2024+
            if not args.actuals_only and chunk_end_str >= _PREV_RUNS_START.isoformat():
                prev_start = max(chunk_start_str, _PREV_RUNS_START.isoformat())
                prev_day1_vars = ",".join(f"{v}_previous_day1" for v in _HOURLY_VARS.split(","))
                try:
                    hourly = _fetch_hourly_bulk(lat, lon, prev_start, chunk_end_str, _PREV_RUNS_URL, prev_day1_vars)
                    times = hourly.get("time", [])
                    if not all_prev_times:
                        all_prev_hourly = hourly
                        all_prev_times = times
                    else:
                        for key in hourly:
                            if key == "time":
                                all_prev_times.extend(hourly["time"])
                            elif key in all_prev_hourly:
                                all_prev_hourly[key].extend(hourly[key])
                            else:
                                all_prev_hourly[key] = hourly[key]
                    total_api_calls += 1
                except Exception as exc:
                    logger.warning("Prev runs fetch failed for %s %s-%s: %s", vid, prev_start, chunk_end_str, exc)

            # Rate limit courtesy (not strictly needed with 600/min limit)
            time.sleep(0.1)

        logger.info("  %s: %d actual hours, %d prev-run hours fetched", vid, len(all_actual_times), len(all_prev_times))

        # Slice by fixture dates and write per-day parquets
        # Group rows by date
        date_rows: dict[str, list[dict[str, object]]] = defaultdict(list)

        # For dates with fixtures at this venue
        for date_str, date_fixtures in fixtures_by_date.items():
            venue_fixtures = [f for f in date_fixtures if f["venue_id"] == vid]
            if not venue_fixtures:
                continue
            ko_hour = int(venue_fixtures[0]["kickoff_hour"])

            row: dict[str, object] = {"venue_id": vid, "date": date_str, "latitude": lat, "longitude": lon}

            # Actuals
            if all_actual_times:
                actual_window = _extract_match_window(all_actual_hourly, all_actual_times, ko_hour, date_str, "actual")
                row.update(actual_window)

            # T-24h and T-0 forecasts
            if all_prev_times:
                t24h_window = _extract_match_window(
                    all_prev_hourly, all_prev_times, ko_hour, date_str, "forecast_t24h", "_previous_day1"
                )
                t0_window = _extract_match_window(all_prev_hourly, all_prev_times, ko_hour, date_str, "forecast_t0")
                row.update(t24h_window)
                row.update(t0_window)

            date_rows[date_str].append(row)

        # Also add this venue for dates without fixture data (all-venues fallback)
        for date_str in _generate_dates(args.start_date, args.end_date):
            if date_str not in fixtures_by_date and date_str not in date_rows:
                ko_hour = 15  # default
                row = {"venue_id": vid, "date": date_str, "latitude": lat, "longitude": lon}
                if all_actual_times:
                    row.update(_extract_match_window(all_actual_hourly, all_actual_times, ko_hour, date_str, "actual"))
                date_rows[date_str].append(row)

        # Write per-day parquets to GCS
        for date_str, rows in date_rows.items():
            if not rows or all(len(r) <= 4 for r in rows):
                continue  # Only metadata, no weather data
            df = pd.DataFrame(rows)
            buf = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
            buf.seek(0)
            path = f"sports_reference/by_date/day={date_str}/entity=weather/weather.parquet"
            blob = bucket.blob(path)
            blob.upload_from_file(buf, content_type="application/octet-stream")
            total_rows_written += len(df)

    logger.info("=== DONE ===")
    logger.info("API calls: %d", total_api_calls)
    logger.info("Rows written: %d", total_rows_written)
    logger.info("Dates with fixtures: %d", len(fixtures_by_date))


def _generate_dates(start: str, end: str) -> list[str]:
    """Generate all dates between start and end inclusive."""
    s = date_type.fromisoformat(start)
    e = date_type.fromisoformat(end)
    dates: list[str] = []
    current = s
    while current <= e:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill weather data for sports fixtures")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--actuals-only", action="store_true", help="Skip Previous Runs API (for pre-2024 dates)")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
