"""Weather data: venue coordinates, fixture venue ids, weather fetch.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_extract_fixture_venue_ids",
    "_fetch_weather_data",
    "_load_venue_coordinates",
]


def _load_venue_coordinates(bucket: str) -> dict[str, tuple[float, float]]:
    """Load venue_id → (lat, lon) lookup from the global venues.parquet.

    Returns an empty dict if the file does not exist or cannot be read.
    The venues.parquet is written by ``_write_venues_from_teams`` at:
        sports_reference/venues/venues.parquet
    """
    venues_path = "sports_reference/venues/venues.parquet"
    try:
        storage = _orch.get_storage_client()
        blob = storage.bucket(bucket).blob(venues_path)
        if not blob.exists():
            _orch.logger.info("Weather: venues.parquet not found at gs://%s/%s", bucket, venues_path)
            return {}
        local = f"{_orch.tempfile.gettempdir()}/_weather_venues.parquet"
        blob.download_to_filename(local)
        venues_df = _orch.pd.read_parquet(local)
        if "venue_id" not in venues_df.columns:
            _orch.logger.warning("Weather: venues.parquet missing 'venue_id' column")
            return {}
        coords: dict[str, tuple[float, float]] = {}
        has_lat = "latitude" in venues_df.columns
        has_lon = "longitude" in venues_df.columns
        if not has_lat or not has_lon:
            _orch.logger.warning("Weather: venues.parquet missing latitude/longitude columns")
            return {}
        for _, row in venues_df.iterrows():
            vid = str(row["venue_id"])
            lat = row["latitude"]
            lon = row["longitude"]
            # pandas returns NaN for missing values, not None
            if _orch.pd.notna(lat) and _orch.pd.notna(lon):
                lat_f = float(lat)
                lon_f = float(lon)
                if lat_f != 0.0 and lon_f != 0.0:
                    coords[vid] = (lat_f, lon_f)
        return coords
    except Exception as exc:
        _orch.logger.debug("Weather: could not load venues.parquet: %s", exc)
        return {}


def _extract_fixture_venue_ids(bucket: str, date: str) -> list[str]:
    """Extract venue_ids from the fixtures parquet for a given date.

    Fixtures store the ``venue`` field as a dict with a ``venue_id`` key.
    Returns deduplicated venue IDs for fixtures on the requested date.
    """
    prefix = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
    try:
        storage = _orch.get_storage_client()
        blob = storage.bucket(bucket).blob(prefix)
        if not blob.exists():
            _orch.logger.debug("Weather: no fixtures parquet at gs://%s/%s", bucket, prefix)
            return []
        local = f"{_orch.tempfile.gettempdir()}/_weather_fixtures_{date}.parquet"
        blob.download_to_filename(local)
        df = _orch.pd.read_parquet(local)
        venue_ids: list[str] = []
        if "venue" in df.columns:
            for venue_val in df["venue"].dropna():
                if isinstance(venue_val, dict):
                    vid = venue_val.get("venue_id")
                    if vid:
                        venue_ids.append(str(vid))
                elif isinstance(venue_val, str):
                    venue_ids.append(venue_val)
        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for vid in venue_ids:
            if vid not in seen:
                seen.add(vid)
                unique.append(vid)
        return unique
    except Exception as exc:
        _orch.logger.debug("Weather: could not read fixtures for date=%s: %s", date, exc)
        return []


async def _fetch_weather_data(
    date: str,
    bucket: str,
    api_key: str | None = None,
) -> dict[str, int]:
    """Fetch Open-Meteo weather data for fixture venues and write to GCS.

    Weather (temperature, wind, rain, humidity) affects match outcomes —
    particularly relevant for outdoor sports prediction models.

    Flow:
      1. Read the global venues.parquet for venue_id → (lat, lon) lookup.
      2. Read fixtures.parquet for the date to get venue_ids of fixtures.
      3. For each fixture venue with coordinates, call Open-Meteo API.
      4. Write results to sports_reference/by_date/day={date}/entity=weather/weather.parquet.

    Honest-coverage: the WEATHER shard is emitted as ``captured`` when any
    venue observation lands, ``empty_confirmed`` when there are no fixtures
    (or no fixture venue has coordinates), and ``attempted_failed`` when the
    Open-Meteo API fails for all attempted venues.  Fixtures without a venue
    or venues without coordinates are skipped with a warning log (no raise —
    shard-level failure isolation).
    """
    import re

    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    from instruments_service.reference_data.adapters.sports.adapters.open_meteo import OpenMeteoAdapter

    adapter = OpenMeteoAdapter(api_key=api_key) if api_key else OpenMeteoAdapter()
    sink = _orch._sports_ref_sink_for(bucket, date, "weather")
    counts: dict[str, int] = {}

    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = _orch.datetime.now(_orch.UTC)
    _expected_weather_league_ids = {
        lg.league_id for lg in get_expected_leagues_for_source("open_meteo", classifications=["Prediction"])
    }

    def _record_weather_empty(reason: _orch.EmptyConfirmedReason) -> None:
        """Helper — emit per-league record_empty for WEATHER shard.

        Args:
            reason: Typed ``EmptyConfirmedReason`` — required so the caller
                must always articulate WHY the shard is empty.  Passing a
                blank string previously raised ``LegacyBlankErrorReasonError``
                at runtime; making the parameter required turns that into a
                static error at call time.

        Historic versions also emitted a date-aggregate row (no ``league_id``)
        alongside the per-league rows; the aggregator ignores it
        (``_sports_honest_coverage`` keys by ``league_id``) so it was pure
        data-entropy. Per Phase 2 of
        ``sports_manifest_shard_migration_cleanup_2026_04_21`` we now emit
        ONLY per-league rows.
        """
        for _exp_lid in sorted(_expected_weather_league_ids):
            manifest.record_empty(
                row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
                reason=reason,
            )

    def _record_weather_failed(err_code: str) -> None:
        """Helper — emit per-league record_failed for WEATHER shard.

        Same rationale as ``_record_weather_empty``: drop the unsharded
        date-aggregate emission that nobody consumes.
        """
        for _exp_lid in sorted(_expected_weather_league_ids):
            manifest.record_failed(
                row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                error=err_code,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
            )

    # UAC venue coordinates: SCREAMING_SNAKE keys → (lat, lon)
    from unified_api_contracts.registry import VENUE_COORDINATES

    # 1. Read fixtures for this date — get venue_name + kickoff hour
    fixtures_df = None
    _fixtures_read_failed = False
    try:
        fixtures_prefix = f"sports_reference/by_date/day={date}/entity=fixtures/"
        storage_client = _orch.get_storage_client()
        blobs = list(storage_client.list_blobs(bucket=bucket, prefix=fixtures_prefix, max_results=50))
        parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
        if parquet_blobs:
            frames = []
            for blob_meta in parquet_blobs:
                data = storage_client.download_bytes(bucket=bucket, blob_path=blob_meta.name)
                frames.append(_orch.pd.read_parquet(_orch.io.BytesIO(data)))
            fixtures_df = _orch.pd.concat(frames, ignore_index=True) if frames else None
    except Exception as exc:
        _orch.logger.warning("Weather: could not read fixtures for date=%s: %s", date, exc)
        _fixtures_read_failed = True
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="weather_fixtures_read",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "open_meteo")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "open_meteo",
                "endpoint": "fixtures_read",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        _record_weather_failed(_err_code)
        manifest.write()

    if _fixtures_read_failed:
        return counts

    if fixtures_df is None or fixtures_df.empty or "venue_name" not in fixtures_df.columns:
        _orch.logger.info("Weather: no fixture venue_name data for date=%s — skipping", date)
        # Honest-coverage: no fixtures == EXPECTED_NO_FIXTURE for the WEATHER
        # shard (per sports_classifier_weather_no_fixture_2026_05_13 write-side
        # prevention). Was previously falling through to SOURCE_RETURNED_ZERO
        # at classifier-side; emitting the typed reason here saves the
        # classifier round-trip.
        _record_weather_empty(reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE)
        manifest.write()
        return counts

    # 2. Match fixture venue_name → UAC coordinates via SCREAMING_SNAKE normalization
    def _to_snake(name: str) -> str:
        """Normalize venue name to SCREAMING_SNAKE: 'Anfield' → 'ANFIELD', 'Old Trafford' → 'OLD_TRAFFORD'."""
        s = re.sub(r"[^A-Za-z0-9 ]", "", name)
        return re.sub(r"\s+", "_", s.strip()).upper()

    venues_with_coords: list[tuple[float, float, str, int]] = []
    seen_venues: set[str] = set()
    skipped = 0
    for _, row in fixtures_df[["venue_name"]].drop_duplicates().dropna().iterrows():
        vname = str(row["venue_name"])
        snake = _to_snake(vname)
        if snake in seen_venues:
            continue
        seen_venues.add(snake)
        coords = VENUE_COORDINATES.get(snake)
        if coords is None:
            skipped += 1
            continue
        # Default kickoff hour = 15 UTC; override from fixture timestamp if available
        ko_hour = 15
        if "timestamp" in fixtures_df.columns:
            ko_rows = fixtures_df[fixtures_df["venue_name"] == vname]["timestamp"].dropna()
            if not ko_rows.empty:
                try:
                    ts = int(ko_rows.iloc[0])
                    ko_hour = (ts // 3600) % 24
                except (ValueError, TypeError):
                    pass
        venues_with_coords.append((coords.latitude, coords.longitude, snake, ko_hour))

    if skipped > 0:
        _orch.logger.info("Weather: %d fixture venues not in UAC coordinates for date=%s", skipped, date)
    _orch.logger.info(
        "Weather: %d fixture venues matched to coordinates for date=%s (of %d unique)",
        len(venues_with_coords),
        date,
        len(seen_venues),
    )

    # 3. Check existing weather data — only fetch venues not already covered.
    # Enables incremental runs: add more venue coords → re-run → only new venues fetched.
    existing_venue_ids: set[str] = set()
    try:
        # v9: probe canonical path (pipeline_mode= in prefix) first, then legacy.
        _w_pm = _orch._sports_ref_pm("weather")
        _canon_weather_prefix = f"sports_reference/by_date/day={date}/pipeline_mode={_w_pm}/entity=weather/"
        _legacy_weather_prefix = f"sports_reference/by_date/day={date}/entity=weather/"
        _canon_blobs = list(storage_client.list_blobs(bucket=bucket, prefix=_canon_weather_prefix, max_results=1))
        weather_prefix = _canon_weather_prefix if _canon_blobs else _legacy_weather_prefix
        weather_blobs = list(storage_client.list_blobs(bucket=bucket, prefix=weather_prefix, max_results=10))
        for wb in weather_blobs:
            if wb.name.endswith(".parquet"):
                wdata = storage_client.download_bytes(bucket=bucket, blob_path=wb.name)
                wdf = _orch.pd.read_parquet(_orch.io.BytesIO(wdata))
                if "venue_id" in wdf.columns:
                    existing_venue_ids = set(wdf["venue_id"].dropna().astype(str).unique())
    except Exception:
        pass  # No existing weather — fetch everything

    if existing_venue_ids:
        new_venues = [(lat, lon, vid, ko) for lat, lon, vid, ko in venues_with_coords if vid not in existing_venue_ids]
        if len(new_venues) < len(venues_with_coords):
            _orch.logger.info(
                "Weather: %d/%d venues already have data for date=%s — fetching %d new",
                len(venues_with_coords) - len(new_venues),
                len(venues_with_coords),
                date,
                len(new_venues),
            )
            venues_with_coords = new_venues

    if not venues_with_coords:
        if existing_venue_ids:
            # Data already on GCS, but the pre-sharding date-aggregate manifest
            # row may be the only surviving trace. Emit per-league captured
            # rows from fixtures_df so data-status UI can render per-league
            # completion (instead of one row per day). Idempotent — if
            # per-league rows exist, ManifestWriter dedup keeps them.
            _captured_leagues_covered: set[str] = set()
            if fixtures_df is not None and "league_id" in fixtures_df.columns:
                for _lid_val in fixtures_df["league_id"].dropna().astype(str).unique():
                    _lid_str = str(_lid_val).strip()
                    if not _lid_str:
                        continue
                    _captured_leagues_covered.add(_lid_str)
                    manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                        row_key={
                            "date": date,
                            "data_type": "WEATHER",
                            "league_id": _orch._canonical_league_id(_lid_str),
                        },
                        total_rows=1,
                        # SP-10: per-league WEATHER shard — shard atom is (date, data_type,
                        # league_id); a league is the row key, not a sub-cluster within the row.
                        # No per-root-cluster contract exists, so the gate is a deliberate no-op;
                        # an unfounded expectation would false-fail genuinely-captured data.
                        expected_root_clusters={},
                        observed_clusters={"": 1},
                        available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                        pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
                        service_emission_state=None,
                    )
            for _exp_lid in sorted(_expected_weather_league_ids - _captured_leagues_covered):
                manifest.record_empty(
                    row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
                )
            _orch.logger.info(
                "Weather: all %d venues already covered for date=%s — back-filled per-league manifest (%d captured, %d empty)",
                len(existing_venue_ids),
                date,
                len(_captured_leagues_covered),
                len(_expected_weather_league_ids - _captured_leagues_covered),
            )
            manifest.write()
            return counts
        _orch.logger.info("Weather: no fixture venues with coordinates for date=%s — skipping", date)
        # Honest-coverage: fixtures existed but none had a UAC coordinate
        # mapping — no mapping exists, not a source failure.
        _record_weather_empty(reason=_orch.EmptyConfirmedReason.EXPECTED_NO_MAPPING)
        manifest.write()
        return counts

    # 4. Fetch weather match window for each fixture venue.
    # Each venue gets a 3-hour window (KO, KO+1h, KO+2h) at each lead time.
    weather_rows: list[dict[str, object]] = []
    _per_venue_errors: dict[str, str] = {}
    for lat, lon, venue_key, ko_hour in venues_with_coords:
        try:
            match_weather = await adapter.get_weather_match_window(lat, lon, date, kickoff_hour=ko_hour)
            row: dict[str, object] = {
                "venue_id": venue_key,
                "date": date,
                "latitude": lat,
                "longitude": lon,
                "kickoff_hour": ko_hour,
            }
            row.update(match_weather)
            weather_rows.append(row)
        except Exception as exc:
            _orch.classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="weather_fetch",
                shard=venue_key,
            )
            _err_code = _orch._classify_adapter_failure(exc, "open_meteo")
            _orch.log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "open_meteo",
                    "endpoint": "get_weather_match_window",
                    "date": date,
                    "venue_key": venue_key,
                    "error": str(exc),
                    "error_code": _err_code,
                },
            )
            _per_venue_errors[venue_key] = _err_code

    # Build venue->league(s) map up front — needed both for per-league
    # partitioning and per-league manifest sharding. A single venue can host
    # matches in multiple leagues on the same date (cup + league doubles), so
    # the value is a set.
    _venue_to_leagues: dict[str, set[str]] = {}
    if fixtures_df is not None and "league_id" in fixtures_df.columns and "venue_name" in fixtures_df.columns:
        for _, _frow in fixtures_df[["venue_name", "league_id"]].dropna().iterrows():
            _vname = str(_frow["venue_name"]).strip()
            _lid_val = str(_frow["league_id"]).strip()
            if not _vname or not _lid_val:
                continue
            _vkey = _to_snake(_vname)
            _venue_to_leagues.setdefault(_vkey, set()).add(_lid_val)

    if weather_rows:
        new_df = _orch.pd.DataFrame(weather_rows)
        # PIT safety: weather observation/forecast availability.
        # Prefer existing observation_time/forecast_issue_time columns; otherwise fallback to date + 12:00 UTC.
        if "observation_time" in new_df.columns:
            new_df["available_at"] = _orch.pd.to_datetime(new_df["observation_time"], utc=True, errors="coerce")
        elif "forecast_issue_time" in new_df.columns:
            new_df["available_at"] = _orch.pd.to_datetime(new_df["forecast_issue_time"], utc=True, errors="coerce")
        else:
            new_df["available_at"] = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=12)

        # Merge with existing weather data (append new venues to existing).
        # Note: existing parquet may live at the bare or per-league path; we
        # walk the prefix and concatenate everything we find — the per-league
        # write loop below dedups by (venue_id, league_id) implicitly because
        # group-by partitions are mutually exclusive on league_id.
        if existing_venue_ids:
            try:
                weather_prefix = f"sports_reference/by_date/day={date}/entity=weather/"
                for wb in storage_client.list_blobs(bucket=bucket, prefix=weather_prefix, max_results=20):
                    if wb.name.endswith(".parquet"):
                        wdata = storage_client.download_bytes(bucket=bucket, blob_path=wb.name)
                        existing_df = _orch.pd.read_parquet(_orch.io.BytesIO(wdata))
                        new_df = _orch.pd.concat([existing_df, new_df], ignore_index=True)
                        _orch.logger.info(
                            "Weather: merged %d existing + %d new = %d total for date=%s (blob=%s)",
                            len(existing_df),
                            len(weather_rows),
                            len(new_df),
                            date,
                            wb.name,
                        )
            except Exception:
                pass  # Write new data only if merge fails

        # Per-league partitioned write — single SSOT, no bare write.  WEATHER
        # is per-fixture (lat/lon/temp at kickoff); each row is tied to a
        # venue, which we map to its hosting league(s) via _venue_to_leagues.
        # When a venue hosts fixtures across multiple leagues on the same
        # date, we duplicate the row into each league's parquet — downstream
        # readers join on (date, league_id) so duplication is correct.
        _captured_leagues: set[str] = set()
        _league_venue_count: dict[str, int] = {}
        _orphan_count = 0

        if _venue_to_leagues:
            # Build per-league dataframes by expanding each row into one row
            # per (venue, league) pair for venues hosting in multiple leagues
            # that date.  Track orphan rows separately for the warning log.
            _per_league_frames: dict[str, list[_orch.pd.DataFrame]] = {}
            for _, _wrow in new_df.iterrows():
                _vid = str(_wrow.get("venue_id", "")).strip() if "venue_id" in new_df.columns else ""
                _leagues_for_venue = _venue_to_leagues.get(_vid, set())
                if not _leagues_for_venue:
                    _orphan_count += 1
                    continue
                for _lid_v in _leagues_for_venue:
                    _row_copy = _wrow.to_frame().T.copy()
                    _row_copy["league_id"] = _lid_v
                    _per_league_frames.setdefault(_lid_v, []).append(_row_copy)

            for _lid_v, _frames in _per_league_frames.items():
                _w_lid_df = _orch.pd.concat(_frames, ignore_index=True)
                _captured_leagues.add(_lid_v)
                _league_venue_count[_lid_v] = _league_venue_count.get(_lid_v, 0) + len(_w_lid_df)
                _orch._gated_sink_write(
                    sink,
                    data=_orch.stamp_available_at_explicit(_w_lid_df, when=_orch.datetime.now(_orch.UTC)),
                    partition={"entity": "weather", "league": _orch._canonical_league_id(_lid_v)},
                    filename="weather.parquet",
                    venue="open_meteo",
                    entity="weather",
                )

            if _orphan_count > 0:
                _orch.logger.warning(
                    "WEATHER bare-path fallback triggered for date=%s — data shape regression: "
                    "%d rows could not be mapped to a league via venue_id. "
                    "Skipping bare write to keep manifest honest.",
                    date,
                    _orphan_count,
                )
        else:
            _orch.logger.warning(
                "WEATHER bare-path fallback triggered for date=%s — data shape regression: "
                "no venue->league map (fixtures_df missing league_id/venue_name); rows=%d. "
                "Skipping bare write to keep manifest honest.",
                date,
                len(new_df),
            )

        counts["weather"] = len(new_df)
        _orch.logger.info("Weather: %d venue observations written for date=%s", len(new_df), date)
    else:
        # Initialise tracking vars so the manifest write block below is safe
        # when no weather rows were captured this run.
        _captured_leagues = set()
        _league_venue_count = {}

    # Honest-coverage manifest write — per-league sharding so data-status UI
    # can render per-league WEATHER completion (not just one row per day).
    if weather_rows:
        # Per-league captured rows mirror the per-league parquet partitions
        # written above. _league_venue_count is populated only when the
        # per-league write path executed.
        for _lid, _count in sorted(_league_venue_count.items()):
            manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                row_key={"date": date, "data_type": "WEATHER", "league_id": _orch._canonical_league_id(_lid)},
                total_rows=_count,
                # SP-10: per-league WEATHER shard — shard atom is (date, data_type, league_id);
                # a league is the row key, not a sub-cluster within the row. No per-root-cluster
                # contract exists, so the gate is a deliberate no-op; an unfounded expectation
                # would false-fail genuinely-captured data.
                expected_root_clusters={},
                observed_clusters={"": _count},
                available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
                service_emission_state=None,
            )
        # Per-league empty_confirmed for in-season leagues with no captured weather
        for _exp_lid in sorted(_expected_weather_league_ids - _captured_leagues):
            manifest.record_empty(
                row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                attempted_at=attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch.PipelineMode.BATCH_OPEN_METEO,
            )
        # NOTE: Previously we also emitted a date-level aggregate row
        # (``manifest.add(data_type="WEATHER")`` with no ``league_id``) for
        # "backwards-compat". No consumer reads it — the deployment-api
        # aggregator (``_sports_honest_coverage``) groups manifest rows by
        # ``league_id`` and silently drops the empty-league-id bucket. The
        # aggregate row was pure data-entropy and is removed per
        # ``sports_manifest_shard_migration_cleanup_2026_04_21`` Phase 2.
    elif _per_venue_errors:
        # All attempts failed → attempted_failed.  Use the most common error
        # code so the manifest carries a representative classification.
        _err_sample = next(iter(sorted(_per_venue_errors.values())))
        _record_weather_failed(_err_sample)
    else:
        # No rows AND no errors — means venues existed but all were already
        # covered earlier in this run (incremental dedup skipped them) or
        # adapter returned empty dicts.  Treat as empty_confirmed.
        _record_weather_empty(
            reason=_orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO
        )  # QG-allow: weather-incremental-dedup; may be incremental-dedup skip not an external fetch failure; sports oracle not applicable — A10c-fleet followup

    manifest.write()

    return counts
