"""Sports FIXTURES status-refresh trigger — ``sports.fixtures.status_refresh``.

Root cause fix for the api_football stale-NS finding (issue doc
``api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``):
``_read_fixture_ids_from_gcs`` (``sports_fixtures.py``) only treats a captured
FIXTURES row as "completed" (and therefore eligible for per-fixture
enrichment) when ``status_short`` is one of ``FT``/``AET``/``PEN``. A date
captured close to real-time freezes at whatever status the match had at
capture time (usually ``NS``, not yet kicked off) — the existing
``sports.fixtures.daily_repoll`` trigger only re-polls a fixed
``[today-1, today+8]`` forward window (module docstring: T+1 closing
re-poll), so a date that is more than a day or two old at capture time and
never gets a fresh write is stuck at a non-terminal status FOREVER; no
amount of non-force enrichment relaunching can ever resolve it, because
enrichment only acts on fixture_ids this reader already considers
"completed".

This trigger closes that gap: it walks a BOUNDED trailing window of
already-captured dates (``[today - min_age_days - lookback_days, today -
min_age_days]`` — single-walk discipline: never a whole-corpus scan),
reads each date's existing captured FIXTURES parquet (one GCS read per
date via ``_find_stale_fixture_leagues_for_date``), and for any
``(date, league)`` cell whose captured rows are still non-terminal
(``status_short`` not in ``TERMINAL_FIXTURE_STATUSES``), re-fetches ONLY
that cell from api-football (targeted ``league_ids=`` fan-out — not a
blanket whole-date re-fetch, which would waste API-key budget re-fetching
already-settled leagues) and overwrites the per-league parquet + manifest
row with the fresh (likely now-terminal) status.

Idempotency: re-running against the same window is safe — a date/league
whose captured status is already terminal is skipped (no API call); a
date/league still non-terminal (e.g. postponed-and-rescheduled) is simply
re-checked and re-fetched again, converging once the real-world match
concludes.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from datetime import date as _date

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_api_contracts.sports import CanonicalFixture, get_league, get_league_by_api_football_id
from unified_trading_library import ManifestWriter, classify_and_emit_error, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _canonical_league_id as canonical_league_id,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.engine.orchestrator import (
    _flatten_canonical_fixture_for_disk as flatten_canonical_fixture_for_disk,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.engine.orchestrator import (
    _sports_ref_sink_for as sports_ref_sink_for,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.engine.orchestrator import (
    _write_fixtures_per_league as write_fixtures_per_league,  # pyright: ignore[reportPrivateUsage]
)

# Not re-exported from the orchestrator package (that __init__.py is at the
# 900-line file-size cap) — import directly from the cohesion module.
from instruments_service.engine.orchestrator.sports_fixtures import (
    _find_stale_fixture_ids_for_date as find_stale_fixture_ids_for_date,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _find_stale_fixture_leagues_for_date as find_stale_fixture_leagues_for_date,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.reference_data import create_sports_reference_adapter
from instruments_service.reference_data.adapters.sports.adapters.base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

SPORTS_FIXTURE_STATUS_REFRESH_TRIGGER: str = "sports.fixtures.status_refresh"
"""Closed-set trigger name routed to :func:`run_sports_fixture_status_refresh`."""

_DEFAULT_MIN_AGE_DAYS: int = 2
"""A date more recent than this is left alone — the match may genuinely still
be in progress or not yet kicked off; ``sports.fixtures.daily_repoll``'s
``[today-1, today+8]`` window already covers near-real-time status updates."""

_DEFAULT_LOOKBACK_DAYS: int = 30
"""Bounded trailing-window size scanned per fire, starting ``min_age_days``
back from today. Deliberately bounded (never whole-corpus) per the
single-walk discipline — a date whose stale status persists past this
window is picked up on a subsequent fire (this trigger is meant to run
periodically) or via a manual bounded ``--force`` sweep for older residual
clusters (tracked separately in the issue doc's P2 todo)."""


def _resolve_today(today: _date | str | None) -> _date:
    """Coerce caller's ``today`` arg (or ``None`` for live UTC clock) to a date."""
    if today is None:
        return datetime.now(UTC).date()
    if isinstance(today, _date):
        return today
    return _date.fromisoformat(today)


def _status_refresh_window(today: _date, min_age_days: int, lookback_days: int) -> list[_date]:
    """Return the trailing window ``[today - min_age_days - lookback_days, today - min_age_days]``."""
    return [today - timedelta(days=i) for i in range(min_age_days, min_age_days + lookback_days)]


def _resolve_stale_league(lid: str) -> int | None:
    """Resolve a ``_find_stale_fixture_leagues_for_date`` entry to an api_football id.

    That scan's ``league_id`` values come from whatever the captured parquet
    carries: a canonical string (``"EPL"``) for rows written after the
    ``_canonical_league_id`` (CF-7) normalization landed, OR a raw numeric
    partition-derived string (``"129"``) for legacy blobs whose ``league_id``
    column predates it (path-injection fallback in
    ``_read_per_league_entity_df``, see
    ``api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``).
    ``get_league()`` only accepts canonical strings — a raw-numeric entry
    silently resolved to ``None`` and was dropped from every re-fetch,
    forever, with no error. Mirrors ``_canonical_league_id``'s own numeric
    resolution pass.
    """
    league = get_league(lid)
    if league is not None:
        return league.api_football_id
    if lid.isdigit():
        numeric_league = get_league_by_api_football_id(int(lid))
        if numeric_league is not None:
            return numeric_league.api_football_id
    return None


def _resolve_stale_league_af_ids(stale_leagues: set[str], day_str: str) -> list[int]:
    """Resolve every stale ``league_id`` for ``day_str`` to an api_football id.

    Split out of :func:`run_sports_fixture_status_refresh` to keep that
    function under the module-level function-size gate. Logs (not raises) on
    a partial resolution failure — an unresolved league is still recovered
    below via the by-id reschedule fallback, which needs no league at all.
    """
    af_ids: list[int] = []
    unresolved: list[str] = []
    for lid in sorted(stale_leagues):
        af_id = _resolve_stale_league(lid)
        if af_id is not None:
            af_ids.append(af_id)
        else:
            unresolved.append(lid)
    if unresolved:
        logger.warning(
            "sports.fixtures.status_refresh: date=%s could not resolve %d stale league_id(s) to an "
            "api_football id — will still attempt a direct by-id re-fetch below: %s",
            day_str,
            len(unresolved),
            unresolved,
        )
    return af_ids


async def _reschedule_fallback_fetch(
    adapter: BaseSportsReferenceAdapter,
    sports_bucket: str,
    day_str: str,
    fixtures_with_raw: list[tuple[CanonicalFixture, dict[str, object]]],
) -> list[tuple[CanonicalFixture, dict[str, object]]]:
    """Recover stale fixtures the date-filtered season-cache lookup couldn't find.

    A fixture captured as stale on ``day_str`` may have been POSTPONED/
    RESCHEDULED by the league to a different real-world date since capture.
    The season cache always reflects the CURRENT live schedule, so it can
    never find a rescheduled fixture under its OLD date again — not a bug in
    the season fetch or league resolution, the match genuinely isn't
    scheduled for ``day_str`` anymore. This also recovers any fixture whose
    league_id couldn't be resolved above, since a direct by-id lookup needs
    no league resolution at all. Root-cause fix for the "648 season
    fixtures, 0 matches for the flagged date" anomaly in
    ``api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``.
    """
    found_ids = {(raw.get("fixture") or {}).get("id") for _fx, raw in fixtures_with_raw}
    stale_ids_by_league = find_stale_fixture_ids_for_date(sports_bucket, day_str)
    all_stale_ids = {fid for fids in stale_ids_by_league.values() for fid in fids}
    missing_ids = sorted(all_stale_ids - found_ids)
    if not missing_ids:
        return fixtures_with_raw

    logger.info(
        "sports.fixtures.status_refresh: date=%s has %d stale fixture(s) not found by the "
        "date-filtered season-cache lookup (likely postponed/rescheduled, or an unresolved "
        "league_id) — re-fetching by id directly: %s",
        day_str,
        len(missing_ids),
        missing_ids,
    )
    try:
        by_id_pairs = await adapter.get_fixtures_by_ids(missing_ids)
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sports_fixture_status_refresh_reschedule_fetch",
            shard=day_str,
        )
        return fixtures_with_raw
    return fixtures_with_raw + by_id_pairs


async def run_sports_fixture_status_refresh(
    *,
    today: _date | str | None = None,
    api_key: str,
    bucket: str | None = None,
    min_age_days: int = _DEFAULT_MIN_AGE_DAYS,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    correlation_id: str | None = None,
) -> dict[str, int]:
    """Run the ``sports.fixtures.status_refresh`` trigger.

    For each date in the trailing window, finds already-captured
    ``(date, league)`` cells whose FIXTURES status is still non-terminal and
    re-fetches ONLY those cells from api-football, overwriting the stale
    per-league parquet + manifest row with the fresh status.

    Args:
        today: Anchor date for the trailing window. ``None`` =
            ``datetime.now(UTC).date()``; override for tests.
        api_key: api-football API key (Secret Manager value
            ``api-football-api-key``). Caller fetches via
            :class:`ApiKeyReloader` so rotations are picked up mid-run;
            this trigger does NOT call Secret Manager itself.
        bucket: Sports instruments-store GCS bucket. ``None`` = resolved
            via ``resolve_bucket_name``.
        min_age_days: Dates more recent than this are skipped (still
            plausibly in progress / covered by the daily re-poll window).
        lookback_days: Trailing window size scanned per fire, starting
            ``min_age_days`` back from ``today``.
        correlation_id: Caller-supplied tag for cross-event correlation.

    Returns:
        Dict ``{f"{day}/{canonical_league_id}": row_count}`` per
        (date, league) cell actually re-fetched and re-written (dates/
        leagues already terminal are not included — no API call was made
        for them).

    Raises:
        ValueError: ``api_key`` empty / blank.
    """
    if not api_key or not api_key.strip():
        raise ValueError(
            "sports.fixtures.status_refresh requires a non-empty api-football API key. "
            "Caller must fetch 'api-football-api-key' from Secret Manager via "
            "ApiKeyReloader and forward it as the ``api_key`` argument."
        )

    anchor = _resolve_today(today)
    window = _status_refresh_window(anchor, min_age_days, lookback_days)
    sports_bucket = bucket or resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not sports_bucket:
        raise ValueError(
            "sports.fixtures.status_refresh: could not resolve SPORTS instruments-store "
            "bucket; pass ``bucket=`` explicitly or ensure UCI cloud config is initialised."
        )

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=sports_bucket)
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)

    counts: dict[str, int] = {}
    logger.info(
        "sports.fixtures.status_refresh START — today=%s window=[%s..%s] bucket=%s correlation_id=%s",
        anchor.isoformat(),
        window[-1].isoformat(),
        window[0].isoformat(),
        sports_bucket,
        correlation_id or "",
    )

    for day in window:
        day_str = day.isoformat()
        try:
            stale_leagues = find_stale_fixture_leagues_for_date(sports_bucket, day_str)
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_fixture_status_refresh_scan",
                shard=day_str,
            )
            continue
        if not stale_leagues:
            continue

        af_ids = _resolve_stale_league_af_ids(stale_leagues, day_str)

        fixtures_with_raw: list[tuple[CanonicalFixture, dict[str, object]]] = []
        if af_ids:
            logger.info(
                "sports.fixtures.status_refresh: date=%s has %d stale league(s) — re-fetching",
                day_str,
                len(af_ids),
            )
            try:
                fixtures_with_raw = await adapter.get_fixtures_with_raw(day_str, league_ids=af_ids)
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_fixture_status_refresh_fetch",
                    shard=day_str,
                )
                manifest.record_failed(
                    row_key={"date": day_str, "data_type": "FIXTURES"},
                    error=str(exc),
                    attempted_at=datetime.now(UTC),
                    pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                )
                continue

        # Reschedule fallback — recovers a stale fixture the date-filtered
        # season-cache lookup above couldn't find (postponed/rescheduled to
        # a different date, or an unresolved league_id). See
        # _reschedule_fallback_fetch's docstring for the root-cause context.
        fixtures_with_raw = await _reschedule_fallback_fetch(adapter, sports_bucket, day_str, fixtures_with_raw)

        if not fixtures_with_raw:
            # A stale-but-since-cancelled/removed fixture set is not this
            # trigger's concern — the empty-gap/typed-reason bookkeeping is
            # owned by the batch fetch + daily_repoll paths.
            continue

        rows: list[dict[str, object]] = []
        for fx, af_raw in fixtures_with_raw:
            row = flatten_canonical_fixture_for_disk(fx, day_str, af_response=af_raw)
            league = getattr(fx, "league", None)
            if league is not None:
                row["league_id"] = getattr(league, "league_id", "") or ""
            rows.append(row)
        df = pd.DataFrame(rows)
        if "timestamp" in df.columns:
            kickoff_utc = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df["available_at"] = kickoff_utc - pd.Timedelta(days=7)
        else:
            df["available_at"] = pd.Timestamp(day_str, tz="UTC")

        write_fixtures_per_league(
            sports_ref_sink_for(sports_bucket, day_str, "fixtures"),
            df,
            day_str,
            source_label="trigger:status_refresh",
            bucket=sports_bucket,
        )

        league_col = "league_id" if "league_id" in df.columns else None
        if league_col is None:
            continue
        for lid_raw, league_df in df.groupby(league_col):
            lid_str = str(lid_raw)
            if not lid_str or lid_str == "nan":
                continue
            canonical_lid = canonical_league_id(lid_str)
            row_count = len(league_df)
            try:
                manifest.record_captured(  # QG-allow: emission-policy-not-applicable — raw api-football FIXTURES input capture; not a derived-output boundary
                    row_key={"date": day_str, "data_type": "FIXTURES", "league_id": canonical_lid},
                    df=league_df,
                    asset_group="sports",
                    instrument_type="football",
                    data_type="FIXTURES",
                    league_id=canonical_lid,
                    pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                    source="api_football",
                )
                counts[f"{day_str}/{canonical_lid}"] = row_count
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_fixture_status_refresh_record",
                    shard=f"{day_str}/{canonical_lid}",
                )

    manifest.flush()
    logger.info(
        "sports.fixtures.status_refresh DONE — re-fetched %d (day, league) cell(s) across %d candidate day(s); "
        "total rows=%d",
        len(counts),
        len(window),
        sum(counts.values()),
    )
    return counts
