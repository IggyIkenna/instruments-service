"""Sports daily fixture re-poll trigger — ``sports.fixtures.daily_repoll``.

Phase B.1 of ``plans/epics/instruments_master.md``: a
live-mode trigger that pulls fixtures from api-football for the rolling
window ``[today, today + 8d]``, upserts to the canonical sports GCS
path (per UAC ``candidate_parquet_paths(SPORTS_FIXTURES, day,
league_id)``), and records the shard via
``ManifestWriter.record_captured`` with the canonical sports row_key
shape.

Behaviour contract:

* **Window**: 9 days = today + 8 days lookahead. Each day is a separate
  api-football ``/fixtures?date=YYYY-MM-DD`` call (same call shape as
  the batch fixture loop in ``orchestrator._fetch_sports_reference``).
  Per league fan-out happens inside the API call via
  ``league_ids=[<af_id>]`` so we cover the full Prediction +
  Reference + Features classification per UAC
  ``get_leagues_by_classification``.
* **Inserts vs upserts**: every fire treats new fixtures as inserts and
  existing fixtures as upserts. Status changes (``scheduled`` →
  ``cancelled`` / ``postponed`` / ``in-play`` / ``finished``) propagate
  via the ``status_long`` / ``status_short`` columns on the same
  ``af_fixture_id`` row. Today's fixtures get re-polled every fire so
  intra-day cancellation / postponement is captured. The lifecycle
  column shape is referenced from the active issue
  ``plans/active/issues/fixtures_postponed_cancelled_lifecycle_2026_05_08.md``;
  this module does NOT re-derive it — it writes whatever
  ``_flatten_canonical_fixture_for_disk`` produces.
* **available_at**: stamped per-row at write time as
  ``announced_at = kickoff_utc - 7 days``, mirroring the batch fixture
  fetch path (``orchestrator.py`` lines 3649-3651, 3703-3705) so live ≡
  batch on the timing axis. UAC
  ``AVAILABILITY_AT_SEMANTICS["FIXTURES"] = "announced_at"`` per
  CLAUDE.md "available_at is per-row, write-time" rule.
* **GCS path SSOT**: writes via ``_write_fixtures_per_league`` →
  ``sports_reference/by_date/day=<D>/entity=fixtures/league=<L>/fixtures.parquet``.
  This matches UAC ``candidate_parquet_paths("FIXTURES", day, league_id)``
  per the workspace-wide SPORTS GCS Path SSOT (CLAUDE.md "Sports GCS
  path SSOT" rule).
* **Manifest shard**: ``record_captured`` row_key
  ``{"date": <D>, "data_type": "FIXTURES", "league_id": <L>}`` with
  kwargs ``category="sports"``, ``instrument_type="football"``,
  ``data_type="FIXTURES"``, ``league_id=<L>``. Same shape as the
  batch path so a future trigger fire cleanly supersedes prior batch
  rows on the same ``(date, FIXTURES, league_id)`` key.
* **Idempotency**: re-running the same trigger on the same UTC day
  produces (a) no duplicate parquet content (the per-partition sink
  overwrites the existing ``fixtures.parquet``), and (b) no duplicate
  manifest rows (CAS dedup by row_key, last-writer-wins). Multi-VM
  invocations use the workspace-wide per-VM-shard isolation pattern
  (caller sets ``VM_NAME`` + ``MANIFEST_PER_VM_SHARDS=true``) per
  CLAUDE.md "Per-VM shard isolation" rule.

References:

* ``plans/epics/instruments_master.md`` § B.1
  (lines 285-294).
* ``orchestrator._flatten_canonical_fixture_for_disk`` —
  CanonicalFixture → SPORTS_FIXTURES SchemaContract row.
* ``orchestrator._write_fixtures_per_league`` — per-league GCS sink.
* ``unified_api_contracts.sports.candidate_parquet_paths`` — path SSOT.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from datetime import date as _date
from typing import TYPE_CHECKING

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_trading_library import (
    ManifestWriter,
    classify_and_emit_error,
    get_data_sink,
)
from unified_trading_library.cloud_interface.bucket_naming import resolve_bucket_name

# Import private functions - these should be made public in future refactor
from instruments_service.engine.orchestrator import (
    _canonical_league_id as canonical_league_id,
)
from instruments_service.engine.orchestrator import (
    _flatten_canonical_fixture_for_disk as flatten_canonical_fixture_for_disk,
)
from instruments_service.engine.orchestrator import (
    _write_fixtures_per_league as write_fixtures_per_league,
)
from instruments_service.reference_data import create_sports_reference_adapter

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


def get_instruments_bucket_for_asset_group(asset_group: str = "SPORTS") -> str:
    """Get the instruments bucket for the given asset group using public API."""
    return resolve_bucket_name(f"instruments-store-{asset_group.lower()}")


SPORTS_FIXTURES_DAILY_REPOLL_TRIGGER: str = "sports.fixtures.daily_repoll"
"""Closed-set trigger name routed to :func:`run_sports_fixtures_daily_repoll`."""

_DEFAULT_LOOKAHEAD_DAYS: int = 8
"""Default rolling-window lookahead. ``[today, today + LOOKAHEAD_DAYS]`` inclusive
covers the full 9-day fixture announcement horizon — api-football publishes
fixture metadata ~1 week before kickoff, so today + 8 days catches the entire
upcoming-fixture surface in one fire."""

_ANNOUNCED_AT_LEAD_DAYS: int = 7
"""Days before kickoff at which a scheduled fixture is announced. Mirrors the
batch path constant (``orchestrator.py`` lines 3651, 3705). Per UAC
``AVAILABILITY_AT_SEMANTICS["FIXTURES"] = "announced_at"`` and CLAUDE.md
"fixtures → announced_at" rule."""


def _resolve_today(today: _date | str | None) -> _date:
    """Coerce caller's ``today`` arg (or ``None`` for live UTC clock) to a date."""
    if today is None:
        return datetime.now(UTC).date()
    if isinstance(today, _date):
        return today
    return _date.fromisoformat(today)


def _date_window(today: _date, lookahead_days: int) -> list[_date]:
    """Return the inclusive list ``[today, today + lookahead_days]``."""
    return [today + timedelta(days=i) for i in range(lookahead_days + 1)]


def _league_ids_for_repoll(league_filter: Iterable[str | int] | None) -> list[int]:
    """Resolve the api-football ``league_id`` set the trigger should cover.

    Without an explicit filter we pull the full Prediction + Features +
    Reference classification from UAC — same set the batch fallback uses
    (``orchestrator.py`` lines 3673-3677). With a filter, callers pass
    canonical league_ids OR raw api_football_ids; we resolve to the
    api_football int IDs since that's what
    :class:`ApiFootballAdapter.get_fixtures` accepts.
    """
    from unified_api_contracts.sports import (
        get_league,
        get_leagues_by_classification,
    )

    if league_filter is None:
        af_ids: list[int] = []
        for cls in ("Prediction", "Features", "Reference"):
            for ld in get_leagues_by_classification(cls):
                if ld.api_football_id is not None:
                    af_ids.append(ld.api_football_id)
        return af_ids

    out: list[int] = []
    for raw in league_filter:
        s = str(raw).strip()
        if s.isdigit():
            out.append(int(s))
            continue
        ld = get_league(s)
        if ld is not None and ld.api_football_id is not None:
            out.append(ld.api_football_id)
    return out


async def run_sports_fixtures_daily_repoll(
    *,
    today: _date | str | None = None,
    api_key: str,
    bucket: str | None = None,
    lookahead_days: int = _DEFAULT_LOOKAHEAD_DAYS,
    league_filter: Iterable[str | int] | None = None,
    correlation_id: str | None = None,
) -> dict[str, int]:
    """Run the ``sports.fixtures.daily_repoll`` trigger — live mode B.1.

    Pulls fixtures from api-football for ``[today, today + lookahead_days]``
    and upserts each (day, league) parquet at the canonical sports GCS path,
    recording the shard in the availability manifest with the canonical row
    shape. Safe to invoke repeatedly on the same UTC day — see module
    docstring for the idempotency contract.

    Args:
        today: Anchor date for the rolling window. ``None`` = ``datetime.now(UTC).date()``;
            override for tests.
        api_key: api-football API key (Secret Manager value
            ``api-football-api-key``). Caller is responsible for fetching
            via :class:`ApiKeyReloader` so rotations are picked up
            mid-run; this trigger does NOT call Secret Manager itself.
        bucket: Sports instruments-store GCS bucket. ``None`` = resolved
            via :func:`_get_instruments_bucket("SPORTS")`.
        lookahead_days: Window size — total dates fetched =
            ``lookahead_days + 1`` (today inclusive). Default 8.
        league_filter: Optional iterable of canonical league_ids or raw
            api_football_ids. ``None`` = full Prediction + Features +
            Reference set from UAC.
        correlation_id: Caller-supplied tag for cross-event correlation
            (CLAUDE.md "No fire-and-forget VM launches" — the launcher
            sets ``VM_NAME`` and forwards it here for log lineage).

    Returns:
        Dict ``{f"{day}/{canonical_league_id}": row_count}`` per
        successfully-written shard. Caller can use this for
        progress events + ``INSTRUMENT_PROCESSED`` event payloads.

    Raises:
        ValueError: ``api_key`` empty / blank — fail loud per
            ApiFootballAdapter._headers().
    """
    if not api_key or not api_key.strip():
        raise ValueError(
            "sports.fixtures.daily_repoll requires a non-empty api-football API key. "
            "Caller must fetch 'api-football-api-key' from Secret Manager via "
            "ApiKeyReloader and forward it as the ``api_key`` argument."
        )

    anchor = _resolve_today(today)
    window = _date_window(anchor, lookahead_days)
    af_league_ids = _league_ids_for_repoll(league_filter)
    if not af_league_ids:
        logger.warning(
            "sports.fixtures.daily_repoll: empty league set after resolution (filter=%r) — nothing to do",
            league_filter,
        )
        return {}

    sports_bucket = bucket or get_instruments_bucket_for_asset_group("SPORTS")
    if not sports_bucket:
        raise ValueError(
            "sports.fixtures.daily_repoll: could not resolve SPORTS instruments-store "
            "bucket; pass ``bucket=`` explicitly or ensure UCI cloud config is initialised."
        )

    sink = get_data_sink(bucket=sports_bucket, prefix="sports_reference/by_date")
    manifest = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=sports_bucket,
    )
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)

    counts: dict[str, int] = {}
    logger.info(
        "sports.fixtures.daily_repoll START — today=%s window=[%s..%s] leagues=%d bucket=%s correlation_id=%s",
        anchor.isoformat(),
        window[0].isoformat(),
        window[-1].isoformat(),
        len(af_league_ids),
        sports_bucket,
        correlation_id or "",
    )

    for day in window:
        day_str = day.isoformat()
        # api-football allows passing multiple league_ids per call but the
        # adapter loops internally if >1. Our window is 9 days x ~20-30
        # leagues = ~200-300 calls; well within the 900 req/min mega-tier
        # quota the adapter throttles to. No fan-out needed here.
        try:
            fixtures = await adapter.get_fixtures(day_str, league_ids=af_league_ids)
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_fixtures_daily_repoll_fetch",
                shard=day_str,
            )
            # Per CLAUDE.md shard-level failure isolation, do NOT raise inside the
            # per-day loop — record_failed for THIS day, then keep iterating.
            manifest.record_failed(
                row_key={"date": day_str, "data_type": "FIXTURES"},
                error=str(exc),
                attempted_at=datetime.now(UTC),
                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            )
            continue

        if not fixtures:
            # Honest empty: api-football returned 0 fixtures for the day in
            # our league set. CLAUDE.md "asset-group-specific empty_confirmed
            # legitimacy rule" — sports CAN have empty_confirmed at the
            # day grain. Reason taxonomy: ``SOURCE_RETURNED_ZERO``.
            manifest.record_empty(
                row_key={"date": day_str, "data_type": "FIXTURES"},
                reason="SOURCE_RETURNED_ZERO",
                attempted_at=datetime.now(UTC),
                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            )
            logger.info(
                "sports.fixtures.daily_repoll: empty fixture set for day=%s — recorded SOURCE_RETURNED_ZERO",
                day_str,
            )
            continue

        # Flatten + stamp available_at per the SPORTS_FIXTURES contract.
        # Inject canonical ``league_id`` from each fixture's CanonicalLeague —
        # ``_flatten_canonical_fixture_for_disk`` only writes ``af_league_id``
        # (which depends on ``api_football_id`` being attached to the league
        # / venue / team objects). For live-mode triggers we always have
        # ``fx.league.league_id`` directly (canonical string per UAC), so we
        # bypass the af_league_id reverse lookup at the per-league split step
        # below + carry the canonical id through to the manifest row_key.
        rows: list[dict[str, object]] = []
        for fx in fixtures:
            row = flatten_canonical_fixture_for_disk(fx, day_str)
            league = getattr(fx, "league", None)
            if league is not None:
                row["league_id"] = getattr(league, "league_id", "") or ""
            rows.append(row)
        df = pd.DataFrame(rows)
        if "timestamp" in df.columns:
            kickoff_utc = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            announced_at = kickoff_utc - pd.Timedelta(days=_ANNOUNCED_AT_LEAD_DAYS)
            df["available_at"] = announced_at
        else:
            # Defensive — every CanonicalFixture carries kickoff_utc (else
            # _flatten emits day-string ``date`` and None ``timestamp``). Stamp
            # ``available_at`` to today midnight so the presence guard passes
            # with the most-conservative possible value (= we'd have it now).
            df["available_at"] = pd.Timestamp(day_str, tz="UTC")

        # Per-league split + write via the existing canonical sink helper.
        # Same path, same partition keys, same filename as batch.
        write_fixtures_per_league(sink, df, day_str, source_label="trigger:daily_repoll")

        # Per-league manifest rows — the writer dedupes by row_key, so a
        # subsequent fire on the same day overwrites cleanly with
        # last-writer-wins. We split by canonical league_id and call
        # record_captured per league so the shard granularity matches the
        # parquet granularity (CLAUDE.md "Shard-granularity SSOT").
        df_with_league = df.copy()
        if "league_id" not in df_with_league.columns or df_with_league["league_id"].isna().all():
            # Fall back to af_league_id mapping (same logic as
            # _write_fixtures_per_league).
            from unified_api_contracts.sports import (
                get_league_by_api_football_id,
            )

            af_int = pd.to_numeric(df_with_league.get("af_league_id"), errors="coerce").astype("Int64")

            def _resolve(v: object) -> str:
                if pd.isna(v):
                    return ""
                ld = get_league_by_api_football_id(int(v))  # pyright: ignore[reportArgumentType]
                return ld.league_id if ld is not None else str(int(v))  # pyright: ignore[reportArgumentType]

            df_with_league["_resolved_league_id"] = af_int.map(_resolve)
            league_col = "_resolved_league_id"
        else:
            league_col = "league_id"

        for lid_raw, league_df in df_with_league.groupby(league_col):
            lid_str = str(lid_raw)
            if not lid_str or lid_str == "nan":
                continue
            canonical_lid = canonical_league_id(lid_str)
            league_df_clean = league_df.drop(columns=["_resolved_league_id"], errors="ignore")
            row_count = len(league_df_clean)
            try:
                manifest.record_captured(  # QG-allow: emission-policy-not-applicable — raw api-football FIXTURES input capture; not a derived-output boundary
                    row_key={
                        "date": day_str,
                        "data_type": "FIXTURES",
                        "league_id": canonical_lid,
                    },
                    df=league_df_clean,
                    category="sports",
                    instrument_type="football",
                    data_type="FIXTURES",
                    league_id=canonical_lid,
                    pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                )
                counts[f"{day_str}/{canonical_lid}"] = row_count
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_fixtures_daily_repoll_record",
                    shard=f"{day_str}/{canonical_lid}",
                )

    manifest.flush()
    logger.info(
        "sports.fixtures.daily_repoll DONE — wrote %d (day, league) shards spanning %d days; total rows=%d",
        len(counts),
        len(window),
        sum(counts.values()),
    )
    return counts
