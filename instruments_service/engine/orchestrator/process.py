"""process_instruments — the per-date / per-venue orchestration entrypoint.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).
The legacy ~1,931-line ``process_instruments`` body is decomposed by stage into
sibling modules (pure behaviour-preserving extraction, same plan):

* ``process_preflight``    — provider short-circuit, freshness pre-flight, fast path
* ``process_fetch``        — URDI fetch, date filtering, per-fixture GCS fast path
* ``process_zero_records`` — stage-4 zero-record handling per asset group
* ``process_write``        — schema validation + per-venue parquet/manifest writes
* ``process_enrichment``   — stage-7 sports reference + enrichment providers
* ``process_completeness`` — stage-8 completeness check, retry, honest coverage

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

from instruments_service.engine.orchestrator.process_completeness import _completeness_and_retry
from instruments_service.engine.orchestrator.process_enrichment import _run_sports_enrichment
from instruments_service.engine.orchestrator.process_fetch import (
    _PER_FIXTURE_ENTITIES,
    _fetch_urdi_records,
    _filter_and_enrich_records,
    _per_fixture_gcs_fast_path,
    _resolve_skip_urdi,
    _UrdiFetchOutcome,
)
from instruments_service.engine.orchestrator.process_preflight import (
    _apply_sports_provider_filter,
    _enrichment_only_fast_path,
    _freshness_preflight,
    _promote_redo_all_for_recovery,
)
from instruments_service.engine.orchestrator.process_write import _validate_records, _write_all_venues
from instruments_service.engine.orchestrator.process_zero_records import _handle_zero_records

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "process_instruments",
]


async def process_instruments(
    date: str | _orch.datetime,
    asset_groups: list[str],
    redo_all: bool = False,
    api_keys: dict[str, str] | None = None,
    venue_override: list[str] | None = None,
    mode: str = "batch",
    sports_entity_filter: str | None = None,
    sports_provider: str | None = None,
    league_filter: list[str] | None = None,
    season_override: int | None = None,
    recovery_fixture_ids: frozenset[int] | None = None,
    source: str | None = None,
) -> dict[str, int]:
    """Process instruments for a single date and set of asset groups.

    Args:
        sports_provider: When set, only run this data provider (e.g. OPEN_METEO,
            API_FOOTBALL, TRANSFERMARKT). Maps to venue filter + entity scope.
        league_filter: When set, only process these canonical league IDs
            (e.g. ["EPL", "BUNDESLIGA"]). Default None = all prediction leagues.
        recovery_fixture_ids: af_fixture_id allowlist for targeted per-fixture
            recovery. When set, the per-fixture entity handlers
            (PLAYER_STATS / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS)
            filter fixture_ids to this set BEFORE calling api_football, and
            the per-league parquet writes do read-modify-write merges so
            existing fixtures' rows are preserved. Bypasses date-level
            pre-flight skip — already-captured (date, league) cells are
            still drilled into for these specific fixture_ids.

    Returns:
        Dict mapping venue → record count written.

    Raises:
        RuntimeError: If URDI returns zero total records (fail the shard).
    """
    _ = _orch.get_config()  # ensure config is initialized

    # Normalise date: BatchIO passes datetime objects from get_date_range(),
    # but all downstream code (URDI, date filter, partition keys) needs str YYYY-MM-DD.
    if isinstance(date, _orch.datetime):
        date = date.strftime("%Y-%m-%d")

    # venue_override bypasses category lookup when --venues filter is active (sharding)
    venues = venue_override if venue_override is not None else _orch.get_venues_for_asset_groups(asset_groups)

    # Recovery-mode hint: promote redo_all=True when recovery is active so the
    # per-provider per-day pre-flight skip is bypassed (full rationale on the helper).
    redo_all = _promote_redo_all_for_recovery(recovery_fixture_ids=recovery_fixture_ids, redo_all=redo_all)

    # 1. Skip venues not yet launched
    active_venues = [v for v in venues if _orch.is_venue_available(v, date)]

    # --sports-provider: restrict to only this provider's venues (and run the
    # enrichment provider short-circuit where it applies).
    if sports_provider:
        active_venues, provider_result = await _apply_sports_provider_filter(
            date=date,
            asset_groups=asset_groups,
            redo_all=redo_all,
            api_keys=api_keys,
            active_venues=active_venues,
            sports_provider=sports_provider,
            sports_entity_filter=sports_entity_filter,
            season_override=season_override,
        )
        if provider_result is not None:
            return provider_result

    if not active_venues:
        _orch.logger.info("No active venues for date=%s asset_groups=%s", date, asset_groups)
        return {}

    is_sports_run = any(c.upper() in ("SPORTS", "ALL") for c in asset_groups)

    # 1b. Skip-if-exists: check manifest for fresh data (unless --force).
    # The outcome carries the (possibly entity-scoped) core/per-fixture entity
    # lists + which sports entities the manifest says are missing (empty when
    # --force is set).
    preflight = _freshness_preflight(
        date=date,
        asset_groups=asset_groups,
        active_venues=active_venues,
        is_sports_run=is_sports_run,
        sports_entity_filter=sports_entity_filter,
        recovery_fixture_ids=recovery_fixture_ids,
        redo_all=redo_all,
    )
    if preflight.skip:
        return {}
    _sports_missing_entities = preflight.missing_entities
    _sports_core_entities = preflight.core_entities
    _sports_per_fixture_entities = preflight.per_fixture_entities

    # Fast path: if only specific sports entities are missing (instruments done),
    # skip URDI fetch and jump to targeted sports enrichment.
    if _sports_missing_entities and api_keys:
        fast_path_counts = await _enrichment_only_fast_path(
            date=date,
            asset_groups=asset_groups,
            api_keys=api_keys,
            missing_entities=_sports_missing_entities,
            core_entities=_sports_core_entities,
            per_fixture_entities=_sports_per_fixture_entities,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
        )
        if fast_path_counts is not None:
            return fast_path_counts

    _orch.log_event(
        "PROCESSING_STARTED",
        details={"date": date, "asset_groups": asset_groups, "venue_count": len(active_venues)},
    )

    _skip_urdi = _resolve_skip_urdi(sports_entity_filter)

    # 2. Fetch from URDI — sole external API path
    fetch_outcome = await _fetch_urdi_records(
        active_venues=active_venues,
        api_keys=api_keys,
        date=date,
        mode=mode,
        source=source,
        skip_urdi=_skip_urdi,
    )
    records = fetch_outcome.records

    # Per-fixture URDI skip: read fixture IDs from GCS and jump to enrichment.
    # This avoids the URDI fetch + date filter which returns 0 for historical dates.
    if _skip_urdi and sports_entity_filter in _PER_FIXTURE_ENTITIES:
        return await _per_fixture_gcs_fast_path(
            date=date,
            asset_groups=asset_groups,
            api_keys=api_keys,
            sports_entity_filter=sports_entity_filter,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
        )

    # 3. Filter to instruments active on the requested date + enrich.
    records, date_dt, defi_venue_set = _filter_and_enrich_records(
        records=records,
        date=date,
        asset_groups=asset_groups,
    )

    # 4. Handle zero records (honest absence per asset group, or fail the shard).
    if not records:
        return await _handle_zero_records(
            date=date,
            asset_groups=asset_groups,
            active_venues=active_venues,
            mode=mode,
            api_keys=api_keys,
            league_filter=league_filter,
            missing_entities=_sports_missing_entities,
            per_fixture_entities=_sports_per_fixture_entities,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
            sports_entity_filter=sports_entity_filter,
            season_override=season_override,
        )

    # 5. Schema validation — per-record failure isolation (hard_schema_enforcement Phase 2).
    records, validation_failed_venues = _validate_records(records=records, date=date)

    # 6. Write per-venue parquet + catalogue + CSV sample + manifest, then run
    # stages 7 (sports enrichment) + 8 (completeness check + retry).
    return await _write_enrich_and_finalize(
        records=records,
        date=date,
        date_dt=date_dt,
        defi_venue_set=defi_venue_set,
        asset_groups=asset_groups,
        api_keys=api_keys,
        mode=mode,
        source=source,
        active_venues=active_venues,
        league_filter=league_filter,
        sports_entity_filter=sports_entity_filter,
        sports_provider=sports_provider,
        missing_entities=_sports_missing_entities,
        recovery_fixture_ids=recovery_fixture_ids,
        redo_all=redo_all,
        season_override=season_override,
        is_sports_run=is_sports_run,
        validation_failed_venues=validation_failed_venues,
        fetch_outcome=fetch_outcome,
    )


async def _write_enrich_and_finalize(
    *,
    records: list[_orch.InstrumentRecord],
    date: str,
    date_dt: _orch.datetime,
    defi_venue_set: frozenset[str] | None,
    asset_groups: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
    source: str | None,
    active_venues: list[str],
    league_filter: list[str] | None,
    sports_entity_filter: str | None,
    sports_provider: str | None,
    missing_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
    season_override: int | None,
    is_sports_run: bool,
    validation_failed_venues: set[str],
    fetch_outcome: _UrdiFetchOutcome,
) -> dict[str, int]:
    """Stages 6-8 — per-venue writes, sports enrichment, completeness + retry."""
    # 6. Write per-venue parquet + catalogue + CSV sample + manifest.
    write_outcome = _write_all_venues(
        records=records,
        date=date,
        asset_groups=asset_groups,
        league_filter=league_filter,
        non_error_venues=fetch_outcome.non_error_venues,
    )
    counts = write_outcome.counts

    # 7. SPORTS enrichment: fetch and write reference data alongside fixtures.
    await _run_sports_enrichment(
        date=date,
        bucket=write_outcome.bucket,
        counts=counts,
        asset_groups=asset_groups,
        api_keys=api_keys,
        active_venues=active_venues,
        sports_entity_filter=sports_entity_filter,
        sports_provider=sports_provider,
        missing_entities=missing_entities,
        recovery_fixture_ids=recovery_fixture_ids,
        redo_all=redo_all,
        season_override=season_override,
    )

    # 8. Shard completeness check + automatic retry for missing venues.
    return await _completeness_and_retry(
        counts=counts,
        date=date,
        date_dt=date_dt,
        defi_venue_set=defi_venue_set,
        asset_groups=asset_groups,
        api_keys=api_keys,
        mode=mode,
        source=source,
        bucket=write_outcome.bucket,
        sink=write_outcome.sink,
        sampler=write_outcome.sampler,
        active_venues=active_venues,
        non_error_venues=fetch_outcome.non_error_venues,
        validation_failed_venues=validation_failed_venues,
        retryable_venues=fetch_outcome.retryable_venues,
        is_sports_run=is_sports_run,
        sports_entity_filter=sports_entity_filter,
        recovery_fixture_ids=recovery_fixture_ids,
    )
