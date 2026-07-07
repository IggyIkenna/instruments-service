"""Instruments engine orchestrator — the entire processing logic of the service.

IMPORT CONTRACT
---------------
This module imports from:
  1. unified_trading_library (UTL) — all infrastructure, framework, validation, storage
  2. unified_api_contracts (T0) — domain types (venue-agnostic enums)

No direct imports from UEI, UCI, UMI, UDC, UCC. If something is needed from
those libraries, it must come through UTL's re-exported surface.

PROCESS FLOW
------------
For each date:
  1. Skip venues whose discovery API has no data on that date (UAC VenueMapping.get_instrument_discovery_start)
  2. Fetch InstrumentRecord[] from URDI via urdi_reference_provider
  3. Filter to instruments active on the requested date (available_from_datetime ≤ date ≤ available_to_datetime)
  4. Fail shard if zero records after filtering
  5. Validate with DomainValidationService("instruments") (UTL)
  6. Write per-venue parquet + catalogue record (UTL get_data_sink / ManifestWriter)
  7. Drop CSV sample in dev mode (UTL create_sampling_service)
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import re
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from typing import Protocol, cast

import pandas as pd
from unified_api_contracts import (
    _SPORTS_ENTITY_TO_PIPELINE_MODE as _ENTITY_NAME_TO_PIPELINE_MODE,
)
from unified_api_contracts import (
    BUNDESLIGA_TEAM_ALIASES,
    CANONICAL_TO_ODDS_API_BUNDESLIGA,
    CANONICAL_TO_ODDS_API_EPL,
    CANONICAL_TO_UNDERSTAT_EPL,
    DEX_VENUE_KEYWORDS,
    EPL_TEAM_ALIASES,
    EmptyConfirmedReason,
    FetchErrorSignal,
    FetchEvidence,
    PipelineMode,
    RecordFailedReason,
    VenueMapping,
    classify_venue_error,
    get_prediction_leagues,
    pipeline_mode_for_sports_entity,
)
from unified_api_contracts.internal import InstrumentRecord, PreflightSkipReason, validate_instrument_records
from unified_api_contracts.predictions import (
    CANONICAL_GROUP_METADATA,
    CanonicalQuestionGroup,
    classify_kalshi_to_canonical_group,
    classify_polymarket_to_canonical_group,
)
from unified_api_contracts.registry import get_supported_chains_for_protocol, is_league_entity_covered
from unified_api_contracts.registry.source_data_latency import SFI_DATA_LAG_P95_SECONDS
from unified_api_contracts.sports import (
    FIXTURES_OUTCOMES,
    FIXTURES_SCHEDULE,
    FOOTYSTATS_HISTORICAL_SEASON_IDS,
    SOCCER_FOOTBALL_INFO_IDS,
    footystats_season_status_for_day,
    get_all_prediction_league_ids,
    get_entity_league_coverage,
    get_expected_leagues_for_source,
    get_expected_team_count_for_league,
    get_league_by_api_football_id,
    get_league_fixture_calendar,
    get_leagues_needing_refresh,
    get_provider_league_id,
    get_source_coverage_start,
    is_any_league_refresh_date,
    is_in_known_gap,
    is_transfer_window_open,
)
from unified_api_contracts.sports import (
    canonicalize_league_id as _uac_canonicalize_league_id,
)
from unified_trading_library import (
    CaptureStatus,
    DataSink,
    DomainValidationService,
    EmissionDecision,
    InstrumentsWriteGate,
    ManifestRow,
    ManifestWriter,
    SamplingService,
    assert_available_at_present,
    check_shard_freshness,
    classify_and_emit_error,
    create_sampling_service,
    emit_preflight_skip,
    get_data_sink,
    get_storage_client,
    log_event,
    publish_with_policy,
    read_availability_index,
    resolve_bucket_name,
    stamp_available_at_explicit,
)
from unified_trading_library import unified_config as _uc
from unified_trading_library.fixtures import extract_match_lifecycle  # noqa: qg-deep-import

from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets
from instruments_service.engine.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.reference_data.adapters.defi._solana_utils import SolanaCacheSession, fill_solana_cache
from instruments_service.reference_data.adapters.sports import create_sports_reference_adapter
from instruments_service.reference_data.adapters.sports.adapters.api_football_reference import (
    _last_completed_fixture_ids as _urdi_completed_fixture_ids,
)
from instruments_service.reference_data.adapters.sports.adapters.footystats import FootystatsAdapter
from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    detect_match_end_time as _sfi_detect_match_end_time,
)
from instruments_service.reference_data.adapters.tradfi.databento import (
    is_non_trading_day,
    non_trading_day_reason,
)
from instruments_service.reference_data.adapters.tradfi.futures_factory import (
    build_futures_contracts,
)
from instruments_service.reference_data.utils.evm_creation_resolver import EvmCacheSession

logger = logging.getLogger(__name__)

_SERVICE_NAME: str = "instruments-service"


class _ModelDumpable(Protocol):
    """Protocol for objects that expose a Pydantic-style .model_dump() method."""

    def model_dump(self) -> dict[str, object]: ...


# v8 pipeline_mode SSOT (Phase 4.INSTRUMENTS — explicit pipeline_mode= at every
# record_* callsite). Closed-set mapping from sports/prediction data_type to
# the external source the instruments-service catalog refresh pulls from. The
# source determines which UAC ``PipelineMode.BATCH_<SOURCE>`` value tags the
# manifest row. Used by helper ``_pipeline_mode_for_sports_data_type``.
#
# Per CLAUDE.md "Live = batch — same data, same fields, same timing semantics,
# different sources OK" rule + writegate Phase 4.DEFAULT-REMOVAL prerequisite:
# every record_* callsite MUST pass an explicit pipeline_mode= matching the
# source that actually served the catalog refresh for that data_type. Implicit
# default ``""`` is being removed once every consumer ships its explicit value
# (plan ``gcs_migration_bundle_pipeline_mode_2026_05_08`` body line 360).
#
# **Footystats** (finding ``footystats_pipeline_mode_gap_2026_05_12.md`` —
# Q2=(A) operator-approved 2026-05-12, UAC enum extension shipped at
# UAC@52d289c): footystats-served catalog rows previously tagged with
# ``BATCH_API_FOOTBALL`` as a documented workaround. With the canonical
# ``BATCH_FOOTYSTATS`` member now present in UAC ``PipelineMode``,
# footystats-served data_types (``PREDICTIONS`` / ``MATCHES`` / ``ODDS``) are
# stamped with ``BATCH_FOOTYSTATS``. ODDS removal reversed 2026-06-27 (operator
# decision): footystats ODDS are pre-match snapshot reference data that stays in IS.
_SPORTS_DATA_TYPE_TO_PIPELINE_MODE: dict[str, PipelineMode] = {
    # api_football catalog (FIXTURES + per-fixture entities + reference data)
    "FIXTURES": PipelineMode.BATCH_API_FOOTBALL,
    "INJURIES": PipelineMode.BATCH_API_FOOTBALL,
    "FIXTURE_LINEUPS": PipelineMode.BATCH_API_FOOTBALL,
    "FIXTURE_EVENTS": PipelineMode.BATCH_API_FOOTBALL,
    "FIXTURE_STATS": PipelineMode.BATCH_API_FOOTBALL,
    "PLAYER_STATS": PipelineMode.BATCH_API_FOOTBALL,
    "TEAMS": PipelineMode.BATCH_API_FOOTBALL,
    "STANDINGS": PipelineMode.BATCH_API_FOOTBALL,
    "LEAGUES": PipelineMode.BATCH_API_FOOTBALL,
    "VENUES": PipelineMode.BATCH_API_FOOTBALL,
    # footystats catalog — canonical ``BATCH_FOOTYSTATS`` per
    # footystats_pipeline_mode_gap_2026_05_12.md Q2=(A) flip (UAC@52d289c
    # shipped the enum extension; instruments-service flipped from the
    # workaround stamp on 2026-05-12).
    "PREDICTIONS": PipelineMode.BATCH_FOOTYSTATS,
    "MATCHES": PipelineMode.BATCH_FOOTYSTATS,
    # ODDS slice — footystats pre-match snapshot odds; BATCH_FOOTYSTATS to match
    # source='footystats'. Removal reversed 2026-06-27 (operator decision #6 REVERSED).
    "ODDS": PipelineMode.BATCH_FOOTYSTATS,
    # understat catalog
    "XG": PipelineMode.BATCH_UNDERSTAT,
    # transfermarkt catalog
    "PLAYER_VALUES": PipelineMode.BATCH_TRANSFERMARKT,
    # soccer_football_info (SFI) catalog
    "SFI_PROGRESSIVE_STATS": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
    # open_meteo catalog
    "WEATHER": PipelineMode.BATCH_OPEN_METEO,
}


# ---------------------------------------------------------------------------
# Write-gate: fail-loud guard against §5 data-crimes at write time.
# ``warn`` mode (default) emits DATA_ALIGNMENT_VIOLATION and proceeds; ``strict``
# raises TimestampAlignmentError which per-shard try/except should catch and
# route to manifest.record_failed. Flip to ``strict`` once warn-mode volume
# baselines clean across sports adapters (see
# ``plans/active/instruments_service_write_gate_validation_2026_04_22.md``).
# ---------------------------------------------------------------------------
_WRITE_GATE = InstrumentsWriteGate(mode="strict")


# ---------------------------------------------------------------------------
# CanonicalFixture → SPORTS_FIXTURES SchemaContract flattening
# ---------------------------------------------------------------------------
# CanonicalFixture (UAC) carries league / home_team / away_team / venue as
# nested Pydantic structs. ``pd.DataFrame([fx.model_dump() for fx in …])``
# preserves those structs as parquet struct cells — that's the LEGACY schema
# scattered across pre-2024 partitions and (regression!) some 2026 days.
#
# The on-disk SSOT is ``SPORTS_FIXTURES`` in
# ``unified_api_contracts.internal.schemas._sports_match_contracts``: 32
# flat columns with ``af_*`` prefixed identifiers. ``_flatten_canonical_fixture_for_disk``
# bridges the in-memory canonical model to the on-disk contract.
#
# Audit (2026-04-28): 594 LEGACY days remain on disk; 112 of them are 2026
# dates produced by the legacy writer. Plan:
# ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.md``.

# Provider names → API-Football logo URL → numeric ID. CanonicalLeague /
# CanonicalTeam / CanonicalVenue carry ``api_football_id`` directly when
# available; we fall back to logo_url parsing for older normalizer outputs.
_AF_LOGO_RE = re.compile(r"/(?:leagues|teams)/(\d+)\.png")


# Q5/Q6 lifecycle column names (fixture-schedule-split Phase 3). Defined once so
# the flatten output ALWAYS carries them (default None) — a stable column set is
# required by the SPORTS_FIXTURES contract + the homogeneous-DataFrame assembly.
_Q5_SCHEDULE_COLUMNS: tuple[str, ...] = (
    "halftime_start_time",
    "halftime_end_time",
    "extra_time_first_half_start_time",
    "extra_time_first_half_end_time",
    "extra_time_second_half_start_time",
    "extra_time_second_half_end_time",
    "penalty_shootout_start_time",
    "penalty_shootout_end_time",
    "whistle_full_time_at",
)
_Q6_OUTCOME_COLUMNS: tuple[str, ...] = (
    "home_score_regulation",
    "away_score_regulation",
    "home_score_after_extra_time",
    "away_score_after_extra_time",
    "home_score_after_penalty_shootout",
    "away_score_after_penalty_shootout",
    "home_penalty_shootout_score",
    "away_penalty_shootout_score",
    "went_to_extra_time",
    "went_to_penalties",
    "match_result",
)


# Venue launch dates SSOT: UAC VenueMapping (canonical PROTOCOL-CHAIN format).
# No local copy — read from VenueMapping at module load. We resolve via
# ``get_instrument_discovery_start(venue)`` rather than reading
# ``venue_start_dates`` directly so per-venue discovery-API coverage overrides
# (HYPERLIQUID 2023-11-01 vs market-data 2023-04-15 — see UAC docstring) are
# respected. Reference incident 2026-05-05: 200 phantom HYPERLIQUID dates.
_VENUE_MAPPING = VenueMapping()


# ---------------------------------------------------------------------------
# DeFi universe cache for batch optimization
# ---------------------------------------------------------------------------
# DeFi instruments are monotonically growing (immutable smart contracts, never
# deleted). The full instrument universe (with available_from_datetime) can be
# fetched once and reused for every date in a batch range. Each date just
# filters available_from <= date — no per-date API call needed.
#
# The cache is populated on the first DeFi fetch in a batch run and reused
# for all subsequent dates. Call clear_defi_universe_cache() between runs.
_defi_universe_cache: list[InstrumentRecord] | None = None
_defi_universe_retryable: list[str] = []

# Sports reference core entity caches — leagues/teams/standings are the same
# across all dates within a batch run. Fetched once, written to every date partition.
_cached_leagues_df: pd.DataFrame | None = None
_cached_teams_df: pd.DataFrame | None = None
_cached_standings_df: pd.DataFrame | None = None
_cached_prediction_league_ids: list[int] = []


# ---------------------------------------------------------------------------
# Honest-coverage helpers (Phase B, plan: honest_coverage_metrics_2026_04_19)
# ---------------------------------------------------------------------------
# Adapters that hit event-driven providers (Polymarket, Kalshi, FootyStats,
# Understat, SFI fixtures) MUST distinguish "we never tried" from "we tried
# and there was nothing".  These helpers wrap the pre-flight skip + venue
# error classification so per-shard call sites stay compact.


# Programming errors (TypeError, KeyError, etc.) propagate — fail the shard


# ---------------------------------------------------------------------------
# sports_reference path helpers — v9 canonical layout
#
# Canonical target (sports_manifest_canonicalisation_2026_06_01.md §C E2):
#   sports_reference/by_date/day={D}/pipeline_mode={PM}/entity={E}/[league={L}/]{fname}
#
# The DataSink._build_partition_path sorts partition keys alphabetically, so
# ``pipeline_mode`` (p) would sort AFTER ``entity`` (e) and ``league`` (l) if
# included in the partition dict — producing the wrong path order.  The correct
# approach: embed ``day=`` and ``pipeline_mode=`` in the sink prefix; only put
# ``entity=`` and ``league=`` in the partition dict (they sort correctly: e < l).
#
# Read helpers probe canonical first, then fall back to the legacy path (no
# pipeline_mode= segment) until Phase E8 removes pre-migration objects.
# ---------------------------------------------------------------------------

# Entity-name → PipelineMode for every entity written under sports_reference/by_date.
# SSOT is UAC ``pipeline_mode_for_sports_entity`` (imported above).
# ``_ENTITY_NAME_TO_PIPELINE_MODE`` is aliased from UAC at the top of this module so the
# IS test suite can import it directly for closed-set parity assertions.


# ---------------------------------------------------------------------------
# END sports_reference path helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Transfermarkt + SFI per-league mapping caches
# (transfermarkt_sfi_team_mapping_cache_and_drift_detection_2026_04_22)
#
# Backfill VMs rerun ``_fetch_transfermarkt_data`` / ``_fetch_sfi_data`` for
# every trigger date inside a window. Without these caches every re-fire hits
# the paid APIs to re-fetch the same ``(league, season)`` roster.  Both helpers
# write a slow-moving parquet under ``sports_reference/mappings/`` — reused by
# features-sports-service's ``read_transfermarkt_team_mapping`` /
# ``read_sfi_league_mapping`` readers.
# ---------------------------------------------------------------------------


_TRANSFERMARKT_CACHE_STALENESS_DAYS = 7
_SFI_CACHE_STALENESS_HOURS = 24


# ---------------------------------------------------------------------------
# Transfermarkt master/ + snapshots/ write-path helpers (ITEM 6a + 6b)
#
# ML training needs point-in-time squad values to avoid lookahead bias:
#   - ``master/entity={entity}/master.parquet`` — accumulating append-only union
#     (teams, team_mapping, player_values); each run merges new rows then deduplicates
#     on the natural key before writing back — semantics: monotonically-growing truth.
#   - ``snapshots/entity=player_values/season={Y}/trigger={T}/player_values.parquet``
#     — point-in-time snapshot keyed by (season, trigger date) so feature pipelines
#     can reconstruct the exact roster visible on a given trigger date without risk
#     of lookahead bias.
#
# Both use the cloud-agnostic ``get_storage_client()`` (upload_bytes / download_bytes)
# and ``resolve_bucket_name`` — never inline gs:// URIs (QG STEP 5.69).
# ---------------------------------------------------------------------------

# ── cohesion submodules: import + re-export (public surface unchanged) ──────
# The orchestrator package is ONE logical namespace split across cohesion
# modules (see _pkg_ref.py); submodules intentionally resolve shared symbols
# through this package module, so patching/monkeypatching at
# `instruments_service.engine.orchestrator.<name>` behaves exactly as before the split.
# pyright: reportPrivateUsage=false, reportImportCycles=false
from instruments_service.engine.orchestrator import catalogue as catalogue
from instruments_service.engine.orchestrator import defi as defi
from instruments_service.engine.orchestrator import failure as failure
from instruments_service.engine.orchestrator import footystats as footystats
from instruments_service.engine.orchestrator import prediction as prediction
from instruments_service.engine.orchestrator import process as process
from instruments_service.engine.orchestrator import sfi as sfi
from instruments_service.engine.orchestrator import sink as sink
from instruments_service.engine.orchestrator import sports as sports
from instruments_service.engine.orchestrator import sports_fixtures as sports_fixtures
from instruments_service.engine.orchestrator import sports_reference as sports_reference
from instruments_service.engine.orchestrator import transfermarkt as transfermarkt
from instruments_service.engine.orchestrator import understat as understat
from instruments_service.engine.orchestrator import venue_core as venue_core
from instruments_service.engine.orchestrator import weather as weather
from instruments_service.engine.orchestrator import writers as writers
from instruments_service.engine.orchestrator.catalogue import (
    _check_emission_policy as _check_emission_policy,
)
from instruments_service.engine.orchestrator.catalogue import (
    _get_instruments_bucket as _get_instruments_bucket,
)
from instruments_service.engine.orchestrator.catalogue import (
    _write_catalogue_record as _write_catalogue_record,
)
from instruments_service.engine.orchestrator.catalogue import (
    resolve_instruments_store_kind as resolve_instruments_store_kind,
)
from instruments_service.engine.orchestrator.defi import (
    _SOLANA_DEFI_VENUES as _SOLANA_DEFI_VENUES,
)
from instruments_service.engine.orchestrator.defi import (
    _STATIC_DEFI_VENUES as _STATIC_DEFI_VENUES,
)
from instruments_service.engine.orchestrator.defi import (
    _SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX as _SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX,
)
from instruments_service.engine.orchestrator.defi import (
    _build_defi_venues as _build_defi_venues,
)
from instruments_service.engine.orchestrator.defi import (
    _count_per_venue as _count_per_venue,
)
from instruments_service.engine.orchestrator.defi import (
    _enforce_defi_monotonicity as _enforce_defi_monotonicity,
)
from instruments_service.engine.orchestrator.defi import (
    _get_defi_manifest_high_watermarks as _get_defi_manifest_high_watermarks,
)
from instruments_service.engine.orchestrator.defi import (
    _get_or_fetch_defi_universe as _get_or_fetch_defi_universe,
)
from instruments_service.engine.orchestrator.defi import (
    _normalize_wrapped_token as _normalize_wrapped_token,
)
from instruments_service.engine.orchestrator.defi import (
    _retry_regressed_venues as _retry_regressed_venues,
)
from instruments_service.engine.orchestrator.defi import (
    clear_defi_universe_cache as clear_defi_universe_cache,
)
from instruments_service.engine.orchestrator.defi import (
    fill_solana_creation_cache as fill_solana_creation_cache,
)
from instruments_service.engine.orchestrator.defi import (
    filter_defi_instruments_by_relevance as filter_defi_instruments_by_relevance,
)
from instruments_service.engine.orchestrator.failure import (
    _classify_adapter_failure as _classify_adapter_failure,
)
from instruments_service.engine.orchestrator.footystats import (
    _fetch_footystats_matches as _fetch_footystats_matches,
)
from instruments_service.engine.orchestrator.footystats import (
    _fetch_footystats_odds as _fetch_footystats_odds,
)
from instruments_service.engine.orchestrator.footystats import (
    _fetch_footystats_predictions as _fetch_footystats_predictions,
)
from instruments_service.engine.orchestrator.footystats import (
    _load_scheduled_footystats_fixture_map as _load_scheduled_footystats_fixture_map,
)
from instruments_service.engine.orchestrator.footystats import (
    _validate_predictions_null_rates as _validate_predictions_null_rates,
)
from instruments_service.engine.orchestrator.prediction import (
    _compute_prediction_shards as _compute_prediction_shards,
)
from instruments_service.engine.orchestrator.prediction import (
    _extract_prediction_canonical_group as _extract_prediction_canonical_group,
)
from instruments_service.engine.orchestrator.process import (
    process_instruments as process_instruments,
)
from instruments_service.engine.orchestrator.sfi import (
    _fetch_sfi_data as _fetch_sfi_data,
)
from instruments_service.engine.orchestrator.sfi import (
    _read_sfi_league_mapping as _read_sfi_league_mapping,
)
from instruments_service.engine.orchestrator.sfi import (
    _sfi_mapping_blob_path as _sfi_mapping_blob_path,
)
from instruments_service.engine.orchestrator.sfi import (
    _write_sfi_league_mapping as _write_sfi_league_mapping,
)
from instruments_service.engine.orchestrator.sink import (
    _coerce_adapter_output as _coerce_adapter_output,
)
from instruments_service.engine.orchestrator.sink import (
    _gated_sink_write as _gated_sink_write,
)
from instruments_service.engine.orchestrator.sports import (
    _af_id_from_canonical as _af_id_from_canonical,
)
from instruments_service.engine.orchestrator.sports import (
    _canonical_league_id as _canonical_league_id,
)
from instruments_service.engine.orchestrator.sports import (
    _empty_lifecycle_columns as _empty_lifecycle_columns,
)
from instruments_service.engine.orchestrator.sports import (
    _flatten_canonical_fixture_for_disk as _flatten_canonical_fixture_for_disk,
)
from instruments_service.engine.orchestrator.sports import (
    _is_in_canonical_write_universe as _is_in_canonical_write_universe,
)
from instruments_service.engine.orchestrator.sports import (
    _lifecycle_columns_from_af_response as _lifecycle_columns_from_af_response,
)
from instruments_service.engine.orchestrator.sports import (
    _pipeline_mode_for_sports_data_type as _pipeline_mode_for_sports_data_type,
)
from instruments_service.engine.orchestrator.sports import (
    _set_cached_leagues as _set_cached_leagues,
)
from instruments_service.engine.orchestrator.sports import (
    _set_cached_standings as _set_cached_standings,
)
from instruments_service.engine.orchestrator.sports import (
    _set_cached_teams as _set_cached_teams,
)
from instruments_service.engine.orchestrator.sports import (
    _should_skip_date_for_per_league as _should_skip_date_for_per_league,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _build_fixture_league_map_from_gcs as _build_fixture_league_map_from_gcs,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _merge_with_existing_per_league_parquet as _merge_with_existing_per_league_parquet,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _per_league_fixtures_data_unchanged as _per_league_fixtures_data_unchanged,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _read_existing_per_league_fixture_ids as _read_existing_per_league_fixture_ids,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _read_fixture_ids_from_gcs as _read_fixture_ids_from_gcs,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _resolve_sports_ref_blob as _resolve_sports_ref_blob,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _sports_ref_canonical_blob_path as _sports_ref_canonical_blob_path,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _sports_ref_legacy_blob_path as _sports_ref_legacy_blob_path,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _sports_ref_pm as _sports_ref_pm,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _sports_ref_sink_for as _sports_ref_sink_for,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _sports_ref_source as _sports_ref_source,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _write_fixture_mapping as _write_fixture_mapping,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _write_fixtures_per_league as _write_fixtures_per_league,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _write_team_mapping as _write_team_mapping,
)
from instruments_service.engine.orchestrator.sports_reference import (
    _fetch_sports_reference_data as _fetch_sports_reference_data,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _cache_is_fresh as _cache_is_fresh,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _fetch_transfermarkt_data as _fetch_transfermarkt_data,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _master_blob_path as _master_blob_path,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _maybe_emit_drift_anomaly as _maybe_emit_drift_anomaly,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _read_transfermarkt_team_mapping as _read_transfermarkt_team_mapping,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _snapshot_blob_path_player_values as _snapshot_blob_path_player_values,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _transfermarkt_mapping_blob_path as _transfermarkt_mapping_blob_path,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _write_master_append as _write_master_append,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _write_snapshot_player_values as _write_snapshot_player_values,
)
from instruments_service.engine.orchestrator.transfermarkt import (
    _write_transfermarkt_team_mapping as _write_transfermarkt_team_mapping,
)
from instruments_service.engine.orchestrator.understat import (
    _fetch_understat_xg as _fetch_understat_xg,
)
from instruments_service.engine.orchestrator.understat import (
    _run_understat_shots_date as _run_understat_shots_date,
)
from instruments_service.engine.orchestrator.venue_core import (
    _SPORTS_PROVIDER_VENUES as _SPORTS_PROVIDER_VENUES,
)
from instruments_service.engine.orchestrator.venue_core import (
    _TRADFI_NON_VENUE_KEYS as _TRADFI_NON_VENUE_KEYS,
)
from instruments_service.engine.orchestrator.venue_core import (
    _VENUE_ADAPTER_EPOCH as _VENUE_ADAPTER_EPOCH,
)
from instruments_service.engine.orchestrator.venue_core import (
    _get_venue_epoch as _get_venue_epoch,
)
from instruments_service.engine.orchestrator.venue_core import (
    _should_skip_shard as _should_skip_shard,
)
from instruments_service.engine.orchestrator.venue_core import (
    earliest_venue_date as earliest_venue_date,
)
from instruments_service.engine.orchestrator.venue_core import (
    expand_cefi_tardis_endpoints as expand_cefi_tardis_endpoints,
)
from instruments_service.engine.orchestrator.venue_core import (
    filter_instruments_by_date as filter_instruments_by_date,
)
from instruments_service.engine.orchestrator.venue_core import (
    get_venues_for_asset_groups as get_venues_for_asset_groups,
)
from instruments_service.engine.orchestrator.venue_core import (
    is_venue_available as is_venue_available,
)
from instruments_service.engine.orchestrator.venue_core import (
    reject_junk_instruments as reject_junk_instruments,
)
from instruments_service.engine.orchestrator.weather import (
    _extract_fixture_venue_ids as _extract_fixture_venue_ids,
)
from instruments_service.engine.orchestrator.weather import (
    _fetch_weather_data as _fetch_weather_data,
)
from instruments_service.engine.orchestrator.weather import (
    _load_venue_coordinates as _load_venue_coordinates,
)
from instruments_service.engine.orchestrator.writers import (
    _build_market_lifecycle_df as _build_market_lifecycle_df,
)
from instruments_service.engine.orchestrator.writers import (
    _canonical_manifest_venue_chain as _canonical_manifest_venue_chain,
)
from instruments_service.engine.orchestrator.writers import (
    _split_by_instrument_type as _split_by_instrument_type,
)
from instruments_service.engine.orchestrator.writers import (
    _write_futures_contracts as _write_futures_contracts,
)
from instruments_service.engine.orchestrator.writers import (
    _write_market_lifecycle as _write_market_lifecycle,
)
from instruments_service.engine.orchestrator.writers import (
    _write_venue as _write_venue,
)
from instruments_service.engine.orchestrator.writers import (
    _write_venues_from_teams as _write_venues_from_teams,
)

# Computed after submodule import: depends on defi._build_defi_venues.
_DEFI_VENUES: list[str] = _build_defi_venues()

__all__ = [
    "BUNDESLIGA_TEAM_ALIASES",
    "CANONICAL_GROUP_METADATA",
    "CANONICAL_TO_ODDS_API_BUNDESLIGA",
    "CANONICAL_TO_ODDS_API_EPL",
    "CANONICAL_TO_UNDERSTAT_EPL",
    "DEX_VENUE_KEYWORDS",
    "EPL_TEAM_ALIASES",
    "FIXTURES_OUTCOMES",
    "FIXTURES_SCHEDULE",
    "FOOTYSTATS_HISTORICAL_SEASON_IDS",
    "SFI_DATA_LAG_P95_SECONDS",
    "SOCCER_FOOTBALL_INFO_IDS",
    "UTC",
    "_AF_LOGO_RE",
    "_DEFI_VENUES",
    "_ENTITY_NAME_TO_PIPELINE_MODE",
    "_Q5_SCHEDULE_COLUMNS",
    "_Q6_OUTCOME_COLUMNS",
    "_SERVICE_NAME",
    "_SFI_CACHE_STALENESS_HOURS",
    "_SOLANA_DEFI_VENUES",
    "_SPORTS_DATA_TYPE_TO_PIPELINE_MODE",
    "_SPORTS_PROVIDER_VENUES",
    "_STATIC_DEFI_VENUES",
    "_SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX",
    "_TRADFI_NON_VENUE_KEYS",
    "_TRANSFERMARKT_CACHE_STALENESS_DAYS",
    "_VENUE_ADAPTER_EPOCH",
    "_VENUE_MAPPING",
    "_WRITE_GATE",
    "Awaitable",
    "Callable",
    "CanonicalQuestionGroup",
    "CaptureStatus",
    "DataSink",
    "DomainValidationService",
    "EmissionDecision",
    "EmptyConfirmedReason",
    "EvmCacheSession",
    "FootystatsAdapter",
    "InstrumentRecord",
    "InstrumentsWriteGate",
    "ManifestRow",
    "ManifestWriter",
    "PipelineMode",
    "PreflightSkipReason",
    "Protocol",
    "RecordFailedReason",
    "SamplingService",
    "Sequence",
    "SolanaCacheSession",
    "VenueMapping",
    "_ModelDumpable",
    "_af_id_from_canonical",
    "_build_defi_venues",
    "_build_fixture_league_map_from_gcs",
    "_build_market_lifecycle_df",
    "_cache_is_fresh",
    "_cached_leagues_df",
    "_cached_prediction_league_ids",
    "_cached_standings_df",
    "_cached_teams_df",
    "_canonical_league_id",
    "_canonical_manifest_venue_chain",
    "_check_emission_policy",
    "_classify_adapter_failure",
    "_coerce_adapter_output",
    "_compute_prediction_shards",
    "_count_per_venue",
    "_defi_universe_cache",
    "_defi_universe_retryable",
    "_empty_lifecycle_columns",
    "_enforce_defi_monotonicity",
    "_extract_fixture_venue_ids",
    "_extract_prediction_canonical_group",
    "_fetch_footystats_matches",
    "_fetch_footystats_predictions",
    "_fetch_sfi_data",
    "_fetch_sports_reference_data",
    "_fetch_transfermarkt_data",
    "_fetch_understat_xg",
    "_fetch_weather_data",
    "_flatten_canonical_fixture_for_disk",
    "_gated_sink_write",
    "_get_defi_manifest_high_watermarks",
    "_get_instruments_bucket",
    "_get_or_fetch_defi_universe",
    "_get_venue_epoch",
    "_is_in_canonical_write_universe",
    "_lifecycle_columns_from_af_response",
    "_load_venue_coordinates",
    "_master_blob_path",
    "_maybe_emit_drift_anomaly",
    "_merge_with_existing_per_league_parquet",
    "_normalize_wrapped_token",
    "_per_league_fixtures_data_unchanged",
    "_pipeline_mode_for_sports_data_type",
    "_read_existing_per_league_fixture_ids",
    "_read_fixture_ids_from_gcs",
    "_read_sfi_league_mapping",
    "_read_transfermarkt_team_mapping",
    "_resolve_sports_ref_blob",
    "_retry_regressed_venues",
    "_run_understat_shots_date",
    "_set_cached_leagues",
    "_set_cached_standings",
    "_set_cached_teams",
    "_sfi_detect_match_end_time",
    "_sfi_mapping_blob_path",
    "_should_skip_date_for_per_league",
    "_should_skip_shard",
    "_snapshot_blob_path_player_values",
    "_split_by_instrument_type",
    "_sports_ref_canonical_blob_path",
    "_sports_ref_legacy_blob_path",
    "_sports_ref_pm",
    "_sports_ref_sink_for",
    "_sports_ref_source",
    "_transfermarkt_mapping_blob_path",
    "_uac_canonicalize_league_id",
    "_uc",
    "_urdi_completed_fixture_ids",
    "_validate_predictions_null_rates",
    "_write_catalogue_record",
    "_write_fixture_mapping",
    "_write_fixtures_per_league",
    "_write_futures_contracts",
    "_write_market_lifecycle",
    "_write_master_append",
    "_write_sfi_league_mapping",
    "_write_snapshot_player_values",
    "_write_team_mapping",
    "_write_transfermarkt_team_mapping",
    "_write_venue",
    "_write_venues_from_teams",
    "assert_available_at_present",
    "asyncio",
    "build_futures_contracts",
    "cast",
    "catalogue",
    "check_shard_freshness",
    "classify_and_emit_error",
    "classify_kalshi_to_canonical_group",
    "classify_polymarket_to_canonical_group",
    "classify_venue_error",
    "clear_defi_universe_cache",
    "contextlib",
    "create_sampling_service",
    "create_sports_reference_adapter",
    "date_type",
    "datetime",
    "defi",
    "earliest_venue_date",
    "emit_preflight_skip",
    "expand_cefi_tardis_endpoints",
    "extract_match_lifecycle",
    "failure",
    "fetch_instruments_for_all_venues",
    "fill_solana_cache",
    "fill_solana_creation_cache",
    "filter_defi_instruments_by_relevance",
    "filter_instruments_by_date",
    "footystats",
    "footystats_season_status_for_day",
    "get_all_prediction_league_ids",
    "get_config",
    "get_data_sink",
    "get_defi_major_assets",
    "get_entity_league_coverage",
    "get_expected_leagues_for_source",
    "get_expected_team_count_for_league",
    "get_league_by_api_football_id",
    "get_league_fixture_calendar",
    "get_leagues_needing_refresh",
    "get_prediction_leagues",
    "get_provider_league_id",
    "get_source_coverage_start",
    "get_storage_client",
    "get_supported_chains_for_protocol",
    "get_venues_for_asset_groups",
    "io",
    "is_any_league_refresh_date",
    "is_in_known_gap",
    "is_league_entity_covered",
    "is_non_trading_day",
    "is_transfer_window_open",
    "is_venue_available",
    "json",
    "log_event",
    "logger",
    "logging",
    "non_trading_day_reason",
    "pd",
    "pipeline_mode_for_sports_entity",
    "prediction",
    "process",
    "process_instruments",
    "publish_with_policy",
    "re",
    "read_availability_index",
    "resolve_bucket_name",
    "resolve_instruments_store_kind",
    "sfi",
    "sink",
    "sports",
    "sports_fixtures",
    "sports_reference",
    "stamp_available_at_explicit",
    "tempfile",
    "timedelta",
    "transfermarkt",
    "understat",
    "validate_instrument_records",
    "venue_core",
    "weather",
    "writers",
]
