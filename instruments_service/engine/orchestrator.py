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
from datetime import UTC, datetime, timedelta
from datetime import date as date_type

import pandas as pd
from unified_api_contracts import (
    BUNDESLIGA_TEAM_ALIASES,
    CANONICAL_TO_ODDS_API_BUNDESLIGA,
    CANONICAL_TO_ODDS_API_EPL,
    CANONICAL_TO_UNDERSTAT_EPL,
    DEX_VENUE_KEYWORDS,
    EPL_TEAM_ALIASES,
    EmptyConfirmedReason,
    PipelineMode,
    VenueMapping,
    classify_venue_error,
    get_prediction_leagues,
)
from unified_api_contracts.internal import InstrumentRecord, validate_instrument_records
from unified_api_contracts.predictions import (
    CanonicalQuestionGroup,
    classify_kalshi_to_canonical_group,
    classify_polymarket_to_canonical_group,
)
from unified_api_contracts.registry import get_supported_chains_for_protocol
from unified_api_contracts.registry.source_data_latency import SFI_DATA_LAG_P95_SECONDS
from unified_api_contracts.sports import (
    FOOTYSTATS_HISTORICAL_SEASON_IDS,
    SOCCER_FOOTBALL_INFO_IDS,
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
    check_shard_freshness,
    classify_and_emit_error,
    create_sampling_service,
    get_data_sink,
    get_storage_client,
    get_write_bucket_name,
    log_event,
    publish_with_policy,
    read_availability_index,
    stamp_available_at_explicit,
)
from unified_trading_library import unified_config as _uc

from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets
from instruments_service.engine.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.reference_data.adapters.defi._solana_utils import SolanaCacheSession, fill_solana_cache
from instruments_service.reference_data.adapters.sports import create_sports_reference_adapter
from instruments_service.reference_data.adapters.sports.adapters.api_football_reference import (
    _last_completed_fixture_ids as _urdi_completed_fixture_ids,
)
from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    detect_match_end_time as _sfi_detect_match_end_time,
)
from instruments_service.reference_data.adapters.tradfi.databento import (
    is_non_trading_day,
    non_trading_day_reason,
)
from instruments_service.reference_data.utils.evm_creation_resolver import EvmCacheSession

logger = logging.getLogger(__name__)

_SERVICE_NAME: str = "instruments-service"


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
# footystats-served data_types (``PREDICTIONS`` / ``MATCHES``) are stamped
# with their canonical source. ``ODDS`` still tags ``BATCH_ODDS_API``
# because the footystats odds adapter wraps the ``odds_api`` source per
# UAC SOURCE_PRIORITY for ``ODDS_SNAPSHOT`` / ``ODDS_MOVEMENT`` /
# ``ARBITRAGE``.
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
    # ODDS slice — UAC SOURCE_PRIORITY top entry for the odds-snapshot slice
    # is ``odds_api``; footystats odds adapter tagged with BATCH_ODDS_API.
    "ODDS": PipelineMode.BATCH_ODDS_API,
    # understat catalog
    "XG": PipelineMode.BATCH_UNDERSTAT,
    # transfermarkt catalog
    "TRANSFERMARKT_LEAGUES": PipelineMode.BATCH_TRANSFERMARKT,
    "PLAYER_VALUES": PipelineMode.BATCH_TRANSFERMARKT,
    # soccer_football_info (SFI) catalog
    "SFI_LEAGUES": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
    "SFI_STANDINGS": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
    "SFI_PROGRESSIVE_STATS": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
    # open_meteo catalog
    "WEATHER": PipelineMode.BATCH_OPEN_METEO,
}


def _pipeline_mode_for_sports_data_type(data_type: str) -> PipelineMode:
    """Return the batch ``PipelineMode`` for a sports/prediction data_type.

    Used by orchestrator code paths that emit per-data_type manifest rows in
    loops (zero-fixture fast-path, sports reference data fan-out, enrichment
    fast-path). Raises ``KeyError`` for unknown data_types — keeps the
    enum-set wired and surfaces typos at runtime instead of silently picking
    an empty string.
    """

    return _SPORTS_DATA_TYPE_TO_PIPELINE_MODE[data_type.upper()]


def _canonical_league_id(lid_raw: object) -> str:
    """Normalize a league_id at write time to canonical form.

    Used at every per-league GCS partition write so legacy numeric
    af_league_ids (39, 78, 140, ...) get rewritten to canonical strings
    (EPL, BUNDESLIGA, LA_LIGA, ...) before they hit disk. Without this
    normalization, mixed numeric/canonical paths accumulate and per-league
    downstream readers (FSS, ML feature joins) miss data.

    - all-digits string → look up via UAC ``get_league_by_api_football_id``
    - already-canonical → pass through unchanged
    - unknown numeric → leave as-is (operator can debug)
    """
    s = str(lid_raw).strip()
    if not s or not s.isdigit():
        return s
    league = get_league_by_api_football_id(int(s))
    return league.league_id if league is not None else s


def _coerce_adapter_output(item: object) -> dict[str, object]:
    # UAC sports normalizers return dict[str, object]; some adapter return-type
    # annotations still claim list[CanonicalX] (Pydantic). Coerce defensively so
    # either shape works — prior assumption of Pydantic-only blew up INJURIES
    # backfill 2026-04-21 with AttributeError on every date.
    if isinstance(item, dict):
        return dict(item)
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        return dump()
    return {}


# ---------------------------------------------------------------------------
# Write-gate: fail-loud guard against §5 data-crimes at write time.
# ``warn`` mode (default) emits DATA_ALIGNMENT_VIOLATION and proceeds; ``strict``
# raises TimestampAlignmentError which per-shard try/except should catch and
# route to manifest.record_failed. Flip to ``strict`` once warn-mode volume
# baselines clean across sports adapters (see
# ``plans/active/instruments_service_write_gate_validation_2026_04_22.md``).
# ---------------------------------------------------------------------------
_WRITE_GATE = InstrumentsWriteGate(mode="warn")


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


def _af_id_from_canonical(obj: object) -> int | None:
    """Extract API-Football numeric ID from a CanonicalLeague / CanonicalTeam / CanonicalVenue.

    Tries ``api_football_id`` attribute first, falls back to ``logo_url``
    pattern parsing (legacy normalizer output).
    """
    af_id = getattr(obj, "api_football_id", None)
    if isinstance(af_id, int):
        return af_id
    if af_id is not None:
        try:
            return int(af_id)
        except (TypeError, ValueError):
            pass
    logo_url = getattr(obj, "logo_url", None)
    if isinstance(logo_url, str):
        match = _AF_LOGO_RE.search(logo_url)
        if match is not None:
            return int(match.group(1))
    return None


def _flatten_canonical_fixture_for_disk(fx: object, day: str) -> dict[str, object]:
    """Flatten a CanonicalFixture into a dict matching SPORTS_FIXTURES SchemaContract.

    Returns 32 columns (matching the new flat schema). Defaults required-non-null
    columns the canonical model doesn't carry (``round``, ``status_long``).
    Sets all extratime / penalty / period fields to None — those are populated
    by sibling writers (entity=fixture_stats / fixture_events).
    """
    home_team = getattr(fx, "home_team", None)
    away_team = getattr(fx, "away_team", None)
    league = getattr(fx, "league", None)
    venue = getattr(fx, "venue", None)
    referee = getattr(fx, "referee", None)
    kickoff = getattr(fx, "kickoff_utc", None)
    home_goals = getattr(fx, "home_goals", None)
    away_goals = getattr(fx, "away_goals", None)
    af_home_id = _af_id_from_canonical(home_team) if home_team is not None else None
    af_away_id = _af_id_from_canonical(away_team) if away_team is not None else None
    af_winner_id: int | None = None
    if home_goals is not None and away_goals is not None and home_goals != away_goals:
        af_winner_id = af_home_id if home_goals > away_goals else af_away_id

    raw_fid = getattr(fx, "source_fixture_id", None) or getattr(fx, "fixture_id", None)
    try:
        af_fixture_id = int(raw_fid) if raw_fid is not None else None
    except (TypeError, ValueError):
        af_fixture_id = None

    season_raw = getattr(fx, "season", None)
    try:
        season_int = int(str(season_raw).split("-")[0]) if season_raw is not None else None
    except (TypeError, ValueError):
        season_int = None

    return {
        "af_fixture_id": af_fixture_id,
        "referee_name": getattr(referee, "name", None) if referee is not None else None,
        "date": kickoff.date().isoformat() if kickoff is not None else day,
        "timestamp": kickoff.isoformat() if kickoff is not None else None,
        "periods_first": None,
        "periods_second": None,
        "venue_id": _af_id_from_canonical(venue) if venue is not None else None,
        "venue_name": getattr(venue, "name", None) if venue is not None else None,
        "venue_city": getattr(venue, "city", None) if venue is not None else None,
        "status_long": getattr(fx, "status", None) or "Unknown",
        "status_short": getattr(fx, "status", None) or "NS",
        "status_elapsed_time": None,
        "af_league_id": _af_id_from_canonical(league) if league is not None else None,
        "season": season_int,
        "round": getattr(fx, "round", "") or "",
        "af_home_id": af_home_id,
        "af_away_id": af_away_id,
        "af_winner_id": af_winner_id,
        "af_home_name": getattr(home_team, "name", "") if home_team is not None else "",
        "af_away_name": getattr(away_team, "name", "") if away_team is not None else "",
        "home_score": home_goals,
        "away_score": away_goals,
        "home_score_halftime": getattr(fx, "home_goals_halftime", None),
        "away_score_halftime": getattr(fx, "away_goals_halftime", None),
        "home_score_fulltime": home_goals,
        "away_score_fulltime": away_goals,
        "home_score_extratime": None,
        "away_score_extratime": None,
        "home_score_penalty": None,
        "away_score_penalty": None,
        "day": day,
        "data_available_at": None,  # caller post-fills with kickoff_utc - 7 days
        "match_end_time": getattr(fx, "match_end_time", None),
        "announced_at": getattr(fx, "announced_at", None),
        "report_time": getattr(fx, "report_time", None),
    }


def _gated_sink_write(
    sink: DataSink,
    *,
    data: pd.DataFrame,
    partition: dict[str, str],
    filename: str,
    venue: str | None = None,
    entity: str | None = None,
    format: str = "parquet",
) -> None:
    """Per-date sink write wrapped by ``InstrumentsWriteGate``.

    Callers should invoke this in place of ``sink.write(...)`` for any write
    whose partition carries ``day={D}`` so row-level timestamp misalignment
    fails loud instead of landing silently in GCS.

    In warn mode (current default) violations emit ``DATA_ALIGNMENT_VIOLATION``
    and the write still proceeds. In strict mode, ``TimestampAlignmentError``
    propagates and the caller's per-shard failure-isolation block records the
    shard as ``attempted_failed`` on the manifest.
    """
    _WRITE_GATE.validate_and_write(
        sink=sink,
        data=data,
        partition=partition,
        format=format,
        filename=filename,
        venue=venue,
        entity=entity,
    )


# Venue launch dates SSOT: UAC VenueMapping (canonical PROTOCOL-CHAIN format).
# No local copy — read from VenueMapping at module load. We resolve via
# ``get_instrument_discovery_start(venue)`` rather than reading
# ``venue_start_dates`` directly so per-venue discovery-API coverage overrides
# (HYPERLIQUID 2023-11-01 vs market-data 2023-04-15 — see UAC docstring) are
# respected. Reference incident 2026-05-05: 200 phantom HYPERLIQUID dates.
_VENUE_MAPPING = VenueMapping()

# ---------------------------------------------------------------------------
# DeFi venue list: dynamically built from UAC SUBGRAPH_IDS + static protocols
# ---------------------------------------------------------------------------
# Protocols with subgraph IDs are multi-chain — we discover all chains
# from the UAC registry so new chain deployments are picked up automatically.
_SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX: dict[str, str] = {
    "aave_v3": "AAVEV3",
    "uniswap_v2": "UNISWAPV2",
    "uniswap_v3": "UNISWAPV3",
    "uniswap_v4": "UNISWAPV4",
    "balancer": "BALANCER",
    "morpho": "MORPHO",
    "curve": "CURVE",
    "compound_v3": "COMPOUNDV3",
    # euler_v2 removed from universe — not needed yet.
    "fluid": "FLUID",
    # DEX forks — each has own subgraph IDs in UAC, reuse UniV3 adapter
    "pancakeswap_v3": "PANCAKESWAPV3",
    "sushiswap_v3": "SUSHISWAPV3",
    "aerodrome_v3": "AERODROMEV3",
    "camelot_v3": "CAMELOTV3",
    "velodrome_v2": "VELODROMEV2",
    "trader_joe_v2": "TRADER_JOEV2",
    "gmx": "GMX",
    "sushiswap": "SUSHISWAP",
    # Lending forks
    "spark": "SPARK",
}

# Protocols that don't use subgraphs (Ethereum-only, custom data sources).
_STATIC_DEFI_VENUES: list[str] = [
    "LIDO-ETHEREUM",
    "ETHERFI-ETHEREUM",
    "ETHENA-ETHEREUM",
    "EIGENLAYER-ETHEREUM",
]

# Solana DeFi venues (non-EVM, REST API-based discovery).
_SOLANA_DEFI_VENUES: list[str] = [
    "DRIFT-SOLANA",
    "KAMINO-SOLANA",
    "RAYDIUM-SOLANA",
    "ORCA-SOLANA",
    "MARINADE-SOLANA",
    "JITO-SOLANA",
    # Pacifica: Solana DEX perp clone (mainnet 2025-06). Added 2026-05-12.
    "PACIFICA-SOLANA",
    # Jupiter is execution-only (swap aggregator), not instrument discovery.
]

# L2 + other chain DEX perp venues (non-EVM-mainnet, non-Solana, REST API discovery).
_L2_DEX_PERP_VENUES: list[str] = [
    # Lighter: zkSync L2 CLOB perp DEX (mainnet 2024-08). Added 2026-05-12.
    "LIGHTER-ZKSYNC",
    # Extended: StarkNet perp DEX (mainnet 2024-07). Added 2026-05-12.
    "EXTENDED-STARKNET",
]


def _build_defi_venues() -> list[str]:
    """Build venue list from protocols that have subgraph IDs + static venues."""
    venues: list[str] = []
    for protocol, prefix in _SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX.items():
        for chain in get_supported_chains_for_protocol(protocol):
            venues.append(f"{prefix}-{chain}")
    venues.extend(_STATIC_DEFI_VENUES)
    venues.extend(_SOLANA_DEFI_VENUES)
    venues.extend(_L2_DEX_PERP_VENUES)
    return venues


_DEFI_VENUES: list[str] = _build_defi_venues()

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


def _set_cached_leagues(df: pd.DataFrame) -> None:
    global _cached_leagues_df
    _cached_leagues_df = df


def _set_cached_teams(df: pd.DataFrame, league_ids: list[int]) -> None:
    global _cached_teams_df, _cached_prediction_league_ids
    _cached_teams_df = df
    _cached_prediction_league_ids = league_ids


def _set_cached_standings(df: pd.DataFrame) -> None:
    global _cached_standings_df
    _cached_standings_df = df


def clear_defi_universe_cache() -> None:
    """Clear the DeFi universe cache. Call at the start of each batch run."""
    global _defi_universe_cache, _defi_universe_retryable
    _defi_universe_cache = None
    _defi_universe_retryable = []


# ---------------------------------------------------------------------------
# Adapter epoch versioning
# ---------------------------------------------------------------------------
# When adapter filtering logic changes (e.g. adding DEFI_MAJOR_ASSET_SYMBOLS
# filter, changing TVL thresholds), old manifest HWM entries become invalid.
# The epoch date marks when the current adapter version started — manifest
# entries BEFORE this date are ignored for monotonicity comparison.
#
# Bump the epoch date when adapter logic changes for a venue.
# Format: venue name → YYYY-MM-DD of the first run with new logic.
_VENUE_ADAPTER_EPOCH: dict[str, str] = {
    # 2026-04-02: removed DEFI_MAJOR_ASSET_SYMBOLS filter from all DeFi adapters
    # and TVL threshold from Uniswap V3 GraphQL query. Filtering now handled
    # post-fetch by filter_defi_instruments_by_relevance(). Manifest tracks
    # true pre-filter counts for monotonicity. Old filtered counts are lower
    # but new unfiltered counts are strictly >=, so no false regressions.
    "AAVEV3": "2026-04-02",
    "UNISWAPV2": "2026-04-02",
    # 2026-04-04: Uniswap V3/V4 and Balancer adapters had _FETCH_LIMIT=1000
    # with no pagination — actual pool counts exceed 1000. Pagination added
    # (skip-based, up to 6000 pools). Epoch bumped past all capped entries.
    "UNISWAPV3": "2026-04-05",
    "UNISWAPV4": "2026-04-05",
    "BALANCER": "2026-04-05",
    # 2026-04-04: Curve adapter was hardcoded to Ethereum API, ignoring chain
    # parameter — CURVE-AVALANCHE and CURVE-OPTIMISM had Ethereum pool counts.
    # Adapter fixed to use per-chain API URLs. Epoch bumped past today's bad entries.
    "CURVE": "2026-04-05",
    "COMPOUNDV3": "2026-04-02",
    "MORPHO": "2026-04-02",
    "FLUID": "2026-04-02",
    # DEX perp venues (L2 + Solana) — epoch from when adapters were registered
    "LIGHTER": "2026-05-12",
    "PACIFICA": "2026-05-12",
    "EXTENDED": "2026-05-12",
    # Solana adapters, LST, and yield venues — epoch from first run
    "DRIFT": "2026-04-02",
    "KAMINO": "2026-04-02",
    "ORCA": "2026-04-02",
    "RAYDIUM": "2026-04-02",
    "MARINADE": "2026-04-02",
    "JITO": "2026-04-02",
    "LIDO": "2026-04-02",
    "ETHERFI": "2026-04-02",
    "ETHENA": "2026-04-02",
}


def _get_venue_epoch(venue: str) -> str | None:
    """Return the adapter epoch date for a venue, or None if no epoch set.

    Matches by venue prefix: 'AAVEV3-ETHEREUM' matches epoch key 'AAVEV3'.
    """
    for prefix, epoch in _VENUE_ADAPTER_EPOCH.items():
        if venue.startswith(prefix):
            return epoch
    return None


# ---------------------------------------------------------------------------
# Honest-coverage helpers (Phase B, plan: honest_coverage_metrics_2026_04_19)
# ---------------------------------------------------------------------------
# Adapters that hit event-driven providers (Polymarket, Kalshi, FootyStats,
# Understat, SFI fixtures) MUST distinguish "we never tried" from "we tried
# and there was nothing".  These helpers wrap the pre-flight skip + venue
# error classification so per-shard call sites stay compact.


def _should_skip_shard(
    manifest: ManifestWriter,
    *,
    row_key: dict[str, str],
    force: bool,
) -> bool:
    """Return True if this shard already has a captured/empty_confirmed row.

    ``attempted_failed`` rows are NOT skipped — operator can decide via
    inspection whether the underlying error has been resolved.  ``force``
    bypasses the skip entirely (re-attempt the shard).
    """
    if force:
        return False
    prev: ManifestRow | None = manifest.lookup(row_key)
    if prev is None:
        return False
    return prev.capture_status in (
        CaptureStatus.CAPTURED.value,
        CaptureStatus.EMPTY_CONFIRMED.value,
    )


def _should_skip_date_for_per_league(
    manifest: ManifestWriter,
    *,
    date: str,
    data_type: str,
    expected_canonical_leagues: list[str],
    force: bool,
) -> bool:
    """Return True only when every expected canonical league is already
    captured / empty_confirmed for this date.

    The plain ``_should_skip_shard`` matches on ``(date, data_type)`` only
    and returns True if ANY league has a row for this date — that's wrong
    for per-league entities: if EPL was captured for date X but LA_LIGA
    wasn't, the orchestrator would still skip and never re-fetch LA_LIGA.

    Pre-fix incident (2026-05-05): MATCHES capped at 18% UI coverage even
    after fs-backfill ran for hours, because most dates had at least one
    league captured early on and the date-level skip prevented backfilling
    the rest. PREDICTIONS / ODDS share the same pattern; ODDS happened to
    look healthy because of dense empty_confirmed writes (the bulk
    /todays-matches endpoint returns odds for many leagues; matches not
    necessarily). Same pattern, same fix.

    ``force`` bypasses the skip entirely.
    """
    if force or not expected_canonical_leagues:
        return False
    for lid in expected_canonical_leagues:
        prev = manifest.lookup({"date": date, "data_type": data_type, "league_id": lid})
        if prev is None:
            return False
        if prev.capture_status not in (
            CaptureStatus.CAPTURED.value,
            CaptureStatus.EMPTY_CONFIRMED.value,
        ):
            return False
    return True


def _classify_adapter_failure(exc: Exception, venue: str) -> str:
    """Return a stable category string for ``record_failed`` from an exception.

    Honest-coverage manifest rows store a categorical failure code, not a
    raw exception message.  We try UAC ``classify_venue_error`` first using
    the exception's class name as the error code; if the venue/code pair is
    not known to UAC, we fall back to the exception class name itself so
    downstream dashboards can still group.
    """
    error_code = type(exc).__name__
    classification = classify_venue_error(venue, error_code)
    if classification is not None:
        return classification.error_code
    return error_code


def _get_defi_manifest_high_watermarks() -> dict[str, int]:
    """Read the DeFi manifest and return the max instrument_count per venue.

    Only considers manifest entries from the current adapter epoch forward.
    Entries before the epoch (from older adapter logic with different filtering)
    are ignored — their counts are not comparable to the current code.

    DeFi instruments are monotonically increasing (immutable smart contracts,
    never deleted). If a fresh API call returns fewer instruments for a venue
    than the manifest's post-epoch maximum, the API gave an incomplete result.
    """
    try:
        bucket = _get_instruments_bucket("DEFI")
        index_df = read_availability_index(bucket)
        if index_df.empty:
            return {}
        hwm: dict[str, int] = {}
        venue_vals: list[object] = list(index_df["venue"])
        count_vals: list[object] = list(index_df["instrument_count"])
        date_vals: list[object] = list(index_df["date"])
        for v_raw, c_raw, d_raw in zip(venue_vals, count_vals, date_vals, strict=True):
            v = str(v_raw)
            c = int(str(c_raw))
            d = str(d_raw)
            # Skip entries from before the current adapter epoch
            epoch = _get_venue_epoch(v)
            if epoch is not None and d < epoch:
                continue
            if c > hwm.get(v, 0):
                hwm[v] = c
        return hwm
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="read_defi_manifest",
        )
        return {}


def _count_per_venue(records: list[InstrumentRecord]) -> dict[str, int]:
    """Count instruments per venue in a record list."""
    counts: dict[str, int] = {}
    for r in records:
        v = r.venue or "UNKNOWN"
        counts[v] = counts.get(v, 0) + 1
    return counts


async def _retry_regressed_venues(
    regressed_venues: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
) -> list[InstrumentRecord]:
    """Re-fetch instruments for venues that showed count regression.

    Returns the retry results (may still be regressed — caller decides).
    """
    logger.info(
        "DeFi monotonicity: retrying %d regressed venues: %s",
        len(regressed_venues),
        regressed_venues,
    )
    with SolanaCacheSession(), EvmCacheSession():
        retry_result = await fetch_instruments_for_all_venues(regressed_venues, api_keys=api_keys, mode=mode)
    return retry_result.records


def _enforce_defi_monotonicity(
    records: list[InstrumentRecord],
    hwm: dict[str, int],
) -> tuple[list[InstrumentRecord], set[str]]:
    """Remove venues from records that regressed below their manifest high-water mark.

    Returns (clean_records, blocked_venues). Blocked venues had fewer instruments
    than the manifest max and must NOT be written to GCS (would overwrite better data).
    Only checks venues that are actually present in the records — venues not fetched
    are ignored (they have 0 count but were never requested).
    """
    new_counts = _count_per_venue(records)
    fetched_venues = set(new_counts.keys())
    blocked: set[str] = set()
    for venue, old_max in hwm.items():
        if venue not in fetched_venues:
            continue
        new_count = new_counts.get(venue, 0)
        if new_count < old_max:
            blocked.add(venue)
            logger.error(
                "DeFi monotonicity BLOCKED: %s has %d instruments (manifest max=%d) — "
                "will NOT write to GCS (would overwrite better data)",
                venue,
                new_count,
                old_max,
            )
        elif new_count > old_max:
            logger.info(
                "DeFi monotonicity OK: %s grew %d → %d (+%d)",
                venue,
                old_max,
                new_count,
                new_count - old_max,
            )

    if blocked:
        records = [r for r in records if r.venue not in blocked]
    return records, blocked


async def _get_or_fetch_defi_universe(
    defi_venues: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
) -> tuple[list[InstrumentRecord], list[str]]:
    """Return cached DeFi universe or fetch fresh.

    Includes a monotonicity check: if any venue returns fewer instruments
    than its historical max in the manifest, that venue is retried once.
    If still regressed after retry, the venue's records are REMOVED from
    the result — they will not be written to GCS (would overwrite better data).
    Good venues still proceed normally.

    Returns (records, retryable_venues). On cache hit, retryable_venues
    is the set from the original fetch.
    """
    global _defi_universe_cache, _defi_universe_retryable

    if _defi_universe_cache is not None:
        logger.info(
            "DeFi batch optimisation: reusing cached universe (%d instruments, skipping API calls)",
            len(_defi_universe_cache),
        )
        return _defi_universe_cache, _defi_universe_retryable

    # First call in this batch run — fetch fresh
    logger.info(
        "DeFi batch optimisation: fetching full universe once (%d venues)",
        len(defi_venues),
    )
    with SolanaCacheSession(), EvmCacheSession():
        fetch_result = await fetch_instruments_for_all_venues(defi_venues, api_keys=api_keys, mode=mode)

    all_records = list(fetch_result.records)
    retryable = list(fetch_result.retryable_venues)

    # Monotonicity check: compare per-venue counts against manifest high-water marks
    # Scope to only the venues we actually fetched — otherwise venues not in the
    # request appear "regressed" (0 vs HWM) and trigger unnecessary retries.
    hwm = _get_defi_manifest_high_watermarks()
    if hwm:
        new_counts = _count_per_venue(all_records)
        fetched_venues = set(new_counts.keys())
        regressed = [v for v, mx in hwm.items() if v in fetched_venues and new_counts.get(v, 0) < mx]

        if regressed:
            for venue in regressed:
                logger.warning(
                    "DeFi monotonicity VIOLATION: %s has %d instruments (manifest max=%d)",
                    venue,
                    new_counts.get(venue, 0),
                    hwm[venue],
                )

            # Retry regressed venues once
            retry_records = await _retry_regressed_venues(regressed, api_keys, mode)
            retry_counts = _count_per_venue(retry_records)

            # For each regressed venue: use whichever fetch returned more
            for venue in regressed:
                old_count = new_counts.get(venue, 0)
                retry_count = retry_counts.get(venue, 0)
                if retry_count > old_count:
                    all_records = [r for r in all_records if r.venue != venue]
                    all_records.extend(r for r in retry_records if r.venue == venue)
                    logger.info(
                        "DeFi monotonicity: %s improved on retry (%d → %d)",
                        venue,
                        old_count,
                        retry_count,
                    )

        # Final enforcement: block any venues still below high-water mark
        all_records, blocked = _enforce_defi_monotonicity(all_records, hwm)
        if blocked:
            logger.error(
                "DeFi monotonicity: %d venue(s) BLOCKED from GCS write: %s",
                len(blocked),
                sorted(blocked),
            )
    else:
        logger.info("DeFi monotonicity: no manifest history — skipping check (first run)")

    _defi_universe_cache = all_records
    _defi_universe_retryable = retryable
    logger.info(
        "DeFi batch optimisation: cached %d instruments from %d venues",
        len(_defi_universe_cache),
        len(defi_venues),
    )
    return _defi_universe_cache, _defi_universe_retryable


_CEFI_VENUES: list[str] = [
    "BINANCE-SPOT",
    "BINANCE-FUTURES",
    "BYBIT",
    # OKX: 3 separate Tardis exchanges — okex (spot), okex-swap (perps), okex-futures (fixed-expiry)
    # Do NOT add bare "OKX" — it maps to same Tardis exchange as OKX-SPOT (duplicate data).
    "OKX-SPOT",
    "OKX-SWAP",
    "OKX-FUTURES",
    "DERIBIT",
    # DERIBIT-COMBO: live multi-leg options strategies (straddles, strangles, spreads, condors).
    # Historical combos are covered by DERIBIT → Tardis. This venue fetches LIVE active combos
    # from the Deribit public REST API (kind=combo, expired=false).
    "DERIBIT-COMBO",
    "COINBASE-SPOT",
    "HYPERLIQUID",
    "UPBIT",
    "ASTER",
    # Tier-3 CeFi (Tardis archive — factory entries exist, added to orchestrator 2026-05-12)
    "KRAKEN-FUTURES",
    "BITFINEX-FUTURES",
]

_TRADFI_VENUES: list[str] = [
    "CME",
    "NASDAQ",
    "NYSE",
    "CBOE",
    "ICE",
    "FX",
]


# ---------------------------------------------------------------------------
# DEFI instrument relevance filter
# ---------------------------------------------------------------------------
def _normalize_wrapped_token(symbol: str) -> str:
    """Normalize wrapped/bridged token symbols to their canonical form.

    Strips chain-specific prefixes and suffixes so that tokens like avUSDC,
    aAvaDAI, USDT.e, renBTC match their canonical equivalents (USDC, DAI,
    USDT, BTC) in the major assets set.

    Prefix priority: longest match first to avoid false strips (e.g. "aAva"
    before "a" so aAvaDAI → DAI, not AvaDAI).
    """
    s = symbol.upper().strip()
    # Suffixes: .e (Avalanche bridged), .b (BNB bridged)
    for suffix in (".E", ".B"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    # Prefixes: longest first. aAva (Aave on Avalanche), av (Avalanche native),
    # ren (Ren bridge), st (staked variants handled by major list already)
    for prefix in ("AAVA", "AV", "REN"):
        if s.startswith(prefix) and len(s) > len(prefix):
            s = s[len(prefix) :]
            break
    return s


def filter_defi_instruments_by_relevance(records: list) -> list:
    """Filter DEFI instruments to major liquid assets only.

    The asset whitelist comes from config_reloaders.get_defi_major_assets()
    (InstrumentsDomainConfigState), which defaults to the hardcoded ETH/BTC/
    USDT/USDC and derivatives set and can be overridden via cloud ConfigStore.

    DEX_VENUE_KEYWORDS is the SSOT from UAC (includes EVM + Solana DEXes).

    Token matching uses _normalize_wrapped_token() to strip chain-specific
    prefixes/suffixes (avUSDC → USDC, aAvaDAI → DAI, renBTC → BTC, USDT.e → USDT).

    Rules:
    - DEX pools (Uniswap, Balancer, Curve, Orca, Raydium, Kamino): both
      base AND quote must match the major assets set (after normalization).
      Eliminates long-tail pairs like PEPE/WETH or FAITH/MILAREPA.
    - Lending protocols (Aave, Morpho, Fluid, LST services): base
      asset must match. Keeps aWETH, aWBTC, aUSDC etc.
    """
    major = get_defi_major_assets()  # reads from config_reloaders (hot-reloadable)
    result = []
    for r in records:
        raw_base = (getattr(r, "base_asset", None) or "").upper().strip()
        raw_quote = (getattr(r, "quote_asset", None) or "").upper().strip()
        base = raw_base if raw_base in major else _normalize_wrapped_token(raw_base)
        quote = raw_quote if raw_quote in major else _normalize_wrapped_token(raw_quote)
        venue = (getattr(r, "venue", None) or "").upper()
        is_dex = any(kw in venue for kw in DEX_VENUE_KEYWORDS)
        if is_dex:
            if base in major and quote in major:
                result.append(r)
            else:
                logger.debug(
                    "DEX relevance reject: venue=%s base=%s(raw=%s) quote=%s(raw=%s) symbol=%s",
                    venue,
                    base,
                    raw_base,
                    quote,
                    raw_quote,
                    getattr(r, "symbol", "?"),
                )
        else:
            if base in major:
                result.append(r)
            else:
                logger.debug(
                    "Lending relevance reject: venue=%s base=%s(raw=%s) symbol=%s",
                    venue,
                    base,
                    raw_base,
                    getattr(r, "symbol", "?"),
                )
    return result


def filter_instruments_by_date(
    records: list,
    date_dt: datetime,
    defi_venues: frozenset[str] | None = None,
) -> list:
    """Return only instruments active on the given UTC datetime.

    An instrument is active on `date_dt` when:
    - available_from_datetime is None OR available_from_datetime <= date_dt
    - available_to_datetime   is None OR available_to_datetime   >= date_dt

    This is required because URDI adapters return the full historical universe.
    function reduces them to only the instruments tradeable on the requested day.

    Args:
        records: InstrumentRecord list from URDI.
        date_dt: UTC datetime representing the requested processing date.
        defi_venues: Optional set of DeFi venue names (uppercase). When provided,
            a WARNING is emitted for any DeFi instrument where available_from_datetime=None
            because on-chain creation timestamps are expected for all DeFi instruments
            and absence indicates the URDI adapter did not provide them (data quality
            is degraded — the instrument will still be included but with unknown
            listing date).
    """
    result = []
    for r in records:
        since: datetime | None = getattr(r, "available_from_datetime", None)
        until: datetime | None = getattr(r, "available_to_datetime", None)
        since_ok = since is None or since <= date_dt
        until_ok = until is None or until >= date_dt
        if since_ok and until_ok:
            if defi_venues is not None and since is None:
                venue = (getattr(r, "venue", None) or "").upper()
                if venue in defi_venues:
                    key = getattr(r, "instrument_key", repr(r))
                    logger.error(
                        "DeFi instrument %s has available_from_datetime=None — "
                        "URDI adapter MUST provide creation timestamp "
                        "(protocol floor date or on-chain); "
                        "instrument included but date accuracy is UNKNOWN",
                        key,
                    )
            result.append(r)
    return result


def get_venues_for_asset_groups(asset_groups: list[str]) -> list[str]:
    """Return UAC canonical venue names for the requested asset groups (CEFI, DEFI, …)."""
    venues: list[str] = []
    for cat in asset_groups:
        cat_upper = cat.upper()
        if cat_upper in ("CEFI", "ALL"):
            venues.extend(_CEFI_VENUES)
        if cat_upper in ("TRADFI", "ALL"):
            venues.extend(_TRADFI_VENUES)
        if cat_upper in ("DEFI", "ALL"):
            venues.extend(_DEFI_VENUES)
        if cat_upper in ("SPORTS", "ALL"):
            # instruments-service owns fixtures + slow-moving reference data
            # (teams, leagues, players, referees, venues) via API-Football.
            # Betting market instruments (the actual tradeable positions) come from
            # market-tick-data-service via Odds API — documented exception because
            # markets are only discoverable alongside odds data.
            # Enrichment providers (no instruments — reference data for features):
            # FootyStats (match stats), Understat (xG), Transfermarkt (player values),
            # SoccerFootball.info (standings), Open-Meteo (weather).
            venues.extend(
                [
                    "API_FOOTBALL",
                    "FOOTYSTATS",
                    "UNDERSTAT",
                    "TRANSFERMARKT",
                    "SOCCER_FOOTBALL_INFO",
                    "OPEN_METEO",
                ]
            )
        if cat_upper in ("PREDICTION", "ALL"):
            # POLYMARKET + KALSHI: prediction market instruments (crypto up/down, soccer, macro).
            # No auth required — Gamma API (Polymarket) and public API (Kalshi) are keyless.
            venues.extend(["POLYMARKET", "KALSHI"])
    return list(dict.fromkeys(venues))


def is_venue_available(venue: str, date: str) -> bool:
    """Return True if the venue's discovery API can produce instruments on this date.

    Uses ``get_instrument_discovery_start`` rather than raw ``venue_start_dates``
    so HYPERLIQUID (and any future venue with a discovery-API gap narrower than
    its market-data archive) gates on the date the discovery endpoint actually
    has data — not the market-data archive earliest date. Pre-2026-05-05 this
    used ``venue_start_dates["HYPERLIQUID"] = 2023-04-15`` and produced 200
    phantom ``attempted_failed`` rows for the April-October 2023 window where
    the discovery API legitimately returns nothing.
    """
    launch_date = _VENUE_MAPPING.get_instrument_discovery_start(venue)
    if launch_date is None:
        return True  # Unknown venue — assume always available
    return date >= launch_date


def earliest_venue_date(venues: list[str]) -> str | None:
    """Return the earliest discovery-start date across the given venues, or None."""
    dates = [d for v in venues if (d := _VENUE_MAPPING.get_instrument_discovery_start(v)) is not None]
    return min(dates) if dates else None


_SPORTS_PROVIDER_VENUES: dict[str, list[str]] = {
    "API_FOOTBALL": ["API_FOOTBALL"],
    "API_FOOTBALL_ENRICHMENT": ["API_FOOTBALL"],
    "OPEN_METEO": ["OPEN_METEO"],
    "TRANSFERMARKT": ["TRANSFERMARKT"],
    "SOCCER_FOOTBALL_INFO": ["SOCCER_FOOTBALL_INFO"],
    "UNDERSTAT": ["UNDERSTAT"],
    "FOOTYSTATS": ["FOOTYSTATS"],
}


async def process_instruments(
    date: str | datetime,
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
    _ = get_config()  # ensure config is initialized

    # Normalise date: BatchIO passes datetime objects from get_date_range(),
    # but all downstream code (URDI, date filter, partition keys) needs str YYYY-MM-DD.
    if isinstance(date, datetime):
        date = date.strftime("%Y-%m-%d")

    # venue_override bypasses category lookup when --venues filter is active (sharding)
    venues = venue_override if venue_override is not None else get_venues_for_asset_groups(asset_groups)

    # Track which sports entities are missing (set in skip-if-exists check).
    # Empty = fetch everything; non-empty = only fetch these specific entities.
    _sports_missing_entities: list[str] = []

    # Recovery-mode hint: when --recovery-fixture-ids is set, the per-provider
    # fetches (footystats / understat / sfi / open_meteo) need to bypass their
    # per-day/per-league pre-flight skip, because empty_confirmed phantom rows
    # would otherwise mask the dates we're trying to recover. Each provider's
    # fetch already has a ``force=...`` parameter that bypasses its skip; we
    # promote redo_all when recovery is active so the existing dispatch
    # ``force=redo_all`` propagates correctly.
    #
    # For api_football the orchestrator's per-fixture loop has its own
    # explicit allowlist filter (further down in _fetch_sports_reference_data),
    # so the redo_all promotion here is harmless — the allowlist is the
    # finer-grained scope.
    if recovery_fixture_ids is not None:
        if not redo_all:
            logger.info(
                "Recovery mode: promoting redo_all=True so per-provider per-day skip "
                "is bypassed (recovery_fixture_ids has %d af_fixture_ids)",
                len(recovery_fixture_ids),
            )
        redo_all = True

    # 1. Skip venues not yet launched
    active_venues = [v for v in venues if is_venue_available(v, date)]

    # --sports-provider: restrict to only this provider's venues
    if sports_provider:
        provider_venues = _SPORTS_PROVIDER_VENUES.get(sports_provider)
        if provider_venues is None:
            logger.error("Unknown --sports-provider: %s. Valid: %s", sports_provider, list(_SPORTS_PROVIDER_VENUES))
            return {}
        active_venues = [v for v in active_venues if v in provider_venues]
        logger.info("Sports provider filter: %s → venues %s", sports_provider, active_venues)

        # Enrichment provider short-circuits: skip ALL orchestrator logic
        # (URDI, API Football fixture fetch, etc.) and go straight to the
        # specific provider's fetch function. Each reads fixtures from GCS
        # (already fetched by API_FOOTBALL runs) and calls only its own API.
        _enrichment_providers = {"OPEN_METEO", "UNDERSTAT", "FOOTYSTATS", "TRANSFERMARKT", "SOCCER_FOOTBALL_INFO"}
        if sports_provider in _enrichment_providers:
            logger.info("%s short-circuit: skipping orchestrator for date=%s", sports_provider, date)
            primary_asset_group = asset_groups[0] if asset_groups else "SPORTS"
            bucket = _get_instruments_bucket(primary_asset_group)
            if not bucket:
                logger.error("No bucket resolved for asset_group=%s", primary_asset_group)
                return {}
            _keys = api_keys or {}

            result: dict[str, int] = {}
            if sports_provider == "OPEN_METEO":
                result = await _fetch_weather_data(date=date, bucket=bucket, api_key=_keys.get("open_meteo"))
            elif sports_provider == "UNDERSTAT":
                result = await _fetch_understat_xg(date=date, bucket=bucket, force=redo_all)
            elif sports_provider == "FOOTYSTATS":
                fs_key = _keys.get("footystats")
                if not fs_key:
                    logger.warning("No footystats API key — skipping date=%s", date)
                    return {}
                _ef = sports_entity_filter
                if not _ef or _ef == "PREDICTIONS":
                    pred_result = await _fetch_footystats_predictions(
                        date=date, api_key=fs_key, bucket=bucket, force=redo_all
                    )
                    result.update(pred_result)
                if not _ef or _ef == "MATCHES":
                    match_result = await _fetch_footystats_matches(
                        date=date, api_key=fs_key, bucket=bucket, force=redo_all
                    )
                    result.update(match_result)
                if not _ef or _ef == "ODDS":
                    odds_result = await _fetch_footystats_odds(date=date, api_key=fs_key, bucket=bucket, force=redo_all)
                    result.update(odds_result)
            elif sports_provider == "TRANSFERMARKT":
                tm_key = _keys.get("transfermarkt")
                if not tm_key:
                    logger.warning("No transfermarkt API key — skipping date=%s", date)
                    return {}
                # Transfermarkt teams are slow (33 leagues x 90s rate limit = ~50 min).
                # Per-league triggers: only fetch teams for leagues whose trigger dates
                # match TODAY (transfer windows, season boundaries) — not all 33 on
                # every trigger. Leagues list (metadata) is fast (1 API call), always fetched.
                _batch_dt = date_type.fromisoformat(date) if isinstance(date, str) else date
                _leagues_today = get_leagues_needing_refresh(_batch_dt)
                # CLI entity filter takes precedence; otherwise per-league trigger logic decides
                _tm_entity = sports_entity_filter
                if not _tm_entity:
                    _tm_entity = None if _leagues_today else "TRANSFERMARKT_LEAGUES"
                if not _leagues_today and not sports_entity_filter:
                    logger.info("Transfermarkt: date=%s has no league triggers — leagues only (skipping teams)", date)
                elif _leagues_today and not sports_entity_filter:
                    logger.info(
                        "Transfermarkt: date=%s triggers %d leagues: %s", date, len(_leagues_today), _leagues_today
                    )
                # Derive the European-football season from the batch date: a league
                # season spans Aug-May by convention, so `season_year = d.year` when
                # d.month >= 8 else `d.year - 1`. CLI `--season` (season_override)
                # wins if explicitly set; otherwise we MUST derive here — passing
                # `season=None` to the adapter defaults to `datetime.now(UTC).year`
                # (= current year), which is a §5 data-crime for any historical
                # backfill (writes today's roster onto a 2023 date partition).
                _tm_season = (
                    season_override
                    if season_override is not None
                    else (_batch_dt.year if _batch_dt.month >= 8 else _batch_dt.year - 1)
                )
                result = await _fetch_transfermarkt_data(
                    date=date,
                    api_key=tm_key,
                    bucket=bucket,
                    entity_filter=_tm_entity,
                    league_filter=_leagues_today if _leagues_today and _tm_entity != "TRANSFERMARKT_LEAGUES" else None,
                    season=_tm_season,
                    force=redo_all,
                )
            elif sports_provider == "SOCCER_FOOTBALL_INFO":
                sfi_key = _keys.get("soccer_football_info")
                if not sfi_key:
                    logger.warning("No soccer_football_info API key — skipping date=%s", date)
                    return {}
                result = await _fetch_sfi_data(
                    date=date,
                    api_key=sfi_key,
                    bucket=bucket,
                    entity_filter=sports_entity_filter,
                    force=redo_all,
                )
            else:
                result = {}

            logger.info("%s DONE for date=%s: %s", sports_provider, date, result)
            return result

    if not active_venues:
        logger.info("No active venues for date=%s asset_groups=%s", date, asset_groups)
        return {}

    # Sports entity lists — used by freshness check AND later fast-path logic,
    # so they must be defined unconditionally (not inside redo_all gate).
    is_sports_run = any(c.upper() in ("SPORTS", "ALL") for c in asset_groups)
    _sports_core_entities = [
        # LEAGUES retired 2026-05-07 (C.1 audit, manifest_migration_master_2026_05_07).
        # UAC ``LeagueDefinition`` + ``provider_league_ids`` (FOOTYSTATS_SEASON_IDS,
        # FOOTYSTATS_HISTORICAL_SEASON_IDS, etc.) canonicalise the league refdata via
        # code commits — daily-cadence GCS dump was 3046 daily shards of identical
        # static data. Existing manifest rows flipped to empty_confirmed with
        # reason=EXPECTED_DEPRECATED_DATA_TYPE via the migration script in
        # instruments-service/scripts/migrate_leagues_kill_2026_05_07.py.
        "TEAMS",
        "STANDINGS",
        "INJURIES",
    ]
    _sports_per_fixture_entities = [
        "FIXTURE_STATS",
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "PLAYER_STATS",
    ]

    # Tracks which sports entities the manifest says are missing.
    # Populated by the freshness check; stays empty when --force is set.
    _sports_missing_entities: list[str] = []

    # 1b. Skip-if-exists: check manifest for fresh data (unless --force)
    if not redo_all:
        primary_asset_group = asset_groups[0] if asset_groups else None
        bucket = _get_instruments_bucket(primary_asset_group)

        # For SPORTS, require both core AND per-fixture reference entities.
        # Core: leagues/teams/standings/injuries (slow-moving, fetched every run).
        # Per-fixture: fixture_stats/events/lineups/player_stats (one API call per
        # completed fixture, rate-limited to 1 req/sec — expensive to re-fetch).
        # Remap venue names to match manifest data_type entries (API_FOOTBALL → FIXTURES).
        expected = ["FIXTURES" if v == "API_FOOTBALL" else v for v in active_venues]
        _active_venues_set_freshness = set(active_venues)
        # Enrichment entity → venue that produces it.
        # Only include in expected[] when that venue is in active_venues
        # (respects --venues filter so API_FOOTBALL-only runs don't wait on
        # SFI/Transfermarkt/Understat/Weather manifest entries).
        _enrichment_entity_venues: list[tuple[str, str]] = [
            ("MATCHES", "FOOTYSTATS"),
            ("PREDICTIONS", "FOOTYSTATS"),
            ("XG", "UNDERSTAT"),
            ("TRANSFERMARKT_LEAGUES", "TRANSFERMARKT"),
            ("PLAYER_VALUES", "TRANSFERMARKT"),
            ("SFI_LEAGUES", "SOCCER_FOOTBALL_INFO"),
            ("SFI_STANDINGS", "SOCCER_FOOTBALL_INFO"),
            ("SFI_PROGRESSIVE_STATS", "SOCCER_FOOTBALL_INFO"),
            ("WEATHER", "OPEN_METEO"),
        ]
        if is_sports_run:
            expected.extend(_sports_core_entities)
            expected.extend(_sports_per_fixture_entities)

            # League-aware enrichment: only expect an enrichment entity if
            # the leagues it covers have fixtures on this date.  Read the
            # manifest index once to get leagues with FIXTURES on this date.
            #
            # Performance: the in-process index cache is invalidated after every
            # ``manifest.write()`` call (manifest_writer.py:_invalidate_index_cache).
            # In the per-date BatchIO loop the orchestrator writes manifest at
            # the END of each date's work, so the next date's read_availability_index
            # call here misses the cache → re-reads the full 25MB / 2.6M-row
            # canonical → ~27s GCS pull. That dominates wall-clock for ALL
            # multi-date sports backfills.
            #
            # Skip this read entirely when scope is already explicit:
            #   * ``sports_entity_filter`` set → entity-scoped run, ``expected``
            #     gets restricted to that one entity later anyway (line 1221)
            #   * ``recovery_fixture_ids`` set → targeted recovery, the allowlist
            #     IS the date-aware scope; no need to introspect the manifest.
            # Both signals mean the league-aware enrichment expectations don't
            # change the orchestration outcome — we already know what to fetch.
            _date_fixture_leagues: set[str] = set()
            _scope_is_explicit = bool(sports_entity_filter) or recovery_fixture_ids is not None
            if not _scope_is_explicit:
                _index_df = read_availability_index(bucket)
                if not _index_df.empty and "league_id" in _index_df.columns:
                    _fix_mask = (_index_df["date"] == date) & (_index_df["data_type"] == "FIXTURES")
                    _lid_series = _index_df.loc[_fix_mask, "league_id"].dropna()
                    _date_fixture_leagues = {str(lid).upper() for lid in _lid_series.unique() if str(lid).strip()}
            else:
                logger.debug(
                    "date=%s: skipping per-date read_availability_index — scope is explicit "
                    "(sports_entity_filter=%s, recovery_fixture_ids=%s)",
                    date,
                    sports_entity_filter,
                    "set" if recovery_fixture_ids is not None else "unset",
                )

            for entity, venue in _enrichment_entity_venues:
                if venue not in _active_venues_set_freshness:
                    continue
                # Check league coverage — skip entity if its covered leagues
                # have no fixtures on this date. With explicit scope, we skip
                # the league check (no fixture data loaded) and let the
                # downstream sports_entity_filter restriction below scope us.
                coverage = get_entity_league_coverage(entity)
                if coverage is not None and _date_fixture_leagues and not coverage & _date_fixture_leagues:
                    logger.debug(
                        "date=%s: skipping %s from expected — no fixture from covered leagues %s",
                        date,
                        entity,
                        sorted(coverage),
                    )
                    continue
                expected.append(entity)

        # Entity-scoped VM: when --sports-entity is set, restrict expected[] to just
        # that one entity. This makes the freshness check and all fetches single-entity,
        # allowing 17 parallel VMs (one per manifest entity type) instead of 8 year VMs.
        if sports_entity_filter and is_sports_run:
            expected = [sports_entity_filter]
            _sports_core_entities = [e for e in _sports_core_entities if e == sports_entity_filter]
            _sports_per_fixture_entities = [e for e in _sports_per_fixture_entities if e == sports_entity_filter]
            logger.info("Entity-scoped mode: restricting to %s only", sports_entity_filter)

        # Historical dates (>7 days ago) have immutable data — completed fixtures
        # and reference data don't change retroactively. Use max_age_hours=0 so the
        # freshness check only fails on schema version mismatch, not on timestamp.
        # The 24h default is correct for live/today runs where data updates daily.
        _date_cutoff = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
        _freshness_max_age = 0.0 if date < _date_cutoff else 24.0

        # Sports per-league entities (FIXTURES + PREDICTIONS + MATCHES + ODDS +
        # 5 per-fixture downstreams + ...) write one manifest row per
        # (date, data_type, league_id). The coarse `check_shard_freshness`
        # only checks "is data_type present for this date" — once any league
        # has e.g. FIXTURES for date X, the whole date is "fresh" and
        # skipped, so other-league missing rows never get re-fetched.
        # Per-league freshness lives in the entity handlers themselves
        # (`_should_skip_date_for_per_league`); skip the coarse pre-flight
        # for these so the per-entity handlers run. Reference incident
        # 2026-05-06: phantom-recovery DELETE of 100k per-(date, league)
        # FIXTURES rows still got skipped because legitimate captures for
        # OTHER leagues kept the date "fresh" at the coarse level.
        _sports_per_league_entities: frozenset[str] = frozenset(
            {
                "FIXTURES",
                "PREDICTIONS",
                "MATCHES",
                "ODDS",
                "STANDINGS",
                "TEAMS",
                "INJURIES",
                "FIXTURE_STATS",
                "FIXTURE_EVENTS",
                "FIXTURE_LINEUPS",
                "PLAYER_STATS",
                "XG",
                "PLAYER_VALUES",
                "TRANSFERMARKT_VALUES",
                "SFI_PROGRESSIVE_STATS",
                "WEATHER",
                "ODDS_HORIZON_BUCKET",
            }
        )
        _has_sports_per_league_in_scope = bool(set(expected) & _sports_per_league_entities)

        if is_sports_run and _has_sports_per_league_in_scope:
            # Defer to per-league checks in the entity handlers. Treat all
            # expected entities as "missing" at the date level so the
            # downstream per-entity dispatch fires; each handler does its
            # own per-league `_should_skip_date_for_per_league`.
            is_fresh = False
            stale = []
            missing = list(expected)
            logger.info(
                "date=%s: deferring pre-flight to per-league entity handlers (sports per-league mode; expected=%s)",
                date,
                expected,
            )
        else:
            is_fresh, stale, missing = check_shard_freshness(
                bucket=bucket,
                date=date,
                service_name="instruments-service",
                expected_venues=expected,
                max_age_hours=_freshness_max_age,
            )
        if is_fresh:
            logger.info(
                "SKIP date=%s: all %d venues/entities already fresh in manifest (use --force to re-fetch)",
                date,
                len(expected),
            )
            return {}

        # Per-entity skip: pass the exact missing list so _fetch_sports_reference_data
        # only fetches entities that are actually absent from the manifest.
        if is_sports_run and missing:
            _sports_missing_entities = list(missing)
            missing_set = set(missing)
            core_missing = missing_set & set(_sports_core_entities)
            pf_missing = [e for e in _sports_per_fixture_entities if e in missing_set]
            instruments_missing = missing_set - set(_sports_core_entities) - set(_sports_per_fixture_entities)
            logger.info(
                "date=%s: per-entity breakdown — %d core missing (%s), %d per-fixture missing (%s), %d instruments missing",
                date,
                len(core_missing),
                sorted(core_missing),
                len(pf_missing),
                pf_missing,
                len(instruments_missing),
            )
            # If only per-fixture entities are missing (core + instruments done),
            # skip the expensive URDI fetch and jump to enrichment.
            if not core_missing and not instruments_missing and pf_missing:
                logger.info(
                    "date=%s: core entities fresh — enrichment-only mode for %s",
                    date,
                    pf_missing,
                )

        if stale or missing:
            logger.info(
                "date=%s: %d stale + %d missing venues/entities — will re-fetch (stale=%s, missing=%s)",
                date,
                len(stale),
                len(missing),
                stale[:5],
                missing[:5],
            )

    # Fast path: if only specific sports entities are missing (instruments done),
    # skip URDI fetch and jump to targeted sports enrichment.
    # Two sub-cases:
    #   A) Only per-fixture entities missing (core + instruments done)
    #   B) Only core entities missing (e.g. injuries only — instruments done)
    # Both skip the expensive URDI fetch and go straight to _fetch_sports_reference_data.
    if _sports_missing_entities and api_keys:
        missing_set = set(_sports_missing_entities)
        # Enrichment entities (XG, Transfermarkt, FootyStats, SFI, Weather) can
        # read existing fixtures from GCS — they don't need a URDI fetch.
        _enrichment_entity_names = {e for e, _ in _enrichment_entity_venues}
        instruments_missing = (
            missing_set - set(_sports_core_entities) - set(_sports_per_fixture_entities) - _enrichment_entity_names
        )
        # Fast path fires when only core/per-fixture/enrichment entities are missing
        # (no actual instrument records to fetch from URDI)
        if not instruments_missing:
            api_football_key = api_keys.get("api_football")
            if api_football_key:
                primary_asset_group = asset_groups[0] if asset_groups else None
                bucket = _get_instruments_bucket(primary_asset_group)
                # Resolve fixture IDs from existing GCS fixtures parquet (0 API calls)
                gcs_fixture_ids = _read_fixture_ids_from_gcs(bucket, date)
                logger.info(
                    "ENRICHMENT-ONLY date=%s: %d fixture IDs from GCS, fetching %s",
                    date,
                    len(gcs_fixture_ids),
                    _sports_missing_entities,
                )
                # Create manifest writer so _fetch_sports_reference_data can write
                # per-league manifest entries for injuries and per-fixture entities.
                sports_manifest = ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                sports_ref_counts = await _fetch_sports_reference_data(
                    date=date,
                    api_key=api_football_key,
                    bucket=bucket,
                    entities_to_fetch=_sports_missing_entities,
                    fixture_ids_override=gcs_fixture_ids,
                    manifest=sports_manifest,
                    recovery_fixture_ids=recovery_fixture_ids,
                    redo_all=redo_all,
                )
                # Write manifest for entities that did NOT write their own
                # manifest entries inside _fetch_sports_reference_data.
                _self_manifested_enr = {
                    "injuries",
                    "fixture_stats",
                    "fixture_events",
                    "fixture_lineups",
                    "player_stats",
                }
                for entity_name, row_count in sports_ref_counts.items():
                    if entity_name not in _self_manifested_enr:
                        sports_manifest.record_captured_from_counts(
                            row_key={"date": date, "data_type": entity_name.upper()},
                            total_rows=row_count,
                            expected_root_clusters={},
                            observed_clusters={"": row_count},
                            available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                            pipeline_mode=_pipeline_mode_for_sports_data_type(entity_name.upper()),
                            service_emission_state=None,
                        )
                # Honest-coverage: per-fixture entities on a 0-fixture date are
                # legitimately empty — record_empty so attempt_coverage_pct lifts
                # while capture_coverage_pct stays accurate.
                _enr_attempt_ts = datetime.now(UTC)
                if not gcs_fixture_ids:
                    for pf_entity in _sports_per_fixture_entities:
                        entity_short = pf_entity.replace("API_FOOTBALL_", "").lower()
                        if entity_short not in sports_ref_counts:
                            sports_manifest.record_empty(
                                row_key={
                                    "date": date,
                                    "data_type": pf_entity.replace("API_FOOTBALL_", "").upper(),
                                },
                                attempted_at=_enr_attempt_ts,
                                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                            )
                sports_manifest.write()
                logger.info(
                    "Enrichment-only manifest: %d entities for %s",
                    len(sports_ref_counts),
                    date,
                )
                return sports_ref_counts

    log_event(
        "PROCESSING_STARTED",
        details={"date": date, "asset_groups": asset_groups, "venue_count": len(active_venues)},
    )

    # Enrichment-only entities don't need URDI at all — they fetch by date,
    # not by fixture ID.  Skip the expensive URDI bootstrap entirely.
    _enrichment_only_entities = frozenset(
        {
            "XG",
            "MATCHES",
            "PREDICTIONS",
            "PLAYER_VALUES",
            "TRANSFERMARKT_LEAGUES",
            "SFI_LEAGUES",
            "SFI_STANDINGS",
        }
    )
    # Per-fixture entities need fixture IDs but can read them from GCS
    # instead of making expensive URDI calls to API Football.
    _per_fixture_entities = frozenset(
        {
            "FIXTURE_EVENTS",
            "FIXTURE_LINEUPS",
            "FIXTURE_STATS",
            "PLAYER_STATS",
        }
    )
    _skip_urdi = sports_entity_filter in (_enrichment_only_entities | _per_fixture_entities)
    if sports_entity_filter in _enrichment_only_entities:
        logger.info(
            "Skipping URDI fetch — %s is an enrichment-only entity (fetches by date, not fixture ID)",
            sports_entity_filter,
        )
    elif sports_entity_filter in _per_fixture_entities:
        logger.info(
            "Skipping URDI fetch — %s will read fixture IDs from existing GCS fixtures",
            sports_entity_filter,
        )

    # 2. Fetch from URDI — sole external API path
    # api_keys injected from preflight() → validate_api_keys_for_venues() → Secret Manager
    # date passed so date-aware adapters (e.g. API-Football) can filter server-side
    #
    # DeFi batch optimisation: DeFi instruments are monotonically growing
    # (immutable contracts, never deleted). In batch mode, the universe is
    # fetched ONCE and cached — subsequent dates in the range just filter
    # by available_from_datetime. Non-DeFi venues are fetched fresh per date.
    records: list[InstrumentRecord] = []
    _retryable_venues: list[str] = []
    # Track venues where the adapter ran without error (even if 0 records returned).
    # Used by the completeness check to distinguish "adapter returned nothing for this
    # date range" (OK) from "adapter failed to respond" (completeness failure).
    _non_error_venues: set[str] = set()

    defi_venue_names = frozenset(_DEFI_VENUES)
    if _skip_urdi:
        # Enrichment-only: empty the venue lists so URDI fetch loops are no-ops.
        defi_active: list[str] = []
        non_defi_active: list[str] = []
    else:
        defi_active = [v for v in active_venues if v in defi_venue_names]
        non_defi_active = [v for v in active_venues if v not in defi_venue_names]

    # DeFi: use cached universe (one API call for entire batch run)
    if defi_active and mode == "batch":
        defi_records, defi_retryable = await _get_or_fetch_defi_universe(defi_active, api_keys=api_keys, mode=mode)
        records.extend(defi_records)
        _retryable_venues.extend(defi_retryable)
        # All DeFi venues that aren't retryable ran OK (even if 0 records after date filter)
        _non_error_venues.update(v for v in defi_active if v not in defi_retryable)
    elif defi_active:
        # Live mode: always fetch fresh DeFi data (with monotonicity check)
        with SolanaCacheSession(), EvmCacheSession():
            defi_result = await fetch_instruments_for_all_venues(defi_active, api_keys=api_keys, date=date, mode=mode)
        defi_live_records = list(defi_result.records)
        _non_error_venues.update(v for v in defi_active if v not in {e.venue for e in defi_result.failed_venues})

        # Monotonicity check: retry regressed venues, then block any still below HWM
        hwm = _get_defi_manifest_high_watermarks()
        if hwm:
            live_counts = _count_per_venue(defi_live_records)
            regressed = [v for v, mx in hwm.items() if live_counts.get(v, 0) < mx]
            if regressed:
                retry_records = await _retry_regressed_venues(regressed, api_keys, mode)
                retry_counts = _count_per_venue(retry_records)
                for venue in regressed:
                    if retry_counts.get(venue, 0) > live_counts.get(venue, 0):
                        defi_live_records = [r for r in defi_live_records if r.venue != venue]
                        defi_live_records.extend(r for r in retry_records if r.venue == venue)
            # Final enforcement: block venues still below HWM from being written
            defi_live_records, blocked = _enforce_defi_monotonicity(defi_live_records, hwm)
            if blocked:
                logger.error(
                    "DeFi live monotonicity: %d venue(s) BLOCKED: %s",
                    len(blocked),
                    sorted(blocked),
                )

        records.extend(defi_live_records)
        _retryable_venues.extend(defi_result.retryable_venues)

    # Non-DeFi: always fetch fresh (CeFi instruments change daily, TradFi has expiries)
    if non_defi_active:
        with SolanaCacheSession():
            non_defi_result = await fetch_instruments_for_all_venues(
                non_defi_active, api_keys=api_keys, date=date, mode=mode
            )
        records.extend(non_defi_result.records)
        _retryable_venues.extend(non_defi_result.retryable_venues)
        _non_error_venues.update(
            v for v in non_defi_active if v not in {e.venue for e in non_defi_result.failed_venues}
        )

    # Per-fixture URDI skip: read fixture IDs from GCS and jump to enrichment.
    # This avoids the URDI fetch + date filter which returns 0 for historical dates.
    if _skip_urdi and sports_entity_filter in _per_fixture_entities:
        primary_asset_group = asset_groups[0] if asset_groups else None
        _pf_bucket = _get_instruments_bucket(primary_asset_group)
        gcs_fixture_ids = _read_fixture_ids_from_gcs(_pf_bucket, date)
        if not gcs_fixture_ids:
            logger.info("Per-fixture GCS skip: no fixtures in GCS for date=%s", date)
            return {}
        api_football_key = api_keys.get("api_football") if api_keys else None
        if not api_football_key:
            logger.warning("Per-fixture backfill: no API Football key for date=%s", date)
            return {}
        logger.info(
            "Per-fixture GCS-based enrichment date=%s: %d fixture IDs from GCS, entity=%s",
            date,
            len(gcs_fixture_ids),
            sports_entity_filter,
        )
        pf_counts = await _fetch_sports_reference_data(
            date=date,
            api_key=api_football_key,
            bucket=_pf_bucket,
            entities_to_fetch=[sports_entity_filter],
            fixture_ids_override=gcs_fixture_ids,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
        )
        pf_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=_pf_bucket)
        for entity_name, row_count in pf_counts.items():
            pf_manifest.record_captured_from_counts(
                row_key={"date": date, "data_type": entity_name.upper()},
                total_rows=row_count,
                expected_root_clusters={},
                observed_clusters={"": row_count},
                available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                pipeline_mode=_pipeline_mode_for_sports_data_type(entity_name.upper()),
                service_emission_state=None,
            )
        pf_manifest.write()
        return pf_counts

    # 3. Filter to instruments active on the requested date.
    # URDI adapters return the full historical instrument universe; this reduces
    # it to only instruments tradeable on the requested day.
    # Pass the DeFi venue set so the filter can warn on missing available_from_datetime.
    is_defi_run = any(c.upper() in ("DEFI", "ALL") for c in asset_groups)
    defi_venue_set: frozenset[str] | None = frozenset(_DEFI_VENUES) if is_defi_run else None
    date_dt = datetime.fromisoformat(date).replace(tzinfo=UTC)
    records = filter_instruments_by_date(records, date_dt, defi_venues=defi_venue_set)
    logger.info(
        "Date filter %s: %d instruments active (from URDI fetch)",
        date,
        len(records),
    )

    # 3b. Enrich CeFi/DeFi instruments with timezone=UTC (24/7 markets).
    # TradFi instruments get timezone from the databento adapter's session metadata.
    _tradfi_set = frozenset(_TRADFI_VENUES)
    for r in records:
        if r.timezone is None and r.venue not in _tradfi_set:
            r.timezone = "UTC"

    # Per-venue breakdown after date filter
    venue_counts: dict[str, int] = {}
    for r in records:
        v = getattr(r, "venue", "UNKNOWN") or "UNKNOWN"
        venue_counts[v] = venue_counts.get(v, 0) + 1
    for v in sorted(venue_counts):
        logger.info("  %s: %d instruments after date filter", v, venue_counts[v])

    # 3a. DeFi available_from_datetime coverage summary.
    # Counts how many DeFi instruments in the date-filtered set have a populated
    # available_from_datetime vs None. Low coverage indicates URDI adapters are not
    # returning on-chain creation timestamps and the date filter is permissive
    # (treating None as "always available").
    if is_defi_run and records:
        defi_records = [r for r in records if (getattr(r, "venue", "") or "").upper() in _DEFI_VENUES]
        if defi_records:
            populated = sum(1 for r in defi_records if getattr(r, "available_from_datetime", None) is not None)
            total_defi = len(defi_records)
            pct = int(populated * 100 / total_defi)
            logger.info(
                "Date accuracy: %d/%d DeFi instruments have available_from_datetime populated (%d%% coverage)",
                populated,
                total_defi,
                pct,
            )

    # 3b. DEFI relevance filter: keep only instruments involving major liquid assets.
    # Whitelist is from config_reloaders.get_defi_major_assets() — defaults to
    # ETH/BTC/USDT/USDC and known derivatives; can be overridden via ConfigStore.
    if any(c.upper() in ("DEFI", "ALL") for c in asset_groups):
        before = len(records)
        records = filter_defi_instruments_by_relevance(records)
        logger.info(
            "DEFI relevance filter: %d → %d instruments (removed %d long-tail)",
            before,
            len(records),
            before - len(records),
        )

    # 4. Handle zero records.
    # For SPORTS: zero fixtures on a given day is normal (no matches scheduled).
    # Write an empty marker parquet so the manifest knows the day was processed
    # successfully and won't re-fetch without --force.
    # For DeFi in batch mode: zero records after date filter is expected for dates
    # before the first pool was created — skip silently (no GCS write, no error).
    # For CeFi/TradFi: zero records = something is broken → fail the shard.
    if not records:
        is_sports_only = all(c.upper() == "SPORTS" for c in asset_groups)
        if is_sports_only:
            primary_asset_group = asset_groups[0] if asset_groups else None
            bucket = _get_instruments_bucket(primary_asset_group)
            sink = get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
            # Write one empty marker per prediction league so downstream
            # consumers see each league as "processed with 0 fixtures".
            #
            # Honest-coverage (CLAUDE.md "4 pillars" #1): we use
            # ``record_empty`` here, NOT ``add(row_count=0)``. Marking
            # zero-fixture days as ``captured`` with row_count=0 is the
            # exact anti-pattern the rule was added for — it inflates the
            # captured count and masks honest absence. Reference incident
            # 2026-05-06: AUSTRIAN_BUNDESLIGA, GREEK_SUPER_LEAGUE et al.
            # showed 3041 captured FIXTURES rows that were ALL phantoms
            # (instrument_count=0, no parquet on disk) before this fix.
            #
            # We also DROP the empty placeholder parquet write — empty
            # placeholders that look populated are worse than missing data
            # because they evade detection. If a date has no fixtures, no
            # parquet should exist; the manifest's ``empty_confirmed`` row
            # is the single honest marker.
            _empty_league_ids = league_filter if league_filter else get_all_prediction_league_ids()
            _empty_attempt_ts = datetime.now(UTC)
            _empty_manifest = ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            for _league_id in _empty_league_ids:
                _empty_manifest.record_empty(
                    row_key={
                        "date": date,
                        "data_type": "FIXTURES",
                        "league_id": _canonical_league_id(_league_id),
                    },
                    attempted_at=_empty_attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                )
            _empty_manifest.write()
            logger.info(
                "SPORTS: No fixtures for date=%s — wrote empty_confirmed markers for %d leagues",
                date,
                len(_empty_league_ids),
            )
            # Still fetch sports reference data (leagues/teams/standings/injuries)
            # even when no fixtures exist. These are date-independent slow-moving
            # entities needed for downstream feature computation.
            if api_keys:
                api_football_key = api_keys.get("api_football")
                if api_football_key:
                    # Only fetch entities that are actually missing from the manifest.
                    # Create manifest writer so _fetch_sports_reference_data can write
                    # per-league manifest entries for injuries and per-fixture entities.
                    sports_manifest = ManifestWriter(
                        service_name="instruments-service",
                        catalogue_bucket=bucket,
                    )
                    sports_ref_counts = await _fetch_sports_reference_data(
                        date=date,
                        api_key=api_football_key,
                        bucket=bucket,
                        entities_to_fetch=_sports_missing_entities if _sports_missing_entities else None,
                        fixture_ids_override=[],  # zero-fixture date — skip 33-league API re-fetch
                        manifest=sports_manifest,
                        recovery_fixture_ids=recovery_fixture_ids,
                        redo_all=redo_all,
                    )
                    if sports_ref_counts:
                        _self_manifested_zf = {
                            "injuries",
                            "fixture_stats",
                            "fixture_events",
                            "fixture_lineups",
                            "player_stats",
                        }
                        for entity_name, row_count in sports_ref_counts.items():
                            if entity_name not in _self_manifested_zf:
                                if row_count > 0:
                                    sports_manifest.record_captured_from_counts(
                                        row_key={"date": date, "data_type": entity_name.upper()},
                                        total_rows=row_count,
                                        expected_root_clusters={},
                                        observed_clusters={"": row_count},
                                        available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                                        pipeline_mode=_pipeline_mode_for_sports_data_type(entity_name.upper()),
                                        service_emission_state=None,
                                    )
                                else:
                                    # Honest-coverage: api returned 0 rows
                                    # → empty_confirmed, not captured-with-0.
                                    sports_manifest.record_empty(
                                        row_key={
                                            "date": date,
                                            "data_type": entity_name.upper(),
                                        },
                                        attempted_at=datetime.now(UTC),
                                        pipeline_mode=_pipeline_mode_for_sports_data_type(entity_name.upper()),
                                    )
                        # Per-fixture entities on zero-fixture dates: nothing
                        # to fetch (no fixtures = no per-fixture data). Write
                        # ``empty_confirmed`` markers so the orchestrator
                        # knows we attempted and skip-on-rerun without
                        # inflating the captured count (CLAUDE.md "4 pillars"
                        # #1: row_count > 0 OR record_empty, never
                        # ``captured`` with row_count=0).
                        for pf_entity in _sports_per_fixture_entities:
                            entity_short = pf_entity.lower()
                            if entity_short not in sports_ref_counts:
                                sports_manifest.record_empty(
                                    row_key={
                                        "date": date,
                                        "data_type": pf_entity,
                                    },
                                    attempted_at=datetime.now(UTC),
                                    pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                                )
                        sports_manifest.write()
                # Zero-fixture fast path: fixture-dependent enrichment entities get
                # 0-count manifest entries immediately (no API calls / rate limits).
                # Fixture-INDEPENDENT entities (Transfermarkt, SFI) are excluded —
                # they provide reference data (team values, standings) regardless
                # of whether matches are played on this date.
                _active_venues_set = set(active_venues)
                _enrichment_zero_entities: list[str] = []
                if "FOOTYSTATS" in _active_venues_set:
                    _enrichment_zero_entities += ["PREDICTIONS", "MATCHES"]
                if "UNDERSTAT" in _active_venues_set:
                    _enrichment_zero_entities += ["XG"]
                # NOTE: TRANSFERMARKT and SFI are NOT zero-gated — they provide
                # fixture-independent reference data (team values, standings).
                if "OPEN_METEO" in _active_venues_set:
                    _enrichment_zero_entities += ["WEATHER"]
                if _enrichment_zero_entities:
                    _enr_manifest = ManifestWriter(
                        service_name="instruments-service",
                        catalogue_bucket=bucket,
                    )
                    _enr_attempt_ts = datetime.now(UTC)
                    for _enr_entity in _enrichment_zero_entities:
                        # Honest-coverage: zero-fixture day → record_empty,
                        # NOT add(row_count=0). See CLAUDE.md "4 pillars" #1
                        # and AUSTRIAN_BUNDESLIGA phantom-row incident
                        # 2026-05-06.
                        _enr_manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": _enr_entity,
                            },
                            attempted_at=_enr_attempt_ts,
                            pipeline_mode=_pipeline_mode_for_sports_data_type(_enr_entity),
                        )
                    _enr_manifest.write()
                    logger.info(
                        "Zero-fixture fast path: wrote empty_confirmed for %d fixture-dependent entities on date=%s",
                        len(_enrichment_zero_entities),
                        date,
                    )

            # Fixture-independent reference data: fetch even on zero-fixture dates,
            # but ONLY on trigger dates (season start, transfer window open/close).
            # This avoids re-fetching identical squad data every day.
            counts: dict[str, int] = {}
            _active_venues_set = set(active_venues)
            _ef = sports_entity_filter

            def _entity_wanted_zf(ent: str) -> bool:
                return _ef is None or _ef == ent

            # Check if today is a reference refresh trigger for any league.
            _batch_date = date_type.fromisoformat(date)
            _is_trigger = is_any_league_refresh_date(_batch_date) or redo_all
            _trigger_leagues = get_leagues_needing_refresh(_batch_date) if _is_trigger else []
            if not _is_trigger:
                logger.info(
                    "date=%s: not a reference refresh trigger — skipping Transfermarkt/SFI team fetches",
                    date,
                )

            transfermarkt_key = (
                api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in _active_venues_set) else None
            )
            if not transfermarkt_key and "TRANSFERMARKT" in _active_venues_set:
                logger.warning(
                    "TRANSFERMARKT is active but no API key found — skipping for date=%s.",
                    date,
                )
            if (
                transfermarkt_key
                and _is_trigger
                and (_entity_wanted_zf("TRANSFERMARKT_LEAGUES") or _entity_wanted_zf("PLAYER_VALUES"))
            ):
                logger.info(
                    "Trigger-based Transfermarkt refresh for date=%s (leagues: %s)",
                    date,
                    _trigger_leagues[:5] if _trigger_leagues else "all (--force)",
                )
                try:
                    tm_counts = await _fetch_transfermarkt_data(
                        date=date,
                        api_key=transfermarkt_key,
                        bucket=bucket,
                        entity_filter=_ef,
                        season=season_override,
                        force=redo_all,
                    )
                    for k, v in tm_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="transfermarkt_data_fetch",
                        shard=date,
                    )

            sfi_key = (
                api_keys.get("soccer_football_info")
                if (api_keys and "SOCCER_FOOTBALL_INFO" in _active_venues_set)
                else None
            )
            if not sfi_key and "SOCCER_FOOTBALL_INFO" in _active_venues_set:
                logger.warning(
                    "SOCCER_FOOTBALL_INFO is active but no API key found — skipping for date=%s.",
                    date,
                )
            if sfi_key and (
                _entity_wanted_zf("SFI_LEAGUES")
                or _entity_wanted_zf("SFI_STANDINGS")
                or _entity_wanted_zf("SFI_PROGRESSIVE_STATS")
            ):
                try:
                    sfi_counts = await _fetch_sfi_data(
                        date=date,
                        api_key=sfi_key,
                        bucket=bucket,
                        entity_filter=_ef,
                        force=redo_all,
                    )
                    for k, v in sfi_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sfi_data_fetch",
                        shard=date,
                    )

            log_event("PROCESSING_COMPLETED", details={"date": date, "asset_groups": asset_groups, "fixtures": 0})
            return counts
        # DeFi batch: zero records after date filter is normal for early dates
        # (venue exists in UAC but no pools created yet on-chain). Skip without error.
        is_defi_only = all(c.upper() in ("DEFI",) for c in asset_groups)
        if is_defi_only and mode == "batch":
            logger.debug(
                "DeFi batch: zero instruments after date filter for date=%s — "
                "all venues pre-date their first pool creation. Skipping.",
                date,
            )
            return {}

        # TradFi non-trading day: zero instruments on weekends/holidays is expected.
        # Write 0-count manifest entries per venue so the manifest marks the day as
        # processed and won't re-fetch without --force. This prevents permanent gaps
        # in instrument data for every weekend and exchange holiday.
        tradfi_active = [v for v in active_venues if v in _TRADFI_VENUES]
        if tradfi_active:
            target_dt = date_type.fromisoformat(date)
            non_trading_venues = [v for v in tradfi_active if is_non_trading_day(v, target_dt)]
            if non_trading_venues and len(non_trading_venues) == len(tradfi_active):
                primary_asset_group = asset_groups[0] if asset_groups else None
                bucket = _get_instruments_bucket(primary_asset_group)
                manifest = ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                _nt_attempt_ts = datetime.now(UTC)
                for venue in non_trading_venues:
                    # Honest-coverage Phase 2.E.2: discriminate weekend vs
                    # holiday so the manifest carries an EXPECTED_* row per
                    # (shard_key, day) instead of a bare empty_confirmed.
                    # instruments-service emits the TradFi non-trading-day
                    # marker on behalf of its own catalog refresh — the
                    # underlying tick source for TradFi venues (CME/NQ) is
                    # databento per UAC SOURCE_PRIORITY, but the manifest
                    # row here represents the instruments-service catalog's
                    # statement that no instruments exist for the day, so
                    # tag with BATCH_INSTRUMENTS_SERVICE.
                    _reason = non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
                    manifest.record_expected_empty(
                        row_key={"date": date, "venue": venue},
                        reason=_reason,
                        attempted_at=_nt_attempt_ts,
                        pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                    )
                manifest.write()
                logger.info(
                    "TRADFI non-trading day: date=%s venues=%s — wrote empty_confirmed manifest entries",
                    date,
                    sorted(non_trading_venues),
                )
                log_event(
                    "PROCESSING_COMPLETED",
                    details={
                        "date": date,
                        "asset_groups": asset_groups,
                        "non_trading_venues": sorted(non_trading_venues),
                    },
                )
                return dict.fromkeys(non_trading_venues, 0)

        msg = (
            f"URDI returned zero records for date={date} asset_groups={asset_groups}. "
            f"Venues attempted: {active_venues}. "
            "Check URDI adapter coverage and network connectivity."
        )
        logger.error(msg)
        log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)

    # 5. Schema validation — bad records fail the entire venue shard.
    #    If ANY instrument in a venue fails validation, the whole venue is skipped.
    #    Validation-failed venues are tracked separately so the completeness check
    #    doesn't count them as "missing" (they were fetched, just rejected).
    valid_records, rejected = validate_instrument_records(records)
    validation_failed_venues: set[str] = set()
    if rejected:
        # Group rejections by venue — fail entire venue shard
        failed_venues: dict[str, list[str]] = {}
        for rec, reason in rejected:
            failed_venues.setdefault(rec.venue, []).append(reason)
        for venue, reasons in sorted(failed_venues.items()):
            logger.error(
                "SHARD FAILED date=%s venue=%s: %d instruments failed validation — %s",
                date,
                venue,
                len(reasons),
                reasons[0],
            )
        # Remove all records from failed venues (fail the shard, not just the record)
        validation_failed_venues = set(failed_venues.keys())
        records = [r for r in valid_records if r.venue not in validation_failed_venues]
        log_event(
            "SHARD_INCOMPLETE",
            details={
                "date": date,
                "failed_venues": sorted(validation_failed_venues),
                "reason": "schema_validation_failure",
            },
        )
    else:
        records = valid_records
    if not records:
        msg = f"All records/venues rejected by schema validation for date={date}"
        logger.error(msg)
        log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)

    rows = []
    for r in records:
        d = r.model_dump()
        # Serialize legs list[InstrumentLeg] → JSON string for parquet storage
        if d.get("legs") is not None:
            d["legs"] = json.dumps(d["legs"])
        rows.append(d)
    df = pd.DataFrame(rows)

    # 6. Domain validation — logs anomalies, doesn't raise for instruments domain
    DomainValidationService("instruments").validate_for_domain(df)

    # 6. Write per-venue parquet + catalogue + CSV sample
    # Pass config explicitly — _uc is read at call time so sampling honours
    # ENABLE_CSV_SAMPLING even when set after the singleton initialised.
    counts: dict[str, int] = {}
    sampler = create_sampling_service(
        {
            "enable_sampling": _uc.enable_csv_sampling,
            "sample_size": _uc.csv_sample_size,
            "sample_dir": _uc.csv_sample_dir,
        }
    )
    # Use the first (primary) category to route to the correct category-specific bucket.
    # UCI naming: instruments-store-{category.lower()}-{project}
    # e.g. DEFI → instruments-store-defi-{gcp_project_id}
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _get_instruments_bucket(primary_asset_group)
    # prefix ensures writes land at instrument_availability/by_date/{day=X}/{venue=Y}/
    sink = get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            venue_str = str(venue_name)
            if venue_str == "API_FOOTBALL":
                # League-based sharding: partition sports fixtures by league_id.
                # instrument_key format: {LEAGUE}:{HOME}_v_{AWAY}:{DATE}
                # Extract league_id as the part before the first colon.
                _sports_df = venue_df.copy()
                _sports_df["_league_id"] = _sports_df["instrument_key"].str.split(":").str[0]
                # Apply league filter if set (--league CLI arg)
                if league_filter:
                    _sports_df = _sports_df[_sports_df["_league_id"].isin(league_filter)]
                _captured_lids: set[str] = set()
                for _lid, _league_df in _sports_df.groupby("_league_id"):
                    _league_id_str = str(_lid)
                    _captured_lids.add(_league_id_str)
                    _league_df_clean = _league_df.drop(columns=["_league_id"])
                    _gated_sink_write(
                        sink,
                        data=_league_df_clean,
                        partition={"day": date, "venue": venue_str, "league": _canonical_league_id(_league_id_str)},
                        filename="instruments.parquet",
                        venue=venue_str,
                        entity="instruments",
                    )
                    _stamped_fixture_df = stamp_available_at_explicit(_league_df_clean, when=datetime.now(UTC))
                    manifest.record_captured(
                        row_key={
                            "date": date,
                            "data_type": "FIXTURES",
                            "league_id": _canonical_league_id(_league_id_str),
                        },
                        df=_stamped_fixture_df,
                        category="sports",
                        instrument_type="",
                        data_type="FIXTURES",
                        league_id=_canonical_league_id(_league_id_str),
                        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                        service_emission_state=None,
                    )
                    counts[f"FIXTURES/{_league_id_str}"] = len(_league_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _league_df_clean,
                            filename_prefix=f"instruments_API_FOOTBALL_{_league_id_str}_{date}",
                        )

                # Honest-coverage: every league that is in-season on this date
                # but had zero fixtures gets a record_empty row. Without this,
                # mid-week gaps render as red "missing" in the data-status
                # drilldown even though the adapter ran and the API legitimately
                # returned zero for that league. Season window comes from UAC
                # get_league_fixture_calendar — only leagues whose season
                # actually covers this date are claimed empty.
                _fx_attempt_ts = datetime.now(UTC)
                _expected_af_lids = {league.league_id for league in get_expected_leagues_for_source("api_football")}
                if league_filter:
                    _expected_af_lids &= set(league_filter)
                for _exp_lid in sorted(_expected_af_lids - _captured_lids):
                    if not get_league_fixture_calendar(_exp_lid, date, date):
                        continue
                    manifest.record_empty(
                        row_key={
                            "date": date,
                            "data_type": "FIXTURES",
                            "league_id": _exp_lid,
                        },
                        attempted_at=_fx_attempt_ts,
                        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                    )

            elif venue_str.upper() in ("POLYMARKET", "KALSHI") and "base_asset" in venue_df.columns:
                # PREDICTION: bundle by canonical_question_group per the UAC
                # SSOT (``BTC_UP_DOWN_HOURLY`` / ``BTC_UP_DOWN_DAILY`` /
                # ``SPX_UP_DOWN_DAILY`` / ``ELECTION_PRESIDENT_2028`` /
                # ``OTHER``, etc.). Recurring canonical groups cycle through
                # multiple condition_ids over time — HOURLY = ~24/day,
                # DAILY = 1/day — so the shard atom is per-(canonical_group,
                # day), with all market_ids active on that day bundled into
                # one parquet (analogous to options-chain bundling). Per
                # ``predictions_master_2026_05_07.plan.md`` Phase 1
                # critical-path + CLAUDE.md "Per-asset-group shard-key
                # matrix → Prediction". Polymarket + Kalshi share this
                # path: both prediction venues classify per the UAC
                # ``classify_*_to_canonical_group`` SSOT and bundle on
                # the same axis so MTDS reads + features compute apply
                # identically.
                _pred_df = venue_df.copy()
                _pred_df["_canonical_group"] = _pred_df.apply(
                    _extract_prediction_canonical_group,
                    axis=1,
                )
                _manifest_venue = venue_str.upper()
                for _group_raw, _group_df in _pred_df.groupby("_canonical_group"):
                    _group_str = str(_group_raw)
                    _group_df_clean = _group_df.drop(columns=["_canonical_group"])
                    _gated_sink_write(
                        sink,
                        data=_group_df_clean,
                        partition={
                            "day": date,
                            "venue": venue_str,
                            "canonical_question_group": _group_str,
                        },
                        filename="instruments.parquet",
                        venue=venue_str,
                        entity="instruments",
                    )
                    # Manifest row: data_type=prediction_canonical_question_group
                    # (the bundled data_type per UAC BUNDLED_DATA_TYPES SSOT),
                    # underlying=<canonical_group> (the per-bundle cluster
                    # identity, mirroring options_chain root-bucketing).
                    _stamped_group_df = stamp_available_at_explicit(_group_df_clean, when=datetime.now(UTC))
                    _pred_pm = (
                        PipelineMode.BATCH_POLYMARKET_GAMMA_API
                        if _manifest_venue == "POLYMARKET"
                        else PipelineMode.BATCH_INSTRUMENTS_SERVICE
                    )
                    manifest.record_captured(
                        row_key={
                            "date": date,
                            "data_type": "prediction_canonical_question_group",
                            "venue": _manifest_venue,
                            "underlying": _group_str,
                        },
                        df=_stamped_group_df,
                        category="prediction",
                        instrument_type="",
                        data_type="prediction_canonical_question_group",
                        venue=_manifest_venue,
                        underlying=_group_str,
                        expected_root_clusters={},
                        cluster_extractor=lambda s: s,
                        pipeline_mode=_pred_pm,
                        service_emission_state=None,
                    )
                    counts[f"{_manifest_venue}/{_group_str}"] = len(_group_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _group_df_clean,
                            filename_prefix=f"instruments_{_manifest_venue}_{_group_str}_{date}",
                        )
            else:
                _write_venue(venue_str, venue_df, date, bucket, sink, counts, sampler, manifest)
    else:
        _write_venue("all", df, date, bucket, sink, counts, sampler, manifest)

    # Write 0-count manifest entries for TRADFI venues that returned 0 instruments
    # because the date is a non-trading day (weekend/holiday). Without this, those
    # venues have no manifest entry and appear as permanent gaps in the data status.
    _tradfi_set_for_manifest = frozenset(_TRADFI_VENUES)
    tradfi_empty = _non_error_venues - set(counts.keys())
    tradfi_empty = {v for v in tradfi_empty if v in _tradfi_set_for_manifest}
    if tradfi_empty:
        target_dt = date_type.fromisoformat(date)
        non_trading = {v for v in tradfi_empty if is_non_trading_day(v, target_dt)}
        if non_trading:
            _nt_attempt_ts = datetime.now(UTC)
            for venue in sorted(non_trading):
                # Honest-coverage Phase 2.E.2: discriminate weekend vs holiday
                # so the manifest carries an EXPECTED_* row per (shard_key, day).
                # See header note on TradFi non-trading day pipeline_mode: this
                # is the instruments-service catalog asserting absence; tag
                # with BATCH_INSTRUMENTS_SERVICE.
                _reason = non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
                manifest.record_expected_empty(
                    row_key={"date": date, "venue": venue},
                    reason=_reason,
                    attempted_at=_nt_attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                )
                counts[venue] = 0
            logger.info(
                "TRADFI non-trading day manifest: date=%s venues=%s — wrote empty_confirmed entries",
                date,
                sorted(non_trading),
            )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    manifest.close()

    # 7. SPORTS enrichment: fetch and write reference data (teams, leagues, etc.)
    # alongside fixtures. These are slow-moving entities that don't change per-date
    # but are re-fetched to capture transfers, promotions, new seasons.
    is_sports = any(c.upper() in ("SPORTS", "ALL") for c in asset_groups)
    # OPEN_METEO doesn't need API keys — allow sports enrichment even with empty api_keys
    # OPEN_METEO and UNDERSTAT don't need API keys (free, no auth)
    _needs_api_keys = sports_provider not in ("OPEN_METEO", "UNDERSTAT") if sports_provider else True
    if is_sports and (api_keys or not _needs_api_keys):
        _keys = api_keys or {}
        api_football_key = _keys.get("api_football")
        if not api_football_key:
            logger.warning("api_football key missing from api_keys — skipping sports reference data")
        else:
            # Pass completed fixture IDs from URDI fetch to avoid 33-league re-fetch
            # (saves 33 API calls per date). _urdi_completed_fixture_ids is populated
            # during the URDI instruments fetch above.
            # Only fetch entities that are actually missing from the manifest.
            # Create manifest writer so _fetch_sports_reference_data can write
            # per-league manifest entries for injuries and per-fixture entities.
            sports_manifest = ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            sports_ref_counts = await _fetch_sports_reference_data(
                date=date,
                api_key=api_football_key,
                bucket=bucket,
                entities_to_fetch=_sports_missing_entities if _sports_missing_entities else None,
                fixture_ids_override=list(_urdi_completed_fixture_ids),
                manifest=sports_manifest,
                recovery_fixture_ids=recovery_fixture_ids,
                redo_all=redo_all,
            )
            for k, v in sports_ref_counts.items():
                counts[k] = counts.get(k, 0) + v

            # Write manifest for sports reference entities that did NOT write
            # their own manifest entries inside _fetch_sports_reference_data
            # (injuries and per-fixture entities write per-league entries directly).
            _self_manifested = {"injuries", "fixture_stats", "fixture_events", "fixture_lineups", "player_stats"}
            for entity_name, row_count in sports_ref_counts.items():
                if entity_name not in _self_manifested:
                    sports_manifest.record_captured_from_counts(
                        row_key={"date": date, "data_type": entity_name.upper()},
                        total_rows=row_count,
                        expected_root_clusters={},
                        observed_clusters={"": row_count},
                        available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                        pipeline_mode=_pipeline_mode_for_sports_data_type(entity_name.upper()),
                        service_emission_state=None,
                    )
            sports_manifest.write()
            logger.info(
                "Sports reference manifest: %d entities for %s",
                len(sports_ref_counts),
                date,
            )

        # FootyStats predictive data: proprietary potentials (btts_potential,
        # o25_potential, xg_prematch, etc.) written as a separate entity so FSS
        # can consume them as third-party signal input alongside odds.
        # Only call each enrichment provider if it's in active_venues (respects --venues filter).
        # When sports_entity_filter is set (entity-scoped VM), also guard individual
        # enrichment calls so only the requested entity is fetched.
        _active_venues_set = set(active_venues)
        _ef = sports_entity_filter  # short alias for entity filter checks

        def _entity_wanted(manifest_name: str) -> bool:
            """Return True if this entity should be fetched in the current run."""
            return _ef is None or _ef == manifest_name

        footystats_key = api_keys.get("footystats") if (api_keys and "FOOTYSTATS" in _active_venues_set) else None
        if footystats_key:
            if _entity_wanted("PREDICTIONS"):
                try:
                    pred_counts = await _fetch_footystats_predictions(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in pred_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_predictions_fetch",
                        shard=date,
                    )

            if _entity_wanted("MATCHES"):
                try:
                    match_counts = await _fetch_footystats_matches(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in match_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_matches_fetch",
                        shard=date,
                    )

            if _entity_wanted("ODDS"):
                try:
                    odds_counts = await _fetch_footystats_odds(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in odds_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_odds_fetch",
                        shard=date,
                    )

        if "UNDERSTAT" in _active_venues_set and _entity_wanted("XG"):
            try:
                xg_counts = await _fetch_understat_xg(date=date, bucket=bucket, force=redo_all)
                for k, v in xg_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="understat_xg_fetch",
                    shard=date,
                )

        transfermarkt_key = (
            api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in _active_venues_set) else None
        )
        if not transfermarkt_key and "TRANSFERMARKT" in _active_venues_set:
            logger.warning(
                "TRANSFERMARKT is active but no API key found — skipping Transfermarkt fetch for date=%s. "
                "Ensure 'transfermarkt' key exists in Secret Manager and is passed via api_keys.",
                date,
            )
        # Trigger-based: only fetch Transfermarkt on reference refresh dates
        # (season start, transfer window open/close) or when --force is set.
        _batch_dt = date_type.fromisoformat(date)
        _tm_trigger = is_any_league_refresh_date(_batch_dt) or redo_all
        if not _tm_trigger and transfermarkt_key:
            logger.info(
                "date=%s: not a reference refresh trigger — skipping Transfermarkt",
                date,
            )
        if (
            transfermarkt_key
            and _tm_trigger
            and (_entity_wanted("TRANSFERMARKT_LEAGUES") or _entity_wanted("PLAYER_VALUES"))
        ):
            try:
                tm_counts = await _fetch_transfermarkt_data(
                    date=date,
                    api_key=transfermarkt_key,
                    bucket=bucket,
                    entity_filter=_ef,
                    season=season_override,
                    force=redo_all,
                )
                for k, v in tm_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="transfermarkt_data_fetch",
                    shard=date,
                )

        sfi_key = (
            api_keys.get("soccer_football_info")
            if (api_keys and "SOCCER_FOOTBALL_INFO" in _active_venues_set)
            else None
        )
        if not sfi_key and "SOCCER_FOOTBALL_INFO" in _active_venues_set:
            logger.warning(
                "SOCCER_FOOTBALL_INFO is active but no API key found — skipping SFI fetch for date=%s.",
                date,
            )
        if sfi_key and (
            _entity_wanted("SFI_LEAGUES") or _entity_wanted("SFI_STANDINGS") or _entity_wanted("SFI_PROGRESSIVE_STATS")
        ):
            try:
                sfi_counts = await _fetch_sfi_data(
                    date=date,
                    api_key=sfi_key,
                    bucket=bucket,
                    entity_filter=_ef,
                    force=redo_all,
                )
                for k, v in sfi_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sfi_data_fetch",
                    shard=date,
                )

        if _entity_wanted("WEATHER") and (
            "OPEN_METEO" in _active_venues_set or sports_entity_filter == "WEATHER" or sports_provider == "OPEN_METEO"
        ):
            try:
                weather_counts = await _fetch_weather_data(date=date, bucket=bucket)
                for k, v in weather_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="weather_data_fetch",
                    shard=date,
                )

    # Weather: runs independently of API Football — no API key needed.
    # Must be outside the api_football_key gate so --sports-provider OPEN_METEO works.
    if is_sports and sports_provider == "OPEN_METEO":
        try:
            weather_counts = await _fetch_weather_data(date=date, bucket=bucket)
            for k, v in weather_counts.items():
                counts[k] = counts.get(k, 0) + v
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="weather_data_fetch",
                shard=date,
            )

    total = sum(counts.values())

    # 8. Shard completeness check + automatic retry for missing venues.
    # Expected = configured active_venues (from category config + launch date filter),
    # NOT what was fetched. If a venue returns 0 instruments (adapter error, network
    # failure), it must show up as missing — never silently pass.
    #
    # HOWEVER, venues are excluded from expected if:
    #  - Adapter ran OK (in _non_error_venues) but returned 0 records after date
    #    filtering — the data source simply has no data for that date (e.g. NASDAQ
    #    before DBEQ.BASIC dataset starts, or CME on a holiday).
    #  - Validation rejected all records (validation_failed_venues) — data quality
    #    issue, not a missing-data issue. Already logged as SHARD FAILED above.
    #  - [SPORTS] The venue doesn't cover any leagues with fixtures on this date.
    #    Each league declares its data_sources in UAC LeagueDefinition. A venue
    #    is only expected if at least one league with fixtures lists it.
    #
    # When venues are missing (typically due to API rate limits or transient errors),
    # retry just the missing venues with exponential backoff before failing.
    expected_venues = set(active_venues)
    written_venues = set(counts.keys())

    # Sports: scope expected venues by league coverage.
    # Understat covers ~6 leagues, FootyStats ~50, SFI varies.
    # Only expect a venue if it covers leagues that had fixtures today.
    #
    # Performance: same 25MB / 2.6M-row manifest read as the upstream
    # _date_fixture_leagues read (line ~1213) — invalidated by every
    # manifest.write() so it misses cache on every date in BatchIO. Skip
    # this read when scope is already explicit (sports_entity_filter or
    # recovery_fixture_ids set) — the per-fixture entity loop has already
    # decided what to fetch from the explicit scope; the venue-scoping
    # is only needed for full-spectrum runs that haven't pre-decided.
    if is_sports_run and not (sports_entity_filter or recovery_fixture_ids is not None):
        try:
            from unified_api_contracts.canonical.domain.sports.league_data import get_league

            # Get leagues with fixtures on this date from written data
            _fixture_leagues: set[str] = set()
            _sports_bucket = _get_instruments_bucket("SPORTS")
            if _sports_bucket:
                _idx = read_availability_index(_sports_bucket)
                if not _idx.empty and "league_id" in _idx.columns:
                    _fix_rows = _idx[(_idx["date"] == date) & (_idx["data_type"] == "FIXTURES")]
                    _fixture_leagues = {
                        str(lid).upper() for lid in _fix_rows["league_id"].dropna().unique() if str(lid).strip()
                    }

            if _fixture_leagues:
                # Build set of data_sources that cover at least one fixture league
                _active_sources: set[str] = set()
                for lid in _fixture_leagues:
                    league_def = get_league(lid)
                    if league_def is not None:
                        _active_sources |= league_def.data_sources
                    else:
                        # Unknown league — assume all sources needed
                        _active_sources |= {
                            "api_football",
                            "footystats",
                            "understat",
                            "transfermarkt",
                            "soccer_football_info",
                            "open_meteo",
                        }

                # Map data_source names to venue names and remove uncovered venues
                _sports_venues = {
                    "API_FOOTBALL",
                    "FOOTYSTATS",
                    "UNDERSTAT",
                    "TRANSFERMARKT",
                    "SOCCER_FOOTBALL_INFO",
                    "OPEN_METEO",
                }
                _uncovered = set()
                for venue in expected_venues & _sports_venues:
                    source_name = venue.lower()
                    if source_name not in _active_sources:
                        _uncovered.add(venue)

                if _uncovered:
                    logger.info(
                        "Sports league scoping: removing %d venue(s) not covering any fixture leagues: %s",
                        len(_uncovered),
                        sorted(_uncovered),
                    )
                    expected_venues -= _uncovered
        except Exception as _scope_exc:
            logger.debug("Sports league scoping skipped: %s", _scope_exc)

    # Venues where the adapter succeeded but no records survived date/relevance filtering
    # are not "missing" — the data source simply had nothing for this date.
    empty_ok_venues = (_non_error_venues - written_venues) - validation_failed_venues
    if empty_ok_venues:
        logger.info(
            "Shard completeness: %d venue(s) fetched OK but 0 records after filtering (excluded from expected): %s",
            len(empty_ok_venues),
            sorted(empty_ok_venues),
        )
    expected_venues -= empty_ok_venues
    expected_venues -= validation_failed_venues

    missing_shards = expected_venues - written_venues

    # Retry ONLY venues that failed with retryable errors (RATE_LIMIT, NETWORK, TIMEOUT,
    # SERVER_ERROR). Permanent failures (UNSUPPORTED, ADAPTER_ERROR, PARSE_ERROR) are not
    # retried — they'll fail the same way again.
    # Exponential backoff: 10s, 30s. Enough for rate limits to clear.
    retry_delays = [10, 30]
    retryable_set = set(_retryable_venues)
    for retry_idx, delay in enumerate(retry_delays):
        # Only retry venues that are both missing AND had retryable errors
        retry_candidates = missing_shards & retryable_set
        if not retry_candidates or not written_venues:
            break  # Nothing retryable, or total failure (retrying won't help)

        logger.warning(
            "Shard incomplete: %d/%d venues missing, %d retryable — retrying in %ds (attempt %d/%d): %s",
            len(missing_shards),
            len(expected_venues),
            len(retry_candidates),
            delay,
            retry_idx + 1,
            len(retry_delays),
            sorted(retry_candidates),
        )
        await asyncio.sleep(delay)

        # Re-fetch just the retryable venues
        retry_venues = sorted(retry_candidates)
        with SolanaCacheSession():
            retry_result = await fetch_instruments_for_all_venues(
                retry_venues,
                api_keys=api_keys,
                date=date,
                mode=mode,
            )
        retry_records = retry_result.records
        # Update retryable set from this attempt's failures
        retryable_set = (retryable_set - set(retry_venues)) | set(retry_result.retryable_venues)

        if not retry_records:
            logger.warning(
                "Retry %d/%d: still 0 records for %d venues",
                retry_idx + 1,
                len(retry_delays),
                len(retry_venues),
            )
            continue

        # Apply same pipeline: date filter → relevance filter → validation → write
        retry_records = filter_instruments_by_date(retry_records, date_dt, defi_venues=defi_venue_set)
        if any(c.upper() in ("DEFI", "ALL") for c in asset_groups):
            retry_records = filter_defi_instruments_by_relevance(retry_records)
        if retry_records:
            valid_retry, _ = validate_instrument_records(retry_records)
            if valid_retry:
                retry_rows = []
                for r in valid_retry:
                    d = r.model_dump()
                    if d.get("legs") is not None:
                        d["legs"] = json.dumps(d["legs"])
                    retry_rows.append(d)
                retry_df = pd.DataFrame(retry_rows)
                if "venue" in retry_df.columns:
                    retry_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
                    for venue_name, venue_df in retry_df.groupby("venue"):
                        _write_venue(str(venue_name), venue_df, date, bucket, sink, counts, sampler, retry_manifest)
                    retry_manifest.close()

        # Recalculate missing
        written_venues = set(counts.keys())
        missing_shards = expected_venues - written_venues
        recovered = len(retry_venues) - len(missing_shards & set(retry_venues))
        if recovered:
            logger.info(
                "Retry %d/%d: recovered %d/%d venues",
                retry_idx + 1,
                len(retry_delays),
                recovered,
                len(retry_venues),
            )
        total = sum(counts.values())

    # Final completeness assessment
    completeness_pct = int(len(written_venues) * 100 / len(expected_venues)) if expected_venues else 0

    if missing_shards:
        logger.error(
            "SHARD COMPLETENESS FAILURE date=%s: %d/%d venues written (%d%% complete), %d missing — %s",
            date,
            len(written_venues),
            len(expected_venues),
            completeness_pct,
            len(missing_shards),
            sorted(missing_shards),
        )
        log_event(
            "SHARD_INCOMPLETE",
            details={
                "date": date,
                "expected": len(expected_venues),
                "written": len(written_venues),
                "missing": sorted(missing_shards),
                "completeness_pct": completeness_pct,
            },
        )
        # Below 50% completeness = catastrophic failure (network outage, API down).
        # Fail the shard — the data is unusable and should not be treated as success.
        if completeness_pct < 50:
            msg = (
                f"SHARD CATASTROPHIC FAILURE date={date}: only {len(written_venues)}/{len(expected_venues)} "
                f"venues written ({completeness_pct}%). "
                f"Missing: {sorted(missing_shards)[:10]}{'...' if len(missing_shards) > 10 else ''}"
            )
            log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
            raise RuntimeError(msg)
    else:
        logger.info(
            "Shard completeness OK: %d/%d venues written for date=%s",
            len(written_venues),
            len(expected_venues),
            date,
        )

    # Emission policy check — PARTIAL_OK: emits PUBLISHED_DEGRADED when completeness < 1.0
    # but always allows write through. Per UAC seed Phase 6.8 PART B.
    _emission = _check_emission_policy(
        date=date,
        completeness_fraction=len(written_venues) / len(expected_venues) if expected_venues else 1.0,
    )
    logger.debug(
        "catalog_snapshot emission decision date=%s: %s (completeness=%.3f)",
        date,
        _emission.service_emission_state,
        _emission.completeness_fraction,
    )

    log_event(
        "PROCESSING_COMPLETED",
        details={"date": date, "total_records": total, "venues": len(counts)},
    )
    logger.info("instruments: date=%s wrote %d records across %d venues", date, total, len(counts))
    return counts


def _extract_prediction_canonical_group(row: pd.Series) -> str:
    """Map a PREDICTION instrument row onto a canonical-question-group name.

    Per the
    ``predictions_master_2026_05_07.plan.md`` Phase 1 critical-path
    todo: replace the legacy per-base_asset shard with the UAC
    canonical-question-group SSOT (``BTC_UP_DOWN_HOURLY``,
    ``BTC_UP_DOWN_DAILY``, ``SPX_UP_DOWN_DAILY``,
    ``ELECTION_PRESIDENT_2028``, etc.). Recurring market_ids cycle
    through canonical groups over time — HOURLY = ~24/day, DAILY = 1/day,
    macro-event = 1 over months. Bundling by canonical group is the
    options-chain-equivalent shape per CLAUDE.md "Per-asset-group
    shard-key matrix → Prediction".

    Polymarket: classifies via ``classify_polymarket_to_canonical_group``
    (override-first per ``POLYMARKET_CONDITION_ID_TO_GROUP``, fallback to
    rule-based slug-prefix path). Title + event_slug context isn't
    plumbed through the writer DataFrame today — the slug-prefix rule
    handles ~95% of real Polymarket slugs (``bitcoin-up-or-down-hour-*``,
    ``oscars-best-picture-2026``, etc.) on ``raw_symbol`` alone, and the
    ``OTHER`` catch-all bucket per UAC :class:`CanonicalQuestionGroup`
    captures the remainder so the data-status drilldown stays honest
    (per the predictions_master plan's C.12 "synthetic OTHER bucket"
    todo).

    Kalshi: classifies via ``classify_kalshi_to_canonical_group(ticker=...)``
    using the override-only path
    (:data:`unified_api_contracts.predictions.KALSHI_TICKER_TO_GROUP`).
    Unrecognised tickers route to ``OTHER``.

    Other venues route to ``OTHER`` defensively — should never trigger
    in practice because the writer only invokes this on rows with
    ``venue ∈ {POLYMARKET, kalshi}``.

    Returns the canonical-question-group string value (the
    ``CanonicalQuestionGroup`` enum's ``.value`` — used as the
    ``underlying`` slot on the manifest row + as the partition key on
    the GCS path).
    """
    venue_raw = str(row.get("venue", "")).strip()
    venue_upper = venue_raw.upper()
    if venue_upper == "POLYMARKET":
        condition_id = str(row.get("instrument_key", "") or "")
        slug = str(row.get("raw_symbol", "") or "")
        group = classify_polymarket_to_canonical_group(
            title="",
            slug=slug,
            event_slug="",
            outcome="",
            condition_id=condition_id,
        )
        return (group or CanonicalQuestionGroup.OTHER).value
    # Kalshi adapter ships venue as lowercase ``"kalshi"`` per
    # ``KalshiReferenceDataAdapter.venue``.
    if venue_upper == "KALSHI":
        ticker = str(row.get("instrument_key", "") or "")
        group = classify_kalshi_to_canonical_group(ticker=ticker)
        return (group or CanonicalQuestionGroup.OTHER).value
    return CanonicalQuestionGroup.OTHER.value


def _compute_prediction_shards(df: pd.DataFrame) -> dict[str, int]:
    """Group PREDICTION instruments by canonical-question-group, return
    ``{venue/group: count}``.

    Mirrors the per-row classifier output of
    :func:`_extract_prediction_canonical_group` — shard counts are keyed
    on the same string the manifest emits as ``underlying`` so the
    data-status drilldown sums up cleanly across the writer atomicity
    boundary + the manifest row key (per CLAUDE.md "Shard-granularity
    SSOT" requirement that the same atom appears at every layer).
    """
    shard_counts: dict[str, int] = {}
    for _, row in df.iterrows():
        group = _extract_prediction_canonical_group(row)
        venue_raw = str(row.get("venue", "")).strip().upper()
        key = f"{venue_raw}/{group}"
        shard_counts[key] = shard_counts.get(key, 0) + 1
    return shard_counts


def _write_venue(
    venue_str: str,
    df: pd.DataFrame,
    date: str,
    bucket: str,
    sink: DataSink,
    counts: dict[str, int],
    sampler: SamplingService,
    manifest: ManifestWriter | None = None,
) -> None:
    """Write one venue's DataFrame to storage, catalogue, and CSV sample.

    Retries transient GCS/network errors up to 3 times with exponential backoff
    (1s, 2s) to avoid wasting the expensive fetch work that produced this data.

    If ``manifest`` is provided, adds the catalogue record to the shared writer
    (caller flushes once after all venues). Otherwise uses the per-venue
    ``_write_catalogue_record`` path.
    """
    import time as _time

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            _gated_sink_write(
                sink,
                data=df,
                partition={"day": date, "venue": venue_str},
                filename="instruments.parquet",
                venue=venue_str,
                entity="instruments",
            )
            # Add to batched manifest writer (flushed by caller) or legacy per-venue write
            # v4: Sports reference entities write data_type (not venue).
            #     API_FOOTBALL → data_type=FIXTURES, venue=""
            #     API_FOOTBALL_INJURIES → data_type=INJURIES, venue=""
            #     Other asset groups keep venue as-is.
            _sports_prefixes = ("API_FOOTBALL", "TRANSFERMARKT", "FOOTYSTATS", "SFI", "UNDERSTAT", "WEATHER")
            is_sports_ref = venue_str.startswith(_sports_prefixes)
            if is_sports_ref:
                # Extract data_type: API_FOOTBALL_INJURIES → INJURIES, API_FOOTBALL → FIXTURES
                if venue_str == "API_FOOTBALL":
                    manifest_data_type = "FIXTURES"
                elif "_" in venue_str:
                    # Strip the provider prefix: API_FOOTBALL_INJURIES → INJURIES
                    for pfx in _sports_prefixes:
                        if venue_str.startswith(pfx + "_"):
                            manifest_data_type = venue_str[len(pfx) + 1 :]
                            break
                    else:
                        manifest_data_type = venue_str
                else:
                    manifest_data_type = venue_str
                manifest_venue = ""
            else:
                manifest_venue = venue_str
                manifest_data_type = ""
            # DeFi: split AAVEV3-ETHEREUM → venue=AAVEV3, chain=ETHEREUM per the
            # canonical v5 shard-key matrix (DeFi axis is `chain`, not packed
            # into venue). The path-based legacy writer at the bottom of this
            # module already does this; the batched manifest writer used here
            # was missing the split, so DeFi rows from the orchestrator landed
            # as `venue=AAVEV3-ETHEREUM, chain=''` and were filtered out by the
            # coverage-summary's legacy-row drop, hiding recent DeFi captures.
            manifest_chain = ""
            if not is_sports_ref and "-" in venue_str:
                from unified_api_contracts.registry.capability_declarations._defi import (
                    KNOWN_CHAINS,
                    parse_defi_venue,
                )

                try:
                    _protocol, _chain = parse_defi_venue(venue_str)
                except ValueError:
                    _protocol, _chain = "", ""
                if _chain in KNOWN_CHAINS:
                    manifest_venue = _protocol.upper()
                    manifest_chain = _chain
            if manifest is not None:
                _stamped_venue_df = stamp_available_at_explicit(df, when=datetime.now(UTC))
                if is_sports_ref:
                    try:
                        _venue_pm = _pipeline_mode_for_sports_data_type(manifest_data_type)
                    except KeyError:
                        _venue_pm = PipelineMode.BATCH_INSTRUMENTS_SERVICE
                    manifest.record_captured(
                        row_key={"date": date, "data_type": manifest_data_type},
                        df=_stamped_venue_df,
                        category="sports",
                        instrument_type="",
                        data_type=manifest_data_type,
                        pipeline_mode=_venue_pm,
                        service_emission_state=None,
                    )
                else:
                    _cat = "defi" if manifest_chain else ("tradfi" if venue_str in _TRADFI_VENUES else "cefi")
                    manifest.record_captured(
                        row_key={"date": date, "venue": manifest_venue, "chain": manifest_chain},
                        df=_stamped_venue_df,
                        category=_cat,
                        instrument_type="",
                        data_type="",
                        venue=manifest_venue,
                        chain=manifest_chain,
                        pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                        service_emission_state=None,
                    )
            else:
                path = f"instrument_availability/by_date/day={date}/venue={venue_str}/instruments.parquet"
                _write_catalogue_record(bucket, path, date, len(df))
            # CSV sample in dev mode — generate_csv_sample is the SamplingService API
            if sampler.enable_sampling:
                sampler.generate_csv_sample(df, filename_prefix=f"instruments_{venue_str}_{date}")
            counts[venue_str] = len(df)
            return  # success
        except (OSError, ConnectionError, TimeoutError) as exc:
            if attempt < max_attempts - 1:
                delay = 2**attempt  # 1s, 2s
                logger.warning(
                    "Write retry %d/%d for venue=%s date=%s (next in %ds): %s",
                    attempt + 1,
                    max_attempts,
                    venue_str,
                    date,
                    delay,
                    exc,
                )
                _time.sleep(delay)
            else:
                logger.error(
                    "Write FAILED after %d attempts for venue=%s date=%s: %s",
                    max_attempts,
                    venue_str,
                    date,
                    exc,
                )
                log_event(
                    "WRITE_FAILED",
                    details={"venue": venue_str, "date": date, "error": str(exc), "attempts": max_attempts},
                )
        except ValueError as exc:
            # Serialization/validation errors — not transient, don't retry
            logger.error("Write failed for venue=%s date=%s: %s", venue_str, date, exc)
            log_event("WRITE_FAILED", details={"venue": venue_str, "date": date, "error": str(exc)})
            return
    # Programming errors (TypeError, KeyError, etc.) propagate — fail the shard


def _write_venues_from_teams(teams_df: pd.DataFrame, bucket: str) -> None:
    """Extract venue metadata from teams and write a global venues.parquet.

    The features-sports-service reads venues from a flat path:
        sports_reference/venues/venues.parquet
    (not date-partitioned -- venues are slow-moving reference data).

    Venue coordinates are enriched by the API Football adapter via the UAC
    static venue coordinates registry. This function extracts the venue dict
    from each team row and writes a deduplicated venues table.
    """
    if "venue" not in teams_df.columns:
        logger.warning("No 'venue' column in teams_df — cannot extract venues")
        return

    venue_rows: list[dict[str, object]] = []
    for _, row in teams_df.iterrows():
        venue_data = row.get("venue")
        if not isinstance(venue_data, dict):
            continue
        venue_id = venue_data.get("venue_id")
        if not venue_id:
            continue
        venue_rows.append(
            {
                "venue_id": str(venue_id),
                "name": venue_data.get("name", ""),
                "city": venue_data.get("city"),
                "country": venue_data.get("country"),
                "capacity": venue_data.get("capacity"),
                "surface": venue_data.get("surface"),
                "latitude": venue_data.get("latitude"),
                "longitude": venue_data.get("longitude"),
            }
        )

    if not venue_rows:
        logger.warning("No venue data extracted from teams — skipping venues.parquet")
        return

    venues_df = pd.DataFrame(venue_rows).drop_duplicates(subset=["venue_id"])

    venues_sink = get_data_sink(bucket=bucket, prefix="sports_reference/venues")
    venues_sink.write(
        data=venues_df,
        partition={},
        format="parquet",
        filename="venues.parquet",
    )
    coords_count = int(venues_df["latitude"].notna().sum())
    logger.info(
        "Venues: %d unique venues written (%d with coordinates)",
        len(venues_df),
        coords_count,
    )


def _read_fixture_ids_from_gcs(bucket: str, date: str) -> list[int]:
    """Read completed fixture IDs from existing GCS fixtures parquet.

    Returns fixture IDs with status FT/AET/PEN. Falls back to empty list
    if no fixtures parquet exists for the date (zero-fixture day).
    """
    prefix = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
    try:
        storage_client = get_storage_client()
        blob = storage_client.bucket(bucket).blob(prefix)
        if not blob.exists():
            logger.debug("No fixtures parquet at gs://%s/%s", bucket, prefix)
            return []
        local = f"{tempfile.gettempdir()}/_fixture_ids_{date}.parquet"
        blob.download_to_filename(local)
        df = pd.read_parquet(local)
        completed = {"FT", "AET", "PEN"}
        if "status_short" in df.columns and "af_fixture_id" in df.columns:
            mask = df["status_short"].isin(completed)
            ids = df.loc[mask, "af_fixture_id"].dropna().astype(int).tolist()
            logger.info("GCS fixture lookup date=%s: %d completed fixture IDs", date, len(ids))
            return ids
        logger.debug("Fixtures parquet missing expected columns for date=%s", date)
        return []
    except Exception as exc:
        logger.debug("Failed to read fixtures from GCS for date=%s: %s", date, exc)
        return []


def _write_fixtures_per_league(
    sink: DataSink,
    fixture_df: pd.DataFrame,
    date: str,
    *,
    source_label: str,
) -> None:
    """Write canonical fixtures parquet per league (single-SSOT, no bare fallback).

    FIXTURES is a league-axis data type. We split the dataframe by league and
    write one parquet per partition — the bare-path date-aggregate is dropped
    per ``sports_manifest_single_ssot_2026_04_30``. When the dataframe is
    missing both ``league_id`` and ``af_league_id`` (or all rows lack a
    league assignment), the call logs a warning and skips the write to keep
    the manifest honest. Caller is responsible for emitting empty/failure
    manifest rows.
    """
    if fixture_df.empty:
        return

    # Prefer canonical league_id if it's already on the frame; otherwise
    # derive from af_league_id via the prediction-league reverse mapping.
    if "league_id" in fixture_df.columns and fixture_df["league_id"].notna().any():
        _league_col = "league_id"
        # Stringify, then blank-out empties so .notna() below treats them
        # as missing (matching the af_league_id branch's behaviour).
        _stringified = fixture_df["league_id"].astype(str)
        _league_series = _stringified.mask(_stringified.str.strip() == "")
    elif "af_league_id" in fixture_df.columns and fixture_df["af_league_id"].notna().any():
        _af_to_canonical: dict[int, str] = {}
        for _league_def in get_prediction_leagues():
            if _league_def.api_football_id is not None:
                _af_to_canonical[_league_def.api_football_id] = _league_def.league_id
        # af_league_id may be float-typed in pandas after read_parquet; coerce.
        _af_int_series = pd.to_numeric(fixture_df["af_league_id"], errors="coerce").astype("Int64")
        _league_col = "_canonical_league_id"
        fixture_df = fixture_df.copy()
        fixture_df[_league_col] = _af_int_series.map(lambda v: _af_to_canonical.get(int(v)) if pd.notna(v) else None)
        # Fallback for unmapped leagues — partition by stringified af_league_id.
        _missing_canonical = fixture_df[_league_col].isna() & _af_int_series.notna()
        if _missing_canonical.any():
            fixture_df.loc[_missing_canonical, _league_col] = _af_int_series[_missing_canonical].astype(str)
        _league_series = fixture_df[_league_col]
    else:
        logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "fixture_df missing both league_id and af_league_id columns (rows=%d). "
            "Skipping write to keep manifest honest.",
            date,
            source_label,
            len(fixture_df),
        )
        return

    _has_league = _league_series.notna() & (_league_series.astype(str).str.strip() != "")
    _with_league = fixture_df[_has_league]
    _without_league = fixture_df[~_has_league]

    if _with_league.empty:
        logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "no rows had a derivable league (rows=%d). Skipping write to keep manifest honest.",
            date,
            source_label,
            len(fixture_df),
        )
        return

    for _lid, _ldf in _with_league.groupby(_league_col):
        _lid_str = str(_lid)
        _ldf_clean = _ldf.drop(columns=["_canonical_league_id"], errors="ignore")
        _gated_sink_write(
            sink,
            data=_ldf_clean,
            partition={"day": date, "entity": "fixtures", "league": _canonical_league_id(_lid_str)},
            filename="fixtures.parquet",
            venue="api_football",
            entity="fixtures",
        )

    if not _without_league.empty:
        logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "%d rows missing league assignment. Skipping bare write to keep manifest honest.",
            date,
            source_label,
            len(_without_league),
        )


def _read_existing_per_league_fixture_ids(
    bucket: str,
    date: str,
    entity_name: str,
    canonical_league_id: str,
) -> frozenset[int]:
    """Return the set of af_fixture_ids already captured in a per-league parquet.

    Reads ``sports_reference/by_date/day={date}/entity={entity}/league={L}/{entity}.parquet``
    and returns ``frozenset({af_fixture_id, ...})`` of rows present. Used by the
    per-fixture pre-fetch skip path to avoid wasting api_football calls on
    fixtures whose data is already on disk.

    Returns empty frozenset on any miss / read failure (the caller treats that
    as "no captured fixtures known, fetch everything in scope"). Logs at debug
    level so operators can confirm the skip path engaged.
    """
    blob_path = (
        f"sports_reference/by_date/day={date}/entity={entity_name}/league={canonical_league_id}/{entity_name}.parquet"
    )
    try:
        storage_client = get_storage_client()
        blob = storage_client.bucket(bucket).blob(blob_path)
        if not blob.exists():
            return frozenset()
        existing_bytes = storage_client.download_bytes(bucket=bucket, blob_path=blob_path)
        existing = pd.read_parquet(io.BytesIO(existing_bytes))
    except Exception as exc:
        logger.debug(
            "Pre-fetch skip read failed for gs://%s/%s — proceeding without skip: %s",
            bucket,
            blob_path,
            exc,
        )
        return frozenset()
    fid_col = "af_fixture_id" if "af_fixture_id" in existing.columns else "fixture_id"
    if fid_col not in existing.columns:
        return frozenset()
    fids = pd.to_numeric(existing[fid_col], errors="coerce").dropna().astype(int)
    return frozenset(int(x) for x in fids.tolist())


def _merge_with_existing_per_league_parquet(
    bucket: str,
    date: str,
    entity_name: str,
    canonical_league_id: str,
    new_rows: pd.DataFrame,
    fid_col: str,
) -> pd.DataFrame:
    """Read-modify-write merge for recovery-mode per-league parquet writes.

    The default per-fixture entity write path overwrites the per-league
    parquet at ``sports_reference/by_date/day={date}/entity={entity}/league={L}/{entity}.parquet``.
    That's correct when we always fetch ALL fixtures for the (date, league)
    cell — but in recovery mode we only fetch a subset (the
    ``--recovery-fixture-ids`` allowlist), so a plain overwrite would drop
    the rest of the cell's previously-captured fixtures.

    This helper reads the existing parquet (if any), drops rows whose
    ``af_fixture_id`` matches our new_rows (we just refetched them, prefer
    the fresh values), and concatenates new_rows. Result: existing fixtures
    survive, our targeted re-fetches replace any stale rows for those
    fixture_ids.

    Returns the merged DataFrame ready for the standard sink write.
    """
    blob_path = (
        f"sports_reference/by_date/day={date}/entity={entity_name}/league={canonical_league_id}/{entity_name}.parquet"
    )
    try:
        storage_client = get_storage_client()
        blob = storage_client.bucket(bucket).blob(blob_path)
        if not blob.exists():
            return new_rows
        existing_bytes = storage_client.download_bytes(bucket=bucket, blob_path=blob_path)
        existing = pd.read_parquet(io.BytesIO(existing_bytes))
    except Exception as exc:
        logger.warning(
            "Recovery-mode merge: could not read existing parquet at gs://%s/%s — "
            "proceeding with overwrite (existing fixture rows for this cell will be lost): %s",
            bucket,
            blob_path,
            exc,
        )
        return new_rows

    if fid_col not in existing.columns:
        # Schema drift — existing parquet lacks the fixture_id column we'd dedup on.
        # Safer to overwrite + log than to concat-with-mismatched-schema.
        logger.warning(
            "Recovery-mode merge: existing parquet at gs://%s/%s missing %r column "
            "(found: %s) — overwriting rather than risk schema mismatch",
            bucket,
            blob_path,
            fid_col,
            list(existing.columns),
        )
        return new_rows

    new_fids = set(pd.to_numeric(new_rows[fid_col], errors="coerce").dropna().astype(int).tolist())
    existing_fid_int = pd.to_numeric(existing[fid_col], errors="coerce")
    keep_mask = ~existing_fid_int.isin(new_fids)
    survivors = existing[keep_mask]
    merged = pd.concat([survivors, new_rows], ignore_index=True, sort=False)
    logger.info(
        "Recovery-mode merge for %s/league=%s on %s: %d existing rows + %d new = %d total "
        "(replaced %d rows with same af_fixture_id)",
        entity_name,
        canonical_league_id,
        date,
        len(existing),
        len(new_rows),
        len(merged),
        len(existing) - len(survivors),
    )
    return merged


def _build_fixture_league_map_from_gcs(bucket: str, date: str) -> dict[str, str]:
    """Build a mapping from AF fixture_id (str) to canonical league_id.

    Reads the fixtures parquet from GCS (sports_reference/by_date/day={date}/entity=fixtures/)
    which contains af_fixture_id and league_id columns. Returns an empty dict if the
    fixtures file is missing or lacks the required columns.
    """
    # Build reverse mapping from UAC: af_league_id -> canonical league_id
    _af_league_to_canonical: dict[int, str] = {}
    for league_def in get_prediction_leagues():
        if league_def.api_football_id is not None:
            _af_league_to_canonical[league_def.api_football_id] = league_def.league_id

    prefix = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
    try:
        storage_client = get_storage_client()
        blob = storage_client.bucket(bucket).blob(prefix)
        if not blob.exists():
            logger.debug("No fixtures parquet at gs://%s/%s for league mapping", bucket, prefix)
            return {}
        local = f"{tempfile.gettempdir()}/_fixture_league_map_{date}.parquet"
        blob.download_to_filename(local)
        df = pd.read_parquet(local)
        result: dict[str, str] = {}
        if "af_fixture_id" in df.columns:
            # Prefer league_id column if present
            if "league_id" in df.columns:
                for _, row in df[["af_fixture_id", "league_id"]].dropna().iterrows():
                    result[str(int(row["af_fixture_id"]))] = str(row["league_id"])
            # Fallback: use af_league_id -> canonical mapping
            elif "af_league_id" in df.columns:
                for _, row in df[["af_fixture_id", "af_league_id"]].dropna().iterrows():
                    af_lid = int(row["af_league_id"])
                    canonical = _af_league_to_canonical.get(af_lid)
                    if canonical:
                        result[str(int(row["af_fixture_id"]))] = canonical
        logger.info("Fixture league map: %d mappings built from GCS for date=%s", len(result), date)
        return result
    except Exception as exc:
        logger.debug("Failed to build fixture league map for date=%s: %s", date, exc)
        return {}


async def _fetch_sports_reference_data(
    date: str,
    api_key: str,
    bucket: str,
    entities_to_fetch: list[str] | None = None,
    enrichment_only: bool = False,
    fixture_ids_override: list[int] | None = None,
    manifest: ManifestWriter | None = None,
    recovery_fixture_ids: frozenset[int] | None = None,
    redo_all: bool = False,
) -> dict[str, int]:
    """Fetch sports reference data (teams, leagues, standings, injuries, etc.).

    Calls USRI api_football adapter for enrichment data that describes the
    instruments (fixtures). Written to the same bucket as instruments under
    separate hive-partitioned prefixes:
        sports_reference/by_date/day={date}/entity={type}/{type}.parquet
        sports_reference/mappings/team_mapping.parquet
        sports_reference/mappings/fixture_mapping.parquet

    This data is slow-moving (leagues/teams change per season, not per day)
    but we re-fetch on each run to capture mid-season transfers, promotions,
    and new referee assignments.

    Args:
        entities_to_fetch: Specific manifest entity names to fetch (e.g.
            ["FIXTURE_LINEUPS", "PLAYER_STATS"]).
            When provided, only these entities are fetched — all others skipped.
            None = fetch everything (legacy behaviour).
        enrichment_only: If True, skip core entities. Superseded by entities_to_fetch.
        fixture_ids_override: Pre-computed list of completed fixture IDs from the
            URDI instruments fetch or GCS lookup. When provided, skips the expensive
            33-league re-fetch (saves 33 API calls per date).
        recovery_fixture_ids: af_fixture_id allowlist for targeted per-fixture
            recovery. When set, the per-fixture entity loop (PLAYER_STATS /
            FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS) intersects
            ``fixture_ids`` with this set, and the per-league parquet writes
            do read-modify-write merges so existing fixtures' rows are
            preserved.
    """
    # Convert entities_to_fetch to a set of short entity names for easy lookup.
    # E.g. "FIXTURE_LINEUPS" → "fixture_lineups"
    _fetch_set: set[str] | None = None
    if entities_to_fetch:
        _fetch_set = set()
        for e in entities_to_fetch:
            short = e.replace("API_FOOTBALL_", "").lower()
            _fetch_set.add(short)
        # If only per-fixture entities requested, set enrichment_only
        core_shorts = {"leagues", "teams", "standings", "injuries"}
        if not (_fetch_set & core_shorts):
            enrichment_only = True
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # Honest-coverage helper: only record when an external manifest is wired
    # in by the caller (existing call-sites always pass one, but the default
    # signature keeps it optional for legacy use).
    _af_attempt_ts = datetime.now(UTC)

    def _af_record_failed(data_type: str, exc: Exception, league_id: str = "") -> None:
        if manifest is None:
            return
        _err_code = _classify_adapter_failure(exc, "api_football")
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "api_football",
                "endpoint": data_type.lower(),
                "date": date,
                "league_id": league_id,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        _row_key: dict[str, str] = {"date": date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=_af_attempt_ts,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )

    def _af_record_empty(data_type: str, league_id: str = "", reason: str = "") -> None:
        if manifest is None:
            return
        _row_key: dict[str, str] = {"date": date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        manifest.record_empty(
            row_key=_row_key,
            attempted_at=_af_attempt_ts,
            reason=reason,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )

    def _af_emit_empty_gaps_for_entity(data_type: str, captured_league_ids: set[str]) -> None:
        """Emit empty_confirmed per expected league with no captured rows (same contract as FIXTURES)."""
        if manifest is None:
            return
        _expected = {lg.league_id for lg in get_expected_leagues_for_source("api_football")}
        for _exp_lid in sorted(_expected - captured_league_ids):
            if not get_league_fixture_calendar(_exp_lid, date, date):
                continue
            manifest.record_empty(
                row_key={"date": date, "data_type": data_type, "league_id": _exp_lid},
                attempted_at=_af_attempt_ts,
                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            )

    def _should_fetch(entity_short: str) -> bool:
        """Check if this entity should be fetched (not in _fetch_set or _fetch_set is None)."""
        if _fetch_set is None:
            return True
        return entity_short in _fetch_set

    if enrichment_only:
        logger.info("Enrichment-only mode: skipping leagues/teams/standings/injuries for date=%s", date)

    # LEAGUES write path retired 2026-05-07 (C.1 audit, manifest_migration_master).
    # Replaced by UAC ``LeagueDefinition`` + ``provider_league_ids`` (FOOTYSTATS_SEASON_IDS,
    # FOOTYSTATS_HISTORICAL_SEASON_IDS) which canonicalise the league refdata via code
    # commits — no daily-cadence GCS dump needed. Downstream consumers were 100%
    # schema-only declarations (features-sports LEAGUES_COLUMNS) — no actual feature
    # consumed `logo_url` or other fields beyond what UAC already provides.
    # The api_football `/leagues` endpoint is no longer called from the daily
    # orchestrator path; teams fetch (below) reads `get_prediction_leagues()` from UAC
    # instead of the freshly-fetched leagues_df.

    # Teams — for each prediction league (cached across dates)
    if not enrichment_only and _should_fetch("leagues"):
        teams_df = _cached_teams_df
        prediction_league_ids: list[int] = []
        if teams_df is None:
            all_teams: list[dict[str, object]] = []
            try:
                for league_def in get_prediction_leagues():
                    if league_def.api_football_id is None:
                        continue
                    prediction_league_ids.append(league_def.api_football_id)
                    try:
                        teams = await adapter.get_teams(league_def.api_football_id)
                        for t in teams:
                            row = _coerce_adapter_output(t)
                            # Tag each team row with the league_id for per-league partitioning
                            row["league_id"] = league_def.league_id
                            all_teams.append(row)
                    except Exception as exc:
                        classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sports_reference_teams_fetch",
                            shard=str(league_def.league_id),
                        )
                        _af_record_failed("TEAMS", exc, league_id=league_def.league_id)
                if all_teams:
                    teams_df = pd.DataFrame(all_teams)
                    _set_cached_teams(teams_df, prediction_league_ids)
                    logger.info("Sports reference: %d teams fetched (API calls — will cache)", len(teams_df))
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_reference_teams_batch",
                )
                _af_record_failed("TEAMS", exc)
        else:
            prediction_league_ids = _cached_prediction_league_ids
            logger.info("Sports reference: %d teams from cache (0 API calls)", len(teams_df))
        if teams_df is not None:
            # Write per-league partitioned team files. The bare-path fallback
            # was retired in sports_manifest_single_ssot_2026_04_30 — TEAMS is
            # a league-axis data type and MUST always carry league_id.
            if "league_id" in teams_df.columns:
                for _t_lid, _t_league_df in teams_df.groupby("league_id"):
                    _t_lid_str = str(_t_lid)
                    _gated_sink_write(
                        sink,
                        data=_t_league_df,
                        partition={"day": date, "entity": "teams", "league": _canonical_league_id(_t_lid_str)},
                        filename="teams.parquet",
                        venue="api_football",
                        entity="teams",
                    )
            else:
                logger.warning(
                    "TEAMS bare-path fallback triggered for date=%s — data shape regression: "
                    "teams_df missing league_id column (rows=%d). Skipping write to keep manifest honest.",
                    date,
                    len(teams_df),
                )
            counts["teams"] = len(teams_df)

            # Extract venues from teams and write a global venues.parquet.
            # The features-sports-service reads this at:
            #   sports_reference/venues/venues.parquet
            # Venue coordinates come from the UAC static registry (enriched
            # by _parse_team_item when get_teams() is called).
            _write_venues_from_teams(teams_df, bucket)

        # Standings — for each prediction league (cached across dates)
        standings_df = _cached_standings_df
        if standings_df is None:
            all_standings: list[dict[str, object]] = []
            for lid in prediction_league_ids:
                try:
                    standings = await adapter.get_standings(lid)
                    for row in standings:
                        d = row.model_dump() if hasattr(row, "model_dump") else row
                        all_standings.append(d)
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sports_reference_standings_fetch",
                        shard=str(lid),
                    )
                    _af_record_failed("STANDINGS", exc, league_id=str(lid))
            if all_standings:
                standings_df = pd.DataFrame(all_standings)
                _set_cached_standings(standings_df)
                logger.info("Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df))
        else:
            logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
        if standings_df is not None:
            # Write per-league partitioned standings files + per-league manifest rows.
            if "league_id" in standings_df.columns:
                _std_captured: set[str] = set()
                for _s_lid, _s_league_df in standings_df.groupby("league_id"):
                    _s_lid_str = str(_s_lid)
                    _std_captured.add(_s_lid_str)
                    _gated_sink_write(
                        sink,
                        data=_s_league_df,
                        partition={"day": date, "entity": "standings", "league": _canonical_league_id(_s_lid_str)},
                        filename="standings.parquet",
                        venue="api_football",
                        entity="standings",
                    )
                    if manifest is not None:
                        _stamped_std_df = stamp_available_at_explicit(_s_league_df, when=datetime.now(UTC))
                        manifest.record_captured(
                            row_key={
                                "date": date,
                                "data_type": "STANDINGS",
                                "league_id": _canonical_league_id(_s_lid_str),
                            },
                            df=_stamped_std_df,
                            category="sports",
                            instrument_type="",
                            data_type="STANDINGS",
                            league_id=_canonical_league_id(_s_lid_str),
                            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                            service_emission_state=None,
                        )
                if manifest is not None:
                    _af_emit_empty_gaps_for_entity("STANDINGS", _std_captured)
            else:
                logger.warning(
                    "STANDINGS bare-path fallback triggered for date=%s — data shape regression: "
                    "standings_df missing league_id column (rows=%d). Skipping write to keep manifest honest.",
                    date,
                    len(standings_df),
                )
            counts["standings"] = len(standings_df)

    # Injuries — date-specific, always fetched fresh.
    # IMPORTANT: outside the leagues/teams/standings block so it runs even when
    # only injuries is requested (entities_to_fetch=["API_FOOTBALL_INJURIES"]).
    if not enrichment_only and _should_fetch("injuries"):
        try:
            injuries = await adapter.get_injuries(date)
            if injuries:
                df = pd.DataFrame([_coerce_adapter_output(inj) for inj in injuries])
                # PIT safety: daily injuries published morning-of (date + 12:00 UTC)
                df["data_available_at"] = pd.Timestamp(date, tz="UTC") + pd.Timedelta(hours=12)
                counts["injuries"] = len(df)

                # Determine league column — prefer league_id, fallback to fixture_id prefix
                _inj_league_col: str | None = None
                if "league_id" in df.columns and df["league_id"].notna().any():
                    _inj_league_col = "league_id"
                elif "fixture_id" in df.columns and df["fixture_id"].notna().any():
                    # Try canonical fixture_id format (LEAGUE:HOME_v_AWAY:DATE)
                    _sample = df["fixture_id"].dropna().iloc[0] if not df["fixture_id"].dropna().empty else ""
                    if ":" in str(_sample):
                        df["_inj_league"] = df["fixture_id"].str.split(":").str[0]
                        _inj_league_col = "_inj_league"

                if _inj_league_col is not None:
                    _has_league = df[_inj_league_col].notna() & (df[_inj_league_col] != "")
                    _with_league = df[_has_league]
                    _without_league = df[~_has_league]
                    _inj_captured: set[str] = set()

                    for _inj_lid, _inj_league_df in _with_league.groupby(_inj_league_col):
                        _inj_lid_str = str(_inj_lid)
                        _inj_captured.add(_inj_lid_str)
                        _inj_clean = _inj_league_df.drop(columns=["_inj_league"], errors="ignore")
                        _gated_sink_write(
                            sink,
                            data=_inj_clean,
                            partition={"day": date, "entity": "injuries", "league": _canonical_league_id(_inj_lid_str)},
                            filename="injuries.parquet",
                            venue="api_football",
                            entity="injuries",
                        )
                        if manifest is not None:
                            _stamped_inj_df = stamp_available_at_explicit(_inj_clean, when=datetime.now(UTC))
                            manifest.record_captured(
                                row_key={
                                    "date": date,
                                    "data_type": "INJURIES",
                                    "league_id": _canonical_league_id(_inj_lid_str),
                                },
                                df=_stamped_inj_df,
                                category="sports",
                                instrument_type="",
                                data_type="INJURIES",
                                league_id=_canonical_league_id(_inj_lid_str),
                                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                                service_emission_state=None,
                            )

                    if not _without_league.empty:
                        logger.warning(
                            "INJURIES bare-path fallback triggered for date=%s — data shape regression: "
                            "%d rows missing league_id (could not derive from fixture_id prefix). "
                            "Skipping bare write to keep manifest honest.",
                            date,
                            len(_without_league),
                        )
                    if manifest is not None:
                        _af_emit_empty_gaps_for_entity("INJURIES", _inj_captured)
                else:
                    logger.warning(
                        "INJURIES bare-path fallback triggered for date=%s — data shape regression: "
                        "no league_id column AND no fixture_id-prefix-derivable league (rows=%d). "
                        "Skipping bare write to keep manifest honest.",
                        date,
                        len(df),
                    )

                logger.info("Sports reference: %d injuries written", len(df))
            else:
                # Honest-coverage: legitimate zero-injuries day for this date
                # (no players on the season-wide injuries list have a reported
                # status — common on off-season days).  Per
                # ``sports_manifest_single_ssot_2026_04_30`` we no longer write
                # an empty bare parquet — record_empty per league (handled by
                # the _af_emit_empty_gaps_for_entity call below) is sufficient.
                counts["injuries"] = 0
                logger.info("Sports reference: 0 injuries returned by API")
                # Honest-coverage: legitimate zero-injuries day for this date
                # (no players on the season-wide injuries list have a reported
                # status — common on off-season days).  Emit empty_confirmed
                # instead of captured(0) so the data-status page distinguishes
                # "source said zero" from "we wrote zero rows".
                _af_emit_empty_gaps_for_entity("INJURIES", set())
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_injuries_fetch",
                shard=date,
            )
            _af_record_failed("INJURIES", exc)

    # Per-fixture enrichment: stats, events, lineups, player stats.
    # Only for completed fixtures (status in FT/AET/PEN — stats unavailable for future/live).
    # Use fixture_ids_override when available (from URDI fetch) to avoid redundant
    # 33-league re-fetch (saves 33 API calls per date).
    fixture_ids: list[int] = []
    # Mapping from AF fixture ID (str) to canonical league_id for per-league writes.
    _af_fid_to_league: dict[str, str] = {}
    if fixture_ids_override is not None:
        fixture_ids = fixture_ids_override
        logger.info("Sports reference: %d completed fixture IDs passed from URDI (0 extra API calls)", len(fixture_ids))
        # Build AF fixture_id -> league mapping from GCS fixtures parquet
        _af_fid_to_league = _build_fixture_league_map_from_gcs(bucket, date)

        # Ensure canonical fixtures exist at sports_reference/by_date/entity=fixtures/.
        # The URDI phase writes instrument records, but features-sports needs the
        # canonical fixture format (af_fixture_id, timestamp, home/away names, etc.).
        # Read from the old path (sports_reference/fixtures/day=) or fetch from API.
        _new_fixtures_path = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
        try:
            _storage = get_storage_client()
            _new_blob = _storage.bucket(bucket).blob(_new_fixtures_path)
            # Check if new path already has canonical data (not instrument records)
            _needs_write = True
            if _new_blob.exists():
                _existing = pd.read_parquet(
                    io.BytesIO(_storage.download_bytes(bucket=bucket, blob_path=_new_fixtures_path))
                )
                if "af_fixture_id" in _existing.columns or "timestamp" in _existing.columns:
                    _needs_write = False  # Already canonical format

            if _needs_write:
                # Try old path first (zero API calls)
                _old_path = f"sports_reference/fixtures/day={date}/fixtures.parquet"
                _old_blob = _storage.bucket(bucket).blob(_old_path)
                if _old_blob.exists():
                    _old_data = _storage.download_bytes(bucket=bucket, blob_path=_old_path)
                    _old_df = pd.read_parquet(io.BytesIO(_old_data))
                    _ref_sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
                    _write_fixtures_per_league(_ref_sink, _old_df, date, source_label="old-path-copy")
                    logger.info(
                        "Canonical fixtures copied from old path to entity=fixtures/ (%d rows)",
                        len(_old_df),
                    )
                else:
                    # No old path — fetch from API Football (costs 33 API calls)
                    _adapter = create_sports_reference_adapter("api_football", api_key=api_key)
                    _fx_list = await _adapter.get_fixtures(date)
                    if _fx_list:
                        _fx_dicts = [_flatten_canonical_fixture_for_disk(fx, date) for fx in _fx_list]
                        _fx_df = pd.DataFrame(_fx_dicts)
                        # PIT safety: scheduled fixtures published ~1 week before kickoff
                        if "timestamp" in _fx_df.columns:
                            _fx_df["data_available_at"] = pd.to_datetime(
                                _fx_df["timestamp"], utc=True, errors="coerce"
                            ) - pd.Timedelta(days=7)
                        _ref_sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
                        _write_fixtures_per_league(_ref_sink, _fx_df, date, source_label="api-fetch-override")
                        logger.info(
                            "Canonical fixtures fetched from API and written to entity=fixtures/ (%d fixtures)",
                            len(_fx_df),
                        )
        except Exception as _fx_exc:
            logger.warning("Could not ensure canonical fixtures at entity=fixtures/: %s", _fx_exc)
    else:
        # Fallback: fetch fixtures from API (33 calls for 33 leagues).
        # Only used when called from the zero-fixture early-return path
        # where URDI returned 0 instruments.
        completed_statuses = {"FT", "AET", "PEN"}
        fallback_league_ids: list[int] = []
        _af_id_to_canonical_league: dict[int, str] = {}
        # Fetch fixtures for ALL football leagues (prediction + features + reference).
        # Reference leagues (cups, continental) provide team workload context for
        # fatigue/distance calculations. Features leagues (lower divisions) provide
        # additional fixture data for cross-division team tracking.
        from unified_api_contracts.canonical.domain.sports.league_data import get_leagues_by_classification

        for cls in ("Prediction", "Features", "Reference"):
            for league_def in get_leagues_by_classification(cls):
                if league_def.api_football_id is not None:
                    fallback_league_ids.append(league_def.api_football_id)
                    _af_id_to_canonical_league[league_def.api_football_id] = league_def.league_id
        try:
            fixtures = await adapter.get_fixtures(date, league_ids=fallback_league_ids)
            for fx in fixtures:
                if fx.status in completed_statuses:
                    raw_id = fx.source_fixture_id or fx.fixture_id
                    with contextlib.suppress(ValueError, TypeError):
                        fid_int = int(raw_id)
                        fixture_ids.append(fid_int)
                        # Map AF ID -> league from the fixture's league object
                        if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                            _af_fid_to_league[str(fid_int)] = str(fx.league.league_id)
                        elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                            af_lid = fx.league.api_football_id
                            if af_lid in _af_id_to_canonical_league:
                                _af_fid_to_league[str(fid_int)] = _af_id_to_canonical_league[af_lid]
                elif fx.status in {"PST", "CANC"}:
                    _reason = (
                        EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED
                        if fx.status == "PST"
                        else EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED
                    )
                    _lid: str = ""
                    if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                        _lid = str(fx.league.league_id)
                    elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                        af_lid = fx.league.api_football_id
                        _lid = _af_id_to_canonical_league.get(af_lid, "")
                    _af_record_empty("FIXTURES", league_id=_lid, reason=str(_reason))
            logger.info("Sports reference: %d completed fixtures found for enrichment (API fetch)", len(fixture_ids))

            # Write canonical fixtures to sports_reference/by_date/entity=fixtures/
            # so features-sports-service and trigger scheduler can read them.
            if fixtures:
                try:
                    fixture_dicts = [_flatten_canonical_fixture_for_disk(fx, date) for fx in fixtures]
                    fixture_df = pd.DataFrame(fixture_dicts)
                    # PIT safety: scheduled fixtures published ~1 week before kickoff
                    if "timestamp" in fixture_df.columns:
                        fixture_df["data_available_at"] = pd.to_datetime(
                            fixture_df["timestamp"], utc=True, errors="coerce"
                        ) - pd.Timedelta(days=7)
                    _fix_ref_sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
                    _write_fixtures_per_league(_fix_ref_sink, fixture_df, date, source_label="api-fetch-fallback")
                    logger.info(
                        "Canonical fixtures written to sports_reference/by_date/entity=fixtures/ (%d fixtures)",
                        len(fixture_df),
                    )
                except Exception as _fx_write_exc:
                    logger.warning("Failed to write canonical fixtures to reference path: %s", _fx_write_exc)

        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_fixtures_fetch",
                shard=date,
            )
            _af_record_failed("FIXTURES", exc)

    # Recovery-mode fixture-id allowlist filter — runs BEFORE the per-fixture
    # entity loop so we only call api_football for the targeted set. Lifts
    # the per-fixture work from O(all_fixtures_on_day x 5 entities) to
    # O(allowlist_intersection_with_day x N_requested_entities). Used for
    # targeted recovery (e.g. Phase 2's truth-set audit produced a 39k
    # fixture-id list; we feed it here so we don't re-burn ~560k api_football
    # calls re-fetching already-captured fixtures' per-fixture entities).
    if recovery_fixture_ids is not None and fixture_ids:
        _pre_filter = len(fixture_ids)
        fixture_ids = [fid for fid in fixture_ids if fid in recovery_fixture_ids]
        logger.info(
            "Recovery fixture-id filter applied for date=%s: %d → %d fixtures (%d skipped — not in allowlist)",
            date,
            _pre_filter,
            len(fixture_ids),
            _pre_filter - len(fixture_ids),
        )
        if not fixture_ids:
            # Allowlist intersected to zero on this date — no per-fixture work
            # to do. Return early so we don't write phantom empty manifest rows
            # for entities we never attempted to fetch on this date.
            logger.info(
                "Recovery fixture-id filter: no targeted fixtures on date=%s — skipping per-fixture loop",
                date,
            )
            # Cross-provider mapping tables can still be useful here, but skip
            # them in recovery mode to keep the run cheap.
            return counts

    if fixture_ids:
        _per_fixture_entities = [
            ("fixture_stats", adapter.get_fixture_statistics),
            ("fixture_events", adapter.get_fixture_events),
            ("fixture_lineups", adapter.get_fixture_lineups),
            ("player_stats", adapter.get_fixture_player_stats),
        ]

        # Filter to only fetch entities that are actually missing
        if _fetch_set is not None:
            _per_fixture_entities = [(name, fn) for name, fn in _per_fixture_entities if _should_fetch(name)]
            skipped = 4 - len(_per_fixture_entities)
            if skipped:
                logger.info(
                    "Per-fixture: skipping %d entities already in manifest, fetching %s",
                    skipped,
                    [n for n, _ in _per_fixture_entities],
                )

        # Concurrent per-fixture fetching with rate-limit semaphore.
        # API Football Mega plan: 900 req/min. With multiple processes sharing
        # the key, cap per-process concurrency at 50 to leave headroom.
        # The adapter's _get_with_retry reads X-RateLimit-Remaining and
        # preemptively sleeps when near-exhausted.
        concurrency = 50
        sem = asyncio.Semaphore(concurrency)
        entity_rows: dict[str, list[dict[str, object]]] = {name: [] for name, _ in _per_fixture_entities}
        # Per-entity failure tracking for honest-coverage: map entity → (failed_count, sample_error_code).
        entity_failures: dict[str, tuple[int, str]] = {name: (0, "") for name, _ in _per_fixture_entities}

        async def _fetch_one(entity_name: str, fetch_fn: object, fid: int) -> None:
            async with sem:
                try:
                    rows = await fetch_fn(fid)  # type: ignore[operator]
                    for row in rows:
                        # Adapters return a mix of Pydantic models and plain dicts
                        # depending on whether the normalizer produces a typed model.
                        d = row.model_dump() if hasattr(row, "model_dump") else row
                        entity_rows[entity_name].append(d)
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation=f"sports_reference_{entity_name}_fetch",
                        shard=str(fid),
                    )
                    # Shard-level failure isolation: count the failure so that
                    # honest-coverage can record_failed at entity-level if
                    # EVERY fixture call for this entity raised.
                    _prev_count, _prev_code = entity_failures[entity_name]
                    entity_failures[entity_name] = (
                        _prev_count + 1,
                        _prev_code or _classify_adapter_failure(exc, "api_football"),
                    )
                # Throttle handled by adapter's _get_with_retry + rate limit headers

        # Pre-fetch skip: read existing per-league parquet for each (entity, league)
        # cell on this date, build the set of af_fixture_ids already captured, and
        # skip api_football calls for those fixtures. Bypassed when ``redo_all`` is
        # True (i.e. the operator passed --force, explicitly asking to re-fetch
        # everything).
        #
        # Why this exists: today's manifest is keyed on (date, data_type, league_id)
        # — it tracks "the cell is captured" but NOT which fixtures within the cell
        # are captured. So the cell-level pre-flight (at orchestrator entry) can't
        # tell "5 of 10 fixtures already done" from "all 10 already done." The fix:
        # at fetch-time, read the per-league parquet (which IS keyed at
        # af_fixture_id row granularity) and skip api calls for fixtures already
        # represented. Generalises to any future per-fixture entity recovery —
        # e.g. when downstream of recovered FIXTURES, only the genuinely-missing
        # fixtures get re-fetched, not the entire cell.
        captured_per_entity_league: dict[tuple[str, str], frozenset[int]] = {}
        if not redo_all and _af_fid_to_league:
            for entity_name, _ in _per_fixture_entities:
                _entity_leagues_seen: set[str] = set()
                for fid in fixture_ids:
                    canonical_league = _af_fid_to_league.get(str(fid))
                    if not canonical_league:
                        continue
                    canonical_league = _canonical_league_id(canonical_league)
                    if canonical_league in _entity_leagues_seen:
                        continue
                    _entity_leagues_seen.add(canonical_league)
                    captured_set = _read_existing_per_league_fixture_ids(
                        bucket=bucket,
                        date=date,
                        entity_name=entity_name,
                        canonical_league_id=canonical_league,
                    )
                    captured_per_entity_league[(entity_name, canonical_league)] = captured_set

        # Build all tasks: N entities x M fixtures (only missing entities)
        tasks: list[asyncio.Task[None]] = []
        skipped_already_captured = 0
        for entity_name, fetch_fn in _per_fixture_entities:
            for fid in fixture_ids:
                if not redo_all and captured_per_entity_league:
                    canonical_league = _af_fid_to_league.get(str(fid))
                    if canonical_league:
                        canonical_league = _canonical_league_id(canonical_league)
                        captured_set = captured_per_entity_league.get((entity_name, canonical_league), frozenset())
                        if int(fid) in captured_set:
                            skipped_already_captured += 1
                            continue
                tasks.append(asyncio.ensure_future(_fetch_one(entity_name, fetch_fn, fid)))

        if skipped_already_captured:
            logger.info(
                "Per-fixture pre-fetch skip: %d (entity, fixture_id) pairs already in existing per-league "
                "parquets — skipping api_football calls (pass --force to re-fetch regardless)",
                skipped_already_captured,
            )
        logger.info(
            "Per-fixture enrichment: %d fixtures x %d entities = %d calls queued (concurrency=%d, "
            "skipped_already_captured=%d)",
            len(fixture_ids),
            len(_per_fixture_entities),
            len(tasks),
            concurrency,
            skipped_already_captured,
        )
        await asyncio.gather(*tasks)

        _entity_dt_by_short = {
            "fixture_stats": "FIXTURE_STATS",
            "fixture_events": "FIXTURE_EVENTS",
            "fixture_lineups": "FIXTURE_LINEUPS",
            "player_stats": "PLAYER_STATS",
        }

        for entity_name, _ in _per_fixture_entities:
            _af_entity_dt = _entity_dt_by_short[entity_name]
            all_rows = entity_rows[entity_name]
            if all_rows:
                df = pd.DataFrame(all_rows)

                # Drop columns containing nested structures (lists/dicts) that
                # cannot be serialised to Parquet.  API Football player_stats
                # responses may carry a raw "statistics" column with nested
                # dicts even after normalisation.
                _nested_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, (dict, list))).any()]
                if _nested_cols:
                    logger.info(
                        "Dropping %d nested columns from %s: %s",
                        len(_nested_cols),
                        entity_name,
                        _nested_cols,
                    )
                    df = df.drop(columns=_nested_cols)

                # PIT safety: per-fixture stats/events/lineups/player_stats available ~2h after kickoff.
                # No per-row kickoff here — approximate using date + 17:00 UTC (15:00 typical KO + 2h).
                df["data_available_at"] = pd.Timestamp(date, tz="UTC") + pd.Timedelta(hours=17)

                counts[entity_name] = len(df)

                # Write per-league partitioned files using AF fixture_id -> league mapping.
                # Column name is "af_fixture_id" (not "fixture_id") in per-fixture entity data.
                _fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
                if _fid_col in df.columns and _af_fid_to_league:
                    # Ensure string type for map lookup (map keys are str(int(af_id)))
                    df["_league_id"] = df[_fid_col].astype(str).str.split(".").str[0].map(_af_fid_to_league)
                    _has_league = df["_league_id"].notna()
                    _with_league = df[_has_league]
                    _without_league = df[~_has_league]
                    _pf_captured: set[str] = set()

                    for _pf_lid, _pf_league_df in _with_league.groupby("_league_id"):
                        _pf_lid_str = str(_pf_lid)
                        _pf_captured.add(_pf_lid_str)
                        _pf_clean = _pf_league_df.drop(columns=["_league_id"])

                        # Recovery mode: read existing per-league parquet (if
                        # any) and merge our newly-fetched fixture rows so we
                        # don't lose previously-captured fixtures' data. The
                        # standard write path is overwrite-on-write, which is
                        # safe when we always fetch ALL fixtures for the
                        # (date, league) cell — but in recovery mode we only
                        # fetched a subset, so a plain overwrite would drop
                        # the rest of the cell.
                        if recovery_fixture_ids is not None and _fid_col in _pf_clean.columns:
                            _pf_clean = _merge_with_existing_per_league_parquet(
                                bucket=bucket,
                                date=date,
                                entity_name=entity_name,
                                canonical_league_id=_canonical_league_id(_pf_lid_str),
                                new_rows=_pf_clean,
                                fid_col=_fid_col,
                            )

                        _gated_sink_write(
                            sink,
                            data=_pf_clean,
                            partition={"day": date, "entity": entity_name, "league": _canonical_league_id(_pf_lid_str)},
                            filename=f"{entity_name}.parquet",
                            venue="api_football",
                            entity=entity_name,
                        )
                        if manifest is not None:
                            _stamped_pf_df = stamp_available_at_explicit(_pf_clean, when=datetime.now(UTC))
                            manifest.record_captured(
                                row_key={
                                    "date": date,
                                    "data_type": _af_entity_dt,
                                    "league_id": _canonical_league_id(_pf_lid_str),
                                },
                                df=_stamped_pf_df,
                                category="sports",
                                instrument_type="",
                                data_type=_af_entity_dt,
                                league_id=_canonical_league_id(_pf_lid_str),
                                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                                service_emission_state=None,
                            )

                    # Drop unmapped rows — single-SSOT means bare writes are
                    # forbidden for league-axis data types. Surface as a
                    # warning so we can spot upstream league-mapping
                    # regressions in logs.
                    if not _without_league.empty:
                        logger.warning(
                            "%s bare-path fallback triggered for date=%s — data shape regression: "
                            "%d rows could not be mapped to a league. Skipping bare write to keep manifest honest.",
                            _af_entity_dt,
                            date,
                            len(_without_league),
                        )
                    if manifest is not None:
                        _af_emit_empty_gaps_for_entity(_af_entity_dt, _pf_captured)
                else:
                    # Single-SSOT: bare manifest row + bare parquet write are
                    # both suppressed; writing one would create a phantom
                    # captured shard with no parquet on disk.  Surface the
                    # upstream regression in logs so it can be diagnosed.
                    logger.warning(
                        "%s bare-path fallback triggered for date=%s — data shape regression: "
                        "no fixture-id column or empty af_fid->league map (rows=%d). "
                        "Skipping bare write + manifest row to keep manifest honest.",
                        _af_entity_dt,
                        date,
                        len(df),
                    )

                logger.info("Sports reference: %d %s rows written", len(df), entity_name)
            else:
                # Honest-coverage: entity produced zero rows.  Distinguish
                # "all fixtures failed" (record_failed) from "legit empty"
                # (record_empty).  No rows when we did fetch fixtures means
                # the API was called but nothing came back.
                _fail_count, _err_code = entity_failures.get(entity_name, (0, ""))
                if _fail_count == len(fixture_ids) and _err_code:
                    # Every fixture call raised → treat the entity as failed.
                    if manifest is not None:
                        manifest.record_failed(
                            row_key={"date": date, "data_type": _af_entity_dt},
                            error=_err_code,
                            attempted_at=_af_attempt_ts,
                            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
                        )
                else:
                    # Some / all calls succeeded but returned zero rows
                    # (e.g. post-match stats not yet published, lineups not
                    # disclosed for low-profile fixture) — legitimate empty.
                    _af_emit_empty_gaps_for_entity(_af_entity_dt, set())

    # Cross-provider mapping tables
    _write_team_mapping(bucket)
    _write_fixture_mapping(bucket, date)

    return counts


def _write_team_mapping(bucket: str) -> None:
    """Build and write TeamMapping table to GCS.

    Combines UAC team_mappings.py (API-Football names) and team_names.py
    (Odds API / Understat names) into a single lookup table keyed by
    canonical team_id. Used by FSS to resolve provider-specific IDs.

    Path: sports_reference/mappings/team_mapping.parquet
    """
    rows: list[dict[str, str]] = []

    # EPL teams
    epl_ids: set[str] = set(EPL_TEAM_ALIASES.keys()) | set(CANONICAL_TO_ODDS_API_EPL.keys())
    for canonical_id in sorted(epl_ids):
        aliases = EPL_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": CANONICAL_TO_ODDS_API_EPL.get(canonical_id, ""),
                "understat_name": CANONICAL_TO_UNDERSTAT_EPL.get(canonical_id, ""),
                "league": "EPL",
            }
        )

    # Bundesliga teams
    bun_ids: set[str] = set(BUNDESLIGA_TEAM_ALIASES.keys()) | set(CANONICAL_TO_ODDS_API_BUNDESLIGA.keys())
    for canonical_id in sorted(bun_ids):
        aliases = BUNDESLIGA_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": CANONICAL_TO_ODDS_API_BUNDESLIGA.get(canonical_id, ""),
                "understat_name": "",
                "league": "BUNDESLIGA",
            }
        )

    if not rows:
        return

    try:
        df = pd.DataFrame(rows)
        mapping_sink = get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=df, partition={}, format="parquet", filename="team_mapping.parquet")
        logger.info("Team mapping: %d entries written", len(df))
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="team_mapping_write",
        )


def _write_fixture_mapping(bucket: str, date: str) -> None:
    """Build and write FixtureMapping table to GCS.

    Reads the fixture instruments already written for today, extracts the
    canonical_fixture_id and the API-Football numeric fixture_id, and
    writes a mapping table that FSS uses to resolve provider-specific IDs.

    Path: sports_reference/mappings/fixture_mapping.parquet
    """
    try:
        # Read today's fixtures from the instruments parquet we just wrote
        blob_path = f"instrument_availability/by_date/day={date}/venue=API_FOOTBALL/instruments.parquet"
        storage = get_storage_client()
        raw = storage.download_bytes(bucket, blob_path)
        if raw is None:
            logger.debug("Fixture mapping: no fixtures parquet found for %s", date)
            return
        df = pd.read_parquet(io.BytesIO(raw))
        if df.empty:
            logger.debug("Fixture mapping: no fixtures found for %s", date)
            return

        rows: list[dict[str, str]] = []
        for _, row in df.iterrows():
            instrument_key = str(row["instrument_key"]) if "instrument_key" in row.index else ""
            raw_symbol = str(row["raw_symbol"]) if "raw_symbol" in row.index else ""
            # Extract API-Football numeric ID from the symbol or key
            # The instrument_key is canonical: ENGLAND_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322
            # The raw_symbol is: Arsenal vs Chelsea
            rows.append(
                {
                    "canonical_fixture_id": instrument_key,
                    "raw_symbol": raw_symbol,
                    "date": date,
                    "venue": "API_FOOTBALL",
                }
            )

        if not rows:
            return

        mapping_df = pd.DataFrame(rows)
        mapping_sink = get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=mapping_df, partition={}, format="parquet", filename="fixture_mapping.parquet")
        logger.info("Fixture mapping: %d entries written for %s", len(mapping_df), date)
    except Exception as exc:
        _exc_name = type(exc).__name__
        _msg = str(exc)
        # GCS blob not found (404): the instruments.parquet for this (date, venue) was
        # never written. This is benign for ANY date (not just forward-poll window) —
        # for historical forward-polled days the per-fixture entities were captured
        # via enrichment-only mode without ever rolling up an availability parquet at
        # `instrument_availability/by_date/.../venue=API_FOOTBALL/instruments.parquet`.
        # Fixture-mapping is a best-effort secondary write; absent the upstream
        # parquet there is nothing to map. Silently no-op rather than escalating to
        # classify_and_emit_error. Reference: issue doc
        # `plans/active/issues/api_football_enrichment_preflight_runtime_mismatch_2026_05_13.md`.
        if _exc_name == "NotFound" or "404" in _msg or "No such object" in _msg:
            logger.info(
                "Fixture mapping: no API_FOOTBALL instruments parquet for %s — skipping (no upstream availability rollup written)",
                date,
            )
            return
        if isinstance(exc, (FileNotFoundError, OSError)):
            logger.debug("Fixture mapping: could not read fixtures for %s: %s", date, exc)
            return
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="fixture_mapping_write",
        )


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


def _transfermarkt_mapping_blob_path(season: int) -> str:
    return f"sports_reference/mappings/transfermarkt_league_teams/season={season}/teams.parquet"


def _sfi_mapping_blob_path() -> str:
    return "sports_reference/mappings/sfi_league_mapping.parquet"


def _write_transfermarkt_team_mapping(
    bucket: str,
    teams: list[dict[str, str | None]],
    season: int,
) -> None:
    """Persist per-season Transfermarkt league → team roster to GCS.

    Path: ``sports_reference/mappings/transfermarkt_league_teams/season={YYYY}/teams.parquet``

    Columns: ``league_id, canonical_league, team_id, name, squad_size,
    player_count, last_fetched_at``. Idempotent — overwrites on every
    orchestrator run.
    """
    if not teams:
        return
    try:
        df = pd.DataFrame(teams)
        df["last_fetched_at"] = datetime.now(UTC).isoformat()
        mapping_sink = get_data_sink(
            bucket=bucket,
            prefix="sports_reference/mappings",
        )
        mapping_sink.write(
            data=df,
            partition={"transfermarkt_league_teams": "", "season": str(season)},
            format="parquet",
            filename="teams.parquet",
        )
        logger.info(
            "Transfermarkt team mapping cache: %d rows written for season=%d",
            len(df),
            season,
        )
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="transfermarkt_team_mapping_write",
        )


def _read_transfermarkt_team_mapping(
    bucket: str,
    season: int,
) -> pd.DataFrame | None:
    """Return cached Transfermarkt roster parquet, or ``None`` when absent.

    Returns ``None`` on 404 or any read error; callers MUST fall back to a
    live fetch in that case. Cache-hit freshness is judged by the caller
    using ``last_fetched_at``.
    """
    blob_path = _transfermarkt_mapping_blob_path(season)
    try:
        storage = get_storage_client()
        raw = storage.download_bytes(bucket, blob_path)
        if raw is None:
            return None
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.debug(
            "Transfermarkt team mapping cache miss for season=%d: %s",
            season,
            exc,
        )
        return None


def _write_sfi_league_mapping(
    bucket: str,
    leagues: list[dict[str, str | None]],
) -> None:
    """Persist SFI league hex-id → canonical mapping to GCS.

    Path: ``sports_reference/mappings/sfi_league_mapping.parquet``

    SFI league hex IDs are long-lived (not season-scoped); a single flat
    parquet is sufficient. Columns: ``canonical_league_id, sfi_league_hex,
    name, last_fetched_at``.
    """
    if not leagues:
        return
    try:
        df = pd.DataFrame(leagues)
        df["last_fetched_at"] = datetime.now(UTC).isoformat()
        mapping_sink = get_data_sink(
            bucket=bucket,
            prefix="sports_reference/mappings",
        )
        mapping_sink.write(
            data=df,
            partition={},
            format="parquet",
            filename="sfi_league_mapping.parquet",
        )
        logger.info("SFI league mapping cache: %d rows written", len(df))
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sfi_league_mapping_write",
        )


def _read_sfi_league_mapping(bucket: str) -> pd.DataFrame | None:
    """Return cached SFI league mapping parquet, or ``None`` when absent."""
    try:
        storage = get_storage_client()
        raw = storage.download_bytes(bucket, _sfi_mapping_blob_path())
        if raw is None:
            return None
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.debug("SFI league mapping cache miss: %s", exc)
        return None


def _maybe_emit_drift_anomaly(
    *,
    venue: str,
    endpoint: str,
    league_id: str,
    date: str,
    season: int | None,
    got_count: int,
    expected_count: int | None,
    threshold_pct: float = 10.0,
    high_severity_pct: float = 25.0,
) -> float | None:
    """Emit ``ADAPTER_FETCH_ANOMALY`` when got vs expected deviates beyond
    ``threshold_pct``.  Returns the deviation percentage on emit, else ``None``.

    Shared by TM per-league drift + SFI league-denominator drift.  ``None``
    expected count means "no seed" → silent skip (never emit).
    """
    if expected_count is None or expected_count <= 0:
        return None
    deviation_pct = abs(got_count - expected_count) / expected_count * 100.0
    if deviation_pct <= threshold_pct:
        return None
    severity = "HIGH" if deviation_pct > high_severity_pct else "MEDIUM"
    details: dict[str, object] = {
        "venue": venue,
        "endpoint": endpoint,
        "league_id": league_id,
        "date": date,
        "expected_count": expected_count,
        "got_count": got_count,
        "deviation_pct": round(deviation_pct, 1),
        "severity": severity,
    }
    if season is not None:
        details["season"] = season
    log_event("ADAPTER_FETCH_ANOMALY", details=details)
    return deviation_pct


def _cache_is_fresh(df: pd.DataFrame, ttl: timedelta) -> bool:
    """Return True when every row in ``df`` was fetched within ``ttl``."""
    if df.empty or "last_fetched_at" not in df.columns:
        return False
    try:
        timestamps = pd.to_datetime(df["last_fetched_at"], utc=True, errors="coerce")
        if timestamps.isna().any():
            return False
        now = datetime.now(UTC)
        oldest = timestamps.min().to_pydatetime()
        return (now - oldest) < ttl
    except Exception:
        return False


async def _fetch_footystats_predictions(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats predictive data and write to GCS as a separate entity.

    Predictive fields (btts_potential, o25_potential, xg_prematch, etc.) are
    FootyStats-proprietary pre-match signals. Written separately from factual
    fixture data so FSS can consume them as third-party signal input.

    GCS path (snapshots preserved per fetch for prediction-evolution tracking):
      sports_reference/by_date/day={date}/entity=footystats_predictions/
        fetched_at_hour={YYYY-MM-DDTHH}/league={league_id}/footystats_predictions.parquet
    """
    adapter = create_sports_reference_adapter("footystats", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}
    fetched_at_ts = pd.Timestamp.now(tz="UTC")
    fetched_at_hour = fetched_at_ts.strftime("%Y-%m-%dT%H")

    # Honest-coverage pre-flight + attempt-stamp.  See module-level helpers.
    pred_manifest = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )
    _row_key: dict[str, str] = {"date": date, "data_type": "PREDICTIONS"}
    # Per-league skip: only skip the date when every expected canonical
    # footystats league has a (captured | empty_confirmed) row for it.
    # See ``_should_skip_date_for_per_league`` for the bug this fixes.
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _should_skip_date_for_per_league(
        pred_manifest,
        date=date,
        data_type="PREDICTIONS",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        logger.info("FootyStats predictions: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = datetime.now(UTC)

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_footystats_team

        predictions = await adapter.get_fixture_predictions(date)  # type: ignore[attr-defined]
        if predictions:
            df = pd.DataFrame([_coerce_adapter_output(p) for p in predictions])
            # PIT safety: FootyStats predictions publish alongside odds ~3 days before kickoff
            # (empirically verified 2026-04-17: 98% coverage at T-24h, 100% at T-72h).
            if "kickoff_utc" in df.columns:
                df["data_available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True) - pd.Timedelta(hours=72)
            df["fetched_at"] = fetched_at_ts
            # Build canonical fixture_id for downstream join.
            # FootyStats fixture_id format: "{competition_id}:{HOME}_v_{AWAY}:{DATE}"
            # Use historical season ID map (covers ALL seasons, not just current).
            _ft_id_to_league = FOOTYSTATS_HISTORICAL_SEASON_IDS

            def _ft_canonical(row: pd.Series) -> str:
                home = str(row.get("home_team", "") or "")
                away = str(row.get("away_team", "") or "")
                if not home or not away:
                    return ""
                # Extract competition_id from fixture_id if present
                fid = str(row.get("fixture_id", "") or "")
                league = ""
                if ":" in fid:
                    comp_str = fid.split(":")[0]
                    if comp_str.isdigit():
                        league = _ft_id_to_league.get(int(comp_str), "")
                return build_fixture_id(
                    league_id=league,
                    home_team_id=resolve_footystats_team(home),
                    away_team_id=resolve_footystats_team(away),
                    date_str=date,
                )

            if "home_team" in df.columns and "away_team" in df.columns:
                df["canonical_fixture_id"] = df.apply(_ft_canonical, axis=1)
            violations = _validate_predictions_null_rates(df, date)
            if violations:
                logger.warning(
                    "FootyStats predictions shard %s has null-rate warnings (writing anyway): %s",
                    date,
                    "; ".join(violations),
                )
            counts["footystats_predictions"] = len(df)

            # Write per-league partitioned files when canonical_fixture_id is available.
            if "canonical_fixture_id" in df.columns:
                df["_pred_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_pred_league"].notna() & (df["_pred_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _pred_lid, _pred_league_df in _with_league.groupby("_pred_league"):
                    _pred_lid_str = str(_pred_lid)
                    _pred_clean = _pred_league_df.drop(columns=["_pred_league"])
                    _gated_sink_write(
                        sink,
                        data=_pred_clean,
                        partition={
                            "day": date,
                            "entity": "footystats_predictions",
                            "fetched_at_hour": fetched_at_hour,
                            "league": _pred_lid_str,
                        },
                        venue="footystats",
                        entity="footystats_predictions",
                        filename="footystats_predictions.parquet",
                    )
                    _stamped_pred_clean = stamp_available_at_explicit(_pred_clean, when=datetime.now(UTC))
                    pred_manifest.record_captured(
                        row_key={
                            "date": date,
                            "data_type": "PREDICTIONS",
                            "league_id": _canonical_league_id(_pred_lid_str),
                        },
                        df=_stamped_pred_clean,
                        category="sports",
                        instrument_type="",
                        data_type="PREDICTIONS",
                        league_id=_canonical_league_id(_pred_lid_str),
                        pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
                        service_emission_state=None,
                    )

                if not _without_league.empty:
                    _pred_unmapped = _without_league.drop(columns=["_pred_league"])
                    _gated_sink_write(
                        sink,
                        data=_pred_unmapped,
                        partition={
                            "day": date,
                            "entity": "footystats_predictions",
                            "fetched_at_hour": fetched_at_hour,
                        },
                        venue="footystats",
                        entity="footystats_predictions",
                        filename="footystats_predictions.parquet",
                    )
                    _stamped_pred_unmapped = stamp_available_at_explicit(_pred_unmapped, when=datetime.now(UTC))
                    pred_manifest.record_captured(
                        row_key={"date": date, "data_type": "PREDICTIONS"},
                        df=_stamped_pred_unmapped,
                        category="sports",
                        instrument_type="",
                        data_type="PREDICTIONS",
                        pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
                        service_emission_state=None,
                    )
            else:
                _gated_sink_write(
                    sink,
                    data=df,
                    partition={
                        "day": date,
                        "entity": "footystats_predictions",
                        "fetched_at_hour": fetched_at_hour,
                    },
                    venue="footystats",
                    entity="footystats_predictions",
                    filename="footystats_predictions.parquet",
                )
                _stamped_pred_df = stamp_available_at_explicit(df, when=datetime.now(UTC))
                pred_manifest.record_captured(
                    row_key={"date": date, "data_type": "PREDICTIONS"},
                    df=_stamped_pred_df,
                    category="sports",
                    instrument_type="",
                    data_type="PREDICTIONS",
                    pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
                    service_emission_state=None,
                )
            pred_manifest.write()

            logger.info(
                "FootyStats predictions: %d rows written for date=%s",
                len(df),
                date,
            )
        else:
            logger.info("FootyStats predictions: no predictive data for date=%s", date)
            # Honest-coverage: legitimate empty (no predictions for this date).
            # footystats catalog refresh tagged canonical BATCH_FOOTYSTATS
            # (Q2=(A) flip 2026-05-12; resolves
            # footystats_pipeline_mode_gap_2026_05_12.md workaround).
            pred_manifest.record_empty(
                row_key=_row_key,
                attempted_at=attempt_ts,
                pipeline_mode=_pipeline_mode_for_sports_data_type("PREDICTIONS"),
            )
            pred_manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_predictions_fetch",
            shard=date,
        )
        _err_code = _classify_adapter_failure(exc, "footystats")
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixture_predictions",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        pred_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_pipeline_mode_for_sports_data_type("PREDICTIONS"),
        )
        with contextlib.suppress(Exception):
            pred_manifest.write()

    return counts


def _validate_predictions_null_rates(
    df: pd.DataFrame,
    date: str,
) -> list[str]:
    """Validate FootyStats predictions null rates against expectations.

    Returns a list of violation messages. Empty list = valid shard.

    Column tiers:
      REQUIRED (max 5% null): core identifiers + core potentials
      SPARSE (allowed >80% null): corners/cards/offsides/avg potentials
      VARIABLE (no constraint): everything else
    """
    # Core identifiers must be present (5% null max).
    core_cols = [
        "fixture_id",
        "source",
        "kickoff_utc",
        "home_team",
        "away_team",
    ]
    # Potentials are coverage-dependent — lower leagues often lack them.
    # Relaxed to 20% null max.
    potential_cols = [
        "btts_potential",
        "o25_potential",
        "o35_potential",
        "o45_potential",
        "xg_prematch_home",
        "xg_prematch_away",
    ]
    violations: list[str] = []
    n = len(df)
    if n == 0:
        return violations
    for col in core_cols:
        if col not in df.columns:
            violations.append(f"{col} missing from schema")
            continue
        null_pct = df[col].isnull().mean() * 100
        if null_pct > 5.0:
            violations.append(f"{col} null rate {null_pct:.1f}% exceeds 5% max (date={date}, n={n})")
    for col in potential_cols:
        if col not in df.columns:
            continue  # Optional — not all dates have all potentials
        null_pct = df[col].isnull().mean() * 100
        if null_pct > 20.0:
            violations.append(f"{col} null rate {null_pct:.1f}% exceeds 20% max (date={date}, n={n})")
    return violations


async def _fetch_footystats_matches(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats match data and write to GCS.

    FootyStats provides detailed match statistics (possession, shots, corners,
    xG) from a different source than API Football. Written as a separate entity
    so downstream consumers can cross-validate or merge with API Football data.

    GCS path: sports_reference/by_date/day={date}/entity=footystats_matches/
              footystats_matches.parquet
    """
    adapter = create_sports_reference_adapter("footystats", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # Honest-coverage pre-flight + attempt-stamp.
    _ft_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    _row_key: dict[str, str] = {"date": date, "data_type": "MATCHES"}
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _should_skip_date_for_per_league(
        _ft_manifest,
        date=date,
        data_type="MATCHES",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        logger.info("FootyStats matches: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = datetime.now(UTC)

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_footystats_team

        # FootyStats league IDs are seasonal — use HISTORICAL map which covers
        # all seasons 2019-2026 (not just current, so old backfill dates match).
        league_ids = list(FOOTYSTATS_HISTORICAL_SEASON_IDS.keys())
        fixtures = await adapter.get_fixtures(date, league_ids=league_ids)
        if fixtures:
            rows = [_coerce_adapter_output(fx) for fx in fixtures]
            # Flatten nested models for parquet compatibility
            flat_rows: list[dict[str, str | None]] = []
            for row in rows:
                flat: dict[str, str | None] = {}
                for k, v in row.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            flat[f"{k}_{sub_k}"] = str(sub_v) if sub_v is not None else None
                    else:
                        flat[k] = str(v) if v is not None else None
                # Build canonical fixture_id from team names + date.
                # Use the SAME resolution path as _fetch_footystats_odds: reverse-map
                # competition_id (numeric FootyStats league ID) via FOOTYSTATS_HISTORICAL_SEASON_IDS
                # so we get EPL, BUNDESLIGA, etc. (same keys as odds — essential for joins).
                home_name = flat.get("home_team_name") or flat.get("home_team") or ""
                away_name = flat.get("away_team_name") or flat.get("away_team") or ""
                league = ""
                # 1. Try league_league_id (numeric from the flattened league sub-dict)
                raw_league = flat.get("league_league_id") or flat.get("competition_id") or ""
                if raw_league and str(raw_league).isdigit():
                    league = FOOTYSTATS_HISTORICAL_SEASON_IDS.get(int(raw_league), "")
                # 2. Fallback: parse from fixture_id prefix (same as odds path)
                if not league:
                    fid = flat.get("fixture_id") or flat.get("source_fixture_id") or ""
                    if ":" in fid:
                        comp_str = fid.split(":")[0]
                        if comp_str.isdigit():
                            league = FOOTYSTATS_HISTORICAL_SEASON_IDS.get(int(comp_str), "")
                if home_name and away_name:
                    canonical_home = resolve_footystats_team(home_name)
                    canonical_away = resolve_footystats_team(away_name)
                    flat["canonical_fixture_id"] = build_fixture_id(
                        league_id=league,
                        home_team_id=canonical_home,
                        away_team_id=canonical_away,
                        date_str=date,
                    )
                flat_rows.append(flat)
            df = pd.DataFrame(flat_rows)
            # PIT safety: post-match stats available ~3h after kickoff when match complete
            if "kickoff_utc" in df.columns:
                df["data_available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce") + pd.Timedelta(
                    hours=3
                )
            counts["footystats_matches"] = len(df)

            # Write per-league partitioned files using canonical_fixture_id.
            _captured_leagues: set[str] = set()
            if "canonical_fixture_id" in df.columns:
                df["_ft_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_ft_league"].notna() & (df["_ft_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _ft_lid, _ft_league_df in _with_league.groupby("_ft_league"):
                    _ft_lid_str = str(_ft_lid)
                    _ft_canonical = _canonical_league_id(_ft_lid_str)
                    _ft_clean = _ft_league_df.drop(columns=["_ft_league"])
                    _gated_sink_write(
                        sink,
                        data=_ft_clean,
                        partition={
                            "day": date,
                            "entity": "footystats_matches",
                            "league": _ft_canonical,
                        },
                        venue="footystats",
                        entity="footystats_matches",
                        filename="footystats_matches.parquet",
                    )
                    _stamped_ft_df = stamp_available_at_explicit(_ft_clean, when=datetime.now(UTC))
                    _ft_manifest.record_captured(
                        row_key={"date": date, "data_type": "MATCHES", "league_id": _ft_canonical},
                        df=_stamped_ft_df,
                        category="sports",
                        instrument_type="",
                        data_type="MATCHES",
                        league_id=_ft_canonical,
                        pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
                        service_emission_state=None,
                    )
                    _captured_leagues.add(_ft_canonical)

                if not _without_league.empty:
                    logger.warning(
                        "MATCHES bare-path fallback triggered for date=%s — data shape regression: "
                        "%d footystats rows could not derive a league from canonical_fixture_id. "
                        "Skipping bare write + manifest row to keep manifest honest.",
                        date,
                        len(_without_league),
                    )
            else:
                logger.warning(
                    "MATCHES bare-path fallback triggered for date=%s — data shape regression: "
                    "footystats df missing canonical_fixture_id column (rows=%d). "
                    "Skipping bare write + manifest row to keep manifest honest.",
                    date,
                    len(df),
                )

            # Honest-coverage per-league: record_empty for expected footystats
            # leagues with no matches on this date (off-season / no fixtures).
            # Mirrors the XG adapter pattern at the understat block below.
            # footystats-served MATCHES tagged canonical BATCH_FOOTYSTATS
            # (Q2=(A) flip 2026-05-12).
            for _exp_lid in sorted(set(_ft_expected) - _captured_leagues):
                _ft_manifest.record_empty(
                    row_key={"date": date, "data_type": "MATCHES", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    pipeline_mode=_pipeline_mode_for_sports_data_type("MATCHES"),
                )
            _ft_manifest.write()
            logger.info("FootyStats matches: %d rows written for date=%s", len(df), date)
        else:
            logger.info("FootyStats matches: no fixtures for date=%s", date)
            # Honest-coverage: emit per-league record_empty for ALL expected
            # leagues — date-aggregate rows were retired in Phase 2 of the
            # sports_manifest_shard_migration_cleanup, mirroring the XG pattern.
            for _exp_lid in sorted(set(_ft_expected)):
                _ft_manifest.record_empty(
                    row_key={"date": date, "data_type": "MATCHES", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    pipeline_mode=_pipeline_mode_for_sports_data_type("MATCHES"),
                )
            _ft_manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_matches_fetch",
            shard=date,
        )
        _err_code = _classify_adapter_failure(exc, "footystats")
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixtures",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        _ft_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_pipeline_mode_for_sports_data_type("MATCHES"),
        )
        with contextlib.suppress(Exception):
            _ft_manifest.write()

    return counts


async def _fetch_footystats_odds(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats pre-match odds (68 markets) and write to GCS.

    FootyStats provides comprehensive pre-match odds from aggregated
    bookmakers: full-time 1X2, O/U 0.5-4.5, 1st/2nd half results and
    O/U, BTTS (full/per-half), corners (result + O/U), clean sheet,
    team to score first, win to nil, double chance, draw no bet.

    Same ``/todays-matches`` endpoint as predictions and matches — no
    extra API calls needed.

    GCS path (snapshots preserved per fetch for odds-evolution tracking):
      sports_reference/by_date/day={date}/entity=footystats_odds/
        fetched_at_hour={YYYY-MM-DDTHH}/league={league_id}/footystats_odds.parquet

    Each fetch lands in its own `fetched_at_hour` partition so repeated polls
    of the same future date accumulate snapshots instead of overwriting.
    """
    adapter = create_sports_reference_adapter("footystats", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}
    fetched_at_ts = pd.Timestamp.now(tz="UTC")
    fetched_at_hour = fetched_at_ts.strftime("%Y-%m-%dT%H")

    # Honest-coverage pre-flight + attempt-stamp.
    odds_manifest = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )
    _row_key: dict[str, str] = {"date": date, "data_type": "ODDS"}
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _should_skip_date_for_per_league(
        odds_manifest,
        date=date,
        data_type="ODDS",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        logger.info("FootyStats odds: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = datetime.now(UTC)

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_footystats_team

        odds_rows = await adapter.get_fixture_odds_snapshot(date)  # type: ignore[attr-defined]
        if odds_rows:
            df = pd.DataFrame(odds_rows)
            # PIT safety: FootyStats publishes odds ~3 days before kickoff (empirically verified
            # 2026-04-17: 98% of matches have odds at T-24h, 100% at T-72h, ~8% at T-168h).
            # Conservative: assume odds available from T-72h.
            if "kickoff_utc" in df.columns:
                df["data_available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True) - pd.Timedelta(hours=72)
            # fetched_at = when we actually captured this snapshot (for odds movement tracking)
            df["fetched_at"] = fetched_at_ts
            _ft_id_to_league = FOOTYSTATS_HISTORICAL_SEASON_IDS

            def _odds_canonical(row: pd.Series) -> str:
                home = str(row.get("home_team", "") or "")
                away = str(row.get("away_team", "") or "")
                if not home or not away:
                    return ""
                fid = str(row.get("fixture_id", "") or "")
                league = ""
                if ":" in fid:
                    comp_str = fid.split(":")[0]
                    if comp_str.isdigit():
                        league = _ft_id_to_league.get(int(comp_str), "")
                return build_fixture_id(
                    league_id=league,
                    home_team_id=resolve_footystats_team(home),
                    away_team_id=resolve_footystats_team(away),
                    date_str=date,
                )

            if "home_team" in df.columns and "away_team" in df.columns:
                df["canonical_fixture_id"] = df.apply(_odds_canonical, axis=1)
            counts["footystats_odds"] = len(df)

            if "canonical_fixture_id" in df.columns:
                df["_odds_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_odds_league"].notna() & (df["_odds_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _odds_lid, _odds_league_df in _with_league.groupby("_odds_league"):
                    _odds_lid_str = str(_odds_lid)
                    _odds_clean = _odds_league_df.drop(columns=["_odds_league"])
                    _gated_sink_write(
                        sink,
                        data=_odds_clean,
                        partition={
                            "day": date,
                            "entity": "footystats_odds",
                            "fetched_at_hour": fetched_at_hour,
                            "league": _odds_lid_str,
                        },
                        venue="footystats",
                        entity="footystats_odds",
                        filename="footystats_odds.parquet",
                    )
                    _stamped_odds_clean = stamp_available_at_explicit(_odds_clean, when=datetime.now(UTC))
                    odds_manifest.record_captured(
                        row_key={"date": date, "data_type": "ODDS", "league_id": _canonical_league_id(_odds_lid_str)},
                        df=_stamped_odds_clean,
                        category="sports",
                        instrument_type="",
                        data_type="ODDS",
                        league_id=_canonical_league_id(_odds_lid_str),
                        pipeline_mode=PipelineMode.BATCH_ODDS_API,
                        service_emission_state=None,
                    )

                if not _without_league.empty:
                    _odds_unmapped = _without_league.drop(columns=["_odds_league"])
                    _gated_sink_write(
                        sink,
                        data=_odds_unmapped,
                        partition={
                            "day": date,
                            "entity": "footystats_odds",
                            "fetched_at_hour": fetched_at_hour,
                        },
                        venue="footystats",
                        entity="footystats_odds",
                        filename="footystats_odds.parquet",
                    )
                    _stamped_odds_unmapped = stamp_available_at_explicit(_odds_unmapped, when=datetime.now(UTC))
                    odds_manifest.record_captured(
                        row_key={"date": date, "data_type": "ODDS"},
                        df=_stamped_odds_unmapped,
                        category="sports",
                        instrument_type="",
                        data_type="ODDS",
                        pipeline_mode=PipelineMode.BATCH_ODDS_API,
                        service_emission_state=None,
                    )
            else:
                _gated_sink_write(
                    sink,
                    data=df,
                    partition={
                        "day": date,
                        "entity": "footystats_odds",
                        "fetched_at_hour": fetched_at_hour,
                    },
                    venue="footystats",
                    entity="footystats_odds",
                    filename="footystats_odds.parquet",
                )
                _stamped_odds_df = stamp_available_at_explicit(df, when=datetime.now(UTC))
                odds_manifest.record_captured(
                    row_key={"date": date, "data_type": "ODDS"},
                    df=_stamped_odds_df,
                    category="sports",
                    instrument_type="",
                    data_type="ODDS",
                    pipeline_mode=PipelineMode.BATCH_ODDS_API,
                    service_emission_state=None,
                )
            odds_manifest.write()
            logger.info("FootyStats odds: %d rows written for date=%s", len(df), date)
        else:
            logger.info("FootyStats odds: no odds data for date=%s", date)
            # Honest-coverage: legitimate empty (no odds for this date).
            # ODDS slice tagged BATCH_ODDS_API per UAC SOURCE_PRIORITY
            # (footystats odds adapter; see footystats_pipeline_mode_gap_2026_05_12.md).
            odds_manifest.record_empty(
                row_key=_row_key,
                attempted_at=attempt_ts,
                pipeline_mode=_pipeline_mode_for_sports_data_type("ODDS"),
            )
            odds_manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_odds_fetch",
            shard=date,
        )
        _err_code = _classify_adapter_failure(exc, "footystats")
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixture_odds_snapshot",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        odds_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_pipeline_mode_for_sports_data_type("ODDS"),
        )
        with contextlib.suppress(Exception):
            odds_manifest.write()

    return counts


async def _fetch_understat_xg(
    date: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch Understat xG data and write to GCS.

    Understat provides expected goals (xG), shot data, and advanced stats
    scraped from public pages. No API key required. Covers 6 leagues:
    EPL, La Liga, Bundesliga, Serie A, Ligue 1, RFPL.

    GCS path: sports_reference/by_date/day={date}/entity=understat_xg/
              understat_xg.parquet
    """
    adapter = create_sports_reference_adapter("understat")
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # Expected-league denominator (Understat covers 5 PREDICTION leagues: EPL,
    # LA_LIGA, BUNDESLIGA, SERIE_A, LIGUE_1). SSOT:
    # ``codex/02-data/sports-data-source-coverage-matrix.md``.
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    xg_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    _expected_understat_leagues = {
        lg.league_id for lg in get_expected_leagues_for_source("understat", classifications=["Prediction"])
    }

    # Honest-coverage per-league pre-flight: only short-circuit if EVERY
    # expected league already has its own captured/empty_confirmed row. The
    # legacy date-aggregate row (row_key without league_id) is ignored so
    # pre-sharding-era shards get back-filled per-league on the next run.
    # ``attempted_failed`` per-league rows fall through and are retried.
    # ``force=True`` bypasses the skip entirely.
    _all_per_league_captured = bool(_expected_understat_leagues) and all(
        _should_skip_shard(
            xg_manifest,
            row_key={"date": date, "data_type": "XG", "league_id": lid},
            force=force,
        )
        for lid in _expected_understat_leagues
    )
    if _all_per_league_captured:
        logger.info(
            "Understat xG: skipping date=%s — all %d expected leagues per-league captured",
            date,
            len(_expected_understat_leagues),
        )
        return counts

    # Stamp attempt-start before the network call so record_empty / record_failed
    # reflect the attempt time, not the manifest write time.
    attempt_ts = datetime.now(UTC)

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_understat_team

        fixtures = await adapter.get_fixtures(date)
        if fixtures:
            rows = [_coerce_adapter_output(fx) for fx in fixtures]
            flat_rows: list[dict[str, str | None]] = []
            for row in rows:
                flat: dict[str, str | None] = {}
                for k, v in row.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            flat[f"{k}_{sub_k}"] = str(sub_v) if sub_v is not None else None
                    else:
                        flat[k] = str(v) if v is not None else None
                # Build canonical fixture_id from team names + date
                home_name = flat.get("h_title") or ""
                away_name = flat.get("a_title") or ""
                league = flat.get("league") or ""
                if home_name and away_name:
                    canonical_home = resolve_understat_team(home_name)
                    canonical_away = resolve_understat_team(away_name)
                    flat["canonical_fixture_id"] = build_fixture_id(
                        league_id=league,
                        home_team_id=canonical_home,
                        away_team_id=canonical_away,
                        date_str=date,
                    )
                flat_rows.append(flat)
            df = pd.DataFrame(flat_rows)
            # PIT safety: Understat xG scraped day after match
            if "kickoff_utc" in df.columns:
                df["data_available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce") + pd.Timedelta(
                    hours=24
                )
            counts["understat_xg"] = len(df)

            _captured_leagues: set[str] = set()
            # Write per-league partitioned files if league column exists
            if "league" in df.columns:
                _has_league = df["league"].notna() & (df["league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _xg_lid, _xg_league_df in _with_league.groupby("league"):
                    _xg_lid_str = str(_xg_lid)
                    _captured_leagues.add(_xg_lid_str)
                    _gated_sink_write(
                        sink,
                        data=_xg_league_df,
                        partition={"day": date, "entity": "understat_xg", "league": _canonical_league_id(_xg_lid_str)},
                        filename="understat_xg.parquet",
                        venue="understat",
                        entity="understat_xg",
                    )
                    _stamped_xg_df = stamp_available_at_explicit(_xg_league_df, when=datetime.now(UTC))
                    xg_manifest.record_captured(
                        row_key={"date": date, "data_type": "XG", "league_id": _canonical_league_id(_xg_lid_str)},
                        df=_stamped_xg_df,
                        category="sports",
                        instrument_type="",
                        data_type="XG",
                        league_id=_canonical_league_id(_xg_lid_str),
                        pipeline_mode=PipelineMode.BATCH_UNDERSTAT,
                        service_emission_state=None,
                    )

                if not _without_league.empty:
                    logger.warning(
                        "XG bare-path fallback triggered for date=%s — data shape regression: "
                        "%d understat rows missing league label. Skipping bare write to keep manifest honest.",
                        date,
                        len(_without_league),
                    )
            else:
                logger.warning(
                    "XG bare-path fallback triggered for date=%s — data shape regression: "
                    "understat df missing league column (rows=%d). "
                    "Skipping bare write + manifest row to keep manifest honest.",
                    date,
                    len(df),
                )

            # Honest-coverage per-league: record_empty for expected PREDICTION
            # leagues with no rows on this date (off-season / no fixtures).
            for _exp_lid in sorted(_expected_understat_leagues - _captured_leagues):
                xg_manifest.record_empty(
                    row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_UNDERSTAT,
                )
            xg_manifest.write()
            logger.info("Understat xG: %d rows written for date=%s", len(df), date)
        else:
            logger.info("Understat xG: no fixtures for date=%s", date)
            # Honest-coverage: record an attempt that legitimately produced zero
            # rows (Understat covers 5 leagues, off-season days are empty).
            # Emit per-league record_empty ONLY — the date-aggregate row was
            # deleted in Phase 2 of sports_manifest_shard_migration_cleanup.
            for _exp_lid in sorted(_expected_understat_leagues):
                xg_manifest.record_empty(
                    row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_UNDERSTAT,
                )
            xg_manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="understat_xg_fetch",
            shard=date,
        )
        _err_code = _classify_adapter_failure(exc, "understat")
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "understat",
                "endpoint": "get_fixtures",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt so the
        # manifest reflects honest attempt-vs-capture coverage and the next
        # run can decide to retry. Emit a per-league failure row for each
        # expected league — the date-aggregate row was deleted in Phase 2 of
        # sports_manifest_shard_migration_cleanup.
        for _exp_lid in sorted(_expected_understat_leagues):
            xg_manifest.record_failed(
                row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_UNDERSTAT,
            )
        with contextlib.suppress(Exception):
            xg_manifest.write()

    return counts


async def _fetch_transfermarkt_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
    season: int | None = None,
    league_filter: list[str] | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Fetch Transfermarkt leagues and teams (with player values) to GCS.

    entity_filter: when set to "TRANSFERMARKT_LEAGUES" or "PLAYER_VALUES",
        only that entity is fetched and written (entity-scoped VM mode).
    season: override season year for historical backfill (e.g. 2019).
        When None, the adapter defaults to the current year.

    Transfermarkt provides squad composition, player market values, and
    transfer history. Data is slow-moving (changes at trigger dates:
    season start, transfer window open/close) and fetched only then.

    Honest-coverage: per-league PLAYER_VALUES shards are emitted as
    ``captured`` / ``empty_confirmed`` / ``attempted_failed`` so the
    data-status page can distinguish "league had no squad data" from
    "API call failed" from "league never attempted".

    GCS paths:
        sports_reference/by_date/day={date}/entity=transfermarkt_leagues/
        sports_reference/by_date/day={date}/entity=transfermarkt_teams/
    """
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    adapter = create_sports_reference_adapter("transfermarkt", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # TRANSFERMARKT_LEAGUES retired 2026-05-05 — was a static provider-catalog
    # mapping (provider_id -> canonical_name + country). Mappings now live in
    # UAC (TRANSFERMARKT_IDS) as versioned config. Don't fetch + write to GCS.
    _want_teams = entity_filter is None or entity_filter == "PLAYER_VALUES"

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = datetime.now(UTC)

    # --- PLAYER_VALUES shards (per expected league) ---
    if _want_teams:
        # Denominator = expected leagues with a Transfermarkt mapping.
        # SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md`` — 55
        # leagues (Prediction + Features).  The adapter also needs a
        # provider-league-id mapping; leagues without a mapping are skipped
        # with a record_empty entry (we have no way to attempt them).
        _expected_tm_leagues = get_expected_leagues_for_source(
            "transfermarkt",
            classifications=["Prediction", "Features"],
        )
        _league_filter_set = set(league_filter) if league_filter else None
        # Merge legacy ``get_prediction_leagues()`` with the canonical expected
        # set so we keep bundled prediction leagues even if the data_sources
        # registry ever drifts.
        _merged_leagues = {lg.league_id: lg for lg in _expected_tm_leagues}
        for _p_lg in get_prediction_leagues():
            _merged_leagues.setdefault(_p_lg.league_id, _p_lg)

        # Per-league skip: only skip the date when EVERY expected canonical
        # league has a captured/empty_confirmed PLAYER_VALUES row. Coarse
        # `_should_skip_shard` at (date, data_type) — the previous shape — is
        # the 2026-05-05 MATCHES 18%-coverage bug pattern: writer emits per-
        # league rows (e.g. lines 4946-4951) but skip is at the bundle level,
        # so any one captured league locks the whole date out from per-league
        # re-fetch.
        _expected_pv_league_ids = sorted(
            lg_id for lg_id in _merged_leagues if _league_filter_set is None or lg_id in _league_filter_set
        )
        if _should_skip_date_for_per_league(
            manifest,
            date=date,
            data_type="PLAYER_VALUES",
            expected_canonical_leagues=_expected_pv_league_ids,
            force=force,
        ):
            logger.info("PLAYER_VALUES: skipping date=%s (all canonical leagues captured)", date)
            return counts

        effective_season = season if season is not None else datetime.now(UTC).year

        # Cache short-circuit: skip API calls on non-trigger dates when we
        # already have a fresh roster for this season.  ``cached_rows`` survives
        # the short-circuit so per-league manifest rows are emitted from it.
        _cache_hit = False
        _cached_df = _read_transfermarkt_team_mapping(bucket, effective_season)
        if _cached_df is not None and _cache_is_fresh(_cached_df, timedelta(days=_TRANSFERMARKT_CACHE_STALENESS_DAYS)):
            try:
                _triggers_today = get_leagues_needing_refresh(date_type.fromisoformat(date))
            except Exception:
                _triggers_today = ["__fallback__"]
            if not _triggers_today:
                _cache_hit = True
                logger.info(
                    "Transfermarkt cache hit for season=%d date=%s — skipping API loop",
                    effective_season,
                    date,
                )

        all_teams: list[dict[str, str | None]] = []
        _captured_league_counts: dict[str, int] = {}
        _failed_leagues: dict[str, str] = {}
        _empty_leagues: set[str] = set()
        _unmapped_leagues: set[str] = set()

        if _cache_hit and _cached_df is not None:
            # Populate ``_captured_league_counts`` from the cache so honest-
            # coverage manifest rows + UPSTREAM_FETCH_COMPLETED events fire
            # exactly as they would on a live fetch — minus the paid API calls.
            if "canonical_league" in _cached_df.columns:
                for _canon_league, _group_df in _cached_df.groupby("canonical_league"):
                    _captured_league_counts[str(_canon_league)] = len(_group_df)
            # Honest-coverage: mark expected leagues missing from the cache as
            # empty so the data-status denominator aligns with the orchestrator's
            # attempt set. Without this the cache-hit branch only emits captured
            # rows and leaves the (cadence-window x non-cached-league) cells as
            # data-status "missing" even though the prior live fetch decided
            # there was nothing to capture.
            _expected_canonical = {lg.league_id for lg in _merged_leagues.values()}
            if _league_filter_set is not None:
                _expected_canonical &= _league_filter_set
            _empty_leagues |= _expected_canonical - set(_captured_league_counts)
            log_event(
                "UPSTREAM_FETCH_COMPLETED",
                details={
                    "venue": "transfermarkt",
                    "endpoint": "get_teams",
                    "date": date,
                    "season": effective_season,
                    "cached": True,
                    "league_count": len(_captured_league_counts),
                },
            )
        else:
            for _lid_key, league_def in sorted(_merged_leagues.items()):
                if _league_filter_set is not None and league_def.league_id not in _league_filter_set:
                    continue
                tm_code = get_provider_league_id(league_def.league_id, "transfermarkt")
                if tm_code is None:
                    # No provider mapping — we cannot attempt this league.
                    # Record empty so the denominator counts it as
                    # confirmed-unavailable (operator may add mapping later).
                    _unmapped_leagues.add(league_def.league_id)
                    continue
                try:
                    teams = await adapter.get_teams(tm_code, season=season)
                    if not teams:
                        _empty_leagues.add(league_def.league_id)
                        continue
                    _league_count = 0
                    for t in teams:
                        row = _coerce_adapter_output(t)
                        flat: dict[str, str | None] = {k: str(v) if v is not None else None for k, v in row.items()}
                        flat["league_id"] = str(tm_code)
                        flat["canonical_league"] = league_def.league_id
                        # Derive player_count for FSS normalizer
                        players = row.get("players")
                        flat["player_count"] = (
                            str(len(players)) if isinstance(players, list) else flat.get("squad_size")
                        )
                        # Drop nested players list (serializes as unhelpful string)
                        flat.pop("players", None)
                        all_teams.append(flat)
                        _league_count += 1
                    # Drift detection — emit ADAPTER_FETCH_ANOMALY before the
                    # manifest write so observability catches silent partial
                    # responses (e.g. EPL returning 17 teams instead of 20).
                    _maybe_emit_drift_anomaly(
                        venue="transfermarkt",
                        endpoint="get_teams",
                        league_id=league_def.league_id,
                        date=date,
                        season=effective_season,
                        got_count=_league_count,
                        expected_count=get_expected_team_count_for_league(league_def.league_id, effective_season),
                    )
                    _captured_league_counts[league_def.league_id] = _league_count
                except Exception as exc:
                    # Shard-level failure isolation — per-league failure MUST
                    # NOT kill the batch.  Record_failed + continue.
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="transfermarkt_teams_fetch",
                        shard=str(league_def.league_id),
                    )
                    _err_code = _classify_adapter_failure(exc, "transfermarkt")
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "transfermarkt",
                            "endpoint": "get_teams",
                            "league_id": league_def.league_id,
                            "date": date,
                            "error": str(exc),
                            "error_code": _err_code,
                        },
                    )
                    _failed_leagues[league_def.league_id] = _err_code

            if all_teams:
                df = pd.DataFrame(all_teams)
                # Add season column for provenance
                df["season"] = effective_season
                # Write as player_values entity — partition by season when
                # doing historical backfill so seasons don't overwrite each other
                pv_partition: dict[str, str] = {"day": date, "entity": "player_values"}
                if season is not None:
                    pv_partition["season"] = str(season)
                _gated_sink_write(
                    sink,
                    data=df,
                    partition=pv_partition,
                    filename="player_values.parquet",
                    venue="transfermarkt",
                    entity="player_values",
                )
                counts["transfermarkt_teams"] = len(df)
                logger.info(
                    "Transfermarkt teams → player_values: %d rows written",
                    len(df),
                )

                # Persist per-season cache for the next backfill iteration.
                _cache_rows: list[dict[str, str | None]] = [
                    {
                        "league_id": str(_r.get("league_id", "")),
                        "canonical_league": str(_r.get("canonical_league", "")),
                        "team_id": str(_r.get("team_id", "") or _r.get("id", "")),
                        "name": str(_r.get("name", "")),
                        "squad_size": str(_r.get("squad_size", "") or ""),
                        "player_count": str(_r.get("player_count", "") or ""),
                    }
                    for _r in all_teams
                ]
                _write_transfermarkt_team_mapping(bucket, _cache_rows, effective_season)

        # Per-league honest-coverage manifest rows — identical between the
        # cache-hit and live-fetch branches.  ``cached=True`` is passed as a
        # kwarg for future schema evolution (ManifestWriter v8 tolerates extra
        # kwargs); the cache-hit path also emits an UPSTREAM_FETCH_COMPLETED
        # event with ``cached=True`` so current observability can filter on it.
        for _cap_lid, _cap_count in _captured_league_counts.items():
            manifest.record_captured_from_counts(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _canonical_league_id(_cap_lid)},
                total_rows=_cap_count,
                expected_root_clusters={},
                observed_clusters={"": _cap_count},
                available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                pipeline_mode=PipelineMode.BATCH_TRANSFERMARKT,
                service_emission_state=None,
            )
        for _emp_lid in sorted(_empty_leagues | _unmapped_leagues):
            manifest.record_empty(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _emp_lid},
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_TRANSFERMARKT,
            )
        for _f_lid, _f_err in sorted(_failed_leagues.items()):
            manifest.record_failed(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _f_lid},
                error=_f_err,
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_TRANSFERMARKT,
            )

    manifest.write()

    return counts


async def _fetch_sfi_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Fetch SoccerFootball.info leagues, standings, and progressive stats to GCS.

    entity_filter: when set to "SFI_LEAGUES", "SFI_STANDINGS", or
        "SFI_PROGRESSIVE_STATS", only that entity is written (entity-scoped
        VM mode). Note: SFI_STANDINGS always fetches leagues first to get
        league IDs, but only writes the requested entity.

    SFI provides league standings and tables from an independent source,
    useful for cross-validation with API Football standings. Progressive
    stats provide 30-second interval match time-series data for halftime
    feature engineering.

    Honest-coverage: per-league SFI_STANDINGS + SFI_PROGRESSIVE_STATS shards
    emit ``captured`` / ``empty_confirmed`` / ``attempted_failed`` so the
    data-status page can distinguish legitimate empties from API failures.
    Shard-level failure isolation: a per-league exception is recorded and
    the loop continues — never raised to caller.

    GCS paths:
        sports_reference/by_date/day={date}/entity=sfi_leagues/
        sports_reference/by_date/day={date}/entity=sfi_standings/
        sports_reference/by_date/day={date}/entity=progressive_stats/
    """
    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    adapter = create_sports_reference_adapter("soccer_football_info", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # SFI_LEAGUES retired 2026-05-05 — provider catalog mapping in UAC.
    # adapter.get_leagues() still runs at runtime to build the prediction-tier
    # filter for progressive_stats fetches, but no GCS write or manifest row.
    # SFI_STANDINGS retired 2026-04-24 — SFI has no standings endpoint.
    _want_sfi_standings = False
    _want_sfi_progressive = entity_filter is None or entity_filter == "SFI_PROGRESSIVE_STATS"

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = datetime.now(UTC)

    # Expected denominator: 33 PREDICTION leagues per coverage matrix.
    _expected_sfi_leagues = get_expected_leagues_for_source(
        "soccer_football_info",
        classifications=["Prediction"],
    )
    _expected_sfi_league_ids = {lg.league_id for lg in _expected_sfi_leagues}

    # Per-league skip: only skip the date when EVERY expected canonical league
    # has a captured/empty_confirmed row for SFI_PROGRESSIVE_STATS. The plain
    # `_should_skip_shard` matched on (date, data_type) only — that's the
    # 2026-05-05 MATCHES 18%-coverage bug shape. SFI_PROGRESSIVE_STATS writes
    # per-league rows at lines 5293-5313 / 5210-5218 / 5335-5343 / 5352-5360 /
    # 5368-5376, so the skip MUST also be per-league.
    if _want_sfi_progressive and _should_skip_date_for_per_league(
        manifest,
        date=date,
        data_type="SFI_PROGRESSIVE_STATS",
        expected_canonical_leagues=sorted(_expected_sfi_league_ids),
        force=force,
    ):
        _want_sfi_progressive = False

    # --- SFI league mapping cache short-circuit (per-date, 24h TTL) ---
    # Backfill VMs hit the SFI ``get_leagues`` endpoint once per trigger date.
    # A fresh cache lets non-trigger dates reuse the canonical league-ID list
    # without a paid API call.  ``sfi_league_ids`` is the list of SFI hex IDs
    # used downstream by ``_filtered_sfi_ids`` for progressive-stats.
    sfi_league_ids: list[str] = []
    _sfi_cache_hit = False
    _sfi_cached_df = _read_sfi_league_mapping(bucket)
    if _sfi_cached_df is not None and _cache_is_fresh(_sfi_cached_df, timedelta(hours=_SFI_CACHE_STALENESS_HOURS)):
        try:
            _sfi_triggers_today = get_leagues_needing_refresh(date_type.fromisoformat(date))
        except Exception:
            _sfi_triggers_today = ["__fallback__"]
        if not _sfi_triggers_today and "sfi_league_hex" in _sfi_cached_df.columns:
            _sfi_cache_hit = True
            sfi_league_ids = [str(v) for v in _sfi_cached_df["sfi_league_hex"].dropna().tolist() if str(v)]
            logger.info(
                "SFI league mapping cache hit for date=%s — skipping get_leagues API",
                date,
            )
            log_event(
                "UPSTREAM_FETCH_COMPLETED",
                details={
                    "venue": "soccer_football_info",
                    "endpoint": "get_leagues",
                    "date": date,
                    "cached": True,
                    "league_count": len(sfi_league_ids),
                },
            )
            # SFI_LEAGUES retired 2026-05-05 — was a static catalog (provider
            # hash -> canonical name); mappings now in UAC SOCCER_FOOTBALL_INFO_IDS.
            # Cache hit: no manifest row, just keep sfi_league_ids for runtime.

    # --- SFI catalog runtime fetch (NOT a captured data type) ---
    # adapter.get_leagues() returns the live SFI catalog; we use it to build
    # the prediction-tier filter for downstream progressive_stats fetches.
    # No GCS write, no manifest row — see retirement note above.
    leagues = [] if _sfi_cache_hit else None
    try:
        if not _sfi_cache_hit:
            leagues = await adapter.get_leagues()
        if leagues:
            sfi_league_ids = [lg.league_id for lg in leagues]

            # Drift detection — compare Prediction-classified league count the
            # provider returned against the canonical denominator.  Fires an
            # ADAPTER_FETCH_ANOMALY event (does not block the write) when we
            # see fewer mapped leagues than expected.  SFI uses a wider 15/30%
            # threshold than TM (10/25%) because the provider routinely drops
            # and re-adds fringe leagues day-to-day.
            if _expected_sfi_league_ids:
                _mapped_sfi_ids_check = set(SOCCER_FOOTBALL_INFO_IDS.values())
                _got_mapped_count = sum(1 for lid in sfi_league_ids if lid in _mapped_sfi_ids_check)
                _maybe_emit_drift_anomaly(
                    venue="soccer_football_info",
                    endpoint="get_leagues",
                    league_id="__denominator__",
                    date=date,
                    season=None,
                    got_count=_got_mapped_count,
                    expected_count=len(_expected_sfi_league_ids),
                    threshold_pct=15.0,
                    high_severity_pct=30.0,
                )

            # Persist SFI league-mapping cache for the next backfill iteration.
            _sfi_hex_by_canonical = {v: k for k, v in SOCCER_FOOTBALL_INFO_IDS.items()}
            _cache_rows: list[dict[str, str | None]] = []
            for _lg in leagues:
                _raw = _coerce_adapter_output(_lg)
                _hex = str(_raw.get("league_id", ""))
                _canonical = _sfi_hex_by_canonical.get(_hex, "")
                _cache_rows.append(
                    {
                        "canonical_league_id": _canonical,
                        "sfi_league_hex": _hex,
                        "name": str(_raw.get("name", "")),
                    }
                )
            _write_sfi_league_mapping(bucket, _cache_rows)
        elif not _sfi_cache_hit:
            logger.info("SFI leagues: 0 rows returned for date=%s (no manifest write — retired)", date)
    except Exception as exc:
        # Retired entity — log + classify but don't write a manifest row.
        # The downstream sfi_progressive_stats fetch will still run with
        # whatever sfi_league_ids we managed to populate (possibly empty).
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sfi_leagues_fetch",
            shard=date,
        )

    # Standings — only for our mapped prediction leagues (not all 2800+ SFI championships).
    # SOCCER_FOOTBALL_INFO_IDS maps canonical league → SFI hex ID. We only fetch standings
    # for IDs in that set to avoid 404s on leagues SFI doesn't support for standings.
    _mapped_sfi_ids = set(SOCCER_FOOTBALL_INFO_IDS.values())
    _filtered_sfi_ids = [lid for lid in sfi_league_ids if lid in _mapped_sfi_ids]
    if _filtered_sfi_ids != sfi_league_ids:
        logger.info(
            "SFI: filtered %d → %d leagues (only mapped prediction leagues)",
            len(sfi_league_ids),
            len(_filtered_sfi_ids),
        )

    if _filtered_sfi_ids and _want_sfi_standings:
        # Currently unreachable — ``_want_sfi_standings`` is hard-coded False
        # above because SFI has no standings endpoint.  Kept for completeness
        # in case the endpoint is reintroduced.
        try:
            all_standings: list[dict[str, str | None]] = []
            for lid in _filtered_sfi_ids:
                try:
                    standings = await adapter.get_standings(lid)  # type: ignore[arg-type]
                    for entry in standings:
                        row = entry.model_dump() if hasattr(entry, "model_dump") else entry
                        all_standings.append({k: str(v) if v is not None else None for k, v in row.items()})
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sfi_standings_fetch",
                        shard=lid,
                    )
            if all_standings:
                df = pd.DataFrame(all_standings)
                _gated_sink_write(
                    sink,
                    data=df,
                    partition={"day": date, "entity": "sfi_standings"},
                    filename="sfi_standings.parquet",
                    venue="soccer_football_info",
                    entity="sfi_standings",
                )
                counts["sfi_standings"] = len(df)
                _stamped_sfi_std_df = stamp_available_at_explicit(df, when=datetime.now(UTC))
                manifest.record_captured(
                    row_key={"date": date, "data_type": "SFI_STANDINGS"},
                    df=_stamped_sfi_std_df,
                    category="sports",
                    instrument_type="",
                    data_type="SFI_STANDINGS",
                    pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                    service_emission_state=None,
                )
                logger.info("SFI standings: %d rows written", len(df))
            else:
                manifest.record_empty(
                    row_key={"date": date, "data_type": "SFI_STANDINGS"},
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                )
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sfi_standings_batch",
                shard=date,
            )
            _err_code = _classify_adapter_failure(exc, "soccer_football_info")
            manifest.record_failed(
                row_key={"date": date, "data_type": "SFI_STANDINGS"},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
            )

    # Progressive stats — per-match 30-second interval time-series.
    # Requires SFI match IDs for the date, then fetches progressive data
    # for each completed match. Written as entity=progressive_stats.
    #
    # Pre-cutoff / known-gap skip: SFI's progressive endpoint has a hard
    # historical floor (probed live 2026-04-30: pre-2020-01-01 returns
    # empty for every match). Honour the per-(source, data_type) coverage
    # start in UAC + any registered known-gap windows so the VM doesn't
    # burn rate-limit quota grinding through dead range.
    _sfi_pp_floor = get_source_coverage_start("soccer_football_info", data_type="SFI_PROGRESSIVE_STATS")
    _sfi_pp_pre_cutoff = bool(_sfi_pp_floor) and date < _sfi_pp_floor.isoformat()
    _sfi_pp_in_known_gap = is_in_known_gap("soccer_football_info", "SFI_PROGRESSIVE_STATS", date)
    if _want_sfi_progressive and (_sfi_pp_pre_cutoff or _sfi_pp_in_known_gap):
        logger.info(
            "SFI progressive stats: skipping date=%s (%s)",
            date,
            "pre-coverage-start" if _sfi_pp_pre_cutoff else "known-gap",
        )
        # Honest-coverage Phase 2.E.2: pre-source-coverage-start vs paused-league
        # window get distinct EXPECTED_* reasons so downstream consumers can
        # classify legacy null-reason rows without re-deriving the calendar.
        _sfi_reason = "EXPECTED_PRE_SOURCE_COVERAGE_START" if _sfi_pp_pre_cutoff else "EXPECTED_PAUSED_LEAGUE"
        manifest.record_expected_empty(
            row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
            reason=_sfi_reason,
            attempted_at=attempt_ts,
            pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
        )
        for _exp_lid in sorted(_expected_sfi_league_ids):
            manifest.record_expected_empty(
                row_key={
                    "date": date,
                    "data_type": "SFI_PROGRESSIVE_STATS",
                    "league_id": _exp_lid,
                },
                reason=_sfi_reason,
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
            )
        _want_sfi_progressive = False
    if _want_sfi_progressive:
        try:
            # League-scoped fetch: SFI's day-list returns ~50 championships'
            # worth of matches but our prediction set is ~4 leagues. Filter
            # match descriptors by championship_id BEFORE the per-match
            # progressive call so we don't burn ~10x RapidAPI quota on
            # leagues we'll never use as features.
            _sfi_descriptors = await adapter.get_match_descriptors_for_date(date)
            _expected_sfi_hex_ids = {
                get_provider_league_id(_canonical, "soccer_football_info") for _canonical in _expected_sfi_league_ids
            }
            _expected_sfi_hex_ids.discard(None)
            _expected_sfi_hex_ids.discard("")
            # Build match_id -> canonical league_id map BEFORE the per-match
            # loop so each progressive-stats entry can be tagged with its
            # league for per-league partitioning.  SOCCER_FOOTBALL_INFO_IDS
            # is canonical->hex; reverse it for hex->canonical lookup.
            _sfi_canonical_by_hex: dict[str, str] = {
                _hex: _canonical for _canonical, _hex in SOCCER_FOOTBALL_INFO_IDS.items()
            }
            _match_to_canonical: dict[str, str] = {}
            sfi_match_ids: list[str] = []
            for _d in _sfi_descriptors:
                _hex = _d["championship_id"]
                if _hex not in _expected_sfi_hex_ids:
                    continue
                _mid = _d["match_id"]
                _canonical_lid = _sfi_canonical_by_hex.get(str(_hex), "")
                if _canonical_lid:
                    _match_to_canonical[str(_mid)] = _canonical_lid
                sfi_match_ids.append(_mid)
            logger.info(
                "SFI progressive: %d/%d matches in mapped prediction leagues for date=%s",
                len(sfi_match_ids),
                len(_sfi_descriptors),
                date,
            )
            if sfi_match_ids:
                all_progressive: list[dict[str, str | int | float | None]] = []
                for mid in sfi_match_ids:
                    try:
                        stats = await adapter.get_progressive_stats(mid)
                        _canonical_for_match = _match_to_canonical.get(str(mid), "")
                        # Derive match_end_time + report_time once per match
                        # (detect_match_end_time needs CanonicalProgressiveStats objects,
                        # which we have before dict-coercion below).
                        _mid_kickoff = datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), 15, 0, tzinfo=UTC)
                        _mid_match_end = _sfi_detect_match_end_time(stats, _mid_kickoff)
                        _mid_report_time: datetime | None = (
                            _mid_match_end + timedelta(seconds=SFI_DATA_LAG_P95_SECONDS)
                            if _mid_match_end is not None
                            else None
                        )
                        for entry in stats:
                            _row: dict[str, str | int | float | None] = {
                                k: str(v) if v is not None else None for k, v in _coerce_adapter_output(entry).items()
                            }
                            # Tag for per-league partitioning at write time.
                            _row["league_id"] = _canonical_for_match or None
                            # Per-match timing fields (None for in-progress or short matches).
                            _row["match_end_time"] = _mid_match_end.isoformat() if _mid_match_end is not None else None
                            _row["report_time"] = _mid_report_time.isoformat() if _mid_report_time is not None else None
                            all_progressive.append(_row)
                    except Exception as exc:
                        classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sfi_progressive_stats_fetch",
                            shard=mid,
                        )
                if all_progressive:
                    df = pd.DataFrame(all_progressive)
                    # PIT safety: progressive stat tick became available at kickoff + timer_seconds.
                    # Without per-match kickoff lookup, approximate using date at 15:00 UTC (common match hour).
                    if "timer_seconds" in df.columns:
                        _sfi_kickoff = pd.Timestamp(date, tz="UTC") + pd.Timedelta(hours=15)
                        df["data_available_at"] = _sfi_kickoff + pd.to_timedelta(
                            pd.to_numeric(df["timer_seconds"], errors="coerce"), unit="s"
                        )
                    # Per-league partitioned write — single SSOT, no bare write.
                    _sfi_pp_captured: set[str] = set()
                    if "league_id" in df.columns:
                        _has_league = df["league_id"].notna() & (df["league_id"].astype(str).str.strip() != "")
                        _with_league = df[_has_league]
                        _without_league = df[~_has_league]

                        for _pp_lid, _pp_league_df in _with_league.groupby("league_id"):
                            _pp_lid_str = str(_pp_lid)
                            _sfi_pp_captured.add(_pp_lid_str)
                            _gated_sink_write(
                                sink,
                                data=_pp_league_df,
                                partition={
                                    "day": date,
                                    "entity": "progressive_stats",
                                    "league": _pp_lid_str,
                                },
                                filename="progressive_stats.parquet",
                                venue="soccer_football_info",
                                entity="progressive_stats",
                            )
                            _stamped_pp_df = stamp_available_at_explicit(_pp_league_df, when=datetime.now(UTC))
                            manifest.record_captured(
                                row_key={
                                    "date": date,
                                    "data_type": "SFI_PROGRESSIVE_STATS",
                                    "league_id": _canonical_league_id(_pp_lid_str),
                                },
                                df=_stamped_pp_df,
                                category="sports",
                                instrument_type="",
                                data_type="SFI_PROGRESSIVE_STATS",
                                league_id=_canonical_league_id(_pp_lid_str),
                                pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                                service_emission_state=None,
                            )

                        if not _without_league.empty:
                            logger.warning(
                                "SFI_PROGRESSIVE_STATS bare-path fallback triggered for date=%s — data shape regression: "
                                "%d rows missing league_id (championship_id->canonical mapping returned empty). "
                                "Skipping bare write to keep manifest honest.",
                                date,
                                len(_without_league),
                            )
                    else:
                        logger.warning(
                            "SFI_PROGRESSIVE_STATS bare-path fallback triggered for date=%s — data shape regression: "
                            "df missing league_id column entirely (rows=%d). "
                            "Skipping bare write + manifest row to keep manifest honest.",
                            date,
                            len(df),
                        )
                    counts["progressive_stats"] = len(df)
                    # Per-league empty_confirmed for in-season leagues that
                    # had no captured rows (mirrors WEATHER / per-fixture
                    # honest-coverage pattern).
                    for _exp_lid in sorted(_expected_sfi_league_ids - _sfi_pp_captured):
                        manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": "SFI_PROGRESSIVE_STATS",
                                "league_id": _exp_lid,
                            },
                            attempted_at=attempt_ts,
                            pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                        )
                    logger.info("SFI progressive stats: %d rows written", len(df))
                else:
                    # Match IDs present but all per-match fetches produced zero
                    # rows (legitimate empty — matches not yet complete).
                    manifest.record_empty(
                        row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                        attempted_at=attempt_ts,
                        pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                    )
                    for _exp_lid in sorted(_expected_sfi_league_ids):
                        manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": "SFI_PROGRESSIVE_STATS",
                                "league_id": _exp_lid,
                            },
                            attempted_at=attempt_ts,
                            pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                        )
            else:
                # No completed matches on this date (off-season / rest day).
                logger.info("SFI progressive stats: no completed matches for date=%s", date)
                manifest.record_empty(
                    row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                )
                for _exp_lid in sorted(_expected_sfi_league_ids):
                    manifest.record_empty(
                        row_key={
                            "date": date,
                            "data_type": "SFI_PROGRESSIVE_STATS",
                            "league_id": _exp_lid,
                        },
                        attempted_at=attempt_ts,
                        pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                    )
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sfi_progressive_stats_batch",
                shard=date,
            )
            _err_code = _classify_adapter_failure(exc, "soccer_football_info")
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "soccer_football_info",
                    "endpoint": "get_match_ids_for_date",
                    "date": date,
                    "error": str(exc),
                    "error_code": _err_code,
                },
            )
            manifest.record_failed(
                row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
            )
            for _exp_lid in sorted(_expected_sfi_league_ids):
                manifest.record_failed(
                    row_key={
                        "date": date,
                        "data_type": "SFI_PROGRESSIVE_STATS",
                        "league_id": _exp_lid,
                    },
                    error=_err_code,
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                )

    manifest.write()

    return counts


def _load_venue_coordinates(bucket: str) -> dict[str, tuple[float, float]]:
    """Load venue_id → (lat, lon) lookup from the global venues.parquet.

    Returns an empty dict if the file does not exist or cannot be read.
    The venues.parquet is written by ``_write_venues_from_teams`` at:
        sports_reference/venues/venues.parquet
    """
    venues_path = "sports_reference/venues/venues.parquet"
    try:
        storage = get_storage_client()
        blob = storage.bucket(bucket).blob(venues_path)
        if not blob.exists():
            logger.info("Weather: venues.parquet not found at gs://%s/%s", bucket, venues_path)
            return {}
        local = f"{tempfile.gettempdir()}/_weather_venues.parquet"
        blob.download_to_filename(local)
        venues_df = pd.read_parquet(local)
        if "venue_id" not in venues_df.columns:
            logger.warning("Weather: venues.parquet missing 'venue_id' column")
            return {}
        coords: dict[str, tuple[float, float]] = {}
        has_lat = "latitude" in venues_df.columns
        has_lon = "longitude" in venues_df.columns
        if not has_lat or not has_lon:
            logger.warning("Weather: venues.parquet missing latitude/longitude columns")
            return {}
        for _, row in venues_df.iterrows():
            vid = str(row["venue_id"])
            lat = row["latitude"]
            lon = row["longitude"]
            # pandas returns NaN for missing values, not None
            if pd.notna(lat) and pd.notna(lon):
                lat_f = float(lat)
                lon_f = float(lon)
                if lat_f != 0.0 and lon_f != 0.0:
                    coords[vid] = (lat_f, lon_f)
        return coords
    except Exception as exc:
        logger.debug("Weather: could not load venues.parquet: %s", exc)
        return {}


def _extract_fixture_venue_ids(bucket: str, date: str) -> list[str]:
    """Extract venue_ids from the fixtures parquet for a given date.

    Fixtures store the ``venue`` field as a dict with a ``venue_id`` key.
    Returns deduplicated venue IDs for fixtures on the requested date.
    """
    prefix = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
    try:
        storage = get_storage_client()
        blob = storage.bucket(bucket).blob(prefix)
        if not blob.exists():
            logger.debug("Weather: no fixtures parquet at gs://%s/%s", bucket, prefix)
            return []
        local = f"{tempfile.gettempdir()}/_weather_fixtures_{date}.parquet"
        blob.download_to_filename(local)
        df = pd.read_parquet(local)
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
        logger.debug("Weather: could not read fixtures for date=%s: %s", date, exc)
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

    from unified_api_contracts.canonical.domain.sports.league_data import (
        get_expected_leagues_for_source,
    )

    from instruments_service.reference_data.adapters.sports.adapters.open_meteo import OpenMeteoAdapter

    adapter = OpenMeteoAdapter(api_key=api_key) if api_key else OpenMeteoAdapter()
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = datetime.now(UTC)
    _expected_weather_league_ids = {
        lg.league_id for lg in get_expected_leagues_for_source("open_meteo", classifications=["Prediction"])
    }

    def _record_weather_empty() -> None:
        """Helper — emit per-league record_empty for WEATHER shard.

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
                pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
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
                pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
            )

    # UAC venue coordinates: SCREAMING_SNAKE keys → (lat, lon)
    from unified_api_contracts.registry.sports_venue_coordinates import VENUE_COORDINATES

    # 1. Read fixtures for this date — get venue_name + kickoff hour
    fixtures_df = None
    _fixtures_read_failed = False
    try:
        fixtures_prefix = f"sports_reference/by_date/day={date}/entity=fixtures/"
        storage_client = get_storage_client()
        blobs = list(storage_client.list_blobs(bucket=bucket, prefix=fixtures_prefix, max_results=50))
        parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
        if parquet_blobs:
            frames = []
            for blob_meta in parquet_blobs:
                data = storage_client.download_bytes(bucket=bucket, blob_path=blob_meta.name)
                frames.append(pd.read_parquet(io.BytesIO(data)))
            fixtures_df = pd.concat(frames, ignore_index=True) if frames else None
    except Exception as exc:
        logger.warning("Weather: could not read fixtures for date=%s: %s", date, exc)
        _fixtures_read_failed = True
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="weather_fixtures_read",
            shard=date,
        )
        _err_code = _classify_adapter_failure(exc, "open_meteo")
        log_event(
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
        logger.info("Weather: no fixture venue_name data for date=%s — skipping", date)
        # Honest-coverage: no fixtures == legitimate empty for the WEATHER
        # shard.  Record empty so attempt-coverage is honest.
        _record_weather_empty()
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
        logger.info("Weather: %d fixture venues not in UAC coordinates for date=%s", skipped, date)
    logger.info(
        "Weather: %d fixture venues matched to coordinates for date=%s (of %d unique)",
        len(venues_with_coords),
        date,
        len(seen_venues),
    )

    # 3. Check existing weather data — only fetch venues not already covered.
    # Enables incremental runs: add more venue coords → re-run → only new venues fetched.
    existing_venue_ids: set[str] = set()
    try:
        weather_prefix = f"sports_reference/by_date/day={date}/entity=weather/"
        weather_blobs = list(storage_client.list_blobs(bucket=bucket, prefix=weather_prefix, max_results=10))
        for wb in weather_blobs:
            if wb.name.endswith(".parquet"):
                wdata = storage_client.download_bytes(bucket=bucket, blob_path=wb.name)
                wdf = pd.read_parquet(io.BytesIO(wdata))
                if "venue_id" in wdf.columns:
                    existing_venue_ids = set(wdf["venue_id"].dropna().astype(str).unique())
    except Exception:
        pass  # No existing weather — fetch everything

    if existing_venue_ids:
        new_venues = [(lat, lon, vid, ko) for lat, lon, vid, ko in venues_with_coords if vid not in existing_venue_ids]
        if len(new_venues) < len(venues_with_coords):
            logger.info(
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
                    manifest.record_captured_from_counts(
                        row_key={"date": date, "data_type": "WEATHER", "league_id": _canonical_league_id(_lid_str)},
                        total_rows=1,
                        expected_root_clusters={},
                        observed_clusters={"": 1},
                        available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                        pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
                        service_emission_state=None,
                    )
            for _exp_lid in sorted(_expected_weather_league_ids - _captured_leagues_covered):
                manifest.record_empty(
                    row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
                )
            logger.info(
                "Weather: all %d venues already covered for date=%s — back-filled per-league manifest (%d captured, %d empty)",
                len(existing_venue_ids),
                date,
                len(_captured_leagues_covered),
                len(_expected_weather_league_ids - _captured_leagues_covered),
            )
            manifest.write()
            return counts
        logger.info("Weather: no fixture venues with coordinates for date=%s — skipping", date)
        # Honest-coverage: fixtures existed but none mapped to coordinates —
        # legitimate empty (not a failure).
        _record_weather_empty()
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
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="weather_fetch",
                shard=venue_key,
            )
            _err_code = _classify_adapter_failure(exc, "open_meteo")
            log_event(
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
        new_df = pd.DataFrame(weather_rows)
        # PIT safety: weather observation/forecast availability.
        # Prefer existing observation_time/forecast_issue_time columns; otherwise fallback to date + 12:00 UTC.
        if "observation_time" in new_df.columns:
            new_df["data_available_at"] = pd.to_datetime(new_df["observation_time"], utc=True, errors="coerce")
        elif "forecast_issue_time" in new_df.columns:
            new_df["data_available_at"] = pd.to_datetime(new_df["forecast_issue_time"], utc=True, errors="coerce")
        else:
            new_df["data_available_at"] = pd.Timestamp(date, tz="UTC") + pd.Timedelta(hours=12)

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
                        existing_df = pd.read_parquet(io.BytesIO(wdata))
                        new_df = pd.concat([existing_df, new_df], ignore_index=True)
                        logger.info(
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
            _per_league_frames: dict[str, list[pd.DataFrame]] = {}
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
                _w_lid_df = pd.concat(_frames, ignore_index=True)
                _captured_leagues.add(_lid_v)
                _league_venue_count[_lid_v] = _league_venue_count.get(_lid_v, 0) + len(_w_lid_df)
                _gated_sink_write(
                    sink,
                    data=_w_lid_df,
                    partition={"day": date, "entity": "weather", "league": _canonical_league_id(_lid_v)},
                    filename="weather.parquet",
                    venue="open_meteo",
                    entity="weather",
                )

            if _orphan_count > 0:
                logger.warning(
                    "WEATHER bare-path fallback triggered for date=%s — data shape regression: "
                    "%d rows could not be mapped to a league via venue_id. "
                    "Skipping bare write to keep manifest honest.",
                    date,
                    _orphan_count,
                )
        else:
            logger.warning(
                "WEATHER bare-path fallback triggered for date=%s — data shape regression: "
                "no venue->league map (fixtures_df missing league_id/venue_name); rows=%d. "
                "Skipping bare write to keep manifest honest.",
                date,
                len(new_df),
            )

        counts["weather"] = len(new_df)
        logger.info("Weather: %d venue observations written for date=%s", len(new_df), date)
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
            manifest.record_captured_from_counts(
                row_key={"date": date, "data_type": "WEATHER", "league_id": _canonical_league_id(_lid)},
                total_rows=_count,
                expected_root_clusters={},
                observed_clusters={"": _count},
                available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
                service_emission_state=None,
            )
        # Per-league empty_confirmed for in-season leagues with no captured weather
        for _exp_lid in sorted(_expected_weather_league_ids - _captured_leagues):
            manifest.record_empty(
                row_key={"date": date, "data_type": "WEATHER", "league_id": _exp_lid},
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
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
        _record_weather_empty()

    manifest.write()

    return counts


async def fill_solana_creation_cache(
    api_keys: dict[str, str] | None = None,
) -> dict[str, int]:
    """Discover all Solana pool addresses and fill the creation timestamp cache.

    Runs all Solana adapters once to discover pool addresses, then uses
    Alchemy RPC to resolve creation timestamps for all discovered addresses.
    Results are saved to GCS cache for all future runs.

    Returns:
        Dict with cache statistics (cached, new, unresolved).
    """
    # 1. Discover all Solana pool addresses by running each adapter
    all_addresses: list[str] = []
    with SolanaCacheSession():
        fetch_result = await fetch_instruments_for_all_venues(_SOLANA_DEFI_VENUES, api_keys=api_keys, mode="batch")

    # Extract raw_symbol (which is the pool/account address) from each instrument
    for record in fetch_result.records:
        raw_sym = getattr(record, "raw_symbol", None)
        if raw_sym and isinstance(raw_sym, str) and len(raw_sym) > 20:
            all_addresses.append(raw_sym)

    if not all_addresses:
        logger.warning("Solana cache fill: no pool addresses discovered")
        return {"cached": 0, "new": 0, "unresolved": 0}

    # Deduplicate
    unique_addresses = list(dict.fromkeys(all_addresses))
    logger.info(
        "Solana cache fill: discovered %d unique pool addresses from %d instruments",
        len(unique_addresses),
        len(fetch_result.records),
    )

    # 2. Fill the cache with higher concurrency
    with SolanaCacheSession():
        results = await fill_solana_cache(unique_addresses, concurrency=4)

    cached_count = len(results)
    unresolved = len(unique_addresses) - cached_count
    logger.info(
        "Solana cache fill complete: %d resolved, %d unresolved out of %d total",
        cached_count,
        unresolved,
        len(unique_addresses),
    )
    return {"cached": cached_count, "new": cached_count, "unresolved": unresolved}


def _get_instruments_bucket(asset_group: str | None = None) -> str:
    """Resolve the instruments write bucket for the given asset group.

    Prod:  instruments-store-{asset_group.lower()}-{project_id}
    Test:  instruments-store-{asset_group.lower()}-test-{project_id}

    When ``IS_TEST_RUN=true``, writes route to the canonical ``-test-`` variant
    (``-test-`` inserted between asset group and project_id — matches the 77
    buckets provisioned by
    ``deployment-service/scripts/provision-test-buckets.sh``). SSOT:
    ``codex/02-data/per-category-bucket-layouts.md``.

    Delegates to UTL ``get_write_bucket_name`` which already handles the
    ``IS_TEST_RUN`` gate via env var.
    """
    cfg = get_config()
    project = cfg.gcp_project_id or "test-project"

    try:
        return get_write_bucket_name("instruments", asset_group, project)
    except (ImportError, AttributeError):
        # Dev-environment fallback when UTL cloud_constants is unavailable.
        cat_lower = asset_group.lower() if asset_group else None
        prefix = cfg.instruments_bucket_prefix
        prod_bucket = f"{prefix}-{cat_lower}-{project}" if cat_lower else f"{prefix}-{project}"
        if not cfg.is_test_run:
            return prod_bucket
        return prod_bucket.replace(f"-{project}", f"-test-{project}", 1)


def _check_emission_policy(
    *,
    date: str,
    completeness_fraction: float,
    correlation_id: str | None = None,
) -> EmissionDecision:
    """Return publish decision for catalog_snapshot emission boundary.

    PARTIAL_OK — catalog_snapshot is a best-effort union of multiple source feeds;
    partial coverage is normal (some venues may lag or be unavailable on any given day).
    completeness < 1.0 → emits PUBLISHED_DEGRADED but still writes.
    completeness == 0.0 → still writes (PARTIAL_OK never suppresses).
    Only STRICT_FAIL / BLOCK_CRITICAL would suppress a write — not applicable here.

    UAC seed: ("instruments-service", "catalog_snapshot"): ServiceEmissionPolicy.PARTIAL_OK
    Plan: writegate_honest_coverage_endtoend_2026_05_06.md Phase 6.8 PART B.
    """
    return publish_with_policy(
        service=_SERVICE_NAME,
        output_data_type="catalog_snapshot",
        row_key={"date": date},
        completeness_fraction=completeness_fraction,
        correlation_id=correlation_id,
    )


def _write_catalogue_record(bucket: str, path: str, date: str, record_count: int) -> None:
    """Update the consolidated availability index in the instruments bucket.

    Merges this venue/date record into:
      gs://{bucket}/_index/availability_index.parquet

    Downstream services call read_availability_index(bucket) to check completeness
    without listing thousands of GCS blobs.

    v4: Extracts chain (DeFi), data_type (prediction markets), league_id from path.
    """
    try:
        venue_match = re.search(r"venue=([^/]+)", path)
        venue_str = venue_match.group(1) if venue_match else ""
        date_match = re.search(r"day[=-](\d{4}-\d{2}-\d{2})", path)
        date_str = date_match.group(1) if date_match else date
        parsed = date_type.fromisoformat(date_str)

        # v4: Extract shard dimensions from path/venue
        manifest_venue = venue_str
        manifest_chain = ""
        manifest_data_type = ""
        manifest_league_id = ""

        # Sports: venue → data_type (API_FOOTBALL_INJURIES → data_type=INJURIES, venue="")
        _sports_prefixes = ("API_FOOTBALL", "TRANSFERMARKT", "FOOTYSTATS", "SFI", "UNDERSTAT", "WEATHER")
        if venue_str.startswith(_sports_prefixes):
            if venue_str == "API_FOOTBALL":
                manifest_data_type = "FIXTURES"
            else:
                for pfx in _sports_prefixes:
                    if venue_str.startswith(pfx + "_"):
                        manifest_data_type = venue_str[len(pfx) + 1 :]
                        break
                else:
                    manifest_data_type = venue_str
            manifest_venue = ""
        # DeFi: split AAVEV3-ETHEREUM → venue=AAVE_V3, chain=ETHEREUM
        elif "-" in venue_str:
            try:
                from unified_api_contracts.registry.capability_declarations._defi import (
                    KNOWN_CHAINS,
                    parse_defi_venue,
                )

                protocol, chain = parse_defi_venue(venue_str)
                if chain in KNOWN_CHAINS:
                    manifest_venue = protocol.upper()
                    manifest_chain = chain
            except (ImportError, ValueError):
                pass
        # Prediction: split POLYMARKET:BTC → venue=POLYMARKET, data_type=BTC
        elif ":" in venue_str:
            parts = venue_str.split(":", 1)
            manifest_venue = parts[0]
            manifest_data_type = parts[1]

        # Sports: extract league from path
        league_match = re.search(r"league=([^/]+)", path)
        if league_match:
            manifest_league_id = league_match.group(1)

        writer = ManifestWriter(
            service_name="instruments-service",
            catalogue_bucket=bucket,
        )
        writer.record_captured_from_counts(
            row_key={
                "date": str(parsed),
                "data_type": manifest_data_type,
                "venue": manifest_venue,
                "chain": manifest_chain,
                "league_id": manifest_league_id,
            },
            total_rows=record_count,
            expected_root_clusters={},
            observed_clusters={"": record_count},
            available_at_envelope=pd.Timestamp(datetime.now(UTC)),
            pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
            service_emission_state=None,
        )
        writer.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="manifest_writer",
            shard=path,
        )


async def refresh_catalogue(
    asset_groups: list[str] | None = None,
    api_keys: dict[str, str] | None = None,
) -> dict[str, str]:
    """Rebuild the canonical instrument catalogue for the requested asset groups.

    For each group in :data:`CATALOGUE_SUPPORTED_ASSET_GROUPS` this uses
    :class:`CatalogueBuilder` to fetch instruments through URDI and writes
    the result to ``reference_data/instruments/{asset_group}/all.parquet``.

    Returns a mapping of ``asset_group -> written URI`` for observability.
    """
    from instruments_service.reference_data.catalogue import (
        CATALOGUE_SUPPORTED_ASSET_GROUPS,
        CatalogueBuilder,
    )

    builder = CatalogueBuilder(api_keys=api_keys)
    target = [c.upper() for c in (asset_groups or list(CATALOGUE_SUPPORTED_ASSET_GROUPS))]
    written: dict[str, str] = {}
    for ag in target:
        if ag not in CATALOGUE_SUPPORTED_ASSET_GROUPS:
            logger.warning("refresh_catalogue: skipping unknown asset_group=%s", ag)
            continue
        records = await builder.build_asset_group_async(ag)
        written[ag] = builder.write_to_gcs(records, ag)
    return written
