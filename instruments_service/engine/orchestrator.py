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
  1. Skip venues not yet launched on that date (startup dates in _VENUE_LAUNCH_DATES)
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
from datetime import UTC, datetime
from datetime import date as date_type

import pandas as pd
from unified_api_contracts import (
    BUNDESLIGA_TEAM_ALIASES,
    CANONICAL_TO_ODDS_API_BUNDESLIGA,
    CANONICAL_TO_ODDS_API_EPL,
    CANONICAL_TO_UNDERSTAT_EPL,
    DEX_VENUE_KEYWORDS,
    EPL_TEAM_ALIASES,
    VenueMapping,
    get_prediction_leagues,
)
from unified_api_contracts.internal import InstrumentRecord, validate_instrument_records
from unified_api_contracts.registry import get_supported_chains_for_protocol
from unified_trading_library import (
    DataSink,
    DomainValidationService,
    ManifestWriter,
    SamplingService,
    check_shard_freshness,
    classify_and_emit_error,
    create_sampling_service,
    get_bucket_name,
    get_data_sink,
    get_storage_client,
    log_event,
    read_availability_index,
)
from unified_trading_library import unified_config as _uc

from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets
from instruments_service.reference_data.adapters._solana_utils import SolanaCacheSession, fill_solana_cache
from instruments_service.reference_data.adapters.api_football import (
    _last_completed_fixture_ids as _urdi_completed_fixture_ids,
)
from instruments_service.reference_data.adapters.sports import create_sports_reference_adapter
from instruments_service.reference_data.utils.evm_creation_resolver import EvmCacheSession

logger = logging.getLogger(__name__)

# Venue launch dates SSOT: UAC VenueMapping.venue_start_dates (canonical PROTOCOL-CHAIN format).
# No local copy — read from VenueMapping at module load.
_VENUE_MAPPING = VenueMapping()
_VENUE_LAUNCH_DATES: dict[str, str] = _VENUE_MAPPING.venue_start_dates

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
}

# Protocols that don't use subgraphs (Ethereum-only, custom data sources).
_STATIC_DEFI_VENUES: list[str] = [
    "LIDO-ETHEREUM",
    "ETHERFI-ETHEREUM",
    "ETHENA-ETHEREUM",
]

# Solana DeFi venues (non-EVM, REST API-based discovery).
_SOLANA_DEFI_VENUES: list[str] = [
    "DRIFT-SOLANA",
    "KAMINO-SOLANA",
    "RAYDIUM-SOLANA",
    "ORCA-SOLANA",
    "MARINADE-SOLANA",
    "JITO-SOLANA",
    # Jupiter is execution-only (swap aggregator), not instrument discovery.
]


def _build_defi_venues() -> list[str]:
    """Build venue list from protocols that have subgraph IDs + static venues."""
    venues: list[str] = []
    for protocol, prefix in _SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX.items():
        for chain in get_supported_chains_for_protocol(protocol):
            venues.append(f"{prefix}-{chain}")
    venues.extend(_STATIC_DEFI_VENUES)
    venues.extend(_SOLANA_DEFI_VENUES)
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
    "COINBASE-SPOT",
    "HYPERLIQUID",
    "UPBIT",
    "ASTER",
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
            if base in major:
                result.append(r)
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


def get_venues_for_categories(categories: list[str]) -> list[str]:
    """Return UAC canonical venue names for the requested market categories."""
    venues: list[str] = []
    for cat in categories:
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
            venues.extend(["API_FOOTBALL"])
        if cat_upper in ("PREDICTION", "ALL"):
            # POLYMARKET + KALSHI: prediction market instruments (crypto up/down, soccer, macro).
            # No auth required — Gamma API (Polymarket) and public API (Kalshi) are keyless.
            venues.extend(["POLYMARKET", "KALSHI"])
    return list(dict.fromkeys(venues))


def is_venue_available(venue: str, date: str) -> bool:
    """Return True if the venue was launched on or before this date."""
    launch_date = _VENUE_LAUNCH_DATES.get(venue)
    if launch_date is None:
        return True  # Unknown venue — assume always available
    return date >= launch_date


def earliest_venue_date(venues: list[str]) -> str | None:
    """Return the earliest launch date across the given venues, or None if unknown."""
    dates = [_VENUE_LAUNCH_DATES[v] for v in venues if v in _VENUE_LAUNCH_DATES]
    return min(dates) if dates else None


async def process_instruments(
    date: str | datetime,
    categories: list[str],
    redo_all: bool = False,
    api_keys: dict[str, str] | None = None,
    venue_override: list[str] | None = None,
    mode: str = "batch",
) -> dict[str, int]:
    """Process instruments for a single date and set of market categories.

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
    venues = venue_override if venue_override is not None else get_venues_for_categories(categories)

    # Track which per-fixture entities are missing (set in skip-if-exists check).
    # Empty = fetch everything; non-empty = only fetch these per-fixture entities.
    _enrichment_only_entities: list[str] = []

    # 1. Skip venues not yet launched
    active_venues = [v for v in venues if is_venue_available(v, date)]
    if not active_venues:
        logger.info("No active venues for date=%s categories=%s", date, categories)
        return {}

    # 1b. Skip-if-exists: check manifest for fresh data (unless --force)
    if not redo_all:
        primary_category = categories[0] if categories else None
        bucket = _get_instruments_bucket(primary_category)

        # For SPORTS, require both core AND per-fixture reference entities.
        # Core: leagues/teams/standings/injuries (slow-moving, fetched every run).
        # Per-fixture: fixture_stats/events/lineups/player_stats (one API call per
        # completed fixture, rate-limited to 1 req/sec — expensive to re-fetch).
        expected = list(active_venues)
        is_sports_run = any(c.upper() in ("SPORTS", "ALL") for c in categories)
        _sports_core_entities = [
            "sports_reference_leagues",
            "sports_reference_teams",
            "sports_reference_standings",
            "sports_reference_injuries",
        ]
        _sports_per_fixture_entities = [
            "sports_reference_fixture_stats",
            "sports_reference_fixture_events",
            "sports_reference_fixture_lineups",
            "sports_reference_player_stats",
        ]
        if is_sports_run:
            expected.extend(_sports_core_entities)
            expected.extend(_sports_per_fixture_entities)

        is_fresh, stale, missing = check_shard_freshness(
            bucket=bucket,
            date=date,
            service_name="instruments-service",
            expected_venues=expected,
        )
        if is_fresh:
            logger.info(
                "SKIP date=%s: all %d venues/entities already fresh in manifest (use --force to re-fetch)",
                date,
                len(expected),
            )
            return {}

        # Detect partial completion: core entities done but per-fixture missing.
        # In this case, skip the expensive URDI + core fetch and jump straight
        # to per-fixture enrichment (saves ~33 API calls per league).
        if is_sports_run and missing:
            missing_set = set(missing)
            core_missing = missing_set & set(_sports_core_entities)
            pf_missing = [e for e in _sports_per_fixture_entities if e in missing_set]
            instruments_missing = missing_set - set(_sports_core_entities) - set(_sports_per_fixture_entities)
            if not core_missing and not instruments_missing and pf_missing:
                _enrichment_only_entities = pf_missing
                logger.info(
                    "date=%s: core entities fresh, %d per-fixture entities missing — enrichment-only mode: %s",
                    date,
                    len(pf_missing),
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

    # Fast path: if only per-fixture enrichment is missing, skip URDI fetch entirely
    # and jump straight to sports enrichment.
    if _enrichment_only_entities and api_keys:
        api_football_key = api_keys.get("api_football")
        if api_football_key:
            primary_category = categories[0] if categories else None
            bucket = _get_instruments_bucket(primary_category)
            logger.info(
                "ENRICHMENT-ONLY date=%s: skipping URDI fetch, only fetching %s",
                date,
                _enrichment_only_entities,
            )
            sports_ref_counts = await _fetch_sports_reference_data(
                date=date,
                api_key=api_football_key,
                bucket=bucket,
                enrichment_only=True,
            )
            # Write manifest for newly fetched entities
            if sports_ref_counts:
                try:
                    sports_manifest = ManifestWriter(
                        service_name="instruments-service",
                        catalogue_bucket=bucket,
                    )
                    for entity_name, row_count in sports_ref_counts.items():
                        sports_manifest.add(
                            processing_date=date_type.fromisoformat(date),
                            row_count=row_count,
                            venue=f"SPORTS_REFERENCE_{entity_name.upper()}",
                        )
                    sports_manifest.write()
                    logger.info(
                        "Enrichment-only manifest: %d entities for %s",
                        len(sports_ref_counts),
                        date,
                    )
                except Exception as exc:
                    logger.warning("Enrichment-only manifest write failed (non-blocking): %s", exc)
            return sports_ref_counts

    log_event(
        "PROCESSING_STARTED",
        details={"date": date, "categories": categories, "venue_count": len(active_venues)},
    )

    # 2. Fetch from URDI — sole external API path
    # api_keys injected from preflight() → validate_api_keys_for_venues() → Secret Manager
    # date passed so date-aware adapters (e.g. API-Football) can filter server-side
    #
    # DeFi batch optimisation: DeFi instruments are monotonically growing
    # (immutable contracts, never deleted). In batch mode, the universe is
    # fetched ONCE and cached — subsequent dates in the range just filter
    # by available_from_datetime. Non-DeFi venues are fetched fresh per date.
    defi_venue_names = frozenset(_DEFI_VENUES)
    defi_active = [v for v in active_venues if v in defi_venue_names]
    non_defi_active = [v for v in active_venues if v not in defi_venue_names]

    records: list[InstrumentRecord] = []
    _retryable_venues: list[str] = []
    # Track venues where the adapter ran without error (even if 0 records returned).
    # Used by the completeness check to distinguish "adapter returned nothing for this
    # date range" (OK) from "adapter failed to respond" (completeness failure).
    _non_error_venues: set[str] = set()

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

    # 3. Filter to instruments active on the requested date.
    # URDI adapters return the full historical instrument universe; this reduces
    # it to only instruments tradeable on the requested day.
    # Pass the DeFi venue set so the filter can warn on missing available_from_datetime.
    is_defi_run = any(c.upper() in ("DEFI", "ALL") for c in categories)
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
    if any(c.upper() in ("DEFI", "ALL") for c in categories):
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
        is_sports_only = all(c.upper() == "SPORTS" for c in categories)
        if is_sports_only:
            primary_category = categories[0] if categories else None
            bucket = _get_instruments_bucket(primary_category)
            sink = get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
            empty_df = pd.DataFrame(columns=["fixture_id", "venue", "league_id", "kickoff_utc", "status"])
            sink.write(
                data=empty_df,
                partition={"day": date, "venue": "API_FOOTBALL"},
                format="parquet",
                filename="instruments.parquet",
            )
            _write_catalogue_record(
                bucket, f"instrument_availability/by_date/day={date}/venue=API_FOOTBALL/instruments.parquet", date, 0
            )
            logger.info("SPORTS: No fixtures for date=%s — wrote empty marker to manifest", date)
            # Still fetch sports reference data (leagues/teams/standings/injuries)
            # even when no fixtures exist. These are date-independent slow-moving
            # entities needed for downstream feature computation.
            if api_keys:
                api_football_key = api_keys.get("api_football")
                if api_football_key:
                    sports_ref_counts = await _fetch_sports_reference_data(
                        date=date,
                        api_key=api_football_key,
                        bucket=bucket,
                    )
                    if sports_ref_counts:
                        try:
                            sports_manifest = ManifestWriter(
                                service_name="instruments-service",
                                catalogue_bucket=bucket,
                            )
                            for entity_name, row_count in sports_ref_counts.items():
                                sports_manifest.add(
                                    processing_date=date_type.fromisoformat(date),
                                    row_count=row_count,
                                    venue=f"SPORTS_REFERENCE_{entity_name.upper()}",
                                )
                            sports_manifest.write()
                        except Exception as exc:
                            logger.warning("Sports reference manifest write failed (non-blocking): %s", exc)
            log_event("PROCESSING_COMPLETED", details={"date": date, "categories": categories, "fixtures": 0})
            return {"api_football": 0}
        # DeFi batch: zero records after date filter is normal for early dates
        # (venue exists in UAC but no pools created yet on-chain). Skip without error.
        is_defi_only = all(c.upper() in ("DEFI",) for c in categories)
        if is_defi_only and mode == "batch":
            logger.debug(
                "DeFi batch: zero instruments after date filter for date=%s — "
                "all venues pre-date their first pool creation. Skipping.",
                date,
            )
            return {}
        msg = (
            f"URDI returned zero records for date={date} categories={categories}. "
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
    primary_category = categories[0] if categories else None
    bucket = _get_instruments_bucket(primary_category)
    # prefix ensures writes land at instrument_availability/by_date/{day=X}/{venue=Y}/
    sink = get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            _write_venue(str(venue_name), venue_df, date, bucket, sink, counts, sampler, manifest)
    else:
        _write_venue("all", df, date, bucket, sink, counts, sampler, manifest)

    # PREDICTION shard breakdown: write per-underlying manifest entries so the
    # deployment UI shows coverage per shard (BTC, ETH, SOL, FOOTBALL, etc.)
    # instead of a single "POLYMARKET: 100%" bar.
    is_prediction = primary_category and primary_category.upper() == "PREDICTION"
    if is_prediction and "base_asset" in df.columns:
        shard_counts = _compute_prediction_shards(df)
        for shard_name, shard_count in shard_counts.items():
            manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=shard_count,
                venue=shard_name,
            )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    manifest.close()

    # 7. SPORTS enrichment: fetch and write reference data (teams, leagues, etc.)
    # alongside fixtures. These are slow-moving entities that don't change per-date
    # but are re-fetched to capture transfers, promotions, new seasons.
    is_sports = any(c.upper() in ("SPORTS", "ALL") for c in categories)
    if is_sports and api_keys:
        api_football_key = api_keys.get("api_football")
        if not api_football_key:
            logger.warning("api_football key missing from api_keys — skipping sports reference data")
        else:
            # Pass completed fixture IDs from URDI fetch to avoid 33-league re-fetch
            # (saves 33 API calls per date). _urdi_completed_fixture_ids is populated
            # during the URDI instruments fetch above.
            sports_ref_counts = await _fetch_sports_reference_data(
                date=date,
                api_key=api_football_key,
                bucket=bucket,
                fixture_ids_override=list(_urdi_completed_fixture_ids),
            )
            for k, v in sports_ref_counts.items():
                counts[k] = counts.get(k, 0) + v

            # Write manifest for sports reference entities
            try:
                sports_manifest = ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                for entity_name, row_count in sports_ref_counts.items():
                    sports_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=row_count,
                        venue=f"SPORTS_REFERENCE_{entity_name.upper()}",
                    )
                sports_manifest.write()
                logger.info(
                    "Sports reference manifest: %d entities for %s",
                    len(sports_ref_counts),
                    date,
                )
            except Exception as exc:
                logger.warning("Sports reference manifest write failed (non-blocking): %s", exc)

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
    #
    # When venues are missing (typically due to API rate limits or transient errors),
    # retry just the missing venues with exponential backoff before failing.
    expected_venues = set(active_venues)
    written_venues = set(counts.keys())

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
        if any(c.upper() in ("DEFI", "ALL") for c in categories):
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

    log_event(
        "PROCESSING_COMPLETED",
        details={"date": date, "total_records": total, "venues": len(counts)},
    )
    logger.info("instruments: date=%s wrote %d records across %d venues", date, total, len(counts))
    return counts


_KNOWN_CRYPTO_UNDERLYINGS = {"BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "BNB"}


def _extract_prediction_shard(base_asset: str) -> str:
    """Extract the underlying shard name from a PREDICTION instrument's base_asset.

    Only extracts shards for well-formed base_assets matching known patterns.
    Everything else → POLYMARKET:OTHER.

    Patterns:
      PREDICTION:POLYMARKET:UP_DOWN:BTC:1D:2026-04-05 → POLYMARKET:BTC
      FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:...          → POLYMARKET:FOOTBALL
      anything else                                    → POLYMARKET:OTHER
    """
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "POLYMARKET:FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN" and parts[3] in _KNOWN_CRYPTO_UNDERLYINGS:
        return f"POLYMARKET:{parts[3]}"
    return "POLYMARKET:OTHER"


def _compute_prediction_shards(df: pd.DataFrame) -> dict[str, int]:
    """Group PREDICTION instruments by underlying shard, return {shard_venue: count}."""
    shard_counts: dict[str, int] = {}
    for ba in df["base_asset"]:
        shard = _extract_prediction_shard(str(ba))
        shard_counts[shard] = shard_counts.get(shard, 0) + 1
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
    (caller flushes once after all venues). Otherwise falls back to per-venue
    ``_write_catalogue_record`` for backward compatibility.
    """
    import time as _time

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            sink.write(
                data=df,
                partition={"day": date, "venue": venue_str},
                format="parquet",
                filename="instruments.parquet",
            )
            # Add to batched manifest writer (flushed by caller) or legacy per-venue write
            if manifest is not None:
                manifest.add(
                    processing_date=date_type.fromisoformat(date),
                    row_count=len(df),
                    venue=venue_str,
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


async def _fetch_sports_reference_data(
    date: str,
    api_key: str,
    bucket: str,
    enrichment_only: bool = False,
    fixture_ids_override: list[int] | None = None,
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
        enrichment_only: If True, skip core entities (leagues/teams/standings/
            injuries) and only fetch per-fixture data (stats/events/lineups/
            player_stats). Used when core entities already exist in manifest.
        fixture_ids_override: Pre-computed list of completed fixture IDs from the
            URDI instruments fetch. When provided, skips the expensive 33-league
            re-fetch (saves 33 API calls per date).
    """
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    if enrichment_only:
        logger.info("Enrichment-only mode: skipping leagues/teams/standings/injuries for date=%s", date)

    # Leagues/teams/standings are slow-moving (same within a season). Cache DataFrames
    # across dates within the same batch run to save ~67 API calls per date.
    if not enrichment_only:
        leagues_df = _cached_leagues_df
        if leagues_df is None:
            try:
                leagues = await adapter.get_leagues()
                if leagues:
                    leagues_df = pd.DataFrame([lg.model_dump() for lg in leagues])
                    _set_cached_leagues(leagues_df)
                    logger.info("Sports reference: %d leagues fetched (API call — will cache)", len(leagues_df))
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_reference_leagues_fetch",
                )
        else:
            logger.info("Sports reference: %d leagues from cache (0 API calls)", len(leagues_df))
        if leagues_df is not None:
            sink.write(
                data=leagues_df,
                partition={"day": date, "entity": "leagues"},
                format="parquet",
                filename="leagues.parquet",
            )
            counts["leagues"] = len(leagues_df)

        # Teams — for each prediction league (cached across dates)
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
                            all_teams.append(t.model_dump())
                    except Exception as exc:
                        classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sports_reference_teams_fetch",
                            shard=str(league_def.league_id),
                        )
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
        else:
            prediction_league_ids = _cached_prediction_league_ids
            logger.info("Sports reference: %d teams from cache (0 API calls)", len(teams_df))
        if teams_df is not None:
            sink.write(
                data=teams_df, partition={"day": date, "entity": "teams"}, format="parquet", filename="teams.parquet"
            )
            counts["teams"] = len(teams_df)

        # Standings — for each prediction league (cached across dates)
        standings_df = _cached_standings_df
        if standings_df is None:
            all_standings: list[dict[str, object]] = []
            for lid in prediction_league_ids:
                try:
                    standings = await adapter.get_standings(lid)
                    for row in standings:
                        row["league_id"] = lid
                        all_standings.append(row)
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sports_reference_standings_fetch",
                        shard=str(lid),
                    )
            if all_standings:
                standings_df = pd.DataFrame(all_standings)
                _set_cached_standings(standings_df)
                logger.info("Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df))
        else:
            logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
        if standings_df is not None:
            sink.write(
                data=standings_df,
                partition={"day": date, "entity": "standings"},
                format="parquet",
                filename="standings.parquet",
            )
            counts["standings"] = len(standings_df)

        # Injuries — date-specific, always fetched fresh
        try:
            injuries = await adapter.get_injuries(date)
            if injuries:
                df = pd.DataFrame(injuries)
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "injuries"},
                    format="parquet",
                    filename="injuries.parquet",
                )
                counts["injuries"] = len(df)
                logger.info("Sports reference: %d injuries written", len(df))
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_injuries_fetch",
                shard=date,
            )

    # Per-fixture enrichment: stats, events, lineups, player stats.
    # Only for completed fixtures (status in FT/AET/PEN — stats unavailable for future/live).
    # Use fixture_ids_override when available (from URDI fetch) to avoid redundant
    # 33-league re-fetch (saves 33 API calls per date).
    fixture_ids: list[int] = []
    if fixture_ids_override is not None:
        fixture_ids = fixture_ids_override
        logger.info("Sports reference: %d completed fixture IDs passed from URDI (0 extra API calls)", len(fixture_ids))
    else:
        # Fallback: fetch fixtures from API (33 calls for 33 leagues).
        # Only used when called from the zero-fixture early-return path
        # where URDI returned 0 instruments.
        completed_statuses = {"FT", "AET", "PEN"}
        fallback_league_ids: list[int] = []
        for league_def in get_prediction_leagues():
            if league_def.api_football_id is not None:
                fallback_league_ids.append(league_def.api_football_id)
        try:
            fixtures = await adapter.get_fixtures(date, league_ids=fallback_league_ids)
            for fx in fixtures:
                if fx.status in completed_statuses:
                    raw_id = fx.source_fixture_id or fx.fixture_id
                    with contextlib.suppress(ValueError, TypeError):
                        fixture_ids.append(int(raw_id))
            logger.info("Sports reference: %d completed fixtures found for enrichment (API fetch)", len(fixture_ids))
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_fixtures_fetch",
                shard=date,
            )

    if fixture_ids:
        _per_fixture_entities = [
            ("fixture_stats", adapter.get_fixture_statistics),
            ("fixture_events", adapter.get_fixture_events),
            ("fixture_lineups", adapter.get_fixture_lineups),
            ("player_stats", adapter.get_fixture_player_stats),
        ]

        # Concurrent per-fixture fetching with rate-limit semaphore.
        # API Football Ultra plan allows ~10 req/sec. We use a semaphore to cap
        # concurrent requests and a small delay between releases to stay safe.
        concurrency = 10
        sem = asyncio.Semaphore(concurrency)
        entity_rows: dict[str, list[dict[str, object]]] = {name: [] for name, _ in _per_fixture_entities}

        async def _fetch_one(entity_name: str, fetch_fn: object, fid: int) -> None:
            async with sem:
                try:
                    rows = await fetch_fn(fid)  # type: ignore[operator]
                    for row in rows:
                        row["fixture_id"] = fid
                        entity_rows[entity_name].append(row)
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation=f"sports_reference_{entity_name}_fetch",
                        shard=str(fid),
                    )
                await asyncio.sleep(0.12)  # ~8 req/sec effective throughput

        # Build all tasks: 4 entities x N fixtures
        tasks: list[asyncio.Task[None]] = []
        for entity_name, fetch_fn in _per_fixture_entities:
            for fid in fixture_ids:
                tasks.append(asyncio.ensure_future(_fetch_one(entity_name, fetch_fn, fid)))

        logger.info(
            "Per-fixture enrichment: %d fixtures x %d entities = %d calls (concurrency=%d)",
            len(fixture_ids),
            len(_per_fixture_entities),
            len(tasks),
            concurrency,
        )
        await asyncio.gather(*tasks)

        for entity_name, _ in _per_fixture_entities:
            all_rows = entity_rows[entity_name]
            if all_rows:
                # Per-fixture API responses have mixed-type columns (e.g. fixture_stats
                # "statistics" column contains "33%" strings alongside ints). Convert
                # all values to strings before DataFrame creation to prevent both
                # Pandas type inference errors and Parquet serialization failures.
                sanitised_rows = [{k: str(v) if v is not None else None for k, v in row.items()} for row in all_rows]
                df = pd.DataFrame(sanitised_rows)
                sink.write(
                    data=df,
                    partition={"day": date, "entity": entity_name},
                    format="parquet",
                    filename=f"{entity_name}.parquet",
                )
                counts[entity_name] = len(df)
                logger.info("Sports reference: %d %s rows written", len(df), entity_name)

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
    except (FileNotFoundError, OSError) as exc:
        logger.debug("Fixture mapping: could not read fixtures for %s: %s", date, exc)
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="fixture_mapping_write",
        )


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


def _get_instruments_bucket(category: str | None = None) -> str:
    """Resolve the instruments write bucket for the given category.

    Prod:  instruments-store-{category.lower()}-{project}
    Test:  instruments-store-{category.lower()}-{project}-test

    Test buckets follow the same naming as prod with -test appended after
    the project ID. IS_TEST_RUN=true writes to the test variant so prod
    data is never touched during local dev / E2E runs.
    """
    cfg = get_config()
    project = cfg.gcp_project_id or "test-project"

    try:
        prod_bucket = get_bucket_name("instruments", category)
    except (ImportError, AttributeError):
        cat_lower = category.lower() if category else None
        prefix = cfg.instruments_bucket_prefix
        prod_bucket = f"{prefix}-{cat_lower}-{project}" if cat_lower else f"{prefix}-{project}"

    return f"{prod_bucket}-test" if cfg.is_test_run else prod_bucket


def _write_catalogue_record(bucket: str, path: str, date: str, record_count: int) -> None:
    """Update the consolidated availability index in the instruments bucket.

    Merges this venue/date record into:
      gs://{bucket}/_index/availability_index.parquet

    Downstream services call read_availability_index(bucket) to check completeness
    without listing thousands of GCS blobs.
    """
    try:
        venue_match = re.search(r"venue=([^/]+)", path)
        venue_str = venue_match.group(1) if venue_match else ""
        date_match = re.search(r"day=(\d{4}-\d{2}-\d{2})", path)
        date_str = date_match.group(1) if date_match else date
        parsed = date_type.fromisoformat(date_str)
        writer = ManifestWriter(
            service_name="instruments-service",
            catalogue_bucket=bucket,
        )
        writer.add(processing_date=parsed, row_count=record_count, venue=venue_str)
        writer.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="manifest_writer",
            shard=path,
        )
