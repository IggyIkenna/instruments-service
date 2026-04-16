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
    VenueMapping,
    get_prediction_leagues,
)
from unified_api_contracts.internal import InstrumentRecord, validate_instrument_records
from unified_api_contracts.registry import get_supported_chains_for_protocol
from unified_api_contracts.sports import (
    FOOTYSTATS_HISTORICAL_SEASON_IDS,
    FOOTYSTATS_SEASON_IDS,
    SOCCER_FOOTBALL_INFO_IDS,
    get_all_prediction_league_ids,
    get_entity_league_coverage,
    get_leagues_needing_refresh,
    get_provider_league_id,
    is_any_league_refresh_date,
)
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

from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets
from instruments_service.engine.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.reference_data.adapters.defi._solana_utils import SolanaCacheSession, fill_solana_cache
from instruments_service.reference_data.adapters.sports import create_sports_reference_adapter
from instruments_service.reference_data.adapters.sports.adapters.api_football_reference import (
    _last_completed_fixture_ids as _urdi_completed_fixture_ids,
)
from instruments_service.reference_data.adapters.tradfi.databento import is_non_trading_day
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
    # DEX forks — each has own subgraph IDs in UAC, reuse UniV3 adapter
    "pancakeswap_v3": "PANCAKESWAPV3",
    "sushiswap_v3": "SUSHISWAPV3",
    "aerodrome_v3": "AERODROMEV3",
    "camelot_v3": "CAMELOTV3",
    "velodrome_v2": "VELODROMEV2",
    "trader_joe_v2": "TRADERJOEV2",
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
    """Return True if the venue was launched on or before this date."""
    launch_date = _VENUE_LAUNCH_DATES.get(venue)
    if launch_date is None:
        return True  # Unknown venue — assume always available
    return date >= launch_date


def earliest_venue_date(venues: list[str]) -> str | None:
    """Return the earliest launch date across the given venues, or None if unknown."""
    dates = [_VENUE_LAUNCH_DATES[v] for v in venues if v in _VENUE_LAUNCH_DATES]
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
    categories: list[str],
    redo_all: bool = False,
    api_keys: dict[str, str] | None = None,
    venue_override: list[str] | None = None,
    mode: str = "batch",
    sports_entity_filter: str | None = None,
    sports_provider: str | None = None,
    league_filter: list[str] | None = None,
    season_override: int | None = None,
) -> dict[str, int]:
    """Process instruments for a single date and set of market categories.

    Args:
        sports_provider: When set, only run this data provider (e.g. OPEN_METEO,
            API_FOOTBALL, TRANSFERMARKT). Maps to venue filter + entity scope.
        league_filter: When set, only process these canonical league IDs
            (e.g. ["EPL", "BUNDESLIGA"]). Default None = all prediction leagues.

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

    # Track which sports entities are missing (set in skip-if-exists check).
    # Empty = fetch everything; non-empty = only fetch these specific entities.
    _sports_missing_entities: list[str] = []

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

        # OPEN_METEO short-circuit: skip ALL orchestrator logic (URDI, fixtures,
        # enrichment, etc.) and go straight to weather fetch. Weather only needs
        # venue coordinates (from GCS) and Open-Meteo API — no API keys, no URDI.
        if sports_provider == "OPEN_METEO":
            logger.info("OPEN_METEO short-circuit: skipping all orchestrator logic for date=%s", date)
            primary_category = categories[0] if categories else "SPORTS"
            bucket = _get_instruments_bucket(primary_category)
            if not bucket:
                logger.error("No bucket resolved for category=%s — cannot fetch weather", primary_category)
                return {}
            logger.info("Calling _fetch_weather_data for date=%s bucket=%s", date, bucket)
            weather_counts = await _fetch_weather_data(date=date, bucket=bucket)
            logger.info("Weather DONE for date=%s: %s", date, weather_counts)
            return weather_counts

    if not active_venues:
        logger.info("No active venues for date=%s categories=%s", date, categories)
        return {}

    # Sports entity lists — used by freshness check AND later fast-path logic,
    # so they must be defined unconditionally (not inside redo_all gate).
    is_sports_run = any(c.upper() in ("SPORTS", "ALL") for c in categories)
    _sports_core_entities = [
        "LEAGUES",
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
        primary_category = categories[0] if categories else None
        bucket = _get_instruments_bucket(primary_category)

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
            _date_fixture_leagues: set[str] = set()
            _index_df = read_availability_index(bucket)
            if not _index_df.empty and "league_id" in _index_df.columns:
                _fix_mask = (_index_df["date"] == date) & (_index_df["data_type"] == "FIXTURES")
                _lid_series = _index_df.loc[_fix_mask, "league_id"].dropna()
                _date_fixture_leagues = {str(lid).upper() for lid in _lid_series.unique() if str(lid).strip()}

            for entity, venue in _enrichment_entity_venues:
                if venue not in _active_venues_set_freshness:
                    continue
                # Check league coverage — skip entity if its covered leagues
                # have no fixtures on this date.
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
                primary_category = categories[0] if categories else None
                bucket = _get_instruments_bucket(primary_category)
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
                        sports_manifest.add(
                            processing_date=date_type.fromisoformat(date),
                            row_count=row_count,
                            data_type=entity_name.upper(),
                        )
                # Write blank entries for per-fixture entities that had 0 fixtures
                if not gcs_fixture_ids:
                    for pf_entity in _sports_per_fixture_entities:
                        entity_short = pf_entity.replace("API_FOOTBALL_", "").lower()
                        if entity_short not in sports_ref_counts:
                            sports_manifest.add(
                                processing_date=date_type.fromisoformat(date),
                                row_count=0,
                                data_type=pf_entity.replace("API_FOOTBALL_", "").upper(),
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
        details={"date": date, "categories": categories, "venue_count": len(active_venues)},
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
        primary_category = categories[0] if categories else None
        _pf_bucket = _get_instruments_bucket(primary_category)
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
        )
        pf_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=_pf_bucket)
        for entity_name, row_count in pf_counts.items():
            pf_manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=row_count,
                data_type=entity_name.upper(),
            )
        pf_manifest.write()
        return pf_counts

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
            # Write one empty marker per prediction league so downstream
            # consumers see each league as "processed with 0 fixtures".
            _empty_league_ids = league_filter if league_filter else get_all_prediction_league_ids()
            _empty_manifest = ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            for _league_id in _empty_league_ids:
                sink.write(
                    data=empty_df,
                    partition={"day": date, "venue": "API_FOOTBALL_FIXTURES", "league": _league_id},
                    format="parquet",
                    filename="instruments.parquet",
                )
                _empty_manifest.add(
                    processing_date=date_type.fromisoformat(date),
                    row_count=0,
                    data_type="FIXTURES",
                    league_id=_league_id,
                )
            _empty_manifest.write()
            logger.info(
                "SPORTS: No fixtures for date=%s — wrote empty markers for %d leagues",
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
                                sports_manifest.add(
                                    processing_date=date_type.fromisoformat(date),
                                    row_count=row_count,
                                    data_type=entity_name.upper(),
                                )
                        # Write blank entries for ALL per-fixture entities on zero-fixture dates
                        # so manifest marks them as "done" and won't re-fetch.
                        for pf_entity in _sports_per_fixture_entities:
                            entity_short = pf_entity.lower()
                            if entity_short not in sports_ref_counts:
                                dt_name = pf_entity
                                sports_manifest.add(
                                    processing_date=date_type.fromisoformat(date),
                                    row_count=0,
                                    data_type=dt_name,
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
                    for _enr_entity in _enrichment_zero_entities:
                        _enr_manifest.add(
                            processing_date=date_type.fromisoformat(date),
                            row_count=0,
                            data_type=_enr_entity,
                        )
                    _enr_manifest.write()
                    logger.info(
                        "Zero-fixture fast path: wrote 0 for %d fixture-dependent entities on date=%s",
                        len(_enrichment_zero_entities),
                        date,
                    )

            # Fixture-independent reference data: fetch even on zero-fixture dates,
            # but ONLY on trigger dates (season start, transfer window open/close).
            # This avoids re-fetching identical squad data every day.
            counts: dict[str, int] = {}
            _active_venues_set = set(active_venues)
            _ef = sports_entity_filter
            _entity_wanted_zf = lambda ent: _ef is None or _ef == ent  # noqa: E731

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

            log_event("PROCESSING_COMPLETED", details={"date": date, "categories": categories, "fixtures": 0})
            return counts
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

        # TradFi non-trading day: zero instruments on weekends/holidays is expected.
        # Write 0-count manifest entries per venue so the manifest marks the day as
        # processed and won't re-fetch without --force. This prevents permanent gaps
        # in instrument data for every weekend and exchange holiday.
        tradfi_active = [v for v in active_venues if v in _TRADFI_VENUES]
        if tradfi_active:
            target_dt = date_type.fromisoformat(date)
            non_trading_venues = [v for v in tradfi_active if is_non_trading_day(v, target_dt)]
            if non_trading_venues and len(non_trading_venues) == len(tradfi_active):
                primary_category = categories[0] if categories else None
                bucket = _get_instruments_bucket(primary_category)
                manifest = ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                for venue in non_trading_venues:
                    manifest.add(
                        processing_date=target_dt,
                        row_count=0,
                        venue=venue,
                    )
                manifest.write()
                logger.info(
                    "TRADFI non-trading day: date=%s venues=%s — wrote 0-count manifest entries",
                    date,
                    sorted(non_trading_venues),
                )
                log_event(
                    "PROCESSING_COMPLETED",
                    details={"date": date, "categories": categories, "non_trading_venues": sorted(non_trading_venues)},
                )
                return dict.fromkeys(non_trading_venues, 0)

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
                for _lid, _league_df in _sports_df.groupby("_league_id"):
                    _league_id_str = str(_lid)
                    _league_df_clean = _league_df.drop(columns=["_league_id"])
                    sink.write(
                        data=_league_df_clean,
                        partition={"day": date, "venue": venue_str, "league": _league_id_str},
                        format="parquet",
                        filename="instruments.parquet",
                    )
                    manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_league_df_clean),
                        data_type="FIXTURES",
                        league_id=_league_id_str,
                    )
                    counts[f"FIXTURES/{_league_id_str}"] = len(_league_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _league_df_clean,
                            filename_prefix=f"instruments_API_FOOTBALL_{_league_id_str}_{date}",
                        )

            elif venue_str == "POLYMARKET" and "base_asset" in venue_df.columns:
                # PREDICTION: split by market (BTC, ETH, SPX, FOOTBALL, etc.)
                # Each market gets its own partition and manifest entry.
                _pred_df = venue_df.copy()
                _pred_df["_market"] = _pred_df["base_asset"].apply(_extract_prediction_shard)
                # Strip venue prefix: "POLYMARKET:BTC" → "BTC"
                _pred_df["_market"] = _pred_df["_market"].str.replace("POLYMARKET:", "", regex=False)
                for _mkt, _mkt_df in _pred_df.groupby("_market"):
                    _mkt_str = str(_mkt)
                    _mkt_df_clean = _mkt_df.drop(columns=["_market"])
                    sink.write(
                        data=_mkt_df_clean,
                        partition={"day": date, "venue": venue_str, "market": _mkt_str},
                        format="parquet",
                        filename="instruments.parquet",
                    )
                    manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_mkt_df_clean),
                        venue="POLYMARKET",
                        data_type=_mkt_str,
                    )
                    counts[f"POLYMARKET/{_mkt_str}"] = len(_mkt_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _mkt_df_clean,
                            filename_prefix=f"instruments_POLYMARKET_{_mkt_str}_{date}",
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
            for venue in sorted(non_trading):
                manifest.add(
                    processing_date=target_dt,
                    row_count=0,
                    venue=venue,
                )
                counts[venue] = 0
            logger.info(
                "TRADFI non-trading day manifest: date=%s venues=%s — wrote 0-count entries",
                date,
                sorted(non_trading),
            )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    manifest.close()

    # 7. SPORTS enrichment: fetch and write reference data (teams, leagues, etc.)
    # alongside fixtures. These are slow-moving entities that don't change per-date
    # but are re-fetched to capture transfers, promotions, new seasons.
    is_sports = any(c.upper() in ("SPORTS", "ALL") for c in categories)
    # OPEN_METEO doesn't need API keys — allow sports enrichment even with empty api_keys
    _needs_api_keys = sports_provider not in ("OPEN_METEO",) if sports_provider else True
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
            )
            for k, v in sports_ref_counts.items():
                counts[k] = counts.get(k, 0) + v

            # Write manifest for sports reference entities that did NOT write
            # their own manifest entries inside _fetch_sports_reference_data
            # (injuries and per-fixture entities write per-league entries directly).
            _self_manifested = {"injuries", "fixture_stats", "fixture_events", "fixture_lineups", "player_stats"}
            for entity_name, row_count in sports_ref_counts.items():
                if entity_name not in _self_manifested:
                    sports_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=row_count,
                        data_type=entity_name.upper(),
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

        if "UNDERSTAT" in _active_venues_set and _entity_wanted("XG"):
            try:
                xg_counts = await _fetch_understat_xg(date=date, bucket=bucket)
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
    if is_sports_run:
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


def _extract_prediction_shard(base_asset: str) -> str:
    """Extract the underlying shard name from a PREDICTION instrument's base_asset.

    No allowlist — every UP_DOWN market's underlying IS the shard.  New
    underlyings (CRUDE_OIL, GOLD, etc.) appear automatically without code
    changes.

    Patterns:
      PREDICTION:POLYMARKET:UP_DOWN:BTC:1D:2026-04-05 → POLYMARKET:BTC
      PREDICTION:POLYMARKET:UP_DOWN:CRUDE_OIL:1D:...  → POLYMARKET:CRUDE_OIL
      FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:...          → POLYMARKET:FOOTBALL
      anything without UP_DOWN pattern                 → POLYMARKET:OTHER
    """
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "POLYMARKET:FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN":
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
            # v4: Sports reference entities write data_type (not venue).
            #     API_FOOTBALL → data_type=FIXTURES, venue=""
            #     API_FOOTBALL_INJURIES → data_type=INJURIES, venue=""
            #     Other categories keep venue as-is.
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
            if manifest is not None:
                if is_sports_ref:
                    manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(df),
                        data_type=manifest_data_type,
                    )
                else:
                    manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(df),
                        venue=manifest_venue,
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
        local = f"/tmp/_fixture_ids_{date}.parquet"
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
        local = f"/tmp/_fixture_league_map_{date}.parquet"
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

    def _should_fetch(entity_short: str) -> bool:
        """Check if this entity should be fetched (not in _fetch_set or _fetch_set is None)."""
        if _fetch_set is None:
            return True
        return entity_short in _fetch_set

    if enrichment_only:
        logger.info("Enrichment-only mode: skipping leagues/teams/standings/injuries for date=%s", date)

    # Leagues/teams/standings are slow-moving (same within a season). Cache DataFrames
    # across dates within the same batch run to save ~67 API calls per date.
    if not enrichment_only and _should_fetch("leagues"):
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
                            row = t.model_dump()
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
            # Write per-league partitioned team files
            if "league_id" in teams_df.columns:
                for _t_lid, _t_league_df in teams_df.groupby("league_id"):
                    _t_lid_str = str(_t_lid)
                    sink.write(
                        data=_t_league_df,
                        partition={"day": date, "entity": "teams", "league": _t_lid_str},
                        format="parquet",
                        filename="teams.parquet",
                    )
            else:
                sink.write(
                    data=teams_df,
                    partition={"day": date, "entity": "teams"},
                    format="parquet",
                    filename="teams.parquet",
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
            if all_standings:
                standings_df = pd.DataFrame(all_standings)
                _set_cached_standings(standings_df)
                logger.info("Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df))
        else:
            logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
        if standings_df is not None:
            # Write per-league partitioned standings files
            if "league_id" in standings_df.columns:
                for _s_lid, _s_league_df in standings_df.groupby("league_id"):
                    _s_lid_str = str(_s_lid)
                    sink.write(
                        data=_s_league_df,
                        partition={"day": date, "entity": "standings", "league": _s_lid_str},
                        format="parquet",
                        filename="standings.parquet",
                    )
            else:
                sink.write(
                    data=standings_df,
                    partition={"day": date, "entity": "standings"},
                    format="parquet",
                    filename="standings.parquet",
                )
            counts["standings"] = len(standings_df)

    # Injuries — date-specific, always fetched fresh.
    # IMPORTANT: outside the leagues/teams/standings block so it runs even when
    # only injuries is requested (entities_to_fetch=["API_FOOTBALL_INJURIES"]).
    if not enrichment_only and _should_fetch("injuries"):
        try:
            injuries = await adapter.get_injuries(date)
            if injuries:
                df = pd.DataFrame([inj.model_dump() for inj in injuries])
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

                    for _inj_lid, _inj_league_df in _with_league.groupby(_inj_league_col):
                        _inj_lid_str = str(_inj_lid)
                        _inj_clean = _inj_league_df.drop(columns=["_inj_league"], errors="ignore")
                        sink.write(
                            data=_inj_clean,
                            partition={"day": date, "entity": "injuries", "league": _inj_lid_str},
                            format="parquet",
                            filename="injuries.parquet",
                        )
                        if manifest is not None:
                            manifest.add(
                                processing_date=date_type.fromisoformat(date),
                                row_count=len(_inj_clean),
                                data_type="INJURIES",
                                league_id=_inj_lid_str,
                            )

                    if not _without_league.empty:
                        _inj_unmapped = _without_league.drop(columns=["_inj_league"], errors="ignore")
                        sink.write(
                            data=_inj_unmapped,
                            partition={"day": date, "entity": "injuries"},
                            format="parquet",
                            filename="injuries.parquet",
                        )
                        if manifest is not None:
                            manifest.add(
                                processing_date=date_type.fromisoformat(date),
                                row_count=len(_inj_unmapped),
                                data_type="INJURIES",
                            )
                else:
                    # No league info — write single file
                    sink.write(
                        data=df,
                        partition={"day": date, "entity": "injuries"},
                        format="parquet",
                        filename="injuries.parquet",
                    )
                    if manifest is not None:
                        manifest.add(
                            processing_date=date_type.fromisoformat(date),
                            row_count=len(df),
                            data_type="INJURIES",
                        )

                logger.info("Sports reference: %d injuries written", len(df))
            else:
                # Write empty parquet with correct schema so the date counts as
                # "processed with 0 injuries" rather than "not processed".
                _empty_injuries_df = pd.DataFrame(
                    columns=["fixture_id", "team_id", "player_id", "player_name", "reason", "severity"],
                )
                sink.write(
                    data=_empty_injuries_df,
                    partition={"day": date, "entity": "injuries"},
                    format="parquet",
                    filename="injuries.parquet",
                )
                counts["injuries"] = 0
                logger.info("Sports reference: 0 injuries written (empty parquet)")
                if manifest is not None:
                    manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=0,
                        data_type="INJURIES",
                    )
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
                    _ref_sink.write(
                        data=_old_df,
                        partition={"day": date, "entity": "fixtures"},
                        format="parquet",
                        filename="fixtures.parquet",
                    )
                    logger.info(
                        "Canonical fixtures copied from old path to entity=fixtures/ (%d rows)",
                        len(_old_df),
                    )
                else:
                    # No old path — fetch from API Football (costs 33 API calls)
                    _adapter = create_sports_reference_adapter("api_football", api_key=api_key)
                    _fx_list = await _adapter.get_fixtures(date)
                    if _fx_list:
                        _fx_dicts = [fx.model_dump() if hasattr(fx, "model_dump") else fx for fx in _fx_list]
                        _fx_df = pd.DataFrame(_fx_dicts)
                        _ref_sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
                        _ref_sink.write(
                            data=_fx_df,
                            partition={"day": date, "entity": "fixtures"},
                            format="parquet",
                            filename="fixtures.parquet",
                        )
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
        for league_def in get_prediction_leagues():
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
            logger.info("Sports reference: %d completed fixtures found for enrichment (API fetch)", len(fixture_ids))

            # Write canonical fixtures to sports_reference/by_date/entity=fixtures/
            # so features-sports-service and trigger scheduler can read them.
            if fixtures:
                try:
                    fixture_dicts = [fx.model_dump() if hasattr(fx, "model_dump") else fx for fx in fixtures]
                    fixture_df = pd.DataFrame(fixture_dicts)
                    _fix_ref_sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
                    _fix_ref_sink.write(
                        data=fixture_df,
                        partition={"day": date, "entity": "fixtures"},
                        format="parquet",
                        filename="fixtures.parquet",
                    )
                    # Per-league partitioned write
                    if "league_id" in fixture_df.columns:
                        for _lid, _ldf in fixture_df.groupby("league_id"):
                            _fix_ref_sink.write(
                                data=_ldf,
                                partition={"day": date, "entity": "fixtures", "league": str(_lid)},
                                format="parquet",
                                filename="fixtures.parquet",
                            )
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
                # Throttle handled by adapter's _get_with_retry + rate limit headers

        # Build all tasks: N entities x M fixtures (only missing entities)
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

                    for _pf_lid, _pf_league_df in _with_league.groupby("_league_id"):
                        _pf_lid_str = str(_pf_lid)
                        _pf_clean = _pf_league_df.drop(columns=["_league_id"])
                        sink.write(
                            data=_pf_clean,
                            partition={"day": date, "entity": entity_name, "league": _pf_lid_str},
                            format="parquet",
                            filename=f"{entity_name}.parquet",
                        )
                        if manifest is not None:
                            manifest.add(
                                processing_date=date_type.fromisoformat(date),
                                row_count=len(_pf_clean),
                                data_type=entity_name.upper(),
                                league_id=_pf_lid_str,
                            )

                    # Write unmapped rows (if any) to a catch-all partition
                    if not _without_league.empty:
                        _unmapped_clean = _without_league.drop(columns=["_league_id"])
                        sink.write(
                            data=_unmapped_clean,
                            partition={"day": date, "entity": entity_name},
                            format="parquet",
                            filename=f"{entity_name}.parquet",
                        )
                        logger.warning(
                            "Sports reference: %d %s rows could not be mapped to a league",
                            len(_unmapped_clean),
                            entity_name,
                        )
                        if manifest is not None:
                            manifest.add(
                                processing_date=date_type.fromisoformat(date),
                                row_count=len(_unmapped_clean),
                                data_type=entity_name.upper(),
                            )
                else:
                    # No league mapping available — write single file (legacy fallback)
                    sink.write(
                        data=df,
                        partition={"day": date, "entity": entity_name},
                        format="parquet",
                        filename=f"{entity_name}.parquet",
                    )
                    if manifest is not None:
                        manifest.add(
                            processing_date=date_type.fromisoformat(date),
                            row_count=len(df),
                            data_type=entity_name.upper(),
                        )

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


async def _fetch_footystats_predictions(
    date: str,
    api_key: str,
    bucket: str,
) -> dict[str, int]:
    """Fetch FootyStats predictive data and write to GCS as a separate entity.

    Predictive fields (btts_potential, o25_potential, xg_prematch, etc.) are
    FootyStats-proprietary pre-match signals. Written separately from factual
    fixture data so FSS can consume them as third-party signal input.

    GCS path: sports_reference/by_date/day={date}/entity=footystats_predictions/
              footystats_predictions.parquet
    """
    adapter = create_sports_reference_adapter("footystats", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_footystats_team

        predictions = await adapter.get_fixture_predictions(date)  # type: ignore[attr-defined]
        if predictions:
            df = pd.DataFrame([p.model_dump() for p in predictions])
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
            pred_manifest = ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            if "canonical_fixture_id" in df.columns:
                df["_pred_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_pred_league"].notna() & (df["_pred_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _pred_lid, _pred_league_df in _with_league.groupby("_pred_league"):
                    _pred_lid_str = str(_pred_lid)
                    _pred_clean = _pred_league_df.drop(columns=["_pred_league"])
                    sink.write(
                        data=_pred_clean,
                        partition={"day": date, "entity": "footystats_predictions", "league": _pred_lid_str},
                        format="parquet",
                        filename="footystats_predictions.parquet",
                    )
                    pred_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_pred_clean),
                        data_type="PREDICTIONS",
                        league_id=_pred_lid_str,
                    )

                if not _without_league.empty:
                    _pred_unmapped = _without_league.drop(columns=["_pred_league"])
                    sink.write(
                        data=_pred_unmapped,
                        partition={"day": date, "entity": "footystats_predictions"},
                        format="parquet",
                        filename="footystats_predictions.parquet",
                    )
                    pred_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_pred_unmapped),
                        data_type="PREDICTIONS",
                    )
            else:
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "footystats_predictions"},
                    format="parquet",
                    filename="footystats_predictions.parquet",
                )
                pred_manifest.add(
                    processing_date=date_type.fromisoformat(date),
                    row_count=len(df),
                    data_type="PREDICTIONS",
                )
            pred_manifest.write()

            logger.info(
                "FootyStats predictions: %d rows written for date=%s",
                len(df),
                date,
            )
        else:
            logger.info("FootyStats predictions: no predictive data for date=%s", date)
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_predictions_fetch",
            shard=date,
        )

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

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_footystats_team

        # FootyStats league IDs are seasonal — use UAC SSOT
        league_ids = list(FOOTYSTATS_SEASON_IDS.values())
        fixtures = await adapter.get_fixtures(date, league_ids=league_ids)
        if fixtures:
            rows = [fx.model_dump() for fx in fixtures]
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
                # League comes from flattened league object (league_league_id)
                # or reverse-map from fixture_id's competition_id prefix.
                home_name = flat.get("home_team_name") or flat.get("home_team") or ""
                away_name = flat.get("away_team_name") or flat.get("away_team") or ""
                league = flat.get("league_league_id") or flat.get("league_name") or ""
                if not league:
                    # Try reverse-mapping from fixture_id prefix (competition_id)
                    _ft_rev = {str(k): v for k, v in FOOTYSTATS_HISTORICAL_SEASON_IDS.items()}
                    fid = flat.get("fixture_id") or flat.get("source_fixture_id") or ""
                    if ":" in fid:
                        league = _ft_rev.get(fid.split(":")[0], "")
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
            counts["footystats_matches"] = len(df)

            # Write per-league partitioned files using canonical_fixture_id.
            _ft_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
            if "canonical_fixture_id" in df.columns:
                df["_ft_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_ft_league"].notna() & (df["_ft_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _ft_lid, _ft_league_df in _with_league.groupby("_ft_league"):
                    _ft_lid_str = str(_ft_lid)
                    _ft_clean = _ft_league_df.drop(columns=["_ft_league"])
                    sink.write(
                        data=_ft_clean,
                        partition={"day": date, "entity": "footystats_matches", "league": _ft_lid_str},
                        format="parquet",
                        filename="footystats_matches.parquet",
                    )
                    _ft_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_ft_clean),
                        data_type="MATCHES",
                        league_id=_ft_lid_str,
                    )

                if not _without_league.empty:
                    _ft_unmapped = _without_league.drop(columns=["_ft_league"])
                    sink.write(
                        data=_ft_unmapped,
                        partition={"day": date, "entity": "footystats_matches"},
                        format="parquet",
                        filename="footystats_matches.parquet",
                    )
                    _ft_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_ft_unmapped),
                        data_type="MATCHES",
                    )
            else:
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "footystats_matches"},
                    format="parquet",
                    filename="footystats_matches.parquet",
                )
                _ft_manifest.add(
                    processing_date=date_type.fromisoformat(date),
                    row_count=len(df),
                    data_type="MATCHES",
                )
            _ft_manifest.write()
            logger.info("FootyStats matches: %d rows written for date=%s", len(df), date)
        else:
            logger.info("FootyStats matches: no fixtures for date=%s", date)
            # Write 0-count manifest so date is marked as processed
            manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
            manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=0,
                data_type="MATCHES",
            )
            manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_matches_fetch",
            shard=date,
        )

    return counts


async def _fetch_understat_xg(
    date: str,
    bucket: str,
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

    try:
        from unified_api_contracts.canonical.domain.sports.canonical_ids import build_fixture_id
        from unified_api_contracts.sports import resolve_understat_team

        fixtures = await adapter.get_fixtures(date)
        if fixtures:
            rows = [fx.model_dump() for fx in fixtures]
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
            counts["understat_xg"] = len(df)

            xg_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
            # Write per-league partitioned files if league column exists
            if "league" in df.columns:
                _has_league = df["league"].notna() & (df["league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _xg_lid, _xg_league_df in _with_league.groupby("league"):
                    _xg_lid_str = str(_xg_lid)
                    sink.write(
                        data=_xg_league_df,
                        partition={"day": date, "entity": "understat_xg", "league": _xg_lid_str},
                        format="parquet",
                        filename="understat_xg.parquet",
                    )
                    xg_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_xg_league_df),
                        data_type="XG",
                        league_id=_xg_lid_str,
                    )

                if not _without_league.empty:
                    sink.write(
                        data=_without_league,
                        partition={"day": date, "entity": "understat_xg"},
                        format="parquet",
                        filename="understat_xg.parquet",
                    )
                    xg_manifest.add(
                        processing_date=date_type.fromisoformat(date),
                        row_count=len(_without_league),
                        data_type="XG",
                    )
            else:
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "understat_xg"},
                    format="parquet",
                    filename="understat_xg.parquet",
                )
                xg_manifest.add(
                    processing_date=date_type.fromisoformat(date),
                    row_count=len(df),
                    data_type="XG",
                )
            xg_manifest.write()
            logger.info("Understat xG: %d rows written for date=%s", len(df), date)
        else:
            logger.info("Understat xG: no fixtures for date=%s", date)
            xg_manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
            xg_manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=0,
                data_type="XG",
            )
            xg_manifest.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="understat_xg_fetch",
            shard=date,
        )

    return counts


async def _fetch_transfermarkt_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
    season: int | None = None,
) -> dict[str, int]:
    """Fetch Transfermarkt leagues and teams (with player values) to GCS.

    entity_filter: when set to "TRANSFERMARKT_LEAGUES" or "PLAYER_VALUES",
        only that entity is fetched and written (entity-scoped VM mode).
    season: override season year for historical backfill (e.g. 2019).
        When None, the adapter defaults to the current year.

    Transfermarkt provides squad composition, player market values, and
    transfer history. Data is slow-moving (changes at trigger dates:
    season start, transfer window open/close) and fetched only then.

    GCS paths:
        sports_reference/by_date/day={date}/entity=transfermarkt_leagues/
        sports_reference/by_date/day={date}/entity=transfermarkt_teams/
    """
    adapter = create_sports_reference_adapter("transfermarkt", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    _want_leagues = entity_filter is None or entity_filter == "TRANSFERMARKT_LEAGUES"
    _want_teams = entity_filter is None or entity_filter == "PLAYER_VALUES"

    if _want_leagues:
        try:
            leagues = await adapter.get_leagues()
            if leagues:
                rows = [lg.model_dump() for lg in leagues]
                df = pd.DataFrame([{k: str(v) if v is not None else None for k, v in r.items()} for r in rows])
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "transfermarkt_leagues"},
                    format="parquet",
                    filename="transfermarkt_leagues.parquet",
                )
                counts["transfermarkt_leagues"] = len(df)
                logger.info("Transfermarkt leagues: %d rows written", len(df))
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="transfermarkt_leagues_fetch",
                shard=date,
            )

    if _want_teams:
        try:
            all_teams: list[dict[str, str | None]] = []
            for league_def in get_prediction_leagues():
                tm_code = get_provider_league_id(league_def.league_id, "transfermarkt")
                if tm_code is None:
                    continue
                try:
                    teams = await adapter.get_teams(tm_code, season=season)
                    for t in teams:
                        row = t.model_dump()
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
                except Exception as exc:
                    classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="transfermarkt_teams_fetch",
                        shard=str(league_def.league_id),
                    )
            if all_teams:
                df = pd.DataFrame(all_teams)
                # Add season column for provenance
                effective_season = season if season is not None else datetime.now(UTC).year
                df["season"] = effective_season
                # Write as player_values entity — partition by season when
                # doing historical backfill so seasons don't overwrite each other
                pv_partition: dict[str, str] = {"day": date, "entity": "player_values"}
                if season is not None:
                    pv_partition["season"] = str(season)
                sink.write(
                    data=df,
                    partition=pv_partition,
                    format="parquet",
                    filename="player_values.parquet",
                )
                counts["transfermarkt_teams"] = len(df)
                logger.info("Transfermarkt teams → player_values: %d rows written", len(df))
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="transfermarkt_teams_batch",
                shard=date,
            )

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if _want_leagues:
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=counts.get("transfermarkt_leagues", 0),
            data_type="TRANSFERMARKT_LEAGUES",
        )
    if _want_teams:
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=counts.get("transfermarkt_teams", 0),
            data_type="PLAYER_VALUES",
        )
    manifest.write()

    return counts


async def _fetch_sfi_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
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

    GCS paths:
        sports_reference/by_date/day={date}/entity=sfi_leagues/
        sports_reference/by_date/day={date}/entity=sfi_standings/
        sports_reference/by_date/day={date}/entity=progressive_stats/
    """
    adapter = create_sports_reference_adapter("soccer_football_info", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    _want_sfi_leagues = entity_filter is None or entity_filter == "SFI_LEAGUES"
    _want_sfi_standings = entity_filter is None or entity_filter == "SFI_STANDINGS"
    _want_sfi_progressive = entity_filter is None or entity_filter == "SFI_PROGRESSIVE_STATS"

    sfi_league_ids: list[str] = []
    try:
        leagues = await adapter.get_leagues()
        if leagues:
            if _want_sfi_leagues:
                rows = [lg.model_dump() for lg in leagues]
                df = pd.DataFrame([{k: str(v) if v is not None else None for k, v in r.items()} for r in rows])
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "sfi_leagues"},
                    format="parquet",
                    filename="sfi_leagues.parquet",
                )
                counts["sfi_leagues"] = len(df)
                logger.info("SFI leagues: %d rows written", len(df))
            sfi_league_ids = [lg.league_id for lg in leagues]
    except Exception as exc:
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
                sink.write(
                    data=df,
                    partition={"day": date, "entity": "sfi_standings"},
                    format="parquet",
                    filename="sfi_standings.parquet",
                )
                counts["sfi_standings"] = len(df)
                logger.info("SFI standings: %d rows written", len(df))
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sfi_standings_batch",
                shard=date,
            )

    # Progressive stats — per-match 30-second interval time-series.
    # Requires SFI match IDs for the date, then fetches progressive data
    # for each completed match. Written as entity=progressive_stats.
    if _want_sfi_progressive:
        try:
            sfi_match_ids = await adapter.get_match_ids_for_date(date)
            if sfi_match_ids:
                all_progressive: list[dict[str, str | int | float | None]] = []
                for mid in sfi_match_ids:
                    try:
                        stats = await adapter.get_progressive_stats(mid)
                        for entry in stats:
                            all_progressive.append(
                                {k: str(v) if v is not None else None for k, v in entry.model_dump().items()}
                            )
                    except Exception as exc:
                        classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sfi_progressive_stats_fetch",
                            shard=mid,
                        )
                if all_progressive:
                    df = pd.DataFrame(all_progressive)
                    sink.write(
                        data=df,
                        partition={"day": date, "entity": "progressive_stats"},
                        format="parquet",
                        filename="progressive_stats.parquet",
                    )
                    counts["progressive_stats"] = len(df)
                    logger.info("SFI progressive stats: %d rows written", len(df))
            else:
                logger.info("SFI progressive stats: no completed matches for date=%s", date)
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sfi_progressive_stats_batch",
                shard=date,
            )

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if _want_sfi_leagues:
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=counts.get("sfi_leagues", 0),
            data_type="SFI_LEAGUES",
        )
    if _want_sfi_standings:
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=counts.get("sfi_standings", 0),
            data_type="SFI_STANDINGS",
        )
    if _want_sfi_progressive:
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=counts.get("progressive_stats", 0),
            data_type="SFI_PROGRESSIVE_STATS",
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
        local = "/tmp/_weather_venues.parquet"
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
        local = f"/tmp/_weather_fixtures_{date}.parquet"
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
) -> dict[str, int]:
    """Fetch Open-Meteo weather data for fixture venues and write to GCS.

    Weather (temperature, wind, rain, humidity) affects match outcomes —
    particularly relevant for outdoor sports prediction models.

    Flow:
      1. Read the global venues.parquet for venue_id → (lat, lon) lookup.
      2. Read fixtures.parquet for the date to get venue_ids of fixtures.
      3. For each fixture venue with coordinates, call Open-Meteo API.
      4. Write results to sports_reference/by_date/day={date}/entity=weather/weather.parquet.

    Fixtures without a venue or venues without coordinates are skipped
    with a warning log (no raise — shard-level failure isolation).
    """
    import re

    from instruments_service.reference_data.adapters.sports.adapters.open_meteo import OpenMeteoAdapter

    adapter = OpenMeteoAdapter()
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # UAC venue coordinates: SCREAMING_SNAKE keys → (lat, lon)
    from unified_api_contracts.registry.sports_venue_coordinates import VENUE_COORDINATES

    # 1. Read fixtures for this date — get venue_name + kickoff hour
    fixtures_df = None
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

    if fixtures_df is None or fixtures_df.empty or "venue_name" not in fixtures_df.columns:
        logger.info("Weather: no fixture venue_name data for date=%s — skipping", date)
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
            logger.info("Weather: all venues already covered for date=%s — skipping", date)
        else:
            logger.info("Weather: no fixture venues with coordinates for date=%s — skipping", date)
        manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
        manifest.add(
            processing_date=date_type.fromisoformat(date),
            row_count=0,
            data_type="WEATHER",
        )
        manifest.write()
        return counts

    # 4. Fetch weather match window for each fixture venue.
    # Each venue gets a 3-hour window (KO, KO+1h, KO+2h) at each lead time.
    weather_rows: list[dict[str, object]] = []
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

    if weather_rows:
        new_df = pd.DataFrame(weather_rows)

        # Merge with existing weather data (append new venues to existing)
        if existing_venue_ids:
            try:
                weather_prefix = f"sports_reference/by_date/day={date}/entity=weather/"
                for wb in storage_client.list_blobs(bucket=bucket, prefix=weather_prefix, max_results=5):
                    if wb.name.endswith(".parquet"):
                        wdata = storage_client.download_bytes(bucket=bucket, blob_path=wb.name)
                        existing_df = pd.read_parquet(io.BytesIO(wdata))
                        new_df = pd.concat([existing_df, new_df], ignore_index=True)
                        logger.info(
                            "Weather: merged %d existing + %d new = %d total for date=%s",
                            len(existing_df),
                            len(weather_rows),
                            len(new_df),
                            date,
                        )
                        break
            except Exception:
                pass  # Write new data only if merge fails

        sink.write(
            data=new_df,
            partition={"day": date, "entity": "weather"},
            format="parquet",
            filename="weather.parquet",
        )
        counts["weather"] = len(new_df)
        logger.info("Weather: %d venue observations written for date=%s", len(new_df), date)

    # Manifest
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    manifest.add(
        processing_date=date_type.fromisoformat(date),
        row_count=counts.get("weather", 0),
        data_type="WEATHER",
    )
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
        writer.add(
            processing_date=parsed,
            row_count=record_count,
            venue=manifest_venue,
            chain=manifest_chain,
            data_type=manifest_data_type,
            league_id=manifest_league_id,
        )
        writer.write()
    except Exception as exc:
        classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="manifest_writer",
            shard=path,
        )
