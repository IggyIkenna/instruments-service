"""Adapter factory for unified-reference-data-interface."""

import contextlib
import logging
from datetime import date as date_type

from unified_api_contracts.registry import (
    CapabilityResolutionError,
    UnsupportedOperationError,
    bootstrap_capabilities,
    validate_operation,
)

from .adapters.aave_v3 import AaveV3ReferenceDataAdapter
from .adapters.api_football import ApiFootballReferenceDataAdapter
from .adapters.aster import AsterReferenceDataAdapter
from .adapters.balancer import BalancerReferenceDataAdapter
from .adapters.betfair import BetfairReferenceDataAdapter
from .adapters.binance import BinanceReferenceDataAdapter
from .adapters.bybit import BybitReferenceDataAdapter
from .adapters.ccxt_adapter import CCXTReferenceDataAdapter
from .adapters.coinbase import CoinbaseReferenceDataAdapter
from .adapters.compound_v3 import CompoundV3ReferenceDataAdapter
from .adapters.curve import CurveReferenceDataAdapter
from .adapters.databento import DatabentoReferenceDataAdapter
from .adapters.deribit import DeribitReferenceDataAdapter
from .adapters.drift import DriftReferenceDataAdapter
from .adapters.eigenlayer import EigenLayerReferenceDataAdapter
from .adapters.ethena import EthenaReferenceDataAdapter
from .adapters.etherfi import EtherFiReferenceDataAdapter
from .adapters.ethfi import EthFiGovernanceReferenceDataAdapter
from .adapters.fluid import FluidReferenceDataAdapter
from .adapters.hyperliquid import HyperliquidReferenceDataAdapter
from .adapters.ibkr import IBKRReferenceDataAdapter
from .adapters.jito import JitoReferenceDataAdapter
from .adapters.kalshi import KalshiReferenceDataAdapter
from .adapters.kamino import KaminoReferenceDataAdapter
from .adapters.lido import LidoReferenceDataAdapter
from .adapters.marinade import MarinadeReferenceDataAdapter
from .adapters.morpho import MorphoReferenceDataAdapter
from .adapters.okx import OKXReferenceDataAdapter
from .adapters.orca import OrcaReferenceDataAdapter
from .adapters.polygon import PolygonReferenceDataAdapter
from .adapters.polymarket import PolymarketReferenceDataAdapter
from .adapters.raydium import RaydiumReferenceDataAdapter
from .adapters.tardis import TardisReferenceDataAdapter
from .adapters.tradfi_live import TradFiLiveReferenceDataAdapter
from .adapters.uniswap_v2 import UniswapV2ReferenceDataAdapter
from .adapters.uniswap_v3 import UniswapV3ReferenceDataAdapter
from .adapters.uniswap_v4 import UniswapV4ReferenceDataAdapter
from .base_adapter import BaseReferenceDataAdapter

_logger = logging.getLogger(__name__)

# Bootstrap capability registry once at module load (idempotent)
with contextlib.suppress(Exception):
    bootstrap_capabilities()

# Maps UAC canonical venue names → URDI adapter factory keys.
# Instruments-service (and any other service) calls get_adapter_for_canonical_venue()
# instead of maintaining their own translation dict.
# Key: UAC canonical venue name (uppercase, hyphenated).
# Value: key into _ADAPTERS below.
CANONICAL_VENUE_TO_ADAPTER: dict[str, str] = {
    # CeFi — Tardis for batch (historical instrument universe),
    # CCXT for live (real-time active markets via public endpoints).
    # Mode-aware routing in get_adapter_for_canonical_venue() selects the adapter.
    "BINANCE-SPOT": "tardis",
    "BINANCE-FUTURES": "tardis",
    "BYBIT": "tardis",
    "BYBIT-SPOT": "tardis",
    "BYBIT-FUTURES": "tardis",
    "OKX": "tardis",
    "OKX-SPOT": "tardis",
    "OKX-SWAP": "tardis",
    "OKX-FUTURES": "tardis",
    "DERIBIT": "tardis",
    "COINBASE-SPOT": "tardis",
    "UPBIT": "tardis",
    # Non-Tardis CeFi
    "HYPERLIQUID": "hyperliquid",
    "ASTER": "aster",
    # TradFi
    "CME": "databento",
    "NASDAQ": "databento",
    "NYSE": "databento",
    "CBOE": "databento",
    "ICE": "databento",
    "FX": "databento",
    # Prediction markets
    "POLYMARKET": "polymarket",
    "KALSHI": "kalshi",
    # Data aggregators
    "POLYGON": "polygon",
    # DeFi — LST/Yield protocols (Ethereum-only, no subgraph multi-chain)
    "LIDO-ETHEREUM": "lido",
    "ETHERFI-ETHEREUM": "etherfi",
    "ETHENA-ETHEREUM": "ethena",
    # DeFi — Governance tokens (on-chain, Ethereum)
    "EIGENLAYER-ETHEREUM": "eigenlayer",
    "ETHERFI-GOV-ETHEREUM": "ethfi_governance",
    # Sports
    "BETFAIR": "betfair",
    "API_FOOTBALL": "api_football",
    # Solana DeFi (REST API, no subgraph)
    "DRIFT-SOLANA": "drift",
    "KAMINO-SOLANA": "kamino",
    "RAYDIUM-SOLANA": "raydium",
    "ORCA-SOLANA": "orca",
    "MARINADE-SOLANA": "marinade",
    "JITO-SOLANA": "jito",
    # Jupiter is execution-only (swap aggregator), not instrument discovery.
}

# Dynamically add multi-chain DeFi venues from SUBGRAPH_IDS (SSOT in UAC).
# This auto-generates entries like AAVEV3-ARBITRUM → aave_v3, MORPHO-BASE → morpho, etc.
_SUBGRAPH_VENUE_PREFIX_TO_ADAPTER: dict[str, str] = {
    "AAVEV3": "aave_v3",
    "COMPOUNDV3": "compound_v3",
    "MORPHO": "morpho",
    "FLUID": "fluid",
    "UNISWAPV2": "uniswap_v2",
    "UNISWAPV3": "uniswap_v3",
    "UNISWAPV4": "uniswap_v4",
    "BALANCER": "balancer",
    "CURVE": "curve",
}

from unified_api_contracts.registry.capability_declarations._defi import (  # noqa: E402, qg-inside-import
    get_supported_chains_for_protocol,
)

# Map adapter keys back to protocol slugs for subgraph lookup
_ADAPTER_TO_PROTOCOL: dict[str, str] = {
    "aave_v3": "aave_v3",
    "compound_v3": "compound_v3",
    "morpho": "morpho",
    "fluid": "fluid",
    "uniswap_v2": "uniswap_v2",
    "uniswap_v3": "uniswap_v3",
    "uniswap_v4": "uniswap_v4",
    "balancer": "balancer",
    "curve": "curve",
}

for _prefix, _adapter_key in _SUBGRAPH_VENUE_PREFIX_TO_ADAPTER.items():
    _protocol_slug = _ADAPTER_TO_PROTOCOL.get(_adapter_key, _adapter_key)
    for _chain in get_supported_chains_for_protocol(_protocol_slug):
        _venue = f"{_prefix}-{_chain}"
        if _venue not in CANONICAL_VENUE_TO_ADAPTER:
            CANONICAL_VENUE_TO_ADAPTER[_venue] = _adapter_key


# Maps canonical CeFi venues → CCXT exchange IDs for live mode.
# In live mode, these venues use CCXT (real-time public endpoints) instead of Tardis
# (historical-only). No API key needed — instrument definitions are public.
_CANONICAL_VENUE_TO_CCXT_EXCHANGE: dict[str, str] = {
    "BINANCE-SPOT": "binance",
    "BINANCE-FUTURES": "binanceusdm",
    "BYBIT": "bybit",
    "BYBIT-SPOT": "bybit",
    "BYBIT-FUTURES": "bybit",
    "OKX": "okx",
    "OKX-SPOT": "okx",
    "OKX-SWAP": "okx",
    "OKX-FUTURES": "okx",
    "DERIBIT": "deribit",
    "COINBASE-SPOT": "coinbase",
    "UPBIT": "upbit",
}

_ADAPTERS: dict[str, type[BaseReferenceDataAdapter]] = {
    "aave_v3": AaveV3ReferenceDataAdapter,
    "api_football": ApiFootballReferenceDataAdapter,
    "aster": AsterReferenceDataAdapter,
    "balancer": BalancerReferenceDataAdapter,
    "betfair": BetfairReferenceDataAdapter,
    "binance": BinanceReferenceDataAdapter,
    "bybit": BybitReferenceDataAdapter,
    "coinbase": CoinbaseReferenceDataAdapter,
    "compound_v3": CompoundV3ReferenceDataAdapter,
    "curve": CurveReferenceDataAdapter,
    "databento": DatabentoReferenceDataAdapter,
    "deribit": DeribitReferenceDataAdapter,
    "drift": DriftReferenceDataAdapter,
    "eigenlayer": EigenLayerReferenceDataAdapter,
    "ethena": EthenaReferenceDataAdapter,
    "ethfi_governance": EthFiGovernanceReferenceDataAdapter,
    "etherfi": EtherFiReferenceDataAdapter,
    "fluid": FluidReferenceDataAdapter,
    "hyperliquid": HyperliquidReferenceDataAdapter,
    "jito": JitoReferenceDataAdapter,
    "kamino": KaminoReferenceDataAdapter,
    "marinade": MarinadeReferenceDataAdapter,
    "ibkr": IBKRReferenceDataAdapter,
    "kalshi": KalshiReferenceDataAdapter,
    "lido": LidoReferenceDataAdapter,
    "morpho": MorphoReferenceDataAdapter,
    "okx": OKXReferenceDataAdapter,
    "orca": OrcaReferenceDataAdapter,
    "polygon": PolygonReferenceDataAdapter,
    "polymarket": PolymarketReferenceDataAdapter,
    "raydium": RaydiumReferenceDataAdapter,
    "tardis": TardisReferenceDataAdapter,
    "uniswap_v2": UniswapV2ReferenceDataAdapter,
    "uniswap_v3": UniswapV3ReferenceDataAdapter,
    "uniswap_v4": UniswapV4ReferenceDataAdapter,
}


# Maps URDI adapter key → UAC data source name (for API key lookup via DATA_SOURCE_TO_SECRET).
# Used by services to know which credential each URDI adapter needs.
# UTL's validate_api_keys_for_venues() returns {data_source: api_key}.
ADAPTER_DATA_SOURCES: dict[str, str] = {
    "binance": "tardis",
    "bybit": "tardis",
    "okx": "tardis",
    "deribit": "tardis",
    "coinbase": "tardis",
    "hyperliquid": "hyperliquid",
    "aster": "aster",
    "tardis": "tardis",
    "databento": "databento",
    "ibkr": "ibkr",
    "polygon": "polygon",
    "polymarket": "polymarket",
    "kalshi": "kalshi",
    "uniswap_v2": "thegraph",
    "uniswap_v3": "thegraph",
    "uniswap_v4": "thegraph",
    "aave_v3": "thegraph",
    "compound_v3": "thegraph",
    "morpho": "thegraph",
    "fluid": "thegraph",
    "lido": "thegraph",
    "etherfi": "thegraph",
    "ethena": "thegraph",
    "balancer": "balancer_api_v3",
    "curve": "rpc",
    "api_football": "api_football",
    "betfair": "betfair",
    # Solana adapters use public REST APIs (no API key needed)
    "drift": "",
    "kamino": "",
    "raydium": "",
    "orca": "",
    "marinade": "",
    "jito": "",
}


# Adapter pool: reuse adapter instances across calls.
# Key: (adapter_key, api_key, venue, date?) → adapter instance.
# Date is included for date-aware adapters like Databento (target_date baked at init).
_adapter_pool: dict[tuple[str, str | None, str, str | None], BaseReferenceDataAdapter] = {}


def clear_adapter_pool() -> None:
    """Clear the adapter pool + all adapter caches. Call on credential rotation."""
    for adapter in _adapter_pool.values():
        adapter.clear_cache()
    _adapter_pool.clear()


def get_adapter_for_canonical_venue(
    canonical_venue: str,
    api_key: str | None = None,
    project_id: str | None = None,
    date: str | None = None,
    extra_api_keys: dict[str, str] | None = None,
    mode: str = "batch",
) -> BaseReferenceDataAdapter:
    """Create a reference data adapter for a UAC canonical venue name.

    This is the preferred entry point for services that work with UAC canonical
    venue names (e.g. "UNISWAPV3-ETHEREUM", "BINANCE-SPOT"). Translates via
    CANONICAL_VENUE_TO_ADAPTER and delegates to create_reference_data_adapter().

    For CeFi venues in live mode, routes to CCXT (real-time public endpoints)
    instead of Tardis (historical-only). Batch mode always uses Tardis for
    the full historical instrument universe.

    Args:
        canonical_venue: UAC canonical venue name (e.g. "UNISWAPV3-ETHEREUM").
        api_key: Injected API key from Secret Manager (via UTL validate_api_keys_for_venues).
        project_id: Deprecated.
        date: ISO date string for date-aware adapters.
        extra_api_keys: Additional API keys for multi-key adapters.
        mode: "batch" (default) or "live". Live mode uses CCXT for CeFi venues.

    Raises:
        ValueError: If no adapter exists for this canonical venue name.
    """
    adapter_key = CANONICAL_VENUE_TO_ADAPTER.get(canonical_venue)
    if adapter_key is None:
        supported = sorted(CANONICAL_VENUE_TO_ADAPTER.keys())
        raise ValueError(
            f"No URDI adapter for canonical venue {canonical_venue!r}. "
            f"Add an entry to CANONICAL_VENUE_TO_ADAPTER. Supported: {supported}"
        )

    # Live mode: route CeFi Tardis venues to CCXT (real-time public endpoints).
    # Tardis is historical-only and can't provide live instrument definitions.
    ccxt_exchange_id = _CANONICAL_VENUE_TO_CCXT_EXCHANGE.get(canonical_venue)
    if mode == "live" and adapter_key == "tardis" and ccxt_exchange_id:
        pool_key = ("ccxt", None, canonical_venue, None)
        if pool_key in _adapter_pool:
            return _adapter_pool[pool_key]
        _logger.info(
            "Live mode: %s → CCXT (%s) instead of Tardis",
            canonical_venue,
            ccxt_exchange_id,
        )
        adapter: BaseReferenceDataAdapter = CCXTReferenceDataAdapter(
            venue=ccxt_exchange_id,
            canonical_venue=canonical_venue,
        )
        _adapter_pool[pool_key] = adapter
        return adapter

    # Live mode: route TradFi Databento venues to GCS-first adapter.
    # Reads the most recent GCS snapshot, filters expired instruments,
    # falls back to Databento (T-3 days) if no GCS data.
    if mode == "live" and adapter_key == "databento":
        pool_key = ("tradfi_live", api_key, canonical_venue, None)
        if pool_key in _adapter_pool:
            return _adapter_pool[pool_key]
        _logger.info(
            "Live mode: %s → GCS-first TradFi adapter (Databento fallback)",
            canonical_venue,
        )
        adapter = TradFiLiveReferenceDataAdapter(
            venue_filter=canonical_venue,
            api_key=api_key,
            project_id=project_id,
        )
        _adapter_pool[pool_key] = adapter
        return adapter

    # Check pool — reuse existing adapter if same key + credentials + venue + date
    # Include canonical_venue in pool key so AAVEV3-ARBITRUM != AAVEV3-ETHEREUM
    # Include date for Databento (target_date baked into adapter at init time)
    pool_date = date if adapter_key == "databento" else None
    pool_key = (adapter_key, api_key, canonical_venue, pool_date)
    if pool_key in _adapter_pool:
        return _adapter_pool[pool_key]

    # DeFi adapters that accept chain parameter (EVM + Solana)
    defi_graph_adapters = {
        "uniswap_v2",
        "uniswap_v3",
        "uniswap_v4",
        "aave_v3",
        "compound_v3",
        "morpho",
        "fluid",
        "balancer",
        "curve",
        # Solana adapters
        "drift",
        "kamino",
        "raydium",
        "orca",
        "marinade",
        "jito",
    }

    # Some adapters need extra constructor parameters derived from the canonical venue name.
    adapter: BaseReferenceDataAdapter
    if adapter_key == "api_football" and date is not None:
        adapter = ApiFootballReferenceDataAdapter(api_key=api_key, project_id=project_id, date=date)
    elif adapter_key in defi_graph_adapters:
        # DeFi adapters: parse chain from venue name, pass chain + optional date
        parts = canonical_venue.split("-", 1)
        chain = parts[1] if len(parts) == 2 else "ETHEREUM"
        adapter_class = _ADAPTERS[adapter_key]
        # Only some adapters accept date (aave_v3, uniswap_v2/v3/v4)
        accepts_date = {"uniswap_v2", "uniswap_v3", "uniswap_v4", "aave_v3", "compound_v3"}
        if adapter_key in accepts_date:
            adapter = adapter_class(project_id=project_id, api_key=api_key, chain=chain, date=date)
        else:
            adapter = adapter_class(project_id=project_id, api_key=api_key, chain=chain)
    elif adapter_key == "tardis":
        # Tardis: pass ONLY the specific exchange for this venue (not all defaults)
        from unified_api_contracts import VenueMapping as _VM_cls

        _vm = _VM_cls()
        tardis_exchange = _vm.get_tardis_exchange_for_venue(canonical_venue)
        if tardis_exchange is None:
            # Fallback: try direct lowercase conversion (BYBIT → bybit, DERIBIT → deribit)
            # Only valid if the result exists as a key in tardis_to_venue
            candidate = canonical_venue.lower()
            if candidate in _vm.tardis_to_venue:
                tardis_exchange = candidate
        if not tardis_exchange:
            # FAIL LOUD — do not silently fetch all exchanges
            raise ValueError(
                f"No Tardis exchange mapping for canonical venue {canonical_venue!r}. "
                f"Add a mapping in VenueMapping.tardis_to_venue or "
                f"venue_instrument_type_to_tardis for this venue."
            )
        _logger.debug("Tardis: %s → exchange=%s", canonical_venue, tardis_exchange)
        adapter = TardisReferenceDataAdapter(
            project_id=project_id,
            api_key=api_key,
            exchanges=[tardis_exchange],
        )
    elif adapter_key == "databento":
        # Databento: pass date + venue filter so each venue only fetches its instruments
        target = date_type.fromisoformat(date) if date else None
        adapter = DatabentoReferenceDataAdapter(
            project_id=project_id,
            api_key=api_key,
            target_date=target,
            venue_filter=canonical_venue,
        )
    elif adapter_key == "polymarket" and extra_api_keys:
        # Polymarket: pass API-Football key for fixture cross-referencing
        af_key = extra_api_keys.get("api_football")
        adapter = PolymarketReferenceDataAdapter(
            project_id=project_id,
            api_key=api_key,
            api_football_api_key=af_key,
        )
    else:
        adapter = create_reference_data_adapter(adapter_key, project_id=project_id, api_key=api_key)

    _adapter_pool[pool_key] = adapter
    return adapter


def create_reference_data_adapter(
    venue: str,
    project_id: str | None = None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Create and return a reference data adapter for the given venue.

    Args:
        venue: Venue identifier (aster, binance, bybit, okx, deribit, coinbase,
               hyperliquid, ibkr, databento, tardis, betfair, polymarket,
               polygon).
        project_id: Deprecated. Retained for call-site compatibility but no
                    longer used for internal Secret Manager lookups.
        api_key: API key for the venue. The calling service MUST fetch this
                 from Secret Manager and pass it in. Adapters that require
                 authentication will raise ``ValueError`` if not provided.

    Raises:
        ValueError: If venue is not supported.
    """
    venue_lower = venue.lower()
    _run_refdata_preflight(venue_lower)
    adapter_class = _ADAPTERS.get(venue_lower)
    if adapter_class is None:
        supported = sorted(_ADAPTERS.keys())
        raise ValueError(f"Unsupported venue: {venue!r}. Supported: {supported}")
    return adapter_class(project_id=project_id, api_key=api_key)


def _run_refdata_preflight(venue: str) -> None:
    """Run informational capability preflight for a reference-data venue.

    URDI is read-only, so this is advisory — log a warning if the venue/env
    combination is flagged as unsupported but never block adapter creation.
    Unknown venues (not in the capability registry) are silently skipped.
    """
    try:
        validate_operation(venue, "get_instruments")
    except CapabilityResolutionError:
        _logger.debug(
            "refdata_preflight_skip: venue=%s not in capability registry",
            venue,
        )
    except UnsupportedOperationError as exc:
        _logger.warning(
            "refdata_preflight_warn: venue=%s flagged as unsupported — proceeding anyway (read-only): %s",
            venue,
            exc,
        )
