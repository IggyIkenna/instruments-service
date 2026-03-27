"""Adapter factory for unified-reference-data-interface."""

import contextlib
import logging

from unified_api_contracts.registry import (  # noqa: qg-deep-import
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
from .adapters.coinbase import CoinbaseReferenceDataAdapter
from .adapters.curve import CurveReferenceDataAdapter
from .adapters.databento import DatabentoReferenceDataAdapter
from .adapters.deribit import DeribitReferenceDataAdapter
from .adapters.ethena import EthenaReferenceDataAdapter
from .adapters.etherfi import EtherFiReferenceDataAdapter
from .adapters.euler import EulerReferenceDataAdapter
from .adapters.fluid import FluidReferenceDataAdapter
from .adapters.hyperliquid import HyperliquidReferenceDataAdapter
from .adapters.ibkr import IBKRReferenceDataAdapter
from .adapters.kalshi import KalshiReferenceDataAdapter
from .adapters.lido import LidoReferenceDataAdapter
from .adapters.morpho import MorphoReferenceDataAdapter
from .adapters.okx import OKXReferenceDataAdapter
from .adapters.polygon import PolygonReferenceDataAdapter
from .adapters.polymarket import PolymarketReferenceDataAdapter
from .adapters.tardis import TardisReferenceDataAdapter
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
    # CeFi — all venues through Tardis for batch (historical instrument universe).
    # Tardis _DEFAULT_EXCHANGES lists the specific exchange names.
    # Direct adapters (binance, deribit, etc.) exist for live mode but are not
    # used for instrument reference data fetching.
    "BINANCE-SPOT": "tardis",
    "BINANCE-FUTURES": "tardis",
    "BYBIT": "tardis",
    "BYBIT-SPOT": "tardis",
    "BYBIT-FUTURES": "tardis",
    "OKX": "tardis",
    "OKX-SPOT": "tardis",
    "OKX-FUTURES": "tardis",
    "DERIBIT": "tardis",
    "COINBASE-SPOT": "tardis",
    "COINBASE": "tardis",
    "UPBIT": "tardis",
    "HUOBI-SPOT": "tardis",
    "HUOBI-FUTURES": "tardis",
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
    # DeFi — DEX protocols
    "UNISWAPV2-ETHEREUM": "uniswap_v2",
    "UNISWAPV3-ETHEREUM": "uniswap_v3",
    "UNISWAPV4-ETHEREUM": "uniswap_v4",
    "CURVE-ETHEREUM": "curve",
    "BALANCER-ETHEREUM": "balancer",
    # DeFi — Lending protocols
    "AAVEV3-ETHEREUM": "aave_v3",
    # To add back: find correct subgraph via scripts/find_subgraph_ids.py,
    "MORPHO-ETHEREUM": "morpho",
    "EULER-ETHEREUM": "euler",
    "FLUID-ETHEREUM": "fluid",
    # DeFi — LST/Yield protocols
    "LIDO-ETHEREUM": "lido",
    "ETHERFI-ETHEREUM": "etherfi",
    "ETHENA-ETHEREUM": "ethena",
    # Sports
    "BETFAIR": "betfair",
    "API_FOOTBALL": "api_football",
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
    "curve": CurveReferenceDataAdapter,
    "databento": DatabentoReferenceDataAdapter,
    "deribit": DeribitReferenceDataAdapter,
    "ethena": EthenaReferenceDataAdapter,
    "etherfi": EtherFiReferenceDataAdapter,
    "euler": EulerReferenceDataAdapter,
    "fluid": FluidReferenceDataAdapter,
    "hyperliquid": HyperliquidReferenceDataAdapter,
    "ibkr": IBKRReferenceDataAdapter,
    "kalshi": KalshiReferenceDataAdapter,
    "lido": LidoReferenceDataAdapter,
    "morpho": MorphoReferenceDataAdapter,
    "okx": OKXReferenceDataAdapter,
    "polygon": PolygonReferenceDataAdapter,
    "polymarket": PolymarketReferenceDataAdapter,
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
    "morpho": "thegraph",
    "euler": "thegraph",
    "fluid": "thegraph",
    "lido": "thegraph",
    "etherfi": "thegraph",
    "ethena": "thegraph",
    "balancer": "balancer_api_v3",
    "curve": "rpc",
    "api_football": "api_football",
    "betfair": "betfair",
}


# Adapter pool: reuse adapter instances across calls.
# Key: (adapter_key, api_key) → adapter instance.
# Adapters are stateless (no mutable state beyond cache) so pooling is safe.
_adapter_pool: dict[tuple[str, str | None], BaseReferenceDataAdapter] = {}


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
) -> BaseReferenceDataAdapter:
    """Create a reference data adapter for a UAC canonical venue name.

    This is the preferred entry point for services that work with UAC canonical
    venue names (e.g. "UNISWAPV3-ETHEREUM", "BINANCE-SPOT"). Translates via
    CANONICAL_VENUE_TO_ADAPTER and delegates to create_reference_data_adapter().

    Args:
        canonical_venue: UAC canonical venue name (e.g. "UNISWAPV3-ETHEREUM").
        api_key: Injected API key from Secret Manager (via UTL validate_api_keys_for_venues).
        project_id: Deprecated.

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
    # Check pool — reuse existing adapter if same key + credentials
    pool_key = (adapter_key, api_key)
    if pool_key in _adapter_pool:
        return _adapter_pool[pool_key]

    # Some adapters need extra constructor parameters derived from the canonical venue name.
    adapter: BaseReferenceDataAdapter
    if adapter_key == "api_football" and date is not None:
        adapter = ApiFootballReferenceDataAdapter(api_key=api_key, project_id=project_id, date=date)
    elif adapter_key in ("uniswap_v2", "uniswap_v3", "uniswap_v4", "aave_v3"):
        # DeFi Graph adapters: pass date for historical block-based querying.
        parts = canonical_venue.split("-", 1)
        chain = parts[1] if len(parts) == 2 else "ETHEREUM"
        adapter_class = _ADAPTERS[adapter_key]
        adapter = adapter_class(project_id=project_id, api_key=api_key, chain=chain, date=date)
    elif adapter_key == "databento":
        # Databento: pass date for session metadata + expiry filtering
        from datetime import date as date_type

        target = date_type.fromisoformat(date) if date else None
        adapter = DatabentoReferenceDataAdapter(project_id=project_id, api_key=api_key, target_date=target)
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
