# SCHEMA_PROVENANCE_EXEMPT — ReferenceDataSourceConfig is a service-internal routing config
"""DataSourceRouter: map (venue, data_source) pairs to reference data adapters.

Services call create_reference_data_adapter_for_source() with a
ReferenceDataSourceConfig to get the correct adapter for a venue+source pair.
The data source is configurable — never hardcoded in the caller.

Routing table
-------------
Crypto
  binance   / tardis    → TardisReferenceDataAdapter(exchanges=["binance-futures"])
  binance   / ccxt      → CcxtReferenceDataAdapter(exchange_id="binance")
  bybit     / tardis    → TardisReferenceDataAdapter(exchanges=["bybit"])
  bybit     / ccxt      → CcxtReferenceDataAdapter(exchange_id="bybit")
  okx       / ccxt      → CcxtReferenceDataAdapter(exchange_id="okx")
  deribit   / tardis    → TardisReferenceDataAdapter(exchanges=["deribit"])
  hyperliquid / direct  → HyperliquidReferenceDataAdapter
  aster     / direct    → AsterReferenceDataAdapter

TradFi equities
  apple     / databento → DatabentoReferenceDataAdapter(datasets=["XNAS.ITCH"])
  apple     / polygon   → PolygonReferenceDataAdapter
  apple     / ibkr      → IBKRReferenceDataAdapter
  nasdaq    / databento → DatabentoReferenceDataAdapter(datasets=["XNAS.ITCH"])
  nyse      / databento → DatabentoReferenceDataAdapter(datasets=["XNYS.PILLAR"])

TradFi futures
  cme_futures / databento → DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
  cme_futures / ibkr      → IBKRReferenceDataAdapter
  ibkr        / ibkr      → IBKRReferenceDataAdapter

TradFi options
  cboe_options / databento → DatabentoReferenceDataAdapter(datasets=["OPRA.PILLAR"])
  cboe_options / polygon   → PolygonReferenceDataAdapter

Generic data-source-only
  databento / databento → DatabentoReferenceDataAdapter (default datasets)
  tardis    / tardis    → TardisReferenceDataAdapter (default exchanges)
  polygon   / polygon   → PolygonReferenceDataAdapter

DeFi protocols
  uniswap_v3  / direct    → UniswapV3ReferenceDataAdapter
  uniswap_v2  / direct    → UniswapV2ReferenceDataAdapter
  uniswap_v4  / direct    → UniswapV4ReferenceDataAdapter
  curve       / direct    → CurveReferenceDataAdapter
  aave_v3     / direct    → AaveV3ReferenceDataAdapter
  morpho      / direct    → MorphoReferenceDataAdapter

  fluid       / direct    → FluidReferenceDataAdapter
  ethena      / direct    → EthenaReferenceDataAdapter
  balancer    / direct    → BalancerReferenceDataAdapter
  lido        / direct    → LidoReferenceDataAdapter
  etherfi     / direct    → EtherFiReferenceDataAdapter

Sports / prediction markets
  api_football / api_football → ApiFootballReferenceDataAdapter
  api_football / direct       → ApiFootballReferenceDataAdapter
  betfair      / betfair      → BetfairReferenceDataAdapter
  polymarket   / polymarket   → PolymarketReferenceDataAdapter
  kalshi       / kalshi       → KalshiReferenceDataAdapter
  kalshi       / direct       → KalshiReferenceDataAdapter
"""

import contextlib
import logging

from pydantic import BaseModel
from unified_api_contracts.registry import (
    CapabilityResolutionError,
    UnsupportedOperationError,
    bootstrap_capabilities,
    validate_operation,
)

from .adapters.cefi.aster import AsterReferenceDataAdapter
from .adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter
from .adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter
from .adapters.cefi.tardis import TardisReferenceDataAdapter
from .adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from .adapters.defi.balancer import BalancerReferenceDataAdapter
from .adapters.defi.curve import CurveReferenceDataAdapter
from .adapters.defi.ethena import EthenaReferenceDataAdapter
from .adapters.defi.etherfi import EtherFiReferenceDataAdapter
from .adapters.defi.fluid import FluidReferenceDataAdapter
from .adapters.defi.lido import LidoReferenceDataAdapter
from .adapters.defi.morpho import MorphoReferenceDataAdapter
from .adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from .adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter
from .adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter
from .adapters.prediction.kalshi import KalshiReferenceDataAdapter
from .adapters.prediction.polymarket import PolymarketReferenceDataAdapter
from .adapters.sports.adapters.api_football_reference import ApiFootballReferenceDataAdapter
from .adapters.sports.adapters.betfair import BetfairReferenceDataAdapter
from .adapters.tradfi.databento import DatabentoReferenceDataAdapter
from .adapters.tradfi.ibkr import IBKRReferenceDataAdapter
from .adapters.tradfi.polygon import PolygonReferenceDataAdapter
from .base_adapter import BaseReferenceDataAdapter

_logger = logging.getLogger(__name__)

# Bootstrap capability registry once at module load (idempotent)
with contextlib.suppress(Exception):
    bootstrap_capabilities()


def _run_refdata_source_preflight(venue: str, data_source: str) -> None:
    """Run informational capability preflight for a routed reference-data source.

    URDI is read-only, so this is advisory — log a warning if the venue/env
    combination is flagged as unsupported but never block adapter creation.
    Unknown venues (not in the capability registry) are silently skipped.

    Uses the data_source as the operation name when it differs from "direct",
    otherwise falls back to "get_instruments".
    """
    operation = "get_instruments" if data_source == "direct" else f"refdata_{data_source}"
    try:
        validate_operation(venue, operation)
    except CapabilityResolutionError:
        _logger.debug(
            "refdata_source_preflight_skip: venue=%s source=%s not in capability registry",
            venue,
            data_source,
        )
    except UnsupportedOperationError as exc:
        _logger.warning(
            "refdata_source_preflight_warn: venue=%s source=%s flagged as unsupported — "
            "proceeding anyway (read-only): %s",
            venue,
            data_source,
            exc,
        )


class ReferenceDataSourceConfig(BaseModel):  # CORRECT-LOCAL — service routing config, not a domain contract
    """Configuration for routing a venue to a specific data source adapter.

    Attributes:
        venue: Logical venue identifier (e.g. "binance", "apple", "betfair").
               Does not need to match the adapter's internal venue string.
        data_source: Data provider to use (e.g. "direct", "databento", "tardis",
                     "ibkr", "polygon", "ccxt", "betfair", "polymarket").
        dataset: For Databento: dataset code (e.g. "XNAS.ITCH", "GLBX.MDP3").
                 When set, overrides default dataset selection.
        exchange: For Tardis or CCXT: exchange slug (e.g. "binance-futures", "bybit").
                  When set, overrides default exchange list.
        fallback_data_source: If set, the caller may use this string to build
                              a second adapter when the primary fails.
                              Not consumed by this router — informational only.
    """

    venue: str
    data_source: str
    dataset: str | None = None
    exchange: str | None = None
    fallback_data_source: str | None = None


# ---------------------------------------------------------------------------
# Databento dataset defaults per venue
# ---------------------------------------------------------------------------

_DATABENTO_VENUE_DATASETS: dict[str, list[str]] = {
    # Crypto
    "binance": ["XNAS.ITCH"],
    # TradFi equities
    "apple": ["XNAS.ITCH"],
    "nasdaq": ["XNAS.ITCH"],
    "nyse": ["XNYS.PILLAR"],
    # TradFi futures
    "cme_futures": ["GLBX.MDP3"],
    # TradFi options
    "cboe_options": ["OPRA.PILLAR"],
    # Generic Databento (all default datasets)
    "databento": [],  # empty → DatabentoReferenceDataAdapter uses its own default
}

# ---------------------------------------------------------------------------
# Tardis exchange defaults per venue
# ---------------------------------------------------------------------------

_TARDIS_VENUE_EXCHANGES: dict[str, list[str]] = {
    "binance": ["binance-futures"],
    "bybit": ["bybit"],
    "deribit": ["deribit"],
    "tardis": [],  # empty → TardisReferenceDataAdapter uses its own default
}

# ---------------------------------------------------------------------------
# CCXT exchange_id defaults per venue
# ---------------------------------------------------------------------------

_CCXT_VENUE_EXCHANGES: dict[str, str] = {
    "binance": "binance",
    "bybit": "bybit",
    "okx": "okx",
}


def create_reference_data_adapter_for_source(
    config: ReferenceDataSourceConfig,
    project_id: str | None = None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Route (venue, data_source) to the correct adapter implementation.

    Args:
        config: Routing configuration specifying venue and data source.
        project_id: Deprecated. Retained for call-site compatibility but no
                    longer used for internal Secret Manager lookups.
        api_key: API key for the venue. The calling service MUST fetch this
                 from Secret Manager and pass it in.

    Returns:
        A BaseReferenceDataAdapter instance ready for use.

    Raises:
        ValueError: If the (venue, data_source) combination is not supported.
    """
    venue = config.venue.lower()
    source = config.data_source.lower()

    _run_refdata_source_preflight(venue, source)

    if source == "direct":
        return _route_direct(venue, project_id, api_key)
    if source == "databento":
        return _route_databento(venue, config, project_id, api_key)
    if source == "tardis":
        return _route_tardis(venue, config, project_id, api_key)
    if source == "ccxt":
        return _route_ccxt(venue, config, project_id, api_key)

    simple_source_map: dict[str, type[BaseReferenceDataAdapter]] = {
        "api_football": ApiFootballReferenceDataAdapter,
        "ibkr": IBKRReferenceDataAdapter,
        "polygon": PolygonReferenceDataAdapter,
        "betfair": BetfairReferenceDataAdapter,
        "polymarket": PolymarketReferenceDataAdapter,
        "kalshi": KalshiReferenceDataAdapter,
    }
    adapter_class = simple_source_map.get(source)
    if adapter_class is not None:
        return adapter_class(project_id=project_id, api_key=api_key)

    supported = "direct, databento, tardis, ccxt, api_football, ibkr, polygon, betfair, polymarket, kalshi"
    msg = f"Unsupported data_source {source!r} for venue {venue!r}. Supported: {supported}."
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _route_databento(
    venue: str,
    config: ReferenceDataSourceConfig,
    project_id: str | None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Route source='databento' to the Databento adapter with resolved datasets."""
    datasets = _resolve_databento_datasets(venue, config.dataset)
    if datasets:
        return DatabentoReferenceDataAdapter(
            project_id=project_id,
            datasets=datasets,
            api_key=api_key,
        )
    return DatabentoReferenceDataAdapter(project_id=project_id, api_key=api_key)


def _route_tardis(
    venue: str,
    config: ReferenceDataSourceConfig,
    project_id: str | None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Route source='tardis' to the Tardis adapter with resolved exchanges."""
    exchanges = _resolve_tardis_exchanges(venue, config.exchange)
    if exchanges:
        return TardisReferenceDataAdapter(
            project_id=project_id,
            exchanges=exchanges,
            api_key=api_key,
        )
    return TardisReferenceDataAdapter(project_id=project_id, api_key=api_key)


def _route_ccxt(
    venue: str,
    config: ReferenceDataSourceConfig,
    project_id: str | None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Route source='ccxt' to the CCXT adapter with resolved exchange_id."""
    exchange_id = config.exchange or _CCXT_VENUE_EXCHANGES.get(venue)
    if exchange_id is None:
        raise ValueError(
            f"No CCXT exchange_id configured for venue {venue!r}. "
            "Pass exchange=<ccxt_exchange_id> in ReferenceDataSourceConfig."
        )
    return CCXTReferenceDataAdapter(venue=exchange_id, project_id=project_id, api_key=api_key)


def _route_direct(
    venue: str,
    project_id: str | None,
    api_key: str | None = None,
) -> BaseReferenceDataAdapter:
    """Route venue=X, data_source='direct' to the native adapter."""
    _direct_map: dict[str, type[BaseReferenceDataAdapter]] = {
        "aave_v3": AaveV3ReferenceDataAdapter,
        "api_football": ApiFootballReferenceDataAdapter,
        "aster": AsterReferenceDataAdapter,
        "balancer": BalancerReferenceDataAdapter,
        "betfair": BetfairReferenceDataAdapter,
        "curve": CurveReferenceDataAdapter,
        "ethena": EthenaReferenceDataAdapter,
        "etherfi": EtherFiReferenceDataAdapter,
        "fluid": FluidReferenceDataAdapter,
        "hyperliquid": HyperliquidReferenceDataAdapter,
        "ibkr": IBKRReferenceDataAdapter,
        "kalshi": KalshiReferenceDataAdapter,
        "lido": LidoReferenceDataAdapter,
        "morpho": MorphoReferenceDataAdapter,
        "polygon": PolygonReferenceDataAdapter,
        "polymarket": PolymarketReferenceDataAdapter,
        "uniswap_v2": UniswapV2ReferenceDataAdapter,
        "uniswap_v3": UniswapV3ReferenceDataAdapter,
        "uniswap_v4": UniswapV4ReferenceDataAdapter,
    }
    adapter_class = _direct_map.get(venue)
    if adapter_class is None:
        raise ValueError(f"No direct adapter for venue {venue!r}. Supported: {sorted(_direct_map.keys())}")
    return adapter_class(project_id=project_id, api_key=api_key)


def _resolve_databento_datasets(venue: str, override: str | None) -> list[str]:
    """Resolve Databento dataset list for a venue.

    If config.dataset is provided, it takes priority.
    Otherwise falls back to _DATABENTO_VENUE_DATASETS.
    Returns empty list for unknown venues (adapter uses its own default).
    """
    if override:
        return [override]
    return _DATABENTO_VENUE_DATASETS.get(venue, [])


def _resolve_tardis_exchanges(venue: str, override: str | None) -> list[str]:
    """Resolve Tardis exchange list for a venue.

    If config.exchange is provided, it takes priority.
    Returns empty list for unknown venues (adapter uses its own default).
    """
    if override:
        return [override]
    return _TARDIS_VENUE_EXCHANGES.get(venue, [])
