"""
Venue Adapter Loader

Provides on-demand loading of venue adapters with singleton caching.
Only loads adapters for requested venues, improving startup time and reducing dependencies.

Complies with:
- Cursor rules: imports at top
- Codex: 06-coding-standards/README.md#imports
- Audit: DOM-02 (asset class taxonomy)
"""

import logging
from typing import Protocol

from unified_market_interface import DataSourceMapping, YahooFinanceAdapter

logger = logging.getLogger(__name__)


class DataSourceAdapter(Protocol):
    """Protocol for data source adapters (Tardis, Databento, DeFi, etc.)."""

    pass


# Singleton adapter cache (one instance per data source)
_ADAPTER_CACHE: dict[str, DataSourceAdapter] = {}


def get_adapter_for_venue(venue: str, api_keys: dict[str, str] | None = None) -> DataSourceAdapter:
    """
    Lazy-load adapter for a venue.

    Args:
        venue: Venue name (e.g., "BINANCE-FUTURES", "ASTER")
        api_keys: Optional dict of data_source -> API key

    Returns:
        Adapter instance (cached singleton per data source)

    Raises:
        ValueError: If venue is unknown or adapter initialization fails
    """
    data_source = DataSourceMapping.get_data_source_for_venue(venue)
    if not data_source:
        raise ValueError(f"Unknown venue: {venue}")

    # For DeFi venues (thegraph), each venue gets its own adapter instance
    # For other data sources (tardis, databento), single shared instance
    cache_key = f"{data_source}:{venue.upper()}" if data_source == "thegraph" else data_source

    # Return cached adapter if already loaded
    if cache_key in _ADAPTER_CACHE:
        logger.debug(f"Reusing cached adapter for {data_source}")
        return _ADAPTER_CACHE[cache_key]

    # Lazy import and instantiate adapter
    logger.info(f"Lazy-loading adapter for {data_source} (venue: {venue})")

    try:
        if data_source == "tardis":
            from unified_market_interface import TardisAdapter

            api_key = api_keys.get("tardis") if api_keys else None
            adapter = TardisAdapter(api_key=api_key)

        elif data_source == "databento":
            from unified_market_interface import DatabentoAdapter

            api_key = api_keys.get("databento") if api_keys else None
            adapter = DatabentoAdapter(api_key=api_key)

        elif data_source == "aster":
            raise NotImplementedError(
                "Aster adapter not available (AsterBaseClient removed from UCS). "
                "Use Hyperliquid or other on-chain perpetual venues."
            )

        elif data_source == "hyperliquid":
            from unified_market_interface import HyperliquidAdapter, HyperliquidBaseClient

            adapter = HyperliquidAdapter(base_client=HyperliquidBaseClient())

        elif data_source == "thegraph":
            # DeFi adapters - load specific adapter based on venue
            adapter = _load_defi_adapter(venue, api_keys)

        elif data_source == "yfinance":
            # FX adapter (KRW/USD, corporate actions) - no API key required

            adapter = YahooFinanceAdapter()

        elif data_source == "barchart":
            # VIX adapter
            # Import will be added when Barchart adapter is implemented
            raise NotImplementedError("barchart adapter not yet implemented")

        else:
            raise ValueError(f"Unsupported data source: {data_source}")

        # Cache and return
        _ADAPTER_CACHE[cache_key] = adapter
        logger.info(f"✅ Loaded adapter for {data_source}")
        return adapter

    except ImportError as e:
        logger.error(f"Failed to import adapter for {data_source}: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize adapter for {data_source}: {e}")
        raise


def _load_defi_adapter(venue: str, api_keys: dict[str, str] | None) -> DataSourceAdapter:
    """Load DeFi adapter based on venue."""
    venue_upper = venue.upper()

    if venue_upper == "UNISWAP-V2":
        from unified_market_interface import UniswapV2Adapter

        return UniswapV2Adapter()
    elif venue_upper == "UNISWAP-V3":
        from unified_market_interface import UniswapV3Adapter

        return UniswapV3Adapter()
    elif venue_upper == "UNISWAP-V4":
        from unified_market_interface import UniswapV4Adapter

        return UniswapV4Adapter()
    elif venue_upper == "AAVE-V3":
        from unified_market_interface import AaveV3Adapter

        return AaveV3Adapter()
    elif venue_upper == "CURVE":
        from unified_market_interface import CurveAdapter

        return CurveAdapter()
    elif venue_upper == "BALANCER":
        from unified_market_interface import BalancerAdapter

        return BalancerAdapter()
    elif venue_upper == "MORPHO":
        from unified_market_interface import MorphoAdapter

        return MorphoAdapter()
    elif venue_upper == "EULER":
        from unified_market_interface import EulerAdapter

        return EulerAdapter()
    elif venue_upper == "FLUID":
        from unified_market_interface import FluidAdapter

        return FluidAdapter()
    elif venue_upper == "LIDO":
        from unified_market_interface import LidoAdapter

        return LidoAdapter()
    elif venue_upper == "ETHERFI":
        from unified_market_interface import EtherFiAdapter

        return EtherFiAdapter()
    elif venue_upper == "ETHENA":
        from unified_market_interface import EthenaAdapter

        return EthenaAdapter()
    else:
        raise ValueError(f"Unknown DeFi venue: {venue}")


def clear_adapter_cache():
    """Clear adapter cache (useful for testing)."""
    global _ADAPTER_CACHE
    _ADAPTER_CACHE.clear()
    logger.debug("Adapter cache cleared")


def get_cached_adapters() -> dict[str, DataSourceAdapter]:
    """Get currently cached adapters (for testing/debugging)."""
    return _ADAPTER_CACHE.copy()
