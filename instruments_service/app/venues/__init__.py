"""
Venues Package - OPTIMIZED MVP

Contains venue adapters for fetching instruments from various sources:
- Tardis (crypto exchanges - parallel processing)
- Databento (TradFi CME + VIX - cached client)
- The Graph (DeFi DEX pools - active protocols only)
- Protocol SDKs (DeFi protocols - AAVE with emode params)
"""

# Import all venue adapters (active only)
from instruments_service.app.venues.databento import DatabentoAdapter
from instruments_service.app.venues.defi import (
    AaveV3Adapter,
    EtherFiAdapter,
    LidoAdapter,
    TheGraphClient,
    UniswapV3Adapter,
)
from instruments_service.app.venues.tardis import TardisAdapter

__all__ = [
    "TardisAdapter",
    "DatabentoAdapter",
    "TheGraphClient",
    "UniswapV3Adapter",
    "AaveV3Adapter",
    "EtherFiAdapter",
    "LidoAdapter",
]
