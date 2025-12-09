"""
DeFi Venue Adapters Package - OPTIMIZED MVP

Contains adapters for DeFi protocols (active protocols only):
- Uniswap V3 (DEX pools)
- Balancer (DEX pools)
- AAVE V3 (lending/borrowing positions with emode params)
- EtherFi (LST staking)
- Lido (LST staking)
- Ethena (sUSDe yield-bearing)
- Morpho (lending protocol)
- Hyperliquid (perpetual futures DEX)
- Aster (perpetual futures exchange)

Removed for performance (0 instruments generated):
- Uniswap V2, V4, Curve, Plasma adapters
"""

from instruments_service.app.venues.defi.the_graph_client import TheGraphClient
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter
from instruments_service.app.venues.defi.balancer_adapter import BalancerAdapter
from instruments_service.app.venues.defi.aave_adapter import AaveV3Adapter
from instruments_service.app.venues.defi.lst_adapters import EtherFiAdapter, LidoAdapter
from instruments_service.app.venues.defi.ethena_adapter import EthenaAdapter
from instruments_service.app.venues.defi.morpho_adapter import MorphoAdapter
from instruments_service.app.venues.defi.hyperliquid_adapter import HyperliquidAdapter
from instruments_service.app.venues.defi.aster_adapter import AsterAdapter
from instruments_service.app.venues.defi.curve_rpc_adapter import CurveRPCAdapter

__all__ = [
    "TheGraphClient",
    "BaseDefiAdapter",
    "UniswapV3Adapter",
    "BalancerAdapter",
    "AaveV3Adapter",
    "EtherFiAdapter",
    "LidoAdapter",
    "EthenaAdapter",
    "MorphoAdapter",
    "HyperliquidAdapter",
    "AsterAdapter",
    "CurveRPCAdapter",
]
