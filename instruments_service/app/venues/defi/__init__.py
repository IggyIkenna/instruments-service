"""
DeFi Venue Adapters Package

Contains adapters for DeFi protocols:
- Uniswap V2/V3/V4 (DEX pools)
- Curve (DEX pools)
- Balancer (DEX pools)
- AAVE V3 (lending/borrowing positions)
- EtherFi (LST staking)
- Lido (LST staking)
- Morpho (lending protocol)
- Euler-Plasma (Plasma lending)
- Fluid-Plasma (Plasma lending)
- AAVE-Plasma (Plasma lending)
- Hyperliquid (perpetual futures DEX)
- Aster (perpetual futures exchange)
- Ethena (sUSDe synthetic dollar)
"""

from .the_graph_client import TheGraphClient
from .uniswapv2_adapter import UniswapV2Adapter
from .uniswapv3_adapter import UniswapV3Adapter
from .uniswapv4_adapter import UniswapV4Adapter
from .curve_adapter import CurveAdapter
from .balancer_adapter import BalancerAdapter
from .aave_adapter import AaveV3Adapter
from .lst_adapters import EtherFiAdapter, LidoAdapter
from .morpho_adapter import MorphoAdapter
from .plasma_adapters import (
    EulerPlasmaAdapter,
    FluidPlasmaAdapter,
    AavePlasmaAdapter,
)
from .hyperliquid_adapter import HyperliquidAdapter
from .aster_adapter import AsterAdapter
from .ethena_adapter import EthenaAdapter

__all__ = [
    "TheGraphClient",
    "UniswapV2Adapter",
    "UniswapV3Adapter",
    "UniswapV4Adapter",
    "CurveAdapter",
    "BalancerAdapter",
    "AaveV3Adapter",
    "EtherFiAdapter",
    "LidoAdapter",
    "MorphoAdapter",
    "EulerPlasmaAdapter",
    "FluidPlasmaAdapter",
    "AavePlasmaAdapter",
    "HyperliquidAdapter",
    "AsterAdapter",
    "EthenaAdapter",
]
