"""
DeFi Venue Adapters Package

Contains adapters for DeFi protocols:
- Uniswap V3 (DEX pools)
- Curve (DEX pools)
- AAVE V3 (lending/borrowing positions)
- EtherFi (LST staking)
- Lido (LST staking)
"""

from .the_graph_client import TheGraphClient
from .uniswapv3_adapter import UniswapV3Adapter
from .curve_adapter import CurveAdapter
from .aave_adapter import AaveV3Adapter
from .lst_adapters import EtherFiAdapter, LidoAdapter

__all__ = [
    'TheGraphClient',
    'UniswapV3Adapter',
    'CurveAdapter',
    'AaveV3Adapter',
    'EtherFiAdapter',
    'LidoAdapter',
]

