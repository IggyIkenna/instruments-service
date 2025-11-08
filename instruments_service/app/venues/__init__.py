"""
Venues Package

Contains venue adapters for fetching instruments from various sources:
- Tardis (crypto exchanges)
- Databento (TradFi exchanges)
- The Graph (DeFi DEX pools)
- Protocol SDKs (DeFi protocols)
"""

# Import all venue adapters
from .tardis import TardisAdapter
from .databento import DatabentoAdapter
from .defi import (
    TheGraphClient,
    UniswapV3Adapter,
    CurveAdapter,
    AaveV3Adapter,
    EtherFiAdapter,
    LidoAdapter,
)

__all__ = [
    'TardisAdapter',
    'DatabentoAdapter',
    'TheGraphClient',
    'UniswapV3Adapter',
    'CurveAdapter',
    'AaveV3Adapter',
    'EtherFiAdapter',
    'LidoAdapter',
]

