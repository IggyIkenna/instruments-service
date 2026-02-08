"""
On-Chain Perpetual Futures Adapters

CLOB-style on-chain perpetual DEXes that produce data identical to CeFi exchanges:
- Hyperliquid (L1 perpetual futures DEX)
- Aster (perpetual futures DEX with Binance-style API)

These are NOT AMM-style DEXes - they have order books and produce:
- trades, derivative_ticker (funding+OI), liquidations, book_snapshot_5

Kept separate from defi/ because data model matches CeFi (Tardis schema).
"""

from instruments_service.app.venues.onchain_perps.aster_adapter import (
    AsterAdapter,
)
from instruments_service.app.venues.onchain_perps.base_onchain_perp_adapter import (
    BaseOnchainPerpAdapter,
)
from instruments_service.app.venues.onchain_perps.hyperliquid_adapter import (
    HyperliquidAdapter,
)

__all__ = [
    "BaseOnchainPerpAdapter",
    "HyperliquidAdapter",
    "AsterAdapter",
]
