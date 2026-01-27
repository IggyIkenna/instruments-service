"""
Base On-Chain Perpetuals Adapter for Instrument Definitions

Provides shared functionality for CLOB-style on-chain perpetual DEXes.
These venues produce data identical to CeFi exchanges:
- trades, derivative_ticker (funding+OI), liquidations, book_snapshot_5

This is different from DeFi protocols which produce:
- swaps, liquidity, rates, yields
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseOnchainPerpAdapter(ABC):
    """
    Base class for on-chain perpetual futures instrument adapters.

    On-chain CLOBs (Hyperliquid, Aster, dYdX) produce CLOB-style data
    identical to CeFi exchanges:
    - trades: Individual trade records
    - derivative_ticker: Funding rates + OI + mark/index prices
    - liquidations: Liquidation events
    - book_snapshot_5: L2 orderbook (5 levels each side)

    Market category is DEFI but data schema matches Tardis (CeFi).
    """

    # Standard data types for on-chain perps (Tardis-compatible)
    SUPPORTED_DATA_TYPES = [
        "trades",
        "derivative_ticker",  # Combines: funding_rate, mark_price, index_price, open_interest
        "liquidations",
        "book_snapshot_5",    # L2 orderbook (5 levels)
    ]

    def __init__(
        self,
        venue: str,
        chain: str,
        api_url: str,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize base on-chain perp adapter.

        Args:
            venue: Venue identifier (e.g., 'HYPERLIQUID', 'ASTER')
            chain: Chain identifier (e.g., 'HYPERLIQUID', 'ASTER')
            api_url: Base API URL for the venue
            api_key: Optional API key
            project_id: GCP project ID for Secret Manager
        """
        self.venue = venue
        self.chain = chain
        self.api_url = api_url
        self._api_key = api_key
        self._project_id = project_id

    @abstractmethod
    def fetch_perpetuals(
        self,
        test_data_availability: bool = False,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all perpetual futures instruments.

        Args:
            test_data_availability: If True, test data availability
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_spot_pairs(
        self,
        test_data_availability: bool = False,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all spot trading pairs.

        Args:
            test_data_availability: If True, test data availability
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        raise NotImplementedError

    def _build_instrument_key(
        self,
        instrument_type: str,
        symbol: str,
        suffix: str = "",
    ) -> str:
        """
        Build canonical instrument key.

        Format: {VENUE}:{TYPE}:{SYMBOL}@{CHAIN}
        Examples:
        - HYPERLIQUID:PERPETUAL:BTC-USDC@LIN@HYPERLIQUID
        - ASTER:PERPETUAL:BTC-USDT@LIN@ASTER
        """
        chain_suffix = f"@{self.chain}"
        if suffix:
            return f"{self.venue}:{instrument_type}:{symbol}{suffix}{chain_suffix}"
        return f"{self.venue}:{instrument_type}:{symbol}{chain_suffix}"

    def _get_data_types_string(self, include_book: bool = True) -> str:
        """
        Get comma-separated string of supported data types.

        Args:
            include_book: Whether to include book_snapshot_5 (may not be available)

        Returns:
            Comma-separated data types string
        """
        if include_book:
            return "trades,derivative_ticker,liquidations,book_snapshot_5"
        return "trades,derivative_ticker,liquidations"
