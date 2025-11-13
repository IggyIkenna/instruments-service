"""
Hyperliquid Adapter

Fetches Hyperliquid perpetual futures instruments and tests historical data availability.
Uses Hyperliquid REST API and S3 archive for historical data.

Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/
"""

import logging
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class HyperliquidAdapter:
    """
    Adapter for fetching Hyperliquid perpetual futures instruments.

    Generates instruments in format:
    HYPERLIQUID:PERPETUAL:BTC-USDC
    """

    def __init__(
        self,
        api_base_url: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        mvp_only: bool = True,
    ):
        """
        Initialize Hyperliquid adapter.

        Args:
            api_base_url: Optional custom API base URL (defaults to https://api.hyperliquid.xyz)
            base_currency_list: List of MVP base currencies from config (defaults to None, uses all if not provided)
            mvp_only: If True, only include MVP coins (default: True)
        """
        self.venue = "HYPERLIQUID"
        self.api_base_url = api_base_url or "https://api.hyperliquid.xyz"
        self.mvp_only = mvp_only
        # Use provided base_currency_list or empty set (no filtering)
        self.mvp_base_currencies = (
            {c.upper() for c in base_currency_list} if base_currency_list else set()
        )
        logger.info(
            f"✅ HyperliquidAdapter initialized (MVP only: {mvp_only}, base currencies: {len(self.mvp_base_currencies) if self.mvp_base_currencies else 'all'})"
        )

    def fetch_perpetuals(self, test_data_availability: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all perpetual futures from Hyperliquid.

        Args:
            test_data_availability: If True, test data availability and populate data_types

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        try:
            # Get perpetual metadata (use "meta" not "perpetualMetadata")
            response = requests.post(
                f"{self.api_base_url}/info",
                json={"type": "meta"},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            metadata = response.json()

            instruments = {}
            universe = metadata.get("universe", [])

            for asset in universe:
                try:
                    coin = asset.get("name", "")
                    if not coin:
                        continue

                    # Skip delisted assets
                    if asset.get("isDelisted", False):
                        continue

                    # Filter to MVP base currencies only if enabled
                    if self.mvp_only and self.mvp_base_currencies:
                        if coin.upper() not in self.mvp_base_currencies:
                            continue

                    inst_def = self._convert_asset_to_instrument(asset)
                    if inst_def:
                        # Set default data types - actual data fetching happens in market tick data handler
                        # Hyperliquid supports 1m candles + funding rates (derivative_ticker)
                        inst_def["data_types"] = "ohlcv_1m,derivative_ticker"

                        instruments[inst_def["instrument_key"]] = inst_def
                except Exception as e:
                    logger.warning(f"Failed to convert asset {asset.get('name', 'unknown')}: {e}")
                    continue

            logger.info(f"✅ Generated {len(instruments)} Hyperliquid perpetual instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Hyperliquid perpetuals: {e}")
            return {}

    def fetch_spot_pairs(self, test_data_availability: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all spot trading pairs from Hyperliquid for MVP coins.

        Args:
            test_data_availability: If True, test data availability and populate data_types

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        try:
            # Get perpetual metadata (same endpoint, but we'll create SPOT_PAIR instruments)
            response = requests.post(
                f"{self.api_base_url}/info",
                json={"type": "meta"},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            metadata = response.json()

            instruments = {}
            universe = metadata.get("universe", [])

            for asset in universe:
                try:
                    coin = asset.get("name", "")
                    if not coin:
                        continue

                    # Skip delisted assets
                    if asset.get("isDelisted", False):
                        continue

                    # Filter to MVP base currencies only if enabled
                    if self.mvp_only and self.mvp_base_currencies:
                        if coin.upper() not in self.mvp_base_currencies:
                            continue

                    # Create SPOT_PAIR instrument (Hyperliquid spot uses USDC as quote)
                    inst_def = self._convert_asset_to_spot_pair(asset)
                    if inst_def:
                        # Set default data types - actual data fetching happens in market tick data handler
                        # Hyperliquid spot supports trades and order book snapshots
                        inst_def["data_types"] = "trades,book_snapshot_5"

                        instruments[inst_def["instrument_key"]] = inst_def
                except Exception as e:
                    logger.warning(
                        f"Failed to convert asset {asset.get('name', 'unknown')} to spot pair: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} Hyperliquid spot pair instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Hyperliquid spot pairs: {e}")
            return {}

    def _convert_asset_to_spot_pair(self, asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Hyperliquid asset data to SPOT_PAIR instrument definition.

        Args:
            asset: Asset metadata from meta endpoint

        Returns:
            Instrument definition dictionary or None
        """
        coin = asset.get("name", "")
        if not coin:
            return None

        # Build symbol (Hyperliquid spot uses coin-USDC format)
        # Per INSTRUMENT_KEY.md: SPOT_PAIR format is BASE-QUOTE
        symbol = f"{coin}-USDC"

        # Build canonical instrument key with chain suffix
        # Format: VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN
        # Hyperliquid is on HYPERLIQUID chain
        instrument_key = f"{self.venue}:SPOT_PAIR:{symbol}@HYPERLIQUID"

        # Use conservative default date
        available_from = datetime(2023, 5, 1).isoformat()

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "SPOT_PAIR",
            "symbol": symbol,
            "base_asset": coin,
            "quote_asset": "USDC",
            "settle_asset": "USDC",
            "chain": "HYPERLIQUID",  # Hyperliquid is on its own chain
            "asset_class": "crypto",
            "venue_type": "exchange",  # Hyperliquid is an exchange, not a protocol
            "data_provider": "hyperliquid_api",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": coin,
            # ccxt_symbol and ccxt_exchange will be populated by centralized CCXT service
            # (same as CEFI exchanges) - don't set manually here
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Spot pairs don't expire
            "data_types": "",  # Will be populated after testing data availability
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": coin,
        }

    def _convert_asset_to_instrument(self, asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Hyperliquid asset data to instrument definition.

        Args:
            asset: Asset metadata from meta endpoint

        Returns:
            Instrument definition dictionary or None
        """
        coin = asset.get("name", "")
        if not coin:
            return None

        # Build symbol (Hyperliquid uses coin-USDC format for perpetuals)
        # Per INSTRUMENT_KEY.md: PERPETUAL format is BASE-QUOTE@LIN or BASE-QUOTE@INV
        # Hyperliquid perpetuals are linear (USDC margin), so use @LIN
        symbol = f"{coin}-USDC@LIN"

        # Build canonical instrument key with chain suffix
        # Format: VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN
        # Hyperliquid is on HYPERLIQUID chain
        instrument_key = f"{self.venue}:PERPETUAL:{symbol}@HYPERLIQUID"

        # Use conservative default date - funding rate fetching is handled by market tick data handler service
        # Hyperliquid launched in early 2024, but some instruments may have been available earlier
        # Use a conservative default (2023-05-01) to avoid filtering out instruments incorrectly
        available_from = datetime(2023, 5, 1).isoformat()

        # Extract metadata
        sz_decimals = asset.get("szDecimals", 8)
        max_leverage = asset.get("maxLeverage", {})
        max_leverage_value = (
            max_leverage
            if isinstance(max_leverage, (int, float))
            else (max_leverage.get("value", 1) if isinstance(max_leverage, dict) else 1)
        )

        # tick_size and min_size will be populated via CCXT enrichment in instrument_processing_service
        # Don't fetch here to avoid rate limits - CCXT cache handles this efficiently
        tick_size = ""
        min_size = ""

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "PERPETUAL",
            "symbol": symbol,
            "base_asset": coin,
            "quote_asset": "USDC",
            "settle_asset": "USDC",
            "chain": "HYPERLIQUID",  # Hyperliquid is on its own chain
            "asset_class": "crypto",
            "venue_type": "exchange",  # Hyperliquid is an exchange, not a protocol
            "data_provider": "hyperliquid_api",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": coin,
            # ccxt_symbol and ccxt_exchange will be populated by centralized CCXT service
            # (same as CEFI exchanges) - don't set manually here
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Perpetuals don't expire
            "data_types": "",  # Will be populated after testing data availability
            "inverse": False,
            "contract_size": None,
            "tick_size": tick_size,
            "min_size": min_size,
            "underlying": coin,
        }

    def test_candles_availability(
        self, coin: str, start_date: datetime, end_date: datetime, interval: str = "1m"
    ) -> bool:
        """
        Test if historical candles (OHLCV) are available for a coin.

        Args:
            coin: Coin symbol (e.g., "BTC")
            start_date: Start date for testing
            end_date: End date for testing
            interval: Candle interval ("1m", "1h", etc.)

        Returns:
            True if candles are available
        """
        try:
            start_ms = int(start_date.timestamp() * 1000)
            end_ms = int(end_date.timestamp() * 1000)

            response = requests.post(
                f"{self.api_base_url}/info",
                json={
                    "type": "candleSnapshot",
                    "req": {
                        "coin": coin,
                        "interval": interval,
                        "startTime": start_ms,
                        "endTime": end_ms,
                    },
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 200:
                candles = response.json()
                if candles and len(candles) > 0:
                    logger.info(f"✅ Historical candles ({interval}) available for {coin}")
                    return True
                else:
                    logger.warning(f"⚠️ No candles found for {coin} in date range")
            else:
                logger.warning(f"⚠️ Failed to fetch candles for {coin}: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Error testing candles for {coin}: {e}")

        return False
