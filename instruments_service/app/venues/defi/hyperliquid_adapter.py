"""
Hyperliquid Adapter

Fetches Hyperliquid perpetual futures instruments and tests historical data availability.
Uses Hyperliquid REST API and S3 archive for historical data.

Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/
"""

import logging
import requests
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from unified_cloud_services import handle_api_errors

logger = logging.getLogger(__name__)

# Module-level cache for Hyperliquid API responses (instrument lists rarely change)
# Format: {cache_key: (data, timestamp)}
_HYPERLIQUID_CACHE: Dict[str, Tuple[Any, datetime]] = {}
_HYPERLIQUID_CACHE_TTL = timedelta(hours=4)  # 4 hour TTL


def _get_cached_response(cache_key: str) -> Optional[Any]:
    """Get cached API response if still valid."""
    if cache_key in _HYPERLIQUID_CACHE:
        data, timestamp = _HYPERLIQUID_CACHE[cache_key]
        if datetime.now(timezone.utc) - timestamp < _HYPERLIQUID_CACHE_TTL:
            return data
    return None


def _set_cached_response(cache_key: str, data: Any):
    """Cache an API response."""
    _HYPERLIQUID_CACHE[cache_key] = (data, datetime.now(timezone.utc))


def clear_hyperliquid_cache():
    """Clear all Hyperliquid caches."""
    global _HYPERLIQUID_CACHE
    _HYPERLIQUID_CACHE.clear()
    logger.info("🧹 Cleared Hyperliquid cache")


class HyperliquidAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Hyperliquid perpetual futures instruments.

    Generates instruments in format:
    HYPERLIQUID:PERPETUAL:BTC-USDC
    """

    # Hyperliquid mainnet launch (December 2022)
    HYPERLIQUID_LAUNCH_DATE = datetime(2022, 12, 1)

    def __init__(
        self,
        api_base_url: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        mvp_only: bool = True,
        chain: str = "off-chain",  # CEFI classification for bucket routing
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Hyperliquid adapter.

        Args:
            api_base_url: Optional custom API base URL (default from config)
            base_currency_list: List of MVP base currencies from config (defaults to None, uses all if not provided)
            mvp_only: If True, only include MVP coins (default: True)
            chain: Chain identifier (default: 'HYPERLIQUID')
            api_key: Optional API key (not used by Hyperliquid but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "HYPERLIQUID"
        from instruments_service.config import instruments_config

        self.api_base_url = api_base_url or instruments_config.hyperliquid_api_url
        self.mvp_only = mvp_only
        # Use provided base_currency_list or empty set (no filtering)
        self.mvp_base_currencies = {c.upper() for c in base_currency_list} if base_currency_list else set()
        logger.info(
            f"✅ HyperliquidAdapter initialized (MVP only: {mvp_only}, base currencies: {len(self.mvp_base_currencies) if self.mvp_base_currencies else 'all'})"
        )

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Hyperliquid.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_perpetuals()
        instruments.update(self.fetch_spot_pairs())
        return list(instruments.values())

    def fetch_perpetuals(
        self, test_data_availability: bool = False, target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all perpetual futures from Hyperliquid.

        Args:
            test_data_availability: If True, test data availability and populate data_types
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before Hyperliquid launch
        if target_date and target_date < self.HYPERLIQUID_LAUNCH_DATE:
            logger.info(
                f"ℹ️ Hyperliquid not available for {target_date.strftime('%Y-%m-%d')} "
                f"(Hyperliquid launched December 2022). Returning empty instruments - this is expected."
            )
            return {}

        try:
            # Check cache first (instrument lists rarely change)
            cache_key = "hyperliquid_perp_meta"
            cached_metadata = _get_cached_response(cache_key)

            if cached_metadata is not None:
                logger.info("📋 Using cached Hyperliquid perpetual metadata")
                metadata = cached_metadata
            else:
                # Get perpetual metadata (use "meta" not "perpetualMetadata")
                response = requests.post(
                    f"{self.api_base_url}/info",
                    json={"type": "meta"},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                metadata = response.json()
                _set_cached_response(cache_key, metadata)

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
                        # Hyperliquid supports 1m candles, funding rates (derivative_ticker), and liquidations
                        inst_def["data_types"] = "ohlcv_1m,derivative_ticker,liquidations"

                        instruments[inst_def["instrument_key"]] = inst_def
                except Exception as e:
                    logger.warning(f"Failed to convert asset {asset.get('name', 'unknown')}: {e}")
                    continue

            logger.info(f"✅ Generated {len(instruments)} Hyperliquid perpetual instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Hyperliquid perpetuals: {e}")
            return {}

    def fetch_spot_pairs(
        self, test_data_availability: bool = False, target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all spot trading pairs from Hyperliquid for MVP coins.

        Args:
            test_data_availability: If True, test data availability and populate data_types
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before Hyperliquid launch
        if target_date and target_date < self.HYPERLIQUID_LAUNCH_DATE:
            logger.info(
                f"ℹ️ Hyperliquid not available for {target_date.strftime('%Y-%m-%d')} "
                f"(Hyperliquid launched December 2022). Returning empty instruments - this is expected."
            )
            return {}

        try:
            # Check cache first (reuses same metadata as perpetuals)
            cache_key = "hyperliquid_perp_meta"
            cached_metadata = _get_cached_response(cache_key)

            if cached_metadata is not None:
                logger.debug("📋 Using cached Hyperliquid metadata for spot pairs")
                metadata = cached_metadata
            else:
                # Get perpetual metadata (same endpoint, but we'll create SPOT_PAIR instruments)
                response = requests.post(
                    f"{self.api_base_url}/info",
                    json={"type": "meta"},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                metadata = response.json()
                _set_cached_response(cache_key, metadata)

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
                    logger.warning(f"Failed to convert asset {asset.get('name', 'unknown')} to spot pair: {e}")
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
            "chain": "off-chain",  # CEFI classification for bucket routing
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
        asset.get("szDecimals", 8)
        max_leverage = asset.get("maxLeverage", {})
        (
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
            "chain": "off-chain",  # CEFI classification for bucket routing
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
