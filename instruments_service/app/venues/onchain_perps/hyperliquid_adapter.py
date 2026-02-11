"""
Hyperliquid Adapter

Fetches Hyperliquid perpetual futures instruments and tests historical data availability.
Uses HyperliquidBaseClient from unified-cloud-services for network management.

Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from unified_cloud_services import HyperliquidBaseClient, HyperliquidClientConfig, handle_api_errors
from unified_cloud_services.clients.hyperliquid_base_client import (
    get_cached_response,
    set_cached_response,
)

from instruments_service.app.venues.onchain_perps.base_onchain_perp_adapter import BaseOnchainPerpAdapter

logger = logging.getLogger(__name__)


class HyperliquidAdapter(BaseOnchainPerpAdapter):
    """
    Adapter for fetching Hyperliquid perpetual futures instruments.

    Generates instruments in format:
    HYPERLIQUID:PERPETUAL:BTC-USDC
    """

    # Hyperliquid mainnet launch (December 2022)
    HYPERLIQUID_LAUNCH_DATE = datetime(2022, 12, 1, tzinfo=timezone.utc)

    def __init__(
        self,
        base_client: Optional[HyperliquidBaseClient] = None,
        base_currency_list: Optional[List[str]] = None,
        mvp_only: bool = True,
        chain: str = "off-chain",  # CEFI classification for bucket routing
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Hyperliquid adapter.

        Args:
            base_client: Optional HyperliquidBaseClient instance (creates default if not provided)
            base_currency_list: List of MVP base currencies from config (defaults to None, uses all if not provided)
            mvp_only: If True, only include MVP coins (default: True)
            chain: Chain identifier (default: 'HYPERLIQUID')
            api_key: Optional API key (not used by Hyperliquid but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(
            venue="HYPERLIQUID",
            chain=chain,
            api_url="https://api.hyperliquid.xyz",
            api_key=api_key,
            project_id=project_id,
        )
        self._base_client = base_client
        self._project_id = project_id
        self.mvp_only = mvp_only
        # Use provided base_currency_list or empty set (no filtering)
        self.mvp_base_currencies = {c.upper() for c in base_currency_list} if base_currency_list else set()
        logger.info(
            f"✅ HyperliquidAdapter initialized (MVP only: {mvp_only}, base currencies: {len(self.mvp_base_currencies) if self.mvp_base_currencies else 'all'})"
        )

    @property
    def client(self) -> HyperliquidBaseClient:
        """Lazy-load HyperliquidBaseClient (singleton pattern per adapter instance)."""
        if self._base_client is None:
            self._base_client = HyperliquidBaseClient(
                config=HyperliquidClientConfig.from_env(),
                project_id=self._project_id,
            )
            logger.debug("Lazy-loaded HyperliquidBaseClient")
        return self._base_client

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
            cached_metadata = get_cached_response(cache_key)

            if cached_metadata is not None:
                logger.info("📋 Using cached Hyperliquid perpetual metadata")
                metadata = cached_metadata
            else:
                # Get perpetual metadata (use "meta" not "perpetualMetadata")
                url = self.client.get_api_url("/info")
                response = self.client.sync_session.post(
                    url,
                    json={"type": "meta"},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                metadata = response.json()
                set_cached_response(cache_key, metadata)

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
                        # Data types matching Tardis schema for CLOB perps
                        # NOTE: liquidations NOT available from any source
                        inst_def["data_types"] = "trades,derivative_ticker,book_snapshot_5"

                        # Data source routing metadata (for variable sources per data type)
                        # This tells market-tick-data-handler which source to use for each date range
                        import json

                        inst_def["data_sources_metadata"] = json.dumps(
                            {
                                "trades": [
                                    {
                                        "source": "tardis",
                                        "start": "2024-10-29",
                                        "end": "2025-03-21",
                                        "notes": "Tardis.dev hyperliquid exchange",
                                    },
                                    {
                                        "source": "hyperliquid_s3",
                                        "start": "2025-03-22",
                                        "end": None,
                                        "notes": "hl-mainnet-node-data/node_fills bucket",
                                    },
                                ],
                                "derivative_ticker": [
                                    {
                                        "source": "hyperliquid_s3",
                                        "start": "2023-05-20",
                                        "end": None,
                                        "notes": "hyperliquid-archive/asset_ctxs bucket",
                                    }
                                ],
                                "book_snapshot_5": [
                                    {
                                        "source": "hyperliquid_s3",
                                        "start": "2023-04-15",
                                        "end": None,
                                        "notes": "hyperliquid-archive/market_data bucket",
                                    }
                                ],
                                "liquidations": None,  # NOT AVAILABLE from any source
                            }
                        )

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
            cached_metadata = get_cached_response(cache_key)

            if cached_metadata is not None:
                logger.debug("📋 Using cached Hyperliquid metadata for spot pairs")
                metadata = cached_metadata
            else:
                # Get perpetual metadata (same endpoint, but we'll create SPOT_PAIR instruments)
                url = self.client.get_api_url("/info")
                response = self.client.sync_session.post(
                    url,
                    json={"type": "meta"},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                metadata = response.json()
                set_cached_response(cache_key, metadata)

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
                        # Hyperliquid spot supports trades and order book snapshots
                        inst_def["data_types"] = "trades,book_snapshot_5"

                        # Data source routing metadata for spot (same as perps)
                        import json

                        inst_def["data_sources_metadata"] = json.dumps(
                            {
                                "trades": [
                                    {"source": "tardis", "start": "2024-10-29", "end": "2025-03-21"},
                                    {"source": "hyperliquid_s3", "start": "2025-03-22", "end": None},
                                ],
                                "book_snapshot_5": [{"source": "hyperliquid_s3", "start": "2023-04-15", "end": None}],
                            }
                        )

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
        available_from = datetime(2023, 5, 1, tzinfo=timezone.utc).isoformat()

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
        available_from = datetime(2023, 5, 1, tzinfo=timezone.utc).isoformat()

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

            url = self.client.get_api_url("/info")
            response = self.client.sync_session.post(
                url,
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
