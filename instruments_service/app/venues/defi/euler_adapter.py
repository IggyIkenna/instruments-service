"""
Euler Adapter

Fetches Euler v2 (Plasma) lending market instruments.
Generates canonical instrument keys for Euler positions.

Euler v2 (branded "Euler Plasma") launched March 2024.
Reference: https://docs.euler.finance/

NOTE: Venue start dates are centralized in unified_cloud_services.models.VenueMapping.venue_start_dates
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from unified_cloud_services import get_secret_with_fallback, handle_api_errors, VenueMapping
from instruments_service.config import instruments_config
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)

# Centralized venue config
_venue_mapping = VenueMapping()

# Try to import web3 for contract interaction
try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    logger.warning("⚠️ web3 not available - Euler contract interaction disabled")


class EulerAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Euler v2 (Plasma) lending market instruments.

    Generates instruments in format:
    EULER-PLASMA:LENDING_MARKET:WETH-USDC@ETHEREUM
    """

    # Euler v2 launch date (Plasma rebranding)
    EULER_V2_LAUNCH = datetime(2024, 3, 1, tzinfo=timezone.utc)

    def __init__(
        self,
        chain: str = "ETHEREUM",
        rpc_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Euler adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            rpc_url: Optional RPC URL for contract interaction
            api_key: Optional API key (not used by Euler but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "EULER-PLASMA"

        # Initialize web3 provider for contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                if not rpc_url:
                    try:
                        alchemy_key = get_secret_with_fallback(
                            project_id=instruments_config.gcp_project_id,
                            secret_name=instruments_config.alchemy_secret_name,
                            fallback_env_var="ALCHEMY_API_KEY",
                        )
                        if alchemy_key:
                            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
                            logger.info("✅ Constructed Ethereum RPC URL from Alchemy API key")
                        else:
                            rpc_url = instruments_config.ethereum_rpc_url
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get RPC URL from Secret Manager: {e}")
                        rpc_url = instruments_config.ethereum_rpc_url

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info("✅ Connected to Ethereum RPC for Euler queries")
                    else:
                        self.web3 = None
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize web3: {e}")

        logger.info(f"✅ EulerAdapter initialized for chain: {self.chain}")

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """Get instrument metadata for Euler."""
        instruments = self.fetch_markets()
        return list(instruments.values())

    def fetch_markets(
        self,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Euler markets and convert to instrument definitions.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before venue launch
        if target_date and not _venue_mapping.is_venue_available_on_date(self.venue, target_date):
            start_date = _venue_mapping.get_venue_start_date(self.venue)
            logger.info(
                f"ℹ️ {self.venue} not available for {target_date.strftime('%Y-%m-%d')} "
                f"(launched {start_date}). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        try:
            # Fetch markets from Euler (using known markets for MVP)
            markets = self._get_mvp_markets()

            for market in markets:
                try:
                    instrument_key = self._build_instrument_key(
                        venue=self.venue,
                        instrument_type="LENDING_MARKET",
                        symbol=f"{market['collateral_asset']}-{market['borrow_asset']}"
                    )

                    symbol = f"{market['collateral_asset']}-{market['borrow_asset']}"
                    instruments[instrument_key] = {
                        "instrument_key": instrument_key,
                        "venue": self.venue,
                        "instrument_type": "LENDING_MARKET",
                        "symbol": symbol,
                        "base_asset": market["collateral_asset"],
                        "quote_asset": market["borrow_asset"],
                        "collateral_asset": market["collateral_asset"],
                        "borrow_asset": market["borrow_asset"],
                        "chain": self.chain,
                        "available_from_datetime": self.EULER_V2_LAUNCH.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "data_types": "utilization,rate_indices,oracle_prices",
                        "market_address": market.get("market_address"),
                        # Required fields for market-tick-data-handler routing
                        "asset_class": "crypto",
                        "venue_type": "protocol",
                        "data_provider": "euler_rpc",  # Uses RPC for on-chain data
                        "tardis_exchange": "",
                        "tardis_symbol": "",
                        "exchange_raw_symbol": market.get("market_address") or "",
                    }

                except Exception as e:
                    logger.warning(f"⚠️ Failed to process Euler market: {e}")
                    continue

            logger.info(f"✅ Fetched {len(instruments)} Euler v2 markets")

        except Exception as e:
            logger.error(f"❌ Failed to fetch Euler markets: {e}")

        return instruments

    def _get_mvp_markets(self) -> List[Dict[str, Any]]:
        """
        Get MVP markets for Euler v2 (Plasma).

        Returns curated list of high-liquidity Euler v2 markets.
        """
        # Major Euler v2 lending markets on Ethereum
        # These are the most liquid markets on Euler v2
        mvp_markets = [
            {
                "collateral_asset": "WETH",
                "borrow_asset": "USDC",
                "market_address": None,  # TODO: Add actual contract address
            },
            {
                "collateral_asset": "WETH",
                "borrow_asset": "USDT",
                "market_address": None,
            },
            {
                "collateral_asset": "WSTETH",
                "borrow_asset": "WETH",
                "market_address": None,
            },
            {
                "collateral_asset": "WSTETH",
                "borrow_asset": "USDC",
                "market_address": None,
            },
            {
                "collateral_asset": "WBTC",
                "borrow_asset": "USDC",
                "market_address": None,
            },
            {
                "collateral_asset": "WBTC",
                "borrow_asset": "USDT",
                "market_address": None,
            },
            {
                "collateral_asset": "WEETH",
                "borrow_asset": "WETH",
                "market_address": None,
            },
            {
                "collateral_asset": "CBETH",
                "borrow_asset": "WETH",
                "market_address": None,
            },
        ]

        return mvp_markets
