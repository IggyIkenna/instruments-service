"""
Ethena Adapter

Fetches Ethena sUSDe/USDe instruments for yield calculation.
Uses Aave oracle price feed for sUSDe to calculate staking yield from conversion rate changes.

Reference: https://docs.ethena.fi/
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)


class EthenaAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Ethena sUSDe/USDe instruments.

    Ethena is a synthetic dollar protocol:
    - USDe: Synthetic dollar (delta-neutral stablecoin)
    - sUSDe: Staked USDe (accrues yield from funding rate arbitrage)

    Yield calculation:
    - sUSDe/USDe exchange rate increases over time as yield accrues
    - We track the oracle price of sUSDe from Aave to calculate APY
    - APY = (sUSDe_price_t2 / sUSDe_price_t1 - 1) * (365 / days_diff)

    Generates instruments in format:
    ETHENA:YIELD_BEARING:SUSDE@ETHEREUM
    """

    # Contract addresses on Ethereum mainnet
    SUSDE_ADDRESS = "0x9d39a5de30e57443bff2a8307a4256c8797a3497"  # sUSDe token
    USDE_ADDRESS = "0x4c9edd5852cd905f086c759e8383e09bff1e68b3"   # USDe token

    # Ethena mainnet launch date (February 2024)
    ETHENA_LAUNCH_DATE = datetime(2024, 2, 16)

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Ethena adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            api_key: Optional API key (not used by Ethena but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "ETHENA"
        logger.info(f"✅ EthenaAdapter initialized for chain: {self.chain}")

    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Ethena.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_yield_bearing_instruments()
        return list(instruments.values())

    def fetch_yield_bearing_instruments(
        self,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Ethena yield-bearing instruments.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before Ethena launch
        if target_date and target_date < self.ETHENA_LAUNCH_DATE:
            logger.info(
                f"ℹ️ Ethena not available for {target_date.strftime('%Y-%m-%d')} "
                f"(Ethena mainnet launched February 2024). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        # sUSDe - Staked USDe (yield-bearing)
        # This is the primary instrument for tracking Ethena yield
        susde_instrument = self._create_yield_bearing_instrument(
            symbol="SUSDE",
            contract_address=self.SUSDE_ADDRESS,
            underlying="USDE",
            underlying_address=self.USDE_ADDRESS,
        )
        if susde_instrument:
            instruments[susde_instrument["instrument_key"]] = susde_instrument

        logger.info(f"✅ Generated {len(instruments)} Ethena instruments")
        return instruments

    def _create_yield_bearing_instrument(
        self,
        symbol: str,
        contract_address: str,
        underlying: str,
        underlying_address: str,
    ) -> Dict[str, Any]:
        """
        Create yield-bearing instrument definition.

        Args:
            symbol: Token symbol (e.g., 'SUSDE')
            contract_address: Token contract address
            underlying: Underlying asset symbol (e.g., 'USDE')
            underlying_address: Underlying asset contract address

        Returns:
            Instrument definition dictionary
        """
        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:YIELD_BEARING:{symbol}{chain_suffix}"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "YIELD_BEARING",  # New type for staked/wrapped yield tokens
            "symbol": symbol,
            "base_asset": underlying,  # USDe is the base
            "quote_asset": underlying,  # sUSDe is quoted in USDE (exchange rate)
            "settle_asset": underlying,  # Redeemable for USDe
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": contract_address,
            "pool_address": contract_address,  # sUSDe contract
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "ethena",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 2, 16
            ).isoformat(),  # Ethena mainnet launch (Feb 2024)
            "available_to_datetime": None,
            "data_types": "yields,oracle_prices",  # sUSDe APY from DefiLlama + oracle price
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": underlying,
        }

















