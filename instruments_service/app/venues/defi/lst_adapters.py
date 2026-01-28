"""
EtherFi and Lido Adapters

Fetches LST (Liquid Staking Token) instruments from protocol SDKs or The Graph.
Generates canonical instrument keys for staking positions.

Reference: instruments-service/docs/MVP_INSTRUMENTS.md (DeFi section)
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from unified_cloud_services import handle_api_errors
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)


class EtherFiAdapter(BaseDefiAdapter):
    """
    Adapter for fetching EtherFi LST instruments.

    Generates instruments in format:
    ETHERFI:LST:WEETH@ETHEREUM
    """

    # EtherFi mainnet launch (weETH launched ~January 2024)
    ETHERFI_LAUNCH_DATE = datetime(2024, 1, 1)

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize EtherFi adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            api_key: Optional API key (not used by EtherFi but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "ETHERFI"
        logger.info(f"✅ EtherFiAdapter initialized for chain: {self.chain}")

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for EtherFi.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_lst_instruments()
        return list(instruments.values())

    def fetch_lst_instruments(
        self,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch EtherFi LST instruments.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before EtherFi launch
        if target_date and target_date < self.ETHERFI_LAUNCH_DATE:
            logger.info(
                f"ℹ️ EtherFi weETH not available for {target_date.strftime('%Y-%m-%d')} "
                f"(EtherFi weETH launched January 2024). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        # Known EtherFi LST tokens
        lst_tokens = [
            {
                "symbol": "WEETH",
                "contract_address": "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee",  # Ethereum mainnet
                "underlying": "ETH",
            }
        ]

        for token in lst_tokens:
            try:
                inst_def = self._create_lst_instrument(token)
                if inst_def:
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to create EtherFi instrument for {token['symbol']}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} EtherFi instruments")
        return instruments

    def _create_lst_instrument(self, token: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create LST instrument definition.

        Args:
            token: Token data

        Returns:
            Instrument definition dictionary
        """
        symbol = token["symbol"]
        contract_address = token["contract_address"]
        underlying = token["underlying"]

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:LST:{symbol}{chain_suffix}"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "LST",
            "symbol": symbol,
            "base_asset": underlying,
            "quote_asset": underlying,  # weETH is quoted in ETH (exchange rate)
            "settle_asset": underlying,  # Redeemable for ETH
            "base_asset_contract_address": "0x0000000000000000000000000000000000000000",  # ETH native
            "quote_asset_contract_address": contract_address,
            "pool_address": contract_address,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "etherfi",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 2, 1
            ).isoformat(),  # EtherFi weETH launch date (Feb 2024)
            "available_to_datetime": None,
            "data_types": "yields,oracle_prices",  # LST APY from DefiLlama + oracle price
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": underlying,
        }


class LidoAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Lido LST instruments.

    Generates instruments in format:
    LIDO:LST:STETH@ETHEREUM
    LIDO:LST:WSTETH@ETHEREUM
    """

    # Lido mainnet launch (December 2020)
    LIDO_LAUNCH_DATE = datetime(2020, 12, 17)

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Lido adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            api_key: Optional API key (not used by Lido but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "LIDO"
        logger.info(f"✅ LidoAdapter initialized for chain: {self.chain}")

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Lido.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_lst_instruments()
        return list(instruments.values())

    def fetch_lst_instruments(
        self,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Lido LST instruments.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Lido launched in December 2020, so it's available for most historical dates
        if target_date and target_date < self.LIDO_LAUNCH_DATE:
            logger.info(
                f"ℹ️ Lido not available for {target_date.strftime('%Y-%m-%d')} "
                f"(Lido launched December 2020). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        # Known Lido LST tokens
        lst_tokens = [
            {
                "symbol": "STETH",
                "contract_address": "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",  # Ethereum mainnet
                "underlying": "ETH",
                "wrapped": False,
            },
            {
                "symbol": "WSTETH",
                "contract_address": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",  # Ethereum mainnet
                "underlying": "ETH",
                "wrapped": True,
            },
        ]

        for token in lst_tokens:
            try:
                inst_def = self._create_lst_instrument(token)
                if inst_def:
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to create Lido instrument for {token['symbol']}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Lido instruments")
        return instruments

    def _create_lst_instrument(self, token: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create LST instrument definition.

        Args:
            token: Token data

        Returns:
            Instrument definition dictionary
        """
        symbol = token["symbol"]
        contract_address = token["contract_address"]
        underlying = token["underlying"]

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:LST:{symbol}{chain_suffix}"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "LST",
            "symbol": symbol,
            "base_asset": underlying,
            "quote_asset": underlying,  # stETH/wstETH is quoted in ETH (exchange rate)
            "settle_asset": underlying,
            "base_asset_contract_address": "0x0000000000000000000000000000000000000000",  # ETH native
            "quote_asset_contract_address": contract_address,
            "pool_address": contract_address,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "lido",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2020, 12, 18
            ).isoformat(),  # Lido stETH launch date (Dec 18, 2020)
            "available_to_datetime": None,
            "data_types": "yields,oracle_prices",  # LST APY from DefiLlama + oracle price
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": underlying,
        }
