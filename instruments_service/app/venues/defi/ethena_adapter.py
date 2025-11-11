"""
Ethena Adapter

Fetches sUSDe (Ethena synthetic dollar) instrument definitions.
Generates canonical instrument keys for Ethena positions.

Reference: archive/basis-strategy-v1/docs/SCRIPTS_DATA_GUIDE.md
Ethena launch date: February 2024 (first benchmark data: 2024-02-16)
"""

import logging
from typing import Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class EthenaAdapter:
    """
    Adapter for fetching Ethena sUSDe instrument definitions.

    Generates instruments in format:
    ETHENA:SPOT_ASSET:SUSDE@ETHEREUM

    Note: sUSDe is Ethena's synthetic dollar product, a yield-bearing stablecoin
    backed by staked ETH collateral and delta-neutral derivatives positions.
    """

    def __init__(self, chain: str = "ETHEREUM"):
        """
        Initialize Ethena adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
        """
        self.chain = chain.upper()
        self.venue = "ETHENA"
        logger.info(f"✅ EthenaAdapter initialized for chain: {self.chain}")

    def fetch_instruments(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Ethena sUSDe instrument definitions.

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        instruments = {}

        # sUSDe token information
        # Contract address: sUSDe token on Ethereum mainnet
        # Reference: Ethena protocol documentation and on-chain data
        susde_token = {
            "symbol": "SUSDE",
            "contract_address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # Ethereum mainnet sUSDe
            "underlying": "USDE",  # Ethena's base synthetic dollar
            "collateral": "ETH",  # Backed by staked ETH
        }

        try:
            inst_def = self._create_susde_instrument(susde_token)
            if inst_def:
                instruments[inst_def["instrument_key"]] = inst_def
        except Exception as e:
            logger.warning(f"Failed to create Ethena instrument for sUSDe: {e}")

        logger.info(f"✅ Generated {len(instruments)} Ethena instruments")
        return instruments

    def _create_susde_instrument(self, token: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create sUSDe instrument definition.

        Args:
            token: Token data dictionary

        Returns:
            Instrument definition dictionary
        """
        symbol = token["symbol"]
        contract_address = token["contract_address"]
        underlying = token["underlying"]

        # Build canonical instrument key
        # Format: VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN
        # sUSDe is a SPOT_ASSET (yield-bearing stablecoin position)
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:SPOT_ASSET:{symbol}{chain_suffix}"

        # Ethena launched in February 2024
        # First benchmark data available: 2024-02-16
        # Use conservative launch date: 2024-02-16
        available_from = datetime(2024, 2, 16).isoformat()

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "SPOT_ASSET",
            "symbol": symbol,
            "base_asset": underlying,  # USDE (Ethena's synthetic dollar)
            "quote_asset": "",  # SPOT_ASSET doesn't have quote asset
            "settle_asset": underlying,  # Redeemable for USDE
            "base_asset_contract_address": contract_address,
            "quote_asset_contract_address": None,
            "pool_address": None,  # Not a pool, it's a token
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "protocol_sdk",  # Per config.py
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # No expiry
            "data_types": "trades,book_snapshot_5",  # sUSDe trades on DEXes
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": underlying,
        }
