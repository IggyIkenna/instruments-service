"""
AAVE V3 Adapter

Fetches AAVE V3 market instruments (aTokens, debtTokens) using AaveScan API or AAVE SDK.
Generates canonical instrument keys for AAVE positions.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class AaveV3Adapter:
    """
    Adapter for fetching AAVE V3 market instruments.

    Generates instruments in format:
    AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM
    AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize AAVE V3 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM', 'POLYGON')
            api_key: AaveScan API key (optional, uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        self.chain = chain.upper()
        self.venue = f"AAVE_V3_{chain.upper()}"

        # Try provided API key first
        self.api_key = api_key

        # If not provided, try Secret Manager
        if not self.api_key:
            try:
                from unified_cloud_services import get_secret_with_fallback

                secret_name = os.getenv("AAVESCAN_SECRET_NAME", "aavescan-api-key")
                project_id = project_id or os.getenv(
                    "GCP_PROJECT_ID", "central-element-323112"
                )

                self.api_key = get_secret_with_fallback(
                    project_id=project_id,
                    secret_name=secret_name,
                    fallback_env_var="AAVESCAN_API_KEY",
                )

                if self.api_key:
                    logger.info(
                        f"✅ Retrieved AaveScan API key from Secret Manager (secret: {secret_name})"
                    )
            except ImportError:
                logger.warning(
                    "unified-cloud-services not available, falling back to env var"
                )
                self.api_key = os.getenv("AAVESCAN_API_KEY")
            except Exception as e:
                logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")
                self.api_key = os.getenv("AAVESCAN_API_KEY")

        if not self.api_key:
            logger.warning("AaveScan API key not found. Some features may be limited.")

        self.base_url = (
            "https://api.aavescan.com"
            if self.chain == "ETHEREUM"
            else f"https://{chain.lower()}.aavescan.com"
        )
        logger.info(f"✅ AaveV3Adapter initialized for chain: {self.chain}")

    def fetch_markets(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch AAVE V3 markets and convert to instrument definitions.

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        instruments = {}

        try:
            # Fetch reserves from AaveScan API
            reserves = self._fetch_reserves()

            for reserve in reserves:
                try:
                    # Generate aToken instrument
                    a_token_def = self._create_a_token_instrument(reserve)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    # Generate debtToken instrument
                    debt_token_def = self._create_debt_token_instrument(reserve)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(
                        f"Failed to process reserve {reserve.get('symbol')}: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} AAVE V3 instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch AAVE markets: {e}")
            return {}

    def _fetch_reserves(self) -> List[Dict[str, Any]]:
        """
        Fetch reserves from AaveScan API.

        Returns:
            List of reserve dictionaries
        """
        try:
            url = f"{self.base_url}/api/v3/reserves"
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            reserves = data.get("data", []) if isinstance(data, dict) else data

            logger.info(f"✅ Fetched {len(reserves)} reserves from AaveScan")
            return reserves

        except Exception as e:
            logger.error(f"Failed to fetch reserves from AaveScan: {e}")
            # Fallback: return empty list or use hardcoded known reserves
            return self._get_fallback_reserves()

    def _get_fallback_reserves(self) -> List[Dict[str, Any]]:
        """
        Fallback reserves if API fails.

        Returns:
            List of known reserve dictionaries
        """
        # Known AAVE V3 Ethereum reserves
        return [
            {
                "symbol": "USDT",
                "underlyingAsset": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "aToken": {"address": "0x3Ed3B47Dd13EC9a98b44e6204A523E766B225811"},
                "variableDebtToken": {
                    "address": "0x531842cEbbdD378f8ee36D171d6cC9C4fcf475Ec"
                },
            },
            {
                "symbol": "WETH",
                "underlyingAsset": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "aToken": {"address": "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8"},
                "variableDebtToken": {
                    "address": "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE"
                },
            },
        ]

    def _create_a_token_instrument(
        self, reserve: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create aToken instrument definition.

        Args:
            reserve: Reserve data from AaveScan

        Returns:
            Instrument definition dictionary or None
        """
        symbol = reserve.get("symbol", "")
        underlying_address = reserve.get("underlyingAsset", "")
        a_token_address = reserve.get("aToken", {}).get("address", "")

        if not symbol or not a_token_address:
            return None

        # Build canonical instrument key
        a_token_symbol = f"A{symbol}"  # e.g., AUSDT, AWETH
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "A_TOKEN",
            "symbol": a_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",  # aTokens don't have quote
            "settle_asset": symbol,  # Redeemable for underlying
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": a_token_address,  # aToken contract address
            "pool_fee_tier": None,
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aavescan",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime.now().isoformat(),  # TODO: Get actual launch date
            "available_to_datetime": None,
            "data_types": "",  # Protocol positions don't have market data
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
        }

    def _create_debt_token_instrument(
        self, reserve: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create debtToken instrument definition.

        Args:
            reserve: Reserve data from AaveScan

        Returns:
            Instrument definition dictionary or None
        """
        symbol = reserve.get("symbol", "")
        underlying_address = reserve.get("underlyingAsset", "")
        debt_token_address = reserve.get("variableDebtToken", {}).get("address", "")

        if not symbol or not debt_token_address:
            return None

        # Build canonical instrument key
        debt_token_symbol = f"DEBT{symbol}"  # e.g., DEBTWETH, DEBTUSDT
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "DEBT_TOKEN",
            "symbol": debt_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,  # Must repay with underlying
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address,  # debtToken contract address
            "pool_fee_tier": None,
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aavescan",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime.now().isoformat(),  # TODO: Get actual launch date
            "available_to_datetime": None,
            "data_types": "",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
        }
