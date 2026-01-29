"""
Morpho Adapter

Fetches Morpho lending market instruments using Morpho SDK or The Graph.
Generates canonical instrument keys for Morpho positions.

Reference: instruments-service/docs/MVP_INSTRUMENTS.md (DeFi section)

NOTE: Venue start dates are centralized in unified_cloud_services.models.VenueMapping.venue_start_dates
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
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
    logger.warning("⚠️ web3 not available - Morpho contract interaction disabled")


class MorphoAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Morpho lending market instruments.

    Generates instruments in format:
    MORPHO-ETHEREUM:A_TOKEN:AUSDT@ETHEREUM
    MORPHO-ETHEREUM:DEBT_TOKEN:DEBTWETH@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        rpc_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Morpho adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            rpc_url: Optional RPC URL for contract interaction (defaults to env var or Secret Manager)
            api_key: Optional API key (not used by Morpho but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "MORPHO-ETHEREUM"  # Per config.py

        # Initialize web3 provider for contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                # Get RPC URL from parameter, env var, or Secret Manager
                if not rpc_url:
                    try:
                        # Try to get Alchemy API key from Secret Manager
                        alchemy_key = get_secret_with_fallback(
                            project_id=instruments_config.gcp_project_id,
                            secret_name=instruments_config.alchemy_secret_name,
                            fallback_env_var="ALCHEMY_API_KEY",
                        )
                        if alchemy_key:
                            # Construct Alchemy RPC URL from API key
                            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
                            logger.info("✅ Constructed Ethereum RPC URL from Alchemy API key")
                        else:
                            # Fallback to direct RPC URL from env var
                            rpc_url = instruments_config.ethereum_rpc_url
                            if rpc_url:
                                logger.info("✅ Using Ethereum RPC URL from environment variable")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get RPC URL from Secret Manager: {e}")
                        rpc_url = instruments_config.ethereum_rpc_url

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info(f"✅ Connected to Ethereum RPC: {rpc_url[:50]}...")
                    else:
                        logger.warning("⚠️ Failed to connect to Ethereum RPC")
                        self.web3 = None
                else:
                    logger.warning("⚠️ No RPC URL provided - Morpho contract interaction disabled")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize web3: {e}")

        # Morpho Blue contract addresses (Ethereum mainnet)
        # Morpho Blue: 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb
        # MarketParams are stored in Morpho Blue contract
        self.morpho_blue_address = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

        # Cache for market configurations
        self._market_config_cache: Dict[str, Dict[str, Any]] = {}

        logger.info(f"✅ MorphoAdapter initialized for chain: {self.chain}")

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Morpho.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_markets()
        return list(instruments.values())

    def fetch_markets(
        self,
        target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Morpho markets and convert to instrument definitions.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before venue launch (using centralized config)
        if target_date and not _venue_mapping.is_venue_available_on_date(self.venue, target_date):
            start_date = _venue_mapping.get_venue_start_date(self.venue)
            logger.info(
                f"ℹ️ {self.venue} not available for {target_date.strftime('%Y-%m-%d')} "
                f"(launched {start_date}). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        try:
            # Fetch markets from Morpho (using known markets for MVP)
            # TODO: Integrate with Morpho SDK or The Graph when available
            markets = self._get_mvp_markets()

            for market in markets:
                try:
                    # Generate aToken instrument (supply position)
                    a_token_def = self._create_a_token_instrument(market)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    # Generate debtToken instrument (borrow position)
                    debt_token_def = self._create_debt_token_instrument(market)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(f"Failed to process Morpho market {market.get('symbol')}: {e}")
                    continue

            logger.info(f"✅ Generated {len(instruments)} Morpho instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Morpho markets: {e}")
            return {}

    def _get_mvp_markets(self) -> List[Dict[str, Any]]:
        """
        Get MVP markets for Morpho (known markets for MVP).

        Returns:
            List of market dictionaries
        """
        # MVP markets based on MVP_DEFI_INSTRUMENTS.md
        # Morpho is used for flash loans (WETH) and lending
        return [
            {
                "symbol": "WETH",
                "underlyingAsset": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "aToken": {"address": ""},  # TODO: Get actual Morpho market addresses
                "variableDebtToken": {"address": ""},
            },
            {
                "symbol": "USDT",
                "underlyingAsset": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
        ]

    def _fetch_market_params_from_contract(self, market_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch MarketParams from Morpho Blue contract.

        Args:
            market_id: Market ID (can be market address or market ID hash)

        Returns:
            MarketParams dictionary with lltv, liquidationIncentiveFactor, etc.
        """
        if not self.web3 or not WEB3_AVAILABLE:
            return None

        if market_id in self._market_config_cache:
            return self._market_config_cache[market_id]

        try:
            # Morpho Blue contract ABI (simplified - just the functions we need)
            # MarketParams struct: (loanToken, collateralToken, oracle, irm, lltv)
            # liquidationIncentiveFactor is calculated from lltv
            morpho_blue_abi = [
                {
                    "inputs": [{"internalType": "bytes32", "name": "id", "type": "bytes32"}],
                    "name": "idToMarketParams",
                    "outputs": [
                        {
                            "internalType": "address",
                            "name": "loanToken",
                            "type": "address",
                        },
                        {
                            "internalType": "address",
                            "name": "collateralToken",
                            "type": "address",
                        },
                        {
                            "internalType": "address",
                            "name": "oracle",
                            "type": "address",
                        },
                        {"internalType": "address", "name": "irm", "type": "address"},
                        {"internalType": "uint256", "name": "lltv", "type": "uint256"},
                    ],
                    "stateMutability": "view",
                    "type": "function",
                }
            ]

            contract = self.web3.eth.contract(
                address=Web3.to_checksum_address(self.morpho_blue_address),
                abi=morpho_blue_abi,
            )

            # Convert market_id to bytes32 if it's a hex string
            if isinstance(market_id, str) and market_id.startswith("0x"):
                market_id_bytes = bytes.fromhex(market_id[2:])
                if len(market_id_bytes) == 32:
                    market_params = contract.functions.idToMarketParams(market_id_bytes).call()

                    # Extract values
                    loan_token = market_params[0]
                    collateral_token = market_params[1]
                    oracle = market_params[2]
                    irm = market_params[3]
                    lltv = market_params[4]

                    # Convert lltv from WAD (1e18) to decimal
                    # lltv is loan-to-liquidation-threshold-value (e.g., 945000000000000000 = 94.5%)
                    lltv_decimal = float(lltv) / 1e18 if lltv else None

                    # Liquidation threshold = 100% (1.0) since lltv is the max LTV
                    # Liquidation bonus = (1 / lltv) - 1
                    liquidation_threshold = 1.0  # Morpho uses lltv as max LTV
                    liquidation_bonus = (
                        (1.0 / lltv_decimal - 1.0) if lltv_decimal and lltv_decimal > 0 else None
                    )

                    config = {
                        "ltv": lltv_decimal,
                        "liquidation_threshold": liquidation_threshold,
                        "liquidation_bonus": liquidation_bonus,
                        "loan_token": loan_token,
                        "collateral_token": collateral_token,
                        "oracle": oracle,
                        "irm": irm,
                    }

                    self._market_config_cache[market_id] = config
                    return config

            return None

        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch Morpho market params from contract: {e}")
            return None

    def _extract_lending_metadata(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from market data.

        Fetches data from:
        - Morpho Blue contract (MarketParams: lltv, liquidationIncentiveFactor)
        - Interest Rate Model contract (if available)

        Args:
            market: Market data

        Returns:
            Dictionary with lending protocol metadata fields
        """
        # Morpho Blue contract address (flash loan provider)
        flash_loan_providers = self.morpho_blue_address if self.web3 else None

        # Try to fetch market params from contract if we have market_id
        market_id = market.get("marketId") or market.get("id")
        market_params = None
        if market_id and self.web3:
            market_params = self._fetch_market_params_from_contract(market_id)

        # Extract values from market_params if available
        ltv = market_params.get("ltv") if market_params else None
        liquidation_threshold = (
            market_params.get("liquidation_threshold") if market_params else None
        )
        liquidation_bonus = market_params.get("liquidation_bonus") if market_params else None

        return {
            "flash_loan_providers": flash_loan_providers,
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": ltv,
            "liquidation_threshold": liquidation_threshold,
            "liquidation_bonus": liquidation_bonus,
            "reserve_factor": None,  # Not applicable to Morpho (no reserve factor)
            "emode_category_id": None,  # Not applicable to Morpho
            "emode_label": None,  # Not applicable to Morpho
            "emode_underlying": None,  # Not applicable to Morpho
            "emode_liquidation_threshold": None,  # Not applicable to Morpho
            "emode_liquidation_bonus": None,  # Not applicable to Morpho
            "optimal_utilization_rate": None,  # TODO: Fetch from IRM contract
            "base_variable_borrow_rate": None,  # TODO: Fetch from IRM contract
            "variable_rate_slope1": None,  # TODO: Fetch from IRM contract
            "variable_rate_slope2": None,  # TODO: Fetch from IRM contract
        }

    def _create_a_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create aToken instrument definition (supply position).

        Args:
            market: Market data

        Returns:
            Instrument definition dictionary or None
        """
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        a_token_address = market.get("aToken", {}).get("address", "")

        if not symbol:
            return None

        # Build canonical instrument key
        a_token_symbol = f"A{symbol}"  # e.g., AWETH, AUSDT
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "A_TOKEN",
            "symbol": a_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": a_token_address if a_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "morpho",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2023, 6, 1
            ).isoformat(),  # Morpho launch date (June 2023)
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization",  # Raw data: supplyIndex, utilization rate
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }

    def _create_debt_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create debtToken instrument definition (borrow position).

        Args:
            market: Market data

        Returns:
            Instrument definition dictionary or None
        """
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        debt_token_address = market.get("variableDebtToken", {}).get("address", "")

        if not symbol:
            return None

        # Build canonical instrument key
        debt_token_symbol = f"DEBT{symbol}"  # e.g., DEBTWETH, DEBTUSDT
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "DEBT_TOKEN",
            "symbol": debt_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address if debt_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (ETHEREUM)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "morpho",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2023, 6, 1
            ).isoformat(),  # Morpho launch date (June 2023)
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization",  # Raw data: borrowIndex, utilization rate
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }
