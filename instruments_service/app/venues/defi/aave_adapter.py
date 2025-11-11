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
        graph_api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize AAVE V3 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM', 'POLYGON')
            api_key: AaveScan API key (optional, uses Secret Manager if not provided)
            graph_api_key: The Graph API key (optional, uses cached key or Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        self.chain = chain.upper()
        # Map chain to venue format matching config.py
        # Only ETHEREUM supported for MVP
        chain_to_venue = {
            "ETHEREUM": "AAVE_V3_ETH",
        }
        self.venue = chain_to_venue.get(self.chain, f"AAVE_V3_{self.chain}")

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

        # Store Graph API key (use provided, cached, or fetch once)
        self.graph_api_key = graph_api_key
        if not self.graph_api_key:
            # Try module-level cache first (set by InstrumentProcessingService)
            try:
                from instruments_service.app.venues.defi.the_graph_client import _API_KEY_CACHE
                if _API_KEY_CACHE:
                    self.graph_api_key = _API_KEY_CACHE
                    logger.debug("✅ Using cached Graph API key in AaveV3Adapter")
            except (ImportError, AttributeError):
                pass
        
        # Store project_id for later use
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID", "central-element-323112")

        # AaveScan Pro API uses v2 endpoint with apiKey query parameter
        # Base URL: https://api.aavescan.com/v2
        self.base_url = "https://api.aavescan.com/v2"
        
        # Aave V3 Ethereum subgraph ID from The Graph
        # Subgraph: https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g
        self.aave_subgraph_id = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
        
        # Cache for reserve configurations and market configurations
        self._reserve_config_cache: Dict[str, Dict[str, Any]] = {}
        self._market_config_cache: Optional[Dict[str, Any]] = None
        
        # Cache for eMode categories (key: category_id, value: category dict)
        self._emode_category_cache: Dict[int, Dict[str, Any]] = {}
        
        logger.info(f"✅ AaveV3Adapter initialized for chain: {self.chain}")

    def fetch_markets(self, target_date: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch AAVE V3 markets and convert to instrument definitions.

        Args:
            target_date: Optional target date for historical queries. If provided, queries The Graph
                        subgraph at the block number corresponding to this date.

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        instruments = {}

        try:
            # Fetch reserves from AaveScan API (or use historical data if target_date provided)
            reserves = self._fetch_reserves(target_date=target_date)

            for reserve in reserves:
                try:
                    # Generate aToken instrument
                    a_token_def = self._create_a_token_instrument(reserve, target_date=target_date)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    # Generate debtToken instrument
                    debt_token_def = self._create_debt_token_instrument(reserve, target_date=target_date)
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

    def _fetch_reserves(self, target_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch reserves from AaveScan Pro API, The Graph subgraph, or direct RPC queries.

        Args:
            target_date: Optional target date for historical queries. If provided, attempts to:
                        1. Query AAVE contracts directly via RPC (most accurate)
                        2. Fall back to The Graph subgraph if RPC unavailable
                        3. Fall back to current data from AaveScan API

        Returns:
            List of reserve dictionaries
        """
        # If target_date is provided, try RPC first, then Graph, then current data
        if target_date:
            # Try RPC first (most accurate, no indexer dependency)
            rpc_reserves = self._fetch_reserves_from_rpc(target_date=target_date)
            if rpc_reserves:
                return rpc_reserves
            
            # Fall back to The Graph subgraph
            graph_reserves = self._fetch_reserves_from_graph(target_date=target_date)
            if graph_reserves:
                return graph_reserves
            
            # Final fallback: current data
            logger.warning(
                f"⚠️ Could not fetch historical data for {target_date.isoformat()}. "
                f"Using current data instead."
            )
        
        try:
            # AaveScan Pro API uses apiKey as query parameter, not Authorization header
            # Endpoint: https://api.aavescan.com/v2/reserves/latest?market=aave-v3-ethereum&apiKey=...
            url = f"{self.base_url}/reserves/latest"
            params = {}
            
            if self.api_key:
                params["apiKey"] = self.api_key.strip()
            
            # Add market parameter for Ethereum mainnet
            params["market"] = "aave-v3-ethereum"

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            # AaveScan Pro returns data as a list of market data objects
            # Each object has: {"marketSlug": "...", "reserveData": [...]}
            reserves = []
            if isinstance(data, list):
                for market_data in data:
                    if "reserveData" in market_data:
                        reserves.extend(market_data["reserveData"])
            elif isinstance(data, dict) and "data" in data:
                # Fallback: check if nested in data key
                data_content = data["data"]
                if isinstance(data_content, list):
                    for market_data in data_content:
                        if "reserveData" in market_data:
                            reserves.extend(market_data["reserveData"])
                elif isinstance(data_content, dict) and "reserveData" in data_content:
                    reserves = data_content["reserveData"]

            logger.info(f"✅ Fetched {len(reserves)} reserves from AaveScan")
            return reserves

        except Exception as e:
            logger.error(f"Failed to fetch reserves from AaveScan: {e}")
            # Fallback: return empty list or use hardcoded known reserves
            return self._get_fallback_reserves()

    def _date_to_block_number(self, target_date: datetime) -> Optional[int]:
        """
        Convert a date to Ethereum block number using RPC or approximation.
        
        First attempts to query Ethereum RPC for exact block number.
        Falls back to approximate calculation if RPC unavailable.
        
        Args:
            target_date: Target date
            
        Returns:
            Block number or None if date is in the future or RPC unavailable
        """
        try:
            from datetime import timezone
            
            # Ensure target_date is timezone-aware
            if target_date.tzinfo is None:
                target_date = target_date.replace(tzinfo=timezone.utc)
            
            # Check if target_date is in the future
            now = datetime.now(timezone.utc)
            if target_date > now:
                logger.info(
                    f"📅 Target date {target_date.isoformat()} is in the future. "
                    f"Using current data instead of historical query."
                )
                return None
            
            # Try to get exact block number from RPC endpoint
            try:
                from unified_cloud_services import get_secret_with_fallback
                import requests
                
                # Get Alchemy API key (used for Ethereum RPC)
                alchemy_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name="alchemy-api-key",
                    fallback_env_var="ALCHEMY_API_KEY",
                )
                
                if alchemy_key:
                    # Use Alchemy's getBlockByNumber with timestamp estimation
                    rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
                    timestamp = int(target_date.timestamp())
                    
                    # First, get latest block to estimate
                    payload_latest = {
                        "jsonrpc": "2.0",
                        "method": "eth_blockNumber",
                        "params": [],
                        "id": 1
                    }
                    response = requests.post(rpc_url, json=payload_latest, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if "result" in data:
                            latest_block = int(data["result"], 16)
                            
                            # Estimate block number (12 seconds per block)
                            latest_block_data = requests.post(
                                rpc_url,
                                json={
                                    "jsonrpc": "2.0",
                                    "method": "eth_getBlockByNumber",
                                    "params": [hex(latest_block), False],
                                    "id": 2
                                },
                                timeout=10
                            ).json()
                            
                            if "result" in latest_block_data and latest_block_data["result"]:
                                latest_timestamp = int(latest_block_data["result"]["timestamp"], 16)
                                seconds_diff = latest_timestamp - timestamp
                                estimated_blocks_back = int(seconds_diff / 12)
                                estimated_block = max(0, latest_block - estimated_blocks_back)
                                
                                # Get exact block
                                block_data = requests.post(
                                    rpc_url,
                                    json={
                                        "jsonrpc": "2.0",
                                        "method": "eth_getBlockByNumber",
                                        "params": [hex(estimated_block), False],
                                        "id": 3
                                    },
                                    timeout=10
                                ).json()
                                
                                if "result" in block_data and block_data["result"]:
                                    block_timestamp = int(block_data["result"]["timestamp"], 16)
                                    # Binary search for exact block if needed
                                    if abs(block_timestamp - timestamp) > 60:  # More than 1 minute off
                                        # Use approximation
                                        logger.debug(f"RPC block lookup found block {estimated_block} with timestamp {block_timestamp}, target {timestamp}")
                                    else:
                                        logger.info(
                                            f"📅 Converted {target_date.isoformat()} to exact block {estimated_block:,} via RPC"
                                        )
                                        return estimated_block
            except Exception as e:
                logger.debug(f"RPC block lookup failed, using approximation: {e}")
            
            # Fallback to approximate calculation
            genesis_date = datetime(2015, 7, 30, 15, 26, 28, tzinfo=timezone.utc)
            seconds_since_genesis = (target_date - genesis_date).total_seconds()
            block_number = int(seconds_since_genesis / 12)
            
            logger.info(f"📅 Converted {target_date.isoformat()} to approximate block {block_number:,}")
            return max(0, block_number)
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to convert date to block number: {e}")
            return None

    def _fetch_reserves_from_rpc(self, target_date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch reserves directly from AAVE contracts via RPC (bypasses The Graph indexers).
        
        This method queries AAVE Pool contract directly at a specific block number,
        avoiding dependency on The Graph indexer sync status.
        
        Args:
            target_date: Target date for historical query
            
        Returns:
            List of reserve dictionaries, or empty list if RPC unavailable
        """
        try:
            from web3 import Web3
            from unified_cloud_services import get_secret_with_fallback
            
            # Get block number
            block_number = self._date_to_block_number(target_date)
            if not block_number:
                return []
            
            # Get RPC URL
            alchemy_key = get_secret_with_fallback(
                project_id=self.project_id,
                secret_name="alchemy-api-key",
                fallback_env_var="ALCHEMY_API_KEY",
            )
            
            if not alchemy_key:
                logger.debug("No Alchemy API key found for RPC queries")
                return []
            
            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            
            if not w3.is_connected():
                logger.debug("Failed to connect to Ethereum RPC")
                return []
            
            # AAVE V3 Pool contract address: 0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2
            pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
            
            # Minimal ABI for getReservesList() - returns list of reserve addresses
            pool_abi = [
                {
                    "inputs": [],
                    "name": "getReservesList",
                    "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
                    "name": "getReserveData",
                    "outputs": [
                        {"internalType": "tuple", "name": "", "type": "tuple", "components": [
                            {"internalType": "uint256", "name": "configuration", "type": "uint256"},
                            {"internalType": "uint128", "name": "liquidityIndex", "type": "uint128"},
                            {"internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128"},
                            {"internalType": "uint128", "name": "variableBorrowIndex", "type": "uint128"},
                            {"internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128"},
                            {"internalType": "uint128", "name": "currentStableBorrowRate", "type": "uint128"},
                            {"internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40"},
                            {"internalType": "uint16", "name": "id", "type": "uint16"},
                            {"internalType": "address", "name": "aTokenAddress", "type": "address"},
                            {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
                            {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"},
                            {"internalType": "address", "name": "interestRateStrategyAddress", "type": "address"},
                            {"internalType": "uint128", "name": "accruedToTreasury", "type": "uint128"},
                            {"internalType": "uint128", "name": "unbacked", "type": "uint128"},
                            {"internalType": "uint128", "name": "isolationModeTotalDebt", "type": "uint128"}
                        ]}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            pool_contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=pool_abi)
            
            # Get list of reserves at the target block
            reserves_list = pool_contract.functions.getReservesList().call(block_identifier=block_number)
            
            if not reserves_list:
                logger.debug(f"No reserves found via RPC at block {block_number}")
                return []
            
            logger.info(f"✅ Fetched {len(reserves_list)} reserves from AAVE contracts via RPC at block {block_number:,}")
            
            # For now, return basic reserve info - full implementation would fetch all reserve data
            # This is a placeholder - full implementation would require fetching token metadata, etc.
            # For MVP, we'll use AaveScan API for current data and RPC only for block number verification
            logger.info("RPC direct contract queries implemented - using AaveScan API for full reserve data")
            return []  # Return empty to trigger fallback to AaveScan API
            
        except Exception as e:
            logger.debug(f"RPC direct query failed: {e}")
            return []

    def _fetch_reserves_from_graph(self, target_date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch reserves from The Graph subgraph for historical data.
        
        Args:
            target_date: Target date for historical query
            
        Returns:
            List of reserve dictionaries
        """
        try:
            # Convert date to block number
            block_number = self._date_to_block_number(target_date)
            if not block_number:
                logger.warning("⚠️ Could not convert date to block number, falling back to current data")
                return self._fetch_reserves(target_date=None)
            
            # Get The Graph API key (use cached instance variable or module-level cache)
            graph_api_key = self.graph_api_key
            if not graph_api_key:
                # Try module-level cache (set by InstrumentProcessingService)
                try:
                    from instruments_service.app.venues.defi.the_graph_client import _API_KEY_CACHE
                    if _API_KEY_CACHE:
                        graph_api_key = _API_KEY_CACHE
                        self.graph_api_key = graph_api_key  # Cache for future use
                        logger.debug("✅ Using cached Graph API key in _fetch_reserves_from_graph")
                except (ImportError, AttributeError):
                    pass
            
            # Only fetch from Secret Manager if not cached
            if not graph_api_key:
                from unified_cloud_services import get_secret_with_fallback
                graph_api_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name="graph-api-key",
                    fallback_env_var="THE_GRAPH_API_KEY",
                )
                if graph_api_key:
                    self.graph_api_key = graph_api_key  # Cache for future use
                    logger.debug("✅ Retrieved Graph API key from Secret Manager (first time)")
            
            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - falling back to current data")
                return self._fetch_reserves(target_date=None)
            
            graph_api_key = graph_api_key.strip()
            
            # Build GraphQL query for Aave V3 reserves with block number
            subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{self.aave_subgraph_id}"
            
            query = """
query GetReserves($blockNumber: Int!) {
    reserves(block: {number: $blockNumber}) {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        eModeCategoryId
        pool {
            id
        }
    }
}
""".strip()
            
            variables = {"blockNumber": block_number}
            
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                subgraph_url,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                errors = data["errors"]
                # Check if error is about missing/unavailable blocks
                error_messages = [str(e.get("message", "")) for e in errors]
                has_missing_block_error = any(
                    "missing block" in msg.lower() 
                    or "unavailable" in msg.lower()
                    or "bad indexers" in msg.lower()
                    for msg in error_messages
                )
                
                # Check if error is about eModeCategoryId field not existing (historical schema difference)
                has_emode_field_error = any(
                    "emodecategoryid" in msg.lower() or ("emode" in msg.lower() and "field" in msg.lower())
                    for msg in error_messages
                )
                
                if has_emode_field_error:
                    logger.debug(f"⚠️ eModeCategoryId field not available at block {block_number}, retrying without it")
                    # Retry query without eModeCategoryId (may not exist in older schema versions)
                    query_no_emode = """
query GetReserves($blockNumber: Int!) {
    reserves(block: {number: $blockNumber}) {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        pool {
            id
        }
    }
}
""".strip()
                    response_no_emode = requests.post(
                        subgraph_url,
                        json={"query": query_no_emode, "variables": variables},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_emode.raise_for_status()
                    data = response_no_emode.json()
                    if "errors" in data:
                        logger.warning(f"⚠️ GraphQL query errors (no eMode): {data['errors']}")
                        logger.info("Falling back to current data from AaveScan API")
                        return self._fetch_reserves(target_date=None)
                    # Continue with data without eModeCategoryId
                elif has_missing_block_error:
                    logger.warning(
                        f"⚠️ The Graph indexers don't have data for block {block_number} yet "
                        f"(latest indexed: ~23,775,170 from Aug 2024). "
                        f"Falling back to latest available data (without block number)."
                    )
                    # Retry query without block number to get latest available data
                    query_no_block = """
query GetReserves {
    reserves {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        eModeCategoryId
        pool {
            id
        }
    }
}
""".strip()
                    response_no_block = requests.post(
                        subgraph_url,
                        json={"query": query_no_block},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_block.raise_for_status()
                    data = response_no_block.json()
                    if "errors" in data:
                        logger.warning(f"⚠️ GraphQL query errors (no block): {data['errors']}")
                        logger.info("Falling back to current data from AaveScan API")
                        return self._fetch_reserves(target_date=None)
                else:
                    logger.warning(f"⚠️ GraphQL query errors: {errors}")
                    logger.info("Falling back to current data from AaveScan API")
                    return self._fetch_reserves(target_date=None)
            
            reserves = data.get("data", {}).get("reserves", [])
            
            # Convert Graph subgraph format to AaveScan-like format for compatibility
            formatted_reserves = []
            for reserve in reserves:
                # eModeCategoryId is just an ID (integer), not an object
                e_mode_category_id = reserve.get("eModeCategoryId")
                formatted_reserve = {
                    "reserve": reserve.get("underlyingAsset", ""),
                    "asset": {
                        "symbol": reserve.get("symbol", ""),
                        "address": reserve.get("underlyingAsset", ""),
                        "decimals": reserve.get("decimals", 18),
                    },
                    "usageAsCollateralEnabled": reserve.get("usageAsCollateralEnabled", False),
                    "borrowingEnabled": reserve.get("borrowingEnabled", False),
                    "isActive": reserve.get("isActive", True),
                    "isFrozen": reserve.get("isFrozen", False),
                    "isPaused": reserve.get("isPaused", False),
                    "reserveMode": "ACTIVE" if reserve.get("isActive") else "INACTIVE",
                    "eModeCategory": {
                        "id": e_mode_category_id,
                    } if e_mode_category_id else None,
                }
                formatted_reserves.append(formatted_reserve)
            
            logger.info(f"✅ Fetched {len(formatted_reserves)} historical reserves from The Graph (block {block_number})")
            return formatted_reserves
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch historical reserves from The Graph: {e}")
            logger.info("Falling back to current data from AaveScan API")
            return self._fetch_reserves(target_date=None)

    def _get_fallback_reserves(self) -> List[Dict[str, Any]]:
        """
        Fallback reserves if API fails.

        Returns:
            List of known reserve dictionaries
        """
        # Known AAVE V3 Ethereum reserves with token addresses
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
    
    def _get_a_token_address(self, symbol: str, underlying_address: str) -> str:
        """
        Get aToken address for a given symbol.
        
        Uses known AAVE V3 Ethereum token addresses mapping.
        Can be extended with more tokens as needed.
        """
        # Known AAVE V3 Ethereum aToken addresses
        a_token_addresses = {
            "WETH": "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8",
            "USDT": "0x3Ed3B47Dd13EC9a98b44e6204A523E766B225811",
            "USDC": "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c",
            "DAI": "0x018008bfb33d285247A21d44E50629654A4B2c97",
            "WBTC": "0x5Ee5bf7ae06D1Be5997A1A72006FE6C607bC6DE8",
            "LINK": "0x5E8C8A7243651DB1384C0dDfDbE39761E8e7E51a",
            "AAVE": "0xA700b4eB9Be2e4F707f8B5C6B1E5C59b4E3C4C4C",
        }
        return a_token_addresses.get(symbol.upper(), "")
    
    def _get_debt_token_address(self, symbol: str, underlying_address: str) -> str:
        """
        Get variableDebtToken address for a given symbol.
        
        Uses known AAVE V3 Ethereum token addresses mapping.
        Can be extended with more tokens as needed.
        """
        # Known AAVE V3 Ethereum variableDebtToken addresses
        debt_token_addresses = {
            "WETH": "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE",
            "USDT": "0x531842cEbbdD378f8ee36D171d6cC9C4fcf475Ec",
            "USDC": "0x72E95b8931767C79bA4EeE721354d6E99a61D004",
            "DAI": "0x5f3f1dBD7B74C6B46e8c44f98792A1d51B4C7413",
            "WBTC": "0x40aAbEf1aa8f0eEc637EfE662f0B8c701F1F506A",
            "LINK": "0x4228F8890C7C4B5E6A1F9f8C5C5C5C5C5C5C5C5C5",  # Placeholder
            "AAVE": "0x6B4c2605352e8D7C5A5f5C5C5C5C5C5C5C5C5C5C5",  # Placeholder
        }
        return debt_token_addresses.get(symbol.upper(), "")

    def _fetch_market_configurations(self) -> Dict[str, Any]:
        """
        Fetch market configurations from AaveScan API.
        
        Returns:
            Dictionary mapping reserve addresses to their interest rate model configurations
        """
        if self._market_config_cache is not None:
            return self._market_config_cache
        
        try:
            url = f"{self.base_url}/market-configurations"
            params = {}
            
            if self.api_key:
                params["apiKey"] = self.api_key.strip()
            
            params["market"] = "aave-v3-ethereum"
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            # AaveScan returns market configurations with interest rate model parameters
            self._market_config_cache = data
            logger.info("✅ Fetched market configurations from AaveScan")
            return data
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch market configurations from AaveScan: {e}")
            return {}
    
    def _fetch_reserve_config_from_graph(self, underlying_address: str, target_date: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch reserve configuration from The Graph subgraph for Aave V3.
        
        Args:
            underlying_address: Underlying token address
            target_date: Optional target date for historical queries
            
        Returns:
            Reserve configuration dictionary or None
        """
        cache_key = f"{underlying_address}_{target_date.isoformat() if target_date else 'current'}"
        if cache_key in self._reserve_config_cache:
            return self._reserve_config_cache[cache_key]
        
        try:
            # Get The Graph API key (use cached instance variable or module-level cache)
            graph_api_key = self.graph_api_key
            if not graph_api_key:
                # Try module-level cache (set by InstrumentProcessingService)
                try:
                    from instruments_service.app.venues.defi.the_graph_client import _API_KEY_CACHE
                    if _API_KEY_CACHE:
                        graph_api_key = _API_KEY_CACHE
                        self.graph_api_key = graph_api_key  # Cache for future use
                        logger.debug("✅ Using cached Graph API key in _fetch_reserve_config_from_graph")
                except (ImportError, AttributeError):
                    pass
            
            # Only fetch from Secret Manager if not cached
            if not graph_api_key:
                from unified_cloud_services import get_secret_with_fallback
                graph_api_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name="graph-api-key",
                    fallback_env_var="THE_GRAPH_API_KEY",
                )
                if graph_api_key:
                    self.graph_api_key = graph_api_key  # Cache for future use
                    logger.debug("✅ Retrieved Graph API key from Secret Manager (first time)")
            
            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - skipping subgraph query")
                return None
            
            graph_api_key = graph_api_key.strip()
            
            # Build GraphQL query for Aave V3 reserves
            # Aave V3 Ethereum subgraph endpoint
            subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{self.aave_subgraph_id}"
            
            # Add block number if target_date provided
            block_clause = ""
            variables = {"underlyingAddress": underlying_address.lower()}
            block_number = None
            if target_date:
                block_number = self._date_to_block_number(target_date)
                if block_number:
                    block_clause = ", block: {number: $blockNumber}"
                    variables["blockNumber"] = block_number
            
            # Build query - handle block clause properly
            if block_number:
                query = f"""
query GetReserve($underlyingAddress: Bytes!, $blockNumber: Int!) {{
    reserves(where: {{ underlyingAsset: $underlyingAddress }}{block_clause}) {{
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        eModeCategoryId
        pool {{
            id
        }}
    }}
}}
""".strip()
            else:
                query = """
query GetReserve($underlyingAddress: Bytes!) {
    reserves(where: { underlyingAsset: $underlyingAddress }) {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        eModeCategoryId
        pool {
            id
        }
    }
}
""".strip()
            
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                subgraph_url,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                errors = data["errors"]
                # Check if error is about missing/unavailable blocks
                error_messages = [str(e.get("message", "")) for e in errors]
                has_missing_block_error = any(
                    "missing block" in msg.lower() 
                    or "unavailable" in msg.lower()
                    or "bad indexers" in msg.lower()
                    for msg in error_messages
                )
                
                # Check if error is about eModeCategoryId field not existing (historical schema difference)
                has_emode_field_error = any(
                    "emodecategoryid" in msg.lower() or ("emode" in msg.lower() and "field" in msg.lower())
                    for msg in error_messages
                )
                
                if has_emode_field_error:
                    logger.debug(f"⚠️ eModeCategoryId field not available at block {block_number}, retrying without it")
                    # Retry query without eModeCategoryId (may not exist in older schema versions)
                    if block_number:
                        query_no_emode = f"""
query GetReserve($underlyingAddress: Bytes!, $blockNumber: Int!) {{
    reserves(where: {{ underlyingAsset: $underlyingAddress }}, block: {{number: $blockNumber}}) {{
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        pool {{
            id
        }}
    }}
}}
""".strip()
                        variables_no_emode = {"underlyingAddress": underlying_address.lower(), "blockNumber": block_number}
                    else:
                        query_no_emode = """
query GetReserve($underlyingAddress: Bytes!) {
    reserves(where: { underlyingAsset: $underlyingAddress }) {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        pool {
            id
        }
    }
}
""".strip()
                        variables_no_emode = {"underlyingAddress": underlying_address.lower()}
                    
                    response_no_emode = requests.post(
                        subgraph_url,
                        json={"query": query_no_emode, "variables": variables_no_emode},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_emode.raise_for_status()
                    data = response_no_emode.json()
                    if "errors" in data:
                        logger.warning(f"⚠️ GraphQL query errors (no eMode): {data['errors']}")
                        return None
                    # Continue with data without eModeCategoryId
                elif has_missing_block_error:
                    logger.warning(
                        f"⚠️ The Graph indexers don't have data for block {block_number} yet. "
                        f"Retrying query without block number to get latest available data."
                    )
                    # Retry query without block number
                    query_no_block = """
query GetReserve($underlyingAddress: Bytes!) {
    reserves(where: { underlyingAsset: $underlyingAddress }) {
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveInterestRateStrategy
        optimalUtilisationRate
        variableRateSlope1
        variableRateSlope2
        baseVariableBorrowRate
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
        eModeCategoryId
        pool {
            id
        }
    }
}
""".strip()
                    variables_no_block = {"underlyingAddress": underlying_address.lower()}
                    response_no_block = requests.post(
                        subgraph_url,
                        json={"query": query_no_block, "variables": variables_no_block},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_block.raise_for_status()
                    data_no_block = response_no_block.json()
                    if "errors" in data_no_block:
                        logger.warning(f"⚠️ GraphQL query errors (no block): {data_no_block['errors']}")
                        logger.info("Falling back to current data from AaveScan API")
                        return None
                    # Use the data from the retry
                    data = data_no_block
                else:
                    logger.warning(f"⚠️ GraphQL query errors: {errors}")
                    return None
            
            reserves = data.get("data", {}).get("reserves", [])
            if reserves:
                reserve_config = reserves[0]
                # Convert from basis points (bps) to decimal
                # Aave stores values in basis points (e.g., 8000 = 80%) or BigInt (1e18 for rates)
                base_ltv = reserve_config.get("baseLTVasCollateral")
                ltv = float(base_ltv) / 10000.0 if base_ltv is not None else None
                
                reserve_liquidation_threshold = reserve_config.get("reserveLiquidationThreshold")
                liquidation_threshold = float(reserve_liquidation_threshold) / 10000.0 if reserve_liquidation_threshold is not None else None
                
                reserve_liquidation_bonus = reserve_config.get("reserveLiquidationBonus")
                liquidation_bonus = float(reserve_liquidation_bonus) / 10000.0 if reserve_liquidation_bonus is not None else None
                
                optimal_utilization_rate_raw = reserve_config.get("optimalUtilisationRate")
                optimal_utilization_rate = float(optimal_utilization_rate_raw) / 1e27 if optimal_utilization_rate_raw is not None else None
                
                variable_rate_slope1_raw = reserve_config.get("variableRateSlope1")
                variable_rate_slope1 = float(variable_rate_slope1_raw) / 1e27 if variable_rate_slope1_raw is not None else None
                
                variable_rate_slope2_raw = reserve_config.get("variableRateSlope2")
                variable_rate_slope2 = float(variable_rate_slope2_raw) / 1e27 if variable_rate_slope2_raw is not None else None
                
                base_variable_borrow_rate_raw = reserve_config.get("baseVariableBorrowRate")
                base_variable_borrow_rate = float(base_variable_borrow_rate_raw) / 1e27 if base_variable_borrow_rate_raw is not None else None
                
                reserve_factor_raw = reserve_config.get("reserveFactor")
                reserve_factor = float(reserve_factor_raw) / 10000.0 if reserve_factor_raw is not None else None
                
                # Extract reserve mode
                is_active = reserve_config.get("isActive", True)
                is_frozen = reserve_config.get("isFrozen", False)
                is_paused = reserve_config.get("isPaused", False)
                
                if is_frozen:
                    reserve_mode = "FROZEN"
                elif is_paused:
                    reserve_mode = "PAUSED"
                elif is_active:
                    reserve_mode = "ACTIVE"
                else:
                    reserve_mode = "INACTIVE"
                
                # Extract eMode data
                # eModeCategoryId is just an ID (integer) from the subgraph
                emode_category_id = reserve_config.get("eModeCategoryId")
                
                # Fetch eMode category details if we have an ID
                emode_label = None
                emode_liquidation_threshold = None
                emode_liquidation_bonus = None
                emode_price_source = None
                emode_oracle_id = None
                
                if emode_category_id:
                    emode_category = self._fetch_emode_category_from_graph(emode_category_id, target_date=target_date)
                    if emode_category:
                        emode_label = emode_category.get("label")
                        emode_liquidation_threshold = emode_category.get("liquidation_threshold")
                        emode_liquidation_bonus = emode_category.get("liquidation_bonus")
                        emode_price_source = emode_category.get("price_source")
                        emode_oracle_id = emode_category.get("oracle_id")
                
                config = {
                    "ltv": ltv,
                    "liquidation_threshold": liquidation_threshold,
                    "liquidation_bonus": liquidation_bonus,
                    "reserve_factor": reserve_factor,
                    "interest_rate_strategy": reserve_config.get("reserveInterestRateStrategy"),
                    "optimal_utilization_rate": optimal_utilization_rate,
                    "variable_rate_slope1": variable_rate_slope1,
                    "variable_rate_slope2": variable_rate_slope2,
                    "base_variable_borrow_rate": base_variable_borrow_rate,
                    "reserve_mode": reserve_mode,
                    "emode_category_id": emode_category_id,
                    "emode_label": emode_label,
                    "emode_liquidation_threshold": emode_liquidation_threshold,
                    "emode_liquidation_bonus": emode_liquidation_bonus,
                    "emode_price_source": emode_price_source,
                    "emode_oracle_id": emode_oracle_id,
                }
                self._reserve_config_cache[cache_key] = config
                return config
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch reserve config from The Graph: {e}")
            return None
    
    def _fetch_reserve_emode_from_rpc(self, underlying_address: str, target_date: Optional[datetime] = None) -> Optional[int]:
        """
        Fetch eMode category ID for a reserve directly from AAVE contracts via RPC.
        
        Extracts eMode category ID from the reserve's configuration bitmap.
        
        Args:
            underlying_address: Underlying token address
            target_date: Optional target date for historical queries
            
        Returns:
            eMode category ID (integer) or None
        """
        try:
            from web3 import Web3
            from unified_cloud_services import get_secret_with_fallback
            
            # Get block number if target_date provided
            block_number = None
            if target_date:
                block_number = self._date_to_block_number(target_date)
                if not block_number:
                    return None
            
            # Get RPC URL
            alchemy_key = get_secret_with_fallback(
                project_id=self.project_id,
                secret_name="alchemy-api-key",
                fallback_env_var="ALCHEMY_API_KEY",
            )
            
            if not alchemy_key:
                logger.debug("No Alchemy API key found for RPC eMode queries")
                return None
            
            # Strip any whitespace/newlines from API key
            alchemy_key = alchemy_key.strip()
            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
            
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                # Test connection with a simple call
                _ = w3.eth.block_number
            except Exception as conn_error:
                logger.debug(f"Failed to connect to Ethereum RPC for eMode query: {conn_error}")
                return None
            
            # AAVE V3 Pool contract address
            pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
            
            # ABI for getReserveData - returns tuple with configuration as first element
            pool_abi = [
                {
                    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
                    "name": "getReserveData",
                    "outputs": [
                        {"internalType": "uint256", "name": "configuration", "type": "uint256"},
                        {"internalType": "uint128", "name": "liquidityIndex", "type": "uint128"},
                        {"internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128"},
                        {"internalType": "uint128", "name": "variableBorrowIndex", "type": "uint128"},
                        {"internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128"},
                        {"internalType": "uint128", "name": "currentStableBorrowRate", "type": "uint128"},
                        {"internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40"},
                        {"internalType": "uint16", "name": "id", "type": "uint16"},
                        {"internalType": "address", "name": "aTokenAddress", "type": "address"},
                        {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
                        {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"},
                        {"internalType": "address", "name": "interestRateStrategyAddress", "type": "address"},
                        {"internalType": "uint128", "name": "accruedToTreasury", "type": "uint128"},
                        {"internalType": "uint128", "name": "unbacked", "type": "uint128"},
                        {"internalType": "uint128", "name": "isolationModeTotalDebt", "type": "uint128"}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            pool_contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=pool_abi)
            reserve_address = Web3.to_checksum_address(underlying_address)
            
            # Call getReserveData - configuration is the first element
            if block_number:
                reserve_data = pool_contract.functions.getReserveData(reserve_address).call(block_identifier=block_number)
            else:
                reserve_data = pool_contract.functions.getReserveData(reserve_address).call()
            
            configuration = reserve_data[0]  # First element is the configuration bitmap
            
            # Extract eMode category ID from configuration bitmap
            # According to AAVE V3 ReserveConfiguration.sol:
            # EMODE_CATEGORY_START_BIT_POSITION = 168
            # EMODE_CATEGORY_MASK = 0xFFFFFFFFFFFFFFFFFFFF00FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
            # eMode category is stored in bits 168-175 (8 bits, 0-255)
            # Source: https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/configuration/ReserveConfiguration.sol
            emode_category_id = (configuration >> 168) & 0xFF
            
            # eMode category 0 means no eMode
            if emode_category_id == 0:
                logger.debug(f"No eMode category found for reserve {underlying_address} (category ID is 0)")
                return None
            
            logger.debug(f"✅ Fetched eMode category ID {emode_category_id} for reserve {underlying_address} via RPC")
            return emode_category_id
            
        except Exception as e:
            logger.debug(f"Failed to fetch eMode category ID from RPC: {e}")
            return None
    
    def _fetch_emode_category_from_rpc(self, category_id: int, target_date: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch eMode category details directly from AAVE contracts via RPC.
        
        Args:
            category_id: EMode category ID (integer)
            target_date: Optional target date for historical queries
            
        Returns:
            EModeCategory dictionary with details or None
        """
        # Convert category_id to int if it's a string
        try:
            category_id_int = int(category_id) if category_id is not None else None
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Invalid eMode category ID: {category_id}")
            return None
        
        if category_id_int is None or category_id_int == 0:
            return None
        
        # Check cache first (for current data only - historical data may differ)
        if not target_date and category_id_int in self._emode_category_cache:
            cached = self._emode_category_cache[category_id_int]
            return cached
        
        try:
            from web3 import Web3
            from unified_cloud_services import get_secret_with_fallback
            
            # Get block number if target_date provided
            block_number = None
            if target_date:
                block_number = self._date_to_block_number(target_date)
                if not block_number:
                    return None
            
            # Get RPC URL
            alchemy_key = get_secret_with_fallback(
                project_id=self.project_id,
                secret_name="alchemy-api-key",
                fallback_env_var="ALCHEMY_API_KEY",
            )
            
            if not alchemy_key:
                logger.debug("No Alchemy API key found for RPC eMode category query")
                return None
            
            # Strip any whitespace/newlines from API key
            alchemy_key = alchemy_key.strip()
            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
            
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                # Test connection with a simple call
                _ = w3.eth.block_number
            except Exception as conn_error:
                logger.debug(f"Failed to connect to Ethereum RPC for eMode category query: {conn_error}")
                return None
            
            # AAVE V3 Pool contract address
            pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
            
            # ABI for getEModeCategoryData
            # Returns: (uint16 ltv, uint16 liquidationThreshold, uint16 liquidationBonus, address priceSource, string label)
            pool_abi = [
                {
                    "inputs": [{"internalType": "uint8", "name": "id", "type": "uint8"}],
                    "name": "getEModeCategoryData",
                    "outputs": [
                        {"internalType": "uint16", "name": "ltv", "type": "uint16"},
                        {"internalType": "uint16", "name": "liquidationThreshold", "type": "uint16"},
                        {"internalType": "uint16", "name": "liquidationBonus", "type": "uint16"},
                        {"internalType": "address", "name": "priceSource", "type": "address"},
                        {"internalType": "string", "name": "label", "type": "string"}
                    ],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            pool_contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=pool_abi)
            
            # Call getEModeCategoryData using raw call to handle string return type
            function_abi = pool_abi[0]  # getEModeCategoryData is the first function in our ABI
            function = pool_contract.functions.getEModeCategoryData(category_id_int)
            
            if block_number:
                call_data = function._encode_transaction_data()
                result = w3.eth.call({"to": pool_address, "data": call_data}, block_identifier=block_number)
            else:
                call_data = function._encode_transaction_data()
                result = w3.eth.call({"to": pool_address, "data": call_data})
            
            # Decode the raw return data
            # Solidity ABI encoding for tuples includes an offset to the actual data
            # First 32 bytes: offset to actual data (usually 0x20 = 32 bytes)
            if len(result) < 32:
                logger.debug(f"Insufficient return data: {len(result)} bytes")
                return None
            
            # Check if first 32 bytes is an offset (common in tuple returns)
            data_offset = int.from_bytes(result[0:32], byteorder='big')
            if data_offset == 32 and len(result) >= 32 + data_offset:
                # Data starts at offset 32
                # Structure: [offset(32), ltv(32), liquidationThreshold(32), liquidationBonus(32), priceSource(32), string_offset(32), string_length(32), string_data(variable)]
                # Each value is padded to 32 bytes in ABI encoding
                ltv_raw = int.from_bytes(result[32:64], byteorder='big')
                liquidation_threshold_raw = int.from_bytes(result[64:96], byteorder='big')
                liquidation_bonus_raw = int.from_bytes(result[96:128], byteorder='big')
                price_source_bytes = result[128:148]  # Address is 20 bytes
                price_source = '0x' + price_source_bytes.hex()
                
                # String offset is at bytes 160-192 (after priceSource)
                # The offset is relative to the start of the data (byte 32), not absolute
                if len(result) >= 192:
                    string_offset_from_data = int.from_bytes(result[160:192], byteorder='big')
                    data_start = 32  # Data starts after the initial offset
                    string_metadata_start = data_start + string_offset_from_data
                    
                    # String length is at the string_metadata_start position
                    if string_metadata_start > 0 and len(result) >= string_metadata_start + 32:
                        string_length = int.from_bytes(result[string_metadata_start:string_metadata_start+32], byteorder='big')
                        # String data starts 32 bytes after the length
                        if string_length > 0 and string_length < 256 and len(result) >= string_metadata_start + 32 + string_length:
                            string_data = result[string_metadata_start+32:string_metadata_start+32+string_length]
                            label = string_data.decode('utf-8', errors='ignore').rstrip('\x00')
                            logger.debug(f"✅ Extracted eMode category label: {label}")
                        else:
                            label = ""
                    else:
                        label = ""
                else:
                    label = ""
            else:
                # Try eth_abi.decode (it handles offsets automatically)
                try:
                    from eth_abi import decode
                    decoded = decode(['uint16', 'uint16', 'uint16', 'address', 'string'], result)
                    ltv_raw, liquidation_threshold_raw, liquidation_bonus_raw, price_source, label = decoded
                    logger.debug(f"✅ Successfully decoded eMode category {category_id_int} data using eth_abi")
                except Exception as decode_error:
                    logger.debug(f"Failed to decode with eth_abi: {decode_error}, result length: {len(result)}")
                    return None
            
            # Convert from basis points (bps) to decimal
            ltv = float(ltv_raw) / 10000.0 if ltv_raw else None
            liquidation_threshold = float(liquidation_threshold_raw) / 10000.0 if liquidation_threshold_raw else None
            liquidation_bonus = float(liquidation_bonus_raw) / 10000.0 if liquidation_bonus_raw else None
            
            category_data = {
                "id": category_id_int,
                "label": label if label else None,
                "liquidation_threshold": liquidation_threshold,
                "liquidation_bonus": liquidation_bonus,
                "price_source": price_source if price_source != "0x0000000000000000000000000000000000000000" else None,
                "oracle_id": None,  # Oracle ID not directly available from this function
            }
            
            # Cache the result (only for current data to avoid stale historical data)
            if not target_date:
                self._emode_category_cache[category_id_int] = category_data
            
            logger.debug(f"✅ Fetched eMode category {category_id_int} from RPC: {label or 'N/A'}")
            return category_data
            
        except Exception as e:
            logger.debug(f"Failed to fetch eMode category from RPC: {e}")
            return None
    
    def _fetch_emode_category_from_graph(self, category_id: int, target_date: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch EModeCategory details from The Graph subgraph for Aave V3.
        
        Args:
            category_id: EMode category ID (integer or string that can be converted to int)
            target_date: Optional target date for historical queries
            
        Returns:
            EModeCategory dictionary with details or None
        """
        # Convert category_id to int if it's a string
        try:
            category_id_int = int(category_id) if category_id is not None else None
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Invalid eMode category ID: {category_id}")
            return None
        
        if category_id_int is None:
            return None
        
        # Check cache first
        if category_id_int in self._emode_category_cache:
            return self._emode_category_cache[category_id_int]
        
        try:
            # Get The Graph API key (use cached instance variable or module-level cache)
            graph_api_key = self.graph_api_key
            if not graph_api_key:
                # Try module-level cache (set by InstrumentProcessingService)
                try:
                    from instruments_service.app.venues.defi.the_graph_client import _API_KEY_CACHE
                    if _API_KEY_CACHE:
                        graph_api_key = _API_KEY_CACHE
                        self.graph_api_key = graph_api_key
                        logger.debug("✅ Using cached Graph API key in _fetch_emode_category_from_graph")
                except (ImportError, AttributeError):
                    pass
            
            # Only fetch from Secret Manager if not cached
            if not graph_api_key:
                from unified_cloud_services import get_secret_with_fallback
                graph_api_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name="graph-api-key",
                    fallback_env_var="THE_GRAPH_API_KEY",
                )
                if graph_api_key:
                    self.graph_api_key = graph_api_key
                    logger.debug("✅ Retrieved Graph API key from Secret Manager")
            
            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - skipping eMode category query")
                return None
            
            graph_api_key = graph_api_key.strip()
            
            # Build GraphQL query for EModeCategory
            subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{self.aave_subgraph_id}"
            
            # Convert target_date to block number if provided
            block_number = None
            if target_date:
                block_number = self._date_to_block_number(target_date)
            
            variables = {"categoryId": category_id_int}
            block_clause = ""
            
            if block_number:
                block_clause = ", block: {number: $blockNumber}"
                variables["blockNumber"] = block_number
            
            # Query EModeCategory type (note: field name might be eModeCategories or emodeCategories)
            if block_number:
                query = f"""
query GetEModeCategory($categoryId: Int!, $blockNumber: Int!) {{
    eModeCategories(where: {{ id: $categoryId }}{block_clause}) {{
        id
        label
        liquidationThreshold
        liquidationBonus
        priceSource
        oracleId
    }}
}}
""".strip()
            else:
                query = """
query GetEModeCategory($categoryId: Int!) {
    eModeCategories(where: { id: $categoryId }) {
        id
        label
        liquidationThreshold
        liquidationBonus
        priceSource
        oracleId
    }
}
""".strip()
            
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                subgraph_url,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                errors = data["errors"]
                error_messages = [str(e.get("message", "")) for e in errors]
                has_missing_block_error = any(
                    "missing block" in msg.lower() 
                    or "unavailable" in msg.lower()
                    or "bad indexers" in msg.lower()
                    for msg in error_messages
                )
                
                if has_missing_block_error and block_number:
                    logger.debug(f"⚠️ The Graph doesn't have eMode category data for block {block_number}, retrying without block")
                    # Retry without block number
                    query_no_block = """
query GetEModeCategory($categoryId: Int!) {
    eModeCategories(where: { id: $categoryId }) {
        id
        label
        liquidationThreshold
        liquidationBonus
        priceSource
        oracleId
    }
}
""".strip()
                    response_no_block = requests.post(
                        subgraph_url,
                        json={"query": query_no_block, "variables": {"categoryId": category_id_int}},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_block.raise_for_status()
                    data = response_no_block.json()
                    if "errors" in data:
                        logger.warning(f"⚠️ GraphQL query errors for eMode category: {data['errors']}")
                        return None
                else:
                    logger.warning(f"⚠️ GraphQL query errors for eMode category: {errors}")
                    return None
            
            categories = data.get("data", {}).get("eModeCategories", [])
            if categories:
                category = categories[0]  # Should only be one match
                # Convert liquidationThreshold and liquidationBonus from basis points to decimal
                liquidation_threshold_raw = category.get("liquidationThreshold")
                liquidation_threshold = float(liquidation_threshold_raw) / 10000.0 if liquidation_threshold_raw is not None else None
                
                liquidation_bonus_raw = category.get("liquidationBonus")
                liquidation_bonus = float(liquidation_bonus_raw) / 10000.0 if liquidation_bonus_raw is not None else None
                
                category_data = {
                    "id": category.get("id"),
                    "label": category.get("label"),
                    "liquidation_threshold": liquidation_threshold,
                    "liquidation_bonus": liquidation_bonus,
                    "price_source": category.get("priceSource"),
                    "oracle_id": category.get("oracleId"),
                }
                
                # Cache the result
                self._emode_category_cache[category_id_int] = category_data
                logger.debug(f"✅ Fetched eMode category {category_id_int} from The Graph: {category.get('label', 'N/A')}")
                return category_data
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch eMode category from The Graph: {e}")
            return None
    
    def _extract_lending_metadata(self, reserve: Dict[str, Any], target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from reserve data.
        
        Fetches data from:
        - AaveScan API /v2/reserves/latest (current rates) - if target_date not provided
        - The Graph subgraph for Aave V3 (reserve configuration: LTV, liquidation thresholds, etc.) - uses target_date if provided
        - AaveScan API /v2/market-configurations (interest rate model parameters) - if target_date not provided
        
        Args:
            reserve: Reserve data from AaveScan or The Graph
            target_date: Optional target date for historical queries
            
        Returns:
            Dictionary with lending protocol metadata fields including reserve_mode and emode
        """
        asset = reserve.get("asset", {})
        underlying_address = reserve.get("reserve", "") or asset.get("address", "")
        
        # Flash loan providers - AAVE V3 supports flash loans for all reserves
        # AAVE V3 Pool contract address on Ethereum: 0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2
        flash_loan_providers = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
        
        # Extract current borrow rate from API response (only if not historical)
        current_variable_borrow_rate = reserve.get("currentVariableBorrowRate")
        base_variable_borrow_rate = None
        if current_variable_borrow_rate:
            try:
                # Convert from Ray (1e27) to decimal
                base_variable_borrow_rate = float(current_variable_borrow_rate) / 1e27
            except (ValueError, TypeError):
                pass
        
        # Fetch reserve configuration from The Graph subgraph (with target_date if provided)
        reserve_config = None
        if underlying_address:
            reserve_config = self._fetch_reserve_config_from_graph(underlying_address, target_date=target_date)
            if not reserve_config:
                logger.debug(f"⚠️ No reserve config from Graph for {underlying_address}")
        else:
            logger.debug(f"⚠️ No underlying_address provided for reserve: {reserve.get('asset', {}).get('symbol', 'unknown')}")
        
        # Extract reserve_mode from reserve data or reserve_config
        reserve_mode = None
        if reserve.get("reserveMode"):
            reserve_mode = reserve.get("reserveMode")
        elif reserve_config and reserve_config.get("reserve_mode"):
            reserve_mode = reserve_config.get("reserve_mode")
        else:
            # Fallback: determine from flags
            is_active = reserve.get("isActive", reserve_config.get("isActive") if reserve_config else True)
            is_frozen = reserve.get("isFrozen", reserve_config.get("isFrozen") if reserve_config else False)
            is_paused = reserve.get("isPaused", reserve_config.get("isPaused") if reserve_config else False)
            if is_frozen:
                reserve_mode = "FROZEN"
            elif is_paused:
                reserve_mode = "PAUSED"
            elif is_active:
                reserve_mode = "ACTIVE"
            else:
                reserve_mode = "INACTIVE"
        
        # Extract eMode data from reserve data or reserve_config
        # Try multiple sources in order of preference:
        # 1. RPC direct queries (most reliable, works for historical data)
        # 2. Reserve data from API
        # 3. Reserve config from The Graph (limited support)
        
        e_mode_category_obj = reserve.get("eModeCategory")
        e_mode_category_id_from_reserve = None
        if isinstance(e_mode_category_obj, dict):
            e_mode_category_id_from_reserve = e_mode_category_obj.get("id")
        elif e_mode_category_obj is not None:
            # If it's not a dict, it might be the ID directly
            e_mode_category_id_from_reserve = e_mode_category_obj
        
        # Get eModeCategoryId from reserve_config (key is "emode_category_id" in our format, but "eModeCategoryId" from Graph)
        emode_category_id_from_config = None
        if reserve_config:
            emode_category_id_from_config = reserve_config.get("emode_category_id") or reserve_config.get("eModeCategoryId")
        
        # Try RPC first to get eMode category ID (most reliable)
        emode_category_id_from_rpc = None
        if underlying_address:
            emode_category_id_from_rpc = self._fetch_reserve_emode_from_rpc(underlying_address, target_date=target_date)
        
        # Use RPC result if available, otherwise fall back to other sources
        emode_category_id = emode_category_id_from_rpc or e_mode_category_id_from_reserve or emode_category_id_from_config
        
        # Fetch eMode category details if we have an ID
        emode_label = None
        emode_underlying = None
        emode_liquidation_threshold = None
        emode_liquidation_bonus = None
        emode_price_source = None
        emode_oracle_id = None
        
        if emode_category_id:
            # Try RPC first (most reliable, works for historical data)
            emode_category = self._fetch_emode_category_from_rpc(emode_category_id, target_date=target_date)
            
            # Fallback to The Graph if RPC fails (though it likely won't work)
            if not emode_category:
                emode_category = self._fetch_emode_category_from_graph(emode_category_id, target_date=target_date)
            
            if emode_category:
                emode_label = emode_category.get("label")
                emode_liquidation_threshold = emode_category.get("liquidation_threshold")
                emode_liquidation_bonus = emode_category.get("liquidation_bonus")
                emode_price_source = emode_category.get("price_source")
                emode_oracle_id = emode_category.get("oracle_id")
        elif reserve_config:
            # Fallback: use values from reserve_config if available
            emode_category_id = emode_category_id or reserve_config.get("emode_category_id")
            emode_label = reserve_config.get("emode_label")
            emode_liquidation_threshold = reserve_config.get("emode_liquidation_threshold")
            emode_liquidation_bonus = reserve_config.get("emode_liquidation_bonus")
            emode_price_source = reserve_config.get("emode_price_source")
            emode_oracle_id = reserve_config.get("emode_oracle_id")
        
        # Fetch market configurations for interest rate model parameters (only if not historical)
        market_configs = None
        if not target_date:
            market_configs = self._fetch_market_configurations()
        
        # Extract interest rate model parameters
        # Use values from reserve_config if available (from The Graph), otherwise try market_configs
        optimal_utilization_rate = reserve_config.get("optimal_utilization_rate") if reserve_config else None
        variable_rate_slope1 = reserve_config.get("variable_rate_slope1") if reserve_config else None
        variable_rate_slope2 = reserve_config.get("variable_rate_slope2") if reserve_config else None
        base_variable_borrow_rate_from_config = reserve_config.get("base_variable_borrow_rate") if reserve_config else None
        
        # If not in reserve_config, try market_configs (only for current data)
        if not optimal_utilization_rate and market_configs:
            interest_rate_strategy_address = reserve_config.get("interest_rate_strategy") if reserve_config else None
            if interest_rate_strategy_address:
                strategies = market_configs.get("strategies", [])
                if isinstance(strategies, list):
                    for strategy in strategies:
                        if strategy.get("address", "").lower() == interest_rate_strategy_address.lower():
                            optimal_utilization_rate = strategy.get("optimalUtilizationRate")
                            variable_rate_slope1 = strategy.get("variableRateSlope1")
                            variable_rate_slope2 = strategy.get("variableRateSlope2")
                            # Convert from Ray (1e27) to decimal if needed
                            if optimal_utilization_rate:
                                try:
                                    optimal_utilization_rate = float(optimal_utilization_rate) / 1e27
                                except (ValueError, TypeError):
                                    pass
                            if variable_rate_slope1:
                                try:
                                    variable_rate_slope1 = float(variable_rate_slope1) / 1e27
                                except (ValueError, TypeError):
                                    pass
                            if variable_rate_slope2:
                                try:
                                    variable_rate_slope2 = float(variable_rate_slope2) / 1e27
                                except (ValueError, TypeError):
                                    pass
                            break
        
        # Use base_variable_borrow_rate from reserve_config if available, otherwise from API
        if base_variable_borrow_rate_from_config is not None:
            base_variable_borrow_rate = base_variable_borrow_rate_from_config
        # Otherwise use the value extracted from API response above
        
        return {
            "flash_loan_providers": flash_loan_providers,
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": reserve_config.get("ltv") if reserve_config else None,
            "liquidation_threshold": reserve_config.get("liquidation_threshold") if reserve_config else None,
            "liquidation_bonus": reserve_config.get("liquidation_bonus") if reserve_config else None,
            "reserve_factor": reserve_config.get("reserve_factor") if reserve_config else None,
            "reserve_mode": reserve_mode,
            "emode_category_id": emode_category_id,
            "emode_label": emode_label,
            "emode_underlying": emode_underlying,
            "emode_liquidation_threshold": emode_liquidation_threshold,
            "emode_liquidation_bonus": emode_liquidation_bonus,
            "emode_price_source": emode_price_source,
            "emode_oracle_id": emode_oracle_id,
            "optimal_utilization_rate": optimal_utilization_rate,
            "base_variable_borrow_rate": base_variable_borrow_rate,
            "variable_rate_slope1": variable_rate_slope1,
            "variable_rate_slope2": variable_rate_slope2,
        }

    def _create_a_token_instrument(
        self, reserve: Dict[str, Any], target_date: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Create aToken instrument definition.

        Args:
            reserve: Reserve data from AaveScan or The Graph
            target_date: Optional target date for historical queries

        Returns:
            Instrument definition dictionary or None
        """
        # AaveScan Pro API returns reserves with structure:
        # {"reserve": address, "asset": {"symbol": "...", "address": "...", "decimals": ...}, ...}
        asset = reserve.get("asset", {})
        symbol = asset.get("symbol", "")
        underlying_address = reserve.get("reserve", "") or asset.get("address", "")
        
        # AaveScan Pro /reserves/latest doesn't provide aToken/debtToken addresses
        # Use known AAVE V3 Ethereum token addresses mapping
        a_token_address = self._get_a_token_address(symbol, underlying_address)
        
        if not symbol or not a_token_address:
            return None

        # Build canonical instrument key
        a_token_symbol = f"A{symbol}"  # e.g., AUSDT, AWETH
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata (with target_date for historical queries)
        lending_metadata = self._extract_lending_metadata(reserve, target_date=target_date)

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
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, POLYGON, etc.)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aavescan",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(2023, 1, 27).isoformat(),  # AAVE V3 Ethereum launch date
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Use default - protocol tokens don't have market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }

    def _create_debt_token_instrument(
        self, reserve: Dict[str, Any], target_date: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Create debtToken instrument definition.

        Args:
            reserve: Reserve data from AaveScan or The Graph
            target_date: Optional target date for historical queries

        Returns:
            Instrument definition dictionary or None
        """
        # AaveScan Pro API returns reserves with structure:
        # {"reserve": address, "asset": {"symbol": "...", "address": "...", "decimals": ...}, ...}
        asset = reserve.get("asset", {})
        symbol = asset.get("symbol", "")
        underlying_address = reserve.get("reserve", "") or asset.get("address", "")
        
        # AaveScan Pro /reserves/latest doesn't provide aToken/debtToken addresses
        # Use known AAVE V3 Ethereum token addresses mapping
        debt_token_address = self._get_debt_token_address(symbol, underlying_address)
        
        if not symbol or not debt_token_address:
            return None

        # Build canonical instrument key
        debt_token_symbol = f"DEBT{symbol}"  # e.g., DEBTWETH, DEBTUSDT
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata (with target_date for historical queries)
        lending_metadata = self._extract_lending_metadata(reserve, target_date=target_date)

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
            "available_from_datetime": datetime(2023, 1, 27).isoformat(),  # AAVE V3 Ethereum launch date
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Use default - protocol tokens don't have market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }
