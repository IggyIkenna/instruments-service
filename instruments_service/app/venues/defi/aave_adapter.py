"""
AAVE V3 Adapter - REFACTORED

Fetches AAVE V3 market instruments (aTokens, debtTokens) using AaveScan API or AAVE SDK.
Generates canonical instrument keys for AAVE positions.

ARCHITECTURE:
- Uses AlchemyBaseClient (unified-cloud-services) for on-chain RPC calls
- Uses TheGraphBaseClient (unified-cloud-services) for subgraph queries
- Uses get_http_session for HTTP calls (AaveScan API)

Reference: instruments-service/docs/MVP_INSTRUMENTS.md (DeFi section)
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from unified_cloud_services import (
    get_secret_with_fallback,
    AlchemyBaseClient,
    TheGraphBaseClient,
    TheGraphClientConfig,
    get_http_session,  # Centralized HTTP session pool
)
from instruments_service.config import instruments_config
from web3 import Web3

logger = logging.getLogger(__name__)


class AaveV3Adapter(BaseDefiAdapter):
    """
    Adapter for fetching AAVE V3 market instruments.

    Generates instruments in format:
    AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM
    AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM

    OPTIMIZATION: Uses static risk parameters as fallback when RPC/Graph unavailable.

    TODO: Replace static risk parameters with actual AAVE-generated parameters.
    Currently using STATIC_RISK_PARAMS for eMode and standard risk parameters.
    Should fetch these dynamically from AAVE contracts via RPC or The Graph subgraph.
    See instruments-service/issues/aave-dynamic-params.md for details.
    """

    # Static risk parameters used as fallback when RPC/Graph queries fail.
    # These are fetched dynamically when available via _fetch_emode_categories_from_graph()
    # and _fetch_reserve_config_from_graph() methods. See instruments-service/issues/aave-dynamic-params.md
    STATIC_RISK_PARAMS = {
        "emode": {
            "ltv_limits": {
                "weETH_WETH": 0.93,
                "wstETH_WETH": 0.93,
                "ETH_WETH": 0.93,
            },
            "liquidation_thresholds": {
                "weETH_WETH": 0.95,
                "wstETH_WETH": 0.95,
                "ETH_WETH": 0.95,
            },
            "liquidation_bonus": {
                "weETH_WETH": 0.01,
                "wstETH_WETH": 0.01,
                "ETH_WETH": 0.01,
            },
        },
        "standard": {
            "ltv_limits": {
                "weETH_WETH": 0.80,
                "wstETH_WETH": 0.80,
                "ETH_WETH": 0.80,
            },
            "liquidation_thresholds": {
                "weETH_WETH": 0.85,
                "wstETH_WETH": 0.85,
                "ETH_WETH": 0.85,
            },
            "liquidation_bonus": {
                "weETH_WETH": 0.05,
                "wstETH_WETH": 0.05,
                "ETH_WETH": 0.05,
            },
        },
        "reserve_factors": {
            "weETH": 0.10,
            "wstETH": 0.10,
            "WETH": 0.10,
            "USDT": 0.10,
        },
    }

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
        # Initialize base class (sets self.chain, self.project_id)
        # Note: BaseDefiAdapter sets self.api_key, but AaveV3Adapter uses api_key for AaveScan
        # and graph_api_key for The Graph, so we'll override self.api_key after base init
        super().__init__(chain, api_key=None, project_id=project_id)

        # Map chain to venue format matching config.py
        # Only ETHEREUM supported for MVP
        chain_to_venue = {
            "ETHEREUM": "AAVE_V3_ETH",
        }
        self.venue = chain_to_venue.get(self.chain, f"AAVE_V3_{self.chain}")

        # Try provided AaveScan API key first
        self.api_key = api_key

        # If not provided, try Secret Manager
        if not self.api_key:
            try:
                secret_name = instruments_config.aavescan_secret_name
                self.api_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name=secret_name,
                    fallback_env_var="AAVESCAN_API_KEY",
                )

                if self.api_key:
                    logger.info(
                        f"✅ Retrieved AaveScan API key from Secret Manager (secret: {secret_name})"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")
                self.api_key = instruments_config.aavescan_secret_name

        if not self.api_key:
            logger.warning("AaveScan API key not found. Some features may be limited.")

        # Store Graph API key (use provided or centralized client handles it)
        self.graph_api_key = graph_api_key

        # Initialize centralized Alchemy client for on-chain RPC calls
        self._alchemy_client = AlchemyBaseClient(
            chain=self.chain,
            project_id=self.project_id,
        )

        # Initialize centralized The Graph client for subgraph queries
        graph_config = TheGraphClientConfig(secret_name=instruments_config.graph_secret_name)
        self._thegraph_client = TheGraphBaseClient(
            config=graph_config,
            project_id=self.project_id,
        )
        if graph_api_key:
            self._thegraph_client._api_key = graph_api_key

        # AaveScan Pro API uses v2 endpoint with apiKey query parameter
        # Configurable via config (with default fallback)
        self.base_url = instruments_config.aavescan_api_url

        # Aave V3 Ethereum subgraph ID from The Graph
        # Subgraph: https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g
        self.aave_subgraph_id = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"

        # Cache for reserve configurations and market configurations
        self._reserve_config_cache: Dict[str, Dict[str, Any]] = {}
        self._market_config_cache: Optional[Dict[str, Any]] = None

        # Cache for eMode categories (key: category_id, value: category dict)
        self._emode_category_cache: Dict[int, Dict[str, Any]] = {}

        # OPTIMIZATION: Cache reserves data (same data for entire day)
        # Reserves don't change intraday, so we can reuse across multiple calls
        self._reserves_cache: Optional[List[Dict[str, Any]]] = None
        self._reserves_cache_date: Optional[str] = None

        # OPTIMIZATION: Cache block number conversions (same block for all reserves on same date)
        self._block_number_cache: Dict[str, int] = {}  # date_str -> block_number

        # Failure caches to avoid retrying operations that already failed
        # Cache keys: date ISO string or block number
        self._historical_query_failed: set = set()  # Dates/blocks where historical queries failed
        self._block_conversion_failed: set = set()  # Dates where block conversion failed

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
                    debt_token_def = self._create_debt_token_instrument(
                        reserve, target_date=target_date
                    )
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(f"Failed to process reserve {reserve.get('symbol')}: {e}")
                    continue

            logger.info(f"✅ Generated {len(instruments)} AAVE V3 instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch AAVE markets: {e}")
            return {}

    def _get_fallback_reserves(self) -> List[Dict[str, Any]]:
        """
        Get static fallback reserves for AAVE V3 when APIs fail.

        Uses MVP tokens: USDT, WETH, weETH, wstETH
        Emode params will be populated from STATIC_RISK_PARAMS later.

        Returns:
            List of reserve dictionaries with minimal required fields
        """
        # MVP tokens for AAVE V3 Ethereum
        static_reserves = [
            {
                "reserve": "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
                "asset": {
                    "symbol": "USDT",
                    "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                    "decimals": 6,
                },
            },
            {
                "reserve": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
                "asset": {
                    "symbol": "WETH",
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "decimals": 18,
                },
            },
            {
                "reserve": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",  # weETH
                "asset": {
                    "symbol": "weETH",
                    "address": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
                    "decimals": 18,
                },
            },
            {
                "reserve": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",  # wstETH
                "asset": {
                    "symbol": "wstETH",
                    "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
                    "decimals": 18,
                },
            },
        ]

        logger.warning(
            f"⚠️ Using static fallback reserves for AAVE V3 ({len(static_reserves)} reserves) - API/RPC unavailable"
        )
        return static_reserves

    def _fetch_reserves(self, target_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch reserves from AaveScan Pro API (primary) with optional historical Graph fallback.

        OPTIMIZED: Caches reserves for the entire day (same data, reused across multiple instruments).

        Args:
            target_date: Optional target date for historical queries. If provided:
                        1. Tries The Graph subgraph once (if not already failed)
                        2. Falls back immediately to current data from AaveScan API (primary source)

                        Note: AaveScan is the primary data source. Graph is only attempted
                        once for historical data, then cached as failed to avoid retries.

        Returns:
            List of reserve dictionaries
        """
        # OPTIMIZATION: Check cache first (reserves don't change intraday)
        cache_date = (
            target_date.strftime("%Y-%m-%d")
            if target_date
            else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        if self._reserves_cache is not None and self._reserves_cache_date == cache_date:
            logger.debug(
                f"✅ Using cached AAVE reserves for {cache_date} ({len(self._reserves_cache)} reserves)"
            )
            return self._reserves_cache

        # If target_date is provided, try Graph once (if not already failed), then use AaveScan
        if target_date:
            date_key = target_date.isoformat()

            # Try Graph once if we haven't already failed for this date
            if date_key not in self._historical_query_failed:
                logger.info(
                    f"📅 Attempting historical query for {date_key} via The Graph (one-time attempt)"
                )
                graph_reserves = self._fetch_reserves_from_graph(target_date=target_date)
                if graph_reserves:
                    logger.info("✅ Successfully fetched historical reserves from The Graph")
                    return graph_reserves
                else:
                    # Graph failed - log warning and mark as failed, then use AaveScan
                    logger.warning(
                        f"⚠️ Historical Graph query failed for {date_key}. "
                        f"Falling back to current data from AaveScan API (primary source)."
                    )
                    # Failure is already cached in _fetch_reserves_from_graph
            else:
                # Already tried Graph and it failed - use AaveScan directly
                logger.debug(
                    f"⏭️ Skipping Graph query for {date_key} (already failed). "
                    f"Using current data from AaveScan API."
                )

        logger.info("🔍 [METHOD_TRACE] _fetch_reserves calling AaveScan API (primary method)")
        try:
            # AaveScan Pro API uses apiKey as query parameter, not Authorization header
            # Endpoint: https://api.aavescan.com/v2/reserves/latest?market=aave-v3-ethereum&apiKey=...
            url = f"{self.base_url}/reserves/latest"
            params = {}

            if self.api_key:
                params["apiKey"] = self.api_key.strip()

            # Add market parameter for Ethereum mainnet
            params["market"] = "aave-v3-ethereum"

            # Use pooled HTTP session
            session = get_http_session(base_url=self.base_url)
            response = session.get(url, params=params, timeout=30)
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

            # OPTIMIZATION: Filter to MVP tokens only (don't process all 220 reserves)
            # MVP tokens: USDT, WETH, weETH, wstETH (unique by symbol, not by reserve address)
            mvp_tokens = {"USDT", "WETH", "WEETH", "WSTETH"}

            # Further optimization: Keep only ONE reserve per symbol (avoid duplicates)
            seen_symbols = set()
            filtered_reserves = []
            for r in reserves:
                symbol = r.get("asset", {}).get("symbol", "").upper()
                if symbol in mvp_tokens and symbol not in seen_symbols:
                    filtered_reserves.append(r)
                    seen_symbols.add(symbol)

            logger.info(
                f"✅ Fetched {len(reserves)} reserves from AaveScan, "
                f"filtered to {len(filtered_reserves)} MVP tokens"
            )

            # Cache the filtered reserves for reuse
            self._reserves_cache = filtered_reserves
            self._reserves_cache_date = cache_date

            return filtered_reserves

        except Exception as e:
            logger.error(f"Failed to fetch reserves from AaveScan: {e}")
            logger.info("🔍 [METHOD_TRACE] Falling back to _get_fallback_reserves (static reserves)")
            # Fallback: return empty list or use hardcoded known reserves
            fallback_reserves = self._get_fallback_reserves()

            # Cache fallback reserves too
            self._reserves_cache = fallback_reserves
            self._reserves_cache_date = cache_date

            return fallback_reserves

    def _date_to_block_number(self, target_date: datetime) -> Optional[int]:
        """
        Convert a date to Ethereum block number using RPC or approximation.

        OPTIMIZED: Caches block numbers (same block for all reserves on same date).

        First attempts to query Ethereum RPC for exact block number.
        Falls back to approximate calculation if RPC unavailable.

        Args:
            target_date: Target date

        Returns:
            Block number or None if date is in the future or RPC unavailable
        """
        try:
            # Ensure target_date is timezone-aware
            if target_date.tzinfo is None:
                target_date = target_date.replace(tzinfo=timezone.utc)

            # OPTIMIZATION: Check block number cache first
            date_str = target_date.strftime("%Y-%m-%d")
            if date_str in self._block_number_cache:
                logger.debug(
                    f"✅ Using cached block number for {date_str}: {self._block_number_cache[date_str]}"
                )
                return self._block_number_cache[date_str]

            # Check failure cache first - if we already know this date fails, skip retrying
            date_key = target_date.isoformat()
            if date_key in self._block_conversion_failed:
                logger.debug(f"⏭️ Skipping block conversion for {date_key} - already failed")
                return None

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
                # Use centralized Alchemy client for RPC URL
                rpc_url = self._alchemy_client.get_rpc_url()
                timestamp = int(target_date.timestamp())

                if rpc_url:

                    # Use pooled HTTP session
                    session = get_http_session(base_url=rpc_url)

                    # First, get latest block to estimate
                    payload_latest = {
                        "jsonrpc": "2.0",
                        "method": "eth_blockNumber",
                        "params": [],
                        "id": 1,
                    }
                    response = session.post(rpc_url, json=payload_latest, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if "result" in data:
                            latest_block = int(data["result"], 16)

                            # Estimate block number (12 seconds per block)
                            latest_block_data = session.post(
                                rpc_url,
                                json={
                                    "jsonrpc": "2.0",
                                    "method": "eth_getBlockByNumber",
                                    "params": [hex(latest_block), False],
                                    "id": 2,
                                },
                                timeout=10,
                            ).json()

                            if "result" in latest_block_data and latest_block_data["result"]:
                                latest_timestamp = int(latest_block_data["result"]["timestamp"], 16)
                                seconds_diff = latest_timestamp - timestamp
                                estimated_blocks_back = int(seconds_diff / 12)
                                estimated_block = max(0, latest_block - estimated_blocks_back)

                                # Get exact block
                                block_data = session.post(
                                    rpc_url,
                                    json={
                                        "jsonrpc": "2.0",
                                        "method": "eth_getBlockByNumber",
                                        "params": [hex(estimated_block), False],
                                        "id": 3,
                                    },
                                    timeout=10,
                                ).json()

                                if "result" in block_data and block_data["result"]:
                                    block_timestamp = int(block_data["result"]["timestamp"], 16)
                                    # Binary search for exact block if needed
                                    if (
                                        abs(block_timestamp - timestamp) > 60
                                    ):  # More than 1 minute off
                                        # Use approximation
                                        logger.debug(
                                            f"RPC block lookup found block {estimated_block} with timestamp {block_timestamp}, target {timestamp}"
                                        )
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

            logger.info(
                f"📅 Converted {target_date.isoformat()} to approximate block {block_number:,}"
            )

            # Cache the block number for reuse
            block_num = max(0, block_number)
            self._block_number_cache[date_str] = block_num
            logger.debug(f"✅ Cached block number for {date_str}: {block_num}")

            return block_num

        except Exception as e:
            logger.warning(f"⚠️ Failed to convert date to block number: {e}")
            # Cache the failure
            date_key = target_date.isoformat()
            self._block_conversion_failed.add(date_key)
            return None

    def _fetch_reserves_from_graph(self, target_date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch reserves from The Graph subgraph for historical data (one-time attempt).

        This is a fallback attempt - AaveScan is the primary data source.
        If this fails, it will be cached and AaveScan will be used instead.

        Args:
            target_date: Target date for historical query

        Returns:
            List of reserve dictionaries, or empty list if failed (will trigger AaveScan fallback)
        """
        logger.info(f"🔍 [METHOD_TRACE] _fetch_reserves_from_graph called for {target_date}")
        date_key = target_date.isoformat()

        # Check failure cache first - if we already know historical queries fail for this date/block, skip
        if date_key in self._historical_query_failed:
            logger.debug(f"⏭️ Skipping historical Graph query for {date_key} - already failed")
            return []

        try:
            # Convert date to block number
            block_number = self._date_to_block_number(target_date)
            if not block_number:
                logger.warning(
                    "⚠️ Could not convert date to block number, falling back to current data"
                )
                # Cache the failure
                self._historical_query_failed.add(date_key)
                return []

            # Get The Graph API key (use cached instance variable or module-level cache)
            graph_api_key = self.graph_api_key
            if not graph_api_key:
                # Try module-level cache (set by InstrumentProcessingService)
                # Use centralized TheGraph client's API key
                graph_api_key = self._thegraph_client.api_key
                if graph_api_key:
                    self.graph_api_key = graph_api_key
                    logger.debug("✅ Using Graph API key from centralized client")

            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - will use AaveScan current data")
                # Cache the failure
                self._historical_query_failed.add(date_key)
                return []

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
            # Use pooled HTTP session
            session = get_http_session(base_url="https://gateway.thegraph.com")
            response = session.post(
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
                    "emodecategoryid" in msg.lower()
                    or ("emode" in msg.lower() and "field" in msg.lower())
                    for msg in error_messages
                )

                if has_emode_field_error:
                    logger.debug(
                        f"⚠️ eModeCategoryId field not available at block {block_number}, retrying without it"
                    )
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
                    response_no_emode = session.post(
                        subgraph_url,
                        json={"query": query_no_emode, "variables": variables},
                        headers=headers,
                        timeout=30,
                    )
                    response_no_emode.raise_for_status()
                    data = response_no_emode.json()
                    if "errors" in data:
                        logger.warning(f"⚠️ GraphQL query errors (no eMode): {data['errors']}")
                        # Cache the failure - will use AaveScan
                        self._historical_query_failed.add(date_key)
                        if block_number:
                            self._historical_query_failed.add(str(block_number))
                        return []
                    # Continue with data without eModeCategoryId
                elif has_missing_block_error:
                    # Historical data not yet indexed - this is expected for recent dates
                    logger.debug(
                        f"The Graph indexers don't have data for block {block_number} yet "
                        f"(latest indexed: ~23,775,170 from Aug 2024). "
                        f"Caching failure and skipping future retries for this date."
                    )
                    # Cache the failure - don't retry for this date/block
                    self._historical_query_failed.add(date_key)
                    if block_number:
                        self._historical_query_failed.add(str(block_number))
                    return []
                else:
                    logger.warning(f"⚠️ GraphQL query errors: {errors}")
                    # Cache the failure
                    self._historical_query_failed.add(date_key)
                    if block_number:
                        self._historical_query_failed.add(str(block_number))
                    return []

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
                    "eModeCategory": (
                        {
                            "id": e_mode_category_id,
                        }
                        if e_mode_category_id
                        else None
                    ),
                }
                formatted_reserves.append(formatted_reserve)

            logger.info(
                f"✅ Fetched {len(formatted_reserves)} historical reserves from The Graph (block {block_number})"
            )
            return formatted_reserves

        except Exception as e:
            logger.warning(
                f"⚠️ Failed to fetch historical reserves from The Graph: {e}. "
                f"Will use AaveScan current data instead."
            )
            # Cache the failure
            date_key = target_date.isoformat()
            self._historical_query_failed.add(date_key)
            return []

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
            "WEETH": "0x4421A7d21d752f8CC35039678c8D27996c09f18E",  # weETH aToken
            "WSTETH": "0x0B925eD163218f6662a35e0f0371Ac234f9E9371",  # wstETH aToken
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
            "WEETH": "0x24e6e0795b3c7c71D96FEA0e07125B1dC8d3b1b5",  # weETH debt token
            "WSTETH": "0xC96113eED8cAB8CD8321FC2C3C7A47a5e6547A4B",  # wstETH debt token
        }
        return debt_token_addresses.get(symbol.upper(), "")

    def _fetch_market_configurations(self) -> Dict[str, Any]:
        """
        Fetch market configurations from AaveScan API.

        Returns:
            Dictionary mapping reserve addresses to their interest rate model configurations
        """
        if self._market_config_cache is not None:
            logger.debug("🔍 [METHOD_TRACE] _fetch_market_configurations using cache")
            return self._market_config_cache

        logger.info("🔍 [METHOD_TRACE] _fetch_market_configurations calling AaveScan API")
        try:
            url = f"{self.base_url}/market-configurations"
            params = {}

            if self.api_key:
                params["apiKey"] = self.api_key.strip()

            params["market"] = "aave-v3-ethereum"

            # Use pooled HTTP session
            session = get_http_session(base_url=self.base_url)
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            # AaveScan returns market configurations with interest rate model parameters
            self._market_config_cache = data
            logger.info("✅ Fetched market configurations from AaveScan")
            return data

        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch market configurations from AaveScan: {e}")
            return {}

    def _fetch_reserve_config_from_graph(
        self, underlying_address: str, target_date: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch reserve configuration from The Graph subgraph for Aave V3.

        Args:
            underlying_address: Underlying token address
            target_date: Optional target date for historical queries

        Returns:
            Reserve configuration dictionary or None
        """
        logger.info(f"🔍 [METHOD_TRACE] _fetch_reserve_config_from_graph called for {underlying_address}, target_date={target_date}")
        # Check failure cache first for historical queries
        if target_date:
            date_key = target_date.isoformat()
            if date_key in self._historical_query_failed:
                logger.debug(
                    f"⏭️ Skipping historical Graph config query for {date_key} - already failed"
                )
                return None

        cache_key = f"{underlying_address}_{target_date.isoformat() if target_date else 'current'}"
        if cache_key in self._reserve_config_cache:
            return self._reserve_config_cache[cache_key]

        try:
            # Get The Graph API key from centralized client
            graph_api_key = self.graph_api_key or self._thegraph_client.api_key
            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - skipping subgraph query")
                return None

            graph_api_key = graph_api_key.strip()
            self.graph_api_key = graph_api_key  # Cache for future use

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
            # Use pooled HTTP session
            session = get_http_session(base_url="https://gateway.thegraph.com")
            response = session.post(
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
                    "emodecategoryid" in msg.lower()
                    or ("emode" in msg.lower() and "field" in msg.lower())
                    for msg in error_messages
                )

                if has_emode_field_error:
                    logger.debug(
                        f"⚠️ eModeCategoryId field not available at block {block_number}, retrying without it"
                    )
                    # Retry query without eModeCategoryId (may not exist in older schema versions)
                    if block_number:
                        query_no_emode = """
query GetReserve($underlyingAddress: Bytes!, $blockNumber: Int!) {
    reserves(where: { underlyingAsset: $underlyingAddress }, block: {number: $blockNumber}) {
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
                        variables_no_emode = {
                            "underlyingAddress": underlying_address.lower(),
                            "blockNumber": block_number,
                        }
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

                    response_no_emode = session.post(
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
                    # Historical data not yet indexed - this is expected for recent dates
                    logger.debug(
                        f"The Graph indexers don't have data for block {block_number} yet. "
                        f"Caching failure and skipping future retries."
                    )
                    # Cache the failure - don't retry
                    if target_date:
                        self._historical_query_failed.add(target_date.isoformat())
                    if block_number:
                        self._historical_query_failed.add(str(block_number))
                    return None
                else:
                    logger.warning(f"⚠️ GraphQL query errors: {errors}")
                    # Cache the failure
                    if target_date:
                        self._historical_query_failed.add(target_date.isoformat())
                    return None

            reserves = data.get("data", {}).get("reserves", [])
            if reserves:
                reserve_config = reserves[0]
                # Convert from basis points (bps) to decimal
                # Aave stores values in basis points (e.g., 8000 = 80%) or BigInt (1e18 for rates)
                base_ltv = reserve_config.get("baseLTVasCollateral")
                ltv = float(base_ltv) / 10000.0 if base_ltv is not None else None

                reserve_liquidation_threshold = reserve_config.get("reserveLiquidationThreshold")
                liquidation_threshold = (
                    float(reserve_liquidation_threshold) / 10000.0
                    if reserve_liquidation_threshold is not None
                    else None
                )

                reserve_liquidation_bonus = reserve_config.get("reserveLiquidationBonus")
                liquidation_bonus = (
                    float(reserve_liquidation_bonus) / 10000.0
                    if reserve_liquidation_bonus is not None
                    else None
                )

                optimal_utilization_rate_raw = reserve_config.get("optimalUtilisationRate")
                optimal_utilization_rate = (
                    float(optimal_utilization_rate_raw) / 1e27
                    if optimal_utilization_rate_raw is not None
                    else None
                )

                variable_rate_slope1_raw = reserve_config.get("variableRateSlope1")
                variable_rate_slope1 = (
                    float(variable_rate_slope1_raw) / 1e27
                    if variable_rate_slope1_raw is not None
                    else None
                )

                variable_rate_slope2_raw = reserve_config.get("variableRateSlope2")
                variable_rate_slope2 = (
                    float(variable_rate_slope2_raw) / 1e27
                    if variable_rate_slope2_raw is not None
                    else None
                )

                base_variable_borrow_rate_raw = reserve_config.get("baseVariableBorrowRate")
                base_variable_borrow_rate = (
                    float(base_variable_borrow_rate_raw) / 1e27
                    if base_variable_borrow_rate_raw is not None
                    else None
                )

                reserve_factor_raw = reserve_config.get("reserveFactor")
                reserve_factor = (
                    float(reserve_factor_raw) / 10000.0 if reserve_factor_raw is not None else None
                )

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

                # TODO: Fetch eMode category details dynamically from AAVE contracts
                # Currently using STATIC_RISK_PARAMS instead - see instruments-service/issues/aave-dynamic-params.md
                if emode_category_id:
                    # eMode category details are now fetched from STATIC_RISK_PARAMS in _extract_lending_metadata
                    # This code path is no longer used but kept for reference
                    emode_label = None
                    emode_liquidation_threshold = None
                    emode_liquidation_bonus = None
                    emode_price_source = None
                    emode_oracle_id = None

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
            # Cache the failure for historical queries
            if target_date:
                self._historical_query_failed.add(target_date.isoformat())
            return None

    def _fetch_reserve_config_history_from_graph(
        self, reserve_symbol: str, target_date: Optional[datetime] = None, limit: int = 10
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch historical reserve configuration changes from The Graph subgraph.

        Uses ReserveConfigurationHistoryItem entity to track when governance
        changes risk parameters (LTV, liquidation thresholds, etc.).

        Args:
            reserve_symbol: Token symbol (e.g., 'WETH', 'USDC')
            target_date: Optional target date - fetches history up to this date
            limit: Maximum number of history items to return

        Returns:
            List of configuration history items or None
        """
        cache_key = f"config_history_{reserve_symbol}_{target_date.isoformat() if target_date else 'current'}_{limit}"
        if cache_key in self._reserve_config_cache:
            return self._reserve_config_cache[cache_key]

        try:
            graph_api_key = self.graph_api_key or self._thegraph_client.api_key
            if not graph_api_key:
                logger.warning("⚠️ No The Graph API key found - skipping config history query")
                return None

            graph_api_key = graph_api_key.strip()
            subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{self.aave_subgraph_id}"

            # Build timestamp filter if target_date provided
            timestamp_filter = ""
            if target_date:
                target_timestamp = int(target_date.timestamp())
                timestamp_filter = f", timestamp_lte: {target_timestamp}"

            query = f"""
query GetReserveConfigHistory {{
    reserveConfigurationHistoryItems(
        where: {{ reserve_: {{ symbol: "{reserve_symbol}" }}{timestamp_filter} }}
        orderBy: timestamp
        orderDirection: desc
        first: {limit}
    ) {{
        id
        timestamp
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        borrowingEnabled
        usageAsCollateralEnabled
        isActive
        isFrozen
        reserveInterestRateStrategy
        reserve {{
            id
            symbol
            underlyingAsset
        }}
    }}
}}
""".strip()

            headers = {"Content-Type": "application/json"}
            session = get_http_session(base_url="https://gateway.thegraph.com")
            response = session.post(
                subgraph_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()

            data = response.json()
            if "errors" in data:
                logger.warning(f"⚠️ GraphQL config history query errors: {data['errors']}")
                return None

            history_items = data.get("data", {}).get("reserveConfigurationHistoryItems", [])
            if not history_items:
                logger.debug(f"No config history found for {reserve_symbol}")
                return None

            # Convert from basis points to decimals
            result = []
            for item in history_items:
                base_ltv = item.get("baseLTVasCollateral")
                liq_threshold = item.get("reserveLiquidationThreshold")
                liq_bonus = item.get("reserveLiquidationBonus")

                result.append({
                    "timestamp": item.get("timestamp"),
                    "ltv": float(base_ltv) / 10000.0 if base_ltv else None,
                    "liquidation_threshold": float(liq_threshold) / 10000.0 if liq_threshold else None,
                    "liquidation_bonus": float(liq_bonus) / 10000.0 if liq_bonus else None,
                    "borrowing_enabled": item.get("borrowingEnabled"),
                    "collateral_enabled": item.get("usageAsCollateralEnabled"),
                    "is_active": item.get("isActive"),
                    "is_frozen": item.get("isFrozen"),
                    "interest_rate_strategy": item.get("reserveInterestRateStrategy"),
                    "reserve_id": item.get("reserve", {}).get("id"),
                    "underlying_asset": item.get("reserve", {}).get("underlyingAsset"),
                })

            logger.info(
                f"✅ Fetched {len(result)} config history items for {reserve_symbol} "
                f"(latest change: {result[0]['timestamp'] if result else 'N/A'})"
            )

            self._reserve_config_cache[cache_key] = result
            return result

        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch reserve config history from The Graph: {e}")
            return None

    def _fetch_emode_categories_from_graph(self) -> Optional[Dict[int, Dict[str, Any]]]:
        """
        Fetch all eMode categories from The Graph subgraph.

        Returns:
            Dictionary mapping category ID to category details, or None
        """
        cache_key = "emode_categories"
        if cache_key in self._reserve_config_cache:
            return self._reserve_config_cache[cache_key]

        try:
            graph_api_key = self.graph_api_key or self._thegraph_client.api_key
            if not graph_api_key:
                return None

            graph_api_key = graph_api_key.strip()
            subgraph_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{self.aave_subgraph_id}"

            query = """
query GetEModeCategories {
    emodeCategories {
        id
        ltv
        liquidationThreshold
        liquidationBonus
        oracle
        label
    }
}
""".strip()

            headers = {"Content-Type": "application/json"}
            session = get_http_session(base_url="https://gateway.thegraph.com")
            response = session.post(
                subgraph_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()

            data = response.json()
            if "errors" in data:
                logger.warning(f"⚠️ GraphQL eMode query errors: {data['errors']}")
                return None

            categories = data.get("data", {}).get("emodeCategories", [])
            result = {}
            for cat in categories:
                cat_id = int(cat.get("id", 0))
                ltv = cat.get("ltv")
                liq_threshold = cat.get("liquidationThreshold")
                liq_bonus = cat.get("liquidationBonus")

                result[cat_id] = {
                    "id": cat_id,
                    "label": cat.get("label"),
                    "ltv": float(ltv) / 10000.0 if ltv else None,
                    "liquidation_threshold": float(liq_threshold) / 10000.0 if liq_threshold else None,
                    "liquidation_bonus": float(liq_bonus) / 10000.0 if liq_bonus else None,
                    "oracle": cat.get("oracle"),
                }

            if result:
                logger.info(f"✅ Fetched {len(result)} eMode categories from The Graph")
                self._reserve_config_cache[cache_key] = result

            return result

        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch eMode categories from The Graph: {e}")
            return None

    def _fetch_reserve_emode_from_rpc(
        self, underlying_address: str, target_date: Optional[datetime] = None
    ) -> Optional[int]:
        """
        Fetch eMode category ID for a reserve directly from AAVE contracts via RPC.

        Extracts eMode category ID from the reserve's configuration bitmap.

        Args:
            underlying_address: Underlying token address
            target_date: Optional target date for historical queries

        Returns:
            eMode category ID (integer) or None
        """
        logger.info(f"🔍 [METHOD_TRACE] _fetch_reserve_emode_from_rpc called for {underlying_address}, target_date={target_date}")
        try:
            # Get block number if target_date provided
            block_number = None
            if target_date:
                block_number = self._date_to_block_number(target_date)
                if not block_number:
                    return None

            # Use centralized Alchemy client for Web3 provider
            try:
                w3 = self._alchemy_client.get_web3()
            except ValueError as e:
                logger.debug(f"No Alchemy API key found for RPC eMode queries: {e}")
                return None
            if not w3:
                logger.debug("Failed to connect to Ethereum RPC for eMode query")
                return None

            # AAVE V3 Pool contract address
            pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

            # ABI for getReserveData - returns tuple with configuration as first element
            pool_abi = [
                {
                    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
                    "name": "getReserveData",
                    "outputs": [
                        {
                            "internalType": "uint256",
                            "name": "configuration",
                            "type": "uint256",
                        },
                        {
                            "internalType": "uint128",
                            "name": "liquidityIndex",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "currentLiquidityRate",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "variableBorrowIndex",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "currentVariableBorrowRate",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "currentStableBorrowRate",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint40",
                            "name": "lastUpdateTimestamp",
                            "type": "uint40",
                        },
                        {"internalType": "uint16", "name": "id", "type": "uint16"},
                        {
                            "internalType": "address",
                            "name": "aTokenAddress",
                            "type": "address",
                        },
                        {
                            "internalType": "address",
                            "name": "stableDebtTokenAddress",
                            "type": "address",
                        },
                        {
                            "internalType": "address",
                            "name": "variableDebtTokenAddress",
                            "type": "address",
                        },
                        {
                            "internalType": "address",
                            "name": "interestRateStrategyAddress",
                            "type": "address",
                        },
                        {
                            "internalType": "uint128",
                            "name": "accruedToTreasury",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "unbacked",
                            "type": "uint128",
                        },
                        {
                            "internalType": "uint128",
                            "name": "isolationModeTotalDebt",
                            "type": "uint128",
                        },
                    ],
                    "stateMutability": "view",
                    "type": "function",
                }
            ]

            pool_contract = w3.eth.contract(
                address=Web3.to_checksum_address(pool_address), abi=pool_abi
            )
            reserve_address = Web3.to_checksum_address(underlying_address)

            # Call getReserveData - configuration is the first element
            if block_number:
                reserve_data = pool_contract.functions.getReserveData(reserve_address).call(
                    block_identifier=block_number
                )
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
                logger.debug(
                    f"No eMode category found for reserve {underlying_address} (category ID is 0)"
                )
                return None

            logger.debug(
                f"✅ Fetched eMode category ID {emode_category_id} for reserve {underlying_address} via RPC"
            )
            return emode_category_id

        except Exception as e:
            logger.debug(f"Failed to fetch eMode category ID from RPC: {e}")
            return None

    def _extract_lending_metadata(
        self, reserve: Dict[str, Any], target_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
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

        TODO: Currently uses STATIC_RISK_PARAMS for eMode and standard risk parameters.
        Should be replaced with dynamic fetching from AAVE contracts via RPC or The Graph.
        See instruments-service/issues/aave-dynamic-params.md for implementation plan.
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

        # Extract metadata from AaveScan reserve data first (primary source)
        # AaveScan API provides most of the metadata we need
        # Try multiple field name variations (AaveScan may use different naming)
        ltv_from_aavescan = None
        liquidation_threshold_from_aavescan = None
        liquidation_bonus_from_aavescan = None
        reserve_factor_from_aavescan = None

        # Try to extract LTV from AaveScan reserve data (try multiple field names)
        ltv_raw = (
            reserve.get("baseLTVasCollateral")
            or reserve.get("baseLTV")
            or reserve.get("ltv")
            or (
                reserve.get("configuration", {})
                if isinstance(reserve.get("configuration"), dict)
                else {}
            ).get("baseLTVasCollateral")
        )
        if ltv_raw:
            try:
                # AaveScan returns in basis points (bps), convert to decimal
                ltv_from_aavescan = float(ltv_raw) / 10000.0
            except (ValueError, TypeError):
                pass

        # Try to extract liquidation threshold
        liquidation_threshold_raw = (
            reserve.get("reserveLiquidationThreshold")
            or reserve.get("liquidationThreshold")
            or reserve.get("liquidation_threshold")
            or (
                reserve.get("configuration", {})
                if isinstance(reserve.get("configuration"), dict)
                else {}
            ).get("reserveLiquidationThreshold")
        )
        if liquidation_threshold_raw:
            try:
                liquidation_threshold_from_aavescan = float(liquidation_threshold_raw) / 10000.0
            except (ValueError, TypeError):
                pass

        # Try to extract liquidation bonus
        liquidation_bonus_raw = (
            reserve.get("reserveLiquidationBonus")
            or reserve.get("liquidationBonus")
            or reserve.get("liquidation_bonus")
            or (
                reserve.get("configuration", {})
                if isinstance(reserve.get("configuration"), dict)
                else {}
            ).get("reserveLiquidationBonus")
        )
        if liquidation_bonus_raw:
            try:
                liquidation_bonus_from_aavescan = float(liquidation_bonus_raw) / 10000.0
            except (ValueError, TypeError):
                pass

        # Try to extract reserve factor
        reserve_factor_raw = (
            reserve.get("reserveFactor")
            or reserve.get("reserve_factor")
            or (
                reserve.get("configuration", {})
                if isinstance(reserve.get("configuration"), dict)
                else {}
            ).get("reserveFactor")
        )
        if reserve_factor_raw:
            try:
                reserve_factor_from_aavescan = float(reserve_factor_raw) / 10000.0
            except (ValueError, TypeError):
                pass

        # Try Graph config as fallback (one-time attempt, cached if fails)
        reserve_config = None
        if underlying_address and target_date:
            # Only try Graph for historical queries - for current data, use AaveScan
            reserve_config = self._fetch_reserve_config_from_graph(
                underlying_address, target_date=target_date
            )
            if not reserve_config:
                logger.debug(
                    f"⚠️ No reserve config from Graph for {underlying_address} - using AaveScan data"
                )
        elif not underlying_address:
            logger.debug(
                f"⚠️ No underlying_address provided for reserve: {reserve.get('asset', {}).get('symbol', 'unknown')}"
            )

        # Extract reserve_mode from reserve data or reserve_config
        reserve_mode = None
        if reserve.get("reserveMode"):
            reserve_mode = reserve.get("reserveMode")
        elif reserve_config and reserve_config.get("reserve_mode"):
            reserve_mode = reserve_config.get("reserve_mode")
        else:
            # Fallback: determine from flags
            is_active = reserve.get(
                "isActive", reserve_config.get("isActive") if reserve_config else True
            )
            is_frozen = reserve.get(
                "isFrozen", reserve_config.get("isFrozen") if reserve_config else False
            )
            is_paused = reserve.get(
                "isPaused", reserve_config.get("isPaused") if reserve_config else False
            )
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
            emode_category_id_from_config = reserve_config.get(
                "emode_category_id"
            ) or reserve_config.get("eModeCategoryId")

        # Try RPC first to get eMode category ID (most reliable)
        emode_category_id_from_rpc = None
        if underlying_address:
            emode_category_id_from_rpc = self._fetch_reserve_emode_from_rpc(
                underlying_address, target_date=target_date
            )

        # Use RPC result if available, otherwise fall back to other sources
        emode_category_id = (
            emode_category_id_from_rpc
            or e_mode_category_id_from_reserve
            or emode_category_id_from_config
        )

        # STATIC FALLBACK: If still no emode_category_id, use hardcoded values for known ETH correlation assets
        # Category 1 = ETH Correlation (weETH, wstETH vs WETH)
        asset_symbol = reserve.get("asset", {}).get("symbol", "")
        if not emode_category_id and asset_symbol in ["weETH", "wstETH", "WETH"]:
            emode_category_id = 1  # ETH correlation emode
            logger.info(
                f"  ✅ Using STATIC emode_category_id=1 (ETH correlation) for {asset_symbol}"
            )

        # Fetch eMode category details if we have an ID
        emode_label = None
        emode_underlying = None
        emode_liquidation_threshold = None
        emode_liquidation_bonus = None
        emode_price_source = None
        emode_oracle_id = None

        logger.info(
            f"🔍 Fetching eMode params for {reserve.get('asset', {}).get('symbol', 'UNKNOWN')}: emode_category_id={emode_category_id}"
        )

        logger.debug(f"🔍 eMode params for {asset_symbol}: emode_category_id={emode_category_id}")

        # Priority: 1) The Graph eMode categories, 2) reserve_config, 3) STATIC_RISK_PARAMS
        if emode_category_id:
            symbol = reserve.get("asset", {}).get("symbol", "")

            # Try The Graph first for eMode category details
            emode_categories = self._fetch_emode_categories_from_graph()
            if emode_categories and emode_category_id in emode_categories:
                cat_data = emode_categories[emode_category_id]
                emode_label = cat_data.get("label")
                emode_liquidation_threshold = cat_data.get("liquidation_threshold")
                emode_liquidation_bonus = cat_data.get("liquidation_bonus")
                emode_price_source = cat_data.get("oracle")
                logger.info(
                    f"  ✅ Using The Graph eMode params for {symbol} (category {emode_category_id}): "
                    f"label={emode_label}, liq_threshold={emode_liquidation_threshold}"
                )
            elif symbol and emode_category_id == 1:
                # Fallback to STATIC_RISK_PARAMS for ETH correlation mode
                logger.info("🔍 [METHOD_TRACE] _extract_lending_metadata using STATIC_RISK_PARAMS (Graph eMode unavailable)")
                emode_label = "ETH_CORRELATION"
                pair_key = f"{symbol}_WETH"
                emode_liquidation_threshold = self.STATIC_RISK_PARAMS["emode"][
                    "liquidation_thresholds"
                ].get(pair_key)
                emode_liquidation_bonus = self.STATIC_RISK_PARAMS["emode"]["liquidation_bonus"].get(
                    pair_key
                )
                logger.debug(
                    f"  ✅ Using STATIC eMode params for {symbol}: "
                    f"liq_threshold={emode_liquidation_threshold}, bonus={emode_liquidation_bonus}"
                )
        elif reserve_config:
            # Fallback: use values from reserve_config if available
            logger.debug("  → Using reserve_config fallback for eMode data")
            emode_category_id = emode_category_id or reserve_config.get("emode_category_id")
            emode_label = reserve_config.get("emode_label")
            emode_liquidation_threshold = reserve_config.get("emode_liquidation_threshold")
            emode_liquidation_bonus = reserve_config.get("emode_liquidation_bonus")
            emode_price_source = reserve_config.get("emode_price_source")
            emode_oracle_id = reserve_config.get("emode_oracle_id")
            logger.info(f"  ✅ Using reserve_config eMode data: label={emode_label}")
        else:
            # Some tokens (e.g., USDT) don't have eMode categories - this is expected behavior
            logger.debug(
                f"  No eMode category ID or reserve_config available for {reserve.get('asset', {}).get('symbol', 'UNKNOWN')} (expected for some tokens)"
            )

        # Fetch market configurations for interest rate model parameters (only if not historical)
        market_configs = None
        if not target_date:
            market_configs = self._fetch_market_configurations()

        # Extract interest rate model parameters
        # Use values from reserve_config if available (from The Graph), otherwise try market_configs
        optimal_utilization_rate = (
            reserve_config.get("optimal_utilization_rate") if reserve_config else None
        )
        variable_rate_slope1 = (
            reserve_config.get("variable_rate_slope1") if reserve_config else None
        )
        variable_rate_slope2 = (
            reserve_config.get("variable_rate_slope2") if reserve_config else None
        )
        base_variable_borrow_rate_from_config = (
            reserve_config.get("base_variable_borrow_rate") if reserve_config else None
        )

        # If not in reserve_config, try market_configs (only for current data)
        if not optimal_utilization_rate and market_configs:
            interest_rate_strategy_address = (
                reserve_config.get("interest_rate_strategy") if reserve_config else None
            )
            if interest_rate_strategy_address:
                strategies = market_configs.get("strategies", [])
                if isinstance(strategies, list):
                    for strategy in strategies:
                        if (
                            strategy.get("address", "").lower()
                            == interest_rate_strategy_address.lower()
                        ):
                            optimal_utilization_rate = strategy.get("optimalUtilizationRate")
                            variable_rate_slope1 = strategy.get("variableRateSlope1")
                            variable_rate_slope2 = strategy.get("variableRateSlope2")
                            # Convert from Ray (1e27) to decimal if needed
                            if optimal_utilization_rate:
                                try:
                                    optimal_utilization_rate = (
                                        float(optimal_utilization_rate) / 1e27
                                    )
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

        # Use AaveScan data as primary source, Graph config as fallback, STATIC_RISK_PARAMS as final fallback
        ltv = ltv_from_aavescan or (reserve_config.get("ltv") if reserve_config else None)
        liquidation_threshold = liquidation_threshold_from_aavescan or (
            reserve_config.get("liquidation_threshold") if reserve_config else None
        )
        liquidation_bonus = liquidation_bonus_from_aavescan or (
            reserve_config.get("liquidation_bonus") if reserve_config else None
        )
        reserve_factor = reserve_factor_from_aavescan or (
            reserve_config.get("reserve_factor") if reserve_config else None
        )

        # Final fallback to STATIC_RISK_PARAMS if still missing
        asset_symbol = reserve.get("asset", {}).get("symbol", "")
        if asset_symbol and (
            not ltv or not liquidation_threshold or not liquidation_bonus or not reserve_factor
        ):
            logger.info(f"  🔄 Attempting static fallback for {asset_symbol} risk params")

            # Use emode params if in emode (ETH correlation mode)
            if emode_category_id == 1:
                pair_key = f"{asset_symbol}_WETH"
                if not ltv:
                    ltv = self.STATIC_RISK_PARAMS["emode"]["ltv_limits"].get(pair_key)
                if not liquidation_threshold:
                    liquidation_threshold = self.STATIC_RISK_PARAMS["emode"][
                        "liquidation_thresholds"
                    ].get(pair_key)
                if not liquidation_bonus:
                    liquidation_bonus = self.STATIC_RISK_PARAMS["emode"]["liquidation_bonus"].get(
                        pair_key
                    )
                if ltv:
                    logger.info(
                        f"  ✅ Using STATIC eMode risk params for {pair_key}: ltv={ltv}, liq_threshold={liquidation_threshold}"
                    )
            else:
                # Use standard params if not in emode
                pair_key = f"{asset_symbol}_WETH"
                if not ltv:
                    ltv = self.STATIC_RISK_PARAMS["standard"]["ltv_limits"].get(pair_key)
                if not liquidation_threshold:
                    liquidation_threshold = self.STATIC_RISK_PARAMS["standard"][
                        "liquidation_thresholds"
                    ].get(pair_key)
                if not liquidation_bonus:
                    liquidation_bonus = self.STATIC_RISK_PARAMS["standard"][
                        "liquidation_bonus"
                    ].get(pair_key)
                if ltv:
                    logger.info(
                        f"  ✅ Using STATIC standard risk params for {pair_key}: ltv={ltv}, liq_threshold={liquidation_threshold}"
                    )

            # Reserve factor fallback
            if not reserve_factor:
                reserve_factor = self.STATIC_RISK_PARAMS["reserve_factors"].get(asset_symbol)
                if reserve_factor:
                    logger.info(
                        f"  ✅ Using STATIC reserve_factor for {asset_symbol}: {reserve_factor}"
                    )

        return {
            "flash_loan_providers": flash_loan_providers,
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": ltv,
            "liquidation_threshold": liquidation_threshold,
            "liquidation_bonus": liquidation_bonus,
            "reserve_factor": reserve_factor,
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
            "available_from_datetime": datetime(
                2023, 1, 27
            ).isoformat(),  # AAVE V3 Ethereum launch date
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization",  # Raw data: supplyIndex, liquidityIndex, utilization rate
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
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, POLYGON, etc.)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aavescan",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2023, 1, 27
            ).isoformat(),  # AAVE V3 Ethereum launch date
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization",  # Raw data: borrowIndex, utilization rate
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }
