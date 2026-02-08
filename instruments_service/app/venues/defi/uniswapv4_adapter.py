"""
Uniswap V4 Adapter

Fetches Uniswap V4 pool instruments from The Graph.
Generates canonical instrument keys for DEX pools.

Subgraph ID: DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G
Reference: https://docs.uniswap.org/api/subgraph/guides/v4-examples

Uniswap V4 introduces:
- Hooks for customizable pool behavior
- Single PoolManager contract (singleton pattern)
- Pool IDs are keccak256 hashes of poolKey (not contract addresses)
"""

import asyncio
import concurrent.futures
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from unified_cloud_services import get_secret_with_fallback

from instruments_service.app.venues.defi.base_defi_adapter import (
    BaseDefiAdapter,
    create_aiohttp_session,
)
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class UniswapV4Adapter(BaseDefiAdapter):
    """
    Adapter for fetching Uniswap V4 pool instruments.

    Generates instruments in format:
    UNISWAPV4-ETH:POOL:ETH-USDC:500@ETHEREUM

    Uniswap V4 uses the singleton PoolManager pattern where all pools
    are managed by a single contract. Pool IDs are keccak256 hashes.
    """

    # The Graph Uniswap V4 subgraph (decentralized network)
    THEGRAPH_SUBGRAPH_ID = "DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G"
    THEGRAPH_ENDPOINT = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{THEGRAPH_SUBGRAPH_ID}"

    # PoolManager contract address on Ethereum mainnet
    POOL_MANAGER_ADDRESS = "0x000000000004444c5dc75cb358380d2e3de08a90"

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Uniswap V4 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM')
            api_key: Optional The Graph API key
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain, api_key, project_id)

        # Map chain to venue format
        chain_to_venue = {
            "ETHEREUM": "UNISWAPV4-ETH",
        }
        self.venue = chain_to_venue.get(self.chain, f"UNISWAPV4-{self.chain}")

        # Get The Graph API key
        self._thegraph_api_key = api_key
        if not self._thegraph_api_key:
            try:
                self._thegraph_api_key = get_secret_with_fallback(
                    project_id=self.project_id,
                    secret_name=instruments_config.graph_secret_name or "thegraph-api-key",
                    fallback_env_var="THEGRAPH_API_KEY",
                )
            except Exception:
                logger.warning("No The Graph API key found")

        logger.info(f"✅ UniswapV4Adapter initialized for chain: {self.chain}")

    # V4 first subgraph activity Jan 23 2025; 14-day buffer -> Feb 7
    V4_LAUNCH_DATE = datetime(2025, 2, 7, tzinfo=timezone.utc)

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V4 pools and convert to instrument definitions.

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            base_currency_list: Filter by list of base currencies
            quote_currency_list: Filter by list of quote currencies
            min_liquidity: Minimum liquidity threshold in USD (not well supported in V4 yet)
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before V4 launch
        if target_date and target_date < self.V4_LAUNCH_DATE:
            logger.info(
                f"ℹ️ Uniswap V4 not available for {target_date.strftime('%Y-%m-%d')} "
                f"(V4 mainnet launched November 2024). Returning empty instruments - this is expected."
            )
            return {}

        # Handle nested event loops (CLI may already have one running)
        try:
            asyncio.get_running_loop()
            # Already in async context - run in thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self._fetch_pools(base_currency=base_currency, min_tx_count=1000))
                pools = future.result()
        except RuntimeError:
            # No event loop running - safe to use asyncio.run
            pools = asyncio.run(self._fetch_pools(base_currency=base_currency, min_tx_count=1000))

        instruments = {}

        # Normalize filter lists
        allowed_bases = {b.upper() for b in base_currency_list} if base_currency_list else None
        allowed_quotes = {q.upper() for q in quote_currency_list} if quote_currency_list else None

        # Wrapped/staked token mappings
        wrapped_mappings = {
            "WETH": "ETH",
            "WSTETH": "ETH",
            "WEETH": "ETH",
            "STETH": "ETH",
            "ETH": "ETH",  # V4 uses native ETH symbol
        }

        for pool in pools:
            try:
                token0_symbol = pool.get("token0", {}).get("symbol", "").upper()
                token1_symbol = pool.get("token1", {}).get("symbol", "").upper()

                # Apply filtering logic
                if allowed_bases or allowed_quotes:
                    token0_in_bases = token0_symbol in allowed_bases if allowed_bases else True
                    token1_in_bases = token1_symbol in allowed_bases if allowed_bases else True
                    token0_in_quotes = token0_symbol in allowed_quotes if allowed_quotes else True
                    token1_in_quotes = token1_symbol in allowed_quotes if allowed_quotes else True

                    # Check wrapped versions
                    if not token0_in_bases and token0_symbol in wrapped_mappings:
                        token0_in_bases = wrapped_mappings[token0_symbol] in allowed_bases if allowed_bases else True
                    if not token1_in_bases and token1_symbol in wrapped_mappings:
                        token1_in_bases = wrapped_mappings[token1_symbol] in allowed_bases if allowed_bases else True
                    if not token0_in_quotes and token0_symbol in wrapped_mappings:
                        token0_in_quotes = wrapped_mappings[token0_symbol] in allowed_quotes if allowed_quotes else True
                    if not token1_in_quotes and token1_symbol in wrapped_mappings:
                        token1_in_quotes = wrapped_mappings[token1_symbol] in allowed_quotes if allowed_quotes else True

                    # Require: (token0 in bases AND token1 in quotes) OR (token1 in bases AND token0 in quotes)
                    valid_pair = False
                    if allowed_bases and allowed_quotes:
                        valid_pair = (token0_in_bases and token1_in_quotes) or (token1_in_bases and token0_in_quotes)
                    elif allowed_bases:
                        valid_pair = token0_in_bases or token1_in_bases
                    elif allowed_quotes:
                        valid_pair = token0_in_quotes or token1_in_quotes

                    if not valid_pair:
                        continue

                inst_def = self._convert_pool_to_instrument(pool)
                if inst_def:
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Uniswap V4 instruments")
        return instruments

    async def _fetch_pools(
        self,
        base_currency: Optional[str] = None,
        min_tx_count: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch pools from The Graph.
        """
        if not self._thegraph_api_key:
            logger.warning("No The Graph API key available for Uniswap V4")
            return []

        endpoint = self.THEGRAPH_ENDPOINT.format(api_key=self._thegraph_api_key)

        # Query for pools by transaction count (more reliable than TVL for V4)
        if base_currency:
            # Query by base currency
            query = """
            query GetPools($symbol: String!, $minTxCount: BigInt!) {
                pools(
                    first: 100,
                    orderBy: txCount,
                    orderDirection: desc,
                    where: {
                        txCount_gt: $minTxCount,
                        or: [
                            {token0_: {symbol_contains_nocase: $symbol}},
                            {token1_: {symbol_contains_nocase: $symbol}}
                        ]
                    }
                ) {
                    id
                    feeTier
                    tick
                    sqrtPrice
                    liquidity
                    totalValueLockedUSD
                    volumeUSD
                    txCount
                    token0 {
                        id
                        symbol
                        name
                        decimals
                    }
                    token1 {
                        id
                        symbol
                        name
                        decimals
                    }
                }
            }
            """
            variables = {"symbol": base_currency, "minTxCount": str(min_tx_count)}
        else:
            # Query top pools by transaction count
            query = """
            query GetPools($minTxCount: BigInt!) {
                pools(
                    first: 100,
                    orderBy: txCount,
                    orderDirection: desc,
                    where: {txCount_gt: $minTxCount}
                ) {
                    id
                    feeTier
                    tick
                    sqrtPrice
                    liquidity
                    totalValueLockedUSD
                    volumeUSD
                    txCount
                    token0 {
                        id
                        symbol
                        name
                        decimals
                    }
                    token1 {
                        id
                        symbol
                        name
                        decimals
                    }
                }
            }
            """
            variables = {"minTxCount": str(min_tx_count)}

        try:
            async with create_aiohttp_session(timeout=60) as session:
                async with session.post(
                    endpoint, json={"query": query, "variables": variables}, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        logger.warning(f"The Graph returned HTTP {response.status}")
                        return []

                    data = await response.json()

                    if "errors" in data:
                        logger.warning(f"The Graph errors: {data['errors']}")
                        return []

                    pools = data.get("data", {}).get("pools", [])
                    logger.info(f"Fetched {len(pools)} Uniswap V4 pools")
                    return pools

        except Exception as e:
            logger.error(f"Failed to fetch V4 pools: {e}")
            return []

    def _convert_pool_to_instrument(self, pool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert The Graph pool data to instrument definition.

        Args:
            pool: Pool data from The Graph

        Returns:
            Instrument definition dictionary or None
        """
        pool_id = pool.get("id")  # Pool ID (keccak256 hash)
        token0 = pool.get("token0", {})
        token1 = pool.get("token1", {})
        fee_tier = pool.get("feeTier")

        if not pool_id or not token0 or not token1:
            return None

        # Extract token info
        token0_symbol = pool.get("token0", {}).get("symbol", "")
        token1_symbol = pool.get("token1", {}).get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")
        token0.get("decimals", "18")
        token1.get("decimals", "18")

        # V4 uses native ETH (address 0x0) - normalize symbol
        if token0_address == "0x0000000000000000000000000000000000000000":
            token0_symbol = "ETH"
        if token1_address == "0x0000000000000000000000000000000000000000":
            token1_symbol = "ETH"

        # Determine base and quote (ETH as base if present)
        if "ETH" in token0_symbol.upper():
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
        elif "ETH" in token1_symbol.upper():
            base_symbol = token1_symbol
            quote_symbol = token0_symbol
            base_address = token1_address
            quote_address = token0_address
        else:
            # Default: token0 as base, token1 as quote
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address

        # Build symbol with fee tier
        fee_tier_str = str(fee_tier) if fee_tier else "0"
        symbol = f"{base_symbol}-{quote_symbol}:{fee_tier_str}"

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"

        # V4 first subgraph activity Jan 2025, 14-day buffer for data reliability
        available_from = "2025-02-07T00:00:00"

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "POOL",
            "symbol": symbol,
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "settle_asset": quote_symbol,
            "pool_id": pool_id,  # V4 uses pool_id (keccak256 hash)
            "pool_address": pool_id,  # Also store as pool_address for compatibility
            "pool_fee_tier": int(fee_tier) if fee_tier else 0,
            "base_asset_contract_address": base_address,
            "quote_asset_contract_address": quote_address,
            "chain": self.chain,
            "market_category": "DEFI",
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "the_graph",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": f"{token0_symbol}/{token1_symbol}",
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": None,
            "data_types": "swaps,liquidity",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": f"{base_symbol}-{quote_symbol}",
        }
