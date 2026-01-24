"""
Uniswap V2 Adapter

Fetches Uniswap V2 pair instruments from The Graph.
Generates canonical instrument keys for DEX pairs.

Subgraph ID: A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum
Reference: https://docs.uniswap.org/contracts/v2/reference/API/queries
"""

import logging
import asyncio
import concurrent.futures
from typing import Dict, List, Optional, Any
from datetime import datetime

import aiohttp

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from instruments_service.config import instruments_config
from unified_cloud_services import get_secret_with_fallback

logger = logging.getLogger(__name__)


class UniswapV2Adapter(BaseDefiAdapter):
    """
    Adapter for fetching Uniswap V2 pair instruments.

    Generates instruments in format:
    UNISWAPV2-ETH:POOL:WETH-USDC@ETHEREUM
    
    Uniswap V2 uses "pairs" instead of "pools" and has no fee tiers.
    """
    
    # The Graph Uniswap V2 subgraph (decentralized network)
    THEGRAPH_SUBGRAPH_ID = "A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum"
    THEGRAPH_ENDPOINT = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{THEGRAPH_SUBGRAPH_ID}"

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Uniswap V2 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM')
            api_key: Optional The Graph API key
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain, api_key, project_id)
        
        # Map chain to venue format
        chain_to_venue = {
            "ETHEREUM": "UNISWAPV2-ETH",
        }
        self.venue = chain_to_venue.get(self.chain, f"UNISWAPV2-{self.chain}")
        
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
        
        logger.info(f"✅ UniswapV2Adapter initialized for chain: {self.chain}")

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V2 pairs and convert to instrument definitions.

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            base_currency_list: Filter by list of base currencies
            quote_currency_list: Filter by list of quote currencies
            min_liquidity: Minimum liquidity threshold in USD

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Handle nested event loops (CLI may already have one running)
        try:
            loop = asyncio.get_running_loop()
            # Already in async context - run in thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    self._fetch_pairs(
                        base_currency=base_currency,
                        min_liquidity=min_liquidity or 100000
                    )
                )
                pairs = future.result()
        except RuntimeError:
            # No event loop running - safe to use asyncio.run
            pairs = asyncio.run(self._fetch_pairs(
                base_currency=base_currency,
                min_liquidity=min_liquidity or 100000
            ))
        
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
        }
        
        for pair in pairs:
            try:
                token0_symbol = pair.get("token0", {}).get("symbol", "").upper()
                token1_symbol = pair.get("token1", {}).get("symbol", "").upper()
                
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
                
                inst_def = self._convert_pair_to_instrument(pair)
                if inst_def:
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert pair {pair.get('id')}: {e}")
                continue
        
        logger.info(f"✅ Generated {len(instruments)} Uniswap V2 instruments")
        return instruments

    async def _fetch_pairs(
        self,
        base_currency: Optional[str] = None,
        min_liquidity: float = 100000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch pairs from The Graph.
        """
        if not self._thegraph_api_key:
            logger.warning("No The Graph API key available for Uniswap V2")
            return []
        
        endpoint = self.THEGRAPH_ENDPOINT.format(api_key=self._thegraph_api_key)
        
        # Query for pairs by liquidity
        if base_currency:
            # Query by base currency
            query = """
            query GetPairs($symbol: String!, $minLiquidity: BigDecimal!) {
                pairs(
                    first: 100,
                    orderBy: reserveUSD,
                    orderDirection: desc,
                    where: {
                        reserveUSD_gt: $minLiquidity,
                        or: [
                            {token0_: {symbol_contains_nocase: $symbol}},
                            {token1_: {symbol_contains_nocase: $symbol}}
                        ]
                    }
                ) {
                    id
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
                    reserve0
                    reserve1
                    reserveUSD
                    txCount
                    createdAtTimestamp
                }
            }
            """
            variables = {"symbol": base_currency, "minLiquidity": str(min_liquidity)}
        else:
            # Query top pairs by liquidity
            query = """
            query GetPairs($minLiquidity: BigDecimal!) {
                pairs(
                    first: 100,
                    orderBy: reserveUSD,
                    orderDirection: desc,
                    where: {reserveUSD_gt: $minLiquidity}
                ) {
                    id
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
                    reserve0
                    reserve1
                    reserveUSD
                    txCount
                    createdAtTimestamp
                }
            }
            """
            variables = {"minLiquidity": str(min_liquidity)}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    json={"query": query, "variables": variables},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        logger.warning(f"The Graph returned HTTP {response.status}")
                        return []
                    
                    data = await response.json()
                    
                    if "errors" in data:
                        logger.warning(f"The Graph errors: {data['errors']}")
                        return []
                    
                    pairs = data.get("data", {}).get("pairs", [])
                    logger.info(f"Fetched {len(pairs)} Uniswap V2 pairs")
                    return pairs
                    
        except Exception as e:
            logger.error(f"Failed to fetch V2 pairs: {e}")
            return []

    def _convert_pair_to_instrument(self, pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert The Graph pair data to instrument definition.

        Args:
            pair: Pair data from The Graph

        Returns:
            Instrument definition dictionary or None
        """
        pair_id = pair.get("id")  # Pair contract address
        token0 = pair.get("token0", {})
        token1 = pair.get("token1", {})
        
        if not pair_id or not token0 or not token1:
            return None
        
        # Extract token info
        token0_symbol = token0.get("symbol", "")
        token1_symbol = token1.get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")
        token0_decimals = token0.get("decimals", "18")
        token1_decimals = token1.get("decimals", "18")
        
        # Determine base and quote (ETH as base if present)
        if "ETH" in token0_symbol.upper() or "WETH" in token0_symbol.upper():
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
            base_decimals = token0_decimals
            quote_decimals = token1_decimals
        elif "ETH" in token1_symbol.upper() or "WETH" in token1_symbol.upper():
            base_symbol = token1_symbol
            quote_symbol = token0_symbol
            base_address = token1_address
            quote_address = token0_address
            base_decimals = token1_decimals
            quote_decimals = token0_decimals
        else:
            # Default: token0 as base, token1 as quote
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
            base_decimals = token0_decimals
            quote_decimals = token1_decimals
        
        # Build symbol (V2 has no fee tiers)
        symbol = f"{base_symbol}-{quote_symbol}"
        
        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"
        
        # Get creation timestamp
        created_timestamp = pair.get("createdAtTimestamp")
        if created_timestamp:
            available_from = datetime.fromtimestamp(int(created_timestamp)).isoformat()
        else:
            available_from = datetime.now().isoformat()
        
        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "POOL",
            "symbol": symbol,
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "settle_asset": quote_symbol,
            "pool_address": pair_id,  # V2 uses "pair" but we normalize to pool_address
            "pair_address": pair_id,  # Also store as pair_address for V2 compatibility
            "pool_fee_tier": 3000,  # V2 has fixed 0.3% fee (3000 bps)
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
            "data_types": "swaps",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": f"{base_symbol}-{quote_symbol}",
        }
