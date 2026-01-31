"""
Balancer Adapter

Fetches Balancer pool instruments from Balancer API v3 (GraphQL).
Generates canonical instrument keys for Balancer pools.

Uses Balancer's official GraphQL API: https://api-v3.balancer.fi/graphql
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from instruments_service.app.venues.defi.the_graph_client import TheGraphClient
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class BalancerAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Balancer pool instruments.

    Generates instruments in format:
    BALANCER-ETH:POOL:ETH-USDC@ETHEREUM
    BALANCER-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Balancer adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM')
            subgraph_url: Optional custom subgraph URL
            api_key: The Graph API key (optional, uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain, api_key, project_id)
        # Map chain to venue suffix (ETH not ETHEREUM) to match other adapters
        chain_suffix_map = {
            "ETHEREUM": "ETH",
            "ARBITRUM": "ARB",
            "BASE": "BASE",
        }
        venue_suffix = chain_suffix_map.get(self.chain, self.chain[:3])  # Default to first 3 chars
        self.venue = f"BALANCER-{venue_suffix}"

        # Initialize Balancer API v3 client (GraphQL endpoint)
        if subgraph_url is None:
            # Use Balancer's official GraphQL API v3 (not deprecated The Graph hosted service)
            # Endpoint: https://api-v3.balancer.fi/graphql
            if self.chain == "ETHEREUM":
                # Balancer API v3 GraphQL endpoint (no API key needed for public queries)
                subgraph_url = "https://api-v3.balancer.fi/graphql"
            else:
                logger.warning(f"⚠️  Chain {self.chain} not supported yet, defaulting to Ethereum")
                subgraph_url = "https://api-v3.balancer.fi/graphql"

        # Use TheGraphClient for GraphQL queries (works with any GraphQL endpoint)
        # Pass secret_name from config so it uses the correct Secret Manager secret
        self.graph_client = TheGraphClient(
            subgraph_url=subgraph_url,
            api_key=None,
            project_id=project_id,
            secret_name=instruments_config.graph_secret_name
        )  # Balancer API doesn't need API key, but we still want correct secret name for consistency
        logger.info(
            f"✅ BalancerAdapter initialized for chain: {self.chain} (using Balancer API v3)"
        )

    # Default minimum TVL to filter out inactive/low-liquidity pools
    DEFAULT_MIN_TVL = 100_000  # $100k minimum TVL

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
        **kwargs,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Balancer pools and convert to POOL instrument definitions.

        Args:
            base_currency: Filter by single base currency symbol (e.g., 'ETH')
            base_currency_list: Filter by list of base currencies (e.g., ['WETH', 'WBTC'])
            quote_currency_list: Optional list of allowed quote currencies (filters pools where quote is in this list)
            min_liquidity: Minimum liquidity threshold in USD (default: $100k)
            **kwargs: Additional arguments

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Use default min liquidity if not specified to filter out inactive pools
        if min_liquidity is None:
            min_liquidity = self.DEFAULT_MIN_TVL
            logger.info(f"📊 Using default min TVL: ${min_liquidity:,.0f}")

        # Query Balancer pools from API (ordered by TVL descending)
        # Note: _query_balancer_pools already filters by base_currency_list with wrapped variants
        pools = self._query_balancer_pools(base_currency, base_currency_list, min_liquidity)

        instruments = {}

        # Build expanded set of allowed tokens (base + quote + wrapped variants)
        allowed_tokens = set()
        if quote_currency_list:
            allowed_tokens.update(q.upper() for q in quote_currency_list)
        if base_currency_list:
            allowed_tokens.update(b.upper() for b in base_currency_list)
        if base_currency:
            allowed_tokens.add(base_currency.upper())

        # Add wrapped/staked variants for better matching
        wrapped_mappings = {
            "ETH": ["WETH", "STETH", "WSTETH", "RETH", "CBETH", "WEETH", "OSETH", "TETH"],
            "BTC": ["WBTC", "TBTC", "RENBTC", "SBTC"],
            "USD": ["USDC", "USDT", "DAI", "USDE", "SUSDE", "GHO", "FRAX", "LUSD", "SUSD", "GUSD"],
        }
        expanded_tokens = set(allowed_tokens)
        for token in allowed_tokens:
            for canonical, variants in wrapped_mappings.items():
                if token == canonical or token in variants:
                    expanded_tokens.add(canonical)
                    expanded_tokens.update(variants)
        allowed_tokens = expanded_tokens

        for pool in pools:
            try:
                tokens = pool.get("tokens", [])
                token_symbols = [t.get("symbol", "").upper() for t in tokens]

                # Relaxed filter: Pool is valid if it has at least 1 token in our allowed list
                # (The TVL filter already ensures these are active, high-quality pools)
                if allowed_tokens:
                    tokens_matched = sum(1 for sym in token_symbols if sym in allowed_tokens)
                    if tokens_matched < 1:
                        continue  # Skip pools with no recognized tokens

                # Convert pool to instrument (uses first two tokens as base/quote)
                inst_def = self._convert_pool_to_instrument(pool, base_currency, allowed_tokens)
                if inst_def and self._validate_instrument_definition(inst_def):
                    # Add TVL for sorting/filtering
                    inst_def["tvl_usd"] = float(pool.get("totalLiquidity", 0))
                    inst_def["volume_24h_usd"] = float(pool.get("volume24h", 0))
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert Balancer pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Balancer POOL instruments (min TVL: ${min_liquidity:,.0f})")
        return instruments

    def fetch_spot_pairs(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Balancer spot trading pairs (SPOT_PAIR instrument type).

        Args:
            base_currency: Filter by single base currency symbol
            base_currency_list: Filter by list of base currencies
            quote_currency_list: Optional list of allowed quote currencies

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        pools = self.fetch_pools(
            base_currency=base_currency,
            base_currency_list=base_currency_list,
            quote_currency_list=quote_currency_list,
            **kwargs,
        )

        # Convert POOL instruments to SPOT_PAIR instruments
        spot_pairs = {}

        for pool_key, pool_def in pools.items():
            # Create SPOT_PAIR version
            spot_key = pool_key.replace(":POOL:", ":SPOT_PAIR:")
            spot_key = (
                spot_key.split(":")[0]
                + ":"
                + spot_key.split(":")[1]
                + ":"
                + pool_def["base_asset"]
                + "-"
                + pool_def["quote_asset"]
                + f"@{self.chain}"
            )

            spot_def = pool_def.copy()
            spot_def["instrument_key"] = spot_key
            spot_def["instrument_type"] = "SPOT_PAIR"
            spot_def["symbol"] = f"{pool_def['base_asset']}-{pool_def['quote_asset']}"
            # Remove pool-specific fields
            spot_def.pop("pool_address", None)
            spot_def.pop("pool_fee_tier", None)

            spot_pairs[spot_key] = spot_def

        return spot_pairs

    def fetch_instrument_definitions(
        self, base_currency: Optional[str] = None, min_liquidity: Optional[float] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Balancer pools and convert to instrument definitions.

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            min_liquidity: Minimum liquidity threshold in USD

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Query Balancer pools from The Graph
        pools = self._query_balancer_pools(base_currency, min_liquidity)

        instruments = {}

        for pool in pools:
            try:
                inst_def = self._convert_pool_to_instrument(pool)
                if inst_def and self._validate_instrument_definition(inst_def):
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert Balancer pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Balancer instruments")
        return instruments

    def _query_balancer_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query Balancer pools from Balancer API v3 (GraphQL).

        Args:
            base_currency: Filter by single base currency (e.g., 'ETH')
            base_currency_list: Filter by list of base currencies (e.g., ['WETH', 'WBTC', 'ETH'])
            min_liquidity: Minimum liquidity threshold in USD (TVL)
                - Default: $100,000 to filter out low-liquidity/inactive pools
                - Pools ordered by TVL descending to get most active pools first

        Returns:
            List of pool dictionaries
        """
        # Build GraphQL query for Balancer API v3
        # Include chainIn filter for Ethereum mainnet and minTvl for liquidity
        where_clauses = ["chainIn: [MAINNET]"]
        if min_liquidity:
            where_clauses.append(f"minTvl: {min_liquidity}")

        where_str = ", ".join(where_clauses)

        query = f"""
        {{
            poolGetPools(
                first: 500
                where: {{ {where_str} }}
                orderBy: totalLiquidity
                orderDirection: desc
            ) {{
                id
                address
                name
                poolTokens {{
                    address
                    symbol
                    decimals
                    name
                }}
                dynamicData {{
                    totalLiquidity
                    volume24h
                }}
            }}
        }}
        """

        try:
            result = self.graph_client.execute_query_sync(query)
            if result.get("errors"):
                errors = result.get("errors", [])
                error_str = str(errors).lower()
                if "removed" in error_str or "deprecated" in error_str:
                    logger.warning(
                        f"⚠️ Balancer API endpoint deprecated or unavailable. "
                        f"Skipping Balancer instruments for {self.chain}."
                    )
                    return []
                else:
                    logger.error(f"Balancer API query failed: {errors}")
                    return []

            pools_data = result.get("data", {}).get("poolGetPools", [])
            logger.info(f"📊 Balancer API returned {len(pools_data)} pools with TVL >= ${min_liquidity:,.0f}")

            # Build set of allowed base currencies (including wrapped variants)
            allowed_bases = None
            if base_currency_list:
                allowed_bases = {b.upper() for b in base_currency_list}
                # Add common wrapped/staked variants
                wrapped_mappings = {
                    "ETH": ["WETH", "STETH", "WSTETH", "RETH", "CBETH", "WEETH", "OSETH", "TETH"],
                    "BTC": ["WBTC", "TBTC", "RENBTC", "SBTC"],
                    "USD": ["USDC", "USDT", "DAI", "USDE", "SUSDE", "GHO", "FRAX", "LUSD", "SUSD", "GUSD"],
                }
                expanded_bases = set()
                for base in allowed_bases:
                    expanded_bases.add(base)
                    # Add wrapped variants
                    for canonical, variants in wrapped_mappings.items():
                        if base == canonical or base in variants:
                            expanded_bases.add(canonical)
                            expanded_bases.update(variants)
                allowed_bases = expanded_bases
                logger.info(f"📊 Filtering for base currencies: {sorted(allowed_bases)[:10]}...")
            elif base_currency:
                allowed_bases = {base_currency.upper()}

            # Convert Balancer API v3 format to our expected format
            pools = []
            for pool in pools_data:
                # Extract tokens from poolTokens
                tokens = []
                for token in pool.get("poolTokens", []):
                    tokens.append(
                        {
                            "id": token.get("address", ""),
                            "symbol": token.get("symbol", ""),
                            "decimals": token.get("decimals", 18),
                            "name": token.get("name", ""),
                        }
                    )

                # Filter by allowed base currencies
                if allowed_bases:
                    token_symbols = [t.get("symbol", "").upper() for t in tokens]
                    # Check if ANY token in pool matches allowed bases
                    if not any(sym in allowed_bases for sym in token_symbols):
                        continue  # Skip pools that don't contain any allowed base currency

                # Convert to expected format
                # IMPORTANT: Use full pool ID (with suffix) for querying, not just address
                # The full ID is required for poolEvents queries (e.g., swap history)
                tvl = pool.get("dynamicData", {}).get("totalLiquidity", 0)
                vol24h = pool.get("dynamicData", {}).get("volume24h", 0)

                converted_pool = {
                    "id": pool.get("id", pool.get("address", "")),  # Prefer full ID over address
                    "address": pool.get("address", ""),  # Store address separately for reference
                    "tokens": tokens,
                    "totalLiquidity": tvl,
                    "volume24h": vol24h,
                    "name": pool.get("name", ""),
                }

                pools.append(converted_pool)

            logger.info(f"✅ Fetched {len(pools)} Balancer pools matching filter criteria")
            return pools

        except Exception as e:
            logger.warning(
                f"⚠️ Failed to query Balancer API v3: {e}. Skipping Balancer instruments."
            )
            return []

    def _convert_pool_to_instrument(
        self,
        pool: Dict[str, Any],
        base_currency: Optional[str] = None,
        allowed_quotes: Optional[set] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Convert Balancer pool to instrument definition.

        Args:
            pool: Pool data from Balancer API
            base_currency: Base currency symbol (if filtering by base)
            allowed_quotes: Set of allowed quote currencies (if filtering by quote)

        Returns:
            Instrument definition dictionary
        """
        # pool_id is the FULL pool ID (with suffix) needed for API queries
        # pool_address is just the contract address (for on-chain execution)
        pool_id = pool.get("id")  # Full ID like: 0x3de27efa...000200000000000000000588
        pool_address = pool.get("address", pool_id)  # Contract address like: 0x3de27efa...
        tokens = pool.get("tokens", [])

        if len(tokens) < 2:
            return None

        # Determine base and quote based on filtering
        if base_currency and allowed_quotes:
            # Find base token
            base_token = None
            quote_token = None
            base_upper = base_currency.upper()

            for token in tokens:
                token_symbol = token.get("symbol", "").upper()
                if token_symbol == base_upper:
                    base_token = token
                elif token_symbol in allowed_quotes and quote_token is None:
                    quote_token = token

            if not base_token or not quote_token:
                return None  # Required tokens not found

            base_symbol = base_token.get("symbol", "UNKNOWN")
            quote_symbol = quote_token.get("symbol", "UNKNOWN")
            base_address = base_token.get("id", "")
            quote_address = quote_token.get("id", "")
        else:
            # Use first two tokens for pair (default)
            token0 = tokens[0]
            token1 = tokens[1]
            base_symbol = token0.get("symbol", "UNKNOWN")
            quote_symbol = token1.get("symbol", "UNKNOWN")
            base_address = token0.get("id", "")
            quote_address = token1.get("id", "")

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{base_symbol}-{quote_symbol}{chain_suffix}"

        # Get creation timestamp from pool (if available)
        created_timestamp = pool.get("createdAtTimestamp") or pool.get("createdTimestamp")
        if created_timestamp:
            available_from = datetime.fromtimestamp(int(created_timestamp)).isoformat()
        else:
            # Balancer V2 launched on Ethereum: 2021-05-03
            # If no timestamp, use protocol launch date as fallback
            available_from = datetime(2021, 5, 3).isoformat()

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "POOL",
            "symbol": f"{base_symbol}-{quote_symbol}",
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "settle_asset": quote_symbol,
            "base_asset_contract_address": base_address,
            "quote_asset_contract_address": quote_address,
            # pool_id: FULL pool ID with suffix (required for Balancer API queries like poolEvents)
            # Example: 0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588
            "pool_id": pool_id,
            # pool_address: Contract address only (for on-chain execution)
            # Example: 0x3de27efa2f1aa663ae5d458857e731c129069f29
            "pool_address": pool_address,
            "pool_fee_tier": None,  # Balancer uses swapFee (percentage)
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, BASE, etc.)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "balancer_api_v3",
            "tardis_exchange": "",
            "tardis_symbol": "",
            # exchange_raw_symbol: Native exchange identifier (pool address for Balancer)
            # This is the identifier used directly by the exchange/protocol for execution
            "exchange_raw_symbol": pool_address,  # Pool contract address (native identifier)
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,  # Pool creation date or protocol launch (2021-05-03)
            "available_to_datetime": None,
            "data_types": "swaps",  # DEX pool swap events from Balancer subgraph
        }
