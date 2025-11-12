"""
Curve Adapter

Fetches Curve pool instruments from The Graph.
Generates canonical instrument keys for Curve pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

from .the_graph_client import TheGraphClient

logger = logging.getLogger(__name__)


class CurveAdapter:
    """
    Adapter for fetching Curve pool instruments.

    Generates instruments in format:
    CURVE-ETH:POOL:ETH-USDT@ETHEREUM
    CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Curve adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM')
            subgraph_url: Optional custom subgraph URL
            api_key: Optional The Graph API key (uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        self.chain = chain.upper()
        self.venue = f"CURVE-{chain.upper()}"

        # Initialize The Graph client
        if subgraph_url is None:
            # Use SubgraphService to resolve Curve subgraph URL (uses Context7 + API key)
            from instruments_service.app.core.subgraph_service import SubgraphService

            subgraph_service = SubgraphService()
            subgraph_url = subgraph_service.get_subgraph_url("curve", self.chain, api_key=api_key)

            # If SubgraphService returns None, skip Curve (no valid endpoint available)
            if not subgraph_url:
                logger.info(
                    f"ℹ️  Skipping Curve adapter for {self.chain} - no valid subgraph endpoint available."
                )
                self.graph_client = None
                return

        self.graph_client = TheGraphClient(
            subgraph_url=subgraph_url, api_key=api_key, project_id=project_id
        )
        self.project_id = project_id
        logger.info(f"✅ CurveAdapter initialized for chain: {self.chain}")

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Curve pools and convert to instrument definitions.

        Tries multiple data sources in order:
        1. The Graph Network gateway (if subgraph ID available)
        2. RPC direct contract queries (fallback)
        3. Returns empty if all fail

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            quote_currency_list: Filter by quote currencies (MVP list) - only include pools where quote is in this list
            min_liquidity: Minimum liquidity threshold in USD

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        pools = []

        # Option 1: Try The Graph subgraph first
        if self.graph_client:
            logger.info("🔄 Trying Curve pools from The Graph subgraph...")
            pools = self._query_curve_pools(base_currency, min_liquidity)
            if pools:
                logger.info(f"✅ Fetched {len(pools)} Curve pools from The Graph")

        # Option 2: Fallback to RPC if Graph fails
        if not pools:
            logger.info("🔄 Trying Curve pools from RPC (direct contract queries)...")
            try:
                from .curve_rpc_adapter import CurveRPCAdapter

                rpc_adapter = CurveRPCAdapter(project_id=self.project_id)
                pools = rpc_adapter.fetch_pools(base_currency=base_currency)
                if pools:
                    logger.info(f"✅ Fetched {len(pools)} Curve pools via RPC")
            except Exception as e:
                logger.debug(f"RPC fallback failed: {e}")

        # If still no pools, return empty
        if not pools:
            logger.warning("⚠️ Curve adapter: No pools found from any data source")
            return {}

        instruments = {}

        # Normalize base and quote currency lists for comparison
        allowed_bases = None
        allowed_quotes = None
        if base_currency_list:
            allowed_bases = {b.upper() for b in base_currency_list}
        if quote_currency_list:
            allowed_quotes = {q.upper() for q in quote_currency_list}

        # Wrapped/staked token mappings
        wrapped_mappings = {
            "WETH": "ETH",
            "WSTETH": "ETH",
            "WEETH": "ETH",
            "STETH": "ETH",
        }

        for pool in pools:
            try:
                coins = pool.get("coins", [])
                if len(coins) < 2:
                    continue

                token0_symbol = coins[0].get("symbol", "").upper()
                token1_symbol = coins[1].get("symbol", "").upper()

                # Filter by base and quote currencies
                if allowed_bases or allowed_quotes:
                    if base_currency:
                        # When querying by specific base_currency, ensure it's in MVP base list and quote is in MVP quote list
                        base_upper = base_currency.upper()

                        # Check if base currency is in MVP list (or wrapped version)
                        base_in_mvp = base_upper in allowed_bases if allowed_bases else True
                        if not base_in_mvp and base_upper in wrapped_mappings:
                            base_in_mvp = (
                                wrapped_mappings[base_upper] in allowed_bases
                                if allowed_bases
                                else True
                            )
                        if allowed_bases and not base_in_mvp:
                            continue

                        # Determine which token is base and which is quote
                        if token0_symbol == base_upper:
                            quote_symbol = token1_symbol
                        elif token1_symbol == base_upper:
                            quote_symbol = token0_symbol
                        else:
                            continue  # Base currency not in pool, skip

                        # CRITICAL: Quote MUST be in MVP list (or wrapped version)
                        quote_in_mvp = quote_symbol in allowed_quotes if allowed_quotes else True
                        if not quote_in_mvp and quote_symbol in wrapped_mappings:
                            quote_in_mvp = (
                                wrapped_mappings[quote_symbol] in allowed_quotes
                                if allowed_quotes
                                else True
                            )
                        if allowed_quotes and not quote_in_mvp:
                            continue
                    else:
                        # No specific base filter - require at least one token in base list AND one in quote list
                        token0_in_bases = token0_symbol in allowed_bases if allowed_bases else True
                        token1_in_bases = token1_symbol in allowed_bases if allowed_bases else True
                        token0_in_quotes = (
                            token0_symbol in allowed_quotes if allowed_quotes else True
                        )
                        token1_in_quotes = (
                            token1_symbol in allowed_quotes if allowed_quotes else True
                        )

                        # Check wrapped versions
                        if not token0_in_bases and token0_symbol in wrapped_mappings:
                            token0_in_bases = (
                                wrapped_mappings[token0_symbol] in allowed_bases
                                if allowed_bases
                                else True
                            )
                        if not token1_in_bases and token1_symbol in wrapped_mappings:
                            token1_in_bases = (
                                wrapped_mappings[token1_symbol] in allowed_bases
                                if allowed_bases
                                else True
                            )
                        if not token0_in_quotes and token0_symbol in wrapped_mappings:
                            token0_in_quotes = (
                                wrapped_mappings[token0_symbol] in allowed_quotes
                                if allowed_quotes
                                else True
                            )
                        if not token1_in_quotes and token1_symbol in wrapped_mappings:
                            token1_in_quotes = (
                                wrapped_mappings[token1_symbol] in allowed_quotes
                                if allowed_quotes
                                else True
                            )

                        # Require: (token0 in bases AND token1 in quotes) OR (token1 in bases AND token0 in quotes)
                        valid_pair = False
                        if allowed_bases and allowed_quotes:
                            valid_pair = (token0_in_bases and token1_in_quotes) or (
                                token1_in_bases and token0_in_quotes
                            )
                        elif allowed_bases:
                            valid_pair = token0_in_bases or token1_in_bases
                        elif allowed_quotes:
                            valid_pair = token0_in_quotes or token1_in_quotes

                        if not valid_pair:
                            continue  # Skip if pair doesn't meet base/quote requirements

                inst_def = self._convert_pool_to_instrument(pool)
                if inst_def:
                    instruments[inst_def["instrument_key"]] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert Curve pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Curve instruments")
        return instruments

    def _query_curve_pools(
        self, base_currency: Optional[str] = None, min_liquidity: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Query Curve pools from The Graph subgraph.

        Args:
            base_currency: Filter by base currency
            min_liquidity: Minimum liquidity threshold

        Returns:
            List of pool dictionaries
        """
        # Build GraphQL query for Curve pools
        where_clause = []
        if min_liquidity:
            where_clause.append(f'totalValueLockedUSD_gte: "{min_liquidity}"')

        where_str = ", ".join(where_clause) if where_clause else ""

        query = f"""
        {{
            pools(
                first: 1000
                {f'where: {{ {where_str} }}' if where_str else ''}
                orderBy: totalValueLockedUSD
                orderDirection: desc
            ) {{
                id
                name
                symbol
                coins {{
                    id
                    symbol
                    decimals
                }}
                totalValueLockedUSD
                createdAtTimestamp
            }}
        }}
        """

        try:
            import requests

            headers = {"Content-Type": "application/json"}

            response = requests.post(
                self.graph_client.subgraph_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()

            data = response.json()
            if "errors" in data:
                errors = data.get("errors", [])
                # Check if endpoint has been removed (deprecated endpoints)
                error_messages = [str(e.get("message", "")).lower() for e in errors]
                if any(
                    "removed" in msg
                    or "deprecated" in msg
                    or "endpoint" in msg
                    or "not found" in msg
                    for msg in error_messages
                ):
                    logger.debug(
                        f"Curve subgraph endpoint deprecated or unavailable: {self.graph_client.subgraph_url}"
                    )
                    return []
                else:
                    logger.error(f"Curve GraphQL query errors: {errors}")
                    return []

            pools = data.get("data", {}).get("pools", [])

            # Filter by base currency if specified
            if base_currency:
                pools = [
                    p
                    for p in pools
                    if any(
                        coin.get("symbol", "").upper() == base_currency.upper()
                        for coin in p.get("coins", [])
                    )
                ]

            logger.info(f"✅ Fetched {len(pools)} Curve pools from The Graph")
            return pools

        except Exception as e:
            logger.error(f"Failed to query Curve pools from The Graph: {e}")
            return []

    def _convert_pool_to_instrument(self, pool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Curve pool data to instrument definition.

        Args:
            pool: Pool data from The Graph

        Returns:
            Instrument definition dictionary or None
        """
        pool_id = pool.get("id")
        coins = pool.get("coins", [])

        if not pool_id or len(coins) < 2:
            return None

        # Extract token info (Curve pools can have multiple coins, use first two)
        token0 = coins[0]
        token1 = coins[1]

        token0_symbol = token0.get("symbol", "")
        token1_symbol = token1.get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")

        # Determine base and quote (use ETH as base if present)
        if "ETH" in token0_symbol.upper() or "WETH" in token0_symbol.upper():
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
        elif "ETH" in token1_symbol.upper() or "WETH" in token1_symbol.upper():
            base_symbol = token1_symbol
            quote_symbol = token0_symbol
            base_address = token1_address
            quote_address = token0_address
        else:
            # Default: token0 as base
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address

        # Build symbol
        symbol = f"{base_symbol}-{quote_symbol}"

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"

        # Get creation timestamp
        created_timestamp = pool.get("createdAtTimestamp")
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
            "pool_address": pool_id,
            "pool_fee_tier": None,  # Curve uses different fee model
            "base_asset_contract_address": base_address,
            "quote_asset_contract_address": quote_address,
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, etc.)
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
            "data_types": "trades",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": f"{base_symbol}-{quote_symbol}",
        }

    def fetch_spot_pairs(self, base_currency: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Curve spot trading pairs (SPOT_PAIR instrument type).

        Args:
            base_currency: Filter by base currency symbol

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        pools = self.fetch_pools(base_currency=base_currency)

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
