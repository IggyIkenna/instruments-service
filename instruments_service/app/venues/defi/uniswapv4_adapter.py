"""
Uniswap V4 Adapter

Fetches Uniswap V4 pool instruments from The Graph.
Generates canonical instrument keys for DEX pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md

Note: Uniswap V4 uses a hook-based architecture with singleton pattern.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from .the_graph_client import TheGraphClient

logger = logging.getLogger(__name__)


class UniswapV4Adapter:
    """
    Adapter for fetching Uniswap V4 pool instruments.

    Generates instruments in format:
    UNISWAPV4-ETH:POOL:USDC-ETH@ETHEREUM
    UNISWAPV4-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM

    Note: Uniswap V4 uses hook-based architecture and singleton pattern.
    Pool addresses are computed differently than V2/V3.
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Uniswap V4 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM', 'BASE')
            subgraph_url: Optional custom subgraph URL
            api_key: The Graph API key (optional, uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager
        """
        self.chain = chain.upper()
        # Map chain to venue suffix (ETH not ETHEREUM)
        chain_suffix_map = {
            "ETHEREUM": "ETH",
            "ARBITRUM": "ARB",
            "BASE": "BASE",
        }
        venue_suffix = chain_suffix_map.get(
            self.chain, self.chain[:3]
        )  # Default to first 3 chars
        self.venue = f"UNISWAPV4-{venue_suffix}"

        # Initialize The Graph client
        if subgraph_url is None:
            # Use SubgraphService to resolve Uniswap V4 subgraph URL
            # Note: Uniswap V4 subgraph endpoint was removed - no public subgraph available yet
            from instruments_service.app.core.subgraph_service import SubgraphService

            subgraph_service = SubgraphService()
            subgraph_url = subgraph_service.get_subgraph_url(
                "uniswap_v4", self.chain, api_key=api_key
            )

            # If SubgraphService returns None, skip Uniswap V4 (no valid endpoint available)
            if not subgraph_url:
                logger.info(
                    f"ℹ️  Skipping Uniswap V4 adapter for {self.chain} - no valid subgraph endpoint available. "
                    f"Uniswap V4 launched Jan 31, 2025, but public subgraph endpoints have been removed."
                )
                self.graph_client = None
                return

        self.graph_client = TheGraphClient(
            subgraph_url, api_key=api_key, project_id=project_id
        )
        self.project_id = project_id
        logger.info(f"✅ UniswapV4Adapter initialized for chain: {self.chain}")

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V4 pools and convert to instrument definitions.

        Tries multiple data sources in order:
        1. The Graph Network gateway (if subgraph ID available)
        2. Envio indexer (fallback - Option 3)
        3. RPC direct contract queries (fallback - Option 2)
        4. Returns empty if all fail

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            quote_currency_list: Optional list of allowed quote currencies (filters pools where quote is in this list)
            min_liquidity: Minimum liquidity threshold in USD

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        pools = []

        # Option 1: Try The Graph Network gateway first
        if self.graph_client:
            logger.info("🔄 Trying Uniswap V4 pools from The Graph Network gateway...")
            try:
                if base_currency:
                    pools = self.graph_client.query_pools_by_base_currency(
                        base_currency
                    )
                else:
                    pools = self.graph_client.query_pools(min_liquidity=min_liquidity)
                if pools:
                    logger.info(
                        f"✅ Fetched {len(pools)} Uniswap V4 pools from The Graph"
                    )
            except Exception as e:
                logger.debug(f"The Graph query failed: {e}")

        # Option 3: Fallback to Envio indexer
        if not pools:
            logger.info("🔄 Trying Uniswap V4 pools from Envio indexer...")
            try:
                from .envio_client import EnvioClient

                envio_client = EnvioClient(project_id=self.project_id)
                if base_currency:
                    pools = envio_client.query_pools_by_token(
                        base_currency, chain_id="1"
                    )
                else:
                    pools = envio_client.query_pools(chain_id="1", limit=1000)
                if pools:
                    logger.info(f"✅ Fetched {len(pools)} Uniswap V4 pools from Envio")
                    # Convert Envio format to The Graph format for compatibility
                    pools = self._convert_envio_pools(pools)
            except Exception as e:
                logger.debug(f"Envio query failed: {e}")

        # Option 2: Fallback to RPC (complex - would need event tracking)
        # Note: Uniswap V4 doesn't have a simple "getAllPools" function
        # Would need to track PoolInitialized events - skipping for MVP

        # If still no pools, return empty
        if not pools:
            logger.warning("⚠️ Uniswap V4 adapter: No pools found from any data source")
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
                token0_symbol = pool.get("token0", {}).get("symbol", "").upper()
                token1_symbol = pool.get("token1", {}).get("symbol", "").upper()

                # Filter by base and quote currencies
                if allowed_bases or allowed_quotes:
                    if base_currency:
                        # When querying by specific base_currency, ensure it's in MVP base list and quote is in MVP quote list
                        base_upper = base_currency.upper()

                        # Check if base currency is in MVP list (or wrapped version)
                        base_in_mvp = (
                            base_upper in allowed_bases if allowed_bases else True
                        )
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
                        quote_in_mvp = (
                            quote_symbol in allowed_quotes if allowed_quotes else True
                        )
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
                        token0_in_bases = (
                            token0_symbol in allowed_bases if allowed_bases else True
                        )
                        token1_in_bases = (
                            token1_symbol in allowed_bases if allowed_bases else True
                        )
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
                logger.warning(f"Failed to convert pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Uniswap V4 instruments")
        return instruments

    def _convert_envio_pools(
        self, envio_pools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Convert Envio pool format to The Graph format for compatibility.

        Args:
            envio_pools: List of pools from Envio API

        Returns:
            List of pools in The Graph format
        """
        converted = []
        for pool in envio_pools:
            # Envio format: {id, name, token0, token1, ...}
            # The Graph format: {id, token0: {symbol, ...}, token1: {symbol, ...}, ...}
            converted_pool = {
                "id": pool.get("id", ""),
                "token0": {
                    "id": pool.get("token0", ""),
                    "symbol": "",  # Would need to fetch from token contract
                    "decimals": 18,
                },
                "token1": {
                    "id": pool.get("token1", ""),
                    "symbol": "",  # Would need to fetch from token contract
                    "decimals": 18,
                },
                "feeTier": None,  # Uniswap V4 uses different fee structure
                "liquidity": None,
                "totalValueLockedUSD": pool.get("totalValueLockedUSD"),
                "createdAtTimestamp": None,
            }
            converted.append(converted_pool)
        return converted

    def _convert_pool_to_instrument(
        self, pool: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Convert The Graph pool data to instrument definition.

        Args:
            pool: Pool data from The Graph

        Returns:
            Instrument definition dictionary or None
        """
        pool_id = pool.get("id")  # Pool contract address (computed differently in V4)
        token0 = pool.get("token0", {})
        token1 = pool.get("token1", {})
        fee_tier = pool.get("feeTier")  # V4 may have different fee structures

        if not pool_id or not token0 or not token1:
            return None

        # Extract token info
        token0_symbol = token0.get("symbol", "")
        token1_symbol = token1.get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")
        token0_decimals = token0.get("decimals", 18)
        token1_decimals = token1.get("decimals", 18)

        # CRITICAL: Skip Curve LP tokens (they start with "crv" prefix)
        # Curve LP tokens should be handled by CurveAdapter, not Uniswap adapters
        if token0_symbol.upper().startswith("CRV") or token1_symbol.upper().startswith(
            "CRV"
        ):
            logger.debug(
                f"Skipping pool {pool_id}: Contains Curve LP token ({token0_symbol}/{token1_symbol})"
            )
            return None

        # CRITICAL: Skip Balancer LP tokens (they start with "BPT" or "bb-" prefix)
        if (
            token0_symbol.upper().startswith("BPT")
            or token1_symbol.upper().startswith("BPT")
            or token0_symbol.upper().startswith("BB-")
            or token1_symbol.upper().startswith("BB-")
        ):
            logger.debug(
                f"Skipping pool {pool_id}: Contains Balancer LP token ({token0_symbol}/{token1_symbol})"
            )
            return None

        # Determine base and quote (use ETH as base if present)
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

        # Build symbol (V4 may have fee tiers or hooks)
        if fee_tier is not None:
            symbol = f"{base_symbol}-{quote_symbol}:{fee_tier}"
        else:
            symbol = f"{base_symbol}-{quote_symbol}"

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"

        # Convert fee tier if present (V4 may have different fee structures)
        if fee_tier is not None:
            fee_bps = int(fee_tier)  # Already in basis points
        else:
            fee_bps = None  # V4 hooks may define custom fees

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
            "pool_fee_tier": fee_bps,  # May be None for V4 hooks
            "base_asset_contract_address": base_address,
            "quote_asset_contract_address": quote_address,
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, BASE, etc.)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "the_graph",
            "tardis_exchange": "",
            "tardis_symbol": "",
            # exchange_raw_symbol: Native exchange identifier (pool address for Uniswap)
            # This is the identifier used directly by the exchange/protocol for execution
            "exchange_raw_symbol": pool_id,  # Pool contract address (native identifier)
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Pools don't expire
            # data_types: Available market data types for download
            # Uniswap V4 has swap transactions (trades), not order books
            "data_types": "swaps",  # DEX pools have swap transactions (different from trades for backtesting)
            "inverse": False,
            "contract_size": None,
            "tick_size": "",  # V4 uses hook-based architecture
            "min_size": "0",  # No protocol-enforced minimum. Practical minimum ~$10-50 based on gas costs and slippage (~$5-30 gas for swaps)
            "underlying": f"{base_symbol}-{quote_symbol}",
        }

    def fetch_spot_pairs(
        self,
        base_currency: Optional[str] = None,
        quote_currency_list: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V4 spot trading pairs (SPOT_PAIR instrument type).

        Args:
            base_currency: Filter by base currency symbol
            quote_currency_list: Optional list of allowed quote currencies

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        pools = self.fetch_pools(
            base_currency=base_currency, quote_currency_list=quote_currency_list
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
