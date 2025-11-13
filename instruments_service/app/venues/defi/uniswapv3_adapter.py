"""
Uniswap V3 Adapter

Fetches Uniswap V3 pool instruments from The Graph.
Generates canonical instrument keys for DEX pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

from instruments_service.app.venues.defi.the_graph_client import TheGraphClient
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)


class UniswapV3Adapter(BaseDefiAdapter):
    """
    Adapter for fetching Uniswap V3 pool instruments.

    Generates instruments in format:
    UNISWAPV3-ETH:POOL:USDC-ETH:5@ETHEREUM
    UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Uniswap V3 adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM', 'BASE')
            subgraph_url: Optional custom subgraph URL
            api_key: Optional The Graph API key (uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        super().__init__(chain, api_key, project_id)
        # Map chain to venue format matching config.py
        chain_to_venue = {
            "ETHEREUM": "UNISWAPV3-ETH",
            "ARBITRUM": "UNISWAPV3-ARB",
            "BASE": "UNISWAPV3-BASE",
        }
        self.venue = chain_to_venue.get(self.chain, f"UNISWAPV3-{self.chain}")

        # Use provided API key or check TheGraphClient's module-level cache
        # If not provided, TheGraphClient will handle retrieval with caching
        if not self.api_key:
            # Check if TheGraphClient has cached it (module-level cache)
            # Import here to avoid circular dependency
            try:
                from instruments_service.app.venues.defi.the_graph_client import (
                    _API_KEY_CACHE,
                    _API_KEY_PROJECT_ID,
                )

                project_id_check = self.project_id or os.getenv(
                    "GCP_PROJECT_ID", "central-element-323112"
                )
                if _API_KEY_CACHE and _API_KEY_PROJECT_ID == project_id_check:
                    self.api_key = _API_KEY_CACHE
                    logger.debug("✅ Using cached Graph API key in UniswapV3Adapter")
            except (ImportError, AttributeError):
                # Cache not available, let TheGraphClient handle it
                pass

        # Initialize The Graph client
        if subgraph_url is None:
            # Updated Uniswap V3 subgraph URLs - using The Graph Network endpoints
            # Old endpoints (api.thegraph.com) have been deprecated
            # Format: https://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/<SUBGRAPH_ID>
            # Uniswap V3 Ethereum subgraph ID: 5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV

            if self.api_key:
                # Use The Graph Network endpoint with API key
                subgraph_urls = {
                    "ETHEREUM": f"https://gateway.thegraph.com/api/{self.api_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
                    "ARBITRUM": os.getenv(
                        "THE_GRAPH_UNISWAP_V3_ARB_URL",
                        f"https://gateway-arbitrum.network.thegraph.com/api/{self.api_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
                    ),
                    "BASE": os.getenv(
                        "THE_GRAPH_UNISWAP_V3_BASE_URL",
                        f"https://gateway.thegraph.com/api/{self.api_key}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
                    ),
                }
            else:
                # Fallback: Use Studio endpoint (rate-limited, for testing only)
                logger.warning(
                    "⚠️ No The Graph API key found - using Studio endpoint (rate-limited)"
                )
                subgraph_urls = {
                    "ETHEREUM": os.getenv(
                        "THE_GRAPH_UNISWAP_V3_URL",
                        "https://api.studio.thegraph.com/query/50688/uniswap-v3/version/latest",
                    ),
                    "ARBITRUM": os.getenv(
                        "THE_GRAPH_UNISWAP_V3_ARB_URL",
                        "https://api.studio.thegraph.com/query/50688/uniswap-v3-arbitrum/version/latest",
                    ),
                    "BASE": os.getenv(
                        "THE_GRAPH_UNISWAP_V3_BASE_URL",
                        "https://api.studio.thegraph.com/query/50688/uniswap-v3-base/version/latest",
                    ),
                }
            subgraph_url = subgraph_urls.get(self.chain, subgraph_urls["ETHEREUM"])

        self.graph_client = TheGraphClient(
            subgraph_url=subgraph_url, api_key=self.api_key, project_id=self.project_id
        )
        logger.info(f"✅ UniswapV3Adapter initialized for chain: {self.chain}")

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V3 pools and convert to instrument definitions.

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            quote_currency_list: Filter by quote currencies (MVP list) - only include pools where quote is in this list
            min_liquidity: Minimum liquidity threshold in USD

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        if base_currency:
            pools = self.graph_client.query_pools_by_base_currency(base_currency)
        else:
            pools = self.graph_client.query_pools(min_liquidity=min_liquidity)

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
                logger.warning(f"Failed to convert pool {pool.get('id')}: {e}")
                continue

        logger.info(f"✅ Generated {len(instruments)} Uniswap V3 instruments")
        return instruments

    def _convert_pool_to_instrument(self, pool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert The Graph pool data to instrument definition.

        Args:
            pool: Pool data from The Graph

        Returns:
            Instrument definition dictionary or None
        """
        pool_id = pool.get("id")  # Pool contract address
        token0 = pool.get("token0", {})
        token1 = pool.get("token1", {})
        fee_tier = pool.get("feeTier")

        if not pool_id or not token0 or not token1 or fee_tier is None:
            return None

        # Extract token info
        token0_symbol = token0.get("symbol", "")
        token1_symbol = token1.get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")
        token0_decimals = token0.get("decimals", 18)
        token1_decimals = token1.get("decimals", 18)

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

        # Build symbol with fee tier
        symbol = f"{base_symbol}-{quote_symbol}:{fee_tier}"

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"

        # Convert fee tier from Uniswap format (e.g., 3000 = 0.3%) to basis points
        fee_bps = int(fee_tier)  # Already in basis points (3000 = 0.3%)

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
            "pool_fee_tier": fee_bps,
            "base_asset_contract_address": base_address,
            "quote_asset_contract_address": quote_address,
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, BASE, etc.)
            "market_category": "DEFI",  # DeFi instruments have chain != "off-chain"
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "the_graph",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": f"{token0_symbol}/{token1_symbol}",
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Pools don't expire
            "data_types": "trades",  # DEX pools have trade data
            "inverse": False,
            "contract_size": None,
            "tick_size": "",  # Uniswap V3 uses tick spacing
            "min_size": "",
            "underlying": f"{base_symbol}-{quote_symbol}",
        }

    def fetch_spot_pairs(self, base_currency: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V3 spot trading pairs (SPOT_PAIR instrument type).

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
