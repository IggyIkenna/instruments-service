"""
Uniswap V2 Adapter

Fetches Uniswap V2 pool instruments from The Graph.
Generates canonical instrument keys for DEX pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from .the_graph_client import TheGraphClient

logger = logging.getLogger(__name__)


class UniswapV2Adapter:
    """
    Adapter for fetching Uniswap V2 pool instruments.

    Generates instruments in format:
    UNISWAPV2-ETH:POOL:USDC-ETH@ETHEREUM
    UNISWAPV2-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM

    Note: Uniswap V2 uses constant product formula (x * y = k)
    and has a single fee tier (0.3% = 30 bps).
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Uniswap V2 adapter.

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
        venue_suffix = chain_suffix_map.get(self.chain, self.chain[:3])  # Default to first 3 chars
        self.venue = f"UNISWAPV2-{venue_suffix}"

        # Initialize The Graph client
        if subgraph_url is None:
            # Default Uniswap V2 subgraph URL (Ethereum only for now)
            # TheGraphClient will automatically convert to authenticated endpoint if API key is available
            if self.chain == "ETHEREUM":
                subgraph_url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
            else:
                logger.warning(f"⚠️  Chain {self.chain} not supported yet, defaulting to Ethereum")
                subgraph_url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"

        self.graph_client = TheGraphClient(subgraph_url, api_key=api_key, project_id=project_id)
        logger.info(f"✅ UniswapV2Adapter initialized for chain: {self.chain}")

    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        quote_currency_list: Optional[List[str]] = None,
        min_liquidity: Optional[float] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V2 pools and convert to instrument definitions.

        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            quote_currency_list: Optional list of allowed quote currencies (filters pools where quote is in this list)
            min_liquidity: Minimum liquidity threshold in USD
                - Measured as: reserveUSD (total reserve value) from The Graph subgraph
                - Represents: Combined USD value of token0 and token1 reserves in the pair
                - Default: $10,000 minimum to filter out low-liquidity/inactive pools

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        if base_currency:
            pools = self.graph_client.query_pairs_by_base_currency(base_currency)
        else:
            pools = self.graph_client.query_pairs(min_liquidity=min_liquidity)

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

        logger.info(f"✅ Generated {len(instruments)} Uniswap V2 instruments")
        return instruments

    def _convert_pool_to_instrument(self, pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert The Graph pair data to instrument definition.

        Note: Uniswap V2 uses 'pairs' not 'pools', and has a single fee tier (0.3% = 30 bps).

        Args:
            pair: Pair data from The Graph

        Returns:
            Instrument definition dictionary
        """
        pool_id = pair.get("id")
        if not pool_id:
            return None

        token0 = pair.get("token0", {})
        token1 = pair.get("token1", {})

        token0_symbol = token0.get("symbol", "")
        token1_symbol = token1.get("symbol", "")
        token0_address = token0.get("id", "")
        token1_address = token1.get("id", "")

        # CRITICAL: Skip Curve LP tokens (they start with "crv" prefix)
        # Curve LP tokens should be handled by CurveAdapter, not Uniswap adapters
        if token0_symbol.upper().startswith("CRV") or token1_symbol.upper().startswith("CRV"):
            logger.debug(
                f"Skipping pair {pool_id}: Contains Curve LP token ({token0_symbol}/{token1_symbol})"
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
                f"Skipping pair {pool_id}: Contains Balancer LP token ({token0_symbol}/{token1_symbol})"
            )
            return None

        # Uniswap V2 has a single fee tier: 0.3% = 30 basis points
        fee_tier = 30  # 0.3% = 30 bps

        # Build symbol (token0-token1)
        symbol = f"{token0_symbol}-{token1_symbol}"

        # Build canonical instrument key
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:POOL:{symbol}{chain_suffix}"

        # Get creation timestamp or use Uniswap V2 launch date
        created_timestamp = pair.get("createdAtTimestamp")
        if created_timestamp:
            available_from = datetime.fromtimestamp(int(created_timestamp)).isoformat()
        else:
            # Uniswap V2 launched on Ethereum: 2020-05-05
            available_from = datetime(2020, 5, 5).isoformat()

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "POOL",
            "symbol": symbol,
            "base_asset": token0_symbol,
            "quote_asset": token1_symbol,
            "settle_asset": token1_symbol,
            "base_asset_contract_address": token0_address,
            "quote_asset_contract_address": token1_address,
            "pool_address": pool_id,
            "pool_fee_tier": fee_tier,  # Uniswap V2: 30 bps (0.3%)
            "chain": self.chain,  # Chain identifier (ETHEREUM, ARBITRUM, BASE, etc.)
            "asset_class": "crypto",
            "venue_type": "dex",
            "data_provider": "the_graph",
            "tardis_exchange": "",
            "tardis_symbol": "",
            # exchange_raw_symbol: Native exchange identifier (pool contract address for Uniswap V2)
            # This is the identifier used directly by the exchange/protocol for execution
            "exchange_raw_symbol": pool_id,  # Pool contract address (native identifier)
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,  # Pair creation date or protocol launch (2020-05-05)
            "available_to_datetime": None,
            "data_types": "swaps",  # DEX pools have swap transactions (different from trades for backtesting)
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "0",  # No protocol-enforced minimum. Practical minimum ~$10-50 based on gas costs and slippage (~$5-30 gas for swaps)
        }

    def fetch_spot_pairs(
        self,
        base_currency: Optional[str] = None,
        quote_currency_list: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V2 spot trading pairs (SPOT_PAIR instrument type).

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
