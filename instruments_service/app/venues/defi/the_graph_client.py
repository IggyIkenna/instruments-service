"""
The Graph Client for DeFi DEX Pools - REFACTORED

Uses TheGraphBaseClient from unified-cloud-services for network management.
This module provides domain-specific query methods for DEX protocols.

ARCHITECTURE:
- Uses TheGraphBaseClient (unified-cloud-services) for API key, session, retries
- This client provides DEX-specific GraphQL queries (pools, pairs)
"""

import logging
from typing import Any, Dict, List, Optional

from unified_cloud_services import (
    TheGraphClientConfig,
    clear_thegraph_api_key_cache,
)
from unified_market_interface import TheGraphBaseClient

logger = logging.getLogger(__name__)

# Re-export for backward compatibility
clear_api_key_cache = clear_thegraph_api_key_cache

# Module-level cache references (for backward compatibility)
_API_KEY_CACHE = None  # Managed by TheGraphBaseClient
_API_KEY_PROJECT_ID = None  # Managed by TheGraphBaseClient


# Default subgraph URLs - use config for flexibility
def _get_default_uniswap_v3_url() -> str:
    """Get default Uniswap V3 URL from config."""
    from instruments_service.config import instruments_config

    return instruments_config.thegraph_uniswap_v3_studio_url


# Legacy constant for backwards compatibility
DEFAULT_UNISWAP_V3_URL = "https://api.studio.thegraph.com/query/48211/uniswap-v3-mainnet/version/latest"


class TheGraphClient:
    """
    Client for querying The Graph subgraphs.

    Uses TheGraphBaseClient for network management (sessions, retries, API keys).
    This client provides domain-specific methods:
    - query_pools: Uniswap V3 pool queries
    - query_pairs: Uniswap V2 pair queries
    - query_pools_by_base_currency: Filter pools by token

    Supports:
    - Uniswap V3 pools
    - Uniswap V2 pairs
    - Curve pools
    - Other DEX subgraphs
    """

    def __init__(
        self,
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
        secret_name: Optional[str] = None,
    ):
        """
        Initialize The Graph client using centralized TheGraphBaseClient.

        Args:
            subgraph_url: Subgraph URL (uses default if not provided)
            api_key: Optional API key (TheGraphBaseClient handles Secret Manager)
            project_id: GCP project ID for Secret Manager
            secret_name: Secret name for API key (defaults to config value)
        """
        # Get secret name from config if not provided
        if secret_name is None:
            from instruments_service.config import instruments_config

            secret_name = instruments_config.graph_secret_name

        # Create config with custom secret name if provided
        config = TheGraphClientConfig(
            secret_name=secret_name,
            default_uniswap_v3_url=subgraph_url or DEFAULT_UNISWAP_V3_URL,
        )

        # Initialize centralized base client
        self._base_client = TheGraphBaseClient(
            config=config,
            api_key=api_key,
            subgraph_url=subgraph_url,
            project_id=project_id,
        )

        logger.info("✅ TheGraphClient initialized (using TheGraphBaseClient)")
        logger.info(f"   Subgraph URL: {self._base_client.subgraph_url}")
        if self._base_client.api_key:
            logger.info("   Using The Graph API key for authenticated requests")
        else:
            logger.warning("   No API key - using Studio endpoint (rate-limited)")

    @property
    def api_key(self) -> Optional[str]:
        """Get API key."""
        return self._base_client.api_key

    @property
    def subgraph_url(self) -> str:
        """Get subgraph URL."""
        return self._base_client.subgraph_url

    @subgraph_url.setter
    def subgraph_url(self, url: str):
        """Set subgraph URL."""
        self._base_client.subgraph_url = url

    def query_pools(
        self,
        base_token: Optional[str] = None,
        quote_token: Optional[str] = None,
        min_liquidity: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Query pools from Uniswap V3 subgraph.

        Args:
            base_token: Filter by base token address (optional)
            quote_token: Filter by quote token address (optional)
            min_liquidity: Minimum liquidity threshold (optional)
            limit: Maximum number of pools to return

        Returns:
            List of pool dictionaries
        """
        # Build GraphQL query
        where_clause = []
        if base_token:
            where_clause.append(f'token0: "{base_token}"')
        if quote_token:
            where_clause.append(f'token1: "{quote_token}"')
        if min_liquidity:
            where_clause.append(f'totalValueLockedUSD_gte: "{min_liquidity}"')

        where_str = ", ".join(where_clause) if where_clause else ""

        query = f"""
        {{
            pools(
                first: {limit}
                {f"where: {{ {where_str} }}" if where_str else ""}
                orderBy: totalValueLockedUSD
                orderDirection: desc
            ) {{
                id
                token0 {{
                    id
                    symbol
                    decimals
                }}
                token1 {{
                    id
                    symbol
                    decimals
                }}
                feeTier
                liquidity
                totalValueLockedUSD
                createdAtTimestamp
            }}
        }}
        """

        result = self._base_client.execute_query(query)
        pools = result.get("data", {}).get("pools", [])
        logger.info(f"Fetched {len(pools)} pools from The Graph")
        return pools

    def query_pools_by_base_currency(self, base_currency: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query pools containing a specific base currency.

        Args:
            base_currency: Base currency symbol (e.g., 'ETH', 'BTC')
            limit: Maximum number of pools to return

        Returns:
            List of pool dictionaries
        """
        query = f"""
        {{
            pools(
                first: {limit}
                where: {{
                    or: [
                        {{ token0_: {{ symbol: "{base_currency}" }} }}
                        {{ token1_: {{ symbol: "{base_currency}" }} }}
                    ]
                }}
                orderBy: totalValueLockedUSD
                orderDirection: desc
            ) {{
                id
                token0 {{
                    id
                    symbol
                    decimals
                }}
                token1 {{
                    id
                    symbol
                    decimals
                }}
                feeTier
                liquidity
                totalValueLockedUSD
                createdAtTimestamp
            }}
        }}
        """

        result = self._base_client.execute_query(query)
        pools = result.get("data", {}).get("pools", [])
        logger.info(f"Fetched {len(pools)} pools for {base_currency} from The Graph")
        return pools

    def query_pairs(
        self,
        min_liquidity: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Query pairs from Uniswap V2 subgraph (V2 uses 'pairs' not 'pools').

        Args:
            min_liquidity: Minimum liquidity threshold (reserveUSD)
            limit: Maximum number of pairs to return

        Returns:
            List of pair dictionaries
        """
        where_clause = []
        if min_liquidity:
            where_clause.append(f'reserveUSD_gte: "{min_liquidity}"')

        where_str = ", ".join(where_clause) if where_clause else ""

        query = f"""
        {{
            pairs(
                first: {limit}
                {f"where: {{ {where_str} }}" if where_str else ""}
                orderBy: reserveUSD
                orderDirection: desc
            ) {{
                id
                token0 {{
                    id
                    symbol
                    decimals
                }}
                token1 {{
                    id
                    symbol
                    decimals
                }}
                reserveUSD
                createdAtTimestamp
            }}
        }}
        """

        result = self._base_client.execute_query(query)
        pairs = result.get("data", {}).get("pairs", [])
        logger.info(f"Fetched {len(pairs)} pairs from The Graph")
        return pairs

    def execute_query_sync(self, query: str) -> Dict[str, Any]:
        """
        Execute a raw GraphQL query synchronously.

        Args:
            query: GraphQL query string

        Returns:
            Dictionary with 'data' and 'errors' keys
        """
        return self._base_client.execute_query(query)

    def cleanup(self):
        """Cleanup resources."""
        self._base_client.cleanup()
