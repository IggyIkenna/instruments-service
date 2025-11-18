"""
The Graph Client for DeFi DEX Pools

Fetches DEX pool information from The Graph subgraphs.
Supports Uniswap V3, Curve, and other DEX protocols.

Reference: The Graph Protocol documentation
The Graph has moved from hosted service (api.thegraph.com) to:
- Subgraph Studio: https://api.studio.thegraph.com/query/<ID>/<SUBGRAPH_NAME>/<VERSION>
- The Graph Network: https://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/<SUBGRAPH_ID>

For production use, use The Graph Network endpoints with API keys.
"""

import logging
import os
from typing import Dict, List, Optional, Any
import requests
from unified_cloud_services import get_secret_with_fallback
from instruments_service.settings import env_configs

logger = logging.getLogger(__name__)

# Module-level cache for API key to avoid repeated Secret Manager calls
_API_KEY_CACHE: Optional[str] = None
_API_KEY_PROJECT_ID: Optional[str] = None


class TheGraphClient:
    """
    Client for querying The Graph subgraphs.

    Supports:
    - Uniswap V3 pools
    - Curve pools
    - Other DEX subgraphs

    Uses The Graph Network endpoints (gateway.thegraph.com) with API keys.
    Falls back to Studio endpoints if no API key is provided (rate-limited).
    """

    def __init__(
        self,
        subgraph_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize The Graph client.

        Args:
            subgraph_url: Subgraph URL (if provided, used directly)
            api_key: Optional API key for The Graph Network endpoints (uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Try provided API key first
        self.api_key = api_key

        # If not provided, try cached API key or Secret Manager
        if not self.api_key:
            global _API_KEY_CACHE, _API_KEY_PROJECT_ID

            # Check if we have a cached API key for the same project
            project_id = project_id or env_configs.gcp_project_id

            if _API_KEY_CACHE and _API_KEY_PROJECT_ID == project_id:
                # Use cached API key
                self.api_key = _API_KEY_CACHE
                logger.debug("✅ Using cached The Graph API key")
            else:
                # Retrieve from Secret Manager and cache it
                try:
                    secret_name = env_configs.graph_seceret_name
                    self.api_key = get_secret_with_fallback(
                        project_id=project_id,
                        secret_name=secret_name,
                        fallback_env_var="THE_GRAPH_API_KEY",
                    )

                    # Strip whitespace/newlines from API key (common issue when piping to gcloud)
                    if self.api_key:
                        self.api_key = self.api_key.strip()
                        # Cache the API key
                        _API_KEY_CACHE = self.api_key
                        _API_KEY_PROJECT_ID = project_id

                    if self.api_key:
                        logger.info(
                            f"✅ Retrieved The Graph API key from Secret Manager (secret: {secret_name})"
                        )
                except ImportError:
                    logger.warning("unified-cloud-services not available, falling back to env var")
                    self.api_key = env_configs.graph_seceret_name
                    if self.api_key:
                        _API_KEY_CACHE = self.api_key
                        _API_KEY_PROJECT_ID = project_id
                except Exception as e:
                    logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")
                    self.api_key = env_configs.graph_seceret_name
                    if self.api_key:
                        _API_KEY_CACHE = self.api_key
                        _API_KEY_PROJECT_ID = project_id

        if subgraph_url:
            self.subgraph_url = subgraph_url
        else:
            # Default: Try to use Studio endpoint (no API key needed, but rate-limited)
            self.subgraph_url = env_configs.uniswap_v3_graph_url

        logger.info(f"✅ TheGraphClient initialized with URL: {self.subgraph_url}")
        if self.api_key:
            logger.info("✅ Using The Graph API key for authenticated requests")
        else:
            logger.warning("⚠️ No API key provided - using Studio endpoint (rate-limited)")

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
                {f'where: {{ {where_str} }}' if where_str else ''}
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

        try:
            headers = {"Content-Type": "application/json"}

            # Note: The Graph Network endpoints embed API key in URL, not headers
            # API key is already in the URL if using gateway.thegraph.com format

            response = requests.post(
                self.subgraph_url,
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
                    "removed" in msg or "deprecated" in msg or "endpoint" in msg
                    for msg in error_messages
                ):
                    logger.debug(f"The Graph endpoint deprecated: {self.subgraph_url}")
                    return []
                else:
                    logger.error(f"The Graph query errors: {errors}")
                    return []

            pools = data.get("data", {}).get("pools", [])
            logger.info(f"✅ Fetched {len(pools)} pools from The Graph")
            return pools

        except Exception as e:
            logger.error(f"Failed to query The Graph: {e}")
            return []

    def query_pools_by_base_currency(
        self, base_currency: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query pools containing a specific base currency.

        Args:
            base_currency: Base currency symbol (e.g., 'ETH', 'BTC')
            limit: Maximum number of pools to return

        Returns:
            List of pool dictionaries
        """
        # Query pools where token0 or token1 matches base currency
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

        try:
            headers = {"Content-Type": "application/json"}

            # Note: The Graph Network endpoints embed API key in URL, not headers
            # API key is already in the URL if using gateway.thegraph.com format

            response = requests.post(
                self.subgraph_url,
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
                    "removed" in msg or "deprecated" in msg or "endpoint" in msg
                    for msg in error_messages
                ):
                    logger.debug(f"The Graph endpoint deprecated: {self.subgraph_url}")
                    return []
                else:
                    logger.error(f"The Graph query errors: {errors}")
                    return []

            pools = data.get("data", {}).get("pools", [])
            logger.info(f"✅ Fetched {len(pools)} pools for {base_currency} from The Graph")
            return pools

        except Exception as e:
            logger.error(f"Failed to query The Graph for {base_currency}: {e}")
            return []

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
        # Build GraphQL query for Uniswap V2 pairs
        where_clause = []
        if min_liquidity:
            where_clause.append(f'reserveUSD_gte: "{min_liquidity}"')

        where_str = ", ".join(where_clause) if where_clause else ""

        query = f"""
        {{
            pairs(
                first: {limit}
                {f'where: {{ {where_str} }}' if where_str else ''}
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

        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.subgraph_url,
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
                    "removed" in msg or "deprecated" in msg or "endpoint" in msg
                    for msg in error_messages
                ):
                    logger.debug(f"The Graph endpoint deprecated: {self.subgraph_url}")
                    return []
                else:
                    logger.error(f"The Graph query errors: {errors}")
                    return []

            pairs = data.get("data", {}).get("pairs", [])
            logger.info(f"✅ Fetched {len(pairs)} pairs from The Graph")
            return pairs

        except Exception as e:
            logger.error(f"Failed to query The Graph: {e}")
            return []

    def query_pairs_by_base_currency(
        self, base_currency: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query pairs containing a specific base currency (Uniswap V2).

        Args:
            base_currency: Base currency symbol (e.g., 'ETH', 'BTC')
            limit: Maximum number of pairs to return

        Returns:
            List of pair dictionaries
        """
        # Query pairs where token0 or token1 matches base currency
        query = f"""
        {{
            pairs(
                first: {limit}
                where: {{
                    or: [
                        {{ token0_: {{ symbol: "{base_currency}" }} }}
                        {{ token1_: {{ symbol: "{base_currency}" }} }}
                    ]
                }}
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

        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.subgraph_url,
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
                    "removed" in msg or "deprecated" in msg or "endpoint" in msg
                    for msg in error_messages
                ):
                    logger.debug(f"The Graph endpoint deprecated: {self.subgraph_url}")
                    return []
                else:
                    logger.error(f"The Graph query errors: {errors}")
                    return []

            pairs = data.get("data", {}).get("pairs", [])
            logger.info(f"✅ Fetched {len(pairs)} pairs for {base_currency} from The Graph")
            return pairs

        except Exception as e:
            logger.error(f"Failed to query The Graph for {base_currency}: {e}")
            return []

    def execute_query_sync(self, query: str) -> Dict[str, Any]:
        """
        Execute a raw GraphQL query synchronously.

        Args:
            query: GraphQL query string

        Returns:
            Dictionary with 'data' and 'errors' keys
        """
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.subgraph_url,
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
                    "removed" in msg or "deprecated" in msg or "endpoint" in msg
                    for msg in error_messages
                ):
                    logger.debug(f"The Graph endpoint deprecated: {self.subgraph_url}")
                    return {"data": {}, "errors": errors}
                else:
                    logger.error(f"The Graph query errors: {errors}")
                    return {"data": {}, "errors": errors}

            return data

        except Exception as e:
            logger.error(f"Failed to execute GraphQL query: {e}")
            return {"data": {}, "errors": [str(e)]}
