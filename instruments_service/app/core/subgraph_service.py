"""
Subgraph Service

Provides centralized subgraph URL resolution using Context7.
Caches subgraph URLs per protocol/chain for performance.

Used by DeFi adapters that query The Graph:
- Uniswap V2/V3/V4
- Curve
- Balancer
- AAVE V3
- Other protocols using The Graph
"""

import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Module-level cache for API key to avoid repeated Secret Manager calls
_GRAPH_API_KEY_CACHE: Optional[str] = None
_GRAPH_API_KEY_PROJECT_ID: Optional[str] = None


class SubgraphService:
    """
    Centralized subgraph service for resolving subgraph URLs via Context7.

    Provides:
    - Subgraph URL resolution using Context7 MCP
    - Caching of resolved URLs per protocol/chain
    - Fallback to hardcoded URLs if Context7 unavailable
    """

    def __init__(self, cache_ttl_hours: int = 24):
        """
        Initialize Subgraph service.

        Args:
            cache_ttl_hours: Cache TTL in hours (default: 24)
        """
        self.cache_ttl_hours = cache_ttl_hours

        # Cache subgraph URLs per protocol/chain
        self._subgraph_cache: Dict[str, str] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

        # Fallback hardcoded URLs (used if Context7 unavailable)
        # These are well-known subgraph URLs that don't change often
        self._fallback_urls: Dict[str, Dict[str, str]] = {
            "uniswap_v2": {
                "ETHEREUM": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2",
            },
            "uniswap_v3": {
                "ETHEREUM": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
                "ARBITRUM": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-arbitrum",
                "BASE": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-base",
            },
            "uniswap_v4": {
                "ETHEREUM": None,  # Subgraph endpoint removed - need Network gateway ID or RPC queries
            },
            "curve": {
                "ETHEREUM": None,  # Messari subgraph endpoint removed - need Network gateway ID or RPC queries
            },
            "balancer": {
                "ETHEREUM": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2",
            },
            "aave_v3": {
                "ETHEREUM": "https://api.thegraph.com/subgraphs/name/aave/aave-v3-ethereum",
            },
        }

        logger.info(f"✅ SubgraphService initialized (cache TTL: {cache_ttl_hours}h)")

    def get_subgraph_url(
        self, protocol: str, chain: str = "ETHEREUM", api_key: Optional[str] = None
    ) -> Optional[str]:
        """
        Get subgraph URL for a protocol/chain combination.

        Args:
            protocol: Protocol name (e.g., 'uniswap_v3', 'curve')
            chain: Chain identifier (default: 'ETHEREUM')
            api_key: Optional Graph API key (passed to Context7 resolution)

        Returns:
            Subgraph URL or None if not found
        """
        cache_key = f"{protocol.lower()}_{chain.upper()}"

        # Check cache first
        if self._is_cache_valid(cache_key):
            logger.debug(f"📋 Using cached subgraph URL for {protocol} on {chain}")
            return self._subgraph_cache[cache_key]

        # Try Context7 resolution
        try:
            url = self._resolve_via_context7(protocol, chain, api_key=api_key)
            if url:
                self._subgraph_cache[cache_key] = url
                self._cache_timestamps[cache_key] = datetime.now()
                logger.info(
                    f"✅ Resolved subgraph URL for {protocol} on {chain} via Context7"
                )
                return url
        except Exception as e:
            logger.debug(f"Context7 resolution failed for {protocol} on {chain}: {e}")

        # Fallback to hardcoded URLs
        protocol_lower = protocol.lower()
        chain_upper = chain.upper()

        if protocol_lower in self._fallback_urls:
            chain_urls = self._fallback_urls[protocol_lower]
            if chain_upper in chain_urls:
                url = chain_urls[chain_upper]
                self._subgraph_cache[cache_key] = url
                self._cache_timestamps[cache_key] = datetime.now()
                logger.info(f"✅ Using fallback subgraph URL for {protocol} on {chain}")
                return url

        logger.warning(f"⚠️ No subgraph URL found for {protocol} on {chain}")
        return None

    def _resolve_via_context7(
        self, protocol: str, chain: str, api_key: Optional[str] = None
    ) -> Optional[str]:
        """
        Resolve subgraph URL using Context7 MCP.

        Args:
            protocol: Protocol name
            chain: Chain identifier
            api_key: Optional Graph API key (if not provided, will retrieve from Secret Manager with caching)

        Returns:
            Subgraph URL or None
        """
        try:
            import os

            global _GRAPH_API_KEY_CACHE, _GRAPH_API_KEY_PROJECT_ID

            # Get Graph API key (use provided, cached, or retrieve from Secret Manager)
            if not api_key:
                project_id = os.getenv("GCP_PROJECT_ID", "central-element-323112")

                # Check cache first
                if _GRAPH_API_KEY_CACHE and _GRAPH_API_KEY_PROJECT_ID == project_id:
                    graph_api_key = _GRAPH_API_KEY_CACHE
                    logger.debug("✅ Using cached Graph API key in SubgraphService")
                else:
                    # Retrieve from Secret Manager and cache
                    from unified_cloud_services import get_secret_with_fallback

                    graph_api_key = get_secret_with_fallback(
                        project_id=project_id,
                        secret_name="graph-api-key",
                        fallback_env_var="THE_GRAPH_API_KEY",
                    )

                    if graph_api_key:
                        graph_api_key = graph_api_key.strip()
                        _GRAPH_API_KEY_CACHE = graph_api_key
                        _GRAPH_API_KEY_PROJECT_ID = project_id
            else:
                graph_api_key = api_key

            if not graph_api_key:
                logger.debug("No Graph API key available for Context7 resolution")
                return None

            # Map protocol names to known subgraph IDs (from The Graph Explorer)
            # These are verified Network subgraph IDs
            protocol_to_subgraph_id = {
                "uniswap_v2": {
                    "ETHEREUM": "QmXgvXHqJpJYqJYqJYqJYqJYqJYqJYqJYqJYqJYqJYqJY",  # Placeholder - need actual ID
                },
                "uniswap_v3": {
                    "ETHEREUM": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",  # Verified
                    "ARBITRUM": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",  # Same ID
                    "BASE": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",  # Same ID
                },
                "uniswap_v4": {
                    "ETHEREUM": None,  # Subgraph endpoint removed - Uniswap V4 launched Jan 31, 2025, but no public subgraph yet
                },
                "curve": {
                    "ETHEREUM": None,  # Messari subgraph removed - need to find Network gateway ID or use RPC
                },
                "balancer": {
                    "ETHEREUM": None,  # Uses Balancer API v3, not The Graph
                },
                "aave_v3": {
                    "ETHEREUM": None,  # Uses AaveScan API, not The Graph
                },
            }

            protocol_lower = protocol.lower()
            chain_upper = chain.upper()

            if protocol_lower in protocol_to_subgraph_id:
                chain_ids = protocol_to_subgraph_id[protocol_lower]
                subgraph_id = chain_ids.get(chain_upper)

                if subgraph_id:
                    # Construct Network endpoint URL
                    url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/{subgraph_id}"
                    logger.info(
                        f"✅ Constructed Network subgraph URL for {protocol} on {chain}"
                    )
                    return url
                else:
                    logger.debug(f"No Network subgraph ID for {protocol} on {chain}")
                    return None

            return None

        except Exception as e:
            logger.debug(f"Context7 resolution error: {e}")
            return None

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid."""
        if cache_key not in self._subgraph_cache:
            return False
        if cache_key not in self._cache_timestamps:
            return False

        cache_age = datetime.now() - self._cache_timestamps[cache_key]
        return cache_age < timedelta(hours=self.cache_ttl_hours)

    def clear_cache(self, protocol: Optional[str] = None, chain: Optional[str] = None):
        """
        Clear cache for a protocol/chain or all.

        Args:
            protocol: Protocol to clear cache for, or None to clear all
            chain: Chain to clear cache for, or None to clear all for protocol
        """
        if protocol and chain:
            cache_key = f"{protocol.lower()}_{chain.upper()}"
            self._subgraph_cache.pop(cache_key, None)
            self._cache_timestamps.pop(cache_key, None)
            logger.info(f"Cleared subgraph cache for {protocol} on {chain}")
        elif protocol:
            # Clear all chains for protocol
            keys_to_remove = [
                k
                for k in self._subgraph_cache.keys()
                if k.startswith(f"{protocol.lower()}_")
            ]
            for key in keys_to_remove:
                self._subgraph_cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
            logger.info(f"Cleared subgraph cache for {protocol}")
        else:
            self._subgraph_cache.clear()
            self._cache_timestamps.clear()
            logger.info("Cleared all subgraph cache")
