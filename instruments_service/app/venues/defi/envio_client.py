"""
Envio Client for Uniswap V4 Data

Fetches Uniswap V4 pool data from Envio's HyperIndex/HyperSync indexer.
Envio provides an alternative to The Graph for indexing blockchain data.

References:
- Migration Guide: https://docs.envio.dev/docs/HyperIndex/migration-guide
- Configuration: https://docs.envio.dev/docs/HyperIndex/configuration-file
- Multichain Indexing: https://docs.envio.dev/docs/HyperIndex/multichain-indexing
- API Tokens: https://docs.envio.dev/docs/HyperSync/api-tokens

Note: Each deployed HyperIndex indexer exposes its own GraphQL endpoint.
The endpoint URL is specific to your deployed indexer and can be found in:
- Envio Dashboard (if using hosted service)
- Your deployment configuration (if self-hosting)

For the Uniswap V4 indexer (enviodev/uniswap-v4-indexer), check:
- Envio Dashboard for the deployed endpoint URL
- Or deploy your own indexer using the migration guide
"""

import logging
import os
from typing import Dict, List, Optional, Any
import requests

logger = logging.getLogger(__name__)

# Module-level cache for API key to avoid repeated Secret Manager calls
_ENVIO_API_KEY_CACHE: Optional[str] = None
_ENVIO_API_KEY_PROJECT_ID: Optional[str] = None


class EnvioClient:
    """
    Client for querying Envio's HyperSync GraphQL API.
    
    Supports:
    - Uniswap V4 pools
    - Swaps, liquidity changes, tokens
    
    Uses Envio's GraphQL API with API tokens.
    """

    def __init__(
        self,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Envio client.
        
        Args:
            api_url: Envio GraphQL API URL (defaults to HyperSync endpoint)
            api_key: Optional API token (uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager
        """
        # Envio HyperIndex GraphQL endpoint
        # For local development: http://localhost:8080/v1/graphql (when running `pnpm envio dev`)
        # For production: Custom domain/URL where indexer is deployed
        # 
        # Setup instructions:
        # 1. Clone: git clone https://github.com/enviodev/uniswap-v4-indexer.git
        # 2. Install: pnpm install
        # 3. Configure .env with ENVIO_API_TOKEN
        # 4. Run: pnpm envio dev
        # 5. Set ENVIO_API_URL=http://localhost:8080/v1/graphql
        # See docs/ENVIO_DEPLOYMENT_GUIDE.md for detailed instructions
        self.api_url = api_url or os.getenv(
            "ENVIO_API_URL",
            None  # Must be provided - no default public endpoint
        )
        
        if not self.api_url:
            logger.warning(
                "⚠️ No Envio API URL provided. Set ENVIO_API_URL env var. "
                "For local development, run 'pnpm envio dev' in uniswap-v4-indexer directory "
                "and set ENVIO_API_URL=http://localhost:8080/v1/graphql. "
                "See docs/ENVIO_DEPLOYMENT_GUIDE.md for setup instructions."
            )
        
        # Try provided API key first
        self.api_key = api_key
        
        # If not provided, try cached API key or Secret Manager
        if not self.api_key:
            global _ENVIO_API_KEY_CACHE, _ENVIO_API_KEY_PROJECT_ID
            
            project_id = project_id or os.getenv(
                "GCP_PROJECT_ID", "central-element-323112"
            )
            
            if _ENVIO_API_KEY_CACHE and _ENVIO_API_KEY_PROJECT_ID == project_id:
                self.api_key = _ENVIO_API_KEY_CACHE
                logger.debug("✅ Using cached Envio API key")
            else:
                try:
                    from unified_cloud_services import get_secret_with_fallback
                    
                    secret_name = os.getenv("ENVIO_SECRET_NAME", "envio-api-key")
                    
                    self.api_key = get_secret_with_fallback(
                        project_id=project_id,
                        secret_name=secret_name,
                        fallback_env_var="ENVIO_API_KEY",
                    )
                    
                    if self.api_key:
                        self.api_key = self.api_key.strip()
                        _ENVIO_API_KEY_CACHE = self.api_key
                        _ENVIO_API_KEY_PROJECT_ID = project_id
                        logger.info(
                            f"✅ Retrieved Envio API key from Secret Manager (secret: {secret_name})"
                        )
                except ImportError:
                    logger.warning("unified-cloud-services not available, falling back to env var")
                    self.api_key = os.getenv("ENVIO_API_KEY", "")
        
        if self.api_key:
            logger.info("✅ Using Envio API key for authenticated requests")
        else:
            logger.warning("⚠️ No Envio API key found - requests may fail")
    
    def query_pools(
        self,
        chain_id: str = "1",  # Ethereum mainnet
        min_tvl: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Query Uniswap V4 pools from Envio.
        
        Args:
            chain_id: Chain ID (1 = Ethereum mainnet)
            min_tvl: Minimum TVL in USD
            limit: Maximum number of pools to return
            
        Returns:
            List of pool dictionaries
        """
        where_clause = [f'chainId: {{ _eq: "{chain_id}" }}']
        
        query = f"""
        {{
            Pool(
                order_by: {{ totalValueLockedUSD: desc }}
                limit: {limit}
                where: {{ {", ".join(where_clause)} }}
            ) {{
                id
                name
                token0
                token1
                totalValueLockedUSD
                volumeUSD
                feesUSD
                txCount
                hooks
            }}
        }}
        """
        
        if not self.api_url:
            logger.error("Envio API URL not configured - cannot query pools")
            return []
        
        try:
            headers = {
                "Content-Type": "application/json",
            }
            
            # Add API token to headers (required from Nov 3, 2025)
            # Envio uses API tokens for authentication
            if self.api_key:
                # Try both common authentication header formats
                headers["Authorization"] = f"Bearer {self.api_key}"
                # Alternative: headers["x-api-key"] = self.api_key
                # Alternative: headers["x-tenantsecretkey"] = self.api_key
            
            response = requests.post(
                self.api_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                errors = data.get("errors", [])
                logger.error(f"Envio GraphQL query errors: {errors}")
                return []
            
            pools = data.get("data", {}).get("Pool", [])
            logger.info(f"✅ Fetched {len(pools)} pools from Envio")
            return pools
            
        except Exception as e:
            logger.error(f"Failed to query Envio: {e}")
            return []
    
    def query_pools_by_token(
        self,
        token_symbol: str,
        chain_id: str = "1",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query pools containing a specific token.
        
        Args:
            token_symbol: Token symbol (e.g., 'ETH', 'USDC')
            chain_id: Chain ID
            limit: Maximum number of pools to return
            
        Returns:
            List of pool dictionaries
        """
        # Note: Envio schema may differ - adjust query based on actual schema
        query = f"""
        {{
            Pool(
                limit: {limit}
                where: {{
                    chainId: {{ _eq: "{chain_id}" }}
                    _or: [
                        {{ token0: {{ symbol: {{ _eq: "{token_symbol}" }} }} }}
                        {{ token1: {{ symbol: {{ _eq: "{token_symbol}" }} }} }}
                    ]
                }}
                order_by: {{ totalValueLockedUSD: desc }}
            ) {{
                id
                name
                token0
                token1
                totalValueLockedUSD
            }}
        }}
        """
        
        if not self.api_url:
            logger.error("Envio API URL not configured - cannot query pools")
            return []
        
        try:
            headers = {"Content-Type": "application/json"}
            # Add API token to headers (required from Nov 3, 2025)
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            
            response = requests.post(
                self.api_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            
            data = response.json()
            if "errors" in data:
                errors = data.get("errors", [])
                logger.error(f"Envio GraphQL query errors: {errors}")
                return []
            
            pools = data.get("data", {}).get("Pool", [])
            logger.info(f"✅ Fetched {len(pools)} pools for {token_symbol} from Envio")
            return pools
            
        except Exception as e:
            logger.error(f"Failed to query Envio for {token_symbol}: {e}")
            return []

