"""
The Graph Client for DeFi DEX Pools

Fetches DEX pool information from The Graph subgraphs.
Supports Uniswap V3, Curve, and other DEX protocols.

Reference: The Graph Protocol documentation
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class TheGraphClient:
    """
    Client for querying The Graph subgraphs.
    
    Supports:
    - Uniswap V3 pools
    - Curve pools
    - Other DEX subgraphs
    """
    
    def __init__(self, subgraph_url: Optional[str] = None):
        """
        Initialize The Graph client.
        
        Args:
            subgraph_url: Subgraph URL (defaults to Uniswap V3 Ethereum mainnet)
        """
        self.subgraph_url = subgraph_url or os.getenv(
            'THE_GRAPH_UNISWAP_V3_URL',
            'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
        )
        logger.info(f"✅ TheGraphClient initialized with URL: {self.subgraph_url}")
    
    def query_pools(
        self,
        base_token: Optional[str] = None,
        quote_token: Optional[str] = None,
        min_liquidity: Optional[float] = None,
        limit: int = 1000
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
        
        where_str = ', '.join(where_clause) if where_clause else ''
        
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
            response = requests.post(
                self.subgraph_url,
                json={'query': query},
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            data = response.json()
            if 'errors' in data:
                logger.error(f"The Graph query errors: {data['errors']}")
                return []
            
            pools = data.get('data', {}).get('pools', [])
            logger.info(f"✅ Fetched {len(pools)} pools from The Graph")
            return pools
            
        except Exception as e:
            logger.error(f"Failed to query The Graph: {e}")
            return []
    
    def query_pools_by_base_currency(
        self,
        base_currency: str,
        limit: int = 100
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
            response = requests.post(
                self.subgraph_url,
                json={'query': query},
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            data = response.json()
            if 'errors' in data:
                logger.error(f"The Graph query errors: {data['errors']}")
                return []
            
            pools = data.get('data', {}).get('pools', [])
            logger.info(f"✅ Fetched {len(pools)} pools for {base_currency} from The Graph")
            return pools
            
        except Exception as e:
            logger.error(f"Failed to query The Graph for {base_currency}: {e}")
            return []

