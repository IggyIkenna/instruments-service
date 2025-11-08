"""
Curve Adapter

Fetches Curve pool instruments from The Graph.
Generates canonical instrument keys for Curve pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
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
    
    def __init__(self, chain: str = 'ETHEREUM', subgraph_url: Optional[str] = None):
        """
        Initialize Curve adapter.
        
        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM')
            subgraph_url: Optional custom subgraph URL
        """
        self.chain = chain.upper()
        self.venue = f"CURVE-{chain.upper()}"
        
        # Initialize The Graph client
        if subgraph_url is None:
            # Default Curve subgraph URLs by chain
            subgraph_urls = {
                'ETHEREUM': 'https://api.thegraph.com/subgraphs/name/curvefi/curve',
                'ARBITRUM': 'https://api.thegraph.com/subgraphs/name/curvefi/curve-arbitrum',
            }
            subgraph_url = subgraph_urls.get(self.chain, subgraph_urls['ETHEREUM'])
        
        self.graph_client = TheGraphClient(subgraph_url)
        logger.info(f"✅ CurveAdapter initialized for chain: {self.chain}")
    
    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        min_liquidity: Optional[float] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Curve pools and convert to instrument definitions.
        
        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            min_liquidity: Minimum liquidity threshold in USD
            
        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Query Curve pools from The Graph
        pools = self._query_curve_pools(base_currency, min_liquidity)
        
        instruments = {}
        
        for pool in pools:
            try:
                inst_def = self._convert_pool_to_instrument(pool)
                if inst_def:
                    instruments[inst_def['instrument_key']] = inst_def
            except Exception as e:
                logger.warning(f"Failed to convert Curve pool {pool.get('id')}: {e}")
                continue
        
        logger.info(f"✅ Generated {len(instruments)} Curve instruments")
        return instruments
    
    def _query_curve_pools(
        self,
        base_currency: Optional[str] = None,
        min_liquidity: Optional[float] = None
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
        
        where_str = ', '.join(where_clause) if where_clause else ''
        
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
            response = requests.post(
                self.graph_client.subgraph_url,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            if 'errors' in data:
                logger.error(f"Curve GraphQL query errors: {data['errors']}")
                return []
            
            pools = data.get('data', {}).get('pools', [])
            
            # Filter by base currency if specified
            if base_currency:
                pools = [
                    p for p in pools
                    if any(coin.get('symbol', '').upper() == base_currency.upper() 
                          for coin in p.get('coins', []))
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
        pool_id = pool.get('id')
        coins = pool.get('coins', [])
        
        if not pool_id or len(coins) < 2:
            return None
        
        # Extract token info (Curve pools can have multiple coins, use first two)
        token0 = coins[0]
        token1 = coins[1]
        
        token0_symbol = token0.get('symbol', '')
        token1_symbol = token1.get('symbol', '')
        token0_address = token0.get('id', '')
        token1_address = token1.get('id', '')
        
        # Determine base and quote (use ETH as base if present)
        if 'ETH' in token0_symbol.upper() or 'WETH' in token0_symbol.upper():
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
        elif 'ETH' in token1_symbol.upper() or 'WETH' in token1_symbol.upper():
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
        created_timestamp = pool.get('createdAtTimestamp')
        if created_timestamp:
            available_from = datetime.fromtimestamp(int(created_timestamp)).isoformat()
        else:
            available_from = datetime.now().isoformat()
        
        return {
            'instrument_key': instrument_key,
            'venue': self.venue,
            'instrument_type': 'POOL',
            'symbol': symbol,
            'base_asset': base_symbol,
            'quote_asset': quote_symbol,
            'settle_asset': quote_symbol,
            'pool_address': pool_id,
            'pool_fee_tier': None,  # Curve uses different fee model
            'base_asset_contract_address': base_address,
            'quote_asset_contract_address': quote_address,
            'asset_class': 'crypto',
            'venue_type': 'protocol',
            'data_provider': 'the_graph',
            'tardis_exchange': '',
            'tardis_symbol': '',
            'exchange_raw_symbol': f"{token0_symbol}/{token1_symbol}",
            'ccxt_symbol': '',
            'ccxt_exchange': '',
            'available_from_datetime': available_from,
            'available_to_datetime': None,
            'data_types': 'trades',
            'inverse': False,
            'contract_size': None,
            'tick_size': '',
            'min_size': '',
            'underlying': f"{base_symbol}-{quote_symbol}",
        }
    
    def fetch_spot_pairs(
        self,
        base_currency: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
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
            spot_key = pool_key.replace(':POOL:', ':SPOT_PAIR:')
            spot_key = spot_key.split(':')[0] + ':' + spot_key.split(':')[1] + ':' + \
                      pool_def['base_asset'] + '-' + pool_def['quote_asset'] + f"@{self.chain}"
            
            spot_def = pool_def.copy()
            spot_def['instrument_key'] = spot_key
            spot_def['instrument_type'] = 'SPOT_PAIR'
            spot_def['symbol'] = f"{pool_def['base_asset']}-{pool_def['quote_asset']}"
            # Remove pool-specific fields
            spot_def.pop('pool_address', None)
            spot_def.pop('pool_fee_tier', None)
            
            spot_pairs[spot_key] = spot_def
        
        return spot_pairs

