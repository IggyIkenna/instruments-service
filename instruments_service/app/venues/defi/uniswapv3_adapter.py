"""
Uniswap V3 Adapter

Fetches Uniswap V3 pool instruments from The Graph.
Generates canonical instrument keys for DEX pools.

Reference: archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from .the_graph_client import TheGraphClient

logger = logging.getLogger(__name__)


class UniswapV3Adapter:
    """
    Adapter for fetching Uniswap V3 pool instruments.
    
    Generates instruments in format:
    UNISWAPV3-ETH:POOL:USDC-ETH:5@ETHEREUM
    UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM
    """
    
    def __init__(self, chain: str = 'ETHEREUM', subgraph_url: Optional[str] = None):
        """
        Initialize Uniswap V3 adapter.
        
        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM', 'BASE')
            subgraph_url: Optional custom subgraph URL
        """
        self.chain = chain.upper()
        self.venue = f"UNISWAPV3-{chain.upper()}"
        
        # Initialize The Graph client
        if subgraph_url is None:
            # Default Uniswap V3 subgraph URLs by chain
            subgraph_urls = {
                'ETHEREUM': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
                'ARBITRUM': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-arbitrum',
                'BASE': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-base',
            }
            subgraph_url = subgraph_urls.get(self.chain, subgraph_urls['ETHEREUM'])
        
        self.graph_client = TheGraphClient(subgraph_url)
        logger.info(f"✅ UniswapV3Adapter initialized for chain: {self.chain}")
    
    def fetch_pools(
        self,
        base_currency: Optional[str] = None,
        min_liquidity: Optional[float] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Uniswap V3 pools and convert to instrument definitions.
        
        Args:
            base_currency: Filter by base currency symbol (e.g., 'ETH')
            min_liquidity: Minimum liquidity threshold in USD
            
        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        if base_currency:
            pools = self.graph_client.query_pools_by_base_currency(base_currency)
        else:
            pools = self.graph_client.query_pools(min_liquidity=min_liquidity)
        
        instruments = {}
        
        for pool in pools:
            try:
                inst_def = self._convert_pool_to_instrument(pool)
                if inst_def:
                    instruments[inst_def['instrument_key']] = inst_def
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
        pool_id = pool.get('id')  # Pool contract address
        token0 = pool.get('token0', {})
        token1 = pool.get('token1', {})
        fee_tier = pool.get('feeTier')
        
        if not pool_id or not token0 or not token1 or fee_tier is None:
            return None
        
        # Extract token info
        token0_symbol = token0.get('symbol', '')
        token1_symbol = token1.get('symbol', '')
        token0_address = token0.get('id', '')
        token1_address = token1.get('id', '')
        token0_decimals = token0.get('decimals', 18)
        token1_decimals = token1.get('decimals', 18)
        
        # Determine base and quote (use ETH as base if present)
        if 'ETH' in token0_symbol.upper() or 'WETH' in token0_symbol.upper():
            base_symbol = token0_symbol
            quote_symbol = token1_symbol
            base_address = token0_address
            quote_address = token1_address
            base_decimals = token0_decimals
            quote_decimals = token1_decimals
        elif 'ETH' in token1_symbol.upper() or 'WETH' in token1_symbol.upper():
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
            'pool_fee_tier': fee_bps,
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
            'available_to_datetime': None,  # Pools don't expire
            'data_types': 'trades',  # DEX pools have trade data
            'inverse': False,
            'contract_size': None,
            'tick_size': '',  # Uniswap V3 uses tick spacing
            'min_size': '',
            'underlying': f"{base_symbol}-{quote_symbol}",
        }
    
    def fetch_spot_pairs(
        self,
        base_currency: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
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

