"""
Curve RPC Adapter

Fetches Curve pools directly from Ethereum contracts via RPC.
Bypasses The Graph indexers by querying Curve Registry contract directly.

Reference: Curve Registry contract on Ethereum mainnet
Address: 0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d9f5
"""

import logging
from typing import Dict, List, Optional, Any
from web3 import Web3
from unified_cloud_services import get_secret_with_fallback
from instruments_service.config import instruments_config
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)


class CurveRPCAdapter(BaseDefiAdapter):
    """
    RPC-based adapter for fetching Curve pools directly from contracts.

    Queries Curve Registry contract to get pool list, then fetches pool details.
    """

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        project_id: Optional[str] = None,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
    ):
        """
        Initialize Curve RPC adapter.

        Args:
            rpc_url: Optional Ethereum RPC URL (uses Alchemy if not provided)
            project_id: GCP project ID for Secret Manager
            chain: Chain identifier (default: 'ETHEREUM')
            api_key: Optional API key (not used by Curve but required by base class)
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.w3 = None

        # Get RPC URL
        if rpc_url:
            self.rpc_url = rpc_url
        else:

            project_id = project_id or instruments_config.gcp_project_id
            alchemy_key = get_secret_with_fallback(
                project_id=project_id,
                secret_name="alchemy-api-key",
                fallback_env_var="ALCHEMY_API_KEY",
            )

            if alchemy_key:
                self.rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
            else:
                self.rpc_url = instruments_config.ethereum_rpc_url

        if self.rpc_url:
            try:
                self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
                if self.w3.is_connected():
                    logger.info("✅ Connected to Ethereum RPC for Curve queries")
                else:
                    logger.warning("⚠️ Failed to connect to Ethereum RPC")
                    self.w3 = None
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize Web3: {e}")
                self.w3 = None

        # Curve Registry contract address (Ethereum mainnet)
        self.registry_address = "0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d9f5"
        self.venue = "CURVE"

        # Minimal Registry ABI for get_pool_list()
        self.registry_abi = [
            {
                "inputs": [],
                "name": "get_pool_list",
                "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "address", "name": "_pool", "type": "address"}],
                "name": "get_coins",
                "outputs": [{"internalType": "address[8]", "name": "", "type": "address[8]"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "address", "name": "_pool", "type": "address"}],
                "name": "get_pool_name",
                "outputs": [{"internalType": "string", "name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function",
            },
        ]

    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Curve.

        Returns:
            List of instrument definition dictionaries
        """
        pools = self.fetch_pools()
        # Convert pools to instrument definitions
        instruments = []
        for pool in pools:
            instrument_key = self._build_instrument_key(
                venue="CURVE",
                instrument_type="POOL",
                symbol=pool.get("name", pool.get("id", "")[:10])
            )
            instruments.append({
                "instrument_key": instrument_key,
                "venue": "CURVE",
                "instrument_type": "POOL",
                "pool_address": pool.get("id"),
                "pool_name": pool.get("name"),
                "coins": pool.get("coins", []),
                "chain": self.chain,
            })
        return instruments

    def fetch_pools(
        self,
        block_number: Optional[int] = None,
        base_currency: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch Curve pools from Registry contract via RPC.

        Args:
            block_number: Optional block number for historical queries
            base_currency: Optional filter by base currency symbol

        Returns:
            List of pool dictionaries
        """
        if not self.w3:
            logger.debug("RPC not available for Curve queries")
            return []

        try:
            registry = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.registry_address), abi=self.registry_abi
            )

            # Get pool list
            if block_number:
                pool_addresses = registry.functions.get_pool_list().call(
                    block_identifier=block_number
                )
            else:
                pool_addresses = registry.functions.get_pool_list().call()

            logger.info(f"✅ Found {len(pool_addresses)} Curve pools via RPC")

            pools = []
            for pool_addr in pool_addresses[:100]:  # Limit to first 100 for MVP
                try:
                    # Get pool coins
                    coins = registry.functions.get_coins(pool_addr).call(
                        block_identifier=block_number if block_number else "latest"
                    )
                    # Filter out zero addresses
                    coins = [c for c in coins if c != "0x0000000000000000000000000000000000000000"]

                    if not coins:
                        continue

                    # Get pool name
                    try:
                        pool_name = registry.functions.get_pool_name(pool_addr).call(
                            block_identifier=block_number if block_number else "latest"
                        )
                    except:
                        pool_name = f"Curve Pool {pool_addr[:10]}"

                    # Get token symbols (would need ERC20 ABI for full implementation)
                    # For MVP, return basic pool info
                    pools.append(
                        {
                            "id": pool_addr,
                            "name": pool_name,
                            "coins": [{"id": coin, "symbol": "", "decimals": 18} for coin in coins],
                            "totalValueLockedUSD": None,  # Would need price oracle
                            "createdAtTimestamp": None,  # Would need to query creation event
                        }
                    )

                except Exception as e:
                    logger.debug(f"Failed to fetch pool {pool_addr}: {e}")
                    continue

            logger.info(f"✅ Fetched {len(pools)} Curve pools via RPC")
            return pools

        except Exception as e:
            logger.error(f"Failed to fetch Curve pools via RPC: {e}")
            return []
