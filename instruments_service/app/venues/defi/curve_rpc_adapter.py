"""
Curve RPC Adapter

Fetches Curve pools directly from Ethereum contracts via RPC.
Uses the Curve MetaRegistry architecture:
1. AddressProvider (0x0000000022D53366457F9d5E68Ec105046FC4383) - entry point
2. MetaRegistry (obtained via AddressProvider.get_address(7)) - pool enumeration

Reference: https://docs.curve.fi/registry/metaregistry/
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from web3 import Web3
from unified_cloud_services import get_secret_with_fallback
from instruments_service.config import instruments_config
from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

logger = logging.getLogger(__name__)


class CurveRPCAdapter(BaseDefiAdapter):
    """
    RPC-based adapter for fetching Curve pools directly from contracts.

    Uses the MetaRegistry architecture to enumerate all pools.
    """

    # Curve launch date (approximately)
    CURVE_LAUNCH = datetime(2020, 1, 20, tzinfo=timezone.utc)

    # Contract addresses
    ADDRESS_PROVIDER = "0x0000000022D53366457F9d5E68Ec105046FC4383"

    # AddressProvider ABI - minimal for getting MetaRegistry
    ADDRESS_PROVIDER_ABI = [
        {
            "inputs": [{"name": "_id", "type": "uint256"}],
            "name": "get_address",
            "outputs": [{"name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    # MetaRegistry ABI - for pool enumeration
    METAREGISTRY_ABI = [
        {
            "inputs": [],
            "name": "pool_count",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "_index", "type": "uint256"}],
            "name": "pool_list",
            "outputs": [{"name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "_pool", "type": "address"}],
            "name": "get_pool_name",
            "outputs": [{"name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "_pool", "type": "address"}],
            "name": "get_coins",
            "outputs": [{"name": "", "type": "address[8]"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "_pool", "type": "address"}],
            "name": "get_decimals",
            "outputs": [{"name": "", "type": "uint256[8]"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

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
        self.rpc_url = None
        self.metaregistry_address = None
        self.venue = "CURVE-ETH"

        # Initialize web3 provider for RPC interaction
        try:
            if not rpc_url:
                try:
                    alchemy_key = get_secret_with_fallback(
                        project_id=instruments_config.gcp_project_id,
                        secret_name=instruments_config.alchemy_secret_name,
                        fallback_env_var="ALCHEMY_API_KEY",
                    )
                    if alchemy_key:
                        self.rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
                        logger.info("✅ Constructed Ethereum RPC URL from Alchemy API key")
                    else:
                        self.rpc_url = instruments_config.ethereum_rpc_url
                except Exception as e:
                    logger.warning(f"⚠️ Failed to get RPC URL from Secret Manager: {e}")
                    self.rpc_url = instruments_config.ethereum_rpc_url
            else:
                self.rpc_url = rpc_url

            if self.rpc_url:
                self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
                if self.w3.is_connected():
                    logger.info("✅ Connected to Ethereum RPC for Curve queries")
                    # Get MetaRegistry address from AddressProvider
                    self._init_metaregistry()
                else:
                    logger.warning("⚠️ Failed to connect to Ethereum RPC")
                    self.w3 = None
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Web3: {e}")

    def _init_metaregistry(self) -> None:
        """Initialize the MetaRegistry contract address from AddressProvider."""
        try:
            address_provider = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.ADDRESS_PROVIDER),
                abi=self.ADDRESS_PROVIDER_ABI,
            )
            # MetaRegistry is ID 7 in AddressProvider
            self.metaregistry_address = address_provider.functions.get_address(7).call()
            logger.info(f"✅ Got MetaRegistry address: {self.metaregistry_address}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to get MetaRegistry address: {e}")
            self.metaregistry_address = None

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
                venue="CURVE-ETH",
                instrument_type="POOL",
                symbol=pool.get("name", pool.get("id", "")[:10]),
            )
            instruments.append(
                {
                    "instrument_key": instrument_key,
                    "venue": "CURVE-ETH",
                    "instrument_type": "POOL",
                    "pool_address": pool.get("id"),
                    "pool_name": pool.get("name"),
                    "coins": pool.get("coins", []),
                    "chain": self.chain,
                }
            )
        return instruments

    def fetch_pools(
        self,
        block_number: Optional[int] = None,
        base_currency: Optional[str] = None,
        max_pools: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch Curve pools from MetaRegistry contract via RPC.

        Args:
            block_number: Optional block number for historical queries
            base_currency: Optional filter by base currency symbol
            max_pools: Maximum number of pools to fetch (default 100)

        Returns:
            List of pool dictionaries
        """
        if not self.w3:
            logger.debug("RPC not available for Curve queries")
            return []

        if not self.metaregistry_address:
            logger.debug("MetaRegistry address not available")
            return []

        try:
            metaregistry = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.metaregistry_address),
                abi=self.METAREGISTRY_ABI,
            )

            # Get total pool count
            pool_count = metaregistry.functions.pool_count().call()
            logger.info(f"✅ MetaRegistry reports {pool_count} Curve pools")

            # Limit to max_pools for MVP
            fetch_count = min(pool_count, max_pools)

            pools = []
            for i in range(fetch_count):
                try:
                    # Get pool address by index
                    pool_addr = metaregistry.functions.pool_list(i).call()

                    if pool_addr == "0x0000000000000000000000000000000000000000":
                        continue

                    # Get pool name
                    try:
                        pool_name = metaregistry.functions.get_pool_name(pool_addr).call()
                    except Exception:
                        pool_name = f"Curve Pool {pool_addr[:10]}"

                    # Get pool coins
                    try:
                        coins = metaregistry.functions.get_coins(pool_addr).call()
                        # Filter out zero addresses
                        coins = [
                            c
                            for c in coins
                            if c != "0x0000000000000000000000000000000000000000"
                        ]
                    except Exception:
                        coins = []

                    if not coins and not pool_name:
                        continue

                    pools.append(
                        {
                            "id": pool_addr,
                            "name": pool_name,
                            "coins": [
                                {"id": coin, "symbol": "", "decimals": 18} for coin in coins
                            ],
                            "totalValueLockedUSD": None,
                            "createdAtTimestamp": None,
                        }
                    )

                except Exception as e:
                    logger.debug(f"Failed to fetch pool at index {i}: {e}")
                    continue

            logger.info(f"✅ Fetched {len(pools)} Curve pools via MetaRegistry")
            return pools

        except Exception as e:
            logger.error(f"Failed to fetch Curve pools via MetaRegistry: {e}")
            return []

    def fetch_markets(
        self, target_date: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Curve pools as instrument definitions.

        Args:
            target_date: Optional target date for filtering

        Returns:
            Dictionary of instrument definitions keyed by instrument_key
        """
        # Check date availability
        if target_date and target_date < self.CURVE_LAUNCH:
            logger.info(
                f"Target date {target_date.date()} is before Curve launch "
                f"({self.CURVE_LAUNCH.date()}), returning empty"
            )
            return {}

        pools = self.fetch_pools()
        instruments = {}

        for pool in pools:
            pool_name = pool.get("name", pool.get("id", "")[:10])
            # Sanitize pool name for instrument key
            safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in pool_name)
            instrument_key = f"CURVE-ETH:POOL:{safe_name}@ETHEREUM"

            instruments[instrument_key] = {
                "instrument_key": instrument_key,
                "venue": "CURVE-ETH",
                "instrument_type": "POOL",
                "symbol": safe_name,
                "pool_address": pool.get("id"),
                "pool_name": pool_name,
                "chain": self.chain,
                "available_from_datetime": self.CURVE_LAUNCH.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "data_types": "swaps,liquidity,volume",
            }

        logger.info(f"✅ Generated {len(instruments)} Curve instrument definitions")
        return instruments
