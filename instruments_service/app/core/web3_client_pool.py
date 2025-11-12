"""
Web3 Client Pool - Reusable Web3 instances for Ethereum RPC calls.

Avoids creating new Web3 instances and HTTP connections for each RPC call.
Similar to connection pooling in unified cloud services.
"""

import logging
from typing import Optional, Dict
from threading import Lock
from web3 import Web3

logger = logging.getLogger(__name__)

# Module-level pool of Web3 instances: {rpc_url: Web3 instance}
_WEB3_POOL: Dict[str, Web3] = {}
_POOL_LOCK = Lock()


def get_web3_client(rpc_url: str) -> Optional[Web3]:
    """
    Get a Web3 client from the pool, or create a new one if not exists.

    Reuses existing Web3 instances to avoid creating new HTTP connections.

    Args:
        rpc_url: Ethereum RPC URL (e.g., https://eth-mainnet.g.alchemy.com/v2/{key})

    Returns:
        Web3 instance or None if connection fails
    """
    # Normalize URL (remove trailing slashes, etc.)
    normalized_url = rpc_url.rstrip("/")

    # Check pool first (thread-safe)
    with _POOL_LOCK:
        if normalized_url in _WEB3_POOL:
            w3 = _WEB3_POOL[normalized_url]
            # Test connection
            try:
                _ = w3.eth.block_number
                logger.debug(f"✅ Reusing Web3 client for {normalized_url[:50]}...")
                return w3
            except Exception:
                # Connection failed, remove from pool and create new one
                logger.debug(
                    f"Web3 connection failed, removing from pool: {normalized_url[:50]}..."
                )
                del _WEB3_POOL[normalized_url]

    # Create new Web3 instance
    try:
        w3 = Web3(Web3.HTTPProvider(normalized_url))
        # Test connection
        _ = w3.eth.block_number
        if w3.is_connected():
            # Add to pool (thread-safe)
            with _POOL_LOCK:
                _WEB3_POOL[normalized_url] = w3
            logger.debug(f"✅ Created and cached Web3 client for {normalized_url[:50]}...")
            return w3
    except Exception as e:
        logger.debug(f"Failed to create Web3 client for {normalized_url[:50]}...: {e}")
        return None

    return None


def clear_pool():
    """Clear the Web3 client pool."""
    with _POOL_LOCK:
        _WEB3_POOL.clear()
        logger.debug("Cleared Web3 client pool")
