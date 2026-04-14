"""Resolve a date to an EVM block number via RPC binary search.

Supports ALL EVM chains (Ethereum, Arbitrum, Base, Optimism, Polygon,
Avalanche, BSC, Linea, etc.) using per-chain Alchemy RPC URLs from UAC
CHAIN_RPC_TEMPLATES.

Uses binary search on eth_getBlockByNumber to converge in ~25 RPC calls
(~2-4 seconds) for the full block range of any chain. Results are cached
per (chain, date) to avoid repeated RPC calls across adapters.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import aiohttp
import aiohttp.resolver
from unified_api_contracts.registry import MAINNET_CHAIN_IDS, resolve_rpc_url
from unified_trading_library import get_secret_client

logger = logging.getLogger(__name__)

# Module-level cache: (chain_upper, date_str) -> block_number
_block_cache: dict[tuple[str, str], int] = {}

# Non-EVM chains that cannot use eth_getBlockByNumber
_NON_EVM_CHAINS = frozenset({"SOLANA", "BITCOIN"})


def _make_session(
    timeout: aiohttp.ClientTimeout | None = None,
) -> aiohttp.ClientSession:
    """Create an aiohttp session with ThreadedResolver (OS DNS)."""
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


def _resolve_alchemy_key(alchemy_key: str | None) -> str | None:
    """Resolve Alchemy API key, falling back to Secret Manager."""
    if alchemy_key:
        return alchemy_key
    try:
        sc = get_secret_client()
        return sc.get_secret("alchemy-api-key").strip()
    except Exception:
        logger.warning("date_to_block: cannot get alchemy-api-key from Secret Manager")
        return None


async def _binary_search_block(
    url: str,
    target_ts: int,
    session: aiohttp.ClientSession,
) -> int:
    """Binary search for the block closest to target_ts using RPC.

    Full-range binary search from block 1 to latest. Converges in ~25-30
    iterations (log2 of block count), handling all EVM chains regardless
    of block time variability.
    """
    # Get latest block number
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": ["latest", False],
    }
    async with session.post(url, json=payload) as r:
        data = await r.json()
        hi = int(data["result"]["number"], 16)

    lo = 1
    best_block = (lo + hi) // 2
    best_diff = abs(target_ts)  # Large initial diff

    for _ in range(35):  # log2(4B) < 32, 35 gives margin for all chains
        if hi - lo <= 1:
            break
        mid = (lo + hi) // 2
        hex_block = hex(mid)
        block_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [hex_block, False],
        }
        async with session.post(url, json=block_payload) as r:
            block_data = await r.json()
            mid_ts = int(block_data["result"]["timestamp"], 16)

        diff = abs(mid_ts - target_ts)
        if diff < best_diff:
            best_diff = diff
            best_block = mid

        # Within 15 seconds is good enough
        if diff < 15:
            break

        if mid_ts < target_ts:
            lo = mid
        else:
            hi = mid

    return best_block


async def date_to_block(
    date_str: str,
    chain: str = "ETHEREUM",
    api_key: str | None = None,
    alchemy_key: str | None = None,
) -> int | None:
    """Convert a YYYY-MM-DD date to the closest block number on any EVM chain.

    Uses Alchemy RPC binary search (eth_getBlockByNumber) to find the block
    closest to midnight UTC of the given date. Supports all EVM chains with
    Alchemy RPC URLs registered in UAC CHAIN_RPC_TEMPLATES.

    Args:
        date_str: Date in YYYY-MM-DD format.
        chain: Chain name (e.g. ETHEREUM, ARBITRUM, BASE, POLYGON, etc.).
        api_key: Unused -- kept for backwards compatibility.
        alchemy_key: Alchemy API key. If not provided, attempts to fetch
            from Secret Manager via UTL.

    Returns:
        Block number closest to midnight UTC of the given date, or None
        if the chain is non-EVM or the key/RPC is unavailable.
    """
    chain_upper = chain.upper()

    # Reject non-EVM chains
    if chain_upper in _NON_EVM_CHAINS:
        logger.debug("date_to_block: chain %s is non-EVM, returning None", chain_upper)
        return None

    # Reject unknown chains
    if chain_upper not in MAINNET_CHAIN_IDS:
        logger.warning("date_to_block: unknown chain %s, returning None", chain_upper)
        return None

    # Check module-level cache
    cache_key = (chain_upper, date_str)
    if cache_key in _block_cache:
        cached = _block_cache[cache_key]
        logger.debug("date_to_block: cache hit %s/%s -> block %d", chain_upper, date_str, cached)
        return cached

    try:
        dt = datetime.fromisoformat(date_str).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=UTC,
        )
        target_ts = int(dt.timestamp())

        key = _resolve_alchemy_key(alchemy_key)
        if not key:
            return None

        # Resolve chain-specific RPC URL from UAC
        url = resolve_rpc_url(chain_upper, env="mainnet", alchemy_api_key=key)
        if not url:
            logger.warning("date_to_block: no RPC URL for chain %s", chain_upper)
            return None

        async with _make_session(aiohttp.ClientTimeout(total=60)) as session:
            block = await _binary_search_block(url, target_ts, session)

        _block_cache[cache_key] = block
        logger.debug("date_to_block: %s/%s -> block %d", chain_upper, date_str, block)
        return block

    except Exception as _exc:
        logger.warning("date_to_block error for %s on %s: %s", date_str, chain_upper, _exc)
        return None
