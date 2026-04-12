"""Resolve a date to an Ethereum block number via Alchemy RPC binary search.

Uses the alchemy-api-key from Secret Manager for eth_getBlockByNumber calls.
Binary search converges in ~25 RPC calls (~2 seconds) for the full Ethereum
block range.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import aiohttp
import aiohttp.resolver
from unified_trading_library import get_secret_client

logger = logging.getLogger(__name__)


def _make_session(**kwargs: object) -> aiohttp.ClientSession:
    """Create an aiohttp session with ThreadedResolver (OS DNS)."""
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
    return aiohttp.ClientSession(connector=connector, **kwargs)  # type: ignore[arg-type]


# Alchemy RPC endpoint template — key injected at runtime
_ALCHEMY_URL_TEMPLATE = "https://eth-mainnet.g.alchemy.com/v2/{key}"


def _resolve_alchemy_key(alchemy_key: str | None) -> str | None:
    """Resolve Alchemy API key, falling back to Secret Manager."""
    if alchemy_key:
        return alchemy_key
    try:
        sc = get_secret_client()
        return sc.get_secret("alchemy-api-key").strip()
    except Exception as _exc:
        logger.warning("date_to_block: cannot get alchemy-api-key from Secret Manager")
        return None


async def _binary_search_block(url: str, target_ts: int, session: aiohttp.ClientSession) -> int:
    """Binary search for the block closest to target_ts using Alchemy RPC."""
    # Get latest block for upper bound
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
    for _ in range(30):  # log2(25M) ≈ 25, 30 gives margin
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
        if mid_ts < target_ts:
            lo = mid
        else:
            hi = mid
    return lo


async def date_to_block(
    date_str: str,
    chain: str = "ETHEREUM",
    api_key: str | None = None,
    alchemy_key: str | None = None,
) -> int | None:
    """Convert a YYYY-MM-DD date to the closest Ethereum block number.

    Uses Alchemy RPC binary search (eth_getBlockByNumber) to find the block
    closest to midnight UTC of the given date.

    Args:
        date_str: Date in YYYY-MM-DD format.
        chain: Chain name (currently only ETHEREUM supported).
        api_key: Unused — will be removed in a future version.
        alchemy_key: Alchemy API key. If not provided, attempts to fetch
            from Secret Manager via UTL.

    Returns:
        Block number closest to midnight UTC of the given date, or None.
    """
    if chain.upper() != "ETHEREUM":
        logger.debug("date_to_block: chain %s not supported, returning None", chain)
        return None

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

        url = _ALCHEMY_URL_TEMPLATE.format(key=key)

        async with _make_session(timeout=aiohttp.ClientTimeout(total=30)) as session:
            lo = await _binary_search_block(url, target_ts, session)

        logger.debug("date_to_block: %s → block %d", date_str, lo)
        return lo

    except Exception as _exc:
        logger.warning("date_to_block error for %s: %s", date_str, _exc)
        return None
