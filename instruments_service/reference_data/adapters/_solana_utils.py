"""Solana shared utilities for reference data adapters.

Provides creation timestamp resolution for Solana accounts/pools/vaults
via the getSignaturesForAddress RPC method, plus protocol-level floor
dates as guaranteed fallback (same pattern as Aave V3's deploy-date floor).

Resolved timestamps are cached to a local JSON file so the expensive
RPC pagination only runs once per address.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

# ── Protocol floor dates (conservative mainnet launch dates) ──────────
# Used as guaranteed fallback when per-pool RPC resolution fails or is
# unavailable.  Matches the Aave V3 pattern (_AAVE_V3_DEPLOY_DATE).
# Any instrument that currently exists on-chain was created ON or AFTER
# its protocol's launch date.
SOLANA_PROTOCOL_DEPLOY_DATES: dict[str, datetime] = {
    "drift": datetime(2022, 11, 4, tzinfo=UTC),  # Drift v2 mainnet launch
    "kamino": datetime(2024, 1, 1, tzinfo=UTC),  # Kamino vaults mainnet launch
    "raydium": datetime(2021, 2, 21, tzinfo=UTC),  # Raydium AMM mainnet launch
    "orca": datetime(2022, 3, 1, tzinfo=UTC),  # Orca Whirlpools (CLMM) launch
    "marinade": datetime(2021, 8, 1, tzinfo=UTC),  # Marinade mSOL mainnet launch
}

# ── Timestamp cache ───────────────────────────────────────────────────
# Creation dates never change, so we cache them locally.
# First run resolves via RPC (slow), subsequent runs read from cache.
_CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".cache"
_CACHE_FILE = _CACHE_DIR / "solana_creation_timestamps.json"


def _load_cache() -> dict[str, str]:
    """Load cached address → ISO timestamp mapping from disk."""
    if _CACHE_FILE.exists():
        try:
            raw = json.loads(_CACHE_FILE.read_text())
            if isinstance(raw, dict):
                return raw
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load Solana timestamp cache: %s", exc)
    return {}


def _save_cache(cache: dict[str, str]) -> None:
    """Persist address → ISO timestamp mapping to disk."""
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))
    except OSError as exc:
        logger.warning("Failed to save Solana timestamp cache: %s", exc)


def get_protocol_floor_date(protocol: str) -> datetime:
    """Return the conservative floor date for a Solana protocol.

    Raises KeyError if the protocol is not registered — this forces
    callers to register new protocols rather than silently defaulting.
    """
    key = protocol.lower()
    if key not in SOLANA_PROTOCOL_DEPLOY_DATES:
        msg = f"No floor date for Solana protocol {protocol!r} — register it in _solana_utils.SOLANA_PROTOCOL_DEPLOY_DATES"
        raise KeyError(msg)
    return SOLANA_PROTOCOL_DEPLOY_DATES[key]


def _get_solana_rpc_url() -> str:
    """Get the best available Solana RPC URL.

    Priority: SOLANA_RPC_URL env var > Alchemy (from Secret Manager) > public fallback.
    """
    import os

    # 1. Explicit env var (same pattern as PUBSUB_EMULATOR_HOST etc.)
    env_url = os.environ.get("SOLANA_RPC_URL")
    if env_url:
        logger.debug("Using Solana RPC from SOLANA_RPC_URL env var")
        return env_url

    # 2. Alchemy via Secret Manager
    try:
        from unified_api_contracts.registry.capability_declarations._defi import (
            SOLANA_RPC_TEMPLATES,
        )
        from unified_trading_library import get_secret_client

        secret_client = get_secret_client()
        api_key = secret_client.get_secret("alchemy-api-key")
        if api_key:
            template = SOLANA_RPC_TEMPLATES.get("alchemy", "")
            if template:
                url = template.format(api_key=api_key)
                logger.debug("Using Alchemy Solana RPC via Secret Manager")
                return url
    except Exception as exc:
        logger.warning("Alchemy Solana RPC unavailable: %s", exc)

    # 3. Public fallback (rate-limited — timestamps will likely fail for >5 pools)
    logger.warning(
        "Falling back to public Solana RPC (rate-limited). "
        "Set SOLANA_RPC_URL env var or configure alchemy-api-key in Secret Manager"
    )
    return "https://api.mainnet-beta.solana.com"


async def get_account_creation_timestamp(
    address: str,
    rpc_url: str | None = None,
    session: aiohttp.ClientSession | None = None,
) -> datetime | None:
    """Get the creation timestamp of a Solana account via RPC.

    Uses getSignaturesForAddress to find the earliest transaction
    involving this account. The blockTime of that transaction is
    the account's creation timestamp.

    The RPC returns signatures newest-to-oldest. We page backward
    until we find the oldest one (last page, last entry).

    Args:
        address: Solana account address (base-58 encoded).
        rpc_url: Solana RPC endpoint URL. Defaults to public mainnet.
        session: Optional shared aiohttp session for connection pooling.

    Returns:
        datetime of first transaction, or None if unavailable.
    """
    url = rpc_url or _get_solana_rpc_url()
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        # Page backward through signatures to find the oldest
        oldest_block_time: int | None = None
        before: str | None = None
        # 100 pages = 100K transactions — covers most Solana DeFi accounts.
        # Popular accounts (>100K txs) will get the oldest reachable date,
        # which is still much better than the protocol floor date.
        max_pages = 100

        for page in range(max_pages):
            payload: dict[str, object] = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    address,
                    {
                        "commitment": "finalized",
                        "limit": 1000,
                        **({"before": before} if before else {}),
                    },
                ],
            }

            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.debug(
                        "Solana RPC %d for getSignaturesForAddress(%s)",
                        resp.status,
                        address[:16],
                    )
                    return None
                data: dict[str, object] = await resp.json()

            # Check for RPC errors
            rpc_error = data.get("error")
            if rpc_error:
                logger.warning(
                    "Solana RPC error for %s: %s (url=%s)",
                    address[:16],
                    rpc_error,
                    url[:60],
                )
                return None

            result = data.get("result")
            if not isinstance(result, list) or not result:
                break

            # Last entry in this page is the oldest so far
            last_entry = result[-1]
            bt = last_entry.get("blockTime")
            if isinstance(bt, int) and bt > 0:
                oldest_block_time = bt

            # If we got fewer than 1000, we've reached the beginning
            if len(result) < 1000:
                break

            # Page backward from the oldest signature in this batch
            before = str(last_entry.get("signature", ""))
            if not before:
                break

        if oldest_block_time is not None:
            if page >= max_pages - 1:
                logger.debug(
                    "Hit %d-page limit for %s — using oldest reachable date",
                    max_pages,
                    address[:16],
                )
            return datetime.fromtimestamp(oldest_block_time, tz=UTC)
        return None

    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.debug(
            "Solana RPC error for %s: %s",
            address[:16],
            exc,
        )
        return None
    finally:
        if owns_session:
            await session.close()


async def batch_resolve_creation_timestamps(
    addresses: list[str],
    rpc_url: str | None = None,
    concurrency: int = 5,
) -> dict[str, datetime]:
    """Resolve creation timestamps for multiple Solana addresses.

    Uses a local file cache — creation dates never change, so we
    only resolve each address once via RPC. Subsequent calls read
    from cache.

    Returns:
        Dict mapping address → creation datetime (only for resolved ones).
    """
    import asyncio

    # Load cache and separate cached vs uncached addresses
    raw_cache = _load_cache()
    results: dict[str, datetime] = {}
    to_resolve: list[str] = []

    for addr in addresses:
        cached_ts = raw_cache.get(addr)
        if cached_ts:
            try:
                results[addr] = datetime.fromisoformat(cached_ts)
            except ValueError:
                to_resolve.append(addr)
        else:
            to_resolve.append(addr)

    if results:
        logger.info(
            "Loaded %d/%d Solana timestamps from cache",
            len(results),
            len(addresses),
        )

    if not to_resolve:
        return results

    # Resolve uncached addresses via RPC
    url = rpc_url or _get_solana_rpc_url()
    sem = asyncio.Semaphore(concurrency)
    new_resolved: dict[str, datetime] = {}

    async with aiohttp.ClientSession() as session:

        async def _resolve_one(addr: str) -> None:
            async with sem:
                ts = await get_account_creation_timestamp(
                    addr,
                    rpc_url=url,
                    session=session,
                )
                if ts is not None:
                    new_resolved[addr] = ts

        await asyncio.gather(*[_resolve_one(a) for a in to_resolve])

    # Merge new results and update cache
    results.update(new_resolved)

    if new_resolved:
        for addr, ts in new_resolved.items():
            raw_cache[addr] = ts.isoformat()
        _save_cache(raw_cache)
        logger.info(
            "Resolved %d new Solana timestamps via RPC (cached for next run)",
            len(new_resolved),
        )

    logger.info(
        "Solana timestamps: %d/%d resolved (%d cached, %d new, %d unresolved)",
        len(results),
        len(addresses),
        len(results) - len(new_resolved),
        len(new_resolved),
        len(addresses) - len(results),
    )
    return results
