"""EVM contract creation timestamp resolver — binary search via eth_getCode.

Resolves when a smart contract was first deployed on any EVM chain by
binary-searching block numbers: eth_getCode returns empty before deployment,
non-empty after.  ~25 RPC calls per address (log2 of block range).

Resolved timestamps are cached to GCS (persists across container restarts)
with local file fallback for development.  Same cache pattern as
``_solana_utils.py`` — creation timestamps never change, so each address
is resolved once and cached forever.

Supports all EVM chains in UAC ``CHAIN_RPC_TEMPLATES``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

# ── Cache files ──────────────────────────────────────────────────────
_GCS_CACHE_BLOB = "_cache/evm_creation_timestamps.json"
_LOCAL_CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".cache"
_LOCAL_CACHE_FILE = _LOCAL_CACHE_DIR / "evm_creation_timestamps.json"
_SEED_FILE = Path(__file__).resolve().parent / "evm_creation_timestamps_seed.json"

# ── Protocol deploy date floors (same role as Solana floor dates) ────
# Used as guaranteed fallback when RPC resolution fails.
LENDING_PROTOCOL_DEPLOY_DATES: dict[str, dict[str, datetime]] = {
    "aave_v3": {
        "ETHEREUM": datetime(2023, 1, 27, tzinfo=UTC),
        "ARBITRUM": datetime(2023, 3, 16, tzinfo=UTC),
        "OPTIMISM": datetime(2023, 3, 16, tzinfo=UTC),
        "BASE": datetime(2024, 3, 1, tzinfo=UTC),
        "POLYGON": datetime(2023, 1, 27, tzinfo=UTC),
        "AVALANCHE": datetime(2023, 1, 27, tzinfo=UTC),
        "GNOSIS": datetime(2023, 10, 1, tzinfo=UTC),
        "SCROLL": datetime(2024, 1, 15, tzinfo=UTC),
        "BSC": datetime(2024, 6, 1, tzinfo=UTC),
    },
    "compound_v3": {
        "ETHEREUM": datetime(2022, 8, 26, tzinfo=UTC),
        "ARBITRUM": datetime(2023, 6, 28, tzinfo=UTC),
        "BASE": datetime(2023, 8, 9, tzinfo=UTC),
        "POLYGON": datetime(2023, 5, 24, tzinfo=UTC),
        "OPTIMISM": datetime(2024, 1, 10, tzinfo=UTC),
        "SCROLL": datetime(2024, 3, 1, tzinfo=UTC),
    },
    "morpho": {
        "ETHEREUM": datetime(2024, 1, 8, tzinfo=UTC),
        "BASE": datetime(2024, 6, 1, tzinfo=UTC),
    },
    "fluid": {
        "ETHEREUM": datetime(2024, 3, 1, tzinfo=UTC),
    },
}


def get_protocol_floor_date(protocol: str, chain: str) -> datetime:
    """Return conservative floor date for a lending protocol on a given chain."""
    proto_dates = LENDING_PROTOCOL_DEPLOY_DATES.get(protocol, {})
    floor = proto_dates.get(chain.upper())
    if floor:
        return floor
    # Generic fallback: 2020-01-01 (all modern DeFi protocols launched after this)
    return datetime(2020, 1, 1, tzinfo=UTC)


# ── RPC URL resolution ───────────────────────────────────────────────


def _resolve_rpc_url(chain: str, alchemy_key: str | None = None) -> str | None:
    """Resolve Alchemy RPC URL for a given chain."""
    key = alchemy_key
    if not key:
        try:
            from unified_trading_library import get_secret_client

            sc = get_secret_client()
            key = sc.get_secret("alchemy-api-key").strip()
        except Exception:
            logger.warning("evm_creation_resolver: cannot get alchemy-api-key")
            return None

    try:
        from unified_api_contracts.registry.capability_declarations._defi import (
            CHAIN_RPC_TEMPLATES,
        )
        from unified_api_contracts.registry.chain_env import MAINNET_CHAIN_IDS

        chain_id = MAINNET_CHAIN_IDS.get(chain.upper())
        if chain_id is None or chain_id == 0:
            return None
        template = CHAIN_RPC_TEMPLATES.get(chain_id)
        if not template:
            return None
        return template.format(api_key=key)
    except Exception:
        return None


# ── Cache management ─────────────────────────────────────────────────


def _get_gcs_bucket() -> str | None:
    """Resolve the DeFi instruments bucket for cache storage."""
    try:
        from unified_trading_library import get_bucket_name

        return get_bucket_name("instruments", "defi")
    except Exception:
        return None


def _load_cache() -> dict[str, str]:
    """Load cached (chain:address) -> ISO timestamp mapping.

    Priority: GCS -> local file -> seed file.
    """
    # 1. GCS
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            data = storage.download_bytes(bucket, _GCS_CACHE_BLOB)
            if data:
                raw = json.loads(data)
                if isinstance(raw, dict):
                    logger.debug("EVM creation cache: loaded %d entries from GCS", len(raw))
                    return raw
    except Exception as exc:
        logger.debug("GCS cache load failed (will try local): %s", exc)

    # 2. Local file
    # 3. Seed file
    for label, path in [("local", _LOCAL_CACHE_FILE), ("seed", _SEED_FILE)]:
        if path.exists():
            try:
                raw = json.loads(path.read_text())
                if isinstance(raw, dict):
                    logger.debug("EVM creation cache: loaded %d entries from %s", len(raw), label)
                    return raw
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to load %s EVM creation cache: %s", label, exc)
    return {}


def _save_cache(cache: dict[str, str]) -> None:
    """Persist cache to GCS (read-merge-write) + local file."""
    # GCS
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            try:
                existing_data = storage.download_bytes(bucket, _GCS_CACHE_BLOB)
                if existing_data:
                    existing = json.loads(existing_data)
                    merged = {**existing, **cache} if isinstance(existing, dict) else cache
                else:
                    merged = cache
            except Exception:
                merged = cache

            merged_bytes = json.dumps(merged, indent=2, sort_keys=True).encode()
            storage.upload_bytes(bucket, _GCS_CACHE_BLOB, merged_bytes, content_type="application/json")
            logger.debug("EVM creation cache: saved %d entries to GCS", len(merged))
            cache.update(merged)
    except Exception as exc:
        logger.warning("Failed to save EVM creation cache to GCS: %s", exc)

    # Local file
    try:
        _LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _LOCAL_CACHE_FILE.write_bytes(json.dumps(cache, indent=2, sort_keys=True).encode())
    except OSError as exc:
        logger.warning("Failed to save local EVM creation cache: %s", exc)


# ── Shared cache session (same pattern as Solana) ────────────────────
_shared_cache: dict[str, str] | None = None
_shared_cache_initial_size: int = 0


class EvmCacheSession:
    """Context manager: single load before all EVM adapters, single save after.

    Usage (in orchestrator):
        with EvmCacheSession():
            await asyncio.gather(aave.get_instruments(), compound.get_instruments(), ...)
    """

    def __enter__(self) -> EvmCacheSession:
        global _shared_cache, _shared_cache_initial_size
        _shared_cache = _load_cache()
        _shared_cache_initial_size = len(_shared_cache)
        logger.info("EVM creation cache session started (%d entries loaded)", _shared_cache_initial_size)
        return self

    def __exit__(self, *_args: object) -> None:
        global _shared_cache, _shared_cache_initial_size
        if _shared_cache is not None:
            new_count = len(_shared_cache) - _shared_cache_initial_size
            if new_count > 0:
                _save_cache(_shared_cache)
                logger.info("EVM creation cache session closed (%d entries, %d new)", len(_shared_cache), new_count)
            else:
                logger.info("EVM creation cache session closed (%d entries, no new)", len(_shared_cache))
            _shared_cache = None
            _shared_cache_initial_size = 0


def _get_shared_or_load_cache() -> dict[str, str]:
    if _shared_cache is not None:
        return _shared_cache
    return _load_cache()


def _update_cache(new_entries: dict[str, str]) -> None:
    if not new_entries:
        return
    if _shared_cache is not None:
        _shared_cache.update(new_entries)
    else:
        cache = _load_cache()
        before = len(cache)
        cache.update(new_entries)
        if len(cache) > before:
            _save_cache(cache)


# ── Core resolution: binary search eth_getCode ───────────────────────


async def _get_code_at_block(
    session: aiohttp.ClientSession,
    url: str,
    address: str,
    block_hex: str,
) -> bool:
    """Check if contract has code at a given block. Returns True if deployed."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getCode",
        "params": [address, block_hex],
    }
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json()
    code = data.get("result", "0x")
    # "0x" or "" means no code (contract not deployed yet)
    return code not in ("0x", "", None)


async def _get_block_timestamp(
    session: aiohttp.ClientSession,
    url: str,
    block_number: int,
) -> datetime | None:
    """Get timestamp for a specific block number."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [hex(block_number), False],
    }
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json()
    result = data.get("result")
    if not result:
        return None
    ts_hex = result.get("timestamp", "0x0")
    ts = int(ts_hex, 16)
    return datetime.fromtimestamp(ts, tz=UTC)


async def _get_latest_block(session: aiohttp.ClientSession, url: str) -> int:
    """Get the latest block number."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": ["latest", False],
    }
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.json()
    return int(data["result"]["number"], 16)


async def resolve_contract_creation(
    address: str,
    chain: str,
    rpc_url: str | None = None,
    session: aiohttp.ClientSession | None = None,
) -> datetime | None:
    """Find when a contract was deployed via binary search on eth_getCode.

    Binary search: ~25 RPC calls for the full Ethereum block range.
    Returns the timestamp of the first block where the contract has code.
    """
    url = rpc_url
    if not url:
        url = _resolve_rpc_url(chain)
    if not url:
        logger.warning("No RPC URL for chain %s — cannot resolve contract creation", chain)
        return None

    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    try:
        latest = await _get_latest_block(session, url)

        # Quick check: does contract exist at latest block?
        has_code = await _get_code_at_block(session, url, address, hex(latest))
        if not has_code:
            logger.debug("Contract %s has no code at latest block on %s", address[:16], chain)
            return None

        # Binary search: find first block with code
        lo = 0
        hi = latest
        for _ in range(30):  # log2(25M) ~ 25, 30 gives margin
            if hi - lo <= 1:
                break
            mid = (lo + hi) // 2
            has_code = await _get_code_at_block(session, url, address, hex(mid))
            if has_code:
                hi = mid
            else:
                lo = mid

        # hi is the first block with code
        ts = await _get_block_timestamp(session, url, hi)
        if ts:
            logger.debug("Contract %s deployed at block %d on %s (%s)", address[:16], hi, chain, ts.isoformat())
        return ts

    except (aiohttp.ClientError, TimeoutError, KeyError) as exc:
        logger.warning("RPC error resolving %s on %s: %s", address[:16], chain, exc)
        return None
    finally:
        if owns_session:
            await session.close()


async def block_to_timestamp(
    block_number: int,
    chain: str,
    rpc_url: str | None = None,
) -> datetime | None:
    """Convert a block number to its timestamp. One RPC call."""
    url = rpc_url or _resolve_rpc_url(chain)
    if not url:
        return None
    async with aiohttp.ClientSession() as session:
        return await _get_block_timestamp(session, url, block_number)


# ── Batch resolution with caching ────────────────────────────────────


async def batch_resolve_evm_creation_timestamps(
    addresses: list[str],
    chain: str,
    rpc_url: str | None = None,
    concurrency: int = 3,
) -> dict[str, datetime]:
    """Resolve creation timestamps for multiple EVM contract addresses.

    Uses GCS cache (with local fallback) — creation dates never change,
    so each address is resolved once via RPC.

    Cache keys are ``{chain}:{address}`` to handle multi-chain addresses.

    Args:
        addresses: Contract addresses (0x-prefixed hex).
        chain: Chain name (e.g. "ETHEREUM", "ARBITRUM").
        rpc_url: Optional RPC URL override.
        concurrency: Max concurrent RPC binary searches. Default 3.

    Returns:
        Dict mapping address -> creation datetime (only for resolved ones).
    """
    import asyncio

    raw_cache = _get_shared_or_load_cache()
    results: dict[str, datetime] = {}
    to_resolve: list[str] = []
    chain_upper = chain.upper()

    for addr in addresses:
        cache_key = f"{chain_upper}:{addr.lower()}"
        cached_ts = raw_cache.get(cache_key)
        if cached_ts:
            try:
                results[addr] = datetime.fromisoformat(cached_ts)
            except ValueError:
                to_resolve.append(addr)
        else:
            to_resolve.append(addr)

    if results:
        logger.info("Loaded %d/%d EVM creation timestamps from cache (%s)", len(results), len(addresses), chain_upper)

    if not to_resolve:
        return results

    url = rpc_url or _resolve_rpc_url(chain_upper)
    if not url:
        logger.warning("No RPC URL for %s — returning %d cached only", chain_upper, len(results))
        return results

    sem = asyncio.Semaphore(concurrency)
    new_resolved: dict[str, datetime] = {}

    async with aiohttp.ClientSession() as session:

        async def _resolve_one(addr: str) -> None:
            async with sem:
                ts = await resolve_contract_creation(addr, chain_upper, rpc_url=url, session=session)
                if ts is not None:
                    new_resolved[addr] = ts

        await asyncio.gather(*[_resolve_one(a) for a in to_resolve])

    results.update(new_resolved)

    if new_resolved:
        new_iso: dict[str, str] = {f"{chain_upper}:{addr.lower()}": ts.isoformat() for addr, ts in new_resolved.items()}
        _update_cache(new_iso)
        logger.info("Resolved %d new EVM creation timestamps via RPC on %s", len(new_resolved), chain_upper)

    logger.info(
        "EVM creation timestamps (%s): %d/%d resolved (%d cached, %d new, %d unresolved)",
        chain_upper,
        len(results),
        len(addresses),
        len(results) - len(new_resolved),
        len(new_resolved),
        len(addresses) - len(results),
    )
    return results
