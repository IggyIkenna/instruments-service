"""Solana shared utilities for reference data adapters.

Provides creation timestamp resolution for Solana accounts/pools/vaults
via the getSignaturesForAddress RPC method, plus protocol-level floor
dates as guaranteed fallback (same pattern as Aave V3's deploy-date floor).

Resolved timestamps are cached to GCS (persists across container restarts)
with local file fallback for development. The expensive RPC pagination
only runs once per address — subsequent runs read from cache.
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
    "jito": datetime(2021, 11, 1, tzinfo=UTC),  # Jito stake pool mainnet launch
}

# ── Timestamp cache ───────────────────────────────────────────────────
# GCS primary (persists across deploys), local file fallback (dev),
# seed file in package (baked into Docker image — cold-start baseline).
_GCS_CACHE_BLOB = "_cache/solana_creation_timestamps.json"
_LOCAL_CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".cache"
_LOCAL_CACHE_FILE = _LOCAL_CACHE_DIR / "solana_creation_timestamps.json"
_SEED_FILE = Path(__file__).resolve().parent / "solana_creation_timestamps_seed.json"


def _get_gcs_bucket() -> str | None:
    """Resolve the DeFi instruments bucket for cache storage."""
    try:
        from unified_trading_library import get_bucket_name

        return get_bucket_name("instruments", "defi")
    except Exception:
        return None


def _load_cache() -> dict[str, str]:
    """Load cached address → ISO timestamp mapping.

    Priority: GCS (shared, persists across deploys) → local file (dev fallback).
    """
    # 1. Try GCS
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            data = storage.download_bytes(bucket, _GCS_CACHE_BLOB)
            if data:
                raw = json.loads(data)
                if isinstance(raw, dict):
                    logger.debug(
                        "Loaded Solana timestamp cache from GCS (%d entries)",
                        len(raw),
                    )
                    return raw
    except Exception as exc:
        logger.debug("GCS cache load failed (will try local): %s", exc)

    # 2. Local file fallback (dev)
    # 3. Seed file in package (baked into Docker image — cold-start baseline)
    for label, path in [("local", _LOCAL_CACHE_FILE), ("seed", _SEED_FILE)]:
        if path.exists():
            try:
                raw = json.loads(path.read_text())
                if isinstance(raw, dict):
                    logger.debug(
                        "Loaded Solana timestamp cache from %s file (%d entries)",
                        label,
                        len(raw),
                    )
                    return raw
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to load %s Solana timestamp cache: %s", label, exc)
    return {}


def _save_cache(cache: dict[str, str]) -> None:
    """Persist address → ISO timestamp mapping to GCS + local file.

    Uses read-merge-write on GCS to handle cross-VM concurrency safely.
    Multiple VMs (sharded by month) may resolve different addresses
    concurrently.  Since the cache is append-only (creation timestamps
    never change), merging is always conflict-free.
    """
    # 1. GCS (primary — persists across container restarts)
    # Read-merge-write: fetch current GCS state, merge our entries, write back.
    # This prevents last-writer-wins data loss when multiple VMs write concurrently.
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            # Read existing GCS cache (may have entries from other VMs)
            try:
                existing_data = storage.download_bytes(bucket, _GCS_CACHE_BLOB)
                if existing_data:
                    existing = json.loads(existing_data)
                    # Merge: existing entries + our entries (ours win on overlap,
                    # but values are identical for the same address anyway)
                    merged = {**existing, **cache} if isinstance(existing, dict) else cache
                else:
                    merged = cache
            except Exception:
                merged = cache

            merged_bytes = json.dumps(merged, indent=2, sort_keys=True).encode()
            storage.upload_bytes(
                bucket,
                _GCS_CACHE_BLOB,
                merged_bytes,
                content_type="application/json",
            )
            logger.debug(
                "Saved Solana timestamp cache to GCS (%d entries, %d from merge)",
                len(merged),
                len(merged) - len(cache),
            )
            # Update local cache dict with any entries we picked up from GCS
            cache.update(merged)
    except Exception as exc:
        logger.warning("Failed to save Solana timestamp cache to GCS: %s", exc)

    # 2. Local file (fallback for dev + warm start)
    # Re-serialize from cache dict which may have been enriched by GCS merge above
    try:
        _LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        local_bytes = json.dumps(cache, indent=2, sort_keys=True).encode()
        _LOCAL_CACHE_FILE.write_bytes(local_bytes)
    except OSError as exc:
        logger.warning("Failed to save local Solana timestamp cache: %s", exc)


# ── Shared cache session ──────────────────────────────────────────────
# Multiple Solana adapters run concurrently.  Without coordination each
# adapter calls _load_cache / _save_cache independently, leading to
# last-writer-wins data loss.  The session context manager loads the
# cache once before any adapter runs, accumulates all new entries in a
# shared dict, and writes once when all adapters are done.
_shared_cache: dict[str, str] | None = None  # None = no active session
_shared_cache_initial_size: int = 0  # size at load time — used to detect changes


class SolanaCacheSession:
    """Context manager: single load before all Solana adapters, single save after.

    Only writes back to GCS/local if new entries were actually added during the
    session.  This avoids pointless I/O once the cache has plateaued (all
    addresses resolved).

    Usage (in orchestrator):
        with SolanaCacheSession():
            await asyncio.gather(raydium.get_instruments(), orca.get_instruments(), ...)
    """

    def __enter__(self) -> SolanaCacheSession:
        global _shared_cache, _shared_cache_initial_size
        _shared_cache = _load_cache()
        _shared_cache_initial_size = len(_shared_cache)
        logger.info(
            "Solana cache session started (%d entries loaded)",
            _shared_cache_initial_size,
        )
        return self

    def __exit__(self, *_args: object) -> None:
        global _shared_cache, _shared_cache_initial_size
        if _shared_cache is not None:
            new_count = len(_shared_cache) - _shared_cache_initial_size
            if new_count > 0:
                _save_cache(_shared_cache)
                logger.info(
                    "Solana cache session closed (%d entries saved, %d new)",
                    len(_shared_cache),
                    new_count,
                )
            else:
                logger.info(
                    "Solana cache session closed (%d entries, no new — skipped save)",
                    len(_shared_cache),
                )
            _shared_cache = None
            _shared_cache_initial_size = 0


def _get_shared_or_load_cache() -> dict[str, str]:
    """Return the shared session cache if active, otherwise load fresh."""
    if _shared_cache is not None:
        return _shared_cache
    return _load_cache()


def _update_cache(new_entries: dict[str, str]) -> None:
    """Merge new entries into the shared cache, or save immediately if no session.

    Only triggers a save if new_entries contains keys not already in the cache.
    """
    if not new_entries:
        return
    if _shared_cache is not None:
        # Session active — accumulate, save happens in __exit__
        _shared_cache.update(new_entries)
    else:
        # No session — legacy path, save immediately
        cache = _load_cache()
        before = len(cache)
        cache.update(new_entries)
        if len(cache) > before:
            _save_cache(cache)
        else:
            logger.debug("Cache update skipped — no new entries")


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


def _is_public_rpc(url: str) -> bool:
    """Check if the URL is the rate-limited public Solana RPC."""
    return "api.mainnet-beta.solana.com" in url


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

    Key design decisions:
    - Mid-pagination errors (rate limits, timeouts) return the best
      result found so far rather than discarding all progress.
    - Paid RPCs (Alchemy etc.) get higher page limits and no delay.
    - Public RPC gets inter-page delay to avoid rate limiting.

    Args:
        address: Solana account address (base-58 encoded).
        rpc_url: Solana RPC endpoint URL. Defaults to public mainnet.
        session: Optional shared aiohttp session for connection pooling.

    Returns:
        datetime of first transaction, or None if unavailable.
    """
    import asyncio

    url = rpc_url or _get_solana_rpc_url()
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()

    is_public = _is_public_rpc(url)
    # Public RPC: conservative limit + inter-page delay to avoid 429s.
    # Paid RPC (Alchemy etc.): higher limit, small delay to avoid 429s
    # when multiple resolvers run concurrently.
    max_pages = 50 if is_public else 500
    page_delay = 0.2 if is_public else 0.05

    try:
        # Page backward through signatures to find the oldest
        oldest_block_time: int | None = None
        before: str | None = None
        page = 0

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

            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.debug(
                            "Solana RPC %d for %s page %d — returning best so far",
                            resp.status,
                            address[:16],
                            page,
                        )
                        break  # Return best result so far, not None
                    data: dict[str, object] = await resp.json()
            except (aiohttp.ClientError, TimeoutError) as exc:
                logger.debug(
                    "Solana RPC request error for %s page %d: %s — returning best so far",
                    address[:16],
                    page,
                    exc,
                )
                break  # Return best result so far

            # Check for RPC errors (e.g. rate limit JSON error responses)
            rpc_error = data.get("error")
            if rpc_error:
                logger.debug(
                    "Solana RPC error for %s page %d: %s — returning best so far",
                    address[:16],
                    page,
                    rpc_error,
                )
                break  # Return best result so far

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

            # Rate-limit delay for public RPC
            if page_delay > 0:
                await asyncio.sleep(page_delay)

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
            "Solana RPC session error for %s: %s",
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
    concurrency: int = 2,
) -> dict[str, datetime]:
    """Resolve creation timestamps for multiple Solana addresses.

    Uses a GCS cache (with local file fallback) — creation dates never
    change, so we only resolve each address once via RPC. Subsequent
    calls read from cache, even across container restarts.

    Concurrency is deliberately low (default 2) because each resolve
    paginates through hundreds of RPC pages. Even paid RPCs (Alchemy)
    return 429s when multiple resolvers paginate concurrently.

    Returns:
        Dict mapping address → creation datetime (only for resolved ones).
    """
    import asyncio

    # Load cache (shared session if active, otherwise fresh load)
    raw_cache = _get_shared_or_load_cache()
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

    # Merge new results into cache
    results.update(new_resolved)

    if new_resolved:
        new_iso: dict[str, str] = {addr: ts.isoformat() for addr, ts in new_resolved.items()}
        _update_cache(new_iso)
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
