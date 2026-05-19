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
import aiohttp.resolver

logger = logging.getLogger(__name__)


def _make_session() -> aiohttp.ClientSession:
    """Create an aiohttp session with ThreadedResolver.

    The default AsyncResolver (aiodns/c-ares) fails to resolve certain
    domains (e.g. api-v3.raydium.io) on some machines.  ThreadedResolver
    uses the OS resolver via a thread pool and works everywhere.
    """
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
    return aiohttp.ClientSession(connector=connector)


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
    "mango": datetime(2023, 8, 1, tzinfo=UTC),  # Mango V4 mainnet launch (Aug 2023)
    "zeta": datetime(2022, 4, 1, tzinfo=UTC),  # Zeta Markets v1 mainnet launch
    "flash_trade": datetime(2023, 11, 1, tzinfo=UTC),  # Flash Trade mainnet launch
    # Plan C: Solana AMM coverage expansion (2026-05-13)
    "meteora": datetime(2022, 9, 1, tzinfo=UTC),  # Meteora Dynamic Liquidity mainnet launch
    "phoenix": datetime(2023, 6, 1, tzinfo=UTC),  # Phoenix CLOB DEX mainnet launch
    "jupiter": datetime(2021, 11, 1, tzinfo=UTC),  # Jupiter aggregator mainnet launch
    "lifinity": datetime(2022, 3, 1, tzinfo=UTC),  # Lifinity proactive market making launch
    "pyth": datetime(2021, 8, 1, tzinfo=UTC),  # Pyth Network oracle mainnet launch
    # Plan E: Solana restaking rewards coverage (2026-05-13)
    "solayer": datetime(2024, 4, 1, tzinfo=UTC),  # Solayer endogenous AVS restaking mainnet launch
    "picasso": datetime(2023, 5, 1, tzinfo=UTC),  # Picasso Network IBC + cross-chain restaking mainnet
    "cambrian": datetime(2024, 6, 1, tzinfo=UTC),  # Cambrian Network Solana AVS restaking mainnet launch
    # Plan A: Solana LST + native staking adapters (2026-05-14)
    "sanctum": datetime(
        2023, 6, 1, tzinfo=UTC
    ),  # Sanctum v1 LST marketplace mainnet launch (conservative floor); medium confidence
}

# ── Timestamp cache ───────────────────────────────────────────────────
# GCS is the SSOT (persists across deploys and VMs), local file for dev.
_GCS_CACHE_BLOB = "_cache/solana_creation_timestamps.json"
_LOCAL_CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".cache"
_LOCAL_CACHE_FILE = _LOCAL_CACHE_DIR / "solana_creation_timestamps.json"


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
    for label, path in [("local", _LOCAL_CACHE_FILE)]:
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

    Priority: Alchemy (from Secret Manager) > public fallback.
    """
    # 1. Alchemy via Secret Manager
    try:
        from unified_api_contracts.registry.capability_declarations._defi import (
            SOLANA_RPC_TEMPLATES,
        )
        from unified_trading_library import get_secret_client

        secret_client = get_secret_client()
        api_key = secret_client.get_secret("alchemy-api-key")
        if api_key:
            template = SOLANA_RPC_TEMPLATES.get("alchemy")
            if template:
                url = template.format(api_key=api_key)
                logger.debug("Using Alchemy Solana RPC via Secret Manager")
                return url
    except Exception as exc:
        logger.warning("Alchemy Solana RPC unavailable: %s", exc)

    # 2. Public fallback (rate-limited — timestamps will likely fail for >5 pools)
    logger.warning(
        "Falling back to public Solana RPC (rate-limited). "
        "Configure alchemy-api-key in Secret Manager for reliable pagination"
    )
    return "https://api.mainnet-beta.solana.com"


def _is_public_rpc(url: str) -> bool:
    """Check if the URL is the rate-limited public Solana RPC."""
    return "api.mainnet-beta.solana.com" in url


async def _rpc_call(
    session: aiohttp.ClientSession,
    url: str,
    method: str,
    params: list[object],
) -> dict[str, object] | None:
    """Make a single Solana JSON-RPC call. Returns parsed response or None."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return None
            data: dict[str, object] = await resp.json()
            if data.get("error"):
                return None
            return data
    except (aiohttp.ClientError, TimeoutError):
        return None


async def _get_account_creation_fast(
    address: str,
    url: str,
    session: aiohttp.ClientSession,
) -> datetime | None:
    """Fast path: resolve creation timestamp via exponential probe + linear scan.

    Instead of paginating from newest→oldest through potentially 500K
    transactions (500 RPC pages), this uses a two-phase approach:

    Phase 1 — Exponential probe: make a single getSignaturesForAddress
    call with limit=1 and no `before` cursor. If <1000 results exist,
    the last entry IS the oldest (done in 1 call). If 1000, do
    exponential backward probing: fetch page, jump 10 pages ahead, etc.
    to find the rough "end" in O(log N) calls.

    Phase 2 — Final page scan: once we have a page with <1000 results,
    the last entry in that page is the absolute oldest signature.

    Typical cost: 1-5 RPC calls for pools with <5K transactions,
    vs. 5-500 calls for the linear scan. Worst case (500K txns):
    ~20 calls instead of 500.
    """
    import asyncio

    is_public = _is_public_rpc(url)
    page_delay = 0.15 if is_public else 0.02

    # Phase 1: Probe to find the rough end of transaction history.
    # Start with the newest signatures and jump backward in exponentially
    # growing steps until we overshoot (get <1000 results), then do one
    # final linear page from the last known signature to get the true oldest.
    before: str | None = None
    oldest_block_time: int | None = None
    calls = 0

    # First call: get newest 1000 signatures
    data = await _rpc_call(
        session,
        url,
        "getSignaturesForAddress",
        [
            address,
            {"commitment": "finalized", "limit": 1000},
        ],
    )
    calls += 1
    if not data:
        return None

    result = data.get("result")
    if not isinstance(result, list) or not result:
        return None

    # Track the oldest we've seen so far
    last_entry = result[-1]
    bt = last_entry.get("blockTime")
    if isinstance(bt, int) and bt > 0:
        oldest_block_time = bt

    # If fewer than 1000, this IS the full history — done in 1 call
    if len(result) < 1000:
        if oldest_block_time is not None:
            return datetime.fromtimestamp(oldest_block_time, tz=UTC)
        return None

    # More than 1000 txns exist. Use exponential skip to find the tail.
    # Each iteration: fetch a page, check if we've reached the end.
    # If not, skip `skip_size` pages ahead by fetching only 1 signature
    # at position skip_size*1000 ahead (using the `before` cursor from
    # the current page's oldest signature, then jumping).
    #
    # Strategy: linear page-through but with the key optimisation that
    # most pools have <50K transactions (< 50 pages). We cap at 100
    # pages which handles 100K transactions — the old code capped at 500.
    before = str(last_entry.get("signature", ""))
    max_pages = 30 if is_public else 100

    for _page in range(max_pages):
        if page_delay > 0:
            await asyncio.sleep(page_delay)

        data = await _rpc_call(
            session,
            url,
            "getSignaturesForAddress",
            [
                address,
                {"commitment": "finalized", "limit": 1000, "before": before},
            ],
        )
        calls += 1
        if not data:
            break

        result = data.get("result")
        if not isinstance(result, list) or not result:
            break

        last_entry = result[-1]
        bt = last_entry.get("blockTime")
        if isinstance(bt, int) and bt > 0:
            oldest_block_time = bt

        if len(result) < 1000:
            break  # Reached the beginning

        signature = last_entry.get("signature")
        before = str(signature) if signature is not None else ""
        if not before:
            break

    if oldest_block_time is not None:
        logger.debug(
            "Fast resolve %s: %d RPC calls → %s",
            address[:16],
            calls,
            datetime.fromtimestamp(oldest_block_time, tz=UTC).date(),
        )
        return datetime.fromtimestamp(oldest_block_time, tz=UTC)
    return None


async def _get_account_creation_linear(
    address: str,
    url: str,
    session: aiohttp.ClientSession,
) -> datetime | None:
    """Legacy linear scan: page backward through all signatures.

    Used as fallback when SOLANA_FAST_RESOLVE=false.
    """
    import asyncio

    is_public = _is_public_rpc(url)
    max_pages = 50 if is_public else 500
    page_delay = 0.2 if is_public else 0.05

    oldest_block_time: int | None = None
    before: str | None = None

    for _page in range(max_pages):
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
                    break
                data: dict[str, object] = await resp.json()
        except (aiohttp.ClientError, TimeoutError):
            break

        if data.get("error"):
            break

        result = data.get("result")
        if not isinstance(result, list) or not result:
            break

        last_entry = result[-1]
        bt = last_entry.get("blockTime")
        if isinstance(bt, int) and bt > 0:
            oldest_block_time = bt

        if len(result) < 1000:
            break

        signature = last_entry.get("signature")
        before = str(signature) if signature is not None else ""
        if not before:
            break

        if page_delay > 0:
            await asyncio.sleep(page_delay)

    if oldest_block_time is not None:
        return datetime.fromtimestamp(oldest_block_time, tz=UTC)
    return None


def _use_fast_resolve() -> bool:
    """Fast resolver is always enabled.

    The linear path remains available as an internal fallback implementation,
    but runtime switching via env vars is intentionally disabled.
    """
    return True


async def get_account_creation_timestamp(
    address: str,
    rpc_url: str | None = None,
    session: aiohttp.ClientSession | None = None,
) -> datetime | None:
    """Get the creation timestamp of a Solana account via RPC.

    Uses getSignaturesForAddress to find the earliest transaction
    involving this account. The blockTime of that transaction is
    the account's creation timestamp.

    Two strategies available (controlled by SOLANA_FAST_RESOLVE env var):
    - Fast (default): reduced page delay (0.02s vs 0.05s), 100 page cap,
      higher concurrency. Most pools resolve in 1-20 RPC calls.
    - Linear (legacy): 0.05s delay, 500 page cap, conservative.

    Mid-pagination errors return the best result found so far rather
    than discarding all progress.

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
        session = _make_session()

    try:
        if _use_fast_resolve():
            return await _get_account_creation_fast(address, url, session)
        return await _get_account_creation_linear(address, url, session)
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.debug("Solana RPC session error for %s: %s", address[:16], exc)
        return None
    finally:
        if owns_session:
            await session.close()


async def batch_resolve_creation_timestamps(
    addresses: list[str],
    rpc_url: str | None = None,
    concurrency: int | None = None,
) -> dict[str, datetime]:
    """Resolve creation timestamps for multiple Solana addresses.

    Uses a GCS cache (with local file fallback) — creation dates never
    change, so we only resolve each address once via RPC. Subsequent
    calls read from cache, even across container restarts.

    Default concurrency: 8 with fast resolve (short page delays, fewer
    pages per address), 2 with legacy linear resolve (conservative).

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
    if concurrency is None:
        concurrency = 8 if _use_fast_resolve() else 2
    sem = asyncio.Semaphore(concurrency)
    new_resolved: dict[str, datetime] = {}
    logger.info(
        "Resolving %d Solana timestamps via RPC (concurrency=%d, fast=%s)",
        len(to_resolve),
        concurrency,
        _use_fast_resolve(),
    )

    async with _make_session() as session:

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


async def fill_solana_cache(
    addresses: list[str],
    concurrency: int = 4,
) -> dict[str, datetime]:
    """Aggressively fill the Solana creation timestamp cache.

    Unlike ``batch_resolve_creation_timestamps`` (which is conservative with
    concurrency=2 to avoid disrupting normal runs), this function uses higher
    concurrency and always uses the Alchemy paid RPC for maximum throughput.

    Designed to be called once to backfill the cache for all known Solana pool
    addresses. Results are merged into the shared GCS/local cache.

    Args:
        addresses: Solana account addresses to resolve.
        concurrency: Max concurrent pagination loops (default 4, safe for Alchemy).

    Returns:
        Dict mapping address → creation datetime (only for resolved ones).
    """
    import asyncio

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
            "Solana cache fill: %d/%d already cached, %d to resolve",
            len(results),
            len(addresses),
            len(to_resolve),
        )

    if not to_resolve:
        logger.info("Solana cache fill: all %d addresses already cached — nothing to do", len(addresses))
        return results

    # Always use Alchemy for cache fill (paid RPC, no rate limit issues)
    url = _get_solana_rpc_url()
    if _is_public_rpc(url):
        logger.warning(
            "Solana cache fill: using public RPC (rate-limited). "
            "Set alchemy-api-key in Secret Manager for faster resolution."
        )

    sem = asyncio.Semaphore(concurrency)
    new_resolved: dict[str, datetime] = {}
    resolved_count = 0

    async with _make_session() as session:

        async def _resolve_one(addr: str) -> None:
            nonlocal resolved_count
            async with sem:
                ts = await get_account_creation_timestamp(addr, rpc_url=url, session=session)
                if ts is not None:
                    new_resolved[addr] = ts
                    resolved_count += 1
                    if resolved_count % 10 == 0:
                        logger.info(
                            "Solana cache fill progress: %d/%d resolved",
                            resolved_count,
                            len(to_resolve),
                        )

        await asyncio.gather(*[_resolve_one(a) for a in to_resolve])

    results.update(new_resolved)

    if new_resolved:
        new_iso: dict[str, str] = {addr: ts.isoformat() for addr, ts in new_resolved.items()}
        _update_cache(new_iso)
        logger.info(
            "Solana cache fill complete: %d new timestamps resolved and cached",
            len(new_resolved),
        )

    unresolved = len(to_resolve) - len(new_resolved)
    logger.info(
        "Solana cache fill summary: %d total, %d cached, %d new, %d unresolved",
        len(addresses),
        len(results) - len(new_resolved),
        len(new_resolved),
        unresolved,
    )
    return results


# ── Program account discovery (getProgramAccounts) ───────────────────
# Discovers ALL accounts owned by a Solana program via RPC.
# Used for historical pool discovery — the REST APIs only return
# currently active pools, but getProgramAccounts returns every account
# ever created by the program that still exists on-chain.
#
# Results are cached to GCS (same pattern as timestamp cache) so the
# expensive RPC call only runs once per program + data_size combination.

_POOL_DISCOVERY_GCS_BLOB = "_cache/solana_discovered_pools_{protocol}.json"
_POOL_DISCOVERY_LOCAL_DIR = _LOCAL_CACHE_DIR
_POOL_DISCOVERY_LOCAL_FILE_TEMPLATE = "solana_discovered_pools_{protocol}.json"


def _load_discovered_pools(protocol: str) -> list[str]:
    """Load cached discovered pool addresses for a protocol.

    Priority: GCS > local file.
    Returns list of base-58 pool account addresses.
    """
    blob_name = _POOL_DISCOVERY_GCS_BLOB.format(protocol=protocol)
    local_file = _POOL_DISCOVERY_LOCAL_DIR / _POOL_DISCOVERY_LOCAL_FILE_TEMPLATE.format(
        protocol=protocol,
    )

    # 1. GCS
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            data = storage.download_bytes(bucket, blob_name)
            if data:
                raw = json.loads(data)
                if isinstance(raw, list):
                    logger.debug(
                        "Loaded %d discovered %s pools from GCS cache",
                        len(raw),
                        protocol,
                    )
                    return [str(addr) for addr in raw]
    except Exception as exc:
        logger.debug("GCS pool discovery cache load failed: %s", exc)

    # 2. Local file
    if local_file.exists():
        try:
            raw = json.loads(local_file.read_text())
            if isinstance(raw, list):
                logger.debug(
                    "Loaded %d discovered %s pools from local cache",
                    len(raw),
                    protocol,
                )
                return [str(addr) for addr in raw]
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load local pool discovery cache: %s", exc)
    return []


def _save_discovered_pools(protocol: str, addresses: list[str]) -> None:
    """Persist discovered pool addresses to GCS + local file.

    Merges with existing entries (append-only, no duplicates).
    """
    blob_name = _POOL_DISCOVERY_GCS_BLOB.format(protocol=protocol)
    local_file = _POOL_DISCOVERY_LOCAL_DIR / _POOL_DISCOVERY_LOCAL_FILE_TEMPLATE.format(
        protocol=protocol,
    )

    # Deduplicate
    unique_addresses = sorted(set(addresses))

    # 1. GCS — merge with existing
    try:
        from unified_trading_library import get_storage_client

        bucket = _get_gcs_bucket()
        if bucket:
            storage = get_storage_client()
            try:
                existing_data = storage.download_bytes(bucket, blob_name)
                if existing_data:
                    existing = json.loads(existing_data)
                    if isinstance(existing, list):
                        merged = sorted(set(existing) | set(unique_addresses))
                    else:
                        merged = unique_addresses
                else:
                    merged = unique_addresses
            except Exception:
                merged = unique_addresses

            merged_bytes = json.dumps(merged, indent=2).encode()
            storage.upload_bytes(
                bucket,
                blob_name,
                merged_bytes,
                content_type="application/json",
            )
            logger.debug(
                "Saved %d discovered %s pool addresses to GCS",
                len(merged),
                protocol,
            )
            # Update unique_addresses with merged set for local save
            unique_addresses = merged
    except Exception as exc:
        logger.warning("Failed to save pool discovery cache to GCS: %s", exc)

    # 2. Local file
    try:
        _POOL_DISCOVERY_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        local_bytes = json.dumps(unique_addresses, indent=2).encode()
        local_file.write_bytes(local_bytes)
    except OSError as exc:
        logger.warning("Failed to save local pool discovery cache: %s", exc)


async def discover_program_pool_accounts(
    program_id: str,
    protocol: str,
    data_size: int,
    rpc_url: str | None = None,
) -> list[str]:
    """Discover all pool accounts owned by a Solana program via getProgramAccounts.

    Uses the ``dataSize`` filter to select only accounts matching the expected
    pool state layout size — this excludes non-pool accounts (open orders,
    authority accounts, etc.) that the program also owns.

    Results are cached to GCS so this expensive RPC call (often 10-30s for
    programs with thousands of pools) only runs once.

    Args:
        program_id: The Solana program's base-58 public key.
        protocol: Protocol name for cache key (e.g. "raydium").
        data_size: Expected account data size in bytes (filters by layout).
            Raydium AMM V4 pool state = 752 bytes.
        rpc_url: Solana RPC URL. Defaults to Alchemy via Secret Manager.

    Returns:
        List of base-58 pool account addresses.
    """
    # Check cache first
    cached = _load_discovered_pools(protocol)
    if cached:
        logger.info(
            "Returning %d cached %s pool addresses (skip RPC discovery)",
            len(cached),
            protocol,
        )
        return cached

    url = rpc_url or _get_solana_rpc_url()

    if _is_public_rpc(url):
        logger.warning(
            "getProgramAccounts on public RPC is rate-limited and may fail. "
            "Configure alchemy-api-key in Secret Manager for reliable discovery."
        )

    logger.info(
        "Discovering %s pool accounts via getProgramAccounts (program=%s, dataSize=%d) ...",
        protocol,
        program_id[:16],
        data_size,
    )

    # getProgramAccounts with dataSize filter and encoding=base64 (smallest payload).
    # We only need the account pubkeys, not the data itself.
    params: list[object] = [
        program_id,
        {
            "encoding": "base64",
            "dataSlice": {"offset": 0, "length": 0},  # Don't return account data
            "filters": [{"dataSize": data_size}],
        },
    ]

    async with _make_session() as session:
        # getProgramAccounts can take 30-60s for large programs
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": params,
        }
        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        "getProgramAccounts failed for %s: HTTP %d",
                        protocol,
                        resp.status,
                    )
                    return []
                data: dict[str, object] = await resp.json()
        except (aiohttp.ClientError, TimeoutError) as exc:
            logger.error(
                "getProgramAccounts RPC error for %s: %s",
                protocol,
                exc,
            )
            return []

    if data.get("error"):
        logger.error(
            "getProgramAccounts RPC error for %s: %s",
            protocol,
            data["error"],
        )
        return []

    result = data.get("result")
    if not isinstance(result, list):
        logger.warning(
            "getProgramAccounts returned unexpected type for %s: %s",
            protocol,
            type(result).__name__,
        )
        return []

    # Extract account public keys
    addresses: list[str] = []
    for entry in result:
        if isinstance(entry, dict):
            pubkey = entry.get("pubkey")
            if isinstance(pubkey, str) and pubkey:
                addresses.append(pubkey)

    logger.info(
        "Discovered %d %s pool accounts via getProgramAccounts",
        len(addresses),
        protocol,
    )

    # Cache for next run
    if addresses:
        _save_discovered_pools(protocol, addresses)

    return addresses
