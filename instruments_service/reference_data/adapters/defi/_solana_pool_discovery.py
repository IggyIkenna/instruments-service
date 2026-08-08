"""Solana pool discovery via getProgramAccounts RPC.

Discovers all pool accounts owned by a Solana program via the
getProgramAccounts RPC method, with GCS+local caching so the
expensive RPC call only runs once per program / data_size pair.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import aiohttp

from ._solana_utils import _get_gcs_bucket, _get_solana_rpc_url, _is_public_rpc, _make_session

logger = logging.getLogger(__name__)

# ── Program account discovery (getProgramAccounts) ───────────────────
# Discovers ALL accounts owned by a Solana program via RPC.
# Used for historical pool discovery — the REST APIs only return
# currently active pools, but getProgramAccounts returns every account
# ever created by the program that still exists on-chain.
#
# Results are cached to GCS (same pattern as timestamp cache) so the
# expensive RPC call only runs once per program + data_size combination.

_POOL_DISCOVERY_GCS_BLOB = "_cache/solana_discovered_pools_{protocol}.json"
_POOL_DISCOVERY_LOCAL_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".cache"
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
            # GCS read boundary: download_bytes doesn't pre-wrap the GCS SDK's exception
            # surface (NotFound/network/auth — many types), and read-merge is
            # best-effort by design (write still proceeds below with the un-merged
            # addresses). Audited 2026-07-25, left broad:
            # instruments_service_codex_compliance_ceiling_drift_2026_07_20.md P3 #3.
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
