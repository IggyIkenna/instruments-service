"""DeFi on-chain removal probe (Option B truth-gate).

Confirms whether a DeFi instrument's on-chain CONTRACT is gone and, only on a
POSITIVE confirmation, records a removal so the lifecycle roll-up can set
``delisted_at`` (``build_instrument_catalogue`` branch 1). This is the truth-gate
the Option A carve-out deliberately preserves: Option A keeps every DeFi drop-out
ACTIVE (``available_to=None``) because a capture/TVL drop is not a delisting; this
probe is the ONLY thing allowed to re-introduce a DeFi delisting, and only when the
chain itself says the contract no longer exists.

**Conservative by construction.** A removal is recorded ONLY when the probe
POSITIVELY observes the contract is gone (``eth_getCode`` returns empty at the
latest block). Any uncertainty — unresolvable RPC URL, non-EVM chain we cannot
probe, a network error, a malformed address — yields NO removal (the instrument
stays live). So the probe can never re-create the false-delisting Option A fixed;
it can only close a contract the chain confirms is gone (a rare SELFDESTRUCT).

Reuses the battle-tested EVM JSON-RPC primitives in
``reference_data/utils/evm_creation_resolver`` (``_get_code_at_block`` /
``_resolve_rpc_url`` / ``_make_session`` / ``_get_latest_block``) and mirrors its
GCS ``_cache/*.json`` side-artifact pattern. Solana / Starknet / Bitcoin /
Hyperliquid-L1 (chain_id 0) are NOT probed here (no EVM ``eth_getCode``) — they
degrade to "cannot determine → stay live".

Consumed by ``scripts/run_defi_removal_probe.py`` (the daily Cloud Run job) which
writes the artifact, and by ``build_instrument_catalogue`` which reads it via
``load_removal_delisted_at_map``.

Plan: ``defi_catalogue_available_to_false_delisting_2026_07_20`` (Option B) →
``defi_consolidated_closeout_2026_07_18`` Track 3.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import aiohttp
import pandas as pd

from instruments_service.reference_data.utils.evm_creation_resolver import (
    _get_code_at_block,
    _get_latest_block,
    _make_session,
    _resolve_rpc_url,
)

logger = logging.getLogger(__name__)

#: GCS side-artifact (in the instruments-store-defi bucket, alongside prod/catalog.parquet
#: and the _cache/*_creation_timestamps.json genesis caches this mirrors).
GCS_REMOVALS_BLOB = "_cache/defi_removals.json"

PROBE_SOURCE = "alchemy_rpc"
PROBE_KIND_EVM = "evm_eth_getcode_absent"

_DEFAULT_CONCURRENCY = 4


@dataclass(frozen=True)
class RemovalRecord:
    """One confirmed on-chain removal. ``delisted_at`` is the probe date (the date the
    chain confirmed the contract is gone) — a conservative, honest upper bound on the
    real removal date (we mark it delisted from when we PROVED it gone)."""

    canonical_id: str
    chain: str
    address: str
    delisted_at: str  # ISO YYYY-MM-DD
    probe_block: int
    probe_source: str
    probe_kind: str


def _norm(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v).strip()


def _is_evm_address(s: str) -> bool:
    """True iff ``s`` is a 0x-prefixed 20-byte hex address."""
    if len(s) != 42 or not s.startswith("0x"):
        return False
    try:
        int(s[2:], 16)
    except ValueError:
        return False
    return True


def probe_target(row: Mapping[str, object]) -> tuple[str, str] | None:
    """``(chain, evm_address)`` to probe for a catalogue row, or ``None`` if the row
    carries no EVM contract address we can check (skip → stays live).

    Address precedence: ``pool_address`` (pools) → ``raw_symbol`` → ``instrument_id``
    (token rows are commonly 0x-keyed). ``chain`` comes from the ``chain`` column."""
    chain = _norm(row.get("chain"))
    if not chain:
        return None
    for col in ("pool_address", "raw_symbol", "instrument_id"):
        addr = _norm(row.get(col)).lower()
        if _is_evm_address(addr):
            return chain.upper(), addr
    return None


async def probe_removal(
    chain: str,
    address: str,
    *,
    session: aiohttp.ClientSession,
    alchemy_key: str | None,
    as_of: datetime,
) -> RemovalRecord | None:
    """Probe ONE (chain, address). Returns a :class:`RemovalRecord` ONLY when the
    contract is POSITIVELY confirmed gone; ``None`` for exists / cannot-determine /
    error (conservative — never fabricates a removal)."""
    url = _resolve_rpc_url(chain, alchemy_key)
    if not url:
        # Non-EVM chain (chain_id 0) or unknown → cannot probe → keep live.
        return None
    try:
        latest = await _get_latest_block(session, url)
        has_code = await _get_code_at_block(session, url, address, hex(latest))
    except (aiohttp.ClientError, TimeoutError, KeyError, ValueError) as exc:
        logger.warning("removal probe: RPC error for %s on %s (%s) — treating as live", address[:12], chain, exc)
        return None
    if has_code:
        return None  # contract still exists — Option A stays: available_to=None
    canonical = _norm(address)
    return RemovalRecord(
        canonical_id=canonical,
        chain=chain,
        address=canonical,
        delisted_at=as_of.date().isoformat(),
        probe_block=latest,
        probe_source=PROBE_SOURCE,
        probe_kind=PROBE_KIND_EVM,
    )


def _dedupe_targets(catalog: pd.DataFrame) -> list[tuple[str, str, str]]:
    """Distinct ``(canonical_id, chain, address)`` probe targets from the catalogue,
    restricted to instruments that are currently LIVE (blank ``available_to``) with an
    EVM address."""
    targets: dict[str, tuple[str, str, str]] = {}
    if catalog.empty:
        return []
    has_at = "available_to" in catalog.columns
    for record in catalog.to_dict("records"):
        row: dict[str, object] = dict(record)
        if has_at and _norm(row.get("available_to")):
            continue  # already closed (truth-gate/expiry) — nothing to probe
        tgt = probe_target(row)
        if tgt is None:
            continue
        chain, addr = tgt
        cid = _norm(row.get("instrument_id")) or addr
        targets.setdefault(addr, (cid, chain, addr))
    return list(targets.values())


async def probe_catalogue_removals(
    catalog: pd.DataFrame,
    *,
    as_of: datetime,
    alchemy_key: str | None = None,
    concurrency: int = _DEFAULT_CONCURRENCY,
    limit: int | None = None,
) -> list[RemovalRecord]:
    """Probe every live EVM-addressed DeFi instrument in ``catalog`` and return the
    confirmed removals. Bounded concurrency; one shared aiohttp session."""
    targets = _dedupe_targets(catalog)
    if limit is not None:
        targets = targets[:limit]
    if not targets:
        return []
    logger.info("removal probe: %d live EVM-addressed targets (concurrency=%d)", len(targets), concurrency)
    sem = asyncio.Semaphore(max(1, concurrency))
    session = _make_session()
    removals: list[RemovalRecord] = []

    async def _one(cid: str, chain: str, addr: str) -> None:
        async with sem:
            rec = await probe_removal(chain, addr, session=session, alchemy_key=alchemy_key, as_of=as_of)
        if rec is not None:
            # preserve the catalogue canonical id (not just the address) as the key
            removals.append(
                RemovalRecord(
                    canonical_id=cid,
                    chain=rec.chain,
                    address=rec.address,
                    delisted_at=rec.delisted_at,
                    probe_block=rec.probe_block,
                    probe_source=rec.probe_source,
                    probe_kind=rec.probe_kind,
                )
            )

    try:
        await asyncio.gather(*(_one(cid, chain, addr) for cid, chain, addr in targets))
    finally:
        await session.close()
    logger.info("removal probe: %d/%d confirmed gone on-chain", len(removals), len(targets))
    return removals


# ── GCS side-artifact I/O (mirrors evm_creation_resolver's _cache/*.json pattern) ──


def _defi_store_bucket() -> str:
    from unified_trading_library import resolve_bucket_name  # noqa: qg-inside-import

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _record_from_dict(rec: Mapping[str, object]) -> RemovalRecord | None:
    try:
        return RemovalRecord(
            canonical_id=str(rec["canonical_id"]),
            chain=str(rec.get("chain", "")),
            address=str(rec.get("address", "")),
            delisted_at=str(rec["delisted_at"]),
            probe_block=int(str(rec.get("probe_block", "0")) or "0"),
            probe_source=str(rec.get("probe_source", "")),
            probe_kind=str(rec.get("probe_kind", "")),
        )
    except (KeyError, ValueError, TypeError):
        return None


def load_removals(bucket: str | None = None) -> dict[str, RemovalRecord]:
    """Load the removal side-artifact ``{key_lower: RemovalRecord}`` (keyed by BOTH
    ``canonical_id`` and ``address``). Empty when the artifact is absent (no removals
    probed yet — the expected first-run state, checked via ``blob_exists`` rather than
    an exception) or malformed; never raises — the roll-up degrades to Option A (all
    live)."""
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    bkt = bucket or _defi_store_bucket()
    storage = get_storage_client()
    if not storage.blob_exists(bkt, GCS_REMOVALS_BLOB):
        return {}
    try:
        raw = storage.download_bytes(bkt, GCS_REMOVALS_BLOB)
        payload: object = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return {}
    if not isinstance(payload, Mapping):
        return {}
    # Absent "removals" key = malformed/legacy artifact, not an error; degrades to
    # Option A (all live), same as the branches above.
    records_obj = payload.get("removals", [])  # noqa: qg-empty-fallback
    out: dict[str, RemovalRecord] = {}
    if not isinstance(records_obj, list):
        return out
    for rec in records_obj:
        if not isinstance(rec, Mapping):
            continue
        rr = _record_from_dict(rec)
        if rr is None:
            continue
        for key in (rr.canonical_id, rr.address):
            k = key.strip().lower()
            if k:
                out[k] = rr
    return out


def load_removal_delisted_at_map(bucket: str | None = None) -> dict[str, str]:
    """``{key_lower: delisted_at_iso}`` for the lifecycle roll-up — the minimal view
    ``build_instrument_catalogue`` needs to populate ``agg.delisted_at``."""
    return {k: v.delisted_at for k, v in load_removals(bucket).items()}


def write_removals(records: list[RemovalRecord], *, bucket: str | None = None, merge: bool = True) -> int:
    """Write (merge by default) the removal side-artifact. Returns the total count.

    Merge keeps previously-confirmed removals (a contract confirmed gone stays gone),
    so a probe run that transiently can't reach a chain never RESURRECTS a real
    removal — it just doesn't add new ones that run."""
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    bkt = bucket or _defi_store_bucket()
    storage = get_storage_client()
    by_addr: dict[str, RemovalRecord] = {}
    if merge:
        for rr in load_removals(bkt).values():
            by_addr[rr.address.lower()] = rr
    for rr in records:
        by_addr[rr.address.lower()] = rr
    ordered = sorted(by_addr.values(), key=lambda r: r.canonical_id)
    payload = {
        "schema": 1,
        "written_at": datetime.now(UTC).isoformat(),
        "removals": [asdict(r) for r in ordered],
    }
    buf = io.BytesIO(json.dumps(payload, indent=0).encode("utf-8"))
    storage.upload_bytes(bkt, GCS_REMOVALS_BLOB, buf.getvalue())
    logger.info("removal artifact: wrote %d removals → gs://%s/%s", len(ordered), bkt, GCS_REMOVALS_BLOB)
    return len(ordered)
