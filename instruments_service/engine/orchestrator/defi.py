"""DeFi venue universe: venue list assembly, universe cache, manifest high-watermark monotonicity, wrapped-token relevance filtering, Solana creation cache.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_SOLANA_DEFI_VENUES",
    "_STATIC_DEFI_VENUES",
    "_SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX",
    "_build_defi_venues",
    "_count_per_venue",
    "_enforce_defi_monotonicity",
    "_get_defi_manifest_high_watermarks",
    "_get_or_fetch_defi_universe",
    "_normalize_wrapped_token",
    "_retry_regressed_venues",
    "clear_defi_universe_cache",
    "fill_solana_creation_cache",
    "filter_defi_instruments_by_relevance",
]


# ---------------------------------------------------------------------------
# DeFi venue list: dynamically built from UAC SUBGRAPH_IDS + static protocols
# ---------------------------------------------------------------------------
# Protocols with subgraph IDs are multi-chain — we discover all chains
# from the UAC registry so new chain deployments are picked up automatically.
_SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX: dict[str, str] = {
    "aave_v3": "AAVE_V3",
    "uniswap_v2": "UNISWAP_V2",
    "uniswap_v3": "UNISWAP_V3",
    "uniswap_v4": "UNISWAP_V4",
    "balancer": "BALANCER",
    "morpho": "MORPHO",
    "curve": "CURVE",
    "compound_v3": "COMPOUND_V3",
    # euler_v2 removed from universe — not needed yet.
    "fluid": "FLUID",
    # DEX forks — each has own subgraph IDs in UAC, reuse UniV3 adapter
    "pancakeswap_v3": "PANCAKESWAP_V3",
    "sushiswap_v3": "SUSHISWAP_V3",
    "aerodrome_v3": "AERODROME_V3",
    "camelot_v3": "CAMELOT_V3",
    "velodrome_v2": "VELODROME_V2",
    "trader_joe_v2": "TRADER_JOE_V2",
    "gmx": "GMX",
    "sushiswap": "SUSHISWAP",
    # Lending forks
    "spark": "SPARK",
}


# Protocols that don't use subgraphs (custom data sources — curated markets/
# registries, not GraphQL discovery). Not all are Ethereum-only: VENUS/RADIANT
# are multi-chain via a per-chain curated-markets dict (see venus.py/radiant.py
# _MVP_MARKETS_BY_CHAIN), BENQI is Avalanche-only, EULER_V2 is Ethereum-only.
_STATIC_DEFI_VENUES: list[str] = [
    "LIDO-ETHEREUM",
    "ETHERFI-ETHEREUM",
    "ETHENA-ETHEREUM",
    "EIGENLAYER-ETHEREUM",
    # Phase-4 lending protocols (2026-07-07 finding — factory.py/router.py already
    # wire venus/benqi/radiant/euler_v2 adapters incl. chain parsing via
    # defi_graph_adapters, but this venue list never requested them, so 0 real
    # catalogue rows were ever produced despite working code. Chains per adapter's
    # own _MVP_MARKETS_BY_CHAIN / _DEFAULT_CHAIN.
    "VENUS-BSC",
    "VENUS-ETHEREUM",
    "BENQI-AVALANCHE",
    "RADIANT-ARBITRUM",
    "RADIANT-BSC",
    "RADIANT-ETHEREUM",
    "EULER_V2-ETHEREUM",
]


# Solana DeFi venues (non-EVM, REST API-based discovery).
_SOLANA_DEFI_VENUES: list[str] = [
    "DRIFT-SOLANA",
    "KAMINO-SOLANA",
    "RAYDIUM-SOLANA",
    "ORCA-SOLANA",
    "MARINADE-SOLANA",
    "JITO-SOLANA",
    # Jupiter is execution-only (swap aggregator), not instrument discovery.
    # MarginFi + Solend Solana lending adapters (2026-07-09) — real public
    # REST/JSON APIs, now IS-producible (marginfi.py / solend.py).
    "MARGINFI-SOLANA",
    "SOLEND-SOLANA",
]
# NOTE: PACIFICA-SOLANA / LIGHTER-ZKSYNC / EXTENDED-STARKNET are on-chain perp
# CLOBs classified as CeFi (UAC VENUE_TO_ASSET_GROUP=cefi, same as HYPERLIQUID/
# ASTER). They were wrongly enumerated here → captured into the defi
# instrument-catalog. Reclassified to the UAC cefi registry 2026-06-25
# (instruments_foundation_completeness_2026_06_24.md): they ride the cefi
# backfill like HYPERLIQUID/ASTER, not the defi path.


def _build_defi_venues() -> list[str]:
    """Build venue list from protocols that have subgraph IDs + static venues."""
    venues: list[str] = []
    for protocol, prefix in _orch._SUBGRAPH_PROTOCOL_TO_VENUE_PREFIX.items():
        for chain in _orch.get_supported_chains_for_protocol(protocol):
            venues.append(f"{prefix}-{chain}")
    venues.extend(_orch._STATIC_DEFI_VENUES)
    venues.extend(_orch._SOLANA_DEFI_VENUES)
    return venues


def clear_defi_universe_cache() -> None:
    """Clear the DeFi universe cache. Call at the start of each batch run."""
    _orch._defi_universe_cache = None
    _orch._defi_universe_retryable = []


def _get_defi_manifest_high_watermarks() -> dict[str, int]:
    """Read the DeFi manifest and return the max instrument_count per venue.

    Only considers manifest entries from the current adapter epoch forward.
    Entries before the epoch (from older adapter logic with different filtering)
    are ignored — their counts are not comparable to the current code.

    DeFi instruments are monotonically increasing (immutable smart contracts,
    never deleted). If a fresh API call returns fewer instruments for a venue
    than the manifest's post-epoch maximum, the API gave an incomplete result.
    """
    try:
        bucket = _orch._get_instruments_bucket("DEFI")
        index_df = _orch.read_availability_index(bucket)
        if index_df.empty:
            return {}
        hwm: dict[str, int] = {}
        venue_vals: list[object] = list(index_df["venue"])
        count_vals: list[object] = list(index_df["instrument_count"])
        date_vals: list[object] = list(index_df["date"])
        for v_raw, c_raw, d_raw in zip(venue_vals, count_vals, date_vals, strict=True):
            v = str(v_raw)
            c = int(str(c_raw))
            d = str(d_raw)
            # Skip entries from before the current adapter epoch
            epoch = _orch._get_venue_epoch(v)
            if epoch is not None and d < epoch:
                continue
            if c > hwm.get(v, 0):
                hwm[v] = c
        return hwm
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="read_defi_manifest",
        )
        return {}


def _count_per_venue(records: list[_orch.InstrumentRecord]) -> dict[str, int]:
    """Count instruments per venue in a record list."""
    counts: dict[str, int] = {}
    for r in records:
        v = r.venue or "UNKNOWN"
        counts[v] = counts.get(v, 0) + 1
    return counts


async def _retry_regressed_venues(
    regressed_venues: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
) -> list[_orch.InstrumentRecord]:
    """Re-fetch instruments for venues that showed count regression.

    Returns the retry results (may still be regressed — caller decides).
    """
    _orch.logger.info(
        "DeFi monotonicity: retrying %d regressed venues: %s",
        len(regressed_venues),
        regressed_venues,
    )
    with _orch.SolanaCacheSession(), _orch.EvmCacheSession():
        retry_result = await _orch.fetch_instruments_for_all_venues(regressed_venues, api_keys=api_keys, mode=mode)
    return retry_result.records


def _enforce_defi_monotonicity(
    records: list[_orch.InstrumentRecord],
    hwm: dict[str, int],
) -> tuple[list[_orch.InstrumentRecord], set[str]]:
    """Remove venues from records that regressed below their manifest high-water mark.

    Returns (clean_records, blocked_venues). Blocked venues had fewer instruments
    than the manifest max and must NOT be written to GCS (would overwrite better data).
    Only checks venues that are actually present in the records — venues not fetched
    are ignored (they have 0 count but were never requested).
    """
    new_counts = _orch._count_per_venue(records)
    fetched_venues = set(new_counts.keys())
    blocked: set[str] = set()
    for venue, old_max in hwm.items():
        if venue not in fetched_venues:
            continue
        new_count = new_counts.get(venue, 0)
        if new_count < old_max:
            blocked.add(venue)
            _orch.logger.error(
                "DeFi monotonicity BLOCKED: %s has %d instruments (manifest max=%d) — "
                "will NOT write to GCS (would overwrite better data)",
                venue,
                new_count,
                old_max,
            )
        elif new_count > old_max:
            _orch.logger.info(
                "DeFi monotonicity OK: %s grew %d → %d (+%d)",
                venue,
                old_max,
                new_count,
                new_count - old_max,
            )

    if blocked:
        records = [r for r in records if r.venue not in blocked]
    return records, blocked


async def _get_or_fetch_defi_universe(
    defi_venues: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
) -> tuple[list[_orch.InstrumentRecord], list[str]]:
    """Return cached DeFi universe or fetch fresh.

    Includes a monotonicity check: if any venue returns fewer instruments
    than its historical max in the manifest, that venue is retried once.
    If still regressed after retry, the venue's records are REMOVED from
    the result — they will not be written to GCS (would overwrite better data).
    Good venues still proceed normally.

    Returns (records, retryable_venues). On cache hit, retryable_venues
    is the set from the original fetch.
    """

    if _orch._defi_universe_cache is not None:
        _orch.logger.info(
            "DeFi batch optimisation: reusing cached universe (%d instruments, skipping API calls)",
            len(_orch._defi_universe_cache),
        )
        return _orch._defi_universe_cache, _orch._defi_universe_retryable

    # First call in this batch run — fetch fresh
    _orch.logger.info(
        "DeFi batch optimisation: fetching full universe once (%d venues)",
        len(defi_venues),
    )
    with _orch.SolanaCacheSession(), _orch.EvmCacheSession():
        fetch_result = await _orch.fetch_instruments_for_all_venues(defi_venues, api_keys=api_keys, mode=mode)

    all_records = list(fetch_result.records)
    retryable = list(fetch_result.retryable_venues)

    # Monotonicity check: compare per-venue counts against manifest high-water marks
    # Scope to only the venues we actually fetched — otherwise venues not in the
    # request appear "regressed" (0 vs HWM) and trigger unnecessary retries.
    hwm = _orch._get_defi_manifest_high_watermarks()
    if hwm:
        new_counts = _orch._count_per_venue(all_records)
        fetched_venues = set(new_counts.keys())
        regressed = [v for v, mx in hwm.items() if v in fetched_venues and new_counts.get(v, 0) < mx]

        if regressed:
            for venue in regressed:
                _orch.logger.warning(
                    "DeFi monotonicity VIOLATION: %s has %d instruments (manifest max=%d)",
                    venue,
                    new_counts.get(venue, 0),
                    hwm[venue],
                )

            # Retry regressed venues once
            retry_records = await _orch._retry_regressed_venues(regressed, api_keys, mode)
            retry_counts = _orch._count_per_venue(retry_records)

            # For each regressed venue: use whichever fetch returned more
            for venue in regressed:
                old_count = new_counts.get(venue, 0)
                retry_count = retry_counts.get(venue, 0)
                if retry_count > old_count:
                    all_records = [r for r in all_records if r.venue != venue]
                    all_records.extend(r for r in retry_records if r.venue == venue)
                    _orch.logger.info(
                        "DeFi monotonicity: %s improved on retry (%d → %d)",
                        venue,
                        old_count,
                        retry_count,
                    )

        # Final enforcement: block any venues still below high-water mark
        all_records, blocked = _orch._enforce_defi_monotonicity(all_records, hwm)
        if blocked:
            _orch.logger.error(
                "DeFi monotonicity: %d venue(s) BLOCKED from GCS write: %s",
                len(blocked),
                sorted(blocked),
            )
    else:
        _orch.logger.info("DeFi monotonicity: no manifest history — skipping check (first run)")

    _orch._defi_universe_cache = all_records
    _orch._defi_universe_retryable = retryable
    _orch.logger.info(
        "DeFi batch optimisation: cached %d instruments from %d venues",
        len(_orch._defi_universe_cache),
        len(defi_venues),
    )
    return _orch._defi_universe_cache, _orch._defi_universe_retryable


# ---------------------------------------------------------------------------
# DEFI instrument relevance filter
# ---------------------------------------------------------------------------
def _normalize_wrapped_token(symbol: str) -> str:
    """Normalize wrapped/bridged token symbols to their canonical form.

    Strips chain-specific prefixes and suffixes so that tokens like avUSDC,
    aAvaDAI, USDT.e, renBTC match their canonical equivalents (USDC, DAI,
    USDT, BTC) in the major assets set.

    Prefix priority: longest match first to avoid false strips (e.g. "aAva"
    before "a" so aAvaDAI → DAI, not AvaDAI).
    """
    s = symbol.upper().strip()
    # Suffixes: .e (Avalanche bridged), .b (BNB bridged)
    for suffix in (".E", ".B"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    # Prefixes: longest first. aAva (Aave on Avalanche), av (Avalanche native),
    # ren (Ren bridge), st (staked variants handled by major list already)
    for prefix in ("AAVA", "AV", "REN"):
        if s.startswith(prefix) and len(s) > len(prefix):
            s = s[len(prefix) :]
            break
    return s


def filter_defi_instruments_by_relevance(records: list) -> list:
    """Filter DEFI instruments to major liquid assets only.

    The asset whitelist comes from config_reloaders.get_defi_major_assets()
    (InstrumentsDomainConfigState), which defaults to the hardcoded ETH/BTC/
    USDT/USDC and derivatives set and can be overridden via cloud ConfigStore.

    DEX_VENUE_KEYWORDS is the SSOT from UAC (includes EVM + Solana DEXes).

    Token matching uses _normalize_wrapped_token() to strip chain-specific
    prefixes/suffixes (avUSDC → USDC, aAvaDAI → DAI, renBTC → BTC, USDT.e → USDT).

    Rules:
    - DEX pools (Uniswap, Balancer, Curve, Orca, Raydium, Kamino): both
      base AND quote must match the major assets set (after normalization).
      Eliminates long-tail pairs like PEPE/WETH or FAITH/MILAREPA.
    - Lending protocols (Aave, Morpho, Fluid, LST services): base
      asset must match. Keeps aWETH, aWBTC, aUSDC etc.
    """
    major = _orch.get_defi_major_assets()  # reads from config_reloaders (hot-reloadable)
    result = []
    for r in records:
        raw_base = (getattr(r, "base_asset", None) or "").upper().strip()
        raw_quote = (getattr(r, "quote_asset", None) or "").upper().strip()
        base = raw_base if raw_base in major else _orch._normalize_wrapped_token(raw_base)
        quote = raw_quote if raw_quote in major else _orch._normalize_wrapped_token(raw_quote)
        venue = (getattr(r, "venue", None) or "").upper()
        is_dex = any(kw in venue for kw in _orch.DEX_VENUE_KEYWORDS)
        if is_dex:
            if base in major and quote in major:
                result.append(r)
            else:
                _orch.logger.debug(
                    "DEX relevance reject: venue=%s base=%s(raw=%s) quote=%s(raw=%s) symbol=%s",
                    venue,
                    base,
                    raw_base,
                    quote,
                    raw_quote,
                    getattr(r, "symbol", "?"),
                )
        else:
            if base in major:
                result.append(r)
            else:
                _orch.logger.debug(
                    "Lending relevance reject: venue=%s base=%s(raw=%s) symbol=%s",
                    venue,
                    base,
                    raw_base,
                    getattr(r, "symbol", "?"),
                )
    return result


async def fill_solana_creation_cache(
    api_keys: dict[str, str] | None = None,
) -> dict[str, int]:
    """Discover all Solana pool addresses and fill the creation timestamp cache.

    Runs all Solana adapters once to discover pool addresses, then uses
    Alchemy RPC to resolve creation timestamps for all discovered addresses.
    Results are saved to GCS cache for all future runs.

    Returns:
        Dict with cache statistics (cached, new, unresolved).
    """
    # 1. Discover all Solana pool addresses by running each adapter
    all_addresses: list[str] = []
    with _orch.SolanaCacheSession():
        fetch_result = await _orch.fetch_instruments_for_all_venues(
            _orch._SOLANA_DEFI_VENUES, api_keys=api_keys, mode="batch"
        )

    # Extract raw_symbol (which is the pool/account address) from each instrument
    for record in fetch_result.records:
        raw_sym = getattr(record, "raw_symbol", None)
        if raw_sym and isinstance(raw_sym, str) and len(raw_sym) > 20:
            all_addresses.append(raw_sym)

    if not all_addresses:
        _orch.logger.warning("Solana cache fill: no pool addresses discovered")
        return {"cached": 0, "new": 0, "unresolved": 0}

    # Deduplicate
    unique_addresses = list(dict.fromkeys(all_addresses))
    _orch.logger.info(
        "Solana cache fill: discovered %d unique pool addresses from %d instruments",
        len(unique_addresses),
        len(fetch_result.records),
    )

    # 2. Fill the cache with higher concurrency
    with _orch.SolanaCacheSession():
        results = await _orch.fill_solana_cache(unique_addresses, concurrency=4)

    cached_count = len(results)
    unresolved = len(unique_addresses) - cached_count
    _orch.logger.info(
        "Solana cache fill complete: %d resolved, %d unresolved out of %d total",
        cached_count,
        unresolved,
        len(unique_addresses),
    )
    return {"cached": cached_count, "new": cached_count, "unresolved": unresolved}
