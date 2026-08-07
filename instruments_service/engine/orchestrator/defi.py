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
    # gmx removed from universe 2026-07-25 (operator ruling — perp_funding's
    # entire captured history was a synthetic OI-imbalance proxy, not real
    # funding-rate data; native subgraph query never worked for this venue).
    # SSOT: unified-trading-pm/plans/active/defi_gmx_venue_removal_2026_07_25.md.
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
    # LST / restaking / vault protocols (2026-07-18 wiring — factory + adapters
    # existed with populated curated registries but this venue list never
    # requested them, so 0 catalogue rows were ever produced despite working
    # code, exactly like the 2026-07-10 VENUS/RADIANT/BENQI finding. Only chains
    # with a POPULATED per-chain registry are listed (measured: each returns >=1
    # real instrument via the factory). Chains whose adapter registry is empty
    # for that chain (YEARN_V3-OPTIMISM / BEEFY-POLYGON / IDLE-ARBITRUM /
    # IDLE-POLYGON) are deliberately NOT enumerated — they'd emit 0 rows and
    # pollute honest-coverage as expected-but-always-empty; they stay phase=
    # "pipeline" in UAC until their curated vault addresses are researched.
    # UAC DEFI_VENUE_PHASE flips these to "live" in lockstep (denominator drift
    # guard: set(_build_defi_venues()) == VENUES_BY_ASSET_GROUP["defi"]).
    "ROCKETPOOL-ETHEREUM",
    "RENZO-ETHEREUM",
    "RENZO-ARBITRUM",
    "KELPDAO-ETHEREUM",
    "PUFFER-ETHEREUM",
    "KARAK-ETHEREUM",
    "KARAK-ARBITRUM",
    "SYMBIOTIC-ETHEREUM",
    "YEARN_V3-ETHEREUM",
    "YEARN_V3-ARBITRUM",
    "BEEFY-ETHEREUM",
    "BEEFY-ARBITRUM",
    "BEEFY-BASE",
    "BEEFY-AVALANCHE",
    "BEEFY-BSC",
    "PENDLE-ETHEREUM",
    "PENDLE-ARBITRUM",
    "CONVEX-ETHEREUM",
    "IDLE-ETHEREUM",
    # Single-token exchange-issued LSTs (cbeth.py / wbeth.py, new 2026-07-18).
    # cbETH = Coinbase (ETHEREUM only); wBETH = Binance (ETHEREUM + BSC, same
    # contract address on both chains).
    "COINBASE-ETHEREUM",
    "BINANCE-ETHEREUM",
    "BINANCE-BSC",
    # Chainlink oracle price feeds (chainlink.py, 2026-07-20 — resolves
    # BLK-0c7b82fe). Multi-chain via factory._DEFI_GRAPH_ADAPTERS chain
    # parsing; 45 curated aggregator addresses, verified subset of MTDS's
    # production _oracle_prices_constants.py. UAC flips these to phase="live"
    # in lockstep (denominator drift guard).
    "CHAINLINK-ETHEREUM",
    "CHAINLINK-ARBITRUM",
    "CHAINLINK-BASE",
    "CHAINLINK-OPTIMISM",
    "CHAINLINK-POLYGON",
    # AAVE V3 on-chain oracle reserves (aave_oracle.py, 2026-07-21 —
    # lst_rate_honest_coverage_2026_07_21.md Phase 1). Single fixed venue
    # (Ethereum-only Phase 0; NOT in factory._DEFI_GRAPH_ADAPTERS — no chain
    # parsing needed). UAC flips AAVE-ETHEREUM to phase="live" in lockstep
    # (denominator drift guard, unified-api-contracts@6bdbc31d).
    "AAVE-ETHEREUM",
    # 6 LST/vault venues (defi_venue_pipeline_to_live_ao_build_2026_07_30.md
    # todo 5 — operator ruling 2026-07-29). Genuine instruments-service
    # reference-data adapters (ankr.py/stader.py/stakewise.py/swell.py/
    # mantle.py/maker.py, todo 1), a verified-healthy production capture cron
    # (todo 2), a complete 90-day manifest backfill (todo 3, 90/90 days per
    # venue), and instruments-catalogue registration (todo 4) all preceded
    # this — UAC flips these to phase="live" in the SAME commit (denominator
    # drift guard, unified-api-contracts).
    "ANKR-ETHEREUM",
    "STADER-ETHEREUM",
    "STAKEWISE-ETHEREUM",
    "SWELL-ETHEREUM",
    "MANTLE-ETHEREUM",
    "MAKER-ETHEREUM",
    # AAVE-PLASMA (2026-08-01 — defi_plasma_chain_onboarding_gap_2026_07_26.md /
    # defi_satellite_ao_dispatch_batch6_2026_07_30.md todo). Genuinely an Aave V3
    # market (same aave_v3 adapter as every AAVE_V3-* venue; UAC
    # unified-api-contracts@18ed167f registers "AAVE-PLASMA": "aave_v3" in
    # VENUE_TO_ADAPTER_KEY) — its UAC venue constant is the bare "AAVE" form set
    # 2026-05-22 (before chain identity was resolved), not "AAVE_V3-PLASMA", so
    # the subgraph auto-gen loop above can never discover it (Plasma also has no
    # subgraph_id — RPC-only). Real capture verified: 18 manifest rows,
    # venue=AAVE_V3/chain=PLASMA, date=2026-07-30. UAC flipped phase
    # pipeline->live in the SAME window (unified-api-contracts@06c54fee) —
    # listing it here restores the denominator drift guard
    # (set(_build_defi_venues()) == VENUES_BY_ASSET_GROUP["defi"]).
    "AAVE-PLASMA",
    # FLUID-PLASMA (2026-08-05): FLUID lending markets on Plasma chain. Uses
    # the same fluid adapter as every other FLUID-* venue. Plasma has no
    # subgraph_id (RPC-only), so the subgraph auto-gen loop above can never
    # discover it — must be listed here manually to keep the denominator drift
    # guard green (set(_build_defi_venues()) == VENUES_BY_ASSET_GROUP["defi"]).
    # See unified-api-contracts registry/venue_adapter_keys.py: "FLUID-PLASMA":
    # "fluid", flipped defi_venues.py's DEFI_VENUE_PHASE to "live" in the
    # same window.
    "FLUID-PLASMA",
]


# Solana DeFi venues (non-EVM, REST API-based discovery).
# DRIFT (Solana) removed 2026-07-16 (operator ruling: all Solana perp DEXes
# dropped except Jupiter, not integrated). SSOT: unified-trading-pm/codex/
# 04-architecture/solana-defi-coverage.md.
_SOLANA_DEFI_VENUES: list[str] = [
    "JUPITER-SOLANA",
    "KAMINO-SOLANA",
    "RAYDIUM-SOLANA",
    "ORCA-SOLANA",
    "MARINADE-SOLANA",
    "JITO-SOLANA",
    # MarginFi + Solend Solana lending adapters (2026-07-09) — real public
    # REST/JSON APIs, now IS-producible (marginfi.py / solend.py).
    "MARGINFI-SOLANA",
    "SOLEND-SOLANA",
    # Solana LST / restaking / native-staking (2026-07-18 wiring — sanctum.py /
    # solblaze.py / jito_restaking.py / solana_native_staking.py adapters exist
    # with populated curated registries but were never requested. Each returns
    # >=1 real instrument (measured via the factory). UAC flips these to phase=
    # "live" in lockstep. Canonical venue spellings match each adapter's own
    # venue_tag: JITORESTAKING-SOLANA (not JITO_RESTAKING) and SOLANA-NATIVE-
    # SOLANA (not SOLANA_NATIVE) — the underscored forms would create duplicate
    # canonical venues and break the per-venue monotonicity count.
    "SANCTUM-SOLANA",
    "SOLBLAZE-SOLANA",
    "JITORESTAKING-SOLANA",
    "SOLANA-NATIVE-SOLANA",
    # Solana oracle (2026-07-20 DeFi catalogue canonicalization — pyth.py
    # adapter, Hermes upstream measured healthy). UAC keeps this phase="live"
    # (denominator drift guard).
    #
    # METEORA-SOLANA / LIFINITY-SOLANA / PHOENIX-SOLANA deliberately NOT listed
    # here (narrowed back out 2026-07-22): meteora.py/lifinity.py/phoenix.py
    # adapters are wired + registered in factory._ADAPTERS, but all 3
    # upstreams are measurably dead (app.meteora.ag/api/pools -> 404,
    # api.lifinity.io/pools -> no response/522, api.phoenix.trade -> NXDOMAIN;
    # re-verified 2026-07-22, same result as the original 2026-07-20 finding),
    # so requesting them here would enumerate 0-instrument venues and pollute
    # honest-coverage as expected-but-always-empty. UAC's DEFI_VENUE_PHASE for
    # these 3 is "pipeline" in lockstep (denominator drift guard) — re-add
    # here in the SAME commit an upstream migration/replacement makes the
    # adapter produce >=1 real instrument again. SSOT: unified-trading-pm/
    # plans/active/issues/uac_is_defi_oracle_dex_adapter_drift_2026_07_20.md.
    # CHAINLINK-* stays phase="live" via a separate per-chain adapter (not
    # Solana-listed here).
    "PYTH-SOLANA",
]
# NOTE: LIGHTER-ZKSYNC / EXTENDED-STARKNET are on-chain perp CLOBs classified
# as CeFi (UAC VENUE_TO_ASSET_GROUP=cefi, same as HYPERLIQUID/ASTER). They
# were wrongly enumerated here → captured into the defi instrument-catalog.
# Reclassified to the UAC cefi registry 2026-06-25
# (instruments_foundation_completeness_2026_06_24.md): they ride the cefi
# backfill like HYPERLIQUID/ASTER, not the defi path. (PACIFICA (Solana) was
# a third venue in this note until removed entirely 2026-07-16 — operator
# ruling: all Solana perp DEXes dropped except Jupiter, not integrated.)


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

    Thin wrapper over the asset-group-parameterized
    ``venue_core._get_manifest_high_watermarks`` (extracted 2026-07-10,
    Todo 6 of cefi_monotonicity_guard_alerting_and_dark_venues_2026_07_07.md)
    — kept as a zero-arg DeFi-named entry point since callers/tests already
    reference this exact name.
    """
    return _orch._get_manifest_high_watermarks("DEFI")


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
    """Block only a CATASTROPHIC per-venue count collapse; let real delistings through.

    Returns (clean_records, blocked_venues). Blocked venues collapsed to *below half*
    their manifest max — a broken / partial API fetch that would overwrite better data —
    and must NOT be written to GCS. Only checks venues actually present in the records
    (venues not fetched this run have 0 count but were never requested, not a regression).

    R2c honest-``available_to`` relax (IS R2c — instruments-service availability-
    denominator honesty): the original DeFi policy was ``min_ratio=1.0`` ("any decrease
    at all is an incomplete fetch — immutable smart-contract instruments never shrink").
    That premise is wrong at the *per-instrument active-set* grain the catalogue tracks:
    a governance-token delisting, a pool going below a subgraph's TVL cut, or a lending
    market being retired IS a real, small decrease in a venue's live count. Blocking on
    it SUPPRESSED the fresh (smaller) record set, so the delisting never reached
    ``by_date/`` → the catalogue roll-up never observed the drop → the instrument's
    ``available_to`` was never closed and it sat permanently "active". A real count
    regression is a real delist, not an error.

    So this now passes ``min_ratio=_CEFI_TRADFI_THIN_COLLAPSE_RATIO`` (0.5) while keeping
    ``block_on_regression=True``: a real delisting (>=50% of the HWM retained) writes
    through — the drop reaches ``by_date/`` and the roll-up's §7.3 liveness closes
    ``available_to`` at the instrument's last-seen day — while a genuinely broken fetch
    (a subgraph returning a fraction of the universe, <50% of the HWM) is still blocked
    from clobbering good data. This is now the SAME thin-collapse ratio CeFi/TradFi use
    (``venue_core`` docstring), since CeFi delistings, TradFi expiries and DeFi delistings
    are all legitimate small decreases in today's active count. The full per-instrument
    TVL-time-series close is a documented follow-up; this is the guard-relax + last-seen
    ``available_to`` first cut.
    """
    return _orch._enforce_monotonicity(
        records, hwm, block_on_regression=True, min_ratio=_orch._CEFI_TRADFI_THIN_COLLAPSE_RATIO
    )


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
            pool_address = (getattr(r, "pool_address", None) or getattr(r, "raw_symbol", None) or "").lower()
            if base in major and quote in major:
                result.append(r)
            elif _orch.is_defi_force_include_pool(pool_address):
                # Operator-curated high-TVL pool allowlist (UAC SSOT) — kept even
                # when a leg is outside the major-assets set (e.g. high-liquidity
                # Raydium pools that would otherwise be relevance-rejected).
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
