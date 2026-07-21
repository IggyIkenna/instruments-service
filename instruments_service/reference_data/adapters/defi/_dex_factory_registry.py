# Epic: defi_consolidated_closeout_2026_07_18
# Lifecycle: permanent
# Delete-when: NA
"""Static factory-contract-address → DEX protocol-version registry.

Operator ruling (``unified-trading-pm/plans/active/defi_consolidated_closeout_2026_07_18.md``
§ "Operator decisions applied (2026-07-21..."): 199,397 DeFi manifest rows carry a bare
``SUSHISWAP`` or ``UNISWAP`` venue with no version (``_V2``/``_V3``/``_V4``) recorded. Fix:
derive the version from the pool's DEPLOYING FACTORY CONTRACT ADDRESS, not "undecidable" —
Uniswap V2/V3/V4 and SushiSwap V2/V3 factory addresses are permanent, well-known, public
constants on Ethereum mainnet and its L2s.

**Pre-check performed before writing this module (HARD RULE — check before adding a new
field)**: grepped instruments-service + market-tick-data-service for any existing
factory/creator/deployer address column. None exists anywhere in the currently-captured
data path:

* ``InstrumentRecord`` (``unified_api_contracts.internal.reference.instrument``) has no
  factory-address field.
* The availability-manifest schema (v9, ``codex/02-data/availability-manifest-and-data-status.md``)
  has no factory-address column — DEX pools carry ``pool_address`` row-level INSIDE the raw
  MTDS parquet (per the "DEX V3 + CLMM" cluster-shard row, not the manifest itself), never a
  factory address.
* None of the 4 subgraph query cascades used by the shared
  ``UniswapV3ReferenceDataAdapter`` (native ``pools`` query, Algebra-fork fallback, SushiSwap
  ``pairs`` fallback, Messari fallback — see ``uniswap_v3.py``) requests a ``factory`` field.
  A per-adapter ``FACTORY_ADDRESS`` constant exists in MTDS's ``uniswapv2_adapter.py`` but is
  a compile-time constant never written onto the captured row.

So this module is genuinely forward-looking infrastructure: it resolves a factory address
**if one is present** on a row (present today: never; wired so the day a captured pool DOES
carry one — subgraph-query augmentation or an on-chain ``factory()`` RPC lookup keyed off the
already-captured ``pool_address`` — the resolution is automatic, not another migration).

Today's residual is precisely documented, not silently dropped: see
``unified-trading-pm/plans/active/issues/defi_sushiswap_uniswap_bare_version_factory_gap_2026_07_21.md``.

Scope (chains this workspace actually captures Uniswap/SushiSwap DeFi data on — verified
against UAC ``registry/chain_env.PROTOCOL_LAUNCH_DATES`` + ``registry/defi_venues.ALL_DEFI_VENUES``,
2026-07-21): ETHEREUM, ARBITRUM, OPTIMISM, POLYGON, BASE, AVALANCHE.

Verification method: every address below was checked against an official protocol source
(Uniswap docs / GitHub deployments, SushiSwap docs / GitHub) AND cross-checked against a
second independent source (a block explorer's public name tag, or DefiLlama's maintained
``dimension-adapters`` fee/volume-adapter registry) before being hardcoded — never guessed.
Uniswap V3's per-chain factory addresses are NOT duplicated here; they are re-exported from
the already-audited UAC SSOT (``unified_api_contracts.registry.dex_router_addresses``,
audited 2026-06-12 in ``unified-api-contracts/audits/defi_cat_a_audit_2026_05_08_report.md``).

Known residual (documented, not guessed — see the issue doc above for the full writeup):

* SushiSwap on ARBITRUM has NO registered versioned canonical venue in UAC
  ``ALL_DEFI_VENUES`` at all (only the bare ``SUSHISWAP-ARBITRUM`` is registered — neither
  ``SUSHISWAP_V2-ARBITRUM`` nor ``SUSHISWAP_V3-ARBITRUM`` exists). Even a confidently-resolved
  factory address on that chain cannot be safely emitted as a canonical venue until UAC adds
  the missing registry entry — this module's ``resolve_dex_version_from_factory`` still
  returns the correct version tag; the CALLER (``canonicalize_defi_manifest_venue_2026_06_14.py``)
  is responsible for validating the constructed ``{version}-{chain}`` string against
  ``ALL_DEFI_VENUES`` before using it, and falling back to the existing bare-canonical
  behaviour when it is not (yet) registered — this is a cross-repo (UAC) prerequisite, not an
  instruments-service defect.
"""

from __future__ import annotations

from unified_api_contracts.registry.dex_router_addresses import UNISWAP_V3_FACTORY_BY_CHAIN

# ---------------------------------------------------------------------------
# Uniswap V2 Factory — immutable, one deployment (Ethereum mainnet only; Uniswap
# Labs never deployed V2 on the L2s this workspace captures — confirmed via UAC
# PROTOCOL_LAUNCH_DATES, which carries only ("ETHEREUM", "UNISWAP_V2")).
# Source: https://docs.uniswap.org/contracts/v2/reference/smart-contracts/factory
# Cross-checked: https://etherscan.io/address/0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f
# (matches the pre-existing FACTORY_ADDRESS constant already in
# market-tick-data-service/market_tick_data_service/market_interface/adapters/defi/uniswapv2_adapter.py)
# ---------------------------------------------------------------------------
UNISWAP_V2_FACTORY_BY_CHAIN: dict[str, str] = {
    "ETHEREUM": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
}

# ---------------------------------------------------------------------------
# Uniswap V4 PoolManager — V4 has no per-pool factory; a singleton PoolManager
# contract owns every pool (Ethereum mainnet only — the sole UNISWAP_V4-* entry
# in UAC ALL_DEFI_VENUES is UNISWAP_V4-ETHEREUM).
# Source: https://docs.uniswap.org/contracts/v4/deployments
# Cross-checked: https://etherscan.io/address/0x000000000004444c5dc75cb358380d2e3de08a90
# ---------------------------------------------------------------------------
UNISWAP_V4_POOL_MANAGER_BY_CHAIN: dict[str, str] = {
    "ETHEREUM": "0x000000000004444c5dc75cB358380D2e3dE08A90",
}

# ---------------------------------------------------------------------------
# SushiSwap V2 ("classic" constant-product AMM) Factory — NOTE the address is
# NOT uniform across chains the way Uniswap V3's is (SushiSwap re-deployed via
# separate CREATE2 salts per chain in several cases; verified live 2026-07-21 —
# the naive assumption of one universal address was WRONG and would have
# silently mis-resolved Arbitrum/Base/Avalanche, since the same bytecode
# address can denote a DIFFERENT SushiSwap contract on a different chain, e.g.
# 0xc35dadb6...bc74c4 is SushiSwap V2 Classic Factory on ARBITRUM/POLYGON/BSC
# but SushiSwap V3 Factory on BASE — a real collision, not a typo).
# Source: https://docs.sushi.com/contracts/cpamm
# Cross-checked: https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/sushiswap-classic.ts
#   (DefiLlama's maintained per-chain factory-address registry, used for live
#   protocol fee/volume tracking — an independently-audited public source)
# ---------------------------------------------------------------------------
SUSHISWAP_V2_FACTORY_BY_CHAIN: dict[str, str] = {
    "ETHEREUM": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
    "ARBITRUM": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
    "POLYGON": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
    "AVALANCHE": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
    "OPTIMISM": "0xFbc12984689e5f15626Bad03Ad60160Fe98B303C",
    "BASE": "0x71524B4f93c58fcbF659783284E38825f0622859",
}

# ---------------------------------------------------------------------------
# SushiSwap V3 (concentrated-liquidity, Uniswap-V3-fork) Factory — again NOT
# uniform across chains (see the collision note above).
# Source: https://github.com/sushiswap/v3-core (official SushiSwap V3 contracts repo)
# Cross-checked per-chain via block explorer public name tags:
#   Ethereum:  https://etherscan.io/address/0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f
#              ("SushiSwap V3: Factory") — also the pre-existing citation in
#              unified-api-contracts/audits/defi_cat_a_audit_2026_05_08_report.md
#              (sourced "SushiSwap GitHub").
#   Base:      https://basescan.org/address/0xc35dadb65012ec5796536bd9864ed8773abc74c4
#              ("SushiSwap V3: Factory")
#   Arbitrum:  https://arbiscan.io/address/0x1af415a1eba07a4986a52b6f2e7de7003d82231e
#              ("SushiSwap V3: Factory")
#   Avalanche: cross-checked via DefiLlama dimension-adapters sushiswap-v3.ts only
#              (Snowtrace's public name-tag database does not carry a SushiSwap-
#              specific label for this address, only the generic "UniswapV3Factory"
#              bytecode name — expected for an unmodified V3 fork; DefiLlama's
#              adapter is the source of the SushiSwap-specific attribution).
# Both also cross-checked against:
#   https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/sushiswap-v3.ts
# ---------------------------------------------------------------------------
SUSHISWAP_V3_FACTORY_BY_CHAIN: dict[str, str] = {
    "ETHEREUM": "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4f",
    "BASE": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
    "ARBITRUM": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
    "AVALANCHE": "0x3e603C14aF37EBdaD31709C4f848Fc6aD5BEc715",
}

# Combined (chain -> {factory_address.lower(): version_tag}) lookup, built once
# at import time. ``version_tag`` matches the UAC canonical venue-prefix form
# (e.g. "UNISWAP_V3", "SUSHISWAP_V2") — the caller is responsible for gluing on
# "-{chain}" and validating the result against ALL_DEFI_VENUES before using it
# (this module never emits a full venue string, only a version tag, precisely
# so it can never mint an unregistered venue).
_FACTORY_TABLES: tuple[tuple[str, dict[str, str]], ...] = (
    ("UNISWAP_V2", UNISWAP_V2_FACTORY_BY_CHAIN),
    ("UNISWAP_V3", UNISWAP_V3_FACTORY_BY_CHAIN),
    ("UNISWAP_V4", UNISWAP_V4_POOL_MANAGER_BY_CHAIN),
    ("SUSHISWAP_V2", SUSHISWAP_V2_FACTORY_BY_CHAIN),
    ("SUSHISWAP_V3", SUSHISWAP_V3_FACTORY_BY_CHAIN),
)


def _build_index() -> dict[str, dict[str, str]]:
    """Build ``{chain: {factory_address.lower(): version_tag}}``.

    Raises at import time (not silently) if two DIFFERENT protocol versions on
    the SAME chain ever claim the SAME factory address — that would mean this
    table is internally contradictory (a real collision the module must never
    resolve silently in either direction).
    """
    index: dict[str, dict[str, str]] = {}
    for version_tag, by_chain in _FACTORY_TABLES:
        for chain, address in by_chain.items():
            chain_upper = chain.upper()
            addr_lower = address.lower()
            bucket = index.setdefault(chain_upper, {})
            existing = bucket.get(addr_lower)
            if existing is not None and existing != version_tag:
                raise ValueError(
                    f"DEX factory-registry collision on {chain_upper}: address {address} claimed by "
                    f"both {existing!r} and {version_tag!r} — verify the source addresses, do not resolve silently."
                )
            bucket[addr_lower] = version_tag
    return index


_FACTORY_INDEX: dict[str, dict[str, str]] = _build_index()


def resolve_dex_version_from_factory(factory_address: str | None, chain: str) -> str | None:
    """Resolve a pool's deploying-factory address to its protocol-version tag.

    Args:
        factory_address: the pool's deploying factory contract address (checksummed
            or lowercase — comparison is case-insensitive), or ``None``/empty if the
            captured row has no such field (today: every row — see module docstring).
        chain: canonical chain name (case-insensitive), e.g. ``"ARBITRUM"``.

    Returns:
        A version tag (``"UNISWAP_V2"``, ``"UNISWAP_V3"``, ``"UNISWAP_V4"``,
        ``"SUSHISWAP_V2"``, ``"SUSHISWAP_V3"``) if the (chain, factory_address) pair
        matches a known entry, else ``None`` — NEVER a guess. A pool whose factory
        matches none of the known contracts (wrong chain, unregistered fork, or no
        factory captured at all) is the genuine surface-don't-guess residual; the
        caller must leave it unresolved, not default it.
    """
    if not factory_address:
        return None
    chain_bucket = _FACTORY_INDEX.get(chain.upper())
    if not chain_bucket:
        return None
    return chain_bucket.get(factory_address.lower())


__all__ = [
    "SUSHISWAP_V2_FACTORY_BY_CHAIN",
    "SUSHISWAP_V3_FACTORY_BY_CHAIN",
    "UNISWAP_V2_FACTORY_BY_CHAIN",
    "UNISWAP_V3_FACTORY_BY_CHAIN",
    "UNISWAP_V4_POOL_MANAGER_BY_CHAIN",
    "resolve_dex_version_from_factory",
]
