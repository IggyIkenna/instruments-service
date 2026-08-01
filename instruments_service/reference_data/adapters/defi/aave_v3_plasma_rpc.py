# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportUnknownMemberType=false, reportMissingTypeStubs=false
# (web3.py's Contract.functions.<fn>().call() interface is dynamically typed —
# same rationale as market-tick-data-service's lending_indices_rpc.py, which
# disables the identical rule set for the identical class of call.)
"""PLASMA Aave V3 instrument discovery — live on-chain RPC fallback (no subgraph).

Split out of ``aave_v3.py`` so the web3.py Contract call boundary's inherently
dynamic typing (``reportAny`` etc.) doesn't loosen basedpyright strictness for the
rest of that file's (typed) subgraph-parsing code — same rationale as MTDS's own
``lending_indices_rpc.py`` split off ``lending_indices_handler.py``.

Plasma (XPL) Aave V3 launched same-day as chain mainnet (2025-09-25) with no
subgraph deployment (too new for The Graph indexing — same RPC-only class as
``aave_v3.py``'s OPTIMISM abandoned-subgraph fallback, but Plasma's market is
live/growing rather than frozen, so a one-time hardcoded snapshot would go stale
as new reserves list; live discovery is the correct choice here). Uses the SAME
AaveProtocolDataProvider contract + ``getAllReservesTokens()`` call MTDS's own RPC
fallback (``market_tick_data_service/cli/handlers/lending_indices_rpc.py``) already
uses against this exact address (verified working: 18 real rows captured
2026-07-30, venue=AAVE_V3/chain=PLASMA), extended with the two companion
AaveProtocolDataProvider calls needed for a full InstrumentRecord (aToken address,
decimals, borrowingEnabled). Address DERIVED from bgd-labs/aave-address-book
AaveV3Plasma.sol (AaveProtocolDataProvider) — see
aave_plasma_is_denominator_drift_no_producer_2026_08_01.md.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_AAVE_V3_PLASMA_DATA_PROVIDER = "0xf2D6E38B407e31E7E7e4a16E6769728b76c7419F"  # DERIVED 2026-08-01 from plasma bgd-labs/aave-address-book AaveV3Plasma.sol (AaveProtocolDataProvider) — same address market-tick-data-service/cli/handlers/lending_indices_handler.py already uses, verified capturing real rows

# Subset of AaveProtocolDataProvider ABI needed for instrument discovery (distinct
# from lending_indices_rpc.py's ABI, which only needs getAllReservesTokens for its
# rate-series fetch — IS additionally needs the aToken address + decimals +
# borrowingEnabled to build a full InstrumentRecord, same fields the subgraph query
# returns for every other AAVE_V3-* venue).
_DISCOVERY_DATA_PROVIDER_ABI: list[dict[str, object]] = [
    {
        "inputs": [],
        "name": "getAllReservesTokens",
        "outputs": [
            {
                "components": [
                    {"internalType": "string", "name": "symbol", "type": "string"},
                    {"internalType": "address", "name": "tokenAddress", "type": "address"},
                ],
                "internalType": "struct AaveProtocolDataProvider.TokenData[]",
                "name": "",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getReserveTokensAddresses",
        "outputs": [
            {"internalType": "address", "name": "aTokenAddress", "type": "address"},
            {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
            {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getReserveConfigurationData",
        "outputs": [
            {"internalType": "uint256", "name": "decimals", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "liquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "liquidationBonus", "type": "uint256"},
            {"internalType": "uint256", "name": "reserveFactor", "type": "uint256"},
            {"internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool"},
            {"internalType": "bool", "name": "borrowingEnabled", "type": "bool"},
            {"internalType": "bool", "name": "stableBorrowRateEnabled", "type": "bool"},
            {"internalType": "bool", "name": "isActive", "type": "bool"},
            {"internalType": "bool", "name": "isFrozen", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


def discover_plasma_reserves_sync(url: str) -> list[dict[str, object]]:
    """Synchronous on-chain reserve walk against the PLASMA AaveProtocolDataProvider.

    Module-level (not a closure) so it's independently unit-testable via
    ``unittest.mock.patch("web3.Web3", ...)`` without needing to mock through
    ``asyncio.to_thread``. Callers wrap this in ``asyncio.to_thread`` since
    web3.py's HTTPProvider is sync.

    Skips (does not raise for) any single reserve whose detail calls fail or
    whose configuration reports inactive/frozen — shard-level isolation, this
    is a Plasma-wide producer, not a per-reserve one. A caller-level failure
    (no RPC URL, ``getAllReservesTokens`` itself failing) is NOT handled here —
    that propagates so the caller can classify it ``attempted_failed`` rather
    than an honest empty universe.
    """
    from web3 import Web3  # noqa: imports-inside-functions — lazy: web3 connection pools on import

    w3 = Web3(Web3.HTTPProvider(url))
    provider = w3.eth.contract(
        address=Web3.to_checksum_address(_AAVE_V3_PLASMA_DATA_PROVIDER),
        abi=_DISCOVERY_DATA_PROVIDER_ABI,
    )
    reserves_raw = provider.functions.getAllReservesTokens().call()
    reserves: list[dict[str, object]] = []
    for symbol, token_addr in reserves_raw:
        token_addr = Web3.to_checksum_address(token_addr)
        try:
            atoken_addr, _stable_debt, _variable_debt = provider.functions.getReserveTokensAddresses(token_addr).call()
            (
                decimals,
                _ltv,
                _liq_threshold,
                _liq_bonus,
                _reserve_factor,
                _collateral_enabled,
                borrowing_enabled,
                _stable_borrow_enabled,
                is_active,
                is_frozen,
            ) = provider.functions.getReserveConfigurationData(token_addr).call()
        except (ConnectionError, TimeoutError, ValueError, RuntimeError) as _err:
            logger.warning(
                "AaveV3 PLASMA RPC: reserve detail call failed for %s (%s), skipping: %s",
                symbol,
                token_addr,
                _err,
            )
            continue
        if not is_active or is_frozen:
            continue
        reserves.append(
            {
                "id": token_addr.lower(),
                "symbol": str(symbol),
                "underlyingAsset": token_addr,
                "decimals": int(decimals),
                "borrowingEnabled": bool(borrowing_enabled),
                "aToken": {"id": atoken_addr},
            }
        )
    return reserves


def resolve_plasma_alchemy_key(alchemy_key: str | None) -> str | None:
    """Resolve Alchemy API key, falling back to Secret Manager.

    Local copy of the same pattern used by ``block_resolver._resolve_alchemy_key`` /
    ``evm_creation_resolver._resolve_rpc_url`` — each RPC-touching module in this
    package resolves its own key rather than sharing a private cross-module import.
    """
    if alchemy_key:
        return alchemy_key
    try:
        from unified_trading_library import get_secret_client  # noqa: imports-inside-functions

        sc = get_secret_client()
        raw_key: str = str(sc.get_secret("alchemy-api-key") or "")
        return raw_key.strip() or None
    # Secret Manager client construction/access boundary: the ADC/credential exception
    # surface (google.auth.exceptions.*) isn't a small closed set we can enumerate
    # safely, and get_secret() already swallows the GCP-API-level errors internally
    # (returns None) — the only local failure mode left, AttributeError from `.strip()`
    # on that None, still needs the same "no key available" outcome. Same audited
    # broad-except precedent as evm_creation_resolver._resolve_rpc_url.
    except Exception:
        logger.warning("AaveV3 PLASMA RPC: cannot get alchemy-api-key from Secret Manager")
        return None
