"""P0 regression guard — canonical_id_p0_defi_adapter_type_filter_bug_2026_07_08.md.

23 DeFi adapters guarded ``get_instruments(instrument_type=...)`` against a lowercase
snake_case literal (``"lending_market"`` / ``"yield_bearing"``) that never matched the
real uppercase ``InstrumentType`` StrEnum values (``InstrumentType.LENDING`` ==
``"LENDING"``, ``InstrumentType.YIELD_BEARING`` == ``"YIELD_BEARING"`` — see
``unified-api-contracts/unified_api_contracts/_instrument_enums.py``). Real callers
(e.g. ``base_adapter.get_instruments_cached`` → URDI) pass the canonical uppercase
form, so any canonical-form type-filtered fetch against these adapters silently
returned ``[]`` — no error, just missing data.

This test is parametrized across every DeFi lending / yield-bearing reference-data
adapter (the 23 originally broken + the 1 additional broken adapter found during the
fix, ``beefy.py`` + the 2 already-correct reference adapters, ``aave_v3``/``spark``) and
asserts ``get_instruments(instrument_type=<real canonical InstrumentType value>)``
returns non-empty when instruments genuinely exist for that adapter. This locks in the
fix and guards adapter #27 from repeating the same casing mismatch.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentType

from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.beefy import BeefyReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.benqi import BenqiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.convex import ConvexReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.euler_v2 import EulerV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.idle import IdleReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.jito_restaking import (
    JitoRestakingReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.karak import KarakReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.kelpdao import KelpDaoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.pendle import PendleReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.puffer import PufferReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.radiant import RadiantReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.renzo import RenzoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.rocket_pool import RocketPoolReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.sanctum import SanctumReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.solblaze import SolblazeReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.spark import SparkReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.symbiotic import SymbioticReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.venus import VenusReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.yearn import YearnReferenceDataAdapter
from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter


def _mock_aiohttp_session_post(json_data: object) -> MagicMock:
    """Create a mock aiohttp session whose POST returns json_data (offline, credential-free)."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_data)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)
    mock_session.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


@dataclass(frozen=True)
class _Case:
    """One adapter's canonical-type-filter regression case."""

    label: str
    factory: Callable[[], BaseReferenceDataAdapter]
    instrument_type: str
    # Zero-arg builders of ``unittest.mock._patch`` objects — built lazily so a fresh
    # mock (e.g. a fresh mock aiohttp session) is created per test invocation.
    patch_builders: tuple[Callable[[], object], ...] = field(default_factory=tuple)


# ── 5 curated EVM lending adapters — need only the creation-timestamp resolver mocked ──

_RESOLVER_ONLY_LENDING: tuple[tuple[str, Callable[[], BaseReferenceDataAdapter]], ...] = (
    ("euler_v2", EulerV2ReferenceDataAdapter),
    ("benqi", BenqiReferenceDataAdapter),
    ("venus", VenusReferenceDataAdapter),
    ("radiant", RadiantReferenceDataAdapter),
    ("fluid", FluidReferenceDataAdapter),
)


def _resolver_patch(module: str) -> Callable[[], object]:
    return lambda: patch(
        f"instruments_service.reference_data.adapters.defi.{module}.batch_resolve_evm_creation_timestamps",
        new=AsyncMock(return_value={}),
    )


_LENDING_CASES = [
    _Case(
        label=name,
        factory=cls,
        instrument_type=InstrumentType.LENDING,
        patch_builders=(_resolver_patch(name),),
    )
    for name, cls in _RESOLVER_ONLY_LENDING
]

# ── 17 static curated adapters (16 yield-bearing/LST + beefy) — no network, no mocks ──

_STATIC_YIELD: tuple[tuple[str, Callable[[], BaseReferenceDataAdapter]], ...] = (
    ("lido", LidoReferenceDataAdapter),
    ("etherfi", EtherFiReferenceDataAdapter),
    ("jito_restaking", JitoRestakingReferenceDataAdapter),
    ("idle", IdleReferenceDataAdapter),
    ("kelpdao", KelpDaoReferenceDataAdapter),
    ("karak", KarakReferenceDataAdapter),
    ("rocket_pool", RocketPoolReferenceDataAdapter),
    ("solblaze", SolblazeReferenceDataAdapter),
    ("symbiotic", SymbioticReferenceDataAdapter),
    ("sanctum", SanctumReferenceDataAdapter),
    ("convex", ConvexReferenceDataAdapter),
    ("ethena", EthenaReferenceDataAdapter),
    ("renzo", RenzoReferenceDataAdapter),
    ("pendle", PendleReferenceDataAdapter),
    ("puffer", PufferReferenceDataAdapter),
    ("yearn", YearnReferenceDataAdapter),
    ("beefy", BeefyReferenceDataAdapter),
)

_YIELD_CASES = [
    _Case(label=name, factory=cls, instrument_type=InstrumentType.YIELD_BEARING) for name, cls in _STATIC_YIELD
]

# ── 4 network (subgraph) adapters — reuse each adapter's own established test fixtures ──


def _aave_v3_case() -> _Case:
    reserves_data = {
        "data": {
            "reserves": [
                {
                    "id": "0xreserve1",
                    "underlyingAsset": "0xweth",
                    "symbol": "WETH",
                    "name": "Wrapped Ether",
                    "decimals": 18,
                    "baseLTVasCollateral": "8000",
                    "reserveLiquidationThreshold": "8250",
                    "reserveLiquidationBonus": "10500",
                    "reserveFactor": "1000",
                    "usageAsCollateralEnabled": True,
                    "borrowingEnabled": True,
                    "isActive": True,
                    "isFrozen": False,
                    "isPaused": False,
                    "aToken": {"id": "0xatoken1"},
                }
            ]
        }
    }
    return _Case(
        label="aave_v3",
        factory=lambda: AaveV3ReferenceDataAdapter(api_key="test-key"),
        instrument_type=InstrumentType.LENDING,
        patch_builders=(
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            lambda: patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(reserves_data)),
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.batch_resolve_evm_creation_timestamps",
                return_value={},
            ),
        ),
    )


def _spark_case() -> _Case:
    markets_data = {
        "data": {
            "markets": [
                {
                    "id": "0x12b54025c112aa61face2cdb7118740875a566e9",
                    "name": "Spark wstETH",
                    "isActive": True,
                    "canBorrowFrom": True,
                    "canUseAsCollateral": True,
                    "maximumLTV": "83",
                    "liquidationThreshold": "84",
                    "liquidationPenalty": "7",
                    "reserveFactor": "0.3",
                    "createdTimestamp": "1678192187",
                    "inputToken": {
                        "id": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
                        "symbol": "wstETH",
                        "name": "Wrapped liquid staked Ether 2.0",
                        "decimals": 18,
                    },
                    "outputToken": {
                        "id": "0x12b54025c112aa61face2cdb7118740875a566e9",
                        "symbol": "spwstETH",
                        "name": "Spark wstETH",
                        "decimals": 18,
                    },
                    "_vToken": {
                        "id": "0xd5c3e3b566a42a6110513ac7670c1a86d76e13e6",
                        "symbol": "variableDebtwstETH",
                    },
                }
            ]
        }
    }
    return _Case(
        label="spark",
        factory=lambda: SparkReferenceDataAdapter(api_key="test-key", chain="ETHEREUM"),
        instrument_type=InstrumentType.LENDING,
        patch_builders=(
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.spark.get_subgraph_id",
                return_value="test-subgraph",
            ),
            lambda: patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(markets_data)),
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.spark.batch_resolve_evm_creation_timestamps",
                new=AsyncMock(return_value={}),
            ),
        ),
    )


def _compound_v3_case() -> _Case:
    markets_data = {
        "data": {
            "markets": [
                {
                    "id": "0xmarket1",
                    "cometProxy": "0xcomet1",
                    "creationBlockNumber": 17000000,
                    "configuration": {
                        "name": "Compound USDC",
                        "symbol": "cUSDCv3",
                        "baseToken": {
                            "token": {"id": "0xusdc", "symbol": "USDC", "name": "USD Coin", "decimals": 6},
                            "lastPriceUsd": "1.0",
                        },
                    },
                    "accounting": {
                        "totalBaseSupplyUsd": "1000000",
                        "totalBaseBorrowUsd": "500000",
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                    },
                }
            ]
        }
    }
    return _Case(
        label="compound_v3",
        factory=lambda: CompoundV3ReferenceDataAdapter(api_key="test-key"),
        instrument_type=InstrumentType.LENDING,
        patch_builders=(
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.compound_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            lambda: patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(markets_data)),
            lambda: patch(
                "instruments_service.reference_data.adapters.defi.compound_v3.block_to_timestamp",
                return_value=datetime(2023, 6, 1, tzinfo=UTC),
            ),
        ),
    )


def _morpho_case() -> _Case:
    market_data = {
        "data": {
            "markets": {
                "items": [
                    {
                        "marketId": "0xmarketkey123456789",
                        "loanAsset": {"address": "0xusdc", "symbol": "USDC", "name": "USDC", "decimals": 6},
                        "collateralAsset": {
                            "address": "0xweth",
                            "symbol": "WETH",
                            "name": "WETH",
                            "decimals": 18,
                        },
                        "lltv": "860000000000000000",
                        "state": {
                            "supplyAssets": "1000000",
                            "borrowAssets": "500000",
                            "supplyApy": "0.03",
                            "borrowApy": "0.05",
                        },
                    }
                ]
            }
        }
    }
    return _Case(
        label="morpho",
        factory=MorphoReferenceDataAdapter,
        instrument_type=InstrumentType.LENDING,
        patch_builders=(lambda: patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(market_data)),),
    )


# aave_v3/spark/compound_v3/morpho excluded from ALL_CASES/the single-type loop below:
# post-canonicalization (2026-07-13, defi_lending_atoken_debttoken_instrument_split_
# 2026_07_07.md) they emit TWO types (A_TOKEN + DEBT_TOKEN) per reserve/market, not a
# single LENDING type — see test_get_instruments_dual_type_atoken_debttoken_split below
# for their equivalent canonical-value-accepted regression coverage.
_NETWORK_CASES = [_aave_v3_case(), _spark_case(), _compound_v3_case(), _morpho_case()]

ALL_CASES = [*_LENDING_CASES, *_YIELD_CASES]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ALL_CASES, ids=[c.label for c in ALL_CASES])
async def test_get_instruments_accepts_canonical_instrument_type(case: _Case) -> None:
    """Every DeFi lending/yield-bearing adapter must accept its real canonical
    ``InstrumentType`` value and return the same non-empty universe it returns
    unfiltered — this is the exact shape of the P0 casing-mismatch bug.
    """
    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        unfiltered = await adapter.get_instruments()

    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        filtered = await adapter.get_instruments(instrument_type=case.instrument_type)

    assert unfiltered, f"{case.label}: unfiltered get_instruments() returned no instruments — fixture is broken"
    assert filtered, (
        f"{case.label}: get_instruments(instrument_type={case.instrument_type!r}) returned [] "
        "even though unfiltered instruments exist — the type-filter guard is rejecting the "
        "real canonical InstrumentType value (the exact P0 casing-mismatch bug class)."
    )
    assert len(filtered) == len(unfiltered), (
        f"{case.label}: filtering on the adapter's own canonical instrument_type dropped records "
        "it shouldn't have — every record from a single-type adapter must match its own type."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ALL_CASES, ids=[c.label for c in ALL_CASES])
async def test_get_instruments_rejects_non_matching_instrument_type(case: _Case) -> None:
    """Sanity check on the other side of the guard: an unrelated type still filters to empty."""
    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        result = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)
    assert result == [], f"{case.label}: unrelated instrument_type=PERPETUAL should filter to []"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _NETWORK_CASES, ids=[c.label for c in _NETWORK_CASES])
async def test_get_instruments_dual_type_atoken_debttoken_split(case: _Case) -> None:
    """aave_v3/spark/compound_v3/morpho each emit A_TOKEN (supply) + DEBT_TOKEN (borrow)
    records per reserve/market — the dual-type equivalent of the canonical-value-accepted
    regression above. ``instrument_type`` is a call-level GUARD on these adapters (accept
    the whole fetch or reject it with ``[]``), not a per-record filter — a canonical
    A_TOKEN or DEBT_TOKEN value must be ACCEPTED (the exact P0 casing-mismatch bug class)
    rather than rejected, and the output must genuinely contain both types. An unrelated
    type (PERPETUAL) is covered by test_get_instruments_rejects_non_matching_instrument_
    type above for these same 4 adapters.
    """
    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        unfiltered = await adapter.get_instruments()
    assert unfiltered, f"{case.label}: unfiltered get_instruments() returned no instruments — fixture is broken"
    types_present = {r.instrument_type for r in unfiltered}
    assert types_present == {InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN}, (
        f"{case.label}: expected both A_TOKEN and DEBT_TOKEN in the unfiltered output, got {types_present}"
    )

    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        a_tokens = await adapter.get_instruments(instrument_type=InstrumentType.A_TOKEN)
    assert a_tokens, f"{case.label}: instrument_type=A_TOKEN returned [] — canonical value rejected"

    adapter = case.factory()
    with ExitStack() as stack:
        for build_patch in case.patch_builders:
            stack.enter_context(build_patch())
        debt_tokens = await adapter.get_instruments(instrument_type=InstrumentType.DEBT_TOKEN)
    assert debt_tokens, f"{case.label}: instrument_type=DEBT_TOKEN returned [] — canonical value rejected"
