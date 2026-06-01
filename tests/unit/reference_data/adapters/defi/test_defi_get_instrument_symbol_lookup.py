"""Unit tests — DeFi adapter ``get_instrument`` symbol-lookup regression.

Guards the ``inst.symbol == symbol`` latent bug that was fixed across the
curated/discovered DeFi reference-data adapters. ``InstrumentRecord`` has **no**
``symbol`` attribute, so the pre-fix lookup ::

    if inst.raw_symbol == symbol or inst.symbol == symbol:

raised ``AttributeError`` on any symbol that did not match ``raw_symbol``
exactly — the ``or`` short-circuit only hid the bug when ``raw_symbol == symbol``
(an address-key lookup). The canonical form is ::

    if inst.raw_symbol == symbol or inst.instrument_key.endswith(f":{symbol}"):

which resolves both the on-chain-address key and the canonical symbol suffix.

Each adapter's ``get_instruments`` is patched to return one controlled real
``InstrumentRecord`` (``raw_symbol`` set to an address that differs from the
canonical symbol, so the *bug-triggering* branch — the ``instrument_key`` suffix
match — is the one under test). The test is therefore offline + credential-free
and exercises only the ``get_instrument`` lookup loop, the line that carried the
bug, across every fixed adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentType

from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.benqi import BenqiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.euler_v2 import EulerV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.spark import SparkReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

if TYPE_CHECKING:
    from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter

# Every adapter whose ``get_instrument`` carried the ``inst.symbol == symbol`` bug.
_ADAPTER_CLASSES: list[type[BaseReferenceDataAdapter]] = [
    AaveV3ReferenceDataAdapter,
    BalancerReferenceDataAdapter,
    BenqiReferenceDataAdapter,
    CompoundV3ReferenceDataAdapter,
    CurveReferenceDataAdapter,
    EthenaReferenceDataAdapter,
    EtherFiReferenceDataAdapter,
    EulerV2ReferenceDataAdapter,
    JitoReferenceDataAdapter,
    KaminoReferenceDataAdapter,
    LidoReferenceDataAdapter,
    MarinadeReferenceDataAdapter,
    MorphoReferenceDataAdapter,
    OrcaReferenceDataAdapter,
    RaydiumReferenceDataAdapter,
    SparkReferenceDataAdapter,
    UniswapV2ReferenceDataAdapter,
    UniswapV3ReferenceDataAdapter,
    UniswapV4ReferenceDataAdapter,
]

# Canonical symbol lives in the instrument_key suffix; raw_symbol is the on-chain
# address (a different string) — exactly the shape that triggered the old bug.
_SYMBOL = "WETH-USDC"
_RAW_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_INSTRUMENT_KEY = f"TESTVENUE:LENDING_MARKET:{_SYMBOL}"


def _record() -> InstrumentRecord:
    # DeFi on-chain records must carry a reachable identifier (``pool_address``)
    # and non-null ``base_asset_decimals`` — hard_schema_phase1 Phase A enforcement.
    return InstrumentRecord(
        instrument_key=_INSTRUMENT_KEY,
        venue="TESTVENUE",
        instrument_type=InstrumentType.LENDING,
        raw_symbol=_RAW_ADDRESS,
        pool_address=_RAW_ADDRESS,
        base_asset_decimals=18,
    )


def _adapter_id(cls: type) -> str:
    return cls.__name__


@pytest.mark.parametrize("adapter_cls", _ADAPTER_CLASSES, ids=_adapter_id)
@pytest.mark.asyncio
async def test_get_instrument_resolves_symbol_address_and_miss(
    adapter_cls: type[BaseReferenceDataAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_instrument`` resolves by symbol-suffix + raw address, misses → None."""
    adapter = adapter_cls()
    record = _record()

    async def _fake_get_instruments(instrument_type: str | None = None) -> list[InstrumentRecord]:
        return [record]

    # Isolate the lookup loop from each adapter's (network-bound) discovery path.
    monkeypatch.setattr(adapter, "get_instruments", _fake_get_instruments)

    # Bug-triggering branch: symbol differs from raw_symbol → old code hit
    # ``inst.symbol`` and raised AttributeError; canonical code matches the
    # instrument_key suffix and returns the record.
    by_symbol = await adapter.get_instrument(_SYMBOL)
    assert by_symbol is record

    # Address-key lookup still resolves via the raw_symbol short-circuit.
    by_address = await adapter.get_instrument(_RAW_ADDRESS)
    assert by_address is record

    # A non-matching symbol must walk every record and return None WITHOUT raising.
    miss = await adapter.get_instrument("NO-SUCH-SYMBOL")
    assert miss is None


def test_all_fixed_adapters_are_covered() -> None:
    """Guard against drift: every parametrised adapter is unique + importable."""
    assert len(_ADAPTER_CLASSES) == 19
    assert len({cls.__name__ for cls in _ADAPTER_CLASSES}) == 19
    # ``Callable`` import kept meaningful — each entry is an instantiable type.
    factories: list[Callable[[], object]] = list(_ADAPTER_CLASSES)
    assert all(callable(f) for f in factories)
