"""Unit tests — Puffer Finance reference-data adapter (pufETH LRT discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.puffer import (
    PufferReferenceDataAdapter,
)

_PUFETH_ADDRESS = "0xD9A442856C234a39a81a089C06451EBAa4306a72"
_EXPECTED_DEPLOY_DATE = datetime(2024, 5, 9, tzinfo=UTC)


def test_venue() -> None:
    assert PufferReferenceDataAdapter().venue == "puffer"


@pytest.mark.asyncio
async def test_get_instruments_yields_pufeth_record() -> None:
    adapter = PufferReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "PUFFER-ETHEREUM"
    assert rec.instrument_key == "PUFFER-ETHEREUM:LST:PUFETH"
    assert rec.raw_symbol == _PUFETH_ADDRESS
    assert rec.instrument_type == InstrumentType.YIELD_BEARING
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _PUFETH_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = PufferReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = PufferReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_PUFETH_ADDRESS)
    by_symbol = await adapter.get_instrument("PUFETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "PUFFER-ETHEREUM:LST:PUFETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = PufferReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("PUFETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("PUFETH")
