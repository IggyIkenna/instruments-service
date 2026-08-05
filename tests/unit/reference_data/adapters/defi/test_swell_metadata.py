"""Unit tests — Swell reference-data adapter (swETH LST discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.swell import (
    SwellReferenceDataAdapter,
)

_SWETH_ADDRESS = "0xf951E335afb289353dc249e82926178EaC7DEd78"
_EXPECTED_DEPLOY_DATE = datetime(2023, 4, 25, tzinfo=UTC)


def test_venue() -> None:
    assert SwellReferenceDataAdapter().venue == "SWELL-ETHEREUM"


@pytest.mark.asyncio
async def test_get_instruments_yields_sweth_record() -> None:
    adapter = SwellReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "SWELL-ETHEREUM"
    assert rec.instrument_key == "SWELL-ETHEREUM:LST:SWETH"
    assert rec.raw_symbol == _SWETH_ADDRESS
    assert rec.instrument_type == InstrumentType.LST
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _SWETH_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SwellReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type=InstrumentType.YIELD_BEARING)
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = SwellReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_SWETH_ADDRESS)
    by_symbol = await adapter.get_instrument("SWETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "SWELL-ETHEREUM:LST:SWETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SwellReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SWETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SWETH")
