"""Unit tests — Renzo reference-data adapter (ezETH LRT discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.renzo import (
    RenzoReferenceDataAdapter,
)

_EZETH_ADDRESS = "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110"
_EXPECTED_DEPLOY_DATE = datetime(2024, 4, 29, tzinfo=UTC)


def test_venue() -> None:
    assert RenzoReferenceDataAdapter().venue == "renzo"


@pytest.mark.asyncio
async def test_get_instruments_yields_ezeth_record() -> None:
    adapter = RenzoReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "RENZO-ETHEREUM"
    assert rec.instrument_key == "RENZO-ETHEREUM:LST:EZETH"
    assert rec.raw_symbol == _EZETH_ADDRESS
    assert rec.instrument_type == InstrumentType.YIELD_BEARING
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _EZETH_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = RenzoReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = RenzoReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_EZETH_ADDRESS)
    by_symbol = await adapter.get_instrument("EZETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "RENZO-ETHEREUM:LST:EZETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = RenzoReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("EZETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("EZETH")
