"""Unit tests — Stader reference-data adapter (ETHx LST discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.stader import (
    StaderReferenceDataAdapter,
)

_ETHX_ADDRESS = "0xcf5EA1b38380f6aF39068375516Daf40Ed70D299"
_EXPECTED_DEPLOY_DATE = datetime(2023, 7, 10, tzinfo=UTC)


def test_venue() -> None:
    assert StaderReferenceDataAdapter().venue == "STADER-ETHEREUM"


@pytest.mark.asyncio
async def test_get_instruments_yields_ethx_record() -> None:
    adapter = StaderReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "STADER-ETHEREUM"
    assert rec.instrument_key == "STADER-ETHEREUM:LST:ETHX"
    assert rec.raw_symbol == _ETHX_ADDRESS
    assert rec.instrument_type == InstrumentType.LST
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _ETHX_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = StaderReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type=InstrumentType.YIELD_BEARING)
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = StaderReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_ETHX_ADDRESS)
    by_symbol = await adapter.get_instrument("ETHX")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "STADER-ETHEREUM:LST:ETHX"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = StaderReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("ETHX")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("ETHX")
