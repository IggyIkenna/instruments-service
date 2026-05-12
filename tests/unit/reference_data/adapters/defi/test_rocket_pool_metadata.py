"""Unit tests — Rocket Pool reference-data adapter (rETH LST discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.rocket_pool import (
    RocketPoolReferenceDataAdapter,
)

_RETH_ADDRESS = "0xae78736Cd615f374D3085123A210448E74Fc6393"
_EXPECTED_DEPLOY_DATE = datetime(2021, 11, 8, tzinfo=UTC)


def test_venue() -> None:
    assert RocketPoolReferenceDataAdapter().venue == "rocket_pool"


@pytest.mark.asyncio
async def test_get_instruments_yields_reth_record() -> None:
    adapter = RocketPoolReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "ROCKETPOOL-ETHEREUM"
    assert rec.instrument_key == "ROCKETPOOL-ETHEREUM:LST:RETH"
    assert rec.raw_symbol == _RETH_ADDRESS
    assert rec.instrument_type == InstrumentType.YIELD_BEARING
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _RETH_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = RocketPoolReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = RocketPoolReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_RETH_ADDRESS)
    by_symbol = await adapter.get_instrument("RETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "ROCKETPOOL-ETHEREUM:LST:RETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = RocketPoolReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("RETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("RETH")
