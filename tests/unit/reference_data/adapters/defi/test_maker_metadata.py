"""Unit tests — MakerDAO reference-data adapter (sDAI savings-vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.maker import (
    MakerReferenceDataAdapter,
)

_SDAI_ADDRESS = "0x83F20F44975D03b1b09e64809B757c47f942BEeA"
_EXPECTED_DEPLOY_DATE = datetime(2017, 12, 19, tzinfo=UTC)


def test_venue() -> None:
    assert MakerReferenceDataAdapter().venue == "maker"


@pytest.mark.asyncio
async def test_get_instruments_yields_sdai_record() -> None:
    adapter = MakerReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "MAKER-ETHEREUM"
    assert rec.instrument_key == "MAKER-ETHEREUM:YIELD_BEARING:SDAI"
    assert rec.raw_symbol == _SDAI_ADDRESS
    assert rec.instrument_type == InstrumentType.YIELD_BEARING
    assert rec.base_asset == "DAI"
    assert rec.underlying == "DAI"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _SDAI_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = MakerReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type=InstrumentType.YIELD_BEARING)
    assert await adapter.get_instruments(instrument_type=InstrumentType.LST) == []
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = MakerReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_SDAI_ADDRESS)
    by_symbol = await adapter.get_instrument("SDAI")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "MAKER-ETHEREUM:YIELD_BEARING:SDAI"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = MakerReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("DAI")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("DAI")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SDAI")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SDAI")
