"""Unit tests — Idle Finance reference-data adapter (yield vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated-vault
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.idle import (
    IdleReferenceDataAdapter,
)

_IDLEDAI_ADDRESS = "0x3fE7940616e5Bc47b0775a0dccf6237893353bB4"
_EXPECTED_DEPLOY_DATE = datetime(2019, 8, 13, tzinfo=UTC)


def test_venue() -> None:
    assert IdleReferenceDataAdapter().venue == "idle"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = IdleReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "IDLE-ETHEREUM"
        assert rec.instrument_key.startswith("IDLE-ETHEREUM:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = IdleReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = IdleReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_IDLEDAI_ADDRESS)
    by_symbol = await adapter.get_instrument("IDLEDAI-BEST")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "IDLE-ETHEREUM:VAULT:IDLEDAI-BEST"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = IdleReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("DAI")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("DAI")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("IDLEDAI-BEST")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("IDLEDAI-BEST")
