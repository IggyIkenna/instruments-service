"""Unit tests — Yearn Finance reference-data adapter (V3 vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated-vault
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.yearn import (
    YearnReferenceDataAdapter,
)

_YVWETH_ADDRESS = "0xa258C4606Ca8206D8aA700cE2143D7db854D168c"
_EXPECTED_DEPLOY_DATE = datetime(2024, 3, 20, tzinfo=UTC)


def test_venue() -> None:
    assert YearnReferenceDataAdapter().venue == "yearn"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = YearnReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "YEARN-ETHEREUM"
        assert rec.instrument_key.startswith("YEARN-ETHEREUM:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = YearnReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = YearnReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_YVWETH_ADDRESS)
    by_symbol = await adapter.get_instrument("YVWETH-1")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "YEARN-ETHEREUM:VAULT:YVWETH-1"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = YearnReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("WETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("WETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("YVWETH-1")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("YVWETH-1")
