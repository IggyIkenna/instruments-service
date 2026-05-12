"""Unit tests — Symbiotic reference-data adapter (restaking vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated-vault
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.symbiotic import (
    SymbioticReferenceDataAdapter,
)

_WSTETH_VAULT = "0xC329400492c6ff2438472D4651Ad17389fCb843a"
_EXPECTED_DEPLOY_DATE = datetime(2024, 6, 11, tzinfo=UTC)


def test_venue() -> None:
    assert SymbioticReferenceDataAdapter().venue == "symbiotic"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = SymbioticReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "SYMBIOTIC-ETHEREUM"
        assert rec.instrument_key.startswith("SYMBIOTIC-ETHEREUM:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.base_asset == "ETH"
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SymbioticReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = SymbioticReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_WSTETH_VAULT)
    by_symbol = await adapter.get_instrument("SYMB-WSTETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "SYMBIOTIC-ETHEREUM:VAULT:SYMB-WSTETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SymbioticReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SYMB-WSTETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SYMB-WSTETH")
