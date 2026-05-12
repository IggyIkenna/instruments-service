"""Unit tests — Karak reference-data adapter (restaking vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated-vault
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.karak import (
    KarakReferenceDataAdapter,
)

_WSTETH_VAULT_ETH = "0x7BBbcA39bCDCC3B3B1a64a8a9f7c6a42C61A3f1E"
_EXPECTED_DEPLOY_DATE = datetime(2024, 4, 8, tzinfo=UTC)


def test_venue() -> None:
    assert KarakReferenceDataAdapter().venue == "karak"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = KarakReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "KARAK-ETHEREUM"
        assert rec.instrument_key.startswith("KARAK-ETHEREUM:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = KarakReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = KarakReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_WSTETH_VAULT_ETH)
    by_symbol = await adapter.get_instrument("KARAK-WSTETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "KARAK-ETHEREUM:VAULT:KARAK-WSTETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = KarakReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("KARAK-WSTETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("KARAK-WSTETH")
