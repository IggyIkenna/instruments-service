"""Unit tests — Sanctum reference-data adapter (INF + jupSOL + laineSOL LST discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded 3-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.sanctum import (
    SanctumReferenceDataAdapter,
)

_INF_MINT = "5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm"
_JUPSOL_MINT = "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v"
_LAINESOL_MINT = "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZMTlkPradhmPuA"
_EXPECTED_DEPLOY_DATE = datetime(2023, 6, 1, tzinfo=UTC)


def test_venue() -> None:
    assert SanctumReferenceDataAdapter().venue == "sanctum"


@pytest.mark.asyncio
async def test_get_instruments_returns_all_three_tokens() -> None:
    records = await SanctumReferenceDataAdapter().get_instruments()
    assert len(records) == 3
    keys = {r.instrument_key for r in records}
    assert "SANCTUM-SOLANA:LST:INF" in keys
    assert "SANCTUM-SOLANA:LST:JUPSOL" in keys
    assert "SANCTUM-SOLANA:LST:LAINESOL" in keys


@pytest.mark.asyncio
async def test_inf_record_fields() -> None:
    adapter = SanctumReferenceDataAdapter()
    records = await adapter.get_instruments()
    inf = next(r for r in records if r.instrument_key == "SANCTUM-SOLANA:LST:INF")
    assert isinstance(inf, InstrumentRecord)
    assert inf.venue == "SANCTUM-SOLANA"
    assert inf.raw_symbol == _INF_MINT
    assert inf.instrument_type == InstrumentType.YIELD_BEARING
    assert inf.base_asset == "SOL"
    assert inf.underlying == "SOL"
    assert inf.status == InstrumentStatus.ACTIVE
    assert inf.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert inf.base_asset_contract_address == _INF_MINT
    assert inf.base_asset_decimals == 9


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SanctumReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="spot") == []


@pytest.mark.asyncio
async def test_get_instrument_by_mint_address() -> None:
    adapter = SanctumReferenceDataAdapter()
    rec = await adapter.get_instrument(_JUPSOL_MINT)
    assert rec is not None
    assert rec.instrument_key == "SANCTUM-SOLANA:LST:JUPSOL"


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = SanctumReferenceDataAdapter()
    rec = await adapter.get_instrument("LAINESOL")
    assert rec is not None
    assert rec.instrument_key == "SANCTUM-SOLANA:LST:LAINESOL"
    assert rec.base_asset_contract_address == _LAINESOL_MINT


@pytest.mark.asyncio
async def test_get_instrument_not_found_returns_none() -> None:
    assert await SanctumReferenceDataAdapter().get_instrument("NOTEXIST") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SanctumReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("INF")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("INF")
