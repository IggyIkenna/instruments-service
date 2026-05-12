"""Unit tests — Solblaze reference-data adapter (bSOL LST discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.solblaze import (
    SolblazeReferenceDataAdapter,
)

_BSOL_MINT = "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"
_EXPECTED_DEPLOY_DATE = datetime(2022, 2, 17, tzinfo=UTC)


def test_venue() -> None:
    assert SolblazeReferenceDataAdapter().venue == "solblaze"


@pytest.mark.asyncio
async def test_get_instruments_yields_bsol_record() -> None:
    adapter = SolblazeReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "SOLBLAZE-SOLANA"
    assert rec.instrument_key == "SOLBLAZE-SOLANA:LST:BSOL"
    assert rec.raw_symbol == _BSOL_MINT
    assert rec.instrument_type == InstrumentType.YIELD_BEARING
    assert rec.base_asset == "SOL"
    assert rec.underlying == "SOL"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _BSOL_MINT
    assert rec.base_asset_decimals == 9


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SolblazeReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = SolblazeReferenceDataAdapter()
    by_mint = await adapter.get_instrument(_BSOL_MINT)
    by_symbol = await adapter.get_instrument("BSOL")
    assert by_mint is not None and by_symbol is not None
    assert by_mint.instrument_key == by_symbol.instrument_key == "SOLBLAZE-SOLANA:LST:BSOL"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SolblazeReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("BSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("BSOL")
