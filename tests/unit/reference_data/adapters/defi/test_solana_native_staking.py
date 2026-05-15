"""Unit tests — Solana native staking reference-data adapter.

Pure static-registry adapter: ``get_instruments`` returns a single SOL
STAKING instrument with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.solana_native_staking import (
    _SOL_MINT,
    SolanaNativeStakingAdapter,
)

_EXPECTED_GENESIS_DATE = datetime(2020, 3, 16, tzinfo=UTC)


def test_venue() -> None:
    assert SolanaNativeStakingAdapter().venue == "solana_native"


@pytest.mark.asyncio
async def test_get_instruments_returns_one_sol() -> None:
    records = await SolanaNativeStakingAdapter().get_instruments()
    assert len(records) == 1
    assert records[0].instrument_key == "SOLANA-NATIVE-SOLANA:STAKING:SOL"


@pytest.mark.asyncio
async def test_sol_record_fields() -> None:
    adapter = SolanaNativeStakingAdapter()
    records = await adapter.get_instruments()
    sol = records[0]
    assert isinstance(sol, InstrumentRecord)
    assert sol.venue == "SOLANA-NATIVE-SOLANA"
    assert sol.instrument_key == "SOLANA-NATIVE-SOLANA:STAKING:SOL"
    assert sol.raw_symbol == _SOL_MINT
    assert sol.instrument_type == InstrumentType.STAKING
    assert sol.base_asset == "SOL"
    assert sol.underlying == "SOL"
    assert sol.status == InstrumentStatus.ACTIVE
    assert sol.available_from_datetime == _EXPECTED_GENESIS_DATE
    assert sol.base_asset_contract_address == _SOL_MINT
    assert sol.base_asset_decimals == 9


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SolanaNativeStakingAdapter()
    assert await adapter.get_instruments(instrument_type="staking")
    assert await adapter.get_instruments(instrument_type="STAKING")
    assert await adapter.get_instruments(instrument_type="yield_bearing") == []
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_by_mint_address() -> None:
    adapter = SolanaNativeStakingAdapter()
    rec = await adapter.get_instrument(_SOL_MINT)
    assert rec is not None
    assert rec.instrument_key == "SOLANA-NATIVE-SOLANA:STAKING:SOL"


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = SolanaNativeStakingAdapter()
    rec = await adapter.get_instrument("SOL")
    assert rec is not None
    assert rec.instrument_key == "SOLANA-NATIVE-SOLANA:STAKING:SOL"


@pytest.mark.asyncio
async def test_get_instrument_unknown_returns_none() -> None:
    adapter = SolanaNativeStakingAdapter()
    assert await adapter.get_instrument("NOPE") is None
    assert await adapter.get_instrument("BTC") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SolanaNativeStakingAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL")
