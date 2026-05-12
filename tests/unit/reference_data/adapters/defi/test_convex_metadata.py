"""Unit tests — Convex Finance reference-data adapter (CVX + cvxCRV discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded two-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.convex import (
    ConvexReferenceDataAdapter,
)

_CVX_ADDRESS = "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B"
_CVXCRV_ADDRESS = "0x62B9c7356A2Dc64a1969e19C23e4f579F9810Aa7"
_EXPECTED_DEPLOY_DATE = datetime(2021, 5, 17, tzinfo=UTC)


def test_venue() -> None:
    assert ConvexReferenceDataAdapter().venue == "convex"


@pytest.mark.asyncio
async def test_get_instruments_yields_cvx_records() -> None:
    adapter = ConvexReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 2
    symbols = {rec.instrument_key.split(":")[-1] for rec in records}
    assert "CVX" in symbols
    assert "CVXCRV" in symbols
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "CONVEX-ETHEREUM"
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = ConvexReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = ConvexReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_CVX_ADDRESS)
    by_symbol = await adapter.get_instrument("CVX")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "CONVEX-ETHEREUM:VAULT:CVX"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = ConvexReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("CVX")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("CVX")
