"""Unit tests — Renzo reference-data adapter (ezETH LRT discovery, multi-chain).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access (per chain). Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.renzo import (
    RenzoReferenceDataAdapter,
)

_EZETH_ETH_ADDRESS = "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110"
_EZETH_ARB_ADDRESS = "0x2416092f143378750bb29b79eD961ab195CcEea5"
_EXPECTED_ETH_DEPLOY_DATE = datetime(2024, 4, 29, tzinfo=UTC)
_EXPECTED_ARB_DEPLOY_DATE = datetime(2024, 2, 29, tzinfo=UTC)


def test_venue() -> None:
    assert RenzoReferenceDataAdapter().venue == "renzo"
    assert RenzoReferenceDataAdapter(chain="ARBITRUM").venue == "renzo"


@pytest.mark.asyncio
async def test_get_instruments_yields_ezeth_record() -> None:
    adapter = RenzoReferenceDataAdapter()
    records = await adapter.get_instruments()
    # 1 LST + 1 SPOT_ASSET sibling (ezETH itself — P4-B) = 2.
    assert len(records) == 2
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "RENZO-ETHEREUM"
    assert rec.instrument_key == "RENZO-ETHEREUM:LST:EZETH"
    assert rec.raw_symbol == _EZETH_ETH_ADDRESS
    # 2026-07-08: field fixed to match the `:LST:` key segment (key/field consistency
    # fix, same class as PERP-vs-PERPETUAL — see lido.py's module docstring).
    assert rec.instrument_type == InstrumentType.LST
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_ETH_DEPLOY_DATE
    assert rec.base_asset_contract_address == _EZETH_ETH_ADDRESS
    assert rec.base_asset_decimals == 18

    spot_asset = next(r for r in records if r.instrument_type == InstrumentType.SPOT_ASSET)
    assert spot_asset.instrument_key == "RENZO-ETHEREUM:SPOT_ASSET:EZETH"
    # Names the actual on-chain receipt token (EZETH), not the "ETH" economic-peg
    # label the primary LST record carries.
    assert spot_asset.base_asset == "EZETH"
    assert spot_asset.base_asset_contract_address == _EZETH_ETH_ADDRESS
    assert spot_asset.base_asset_decimals == 18
    assert spot_asset.available_from_datetime == _EXPECTED_ETH_DEPLOY_DATE


@pytest.mark.asyncio
async def test_get_instruments_arbitrum_yields_bridged_record() -> None:
    adapter = RenzoReferenceDataAdapter(chain="ARBITRUM")
    records = await adapter.get_instruments()
    # 1 LST + 1 SPOT_ASSET sibling — P4-B.
    assert len(records) == 2
    rec = records[0]
    assert rec.venue == "RENZO-ARBITRUM"
    assert rec.instrument_key == "RENZO-ARBITRUM:LST:EZETH"
    assert rec.raw_symbol == _EZETH_ARB_ADDRESS
    assert rec.base_asset == "ETH"
    assert rec.available_from_datetime == _EXPECTED_ARB_DEPLOY_DATE
    assert rec.base_asset_contract_address == _EZETH_ARB_ADDRESS

    spot_asset = next(r for r in records if r.instrument_type == InstrumentType.SPOT_ASSET)
    assert spot_asset.instrument_key == "RENZO-ARBITRUM:SPOT_ASSET:EZETH"
    assert spot_asset.base_asset == "EZETH"
    assert spot_asset.base_asset_contract_address == _EZETH_ARB_ADDRESS


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = RenzoReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type=InstrumentType.YIELD_BEARING)
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = RenzoReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_EZETH_ETH_ADDRESS)
    by_symbol = await adapter.get_instrument("EZETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "RENZO-ETHEREUM:LST:EZETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unknown_chain_returns_empty_list() -> None:
    """Unknown chains (not in _LRT_TOKENS_BY_CHAIN) yield no instruments — fail-soft, not raise."""
    adapter = RenzoReferenceDataAdapter(chain="POLYGON")
    assert await adapter.get_instruments() == []


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = RenzoReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("EZETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("EZETH")
