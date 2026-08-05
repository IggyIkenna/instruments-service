"""Unit tests — KelpDAO reference-data adapter (rsETH LRT discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded one-token
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.kelpdao import (
    KelpDaoReferenceDataAdapter,
)

_RSETH_ADDRESS = "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"
_EXPECTED_DEPLOY_DATE = datetime(2023, 11, 9, tzinfo=UTC)


def test_venue() -> None:
    assert KelpDaoReferenceDataAdapter().venue == "KELPDAO-ETHEREUM"


@pytest.mark.asyncio
async def test_get_instruments_yields_rseth_record() -> None:
    adapter = KelpDaoReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, InstrumentRecord)
    assert rec.venue == "KELPDAO-ETHEREUM"
    assert rec.instrument_key == "KELPDAO-ETHEREUM:LST:RSETH"
    assert rec.raw_symbol == _RSETH_ADDRESS
    # Operator decision 2026-07-20/22 (distinct_values_noncanonical_audit_2026_07_20.md):
    # rsETH is a liquid RESTAKING token (EigenLayer AVS slashing stacked on base ETH
    # staking slashing), not a plain LST. The `:LST:` key segment is intentionally left
    # unchanged (values-only reclassification, not an id rename — see kelpdao.py docstring).
    assert rec.instrument_type == InstrumentType.RESTAKING
    assert rec.base_asset == "ETH"
    assert rec.underlying == "ETH"
    assert rec.status == InstrumentStatus.ACTIVE
    assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert rec.base_asset_contract_address == _RSETH_ADDRESS
    assert rec.base_asset_decimals == 18


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = KelpDaoReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type=InstrumentType.YIELD_BEARING)
    # RESTAKING is the token's real canonical type (2026-07-20/22 reclassification) —
    # a caller filtering on it must not be rejected.
    assert await adapter.get_instruments(instrument_type=InstrumentType.RESTAKING)
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = KelpDaoReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_RSETH_ADDRESS)
    by_symbol = await adapter.get_instrument("RSETH")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "KELPDAO-ETHEREUM:LST:RSETH"
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = KelpDaoReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("ETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("RSETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("RSETH")
