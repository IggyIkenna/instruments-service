"""Unit tests — Jito Restaking reference-data adapter (VRT vault discovery on Solana).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated VRT
catalogue with no network access. Tests are credential-free and offline.

The adapter is intentionally distinct from the sibling Jito LST adapter
(`adapters/defi/jito.py`); these tests assert that distinction holds (separate
``venue`` tag and separate canonical venue label).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.jito import (
    JitoReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.jito_restaking import (
    JitoRestakingReferenceDataAdapter,
)

_EZSOL_MINT = "ezSoL6fY1PVdJcJsUpe5CM3xkfmy3zoVCABybm5WtiC"
_EXPECTED_DEPLOY_DATE = datetime(2024, 8, 1, tzinfo=UTC)


def test_venue() -> None:
    assert JitoRestakingReferenceDataAdapter().venue == "jito_restaking"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = JitoRestakingReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "JITORESTAKING-SOLANA"
        assert rec.instrument_key.startswith("JITORESTAKING-SOLANA:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 9
        assert rec.base_asset_contract_address == rec.raw_symbol
        # Solana mint addresses are base58 strings, NOT EVM 0x-prefixed.
        assert rec.raw_symbol is not None
        assert not rec.raw_symbol.startswith("0x"), (
            f"VRT mint {rec.raw_symbol!r} looks EVM-style; expected Solana base58"
        )


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = JitoRestakingReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = JitoRestakingReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_EZSOL_MINT)
    by_symbol = await adapter.get_instrument("JTORK-EZSOL")
    assert by_addr is not None and by_symbol is not None
    assert (
        by_addr.instrument_key
        == by_symbol.instrument_key
        == "JITORESTAKING-SOLANA:VAULT:JTORK-EZSOL"
    )
    assert await adapter.get_instrument("NOPE") is None


@pytest.mark.asyncio
async def test_distinct_from_jito_lst_venue() -> None:
    """Jito Restaking and Jito LST are separate protocols — verify the venue
    namespace does NOT collide with the existing JitoReferenceDataAdapter.
    """
    restaking_adapter = JitoRestakingReferenceDataAdapter()
    lst_adapter = JitoReferenceDataAdapter()

    # `venue` property — registry key — must be distinct.
    assert restaking_adapter.venue == "jito_restaking"
    assert lst_adapter.venue == "JITO-SOLANA"
    assert restaking_adapter.venue != lst_adapter.venue

    # The canonical venue label embedded in instrument records must also be distinct.
    restaking_records = await restaking_adapter.get_instruments()
    assert restaking_records, "expected ≥1 restaking VRT record for venue-label check"
    for rec in restaking_records:
        assert rec.venue == "JITORESTAKING-SOLANA"
        assert rec.venue != "JITO-SOLANA"


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = JitoRestakingReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("JTORK-EZSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("JTORK-EZSOL")
