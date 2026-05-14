"""Unit tests — Picasso Network restaking reference-data adapter (cross-chain restaking).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated pSOL
vault catalogue with no network access. Tests are credential-free and offline.

Note: Picasso program ID / vault addresses are best-guess placeholders as of 2026-05-13.
Tests validate structural invariants (venue namespace, instrument_type, deploy date)
rather than specific addresses, so they remain valid when official addresses are updated.

Plan E: Solana restaking rewards coverage — solana_restaking_rewards_coverage_2026_05_13.md
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.picasso import (
    PicassoReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.solayer import (
    SolayerReferenceDataAdapter,
)

_EXPECTED_DEPLOY_DATE = datetime(2023, 5, 1, tzinfo=UTC)


def test_venue() -> None:
    assert PicassoReferenceDataAdapter().venue == "picasso"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = PicassoReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "PICASSO-SOLANA"
        assert rec.instrument_key.startswith("PICASSO-SOLANA:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 9
        assert rec.base_asset_contract_address == rec.raw_symbol


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = PicassoReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="staking") == []


@pytest.mark.asyncio
async def test_get_instruments_none_filter_returns_all() -> None:
    adapter = PicassoReferenceDataAdapter()
    all_records = await adapter.get_instruments(instrument_type=None)
    assert len(all_records) >= 1


@pytest.mark.asyncio
async def test_psol_underlying_is_sol() -> None:
    """Primary pSOL vault should have SOL as the underlying asset."""
    adapter = PicassoReferenceDataAdapter()
    records = await adapter.get_instruments()
    primary = next((r for r in records if "PICA-PSOL" in r.instrument_key), None)
    assert primary is not None, "expected primary pSOL vault record"
    assert primary.base_asset == "SOL"
    assert primary.underlying == "SOL"


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_symbol() -> None:
    adapter = PicassoReferenceDataAdapter()
    by_symbol = await adapter.get_instrument("PICA-PSOL")
    assert by_symbol is not None
    assert by_symbol.instrument_key == "PICASSO-SOLANA:VAULT:PICA-PSOL"


@pytest.mark.asyncio
async def test_get_instrument_returns_none_for_unknown() -> None:
    adapter = PicassoReferenceDataAdapter()
    assert await adapter.get_instrument("NONEXISTENT") is None


@pytest.mark.asyncio
async def test_distinct_from_solayer_venue() -> None:
    """Picasso and Solayer are separate protocols — venue namespaces must not collide."""
    picasso = PicassoReferenceDataAdapter()
    solayer = SolayerReferenceDataAdapter()

    assert picasso.venue == "picasso"
    assert solayer.venue == "solayer"
    assert picasso.venue != solayer.venue

    picasso_records = await picasso.get_instruments()
    for rec in picasso_records:
        assert rec.venue == "PICASSO-SOLANA"
        assert rec.venue != "SOLAYER-SOLANA"


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = PicassoReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("PICA-PSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("PICA-PSOL")
