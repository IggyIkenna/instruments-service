"""Unit tests — Cambrian Network restaking reference-data adapter (Solana AVS restaking).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated cSOL
vault catalogue with no network access. Tests are credential-free and offline.

Note: Cambrian vault addresses are best-guess placeholders as of 2026-05-13.
Tests validate structural invariants (venue namespace, instrument_type, deploy date)
rather than specific addresses — valid when official addresses are updated.

Plan E: Solana restaking rewards coverage — solana_restaking_rewards_coverage_2026_05_13.md
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.cambrian import (
    CambrianReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.jito_restaking import (
    JitoRestakingReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.solayer import (
    SolayerReferenceDataAdapter,
)

_EXPECTED_DEPLOY_DATE = datetime(2024, 6, 1, tzinfo=UTC)


def test_venue() -> None:
    assert CambrianReferenceDataAdapter().venue == "cambrian"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = CambrianReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "CAMBRIAN-SOLANA"
        assert rec.instrument_key.startswith("CAMBRIAN-SOLANA:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 9
        assert rec.base_asset_contract_address == rec.raw_symbol


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = CambrianReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="staking") == []


@pytest.mark.asyncio
async def test_get_instruments_none_filter_returns_all() -> None:
    adapter = CambrianReferenceDataAdapter()
    all_records = await adapter.get_instruments(instrument_type=None)
    assert len(all_records) >= 1


@pytest.mark.asyncio
async def test_csol_underlying_is_sol() -> None:
    """Primary cSOL vault should have SOL as the underlying asset."""
    adapter = CambrianReferenceDataAdapter()
    records = await adapter.get_instruments()
    primary = next((r for r in records if "CAMB-CSOL" in r.instrument_key and "JITOSOL" not in r.instrument_key), None)
    assert primary is not None, "expected primary cSOL vault record"
    assert primary.base_asset == "SOL"
    assert primary.underlying == "SOL"


@pytest.mark.asyncio
async def test_jitosol_vault_present() -> None:
    """JitoSOL-collateral vault should be present (restaking layering pattern)."""
    adapter = CambrianReferenceDataAdapter()
    records = await adapter.get_instruments()
    jito_vault = next((r for r in records if "JITOSOL" in r.instrument_key), None)
    assert jito_vault is not None, "expected JitoSOL-collateral vault record"
    assert jito_vault.base_asset == "JITOSOL"
    assert jito_vault.underlying == "JITOSOL"


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_symbol() -> None:
    adapter = CambrianReferenceDataAdapter()
    by_symbol = await adapter.get_instrument("CAMB-CSOL")
    assert by_symbol is not None
    assert by_symbol.instrument_key == "CAMBRIAN-SOLANA:VAULT:CAMB-CSOL"


@pytest.mark.asyncio
async def test_get_instrument_returns_none_for_unknown() -> None:
    adapter = CambrianReferenceDataAdapter()
    assert await adapter.get_instrument("NONEXISTENT") is None


@pytest.mark.asyncio
async def test_distinct_from_all_restaking_venues() -> None:
    """Cambrian, Solayer, and Jito Restaking are separate protocols — no venue collision."""
    cambrian = CambrianReferenceDataAdapter()
    solayer = SolayerReferenceDataAdapter()
    jito_restaking = JitoRestakingReferenceDataAdapter()

    assert cambrian.venue == "cambrian"
    assert solayer.venue == "solayer"
    assert jito_restaking.venue == "jito_restaking"
    # All three must be distinct
    venues = {cambrian.venue, solayer.venue, jito_restaking.venue}
    assert len(venues) == 3

    cambrian_records = await cambrian.get_instruments()
    for rec in cambrian_records:
        assert rec.venue == "CAMBRIAN-SOLANA"
        assert rec.venue != "SOLAYER-SOLANA"
        assert rec.venue != "JITORESTAKING-SOLANA"


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = CambrianReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("CAMB-CSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("CAMB-CSOL")
