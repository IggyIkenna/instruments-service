"""Unit tests — Solayer restaking reference-data adapter (sSOL vault discovery on Solana).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated sSOL
vault catalogue with no network access. Tests are credential-free and offline.

The adapter is distinct from the Jito LST adapter (jito.py) and Jito Restaking adapter
(jito_restaking.py). These tests assert that distinctness holds (separate venue tag).

Plan E: Solana restaking rewards coverage — solana_restaking_rewards_coverage_2026_05_13.md
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.jito_restaking import (
    JitoRestakingReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.defi.solayer import (
    SolayerReferenceDataAdapter,
)

_SSOL_MINT = "sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh"
_EXPECTED_DEPLOY_DATE = datetime(2024, 4, 1, tzinfo=UTC)


def test_venue() -> None:
    assert SolayerReferenceDataAdapter().venue == "solayer"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = SolayerReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "SOLAYER-SOLANA"
        assert rec.instrument_key.startswith("SOLAYER-SOLANA:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.base_asset_decimals == 9
        assert rec.base_asset_contract_address == rec.raw_symbol
        # Solana addresses are base58 strings, NOT EVM 0x-prefixed.
        assert rec.raw_symbol is not None


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = SolayerReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="staking") == []


@pytest.mark.asyncio
async def test_get_instruments_none_filter_returns_all() -> None:
    adapter = SolayerReferenceDataAdapter()
    all_records = await adapter.get_instruments(instrument_type=None)
    assert len(all_records) >= 1


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_address() -> None:
    adapter = SolayerReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_SSOL_MINT)
    assert by_addr is not None
    assert by_addr.raw_symbol == _SSOL_MINT
    assert by_addr.instrument_key == "SOLAYER-SOLANA:VAULT:SLAYER-SSOL"


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_symbol() -> None:
    adapter = SolayerReferenceDataAdapter()
    by_symbol = await adapter.get_instrument("SLAYER-SSOL")
    assert by_symbol is not None
    assert by_symbol.instrument_key == "SOLAYER-SOLANA:VAULT:SLAYER-SSOL"


@pytest.mark.asyncio
async def test_get_instrument_returns_none_for_unknown() -> None:
    adapter = SolayerReferenceDataAdapter()
    assert await adapter.get_instrument("NONEXISTENT") is None


@pytest.mark.asyncio
async def test_distinct_from_jito_restaking_venue() -> None:
    """Solayer and Jito Restaking are separate protocols — verify venue namespace does NOT collide."""
    solayer_adapter = SolayerReferenceDataAdapter()
    jito_restaking_adapter = JitoRestakingReferenceDataAdapter()

    assert solayer_adapter.venue == "solayer"
    assert jito_restaking_adapter.venue == "jito_restaking"
    assert solayer_adapter.venue != jito_restaking_adapter.venue

    solayer_records = await solayer_adapter.get_instruments()
    assert solayer_records, "expected ≥1 Solayer vault record"
    for rec in solayer_records:
        assert rec.venue == "SOLAYER-SOLANA"
        assert rec.venue != "JITORESTAKING-SOLANA"


@pytest.mark.asyncio
async def test_ssol_underlying_is_sol() -> None:
    """Primary sSOL vault should have SOL as the underlying asset."""
    adapter = SolayerReferenceDataAdapter()
    records = await adapter.get_instruments()
    primary = next(
        (r for r in records if "SLAYER-SSOL" in r.instrument_key and "JITOSOL" not in r.instrument_key), None
    )
    assert primary is not None, "expected primary sSOL vault record"
    assert primary.base_asset == "SOL"
    assert primary.underlying == "SOL"


@pytest.mark.asyncio
async def test_jitosol_vault_present() -> None:
    """JitoSOL-collateral vault should be present (restaking layering pattern)."""
    adapter = SolayerReferenceDataAdapter()
    records = await adapter.get_instruments()
    jito_vault = next((r for r in records if "JITOSOL" in r.instrument_key), None)
    assert jito_vault is not None, "expected JitoSOL-collateral vault record"
    assert jito_vault.base_asset == "JITOSOL"
    assert jito_vault.underlying == "JITOSOL"


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SolayerReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SLAYER-SSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SLAYER-SSOL")
