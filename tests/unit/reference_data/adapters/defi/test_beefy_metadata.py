"""Unit tests — Beefy Finance reference-data adapter (multi-chain vault discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated-vault
catalogue with no network access. Tests are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.beefy import (
    BeefyReferenceDataAdapter,
)

# Known canonical vault addresses for lookup-style tests.
_ETH_MORPHO_USDC_FRONTIER_ADDR = "0x35E7Bf11193C40B943df1fb33e20f947a6EB04E4"
_ETH_MORPHO_USDC_FRONTIER_SYMBOL = "BEEFY-ETH-MORPHO-USDC-FRONTIER"
_ARB_MORPHO_USDC_STEAKHOUSE_ADDR = "0x13EaA79178f2b6C0A43cA265B66d70b9d60F827a"
_ARB_MORPHO_USDC_STEAKHOUSE_SYMBOL = "BEEFY-ARB-MORPHO-USDC-STEAKHOUSE"

_ETH_DEPLOY_DATE = datetime(2021, 12, 1, tzinfo=UTC)
_ARB_DEPLOY_DATE = datetime(2021, 9, 20, tzinfo=UTC)
_BASE_DEPLOY_DATE = datetime(2023, 8, 15, tzinfo=UTC)
_BSC_DEPLOY_DATE = datetime(2020, 10, 8, tzinfo=UTC)
_AVAX_DEPLOY_DATE = datetime(2021, 3, 15, tzinfo=UTC)


def test_venue() -> None:
    assert BeefyReferenceDataAdapter().venue == "beefy"
    assert BeefyReferenceDataAdapter(chain="ARBITRUM").venue == "beefy"


@pytest.mark.asyncio
async def test_get_instruments_yields_vault_records() -> None:
    adapter = BeefyReferenceDataAdapter()
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "BEEFY-ETHEREUM"
        assert rec.instrument_key.startswith("BEEFY-ETHEREUM:VAULT:")
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _ETH_DEPLOY_DATE
        assert rec.base_asset_contract_address == rec.raw_symbol
        assert rec.base_asset_decimals in (6, 8, 18)


@pytest.mark.asyncio
async def test_get_instruments_arbitrum_yields_vault_records() -> None:
    adapter = BeefyReferenceDataAdapter(chain="ARBITRUM")
    records = await adapter.get_instruments()
    assert len(records) >= 1
    for rec in records:
        assert rec.venue == "BEEFY-ARBITRUM"
        assert rec.instrument_key.startswith("BEEFY-ARBITRUM:VAULT:")
        assert rec.available_from_datetime == _ARB_DEPLOY_DATE


@pytest.mark.asyncio
async def test_get_instruments_all_chains_have_records() -> None:
    """Sanity-check every registered chain returns ≥1 vault with the right deploy date."""
    expected = {
        "BASE": _BASE_DEPLOY_DATE,
        "BSC": _BSC_DEPLOY_DATE,
        "AVALANCHE": _AVAX_DEPLOY_DATE,
    }
    for chain, deploy_date in expected.items():
        adapter = BeefyReferenceDataAdapter(chain=chain)
        records = await adapter.get_instruments()
        assert records, f"chain={chain} returned no vaults"
        assert all(rec.venue == f"BEEFY-{chain}" for rec in records)
        assert all(rec.available_from_datetime == deploy_date for rec in records)


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = BeefyReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="spot") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = BeefyReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_ETH_MORPHO_USDC_FRONTIER_ADDR)
    by_symbol = await adapter.get_instrument(_ETH_MORPHO_USDC_FRONTIER_SYMBOL)
    assert by_addr is not None and by_symbol is not None
    assert (
        by_addr.instrument_key
        == by_symbol.instrument_key
        == f"BEEFY-ETHEREUM:VAULT:{_ETH_MORPHO_USDC_FRONTIER_SYMBOL}"
    )
    assert await adapter.get_instrument("DOES-NOT-EXIST") is None

    # Lookup respects the configured chain (Arbitrum vault not visible on Ethereum adapter).
    assert await adapter.get_instrument(_ARB_MORPHO_USDC_STEAKHOUSE_ADDR) is None
    arb_adapter = BeefyReferenceDataAdapter(chain="ARBITRUM")
    arb_hit = await arb_adapter.get_instrument(_ARB_MORPHO_USDC_STEAKHOUSE_SYMBOL)
    assert arb_hit is not None
    assert arb_hit.venue == "BEEFY-ARBITRUM"


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = BeefyReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("WETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("WETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate(_ETH_MORPHO_USDC_FRONTIER_SYMBOL)
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv(_ETH_MORPHO_USDC_FRONTIER_SYMBOL)
