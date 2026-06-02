"""Unit tests — curated EVM lending-market reference-data adapters.

Covers the three Compound/Aave-fork lending adapters that discover instruments
from a static per-chain market registry and stamp each record with a contract
creation timestamp resolved via ``batch_resolve_evm_creation_timestamps``:

* Venus Protocol (BSC primary + Ethereum IL Core Pool)
* Fluid / Instadapp (Ethereum)
* Radiant Capital (Arbitrum primary + BSC + Ethereum)

All three share the same shape: ``get_instruments`` filters on instrument_type,
short-circuits on an unknown chain, resolves creation timestamps (mocked here so
the tests are offline + credential-free), and emits one ``InstrumentRecord`` per
curated market. ``get_instrument`` is a linear scan over ``get_instruments`` and
the market-data methods are unsupported (``NotImplementedError``).

The resolver is patched to return ``{}`` so each record falls back to the
protocol floor date (a static offline lookup) — exercising the real
``get_instruments`` loop body without any network access.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.radiant import RadiantReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.venus import VenusReferenceDataAdapter

_RESOLVER_PATHS = {
    "venus": "instruments_service.reference_data.adapters.defi.venus.batch_resolve_evm_creation_timestamps",
    "fluid": "instruments_service.reference_data.adapters.defi.fluid.batch_resolve_evm_creation_timestamps",
    "radiant": "instruments_service.reference_data.adapters.defi.radiant.batch_resolve_evm_creation_timestamps",
}


# ── venue identifiers ────────────────────────────────────────────────────────


def test_venue_identifiers() -> None:
    assert VenusReferenceDataAdapter().venue == "venus"
    assert FluidReferenceDataAdapter().venue == "fluid"
    assert RadiantReferenceDataAdapter().venue == "radiant"


def test_chain_is_uppercased() -> None:
    assert VenusReferenceDataAdapter(chain="bsc")._chain == "BSC"
    assert FluidReferenceDataAdapter(chain="ethereum")._chain == "ETHEREUM"
    assert RadiantReferenceDataAdapter(chain="arbitrum")._chain == "ARBITRUM"


# ── happy-path instrument discovery ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_venus_get_instruments_emits_lending_records() -> None:
    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        records = await adapter.get_instruments()

    assert len(records) == 3  # BNB / BTCB / USDT curated markets
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "VENUS-BSC"
        assert rec.instrument_key.startswith("VENUS-BSC:LENDING_MARKET:")
        assert rec.instrument_type == InstrumentType.LENDING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.quote_asset == "USDC"
        assert rec.tick_size == Decimal("0.000001")
        # resolver returned {} → record falls back to the protocol floor date, which is the
        # canonical UAC PROTOCOL_LAUNCH_DATES[("BSC","VENUS")] = 2020-10-08 (vBNB BSC deploy;
        # UAC is the SSOT, consulted before the local LENDING_PROTOCOL_DEPLOY_DATES fallback).
        assert rec.available_from_datetime == datetime.fromisoformat("2020-10-08T00:00:00+00:00")


@pytest.mark.asyncio
async def test_fluid_get_instruments_emits_lending_records() -> None:
    adapter = FluidReferenceDataAdapter()
    with patch(_RESOLVER_PATHS["fluid"], return_value={}):
        records = await adapter.get_instruments()

    assert len(records) == 6
    for rec in records:
        assert rec.venue == "FLUID-ETHEREUM"
        assert rec.instrument_type == InstrumentType.LENDING
        assert rec.base_asset_decimals == 18
        assert rec.pool_address == rec.raw_symbol  # vault address is the raw symbol


@pytest.mark.asyncio
async def test_radiant_get_instruments_emits_lending_records() -> None:
    adapter = RadiantReferenceDataAdapter(chain="ARBITRUM")
    with patch(_RESOLVER_PATHS["radiant"], return_value={}):
        records = await adapter.get_instruments()

    assert len(records) == 3  # WETH / WBTC / ARB on Arbitrum
    keys = {r.instrument_key for r in records}
    assert "RADIANT-ARBITRUM:LENDING_MARKET:WETH-USDC" in keys
    assert all(r.available_from_datetime is not None for r in records)


@pytest.mark.asyncio
async def test_resolver_timestamp_overrides_floor_date() -> None:
    """When the resolver returns a creation timestamp it wins over the floor date."""
    adapter = RadiantReferenceDataAdapter(chain="ARBITRUM")
    weth_vault = "0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1"
    resolved = datetime.fromisoformat("2023-09-01T12:00:00+00:00")
    with patch(_RESOLVER_PATHS["radiant"], return_value={weth_vault: resolved}):
        records = await adapter.get_instruments()

    by_addr = {r.raw_symbol: r for r in records}
    assert by_addr[weth_vault].available_from_datetime == resolved


# ── instrument_type filter + unknown-chain short circuit ─────────────────────


@pytest.mark.asyncio
async def test_instrument_type_filter_rejects_non_lending() -> None:
    for adapter in (
        VenusReferenceDataAdapter(),
        FluidReferenceDataAdapter(),
        RadiantReferenceDataAdapter(),
    ):
        assert await adapter.get_instruments(instrument_type="perpetual") == []
        assert await adapter.get_instruments(instrument_type="option") == []


@pytest.mark.asyncio
async def test_lending_market_alias_is_accepted() -> None:
    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        assert await adapter.get_instruments(instrument_type="lending_market")


@pytest.mark.asyncio
async def test_unknown_chain_returns_empty() -> None:
    # Chains absent from the curated registry short-circuit before any network call.
    assert await VenusReferenceDataAdapter(chain="SOLANA").get_instruments() == []
    assert await RadiantReferenceDataAdapter(chain="BASE").get_instruments() == []


@pytest.mark.asyncio
async def test_radiant_secondary_chains_have_markets() -> None:
    for chain, expected in (("BSC", 2), ("ETHEREUM", 1)):
        adapter = RadiantReferenceDataAdapter(chain=chain)
        with patch(_RESOLVER_PATHS["radiant"], return_value={}):
            records = await adapter.get_instruments()
        assert len(records) == expected
        assert all(r.venue == f"RADIANT-{chain}" for r in records)


# ── get_instrument lookup (hit by raw address, hit by symbol, miss) ──────────


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_address_and_symbol_and_miss() -> None:
    # The first BSC market is BNB collateral / USDC borrow → symbol "BNB-USDC".
    bnb_vault = "0xA07c5b74C9B40447a954e1466938b865b6BBea36"

    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        by_addr = await adapter.get_instrument(bnb_vault)
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        by_symbol = await adapter.get_instrument("BNB-USDC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        # A non-matching symbol must walk every record without raising and return None.
        missing = await adapter.get_instrument("0xNOT-A-REAL-VAULT")

    assert by_addr is not None
    assert by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key == "VENUS-BSC:LENDING_MARKET:BNB-USDC"
    assert missing is None


@pytest.mark.asyncio
async def test_fluid_get_instrument_miss_returns_none() -> None:
    adapter = FluidReferenceDataAdapter()
    with patch(_RESOLVER_PATHS["fluid"], return_value={}):
        assert await adapter.get_instrument("nope") is None


# ── unsupported market-data surfaces raise NotImplementedError ───────────────


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    for adapter in (
        VenusReferenceDataAdapter(),
        FluidReferenceDataAdapter(),
        RadiantReferenceDataAdapter(),
    ):
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("WETH")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("WETH")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("WETH-USDC")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("WETH-USDC")
