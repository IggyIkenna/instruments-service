"""Unit tests — curated EVM lending-market reference-data adapters.

Covers the three Compound/Aave-fork lending adapters that discover instruments
from a static per-chain market registry and stamp each record with a contract
creation timestamp resolved via ``batch_resolve_evm_creation_timestamps``:

* Venus Protocol (BSC primary + Ethereum IL Core Pool)
* Fluid / Instadapp (Ethereum)
* Radiant Capital (Arbitrum primary + BSC + Ethereum)

All three share the same shape: ``get_instruments`` filters on instrument_type,
short-circuits on an unknown chain, resolves creation timestamps (mocked here so
the tests are offline + credential-free), and emits an A_TOKEN (supply) +
DEBT_TOKEN (borrow) ``InstrumentRecord`` pair per curated market — the same
split ``aave_v3.py`` uses per reserve
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md). ``get_instrument``
is a linear scan over ``get_instruments`` and the market-data methods are
unsupported (``NotImplementedError``).

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
async def test_venus_get_instruments_emits_supply_debt_records() -> None:
    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        records = await adapter.get_instruments()

    # BNB / BTCB / USDT curated markets, each an A_TOKEN + DEBT_TOKEN pair.
    assert len(records) == 6
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "VENUS-BSC"
        assert rec.instrument_type in (InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN)
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.quote_asset == "USDC"
        assert rec.tick_size == Decimal("0.000001")
        # resolver returned {} → record falls back to the protocol floor date, which is the
        # canonical UAC PROTOCOL_LAUNCH_DATES[("BSC","VENUS")] = 2020-10-08 (vBNB BSC deploy;
        # UAC is the SSOT, consulted before the local LENDING_PROTOCOL_DEPLOY_DATES fallback).
        assert rec.available_from_datetime == datetime.fromisoformat("2020-10-08T00:00:00+00:00")

    a_token_keys = {r.instrument_key for r in records if r.instrument_type == InstrumentType.A_TOKEN}
    debt_token_keys = {r.instrument_key for r in records if r.instrument_type == InstrumentType.DEBT_TOKEN}
    assert "VENUS-BSC:A_TOKEN:ABNB-USDC" in a_token_keys
    assert "VENUS-BSC:DEBT_TOKEN:DEBTBNB-USDC" in debt_token_keys


@pytest.mark.asyncio
async def test_fluid_get_instruments_emits_supply_debt_records() -> None:
    adapter = FluidReferenceDataAdapter()
    with patch(_RESOLVER_PATHS["fluid"], return_value={}):
        records = await adapter.get_instruments()

    # 6 curated markets, each an A_TOKEN + DEBT_TOKEN pair.
    assert len(records) == 12
    for rec in records:
        assert rec.venue == "FLUID-ETHEREUM"
        assert rec.instrument_type in (InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN)
        assert rec.base_asset_decimals == 18
        assert rec.pool_address == rec.raw_symbol  # vault address is the raw symbol
    assert {r.instrument_type for r in records} == {InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN}


@pytest.mark.asyncio
async def test_radiant_get_instruments_emits_supply_debt_records() -> None:
    adapter = RadiantReferenceDataAdapter(chain="ARBITRUM")
    with patch(_RESOLVER_PATHS["radiant"], return_value={}):
        records = await adapter.get_instruments()

    # WETH / WBTC / ARB on Arbitrum, each an A_TOKEN + DEBT_TOKEN pair.
    assert len(records) == 6
    keys = {r.instrument_key for r in records}
    assert "RADIANT-ARBITRUM:A_TOKEN:AWETH-USDC" in keys
    assert "RADIANT-ARBITRUM:DEBT_TOKEN:DEBTWETH-USDC" in keys
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
async def test_canonical_lending_type_is_accepted() -> None:
    """P0 regression guard: real callers filter on the canonical uppercase
    ``InstrumentType.LENDING`` value. A prior guard compared against the
    lowercase literal ``"lending_market"`` instead, which never matched and
    silently returned ``[]`` for any canonical-form request — see
    canonical_id_p0_defi_adapter_type_filter_bug_2026_07_08.md. The lowercase
    literal was never a real alias; it was the bug itself, so it is now
    correctly rejected like any other non-matching value.
    """
    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        assert await adapter.get_instruments(instrument_type=InstrumentType.LENDING)
        assert await adapter.get_instruments(instrument_type="lending_market") == []


@pytest.mark.asyncio
async def test_unknown_chain_returns_empty() -> None:
    # Chains absent from the curated registry short-circuit before any network call.
    assert await VenusReferenceDataAdapter(chain="SOLANA").get_instruments() == []
    assert await RadiantReferenceDataAdapter(chain="BASE").get_instruments() == []


@pytest.mark.asyncio
async def test_radiant_secondary_chains_have_markets() -> None:
    # Each curated market now emits an A_TOKEN + DEBT_TOKEN pair (2 records/market).
    for chain, expected_markets in (("BSC", 2), ("ETHEREUM", 1)):
        adapter = RadiantReferenceDataAdapter(chain=chain)
        with patch(_RESOLVER_PATHS["radiant"], return_value={}):
            records = await adapter.get_instruments()
        assert len(records) == expected_markets * 2
        assert all(r.venue == f"RADIANT-{chain}" for r in records)


# ── get_instrument lookup (hit by raw address, hit by symbol, miss) ──────────


@pytest.mark.asyncio
async def test_get_instrument_lookup_by_address_and_symbol_and_miss() -> None:
    # The first BSC market is BNB collateral / USDC borrow → A_TOKEN symbol
    # "ABNB-USDC", DEBT_TOKEN symbol "DEBTBNB-USDC" — both share the vault address.
    bnb_vault = "0xA07c5b74C9B40447a954e1466938b865b6BBea36"

    adapter = VenusReferenceDataAdapter(chain="BSC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        # Address lookup hits the first record sharing that address (A_TOKEN, emitted first).
        by_addr = await adapter.get_instrument(bnb_vault)
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        by_a_token_symbol = await adapter.get_instrument("ABNB-USDC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        by_debt_token_symbol = await adapter.get_instrument("DEBTBNB-USDC")
    with patch(_RESOLVER_PATHS["venus"], return_value={}):
        # A non-matching symbol must walk every record without raising and return None.
        missing = await adapter.get_instrument("0xNOT-A-REAL-VAULT")

    assert by_addr is not None
    assert by_a_token_symbol is not None
    assert by_debt_token_symbol is not None
    assert by_addr.instrument_key == by_a_token_symbol.instrument_key == "VENUS-BSC:A_TOKEN:ABNB-USDC"
    assert by_debt_token_symbol.instrument_key == "VENUS-BSC:DEBT_TOKEN:DEBTBNB-USDC"
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
