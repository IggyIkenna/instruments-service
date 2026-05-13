"""Unit tests — Phoenix CLOB DEX reference-data adapter (Solana CLOB).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_market_record helper and by mocking
_fetch_markets via monkeypatch.

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 2.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.phoenix import (
    PhoenixReferenceDataAdapter,
    _classify_phoenix_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_SOL_USDC_MARKET: dict[str, object] = {
    "market_address": "4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg",
    "name": "SOL/USDC",
    "base_params": {"symbol": "SOL", "name": "Solana"},
    "quote_params": {"symbol": "USDC", "name": "USD Coin"},
    "tick_size_in_quote_atoms_per_base_unit": "1000",
    "base_lot_size": "1000000000",
}

_JUP_USDC_MARKET: dict[str, object] = {
    "market_address": "3bk8ta5xXrZLzD4HGXFetMb6HYKi2FgD2JYZ5cU76fzN",
    "name": "JUP/USDC",
    "base_params": {"symbol": "JUP"},
    "quote_params": {"symbol": "USDC"},
    "tick_size_in_quote_atoms_per_base_unit": "100",
}

_NAME_DASH_MARKET: dict[str, object] = {
    "market_address": "7wBKmX1zT5q2gBKVh6NKbz4tHmPMuBJx3yrBvdULsDk9",
    "name": "BONK-USDC",
}

_NAME_SLASH_MARKET: dict[str, object] = {
    "market_address": "A5zGKcwHHJWCDVqTu7AFVF9sMX16c1GZJyYUzZDqJJf5",
    "name": "WIF/SOL",
}

_EMPTY_MARKET: dict[str, object] = {}

_EXPECTED_DEPLOY_DATE = datetime(2023, 6, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = PhoenixReferenceDataAdapter()
    assert adapter.venue == "PHOENIX-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = PhoenixReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "PHOENIX-DEVNET"


def test_build_market_record_sol_usdc() -> None:
    adapter = PhoenixReferenceDataAdapter()
    record = adapter._build_market_record(_SOL_USDC_MARKET)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "PHOENIX-SOLANA"
    assert record.instrument_key == "PHOENIX-SOLANA:SPOT:SOL-USDC"
    assert record.raw_symbol == "4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg"
    assert record.instrument_type == InstrumentType.SPOT_PAIR
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE


def test_build_market_record_jup_usdc() -> None:
    adapter = PhoenixReferenceDataAdapter()
    record = adapter._build_market_record(_JUP_USDC_MARKET)
    assert record is not None
    assert record.base_asset == "JUP"
    assert record.quote_asset == "USDC"


def test_build_market_record_dash_name() -> None:
    adapter = PhoenixReferenceDataAdapter()
    record = adapter._build_market_record(_NAME_DASH_MARKET)
    assert record is not None
    assert record.base_asset == "BONK"
    assert record.quote_asset == "USDC"


def test_build_market_record_slash_name() -> None:
    adapter = PhoenixReferenceDataAdapter()
    record = adapter._build_market_record(_NAME_SLASH_MARKET)
    assert record is not None
    assert record.base_asset == "WIF"
    assert record.quote_asset == "SOL"


def test_build_market_record_empty_returns_none() -> None:
    adapter = PhoenixReferenceDataAdapter()
    assert adapter._build_market_record(_EMPTY_MARKET) is None


@pytest.mark.asyncio
async def test_get_instruments_only_returns_spot() -> None:
    adapter = PhoenixReferenceDataAdapter()
    with patch.object(
        adapter, "_fetch_markets", new_callable=AsyncMock, return_value=[_SOL_USDC_MARKET, _JUP_USDC_MARKET]
    ):
        # SPOT type returns results
        results = await adapter.get_instruments(instrument_type="spot")
        assert len(results) == 2

        # PERPETUAL type returns empty
        results_perp = await adapter.get_instruments(instrument_type="perpetual")
        assert len(results_perp) == 0


@pytest.mark.asyncio
async def test_get_instruments_returns_all_active() -> None:
    adapter = PhoenixReferenceDataAdapter()
    with patch.object(
        adapter,
        "_fetch_markets",
        new_callable=AsyncMock,
        return_value=[_SOL_USDC_MARKET, _JUP_USDC_MARKET, _NAME_DASH_MARKET],
    ):
        results = await adapter.get_instruments()
        assert len(results) == 3
        assert all(r.instrument_type == InstrumentType.SPOT_PAIR for r in results)


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = PhoenixReferenceDataAdapter()
    with patch.object(adapter, "_fetch_markets", new_callable=AsyncMock, return_value=[_SOL_USDC_MARKET]):
        record = await adapter.get_instrument("SOL")
        assert record is not None
        assert record.base_asset == "SOL"

        missing = await adapter.get_instrument("NONEXISTENT")
        assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_phoenix_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_phoenix_error(Exception("rate limit")) == "RATE_LIMIT"
    assert _classify_phoenix_error(Exception("ok"), status=429) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_phoenix_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_phoenix_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_phoenix_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = PhoenixReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL-USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL-USDC")
