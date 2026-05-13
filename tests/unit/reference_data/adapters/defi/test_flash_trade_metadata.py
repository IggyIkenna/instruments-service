"""Unit tests — Flash Trade reference-data adapter (Solana perp DEX).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_perp_record helper and by mocking
_fetch_perp_markets via monkeypatch.

Plan: solana_perp_dex_adapters_2026_05_13 Phase 4.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.flash_trade import (
    FlashTradeReferenceDataAdapter,
    _classify_flash_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_SOL_MARKET: dict[str, object] = {
    "name": "SOL-USDC",
    "isActive": True,
    "tickSize": "0.001",
    "minSize": "0.01",
}

_BTC_MARKET: dict[str, object] = {
    "name": "BTC-USDC",
    "enabled": True,
}

_ETH_MARKET_TOKEN: dict[str, object] = {
    "token": "ETH",
    "active": True,
}

_INACTIVE_MARKET: dict[str, object] = {
    "name": "DOGE-USDC",
    "isActive": False,
}

_EXPECTED_DEPLOY_DATE = datetime(2023, 11, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    assert adapter.venue == "FLASH-SOLANA"


def test_build_perp_record_sol() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    record = adapter._build_perp_record(_SOL_MARKET)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "FLASH-SOLANA"
    assert record.instrument_key == "FLASH-SOLANA:PERP:SOL"
    assert record.raw_symbol == "SOL-USDC"
    assert record.instrument_type == InstrumentType.PERPETUAL
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.001")
    assert record.min_size == Decimal("0.01")


def test_build_perp_record_btc_minimal() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    record = adapter._build_perp_record(_BTC_MARKET)
    assert record is not None
    assert record.base_asset == "BTC"
    assert record.tick_size == Decimal("0.0001")
    assert record.min_size == Decimal("0.001")


def test_build_perp_record_via_token_field() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    record = adapter._build_perp_record(_ETH_MARKET_TOKEN)
    assert record is not None
    assert record.base_asset == "ETH"


def test_build_perp_record_inactive_returns_none() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    assert adapter._build_perp_record(_INACTIVE_MARKET) is None


def test_build_perp_record_empty_returns_none() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    assert adapter._build_perp_record({}) is None


@pytest.mark.asyncio
async def test_get_instruments_returns_active_markets() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    markets = [_SOL_MARKET, _BTC_MARKET, _INACTIVE_MARKET]
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=markets):
        results = await adapter.get_instruments()
        assert len(results) == 2
        base_assets = {r.base_asset for r in results}
        assert base_assets == {"SOL", "BTC"}


@pytest.mark.asyncio
async def test_get_instruments_filters_by_instrument_type() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_SOL_MARKET]):
        perpetuals = await adapter.get_instruments(instrument_type="perpetual")
        assert len(perpetuals) == 1

        spots = await adapter.get_instruments(instrument_type="spot")
        assert len(spots) == 0


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_SOL_MARKET]):
        found = await adapter.get_instrument("SOL")
        assert found is not None

        not_found = await adapter.get_instrument("XYZ")
        assert not_found is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_flash_error(Exception("429 rate limit"), status=429) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_flash_error(Exception("service unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_flash_error(Exception("internal server error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_flash_error(Exception("network error")) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = FlashTradeReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL-USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL-USDC")
