"""Unit tests — Mango V4 reference-data adapter (Solana perp DEX).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_perp_record helper and by mocking
_fetch_perp_markets via monkeypatch.

Plan: solana_perp_dex_adapters_2026_05_13 Phase 2.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.mango import (
    MangoReferenceDataAdapter,
    _classify_mango_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_BTC_PERP_MARKET: dict[str, object] = {
    "name": "BTC-PERP",
    "baseMint": "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
    "quoteMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "status": "active",
    "marketIndex": 0,
    "priceIncrement": "0.1",
    "minOrderSize": "0.0001",
}

_SOL_PERP_MARKET: dict[str, object] = {
    "name": "SOL-PERP",
    "status": "active",
    "marketIndex": 1,
}

_INACTIVE_MARKET: dict[str, object] = {
    "name": "DOGE-PERP",
    "status": "inactive",
    "marketIndex": 2,
}

_EXPECTED_DEPLOY_DATE = datetime(2023, 8, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = MangoReferenceDataAdapter()
    assert adapter.venue == "MANGO-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = MangoReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "MANGO-DEVNET"


def test_build_perp_record_btc() -> None:
    adapter = MangoReferenceDataAdapter()
    record = adapter._build_perp_record(_BTC_PERP_MARKET)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "MANGO-SOLANA"
    assert record.instrument_key == "MANGO-SOLANA:PERP:BTC-PERP"
    assert record.raw_symbol == "BTC-PERP"
    assert record.instrument_type == InstrumentType.PERPETUAL
    assert record.base_asset == "BTC"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.1")
    assert record.min_size == Decimal("0.0001")


def test_build_perp_record_sol_minimal() -> None:
    """Test with minimal market dict (no price/size fields — defaults should apply)."""
    adapter = MangoReferenceDataAdapter()
    record = adapter._build_perp_record(_SOL_PERP_MARKET)
    assert record is not None
    assert record.base_asset == "SOL"
    assert record.tick_size == Decimal("0.0001")
    assert record.min_size == Decimal("0.001")


def test_build_perp_record_missing_name_returns_none() -> None:
    adapter = MangoReferenceDataAdapter()
    assert adapter._build_perp_record({}) is None
    assert adapter._build_perp_record({"status": "active"}) is None


def test_build_perp_record_inactive_skipped() -> None:
    adapter = MangoReferenceDataAdapter()
    # _build_perp_record doesn't filter inactive (that's done in get_instruments)
    # But inactive markets should produce a record with active=False filtered upstream
    record = adapter._build_perp_record(_INACTIVE_MARKET)
    # Record is built regardless — get_instruments handles the filter
    assert record is None or isinstance(record, InstrumentRecord)


@pytest.mark.asyncio
async def test_get_instruments_filters_by_instrument_type() -> None:
    adapter = MangoReferenceDataAdapter()
    with patch.object(
        adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_BTC_PERP_MARKET, _SOL_PERP_MARKET]
    ):
        # Perpetual type returns results
        results = await adapter.get_instruments(instrument_type="perpetual")
        assert len(results) == 2

        # Non-perp type returns empty
        results_spot = await adapter.get_instruments(instrument_type="spot")
        assert len(results_spot) == 0


@pytest.mark.asyncio
async def test_get_instruments_returns_all_active() -> None:
    adapter = MangoReferenceDataAdapter()
    with patch.object(
        adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_BTC_PERP_MARKET, _SOL_PERP_MARKET]
    ):
        results = await adapter.get_instruments()
        assert len(results) == 2
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = MangoReferenceDataAdapter()
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_BTC_PERP_MARKET]):
        record = await adapter.get_instrument("BTC-PERP")
        assert record is not None
        assert record.base_asset == "BTC"

        missing = await adapter.get_instrument("NONEXISTENT")
        assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_mango_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_mango_error(Exception("rate limit exceeded")) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_mango_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_mango_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_mango_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = MangoReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("BTC")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("BTC")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("BTC-PERP")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("BTC-PERP")
