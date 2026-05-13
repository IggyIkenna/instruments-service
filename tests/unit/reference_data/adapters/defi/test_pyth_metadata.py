"""Unit tests — Pyth Network oracle reference-data adapter (Solana oracle).

Tests are credential-free and offline — the feed building logic is exercised
via direct calls to _build_feed_record, and the oracle price fetch is
mocked via monkeypatch.

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 5.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.pyth import (
    PYTH_PRICE_FEEDS,
    PythOracleReferenceDataAdapter,
    _classify_pyth_error,
)

# ── Constants ─────────────────────────────────────────────────────────────────

_EXPECTED_DEPLOY_DATE = datetime(2021, 8, 1, tzinfo=UTC)

_SOL_FEED_ID = "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"
_JITOSOL_FEED_ID = "7yyaeuJ1GGtVBLT2z2xub5ZWYKaNhF28mj1RdV4VDFVk"
_MSOL_FEED_ID = "E4v1BBgoso9s64TQvmyownAVJbhbEPGyzA3qn4n46qj9"

_MOCK_HERMES_RESPONSE = {
    "binary": {"data": ["..."], "encoding": "hex"},
    "parsed": [
        {
            "id": _SOL_FEED_ID,
            "price": {
                "price": "157230000000",
                "conf": "57000000",
                "expo": -8,
                "publish_time": 1715000000,
            },
            "ema_price": {
                "price": "157000000000",
                "conf": "60000000",
                "expo": -8,
                "publish_time": 1715000000,
            },
        }
    ],
}


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = PythOracleReferenceDataAdapter()
    assert adapter.venue == "PYTH-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = PythOracleReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "PYTH-DEVNET"


def test_price_feeds_registry_not_empty() -> None:
    """SSOT registry must have all major Solana oracle pairs."""
    assert len(PYTH_PRICE_FEEDS) >= 8
    assert "SOL/USD" in PYTH_PRICE_FEEDS
    assert "JITOSOL/USD" in PYTH_PRICE_FEEDS
    assert "MSOL/USD" in PYTH_PRICE_FEEDS
    assert "BSOL/USD" in PYTH_PRICE_FEEDS


def test_feed_ids_are_solana_addresses() -> None:
    """Feed IDs must look like Solana base-58 addresses (32-44 chars, no 0/O/l/I)."""
    for symbol, (feed_id, _) in PYTH_PRICE_FEEDS.items():
        assert 32 <= len(feed_id) <= 44, f"{symbol} feed_id length {len(feed_id)} out of range"
        # No ambiguous base-58 characters
        for char in ("0", "O", "l", "I"):
            assert char not in feed_id, f"{symbol} feed_id contains ambiguous char {char!r}"


def test_build_feed_record_sol_usd() -> None:
    adapter = PythOracleReferenceDataAdapter()
    feed_id, description = PYTH_PRICE_FEEDS["SOL/USD"]
    record = adapter._build_feed_record("SOL/USD", feed_id, description)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "PYTH-SOLANA"
    assert record.instrument_key == "PYTH-SOLANA:SPOT:SOL-USD"
    # raw_symbol is the Pyth feed ID
    assert record.raw_symbol == _SOL_FEED_ID
    assert record.instrument_type == InstrumentType.SPOT
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USD"
    assert record.settle_asset == "USD"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.000001")
    assert record.min_size == Decimal("0")


def test_build_feed_record_jitosol_usd() -> None:
    adapter = PythOracleReferenceDataAdapter()
    feed_id, description = PYTH_PRICE_FEEDS["JITOSOL/USD"]
    record = adapter._build_feed_record("JITOSOL/USD", feed_id, description)
    assert record is not None
    assert record.base_asset == "JITOSOL"
    assert record.quote_asset == "USD"
    assert record.raw_symbol == _JITOSOL_FEED_ID


def test_build_feed_record_msol_usd() -> None:
    adapter = PythOracleReferenceDataAdapter()
    feed_id, description = PYTH_PRICE_FEEDS["MSOL/USD"]
    record = adapter._build_feed_record("MSOL/USD", feed_id, description)
    assert record is not None
    assert record.base_asset == "MSOL"
    assert record.raw_symbol == _MSOL_FEED_ID


def test_build_feed_record_invalid_symbol_returns_none() -> None:
    adapter = PythOracleReferenceDataAdapter()
    assert adapter._build_feed_record("", "somefeedid", "desc") is None
    assert adapter._build_feed_record("INVALID_NO_SLASH", "somefeedid", "desc") is None
    assert adapter._build_feed_record("SOL/USD", "", "desc") is None


@pytest.mark.asyncio
async def test_get_instruments_returns_all_feeds() -> None:
    adapter = PythOracleReferenceDataAdapter()
    results = await adapter.get_instruments()
    assert len(results) == len(PYTH_PRICE_FEEDS)
    assert all(r.instrument_type == InstrumentType.SPOT for r in results)


@pytest.mark.asyncio
async def test_get_instruments_only_returns_spot() -> None:
    adapter = PythOracleReferenceDataAdapter()
    # SPOT type returns results
    results = await adapter.get_instruments(instrument_type="spot")
    assert len(results) == len(PYTH_PRICE_FEEDS)

    # PERPETUAL type returns empty
    results_perp = await adapter.get_instruments(instrument_type="perpetual")
    assert len(results_perp) == 0


@pytest.mark.asyncio
async def test_fetch_latest_prices_mock() -> None:
    """Verify Hermes batch price fetch parses parsed array."""
    adapter = PythOracleReferenceDataAdapter()
    with patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value=_MOCK_HERMES_RESPONSE):
        prices = await adapter.fetch_latest_prices(feed_ids=[_SOL_FEED_ID])
        assert len(prices) == 1
        assert prices[0]["id"] == _SOL_FEED_ID


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = PythOracleReferenceDataAdapter()
    record = await adapter.get_instrument("SOL")
    assert record is not None
    assert record.base_asset == "SOL"

    missing = await adapter.get_instrument("NONEXISTENT")
    assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_pyth_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_pyth_error(Exception("rate limit exceeded")) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_pyth_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_pyth_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_pyth_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = PythOracleReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL/USD")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL/USD")
