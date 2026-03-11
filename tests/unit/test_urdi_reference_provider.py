"""Tests for urdi_reference_provider — URDI instrument wiring in instruments-service."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_internal_contracts import InstrumentRecord

from instruments_service.adapters.urdi_reference_provider import (
    URDI_SUPPORTED_VENUES,
    fetch_instruments_for_venues,
    fetch_instruments_via_urdi,
)


def _make_instrument_record(symbol: str = "BTCUSDT") -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=f"binance:perp:{symbol}",
        venue="binance",
        instrument_type="perp",
        base="BTC",
        quote="USDT",
    )


# ---------------------------------------------------------------------------
# fetch_instruments_via_urdi
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_instruments_via_urdi_supported_venue():
    record = _make_instrument_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_reference_adapter",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_via_urdi("binance", "perp")

    assert result == [record]
    mock_adapter.get_instruments.assert_awaited_once_with(instrument_type="perp")


@pytest.mark.asyncio
async def test_fetch_instruments_via_urdi_unsupported_venue_returns_empty():
    result = await fetch_instruments_via_urdi("unknown_venue_xyz", "perp")
    assert result == []


@pytest.mark.asyncio
async def test_fetch_instruments_via_urdi_not_implemented_returns_empty():
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(side_effect=NotImplementedError("no options"))

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_reference_adapter",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_via_urdi("deribit", "option")

    assert result == []


@pytest.mark.asyncio
async def test_fetch_instruments_via_urdi_network_error_returns_empty():
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(side_effect=ConnectionError("timeout"))

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_reference_adapter",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_via_urdi("binance", "perp")

    assert result == []


@pytest.mark.asyncio
async def test_fetch_instruments_via_urdi_empty_list():
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(return_value=[])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_reference_adapter",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_via_urdi("binance", "perp")

    assert result == []


# ---------------------------------------------------------------------------
# fetch_instruments_for_venues
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_instruments_for_venues_multiple():
    binance_record = _make_instrument_record("BTCUSDT")
    bybit_record = _make_instrument_record("BTCUSDT")

    async def _mock_fetch(venue: str, instrument_type: str = "perp"):  # type: ignore[override]
        if venue == "binance":
            return [binance_record]
        if venue == "bybit":
            return [bybit_record]
        return []

    with patch(
        "instruments_service.adapters.urdi_reference_provider.fetch_instruments_via_urdi",
        side_effect=_mock_fetch,
    ):
        result = await fetch_instruments_for_venues(["binance", "bybit"], "perp")

    assert "binance" in result
    assert "bybit" in result
    assert result["binance"] == [binance_record]
    assert result["bybit"] == [bybit_record]


@pytest.mark.asyncio
async def test_fetch_instruments_for_venues_skips_unsupported():
    binance_record = _make_instrument_record()

    async def _mock_fetch(venue: str, instrument_type: str = "perp"):  # type: ignore[override]
        if venue == "binance":
            return [binance_record]
        return []

    with patch(
        "instruments_service.adapters.urdi_reference_provider.fetch_instruments_via_urdi",
        side_effect=_mock_fetch,
    ):
        result = await fetch_instruments_for_venues(["binance", "unknown_xyz"], "perp")

    assert "binance" in result
    assert "unknown_xyz" not in result


@pytest.mark.asyncio
async def test_fetch_instruments_for_venues_empty_input():
    result = await fetch_instruments_for_venues([], "perp")
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_instruments_for_venues_excludes_empty_results():
    async def _mock_fetch(venue: str, instrument_type: str = "perp"):  # type: ignore[override]
        return []  # all venues return empty

    with patch(
        "instruments_service.adapters.urdi_reference_provider.fetch_instruments_via_urdi",
        side_effect=_mock_fetch,
    ):
        result = await fetch_instruments_for_venues(["binance", "bybit"], "perp")

    assert result == {}


# ---------------------------------------------------------------------------
# URDI_SUPPORTED_VENUES sanity checks
# ---------------------------------------------------------------------------


def test_urdi_supported_venues_is_frozenset():
    assert isinstance(URDI_SUPPORTED_VENUES, frozenset)


def test_urdi_supported_venues_contains_core_cefi():
    assert "binance" in URDI_SUPPORTED_VENUES
    assert "bybit" in URDI_SUPPORTED_VENUES
    assert "okx" in URDI_SUPPORTED_VENUES
