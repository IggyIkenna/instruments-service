"""Tests for urdi_reference_provider — URDI instrument wiring in instruments-service.

The provider uses CANONICAL venue names (UAC uppercase, e.g. "BINANCE-SPOT")
and translates them to URDI adapter keys via CANONICAL_VENUE_TO_ADAPTER from URDI.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.adapters.urdi_reference_provider import (
    URDI_SUPPORTED_VENUES,
    fetch_instruments_for_all_venues,
    fetch_instruments_via_urdi,
)


def _make_record(venue: str = "MORPHO-ETHEREUM") -> object:
    from unified_api_contracts.internal import InstrumentRecord

    return InstrumentRecord(
        instrument_key=f"{venue}:A_TOKEN:WETH",
        venue=venue,
        instrument_type="A_TOKEN",
        base_asset="WETH",
        quote_asset="USDC",
    )


# ---------------------------------------------------------------------------
# URDI_SUPPORTED_VENUES — canonical name set
# ---------------------------------------------------------------------------


def test_urdi_supported_venues_is_frozenset():
    assert isinstance(URDI_SUPPORTED_VENUES, frozenset)


def test_urdi_supported_venues_uses_canonical_names():
    """All venue names are UAC canonical (uppercase, not URDI adapter file stems)."""
    # Canonical names are uppercase hyphenated
    assert "BINANCE-SPOT" in URDI_SUPPORTED_VENUES or "BINANCE-FUTURES" in URDI_SUPPORTED_VENUES
    assert "UNISWAPV3-ETHEREUM" in URDI_SUPPORTED_VENUES
    assert "MORPHO-ETHEREUM" in URDI_SUPPORTED_VENUES
    assert "BETFAIR" in URDI_SUPPORTED_VENUES
    # Must NOT contain lowercase URDI adapter keys
    assert "binance" not in URDI_SUPPORTED_VENUES
    assert "uniswap_v3" not in URDI_SUPPORTED_VENUES


def test_urdi_supported_venues_count():
    assert len(URDI_SUPPORTED_VENUES) >= 20


# ---------------------------------------------------------------------------
# fetch_instruments_for_all_venues
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_empty_venues_returns_empty():
    result = await fetch_instruments_for_all_venues([])
    assert result == []


@pytest.mark.asyncio
async def test_fetch_unsupported_venue_warns_and_skips(caplog):
    with caplog.at_level("WARNING"):
        result = await fetch_instruments_for_all_venues(["TOTALLY_UNKNOWN_VENUE_XYZ"])
    assert result == []
    assert any("TOTALLY_UNKNOWN_VENUE_XYZ" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_supported_venue_returns_records():
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert len(result) == 1
    mock_adapter.get_instruments_cached.assert_awaited_once_with(instrument_type=None)


@pytest.mark.asyncio
async def test_fetch_deduplicates_shared_adapters():
    """Two canonical venues mapping to the same adapter key are deduplicated."""
    record = _make_record("AAVEV3-ETHEREUM")
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        # Only AAVEV3-ETHEREUM (single venue, single adapter call)
        result = await fetch_instruments_for_all_venues(["AAVEV3-ETHEREUM"])

    assert len(result) == 1
    assert mock_adapter.get_instruments_cached.call_count == 1


@pytest.mark.asyncio
async def test_fetch_network_error_skips_venue_logs_warning(caplog):
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=ConnectionError("timeout"))

    with (
        patch(
            "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result == []
    assert any("network error" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_not_implemented_skips_venue():
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=NotImplementedError("no options"))

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["DERIBIT"], instrument_type="OPTION")

    assert result == []


@pytest.mark.asyncio
async def test_fetch_multiple_venues_flat_list():
    """Results from multiple venues are returned as a single flat list."""
    r1 = _make_record("MORPHO-ETHEREUM")
    r2 = _make_record("CURVE-ETHEREUM")

    mock1 = MagicMock()
    mock1.get_instruments_cached = AsyncMock(return_value=[r1])
    mock2 = MagicMock()
    mock2.get_instruments_cached = AsyncMock(return_value=[r2])

    call_count = 0

    def _adapter_factory(venue: str, api_key=None, date=None, extra_api_keys=None):
        nonlocal call_count
        call_count += 1
        return mock1 if call_count == 1 else mock2

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        side_effect=_adapter_factory,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM", "CURVE-ETHEREUM"])

    assert len(result) == 2


@pytest.mark.asyncio
async def test_fetch_injects_api_key():
    """API key is passed from api_keys dict to the adapter via data source lookup."""
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ) as mock_factory:
        await fetch_instruments_for_all_venues(
            ["MORPHO-ETHEREUM"],
            api_keys={"thegraph": "my-graph-key"},
        )

    # adapter factory was called with api_key="my-graph-key" (morpho uses thegraph)
    call_kwargs = mock_factory.call_args
    assert call_kwargs is not None


# ---------------------------------------------------------------------------
# fetch_instruments_via_urdi (single-venue convenience)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_via_urdi_delegates_to_all_venues():
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_via_urdi("MORPHO-ETHEREUM")

    assert result == [record]


@pytest.mark.asyncio
async def test_fetch_via_urdi_unsupported_returns_empty():
    result = await fetch_instruments_via_urdi("UNKNOWN_VENUE_999")
    assert result == []


@pytest.mark.asyncio
async def test_fetch_adapter_value_error_is_logged_not_raised(caplog):
    """ValueError from adapter (bad config) is logged as error and returns empty, not raised."""
    from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues

    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=ValueError("invalid endpoint config"))

    with (
        caplog.at_level("ERROR"),
        patch(
            "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
    ):
        result = await fetch_instruments_for_all_venues(["AAVEV3-ETHEREUM"])

    assert result == []
    assert any("adapter error" in r.message for r in caplog.records)
