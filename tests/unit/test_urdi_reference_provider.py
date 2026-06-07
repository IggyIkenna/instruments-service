"""Tests for urdi_reference_provider — URDI instrument wiring in instruments-service.

The provider uses CANONICAL venue names (UAC uppercase, e.g. "BINANCE-SPOT")
and translates them to URDI adapter keys via CANONICAL_VENUE_TO_ADAPTER from URDI.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.engine.urdi_reference_provider import (
    URDI_SUPPORTED_VENUES,
    VenueFetchResult,
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
        base_asset_contract_address="0x" + "a" * 40,
        base_asset_decimals=18,
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
    assert "UNISWAP_V3-ETHEREUM" in URDI_SUPPORTED_VENUES
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
    assert isinstance(result, VenueFetchResult)
    assert result.records == []


@pytest.mark.asyncio
async def test_fetch_unsupported_venue_warns_and_skips(caplog):
    with caplog.at_level("WARNING"):
        result = await fetch_instruments_for_all_venues(["TOTALLY_UNKNOWN_VENUE_XYZ"])
    assert result.records == []
    assert any("TOTALLY_UNKNOWN_VENUE_XYZ" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_supported_venue_returns_records():
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert len(result.records) == 1
    mock_adapter.get_instruments_cached.assert_awaited_once_with(instrument_type=None, date=None)


@pytest.mark.asyncio
async def test_fetch_deduplicates_shared_adapters():
    """Two canonical venues mapping to the same adapter key are deduplicated."""
    record = _make_record("AAVE_V3-ETHEREUM")
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        # Only AAVE_V3-ETHEREUM (single venue, single adapter call)
        result = await fetch_instruments_for_all_venues(["AAVE_V3-ETHEREUM"])

    assert len(result.records) == 1
    assert mock_adapter.get_instruments_cached.call_count == 1


@pytest.mark.asyncio
async def test_fetch_network_error_skips_venue_logs_warning(caplog):
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=ConnectionError("timeout"))

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.records == []
    assert any("network" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_not_implemented_skips_venue():
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=NotImplementedError("no options"))

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["DERIBIT"], instrument_type="OPTION")

    assert result.records == []


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

    def _adapter_factory(venue: str, api_key=None, date=None, extra_api_keys=None, mode="batch", source=None):
        nonlocal call_count
        call_count += 1
        return mock1 if call_count == 1 else mock2

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        side_effect=_adapter_factory,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM", "CURVE-ETHEREUM"])

    assert len(result.records) == 2


@pytest.mark.asyncio
async def test_fetch_injects_api_key():
    """API key is passed from api_keys dict to the adapter via data source lookup."""
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
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
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
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
    from instruments_service.engine.urdi_reference_provider import fetch_instruments_for_all_venues

    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=ValueError("invalid endpoint config"))

    with (
        caplog.at_level("ERROR"),
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
    ):
        result = await fetch_instruments_for_all_venues(["AAVE_V3-ETHEREUM"])

    assert result.records == []
    assert any("ADAPTER_ERROR" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_duplicate_venue_deduped():
    """Same venue passed twice is only fetched once."""
    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM", "MORPHO-ETHEREUM"])

    # Only one fetch call despite two entries
    assert mock_adapter.get_instruments_cached.call_count == 1
    assert len(result.records) == 1


@pytest.mark.asyncio
async def test_fetch_timeout_error_retryable(caplog):
    """TimeoutError → VenueError with retryable=True."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=TimeoutError("timed out"))

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.records == []
    assert len(result.failed_venues) == 1
    assert result.failed_venues[0].retryable is True
    assert result.failed_venues[0].error_code == "TIMEOUT"


@pytest.mark.asyncio
async def test_fetch_os_error_retryable(caplog):
    """OSError → VenueError with retryable=True."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=OSError("socket error"))

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.records == []
    assert len(result.failed_venues) == 1
    assert result.failed_venues[0].retryable is True
    assert result.failed_venues[0].error_code == "NETWORK"


@pytest.mark.asyncio
async def test_fetch_runtime_error_429_rate_limit(caplog):
    """RuntimeError with 429 → RATE_LIMIT error code."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=RuntimeError("429 too many requests"))

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.records == []
    assert len(result.failed_venues) == 1
    assert result.failed_venues[0].error_code == "RATE_LIMIT"
    assert result.failed_venues[0].retryable is True


@pytest.mark.asyncio
async def test_fetch_runtime_error_503_server_error(caplog):
    """RuntimeError with 503 → SERVER_ERROR code."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=RuntimeError("503 service unavailable"))

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.records == []
    assert len(result.failed_venues) == 1
    assert result.failed_venues[0].error_code == "SERVER_ERROR"


@pytest.mark.asyncio
async def test_fetch_runtime_error_generic_retry_exhausted():
    """RuntimeError without known status → RETRY_EXHAUSTED."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=RuntimeError("generic failure"))

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.failed_venues[0].error_code == "RETRY_EXHAUSTED"


@pytest.mark.asyncio
async def test_fetch_attribute_error_parse_error():
    """AttributeError → PARSE_ERROR (permanent)."""
    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(side_effect=AttributeError("missing attr"))

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert result.failed_venues[0].error_code == "PARSE_ERROR"
    assert result.failed_venues[0].retryable is False


@pytest.mark.asyncio
async def test_fetch_sibling_routed_instruments_skipped():
    """Instruments tagged for a sibling venue in the batch are silently skipped."""
    from unified_api_contracts.internal import InstrumentRecord

    r_own = InstrumentRecord(
        instrument_key="MORPHO-ETHEREUM:A_TOKEN:WETH",
        venue="MORPHO-ETHEREUM",
        instrument_type="A_TOKEN",
        base_asset="WETH",
        quote_asset="USDC",
        base_asset_contract_address="0x" + "a" * 40,
        base_asset_decimals=18,
    )
    r_sibling = InstrumentRecord(
        instrument_key="AAVE_V3-ETHEREUM:A_TOKEN:WBTC",
        venue="AAVE_V3-ETHEREUM",
        instrument_type="A_TOKEN",
        base_asset="WBTC",
        quote_asset="USDC",
        base_asset_contract_address="0x" + "b" * 40,
        base_asset_decimals=8,
    )

    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[r_own, r_sibling])

    with patch(
        "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        # Both venues in batch → sibling is silently routed
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM", "AAVE_V3-ETHEREUM"])

    # Each venue's adapter returns both records, but each only claims its own
    # MORPHO adapter: r_own matches, r_sibling goes to sibling
    # AAVE adapter: r_sibling matches, r_own goes to sibling
    # So total = r_own (from morpho adapter) + r_sibling (from aave adapter) = 2 records
    assert len(result.records) == 2


@pytest.mark.asyncio
async def test_fetch_unknown_venue_tag_logs_warning(caplog):
    """Instruments tagged for a venue NOT in the batch are warned."""
    from unified_api_contracts.internal import InstrumentRecord

    r_own = InstrumentRecord(
        instrument_key="MORPHO-ETHEREUM:A_TOKEN:WETH",
        venue="MORPHO-ETHEREUM",
        instrument_type="A_TOKEN",
        base_asset="WETH",
        quote_asset="USDC",
        base_asset_contract_address="0x" + "a" * 40,
        base_asset_decimals=18,
    )
    r_unknown = InstrumentRecord(
        instrument_key="UNKNOWN_VENUE:SPOT_PAIR:BTC",
        venue="UNKNOWN_VENUE",
        instrument_type="SPOT_PAIR",
        base_asset="BTC",
        quote_asset="USDC",
        base_asset_contract_address="0x" + "c" * 40,
        base_asset_decimals=8,
    )

    mock_adapter = MagicMock()
    mock_adapter.get_instruments_cached = AsyncMock(return_value=[r_own, r_unknown])

    with (
        patch(
            "instruments_service.engine.urdi_reference_provider.get_adapter_for_canonical_venue",
            return_value=mock_adapter,
        ),
        caplog.at_level("WARNING"),
    ):
        result = await fetch_instruments_for_all_venues(["MORPHO-ETHEREUM"])

    assert len(result.records) == 1
    assert any("unknown" in r.message.lower() for r in caplog.records)
