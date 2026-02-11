"""Unit tests for databento symbol_resolver converter."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from instruments_service.app.venues.databento.converters.symbol_resolver import (
    resolve_instrument_id_to_raw_symbol,
)


def test_resolve_instrument_id_to_raw_symbol_import():
    """Test that symbol_resolver module imports correctly."""
    assert resolve_instrument_id_to_raw_symbol is not None
    assert callable(resolve_instrument_id_to_raw_symbol)


def test_resolve_returns_none_on_empty_response():
    """Test that empty symbology response returns None."""
    client = MagicMock()
    client.symbology.resolve.return_value = None

    base_client = MagicMock()

    result = resolve_instrument_id_to_raw_symbol(
        client=client,
        base_client=base_client,
        instrument_id=12345,
        exchange="CME",
        dataset="GLBX.MDP3",
        target_date=datetime(2024, 7, 15, tzinfo=timezone.utc),
    )

    assert result is None
    base_client.ip_rate_limiter.acquire.assert_called_once_with("symbology")
    client.symbology.resolve.assert_called_once()


def test_resolve_returns_string_from_dict_s_key():
    """Test that dict response with 'S' key returns symbol string."""
    client = MagicMock()
    client.symbology.resolve.return_value = {"12345": {"S": "ESZ0 C3620", "D0": "2024-07-15"}}

    base_client = MagicMock()

    result = resolve_instrument_id_to_raw_symbol(
        client=client,
        base_client=base_client,
        instrument_id=12345,
        exchange="CME",
        dataset="GLBX.MDP3",
        target_date=datetime(2024, 7, 15, tzinfo=timezone.utc),
    )

    assert result == "ESZ0 C3620"
