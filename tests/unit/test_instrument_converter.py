"""Unit tests for databento instrument_converter."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd

from instruments_service.app.venues.databento.converters.instrument_converter import (
    convert_to_instrument_definition,
    get_exchange_trading_hours,
)


def test_convert_to_instrument_definition_import():
    """Test that instrument_converter module imports correctly."""
    assert convert_to_instrument_definition is not None
    assert callable(convert_to_instrument_definition)
    assert get_exchange_trading_hours is not None
    assert callable(get_exchange_trading_hours)


def test_get_exchange_trading_hours_returns_dict():
    """Test get_exchange_trading_hours returns expected keys with mock adapter."""
    mock_adapter = MagicMock()
    mock_adapter._is_trading_holiday.return_value = False
    mock_adapter._get_exchange_calendar.return_value = None  # No early close check

    result = get_exchange_trading_hours(
        mock_adapter,
        exchange="NASDAQ",
        instrument_type="EQUITY",
        target_date=datetime(2024, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
    )

    assert isinstance(result, dict)
    assert "open" in result
    assert "close" in result
    assert "session_start_utc" in result
    assert "session_end_utc" in result
    mock_adapter._is_trading_holiday.assert_called_once()


def test_convert_to_instrument_definition_equity_row():
    """Test convert_to_instrument_definition for a simple equity row."""
    mock_adapter = MagicMock()
    mock_adapter._resolve_instrument_id_to_raw_symbol.return_value = None

    row = pd.Series(
        {
            "asset": "AAPL",
            "currency": "USD",
            "security_type": "STK",
            "min_price_increment": 0.01,
        }
    )

    result = convert_to_instrument_definition(
        mock_adapter,
        row,
        exchange="NASDAQ",
        dataset="DBEQ.BASIC",
        databento_symbol="AAPL",
        exchange_raw_symbol="AAPL",
        target_date=datetime(2024, 7, 15, tzinfo=timezone.utc),
    )

    assert result is not None
    assert result["instrument_key"] == "NASDAQ:EQUITY:AAPL-USD"
    assert result["base_asset"] == "AAPL"
    assert result["quote_asset"] == "USD"
    assert result["venue"] == "NASDAQ"
