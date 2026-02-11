"""Unit tests for databento special_instruments converter."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from instruments_service.app.venues.databento.converters.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
)


def test_create_krwusd_returns_expected_keys():
    """Test KRW/USD instrument definition has required keys."""
    target_date = datetime(2024, 7, 15, tzinfo=timezone.utc)
    result = create_krwusd_instrument_definition(target_date)
    assert result["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"
    assert result["venue"] == "FX"
    assert result["instrument_type"] == "SPOT_PAIR"
    assert result["base_asset"] == "KRW"
    assert result["quote_asset"] == "USD"
    assert result["data_provider"] == "yahoo_finance"


def test_create_bitcoin_etf_ibit_returns_definition():
    """Test IBIT Bitcoin ETF returns valid definition with mock trading hours."""
    target_date = datetime(2024, 11, 11, tzinfo=timezone.utc)
    mock_get_hours = MagicMock(
        return_value={
            "session_start_utc": "2024-11-11T14:30:00+00:00",
            "session_end_utc": "2024-11-11T21:00:00+00:00",
        }
    )
    result = create_bitcoin_etf_instrument_definition("IBIT", target_date, mock_get_hours)
    assert result is not None
    assert result["instrument_key"] == "NASDAQ:ETF:IBIT-USD"
    assert result["venue"] == "NASDAQ"
    assert result["underlying"] == "BTC"
    mock_get_hours.assert_called_once_with("NASDAQ", "ETF", target_date)


def test_create_bitcoin_etf_unsupported_returns_none():
    """Test unsupported ticker returns None."""
    target_date = datetime(2024, 11, 11, tzinfo=timezone.utc)
    mock_get_hours = MagicMock()
    result = create_bitcoin_etf_instrument_definition("GBTC", target_date, mock_get_hours)
    assert result is None
    mock_get_hours.assert_not_called()
