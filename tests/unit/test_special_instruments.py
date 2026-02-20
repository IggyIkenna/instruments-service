"""
Unit tests for special_instruments module (Task 110 coverage).
"""

from datetime import datetime, timezone

from instruments_service.utils.special_instruments import (
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
)


class TestCreateVixInstrumentDefinition:
    """Tests for create_vix_instrument_definition."""

    def test_returns_dict_with_required_keys(self):
        """Test VIX definition has required keys."""
        target_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        result = create_vix_instrument_definition(target_date)
        assert isinstance(result, dict)
        assert result["instrument_key"] == "CBOE:INDEX:VIX"
        assert result["venue"] == "CBOE"
        assert result["instrument_type"] == "INDEX"
        assert result["symbol"] == "VIX"
        assert result["market_category"] == "TRADFI"

    def test_available_from_datetime(self):
        """Test VIX has correct availability window."""
        target_date = datetime(2024, 6, 1, tzinfo=timezone.utc)
        result = create_vix_instrument_definition(target_date)
        assert "2020-01-01" in result["available_from_datetime"]
        assert result["available_to_datetime"] is None


class TestCreateKrwusdInstrumentDefinition:
    """Tests for create_krwusd_instrument_definition."""

    def test_returns_dict_with_required_keys(self):
        """Test KRW/USD definition has required keys."""
        target_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        result = create_krwusd_instrument_definition(target_date)
        assert isinstance(result, dict)
        assert result["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"
        assert result["venue"] == "FX"
        assert result["base_asset"] == "KRW"
        assert result["quote_asset"] == "USD"
        assert result["market_category"] == "TRADFI"
