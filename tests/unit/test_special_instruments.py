"""
Unit tests for special_instruments module (Task 110 coverage).
"""

from datetime import UTC, datetime

from instruments_service.utils.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)


class TestCreateVixInstrumentDefinition:
    """Tests for create_vix_instrument_definition."""

    def test_returns_dict_with_required_keys(self):
        """Test VIX definition has required keys."""
        target_date = datetime(2024, 1, 15, tzinfo=UTC)
        result = create_vix_instrument_definition(target_date)
        assert isinstance(result, dict)
        assert result["instrument_key"] == "CBOE:INDEX:VIX"
        assert result["venue"] == "CBOE"
        assert result["instrument_type"] == "INDEX"
        assert result["symbol"] == "VIX"
        assert result["market_category"] == "TRADFI"

    def test_available_from_datetime(self):
        """Test VIX has correct availability window."""
        target_date = datetime(2024, 6, 1, tzinfo=UTC)
        result = create_vix_instrument_definition(target_date)
        assert "2020-01-01" in result["available_from_datetime"]
        assert result["available_to_datetime"] is None


class TestCreateKrwusdInstrumentDefinition:
    """Tests for create_krwusd_instrument_definition."""

    def test_returns_dict_with_required_keys(self):
        """Test KRW/USD definition has required keys."""
        target_date = datetime(2024, 1, 15, tzinfo=UTC)
        result = create_krwusd_instrument_definition(target_date)
        assert isinstance(result, dict)
        assert result["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"
        assert result["venue"] == "FX"
        assert result["base_asset"] == "KRW"
        assert result["quote_asset"] == "USD"
        assert result["market_category"] == "TRADFI"


# From test_low_coverage_modules


class TestGetUsEquityTradingHours:
    def test_returns_session_start(self):
        result = get_us_equity_trading_hours("NASDAQ", "ETF", datetime(2025, 3, 28, tzinfo=UTC))
        assert "14:30" in result["session_start_utc"]

    def test_returns_session_end(self):
        result = get_us_equity_trading_hours("NYSE", "STOCK", datetime(2025, 3, 28, tzinfo=UTC))
        assert "21:00" in result["session_end_utc"]

    def test_trading_day_flag(self):
        result = get_us_equity_trading_hours("NASDAQ", "ETF", datetime(2025, 3, 28, tzinfo=UTC))
        assert result["is_trading_day"] is True

    def test_holiday_calendar_nyse(self):
        result = get_us_equity_trading_hours("NYSE", "ETF", datetime(2025, 3, 28, tzinfo=UTC))
        assert result["holiday_calendar"] == "NYSE"

    def test_naive_datetime_gets_utc(self):
        # naive datetime should get UTC tzinfo assigned
        result = get_us_equity_trading_hours("NASDAQ", "ETF", datetime(2025, 3, 28))
        assert result["session_start_utc"] is not None


class TestCreateBitcoinEtfInstrumentDefinition:
    def _trading_hours(self, venue, instrument_type, target_date):
        return {
            "session_start_utc": "2025-03-28T14:30:00+00:00",
            "session_end_utc": "2025-03-28T21:00:00+00:00",
            "open": "09:30:00-05:00",
            "close": "16:00:00-05:00",
            "session": "regular",
            "is_trading_day": True,
            "holiday_calendar": "NYSE",
            "regular_open_utc": "2025-03-28T14:30:00+00:00",
            "regular_close_utc": "2025-03-28T21:00:00+00:00",
            "auction_open_utc": None,
            "auction_close_utc": None,
            "early_close_utc": None,
        }

    def test_ibit_etf_key(self):
        result = create_bitcoin_etf_instrument_definition(
            "IBIT", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["instrument_key"] == "NASDAQ:ETF:IBIT-USD"

    def test_fbtc_etf_key(self):
        result = create_bitcoin_etf_instrument_definition(
            "FBTC", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["venue"] == "NASDAQ"

    def test_arkb_etf_key(self):
        result = create_bitcoin_etf_instrument_definition(
            "ARKB", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["underlying"] == "BTC"

    def test_unknown_ticker_returns_none(self):
        result = create_bitcoin_etf_instrument_definition(
            "UNKNOWNTICKER", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is None

    def test_lowercase_ticker_works(self):
        result = create_bitcoin_etf_instrument_definition(
            "ibit", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["instrument_key"] == "NASDAQ:ETF:IBIT-USD"

    def test_trading_hours_propagated(self):
        result = create_bitcoin_etf_instrument_definition(
            "IBIT", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["trading_session"] == "regular"
        assert result["is_trading_day"] is True

    def test_missing_session_start_uses_fallback(self):
        def no_session_start(venue, inst_type, target_date):
            return {
                "session_start_utc": None,
                "session_end_utc": None,
                "open": "09:30:00-05:00",
                "close": "16:00:00-05:00",
                "session": "regular",
                "is_trading_day": True,
                "holiday_calendar": "NYSE",
                "regular_open_utc": None,
                "regular_close_utc": None,
                "auction_open_utc": None,
                "auction_close_utc": None,
                "early_close_utc": None,
            }

        result = create_bitcoin_etf_instrument_definition("IBIT", datetime(2025, 3, 28, tzinfo=UTC), no_session_start)
        assert result is not None
        # Falls back to midnight
        assert "available_from_datetime" in result
