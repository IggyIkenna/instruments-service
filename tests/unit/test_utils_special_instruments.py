"""
Unit tests for instruments_service.utils.special_instruments module.

This module re-exports factory functions — tested via the engine module,
but we also need to cover the `get_us_equity_trading_hours` and
`create_bitcoin_etf_instrument_definition` code paths here.
"""

from datetime import UTC, datetime

from instruments_service.utils.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)


class TestUtilsSpecialInstruments:
    """Tests for utils/special_instruments.py covering uncovered paths."""

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

    def test_get_us_equity_trading_hours_with_utc(self):
        result = get_us_equity_trading_hours("NASDAQ", "ETF", datetime(2025, 3, 28, tzinfo=UTC))
        assert "14:30" in result["session_start_utc"]
        assert "21:00" in result["session_end_utc"]

    def test_get_us_equity_trading_hours_naive_datetime(self):
        # naive datetime — should add UTC
        result = get_us_equity_trading_hours("NYSE", "STOCK", datetime(2025, 3, 28))
        assert result["is_trading_day"] is True

    def test_vix_instrument_key(self):
        result = create_vix_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["instrument_key"] == "CBOE:INDEX:VIX"
        assert result["data_provider"] == "databento"

    def test_krwusd_instrument_key(self):
        result = create_krwusd_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"
        assert result["data_provider"] == "yahoo_finance"

    def test_ibit_etf_created(self):
        result = create_bitcoin_etf_instrument_definition(
            "IBIT", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["instrument_key"] == "NASDAQ:ETF:IBIT-USD"
        assert result["underlying"] == "BTC"
        assert result["market_category"] == "TRADFI"

    def test_arkb_etf_created(self):
        result = create_bitcoin_etf_instrument_definition(
            "ARKB", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is not None
        assert result["venue"] == "NASDAQ"

    def test_unknown_etf_returns_none(self):
        result = create_bitcoin_etf_instrument_definition(
            "GBTC", datetime(2025, 3, 28, tzinfo=UTC), self._trading_hours
        )
        assert result is None

    def test_etf_with_no_session_start_falls_back(self):
        def no_start(venue, inst_type, target_date):
            return {
                "session_start_utc": None,
                "session_end_utc": None,
                "open": None,
                "close": None,
                "session": "regular",
                "is_trading_day": True,
                "holiday_calendar": "NYSE",
                "regular_open_utc": None,
                "regular_close_utc": None,
                "auction_open_utc": None,
                "auction_close_utc": None,
                "early_close_utc": None,
            }
        result = create_bitcoin_etf_instrument_definition(
            "FBTC", datetime(2025, 3, 28, tzinfo=UTC), no_start
        )
        assert result is not None
        assert result["available_from_datetime"] is not None
