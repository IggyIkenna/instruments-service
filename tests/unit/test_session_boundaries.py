"""
Tests for session boundary fields across all 5 TradFi venues.

Validates that _get_exchange_trading_hours returns correct and consistent
session metadata for downstream services.

Test scenarios:
- All 5 venues return complete field set
- DST transition changes UTC hours
- Holidays return is_trading_day=False with nullified session times
- CME/ICE Sunday is not a trading day
- Auction times are correct per venue
- Early close detection
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def adapter():
    """Create a DatabentoAdapter with mocked dependencies to test _get_exchange_trading_hours."""
    from instruments_service.app.venues.databento.databento_adapter import DatabentoAdapter

    # Create adapter without calling __init__ (avoids API key requirements)
    inst = DatabentoAdapter.__new__(DatabentoAdapter)

    # _get_exchange_trading_hours calls _is_trading_holiday and _get_exchange_calendar
    # Mock _is_trading_holiday to return True for holidays AND weekends
    # (the real implementation uses exchange_calendars which treats weekends as non-sessions)
    def mock_is_trading_holiday(date, calendar):
        if isinstance(date, datetime):
            check_date = date.date() if hasattr(date, "date") else date
        else:
            check_date = date
        # Weekends are non-trading days
        if check_date.weekday() >= 5:
            return True
        # Memorial Day 2023
        if check_date == datetime(2023, 5, 29).date():
            return True
        # Christmas 2024
        if check_date == datetime(2024, 12, 25).date():
            return True
        return False

    inst._is_trading_holiday = mock_is_trading_holiday

    # Mock _get_exchange_calendar to return a mock calendar with early_closes
    def mock_get_exchange_calendar(calendar_name):
        mock_cal = MagicMock()
        # Simulate early_closes index (day after Thanksgiving 2024 = Nov 29)
        early_close_ts = pd.Timestamp("2024-11-29")
        mock_cal.early_closes = pd.DatetimeIndex([early_close_ts])
        # Simulate schedule DataFrame with actual close time
        mock_cal.schedule = pd.DataFrame(
            {"close": [pd.Timestamp("2024-11-29 18:00:00", tz="UTC")]},
            index=pd.DatetimeIndex([early_close_ts]),
        )
        return mock_cal

    inst._get_exchange_calendar = mock_get_exchange_calendar

    return inst


def _get_hours(adapter, exchange, target_date_str, instrument_type="EQUITY"):
    target_date = datetime.fromisoformat(target_date_str).replace(tzinfo=timezone.utc)
    return adapter._get_exchange_trading_hours(exchange, instrument_type, target_date)


class TestAllVenuesReturnCompleteFields:
    """Every venue should return all expected keys."""

    EXPECTED_KEYS = {
        "open",
        "close",
        "session",
        "is_trading_day",
        "holiday_calendar",
        "session_start_utc",
        "session_end_utc",
        "regular_open_utc",
        "regular_close_utc",
        "auction_open_utc",
        "auction_close_utc",
        "early_close_utc",
    }

    @pytest.mark.parametrize(
        "venue,instrument_type",
        [
            ("NYSE", "EQUITY"),
            ("NASDAQ", "EQUITY"),
            ("CME", "FUTURE"),
            ("ICE", "FUTURE"),
            ("CBOE", "EQUITY"),
        ],
    )
    def test_venue_returns_all_keys(self, adapter, venue, instrument_type):
        hours = _get_hours(adapter, venue, "2024-01-15", instrument_type)
        missing = self.EXPECTED_KEYS - set(hours.keys())
        assert not missing, f"{venue} missing keys: {missing}"

    def test_unknown_venue_returns_none_fields(self, adapter):
        hours = _get_hours(adapter, "UNKNOWN_EXCHANGE", "2024-01-15")
        assert hours["regular_open_utc"] is None
        assert hours["regular_close_utc"] is None
        assert hours["auction_open_utc"] is None
        assert hours["auction_close_utc"] is None
        assert hours["early_close_utc"] is None
        assert hours["is_trading_day"] is None


class TestRegularSessionTimes:
    """Regular session open/close should be non-None on trading days."""

    @pytest.mark.parametrize(
        "venue,instrument_type",
        [
            ("NYSE", "EQUITY"),
            ("NASDAQ", "EQUITY"),
            ("CME", "FUTURE"),
            ("ICE", "FUTURE"),
            ("CBOE", "EQUITY"),
        ],
    )
    def test_regular_open_close_populated(self, adapter, venue, instrument_type):
        hours = _get_hours(adapter, venue, "2024-01-15", instrument_type)
        assert hours["is_trading_day"] is True
        assert hours["regular_open_utc"] is not None, f"{venue} regular_open_utc is None"
        assert hours["regular_close_utc"] is not None, f"{venue} regular_close_utc is None"
        # Should be valid ISO datetime strings
        open_dt = datetime.fromisoformat(hours["regular_open_utc"])
        close_dt = datetime.fromisoformat(hours["regular_close_utc"])
        assert open_dt.tzinfo is not None
        assert close_dt.tzinfo is not None


class TestAuctionTimes:
    """NYSE, NASDAQ, CBOE should have auction times. CME, ICE should not."""

    @pytest.mark.parametrize("venue", ["NYSE", "NASDAQ", "CBOE"])
    def test_equity_venues_have_auctions(self, adapter, venue):
        hours = _get_hours(adapter, venue, "2024-01-15")
        assert hours["auction_open_utc"] is not None, f"{venue} should have opening auction"
        assert hours["auction_close_utc"] is not None, f"{venue} should have closing auction"

    @pytest.mark.parametrize("venue", ["CME", "ICE"])
    def test_futures_venues_no_auctions(self, adapter, venue):
        hours = _get_hours(adapter, venue, "2024-01-15", "FUTURE")
        assert hours["auction_open_utc"] is None, f"{venue} should not have opening auction"
        assert hours["auction_close_utc"] is None, f"{venue} should not have closing auction"


class TestHolidays:
    """Holidays should set is_trading_day=False and nullify session times."""

    def test_memorial_day_closed(self, adapter):
        hours = _get_hours(adapter, "NYSE", "2023-05-29")
        assert hours["is_trading_day"] is False
        assert hours["open"] == "holiday"
        assert hours["close"] == "holiday"
        assert hours["regular_open_utc"] is None
        assert hours["regular_close_utc"] is None
        assert hours["auction_open_utc"] is None
        assert hours["auction_close_utc"] is None
        assert hours["early_close_utc"] is None

    def test_christmas_closed(self, adapter):
        hours = _get_hours(adapter, "NASDAQ", "2024-12-25")
        assert hours["is_trading_day"] is False
        assert hours["open"] == "holiday"


class TestWeekends:
    """Weekends should be is_trading_day=False for all venues."""

    @pytest.mark.parametrize(
        "venue,instrument_type",
        [
            ("NYSE", "EQUITY"),
            ("NASDAQ", "EQUITY"),
            ("CBOE", "EQUITY"),
        ],
    )
    def test_saturday_not_trading(self, adapter, venue, instrument_type):
        hours = _get_hours(adapter, venue, "2024-01-13", instrument_type)  # Saturday
        assert hours["is_trading_day"] is False


class TestCMEICESunday:
    """CME and ICE Sunday should be is_trading_day=False."""

    def test_cme_sunday_not_trading(self, adapter):
        hours = _get_hours(adapter, "CME", "2024-01-14", "FUTURE")  # Sunday
        assert hours["is_trading_day"] is False

    def test_ice_sunday_not_trading(self, adapter):
        hours = _get_hours(adapter, "ICE", "2024-01-14", "FUTURE")  # Sunday
        assert hours["is_trading_day"] is False


class TestDSTTransition:
    """EST vs EDT should produce different UTC times for the same local hour."""

    def test_nyse_est_vs_edt(self, adapter):
        # EST (January): 9:30 AM ET = 14:30 UTC
        hours_est = _get_hours(adapter, "NYSE", "2024-01-15")
        # EDT (July): 9:30 AM ET = 13:30 UTC
        hours_edt = _get_hours(adapter, "NYSE", "2024-07-15")

        open_est = datetime.fromisoformat(hours_est["regular_open_utc"])
        open_edt = datetime.fromisoformat(hours_edt["regular_open_utc"])

        # EDT should open 1 hour earlier in UTC than EST
        assert open_edt.hour == open_est.hour - 1, (
            f"EDT open {open_edt.hour} should be 1 hour before EST open {open_est.hour}"
        )


class TestEarlyClose:
    """Early close days should populate early_close_utc and adjust close time."""

    def test_early_close_day_has_early_close_utc(self, adapter):
        # Day after Thanksgiving 2024 (Nov 29) -- configured in mock
        hours = _get_hours(adapter, "NYSE", "2024-11-29")
        assert hours["early_close_utc"] is not None
        early_dt = datetime.fromisoformat(hours["early_close_utc"])
        assert early_dt.hour == 18  # 1 PM ET = 18:00 UTC (EST)

    def test_normal_day_no_early_close(self, adapter):
        hours = _get_hours(adapter, "NYSE", "2024-01-15")
        assert hours["early_close_utc"] is None


class TestHolidayDensePeriod:
    """Dec 24 - Jan 2 should correctly handle multiple non-trading days."""

    def test_christmas_week_has_mixed_trading_days(self, adapter):
        """Christmas week should have a mix of trading and non-trading days."""
        results = {}
        for day in range(24, 32):
            date_str = f"2024-12-{day:02d}" if day <= 31 else "2025-01-01"
            if day > 31:
                break
            hours = _get_hours(adapter, "NYSE", date_str)
            results[day] = hours["is_trading_day"]

        # Dec 25 (Christmas) should be closed
        assert results[25] is False
        # Dec 28-29 (Sat/Sun) should be closed
        assert results[28] is False
        assert results[29] is False

    def test_new_years_day_closed(self, adapter):
        """New Year's Day should be closed."""
        # Add New Year's to mock
        original_mock = adapter._is_trading_holiday

        def mock_with_new_years(date, calendar):
            if hasattr(date, "date"):
                check_date = date.date()
            else:
                check_date = date
            if check_date == datetime(2025, 1, 1).date():
                return True
            return original_mock(date, calendar)

        adapter._is_trading_holiday = mock_with_new_years

        hours = _get_hours(adapter, "NYSE", "2025-01-01")
        assert hours["is_trading_day"] is False


class TestVIXCBOE:
    """CBOE session times should work for VIX even though data source is Barchart."""

    def test_cboe_has_session_fields(self, adapter):
        """CBOE should have all session fields populated on a trading day."""
        hours = _get_hours(adapter, "CBOE", "2024-01-15")
        assert hours["is_trading_day"] is True
        assert hours["regular_open_utc"] is not None
        assert hours["regular_close_utc"] is not None
        assert hours["holiday_calendar"] == "CBOE"

    def test_cboe_close_time_is_later_than_nyse(self, adapter):
        """CBOE closes at 4:15 PM ET, later than NYSE 4:00 PM ET."""
        cboe_hours = _get_hours(adapter, "CBOE", "2024-01-15")
        nyse_hours = _get_hours(adapter, "NYSE", "2024-01-15")

        cboe_close = datetime.fromisoformat(cboe_hours["regular_close_utc"])
        nyse_close = datetime.fromisoformat(nyse_hours["regular_close_utc"])

        assert cboe_close > nyse_close, f"CBOE close {cboe_close} should be after NYSE close {nyse_close}"

    def test_cboe_weekend_closed(self, adapter):
        """CBOE should be closed on weekends just like NYSE."""
        hours = _get_hours(adapter, "CBOE", "2024-01-13")  # Saturday
        assert hours["is_trading_day"] is False
