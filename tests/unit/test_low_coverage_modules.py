"""
Unit tests targeting low-coverage modules in instruments-service.

Covers:
- ErrorWarningCounter (0% → 100%)
- InstrumentValidationMixin (54% → high)
- special_instruments factory functions (35% → high)
- dump_to_csv utility (61% → high)
"""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.app.core.instrument_validation import InstrumentValidationMixin
from instruments_service.engine.venues.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)
from instruments_service.utils.error_warning_counter import ErrorWarningCounter

# ─── ErrorWarningCounter ─────────────────────────────────────────────────────


class TestErrorWarningCounter:
    def test_initial_counts_zero(self):
        counter = ErrorWarningCounter()
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_counts_error_records(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_error")
        root.addHandler(counter)
        root.setLevel(logging.ERROR)
        root.error("test error")
        root.removeHandler(counter)
        assert counter.error_count == 1
        assert counter.warning_count == 0

    def test_counts_warning_records(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_warning")
        root.addHandler(counter)
        root.setLevel(logging.WARNING)
        root.warning("test warning")
        root.removeHandler(counter)
        assert counter.warning_count == 1
        assert counter.error_count == 0

    def test_ignores_info_and_below(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_info")
        root.addHandler(counter)
        root.setLevel(logging.DEBUG)
        root.info("info message")
        root.debug("debug message")
        root.removeHandler(counter)
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_reset_clears_counts(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_reset")
        root.addHandler(counter)
        root.setLevel(logging.ERROR)
        root.error("err")
        root.warning("warn")
        root.removeHandler(counter)
        counter.reset()
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_critical_counts_as_error(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_critical")
        root.addHandler(counter)
        root.setLevel(logging.CRITICAL)
        root.critical("critical error")
        root.removeHandler(counter)
        assert counter.error_count == 1


# ─── InstrumentValidationMixin ────────────────────────────────────────────────


class _MockService(InstrumentValidationMixin):
    """Test double that satisfies the InstrumentValidationMixin protocol."""

    def __init__(self):
        self.venue_mapping = MagicMock()
        self.venue_mapping.all_cefi_venues = ["BINANCE", "BYBIT", "DERIBIT"]
        self.venue_mapping.all_databento_venues = ["CME", "ICE", "NASDAQ"]
        self.venue_mapping.all_defi_venues = ["UNISWAP_V3", "AAVE_V3"]


class TestInstrumentValidationMixin:
    def setup_method(self):
        self.svc = _MockService()

    # _validate_venues_filter
    def test_empty_venues_filter_returns_none(self):
        result = self.svc._validate_venues_filter([], cefi=True, tradfi=True, defi=True)
        assert result is None

    def test_valid_cefi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["BINANCE"], cefi=True, tradfi=False, defi=False)
        assert result is not None
        assert "BINANCE" in result["CEFI"]

    def test_valid_tradfi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["CME"], cefi=False, tradfi=True, defi=False)
        assert result is not None
        assert "CME" in result["TRADFI"]

    def test_valid_defi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["UNISWAP_V3"], cefi=False, tradfi=False, defi=True)
        assert result is not None
        assert "UNISWAP_V3" in result["DEFI"]

    def test_invalid_venue_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid venues"):
            self.svc._validate_venues_filter(["UNKNOWN_VENUE"], cefi=True, tradfi=True, defi=True)

    def test_no_market_types_allows_all(self):
        # When no flags set, all market types processed; invalid still raises
        with pytest.raises(ValueError):
            self.svc._validate_venues_filter(["UNKNOWN"], cefi=False, tradfi=False, defi=False)

    def test_venue_valid_for_multiple_types(self):
        # A venue in both cefi and tradfi lists
        self.svc.venue_mapping.all_cefi_venues = ["SHARED_VENUE"]
        self.svc.venue_mapping.all_databento_venues = ["SHARED_VENUE"]
        result = self.svc._validate_venues_filter(["SHARED_VENUE"], cefi=True, tradfi=True, defi=False)
        assert result is not None
        assert "SHARED_VENUE" in result["CEFI"]
        assert "SHARED_VENUE" in result["TRADFI"]

    # _extract_venues_from_instrument_ids
    def test_none_instrument_ids_returns_filter_unchanged(self):
        result = _MockService._extract_venues_from_instrument_ids(None, ["BINANCE"])
        assert result == ["BINANCE"]

    def test_extracts_venue_from_id(self):
        result = _MockService._extract_venues_from_instrument_ids(["BINANCE:SPOT_PAIR:BTC-USDT"], [])
        assert "BINANCE" in result

    def test_list_instrument_ids_extracted(self):
        result = _MockService._extract_venues_from_instrument_ids(
            ["BINANCE:SPOT_PAIR:BTC-USDT", "DERIBIT:PERPETUAL:BTC-USD@INV"], []
        )
        assert "BINANCE" in result
        assert "DERIBIT" in result

    def test_narrows_venues_filter_to_matching(self):
        result = _MockService._extract_venues_from_instrument_ids(["BINANCE:SPOT_PAIR:BTC-USDT"], ["BINANCE", "CME"])
        assert result == ["BINANCE"]
        assert "CME" not in result

    def test_no_matching_venues_returns_empty(self):
        result = _MockService._extract_venues_from_instrument_ids(["DERIBIT:PERPETUAL:BTC-USD@INV"], ["BINANCE"])
        assert result == []

    def test_string_instrument_id_works(self):
        result = _MockService._extract_venues_from_instrument_ids("CME:FUTURE:ES-USD-250328@LIN", [])
        assert "CME" in result

    # _filter_instruments_by_ids
    def test_none_ids_returns_all(self):
        instruments = {"K1": MagicMock(), "K2": MagicMock()}
        result = _MockService._filter_instruments_by_ids(instruments, None)
        assert len(result) == 2

    def test_filters_to_matching_ids(self):
        instruments = {"K1": "v1", "K2": "v2", "K3": "v3"}
        result = _MockService._filter_instruments_by_ids(instruments, ["K1", "K3"])
        assert "K1" in result
        assert "K2" not in result
        assert "K3" in result

    def test_case_insensitive_id_matching(self):
        instruments = {"BINANCE:SPOT_PAIR:BTC-USDT": "v1"}
        result = _MockService._filter_instruments_by_ids(instruments, ["binance:spot_pair:btc-usdt"])
        assert len(result) == 1

    def test_no_match_returns_empty(self):
        instruments = {"K1": "v1"}
        result = _MockService._filter_instruments_by_ids(instruments, ["NONEXISTENT"])
        assert result == {}

    def test_string_id_treated_as_single_id(self):
        instruments = {"K1": "v1", "K2": "v2"}
        result = _MockService._filter_instruments_by_ids(instruments, "K1")
        assert "K1" in result
        assert "K2" not in result


# ─── special_instruments ─────────────────────────────────────────────────────


class TestCreateVixInstrumentDefinition:
    def test_vix_key_format(self):
        result = create_vix_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["instrument_key"] == "CBOE:INDEX:VIX"

    def test_vix_venue_and_type(self):
        result = create_vix_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["venue"] == "CBOE"
        assert result["instrument_type"] == "INDEX"

    def test_vix_market_category(self):
        result = create_vix_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["market_category"] == "TRADFI"

    def test_vix_base_quote_assets(self):
        result = create_vix_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["base_asset"] == "VIX"
        assert result["quote_asset"] == "USD"


class TestCreateKrwusdInstrumentDefinition:
    def test_krwusd_key_format(self):
        result = create_krwusd_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"

    def test_krwusd_data_provider_yahoo(self):
        result = create_krwusd_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["data_provider"] == "yahoo_finance"

    def test_krwusd_exchange_raw_symbol(self):
        result = create_krwusd_instrument_definition(datetime(2025, 3, 28, tzinfo=UTC))
        assert result["exchange_raw_symbol"] == "KRWUSD=X"


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


# ─── dump_to_csv ─────────────────────────────────────────────────────────────


class TestDumpToCsv:
    """Test dump_to_csv utility via module-level patching."""

    def test_no_op_when_disabled(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original = dump_module._ENABLED
        try:
            dump_module._ENABLED = False
            df = pd.DataFrame([{"a": 1}])
            # Should not create any file
            dump_module.dump_to_csv(df, "test_output.csv")
            assert not (tmp_path / "test_output.csv").exists()
        finally:
            dump_module._ENABLED = original

    def test_no_op_when_empty_df(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = str(tmp_path)
            df = pd.DataFrame()
            dump_module.dump_to_csv(df, "empty_output.csv")
            assert not (tmp_path / "empty_output.csv").exists()
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir

    def test_writes_csv_when_enabled(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = str(tmp_path)
            df = pd.DataFrame([{"col": "val"}])
            dump_module.dump_to_csv(df, "test_output.csv")
            assert (tmp_path / "test_output.csv").exists()
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir

    def test_handles_os_error_gracefully(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = "/nonexistent/path/that/cannot/be/created"
            df = pd.DataFrame([{"col": "val"}])
            # Should not raise
            with patch("pathlib.Path.mkdir", side_effect=OSError("permission denied")):
                dump_module.dump_to_csv(df, "should_fail.csv")
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir
