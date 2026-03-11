"""
Unit tests for orchestrator_helpers module.

Tests normalize_venues_filter, extract_venues_from_instrument_ids,
validate_venues, filter_by_instrument_ids, convert_to_dataframe,
handle_no_instruments, and add_tradfi_placeholders.
"""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd

from instruments_service.engine.operations.instruments.orchestrator_helpers import (
    add_tradfi_placeholders,
    convert_to_dataframe,
    extract_venues_from_instrument_ids,
    filter_by_instrument_ids,
    handle_no_instruments,
    normalize_venues_filter,
    validate_venues,
)

# ─── normalize_venues_filter ─────────────────────────────────────────────────


class TestNormalizeVenuesFilter:
    def test_none_returns_empty_list(self):
        assert normalize_venues_filter(None) == []

    def test_string_returns_uppercase_list(self):
        assert normalize_venues_filter("binance") == ["BINANCE"]

    def test_list_returns_uppercase(self):
        assert normalize_venues_filter(["binance", "bybit"]) == ["BINANCE", "BYBIT"]

    def test_list_filters_empty_strings(self):
        result = normalize_venues_filter(["binance", "", "bybit"])
        assert "" not in result
        assert "BINANCE" in result
        assert "BYBIT" in result

    def test_already_uppercase_unchanged(self):
        assert normalize_venues_filter("DERIBIT") == ["DERIBIT"]


# ─── extract_venues_from_instrument_ids ───────────────────────────────────────


class TestExtractVenuesFromInstrumentIds:
    def test_single_id_with_colon(self):
        result = extract_venues_from_instrument_ids("BINANCE:SPOT_PAIR:BTC-USDT")
        assert "BINANCE" in result

    def test_list_of_ids(self):
        result = extract_venues_from_instrument_ids(
            [
                "BINANCE:SPOT_PAIR:BTC-USDT",
                "DERIBIT:PERPETUAL:BTC-USD@INV",
            ]
        )
        assert "BINANCE" in result
        assert "DERIBIT" in result

    def test_id_without_colon_returns_empty(self):
        result = extract_venues_from_instrument_ids("BTCUSDT")
        assert result == []

    def test_deduplication(self):
        result = extract_venues_from_instrument_ids(
            [
                "BINANCE:SPOT_PAIR:BTC-USDT",
                "BINANCE:SPOT_PAIR:ETH-USDT",
            ]
        )
        assert result.count("BINANCE") == 1

    def test_string_input_treated_as_single_id(self):
        result = extract_venues_from_instrument_ids("CME:FUTURE:ES-USD-250328@LIN")
        assert "CME" in result


# ─── validate_venues ─────────────────────────────────────────────────────────


class TestValidateVenues:
    def _make_mapping(self, tardis=None, databento=None, defi=None):
        mapping = MagicMock()
        mapping.all_tardis_exchanges = tardis or ["BINANCE", "BYBIT", "DERIBIT"]
        mapping.all_databento_venues = databento or ["CME", "ICE", "NASDAQ"]
        mapping.all_defi_venues = defi or ["UNISWAP_V3", "AAVE_V3"]
        return mapping

    def test_valid_cefi_venue_passes(self):
        mapping = self._make_mapping()
        valid, tradfi = validate_venues(["BINANCE"], mapping, cefi=True, tradfi=False, defi=False)
        assert "BINANCE" in valid
        assert tradfi is None

    def test_invalid_venue_excluded(self):
        mapping = self._make_mapping()
        valid, _ = validate_venues(["UNKNOWN_VENUE"], mapping, cefi=True, tradfi=True, defi=True)
        assert "UNKNOWN_VENUE" not in valid

    def test_tradfi_only_venues_returns_tradfi_venues(self):
        mapping = self._make_mapping()
        valid, tradfi = validate_venues(["CME"], mapping, cefi=False, tradfi=True, defi=False)
        assert "CME" in valid
        assert tradfi == ["CME"]

    def test_mixed_cefi_tradfi_not_tradfi_only(self):
        mapping = self._make_mapping()
        valid, tradfi = validate_venues(["BINANCE", "CME"], mapping, cefi=True, tradfi=True, defi=False)
        assert "BINANCE" in valid
        assert "CME" in valid
        assert tradfi is None  # mixed, not tradfi-only

    def test_empty_venues_filter_returns_empty(self):
        mapping = self._make_mapping()
        valid, tradfi = validate_venues([], mapping, cefi=True, tradfi=True, defi=True)
        assert valid == []
        assert tradfi is None

    def test_defi_venue_passes_when_defi_enabled(self):
        mapping = self._make_mapping()
        valid, _ = validate_venues(["UNISWAP_V3"], mapping, cefi=False, tradfi=False, defi=True)
        assert "UNISWAP_V3" in valid


# ─── filter_by_instrument_ids ─────────────────────────────────────────────────


class TestFilterByInstrumentIds:
    def _make_instruments(self, keys: list[str]) -> dict:
        result = {}
        for key in keys:
            mock_inst = MagicMock()
            result[key] = mock_inst
        return result

    def test_exact_match(self):
        instruments = self._make_instruments(
            [
                "BINANCE:SPOT_PAIR:BTC-USDT",
                "BINANCE:SPOT_PAIR:ETH-USDT",
            ]
        )
        result = filter_by_instrument_ids(instruments, "BINANCE:SPOT_PAIR:BTC-USDT")
        assert "BINANCE:SPOT_PAIR:BTC-USDT" in result
        assert "BINANCE:SPOT_PAIR:ETH-USDT" not in result

    def test_list_of_ids(self):
        instruments = self._make_instruments(
            [
                "BINANCE:SPOT_PAIR:BTC-USDT",
                "BINANCE:SPOT_PAIR:ETH-USDT",
                "DERIBIT:PERPETUAL:BTC-USD@INV",
            ]
        )
        result = filter_by_instrument_ids(instruments, ["BINANCE:SPOT_PAIR:BTC-USDT", "DERIBIT:PERPETUAL:BTC-USD@INV"])
        assert len(result) == 2

    def test_no_match_returns_empty(self):
        instruments = self._make_instruments(["BINANCE:SPOT_PAIR:BTC-USDT"])
        result = filter_by_instrument_ids(instruments, "NONEXISTENT:KEY")
        assert result == {}

    def test_case_insensitive_match(self):
        instruments = self._make_instruments(["BINANCE:SPOT_PAIR:BTC-USDT"])
        result = filter_by_instrument_ids(instruments, "binance:spot_pair:btc-usdt")
        assert len(result) == 1


# ─── convert_to_dataframe ─────────────────────────────────────────────────────


class TestConvertToDataframe:
    def test_empty_dict_returns_empty_df(self):
        result = convert_to_dataframe({}, skip_storage=False)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_model_dump_instruments(self):
        mock_inst = MagicMock()
        mock_inst.model_dump.return_value = {
            "instrument_key": "BINANCE:SPOT_PAIR:BTC-USDT",
            "venue": "BINANCE",
            "instrument_type": "SPOT_PAIR",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "market_category": "CEFI",
        }
        result = convert_to_dataframe({"BINANCE:SPOT_PAIR:BTC-USDT": mock_inst}, skip_storage=False)
        assert not result.empty
        assert "instrument_key" in result.columns

    def test_non_pydantic_instruments(self):
        instruments = {"KEY1": {"instrument_key": "KEY1", "venue": "TEST"}}
        result = convert_to_dataframe(instruments, skip_storage=False)
        assert not result.empty


# ─── handle_no_instruments ────────────────────────────────────────────────────


class TestHandleNoInstruments:
    def _make_counter(self, errors=0, warnings=0):
        counter = MagicMock(spec=logging.Handler)
        counter.error_count = errors
        counter.warning_count = warnings
        return counter

    def test_no_tradfi_returns_error_dict(self):
        counter = self._make_counter(errors=2, warnings=1)
        root_logger = MagicMock()
        result = handle_no_instruments(
            date_str="2025-03-28",
            tradfi=False,
            venues_filter=[],
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result["status"] == "error"
        assert result["instruments_generated"] == 0
        assert result["error_count"] == 2

    def test_tradfi_with_venues_returns_empty_dict(self):
        counter = self._make_counter()
        root_logger = MagicMock()
        result = handle_no_instruments(
            date_str="2025-03-28",
            tradfi=True,
            venues_filter=["CME"],
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result == {}

    def test_tradfi_without_venues_returns_error(self):
        counter = self._make_counter(errors=1, warnings=0)
        root_logger = MagicMock()
        result = handle_no_instruments(
            date_str="2025-03-28",
            tradfi=True,
            venues_filter=[],  # empty filter -> not tradfi with venues
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result["status"] == "error"


# ─── add_tradfi_placeholders ─────────────────────────────────────────────────


class TestAddTradfiPlaceholders:
    def _make_mapping(self, databento_venues=None):
        mapping = MagicMock()
        mapping.all_databento_venues = databento_venues or ["CME", "ICE"]
        return mapping

    def test_adds_placeholder_for_missing_venue(self):
        mapping = self._make_mapping()
        instruments_df = pd.DataFrame()
        date = datetime(2025, 3, 28, tzinfo=UTC)
        result = add_tradfi_placeholders(instruments_df, date, ["CME"], mapping)
        assert not result.empty
        assert any(result["venue"] == "CME")
        assert any(result["instrument_type"] == "MARKET_CLOSED")

    def test_no_placeholder_when_venue_already_present(self):
        mapping = self._make_mapping()
        df = pd.DataFrame(
            [
                {
                    "venue": "CME",
                    "instrument_key": "CME:FUTURE:ES-USD-250328@LIN",
                    "instrument_type": "FUTURE",
                }
            ]
        )
        date = datetime(2025, 3, 28, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["CME"], mapping)
        # Should not add a placeholder since CME is present
        cme_rows = result[result["venue"] == "CME"]
        closed_rows = cme_rows[cme_rows["instrument_type"] == "MARKET_CLOSED"]
        assert len(closed_rows) == 0

    def test_no_tradfi_venues_in_filter_returns_unchanged(self):
        mapping = self._make_mapping()
        df = pd.DataFrame([{"venue": "BINANCE", "instrument_key": "BINANCE:SPOT_PAIR:BTC-USDT"}])
        date = datetime(2025, 3, 28, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["BINANCE"], mapping)
        # BINANCE is not in databento_venues so no placeholder added
        assert len(result) == len(df)

    def test_placeholder_is_false_trading_day(self):
        mapping = self._make_mapping()
        df = pd.DataFrame()
        date = datetime(2025, 1, 1, tzinfo=UTC)  # holiday
        result = add_tradfi_placeholders(df, date, ["ICE"], mapping)
        ice_rows = result[result["venue"] == "ICE"]
        # np.False_ == False but is not identical with `is`; use == instead
        assert not ice_rows.iloc[0]["is_trading_day"]
