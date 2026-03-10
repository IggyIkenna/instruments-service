"""
Unit tests for engine/operations/instruments/orchestrator_helpers.py

Covers:
- normalize_venues_filter
- extract_venues_from_instrument_ids
- validate_venues
- filter_by_instrument_ids
- convert_to_dataframe
- handle_no_instruments
- add_tradfi_placeholders
- handle_utc_midnight_spanning
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
from instruments_service.models import InstrumentDefinition

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_instrument(key: str, venue: str = "BINANCE") -> InstrumentDefinition:
    return InstrumentDefinition(
        instrument_key=key,
        venue=venue,
        instrument_type="PERPETUAL",
        symbol="BTC-USDT",
        available_from_datetime="2024-01-01T00:00:00Z",
        base_asset="BTC",
        quote_asset="USDT",
        market_category="CEFI",
    )


def _make_error_counter() -> MagicMock:
    counter = MagicMock()
    counter.error_count = 2
    counter.warning_count = 3
    return counter


# ── normalize_venues_filter ───────────────────────────────────────────────────


class TestNormalizeVenuesFilter:
    def test_none_returns_empty(self) -> None:
        assert normalize_venues_filter(None) == []

    def test_string_returns_list(self) -> None:
        result = normalize_venues_filter("binance")
        assert result == ["BINANCE"]

    def test_list_uppercases(self) -> None:
        result = normalize_venues_filter(["binance", "deribit"])
        assert result == ["BINANCE", "DERIBIT"]

    def test_filters_empty_strings(self) -> None:
        result = normalize_venues_filter(["binance", "", "deribit"])
        assert result == ["BINANCE", "DERIBIT"]

    def test_single_item_list(self) -> None:
        result = normalize_venues_filter(["CME"])
        assert result == ["CME"]


# ── extract_venues_from_instrument_ids ────────────────────────────────────────


class TestExtractVenuesFromInstrumentIds:
    def test_single_id_with_colon(self) -> None:
        result = extract_venues_from_instrument_ids(["BINANCE:PERPETUAL:BTC-USDT@LIN"])
        assert "BINANCE" in result

    def test_multiple_ids(self) -> None:
        result = extract_venues_from_instrument_ids(
            [
                "BINANCE:PERPETUAL:BTC-USDT@LIN",
                "DERIBIT:OPTION:ETH-USD-1234",
            ]
        )
        assert "BINANCE" in result
        assert "DERIBIT" in result

    def test_single_string_id(self) -> None:
        result = extract_venues_from_instrument_ids("CME:FUTURE:ES-USD-250321@LIN")
        assert "CME" in result

    def test_id_without_colon(self) -> None:
        result = extract_venues_from_instrument_ids(["NOCOLON"])
        assert result == []

    def test_deduplicates_venues(self) -> None:
        result = extract_venues_from_instrument_ids(
            [
                "BINANCE:PERPETUAL:BTC-USDT@LIN",
                "BINANCE:SPOT_PAIR:ETH-USDT",
            ]
        )
        assert result.count("BINANCE") == 1


# ── validate_venues ───────────────────────────────────────────────────────────


class TestValidateVenues:
    def _make_venue_mapping(self) -> MagicMock:
        vm = MagicMock()
        vm.all_tardis_exchanges = ["BINANCE", "DERIBIT", "BYBIT"]
        vm.all_databento_venues = ["CME", "ICE", "NASDAQ"]
        vm.all_defi_venues = ["UNISWAP_V3", "AAVE_V3"]
        return vm

    def test_valid_cefi_venues(self) -> None:
        vm = self._make_venue_mapping()
        valid, tradfi = validate_venues(["BINANCE"], vm, cefi=True, tradfi=False, defi=False)
        assert "BINANCE" in valid
        assert tradfi is None

    def test_valid_tradfi_venues(self) -> None:
        vm = self._make_venue_mapping()
        valid, tradfi = validate_venues(["CME"], vm, cefi=False, tradfi=True, defi=False)
        assert "CME" in valid
        assert tradfi == ["CME"]

    def test_invalid_venue_excluded(self) -> None:
        vm = self._make_venue_mapping()
        valid, _tradfi = validate_venues(["NONEXISTENT"], vm, cefi=True, tradfi=True, defi=True)
        assert "NONEXISTENT" not in valid

    def test_mixed_valid_invalid(self) -> None:
        vm = self._make_venue_mapping()
        valid, _tradfi = validate_venues(["BINANCE", "FAKE"], vm, cefi=True, tradfi=False, defi=False)
        assert "BINANCE" in valid
        assert "FAKE" not in valid

    def test_empty_filter(self) -> None:
        vm = self._make_venue_mapping()
        valid, tradfi = validate_venues([], vm, cefi=True, tradfi=True, defi=True)
        assert valid == []
        assert tradfi is None

    def test_defi_venue(self) -> None:
        vm = self._make_venue_mapping()
        valid, tradfi = validate_venues(["UNISWAP_V3"], vm, cefi=False, tradfi=False, defi=True)
        assert "UNISWAP_V3" in valid
        assert tradfi is None


# ── filter_by_instrument_ids ─────────────────────────────────────────────────


class TestFilterByInstrumentIds:
    def test_filters_matching_keys(self) -> None:
        instruments = {
            "BINANCE:PERPETUAL:BTC-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN"),
            "BINANCE:PERPETUAL:ETH-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:ETH-USDT@LIN"),
        }
        result = filter_by_instrument_ids(instruments, ["BINANCE:PERPETUAL:BTC-USDT@LIN"])
        assert len(result) == 1
        assert "BINANCE:PERPETUAL:BTC-USDT@LIN" in result

    def test_case_insensitive(self) -> None:
        instruments = {
            "BINANCE:PERPETUAL:BTC-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN"),
        }
        result = filter_by_instrument_ids(instruments, ["binance:perpetual:btc-usdt@lin"])
        assert len(result) == 1

    def test_single_string_id(self) -> None:
        instruments = {
            "BINANCE:PERPETUAL:BTC-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN"),
        }
        result = filter_by_instrument_ids(instruments, "BINANCE:PERPETUAL:BTC-USDT@LIN")
        assert len(result) == 1

    def test_no_match_returns_empty(self) -> None:
        instruments = {
            "BINANCE:PERPETUAL:BTC-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN"),
        }
        result = filter_by_instrument_ids(instruments, ["NONEXISTENT"])
        assert result == {}

    def test_empty_instruments_returns_empty(self) -> None:
        result = filter_by_instrument_ids({}, ["BINANCE:PERPETUAL:BTC-USDT@LIN"])
        assert result == {}


# ── convert_to_dataframe ──────────────────────────────────────────────────────


class TestConvertToDataframe:
    def test_empty_instruments_returns_empty_df(self) -> None:
        df = convert_to_dataframe({}, skip_storage=False)
        assert df.empty

    def test_non_empty_instruments(self) -> None:
        instruments = {
            "BINANCE:PERPETUAL:BTC-USDT@LIN": _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN"),
        }
        df = convert_to_dataframe(instruments, skip_storage=False)
        assert len(df) == 1

    def test_skip_storage_adds_market_category(self) -> None:
        inst = _make_instrument("BINANCE:PERPETUAL:BTC-USDT@LIN")
        instruments = {"BINANCE:PERPETUAL:BTC-USDT@LIN": inst}
        df = convert_to_dataframe(instruments, skip_storage=True)
        assert len(df) == 1
        assert "market_category" in df.columns


# ── handle_no_instruments ────────────────────────────────────────────────────


class TestHandleNoInstruments:
    def test_returns_error_when_not_tradfi(self) -> None:
        counter = _make_error_counter()
        root_logger = logging.getLogger("test_root")
        root_logger.addHandler(counter)

        result = handle_no_instruments(
            date_str="2024-01-01",
            tradfi=False,
            venues_filter=[],
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result.get("status") == "error"
        assert result.get("instruments_generated") == 0

    def test_returns_empty_when_tradfi_with_venues(self) -> None:
        counter = _make_error_counter()
        root_logger = logging.getLogger("test_root2")

        result = handle_no_instruments(
            date_str="2024-01-01",
            tradfi=True,
            venues_filter=["CME"],
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result == {}

    def test_error_response_contains_counts(self) -> None:
        counter = _make_error_counter()
        root_logger = logging.getLogger("test_root3")
        root_logger.addHandler(counter)

        result = handle_no_instruments(
            date_str="2024-01-15",
            tradfi=False,
            venues_filter=[],
            error_warning_counter=counter,
            root_logger=root_logger,
        )
        assert result.get("error_count") == 2
        assert result.get("warning_count") == 3
        assert result.get("date") == "2024-01-15"


# ── add_tradfi_placeholders ──────────────────────────────────────────────────


class TestAddTradfiPlaceholders:
    def _make_venue_mapping(self) -> MagicMock:
        vm = MagicMock()
        vm.all_databento_venues = ["CME", "ICE", "NASDAQ"]
        return vm

    def test_adds_placeholder_for_missing_venue(self) -> None:
        vm = self._make_venue_mapping()
        df = pd.DataFrame()
        date = datetime(2024, 1, 1, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["CME"], vm)
        assert len(result) == 1
        assert result.iloc[0]["venue"] == "CME"
        assert result.iloc[0]["instrument_type"] == "MARKET_CLOSED"

    def test_no_placeholder_when_venue_present(self) -> None:
        vm = self._make_venue_mapping()
        df = pd.DataFrame(
            [
                {
                    "venue": "CME",
                    "instrument_key": "CME:FUTURE:ES",
                    "instrument_type": "FUTURE",
                }
            ]
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["CME"], vm)
        # No new rows added since CME already present
        assert len(result) == 1

    def test_no_placeholder_when_no_requested_tradfi(self) -> None:
        vm = self._make_venue_mapping()
        df = pd.DataFrame()
        date = datetime(2024, 1, 1, tzinfo=UTC)
        # Non-TradFi venue requested
        result = add_tradfi_placeholders(df, date, ["BINANCE"], vm)
        assert len(result) == 0

    def test_placeholder_has_correct_fields(self) -> None:
        vm = self._make_venue_mapping()
        df = pd.DataFrame()
        date = datetime(2024, 6, 15, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["CME"], vm)
        row = result.iloc[0]
        assert row["market_category"] == "TRADFI"
        assert row["data_provider"] == "databento"
        assert row["is_trading_day"] == False  # noqa: E712 — numpy.False_ does not satisfy `is False`
        assert "CME:MARKET_CLOSED:PLACEHOLDER" in str(row["instrument_key"])

    def test_multiple_venues_get_placeholders(self) -> None:
        vm = self._make_venue_mapping()
        df = pd.DataFrame()
        date = datetime(2024, 1, 1, tzinfo=UTC)
        result = add_tradfi_placeholders(df, date, ["CME", "ICE"], vm)
        assert len(result) == 2
        venues_in_result = set(result["venue"].tolist())
        assert "CME" in venues_in_result
        assert "ICE" in venues_in_result
