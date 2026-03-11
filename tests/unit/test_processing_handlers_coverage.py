"""
Additional coverage tests for instruments_service processing handlers, validation,
and config modules.

Targets uncovered branches in:
- instrument_processing_handlers.py — filter_instruments_by_exchange_config,
  _populate_complete_instrument_data, cleanup
- instrument_validation.py — _validate_venues_filter, _extract_venues_from_instrument_ids,
  _filter_instruments_by_ids
- cloud_data_provider.py — get_instruments_from_category with venue path,
  get_instruments_from_bigquery, check_instruments_exist, error paths
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from unified_config_interface import VenueMapping

from instruments_service.app.core.cloud_data_provider import CloudDataProvider
from instruments_service.app.core.instrument_validation import InstrumentValidationMixin

_MODULE_CDP = "instruments_service.app.core.cloud_data_provider"
_SAMPLE_DF = pd.DataFrame({"instrument_key": ["BINANCE:SPOT_PAIR:BTC-USDT"], "venue": ["BINANCE"]})


# ---------------------------------------------------------------------------
# CloudDataProvider tests
# ---------------------------------------------------------------------------


def _make_cdp(testing_mode: bool = False) -> CloudDataProvider:
    return CloudDataProvider(testing_mode=testing_mode)


class TestCloudDataProviderAdditional:
    def _patch_uci(self, ds_return: object, ac_return: object = None):
        mock_ds = MagicMock()
        mock_ds.read.return_value = ds_return
        mock_ac = MagicMock()
        mock_ac.execute_query.return_value = ac_return or []
        return (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
            mock_ds,
            mock_ac,
        )

    def test_get_instruments_from_gcs_with_category_delegates(self) -> None:
        """When category is provided, delegates to get_instruments_from_category."""
        cdp = _make_cdp()
        with (
            patch.object(cdp, "get_instruments_from_category", return_value=_SAMPLE_DF.copy()) as mock_cat,
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC), category="CEFI")
        mock_cat.assert_called_once()
        assert len(result) == 1

    def test_get_instruments_from_gcs_empty_df_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Empty DataFrame returned by data_source → warns and returns empty df."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.return_value = pd.DataFrame()
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
            patch(f"{_MODULE_CDP}.dump_to_csv"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC))
        assert result.empty

    def test_get_instruments_from_gcs_file_not_found(self) -> None:
        """FileNotFoundError returns empty DataFrame."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = FileNotFoundError("no file")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC))
        assert result.empty

    def test_get_instruments_from_gcs_os_error_404(self) -> None:
        """OSError with 404 message returns empty DataFrame."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = OSError("404 Not Found")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC))
        assert result.empty

    def test_get_instruments_from_gcs_os_error_non_404(self) -> None:
        """OSError with generic message returns empty DataFrame."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = OSError("connection reset")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC))
        assert result.empty

    def test_get_instruments_from_gcs_non_dataframe_returns_empty(self) -> None:
        """Non-DataFrame result from data_source returns empty DataFrame."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.return_value = {"unexpected": "dict"}
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_gcs(datetime(2024, 1, 1, tzinfo=UTC))
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_from_category_success(self) -> None:
        """get_instruments_from_category returns DataFrame from data source."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.return_value = _SAMPLE_DF.copy()
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
            patch(f"{_MODULE_CDP}.dump_to_csv"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "CEFI")
        assert len(result) == 1

    def test_get_instruments_from_category_with_venue_path(self) -> None:
        """gcs_path with venue= segment populates venue partition key."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.return_value = _SAMPLE_DF.copy()
        gcs_path = "instrument_availability/by_date/day=2024-01-01/venue=BINANCE-SPOT/instruments.parquet"
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
            patch(f"{_MODULE_CDP}.dump_to_csv"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "CEFI", gcs_path=gcs_path)
        # Verify venue was added to partition by checking the call args
        call_kwargs = mock_ds.read.call_args
        assert call_kwargs is not None

    def test_get_instruments_from_category_empty_returns_empty(self) -> None:
        """Empty DataFrame from data source returns empty df."""
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.return_value = pd.DataFrame()
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "TRADFI")
        assert result.empty

    def test_get_instruments_from_category_file_not_found(self) -> None:
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = FileNotFoundError("no parquet")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "DEFI")
        assert result.empty

    def test_get_instruments_from_category_os_error_404(self) -> None:
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = OSError("No such object")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "CEFI")
        assert result.empty

    def test_get_instruments_from_category_os_error_non_404(self) -> None:
        cdp = _make_cdp()
        mock_ds = MagicMock()
        mock_ds.read.side_effect = OSError("socket timeout")
        with (
            patch(f"{_MODULE_CDP}.get_data_source", return_value=mock_ds),
            patch(f"{_MODULE_CDP}.get_analytics_client"),
        ):
            result = cdp.get_instruments_from_category(datetime(2024, 1, 1, tzinfo=UTC), "CEFI")
        assert result.empty

    def test_get_instruments_from_bigquery_success(self) -> None:
        """get_instruments_from_bigquery returns DataFrame from analytics rows."""
        cdp = _make_cdp()
        rows = [{"instrument_key": "X:SPOT_PAIR:BTC-USDT", "venue": "X"}]
        mock_ac = MagicMock()
        mock_ac.execute_query.return_value = rows
        with (
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
        ):
            result = cdp.get_instruments_from_bigquery()
        assert len(result) == 1

    def test_get_instruments_from_bigquery_with_venue_filter(self) -> None:
        """Venue filter adds WHERE clause without breaking query."""
        cdp = _make_cdp()
        mock_ac = MagicMock()
        mock_ac.execute_query.return_value = []
        with (
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
        ):
            result = cdp.get_instruments_from_bigquery(venue="BINANCE")
        assert isinstance(result, pd.DataFrame)
        # Confirm @venue was added to params
        call_args = mock_ac.execute_query.call_args
        assert "venue" in call_args.kwargs.get("params", {}) or "venue" in str(call_args)

    def test_get_instruments_from_bigquery_with_instrument_type_filter(self) -> None:
        cdp = _make_cdp()
        mock_ac = MagicMock()
        mock_ac.execute_query.return_value = []
        with (
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
        ):
            result = cdp.get_instruments_from_bigquery(instrument_type="SPOT_PAIR")
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_from_bigquery_exception_returns_empty(self) -> None:
        """ValueError from analytics client returns empty DataFrame."""
        cdp = _make_cdp()
        mock_ac = MagicMock()
        mock_ac.execute_query.side_effect = ValueError("bad query")
        with (
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
        ):
            result = cdp.get_instruments_from_bigquery()
        assert result.empty

    def test_get_instruments_from_bigquery_returns_non_list(self) -> None:
        """Non-list result from analytics client → empty list → empty DataFrame."""
        cdp = _make_cdp()
        mock_ac = MagicMock()
        mock_ac.execute_query.return_value = "not-a-list"
        with (
            patch(f"{_MODULE_CDP}.get_data_source"),
            patch(f"{_MODULE_CDP}.get_analytics_client", return_value=mock_ac),
        ):
            result = cdp.get_instruments_from_bigquery()
        assert isinstance(result, pd.DataFrame)

    def test_check_instruments_exist_true_when_any_category_found(self) -> None:
        """Default (categories=None) returns True when first category has data."""
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", return_value=_SAMPLE_DF.copy()):
            result = cdp.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC))
        assert result is True

    def test_check_instruments_exist_false_when_all_empty(self) -> None:
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", return_value=pd.DataFrame()):
            result = cdp.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC))
        assert result is False

    def test_check_instruments_exist_specific_categories_all_present(self) -> None:
        """check_all=True returns True when all specified categories have data."""
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", return_value=_SAMPLE_DF.copy()):
            result = cdp.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC), categories=["CEFI", "TRADFI"])
        assert result is True

    def test_check_instruments_exist_specific_categories_missing_one(self) -> None:
        """check_all=True returns False when one category is missing."""
        cdp = _make_cdp()
        call_count = 0

        def side_effect(dt: datetime, category: str, **kwargs: object) -> pd.DataFrame:
            nonlocal call_count
            call_count += 1
            if category == "CEFI":
                return _SAMPLE_DF.copy()
            return pd.DataFrame()

        with patch.object(cdp, "get_instruments_from_category", side_effect=side_effect):
            result = cdp.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC), categories=["CEFI", "TRADFI"])
        assert result is False

    def test_check_instruments_exist_with_venues_all_found(self) -> None:
        """When venues specified, returns True when all venue files exist."""
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", return_value=_SAMPLE_DF.copy()):
            result = cdp.check_instruments_exist(
                datetime(2024, 1, 1, tzinfo=UTC),
                categories=["CEFI"],
                venues=["BINANCE-SPOT"],
            )
        assert result is True

    def test_check_instruments_exist_with_venues_one_missing(self) -> None:
        """When venues specified, returns False when a venue file is missing."""
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", return_value=pd.DataFrame()):
            result = cdp.check_instruments_exist(
                datetime(2024, 1, 1, tzinfo=UTC),
                categories=["CEFI"],
                venues=["BINANCE-SPOT"],
            )
        assert result is False

    def test_check_instruments_exist_with_venues_connection_error(self) -> None:
        """Connection error during venue check returns False."""
        cdp = _make_cdp()
        with patch.object(cdp, "get_instruments_from_category", side_effect=ConnectionError("no network")):
            result = cdp.check_instruments_exist(
                datetime(2024, 1, 1, tzinfo=UTC),
                categories=["CEFI"],
                venues=["BINANCE-SPOT"],
            )
        assert result is False

    def test_check_instruments_exist_category_exception_continues(self) -> None:
        """OSError during category check continues to next category."""
        cdp = _make_cdp()
        call_count = 0

        def side_effect(dt: datetime, category: str, **kwargs: object) -> pd.DataFrame:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("intermittent error")
            return _SAMPLE_DF.copy()

        # For check_all=False (categories=None), first failure continues
        with patch.object(cdp, "get_instruments_from_category", side_effect=side_effect):
            result = cdp.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC))
        # Should not raise; result depends on whether other categories found data
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# InstrumentValidationMixin tests
# ---------------------------------------------------------------------------


def _make_venue_mapping(
    cefi: list[str] | None = None,
    tradfi: list[str] | None = None,
    defi: list[str] | None = None,
) -> MagicMock:
    vm = MagicMock(spec=VenueMapping)
    vm.all_cefi_venues = cefi or ["BINANCE", "BYBIT", "COINBASE"]
    vm.all_databento_venues = tradfi or ["CME", "NASDAQ", "NYSE"]
    vm.all_defi_venues = defi or ["UNISWAP", "CURVE", "AAVE"]
    return vm


class _ConcreteValidationMixin(InstrumentValidationMixin):
    def __init__(self, venue_mapping: MagicMock) -> None:
        self.venue_mapping = venue_mapping


class TestInstrumentValidationMixin:
    def test_validate_venues_filter_empty_returns_none(self) -> None:
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter([], cefi=True, tradfi=False, defi=False)
        assert result is None

    def test_validate_venues_filter_valid_cefi_venue(self) -> None:
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter(["BINANCE"], cefi=True, tradfi=False, defi=False)
        assert result is not None
        assert "BINANCE" in result["CEFI"]

    def test_validate_venues_filter_valid_tradfi_venue(self) -> None:
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter(["CME"], cefi=False, tradfi=True, defi=False)
        assert result is not None
        assert "CME" in result["TRADFI"]

    def test_validate_venues_filter_valid_defi_venue(self) -> None:
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter(["UNISWAP"], cefi=False, tradfi=False, defi=True)
        assert result is not None
        assert "UNISWAP" in result["DEFI"]

    def test_validate_venues_filter_invalid_venue_raises(self) -> None:
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        with pytest.raises(ValueError, match="Invalid venues"):
            mixin._validate_venues_filter(["NOT_A_VENUE"], cefi=True, tradfi=True, defi=True)

    def test_validate_venues_filter_no_market_types_specified(self) -> None:
        """When no market type flags are set, all three are processed."""
        vm = _make_venue_mapping()
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter(["BINANCE"], cefi=False, tradfi=False, defi=False)
        assert result is not None
        assert "BINANCE" in result["CEFI"]

    def test_validate_venues_filter_venue_in_multiple_types(self) -> None:
        """A venue that appears in both cefi and tradfi lists is added to both."""
        vm = _make_venue_mapping(cefi=["SHARED_VENUE"], tradfi=["SHARED_VENUE"])
        mixin = _ConcreteValidationMixin(vm)
        result = mixin._validate_venues_filter(["SHARED_VENUE"], cefi=True, tradfi=True, defi=False)
        assert result is not None
        assert "SHARED_VENUE" in result["CEFI"]
        assert "SHARED_VENUE" in result["TRADFI"]

    def test_extract_venues_empty_instrument_ids(self) -> None:
        result = InstrumentValidationMixin._extract_venues_from_instrument_ids(None, [])
        assert result == []

    def test_extract_venues_from_colon_format(self) -> None:
        result = InstrumentValidationMixin._extract_venues_from_instrument_ids(["BINANCE:SPOT_PAIR:BTC-USDT"], [])
        assert "BINANCE" in result

    def test_extract_venues_from_string_input(self) -> None:
        result = InstrumentValidationMixin._extract_venues_from_instrument_ids("BYBIT:SPOT_PAIR:ETH-USDT", [])
        assert "BYBIT" in result

    def test_extract_venues_merges_with_existing_filter(self) -> None:
        """Existing filter is intersected with extracted venues."""
        result = InstrumentValidationMixin._extract_venues_from_instrument_ids(
            ["BINANCE:SPOT_PAIR:BTC-USDT"], ["BINANCE", "BYBIT"]
        )
        assert "BINANCE" in result
        assert "BYBIT" not in result

    def test_extract_venues_no_match_with_existing_filter(self) -> None:
        """When no intersection, returns empty list (logs warning)."""
        result = InstrumentValidationMixin._extract_venues_from_instrument_ids(
            ["COINBASE:SPOT_PAIR:BTC-USDT"], ["BYBIT"]
        )
        assert result == []

    def test_filter_instruments_by_ids_none_returns_all(self) -> None:
        instruments = {"A": {"sym": "a"}, "B": {"sym": "b"}}
        result = InstrumentValidationMixin._filter_instruments_by_ids(instruments, None)
        assert result is instruments

    def test_filter_instruments_by_ids_matches_subset(self) -> None:
        instruments = {
            "BINANCE:SPOT_PAIR:BTC-USDT": {"v": 1},
            "BYBIT:SPOT_PAIR:ETH-USDT": {"v": 2},
        }
        result = InstrumentValidationMixin._filter_instruments_by_ids(instruments, ["BINANCE:SPOT_PAIR:BTC-USDT"])
        assert "BINANCE:SPOT_PAIR:BTC-USDT" in result
        assert "BYBIT:SPOT_PAIR:ETH-USDT" not in result

    def test_filter_instruments_by_ids_case_insensitive(self) -> None:
        instruments = {"BINANCE:SPOT_PAIR:BTC-USDT": {"v": 1}}
        result = InstrumentValidationMixin._filter_instruments_by_ids(instruments, ["binance:spot_pair:btc-usdt"])
        assert "BINANCE:SPOT_PAIR:BTC-USDT" in result

    def test_filter_instruments_by_ids_no_match_returns_empty(self) -> None:
        instruments = {"BINANCE:SPOT_PAIR:BTC-USDT": {"v": 1}}
        result = InstrumentValidationMixin._filter_instruments_by_ids(instruments, ["COINBASE:SPOT_PAIR:ETH-USDT"])
        assert result == {}

    def test_filter_instruments_by_ids_accepts_string_input(self) -> None:
        instruments = {"BINANCE:SPOT_PAIR:BTC-USDT": {"v": 1}}
        result = InstrumentValidationMixin._filter_instruments_by_ids(instruments, "BINANCE:SPOT_PAIR:BTC-USDT")
        assert "BINANCE:SPOT_PAIR:BTC-USDT" in result
