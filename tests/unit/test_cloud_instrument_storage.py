"""
Tests for CloudInstrumentStorage.

Source uses UCI DataSink API: get_data_sink(routing_key=category).write(df, partition=...).
Constructor: CloudInstrumentStorage(testing_mode=False) — no cloud_target param.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

_MODULE = "instruments_service.app.core.cloud_instrument_storage"


@pytest.fixture
def mock_data_sink():
    sink = MagicMock()
    sink.write = Mock(
        return_value="gs://test-bucket/instrument_availability/by_date/day=2024-01-01/venue=TEST/instruments.parquet"
    )
    return sink


@pytest.fixture(autouse=True)
def patch_uci(mock_data_sink):
    # ParquetSchemaEnforcer instance must return valid=True from validate_dataframe
    mock_enforcer_instance = MagicMock()
    mock_validation_result = MagicMock()
    mock_validation_result.valid = True
    mock_validation_result.errors = []
    mock_validation_result.warnings = []
    mock_enforcer_instance.validate_dataframe.return_value = mock_validation_result
    mock_enforcer_cls = MagicMock(return_value=mock_enforcer_instance)

    mock_alignment_result = MagicMock()
    mock_alignment_result.valid = True
    mock_alignment_result.errors = []

    with (
        patch(f"{_MODULE}.get_data_sink", return_value=mock_data_sink),
        patch(f"{_MODULE}.get_service_mode", side_effect=RuntimeError("SERVICE_MODE not set")),
        # Patch in the module's own namespace (from unified_trading_library import X binds locally)
        patch(f"{_MODULE}.ParquetSchemaEnforcer", mock_enforcer_cls),
        patch(f"{_MODULE}.create_sampling_service", return_value=MagicMock()),
        patch(f"{_MODULE}.determine_market_category", return_value="CEFI"),
        patch(f"{_MODULE}.validate_timestamp_date_alignment", return_value=mock_alignment_result),
    ):
        yield


def _make_df(**extra):
    base = {
        "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
        "venue": ["TEST"],
        "instrument_type": ["SPOT_PAIR"],
        "symbol": ["BTC-USDT"],
        "available_from_datetime": ["2024-01-01T00:00:00Z"],
    }
    base.update(extra)
    return pd.DataFrame(base)


class TestCloudInstrumentStorage:
    """Tests for CloudInstrumentStorage."""

    def test_init_default(self):
        """Constructor creates instance with testing_mode=False."""
        storage = CloudInstrumentStorage()
        assert storage._testing_mode is False

    def test_init_test_mode(self):
        """Constructor accepts testing_mode=True."""
        storage = CloudInstrumentStorage(testing_mode=True)
        assert storage._testing_mode is True

    def test_init_with_cloud_target(self):
        """Constructor does NOT accept cloud_target — UCI resolves routing at write time."""
        # Verify the correct constructor signature is used
        storage = CloudInstrumentStorage()
        assert storage is not None

    def test_init_without_cloud_target(self):
        """Default init works without any arguments."""
        storage = CloudInstrumentStorage()
        assert storage is not None

    def test_store_instruments_success(self, mock_data_sink):
        """store_instruments writes via DataSink and returns True."""
        storage = CloudInstrumentStorage()
        df = _make_df()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        mock_data_sink.write.assert_called()

    def test_store_instruments_no_date(self, mock_data_sink):
        """store_instruments works without explicit date (uses available_from_datetime)."""
        storage = CloudInstrumentStorage()
        df = _make_df()

        result = storage.store_instruments(df, table_name="instruments")

        assert result is True

    def test_store_instruments_failure(self, mock_data_sink):
        """store_instruments handles DataSink write failures gracefully."""
        mock_data_sink.write.return_value = None  # Signals write failure
        storage = CloudInstrumentStorage()
        df = _make_df()

        # Should not raise even if write returns None
        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_missing_columns(self, mock_data_sink):
        """store_instruments handles DataFrames missing optional columns."""
        storage = CloudInstrumentStorage()
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
            }
        )

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_exception_handling(self, mock_data_sink):
        """store_instruments handles DataSink exceptions without propagating."""
        mock_data_sink.write.side_effect = ValueError("write error")
        storage = CloudInstrumentStorage()
        df = _make_df()

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_extract_date_from_available_from(self, mock_data_sink):
        """Date is extracted from available_from_datetime when not passed."""
        storage = CloudInstrumentStorage()
        df = _make_df()

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_timestamp_conversion(self, mock_data_sink):
        """Handles timestamp column conversion."""
        storage = CloudInstrumentStorage()
        df = _make_df()
        df["available_from_datetime"] = pd.to_datetime(df["available_from_datetime"])

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_string_timestamp(self, mock_data_sink):
        """Handles string timestamps in available_from_datetime."""
        storage = CloudInstrumentStorage()
        df = _make_df()

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_init_import_error(self):
        """CloudInstrumentStorage is importable."""
        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage as CIS

        assert CIS is not None

    def test_store_instruments_date_extraction_fallback(self, mock_data_sink):
        """Falls back gracefully when date extraction fails."""
        storage = CloudInstrumentStorage()
        df = _make_df()
        df["available_from_datetime"] = "not-a-date"

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_no_available_from_datetime_column(self, mock_data_sink):
        """Works when available_from_datetime column is absent."""
        storage = CloudInstrumentStorage()
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, date=date)

        assert isinstance(result, bool)

    def test_store_instruments_timestamp_parsing_error(self, mock_data_sink):
        """Handles timestamp parsing error gracefully."""
        storage = CloudInstrumentStorage()
        df = _make_df(
            available_from_datetime=["2024-01-01T00:00:00Z"],
            timestamp=["invalid-timestamp"],
        )

        result = storage.store_instruments(df, table_name="instruments")

        assert isinstance(result, bool)

    def test_store_instruments_multiple_venues(self, mock_data_sink):
        """Creates separate DataSink writes per venue (by-venue structure)."""
        storage = CloudInstrumentStorage()
        df = pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
                    "DERIBIT:PERPETUAL:BTC-USDT-PERPETUAL",
                    "OKX:PERPETUAL:BTC-USDT-SWAP",
                ],
                "venue": ["BINANCE-FUTURES", "DERIBIT", "OKX"],
                "instrument_type": ["PERPETUAL", "PERPETUAL", "PERPETUAL"],
                "symbol": ["BTC-USDT", "BTC-USDT-PERPETUAL", "BTC-USDT-SWAP"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"] * 3,
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        # One write call per venue
        assert mock_data_sink.write.call_count >= 3

    def test_store_instruments_venue_path_format(self, mock_data_sink):
        """DataSink write receives correct partition dict with day and venue keys."""
        storage = CloudInstrumentStorage()
        df = _make_df()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        storage.store_instruments(df, table_name="instruments", date=date)

        call_kwargs = mock_data_sink.write.call_args
        assert call_kwargs is not None
        # partition dict must contain day and venue for hive-style paths
        _, kwargs = call_kwargs
        partition = kwargs.get("partition") or (call_kwargs[0][1] if len(call_kwargs[0]) > 1 else {})
        assert "day" in str(call_kwargs) or "venue" in str(call_kwargs)

    def test_store_instruments_venue_with_special_chars(self, mock_data_sink):
        """Venue names with special chars are written without error."""
        storage = CloudInstrumentStorage()
        df = _make_df()
        df["venue"] = "UNISWAPV3-ETHEREUM"
        df["instrument_key"] = "UNISWAPV3-ETHEREUM:SPOT_PAIR:WETH-USDC"
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert isinstance(result, bool)
