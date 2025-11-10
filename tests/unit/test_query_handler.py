"""
Comprehensive unit tests for InstrumentsQueryHandler to increase coverage to 80%+.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
from datetime import datetime
from instruments_service.cli.handlers.instruments_query_handler import (
    InstrumentsQueryHandler,
)


class TestInstrumentsQueryHandler:
    """Tests for InstrumentsQueryHandler."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock InstrumentsClient."""
        client = Mock()
        client.get_instruments_for_date = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": [
                        "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                        "DERIBIT:OPTION:BTC-USD",
                    ],
                    "venue": ["BINANCE-SPOT", "DERIBIT"],
                    "instrument_type": ["SPOT_PAIR", "OPTION"],
                    "base_asset": ["BTC", "BTC"],
                    "quote_asset": ["USDT", "USD"],
                }
            )
        )
        client.get_instruments_date_range = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["BINANCE-SPOT:SPOT_PAIR:BTC-USDT"],
                    "venue": ["BINANCE-SPOT"],
                    "instrument_type": ["SPOT_PAIR"],
                }
            )
        )
        client.get_summary_stats = Mock(
            return_value={"total_instruments": 100, "venues": 3, "instrument_types": 4}
        )
        client.get_instrument_details = Mock(
            return_value={
                "instrument_key": "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                "venue": "BINANCE-SPOT",
            }
        )
        client.get_trading_parameters = Mock(
            return_value={
                "tick_size": "0.01",
                "min_size": "0.001",
                "data_types": ["trades"],
            }
        )
        client.get_instruments_by_data_type = Mock(
            return_value=pd.DataFrame(
                {"instrument_key": ["BINANCE-SPOT:SPOT_PAIR:BTC-USDT"]}
            )
        )
        client.get_expiring_instruments = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["DERIBIT:OPTION:BTC-USD"],
                    "available_to_datetime": ["2024-12-31T00:00:00Z"],
                }
            )
        )
        return client

    @pytest.fixture
    def handler(self, mock_client):
        """Create handler with mocked client."""
        with patch(
            "instruments_service.clients.instruments_client.InstrumentsClient",
            return_value=mock_client,
        ):
            config = {"project_id": "test-project", "gcs_bucket": "test-bucket"}
            handler = InstrumentsQueryHandler(config)
            handler.client = mock_client
            return handler

    def test_init(self, mock_client):
        """Test handler initialization."""
        with patch(
            "instruments_service.clients.instruments_client.InstrumentsClient",
            return_value=mock_client,
        ):
            config = {"project_id": "test-project", "gcs_bucket": "test-bucket"}
            handler = InstrumentsQueryHandler(config)
            assert handler.client is not None

    def test_run_list_query_single_date(self, handler):
        """Test list query with single date."""
        result = handler.run("2024-01-01", query_type="list")
        assert result["status"] == "success"
        assert result["query_type"] == "list"
        assert result["date_range"] == "2024-01-01 to 2024-01-01"
        handler.client.get_instruments_for_date.assert_called_once()

    def test_run_list_query_date_range(self, handler):
        """Test list query with date range."""
        result = handler.run("2024-01-01", "2024-01-02", query_type="list")
        assert result["status"] == "success"
        assert result["query_type"] == "list"
        assert result["date_range"] == "2024-01-01 to 2024-01-02"
        handler.client.get_instruments_date_range.assert_called_once()

    def test_run_summary_query(self, handler):
        """Test summary query."""
        result = handler.run("2024-01-01", query_type="summary")
        assert result["status"] == "success"
        assert result["query_type"] == "summary"
        assert result["results"]["total_instruments"] == 100
        handler.client.get_summary_stats.assert_called_once_with("2024-01-01")

    def test_run_details_query(self, handler):
        """Test details query."""
        result = handler.run(
            "2024-01-01", query_type="details", instrument_id="TEST:SPOT_PAIR:BTC-USDT"
        )
        assert result["status"] == "success"
        assert result["query_type"] == "details"
        assert result["instrument_id"] == "TEST:SPOT_PAIR:BTC-USDT"
        handler.client.get_instrument_details.assert_called_once()

    def test_run_details_query_missing_id(self, handler):
        """Test details query without instrument_id raises error."""
        with pytest.raises(ValueError, match="instrument_id required"):
            handler.run("2024-01-01", query_type="details")

    def test_run_trading_params_query(self, handler):
        """Test trading parameters query."""
        result = handler.run(
            "2024-01-01",
            query_type="trading-params",
            instrument_id="TEST:SPOT_PAIR:BTC-USDT",
        )
        assert result["status"] == "success"
        assert result["query_type"] == "trading-params"
        handler.client.get_trading_parameters.assert_called_once()

    def test_run_trading_params_query_missing_id(self, handler):
        """Test trading params query without instrument_id raises error."""
        with pytest.raises(ValueError, match="instrument_id required"):
            handler.run("2024-01-01", query_type="trading-params")

    def test_run_data_types_query(self, handler):
        """Test data types query."""
        result = handler.run("2024-01-01", query_type="data-types", data_type="trades")
        assert result["status"] == "success"
        assert result["query_type"] == "data-types"
        assert result["data_type"] == "trades"
        handler.client.get_instruments_by_data_type.assert_called_once()

    def test_run_data_types_query_missing_type(self, handler):
        """Test data types query without data_type raises error."""
        with pytest.raises(ValueError, match="data_type required"):
            handler.run("2024-01-01", query_type="data-types")

    def test_run_expiring_query(self, handler):
        """Test expiring instruments query."""
        result = handler.run("2024-01-01", query_type="expiring", days_until_expiry=30)
        assert result["status"] == "success"
        assert result["query_type"] == "expiring"
        assert result["days_until_expiry"] == 30
        handler.client.get_expiring_instruments.assert_called_once()

    def test_run_unknown_query_type(self, handler):
        """Test unknown query type raises error."""
        with pytest.raises(ValueError, match="Unknown query_type"):
            handler.run("2024-01-01", query_type="unknown")

    def test_query_instruments_list_with_filters(self, handler):
        """Test list query with various filters."""
        result = handler._query_instruments_list(
            "2024-01-01",
            "2024-01-01",
            venues="BINANCE-SPOT",
            instrument_types="SPOT_PAIR",
            base_currency="BTC",
            quote_currency="USDT",
            symbol_pattern="BTC.*",
            instrument_ids=["TEST:SPOT_PAIR:BTC-USDT"],
        )
        assert result["status"] == "success"
        assert "filters_applied" in result
        assert result["filters_applied"]["venue"] == "BINANCE-SPOT"

    def test_query_instruments_list_venue_list(self, handler):
        """Test list query with venue as list."""
        result = handler._query_instruments_list(
            "2024-01-01", "2024-01-01", venues=["BINANCE-SPOT", "DERIBIT"]
        )
        assert result["filters_applied"]["venue"] == "BINANCE-SPOT,DERIBIT"

    def test_query_instruments_list_output_json(self, handler):
        """Test list query with JSON output format."""
        result = handler._query_instruments_list(
            "2024-01-01", "2024-01-01", output_format="json"
        )
        assert result["status"] == "success"
        assert "results" in result

    def test_query_instruments_list_output_csv(self, handler, tmp_path):
        """Test list query with CSV output format."""
        csv_file = tmp_path / "test.csv"
        result = handler._query_instruments_list(
            "2024-01-01", "2024-01-01", output_format="csv", output_file=str(csv_file)
        )
        assert result["status"] == "success"
        assert result["results"]["csv_file"] == str(csv_file)

    def test_query_instruments_list_output_summary(self, handler):
        """Test list query with summary output format."""
        result = handler._query_instruments_list(
            "2024-01-01", "2024-01-01", output_format="summary"
        )
        assert result["status"] == "success"
        assert "instruments_found" in result["results"]

    def test_query_instruments_list_empty_dataframe(self, handler):
        """Test list query with empty DataFrame."""
        handler.client.get_instruments_for_date.return_value = pd.DataFrame()
        result = handler._query_instruments_list("2024-01-01", "2024-01-01")
        assert result["status"] == "success"
        assert result["results"]["instruments_found"] == 0

    def test_query_summary_stats(self, handler):
        """Test summary stats query."""
        result = handler._query_summary_stats("2024-01-01")
        assert result["status"] == "success"
        assert result["results"]["total_instruments"] == 100

    def test_query_instrument_details_found(self, handler):
        """Test instrument details query when found."""
        result = handler._query_instrument_details(
            "2024-01-01", instrument_id="TEST:SPOT_PAIR:BTC-USDT"
        )
        assert result["status"] == "success"
        assert result["results"] is not None

    def test_query_instrument_details_not_found(self, handler):
        """Test instrument details query when not found."""
        handler.client.get_instrument_details.return_value = None
        result = handler._query_instrument_details(
            "2024-01-01", instrument_id="TEST:SPOT_PAIR:BTC-USDT"
        )
        assert result["status"] == "success"
        assert result["results"] is None

    def test_query_trading_parameters_found(self, handler):
        """Test trading parameters query when found."""
        result = handler._query_trading_parameters(
            "2024-01-01", instrument_id="TEST:SPOT_PAIR:BTC-USDT"
        )
        assert result["status"] == "success"
        assert result["results"]["tick_size"] == "0.01"

    def test_query_trading_parameters_not_found(self, handler):
        """Test trading parameters query when not found."""
        handler.client.get_trading_parameters.return_value = None
        result = handler._query_trading_parameters(
            "2024-01-01", instrument_id="TEST:SPOT_PAIR:BTC-USDT"
        )
        assert result["status"] == "success"
        assert result["results"] is None

    def test_query_by_data_type(self, handler):
        """Test query by data type."""
        result = handler._query_by_data_type(
            "2024-01-01", data_type="trades", venues="BINANCE-SPOT", limit=100
        )
        assert result["status"] == "success"
        assert result["data_type"] == "trades"
        handler.client.get_instruments_by_data_type.assert_called_once()

    def test_query_by_data_type_venue_list(self, handler):
        """Test query by data type with venue as list."""
        result = handler._query_by_data_type(
            "2024-01-01", data_type="trades", venues=["BINANCE-SPOT"]
        )
        assert result["venue"] == "BINANCE-SPOT"

    def test_query_expiring_instruments(self, handler):
        """Test expiring instruments query."""
        result = handler._query_expiring_instruments(
            "2024-01-01", days_until_expiry=30, instrument_types="OPTION"
        )
        assert result["status"] == "success"
        assert result["days_until_expiry"] == 30
        handler.client.get_expiring_instruments.assert_called_once()

    def test_query_expiring_instruments_type_list(self, handler):
        """Test expiring instruments query with instrument_types as list."""
        result = handler._query_expiring_instruments(
            "2024-01-01", instrument_types=["OPTION", "FUTURE"]
        )
        assert "results" in result

    def test_query_expiring_instruments_empty(self, handler):
        """Test expiring instruments query with empty results."""
        handler.client.get_expiring_instruments.return_value = pd.DataFrame()
        result = handler._query_expiring_instruments("2024-01-01")
        assert result["status"] == "success"
        assert result["results"]["instruments_found"] == 0
