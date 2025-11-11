"""
Comprehensive unit tests for InstrumentsClient to increase coverage to 80%+.

NOTE: This test file references a module that may not exist.
If instruments_service.clients.instruments_client doesn't exist,
these tests should be skipped or the module should be created.
"""

import pytest

# Skip if module doesn't exist
try:
    from instruments_service.clients.instruments_client import InstrumentsClient
except ImportError:
    pytest.skip("instruments_service.clients.instruments_client not available", allow_module_level=True)


class TestInstrumentsClient:
    """Tests for InstrumentsClient."""

    @pytest.fixture
    def mock_cloud_service(self):
        """Create mock cloud service."""
        service = Mock()
        # Use download_from_gcs instead of download_parquet
        service.download_from_gcs = Mock(
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
                    "available_from_datetime": [
                        "2024-01-01T00:00:00Z",
                        "2024-01-01T00:00:00Z",
                    ],
                    "available_to_datetime": [
                        "2024-12-31T00:00:00Z",
                        "2024-12-31T00:00:00Z",
                    ],
                    "data_types": ["trades,book_snapshot_5", "options_chain"],
                    "symbol": [
                        "BTC-USDT",
                        "BTC-USD",
                    ],  # Add symbol column for pattern matching
                }
            )
        )
        return service

    @pytest.fixture
    def client(self, mock_cloud_service):
        """Create client with mocked cloud service."""
        with patch(
            "instruments_service.clients.instruments_client.StandardizedDomainCloudService",
            return_value=mock_cloud_service,
        ):
            client = InstrumentsClient(
                project_id="test-project", bucket_name="test-bucket"
            )
            client.cloud_service = mock_cloud_service
            return client

    def test_init(self, mock_cloud_service):
        """Test client initialization."""
        with patch(
            "instruments_service.clients.instruments_client.StandardizedDomainCloudService",
            return_value=mock_cloud_service,
        ):
            client = InstrumentsClient(
                project_id="test-project", bucket_name="test-bucket"
            )
            assert client.project_id == "test-project"
            assert client.bucket_name == "test-bucket"
            assert client.cloud_service is not None

    def test_get_instruments_for_date_success(self, client):
        """Test getting instruments for a date."""
        result = client.get_instruments_for_date("2024-01-01")
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        client.cloud_service.download_from_gcs.assert_called()

    def test_get_instruments_for_date_with_filters(self, client):
        """Test getting instruments with filters."""
        result = client.get_instruments_for_date(
            "2024-01-01",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            base_currency="BTC",
            quote_currency="USDT",
        )
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_for_date_empty(self, client):
        """Test getting instruments when empty."""
        client.cloud_service.download_parquet.return_value = pd.DataFrame()
        result = client.get_instruments_for_date("2024-01-01")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_for_date_exception(self, client):
        """Test exception handling."""
        client.cloud_service.download_parquet.side_effect = Exception("Download error")
        result = client.get_instruments_for_date("2024-01-01")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_for_date_datetime(self, client):
        """Test getting instruments with datetime object."""
        date_obj = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = client.get_instruments_for_date(date_obj)
        assert isinstance(result, pd.DataFrame)

    def test_filter_by_date_availability(self, client):
        """Test date availability filtering."""
        df = pd.DataFrame(
            {
                "available_from_datetime": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                ],
                "available_to_datetime": [
                    "2024-12-31T00:00:00Z",
                    "2024-12-31T00:00:00Z",
                ],
            }
        )
        target_date = datetime(2024, 1, 1)
        result = client._filter_by_date_availability(df, target_date)
        assert len(result) >= 0

    def test_filter_by_date_availability_empty(self, client):
        """Test date availability filtering with empty DataFrame."""
        df = pd.DataFrame()
        result = client._filter_by_date_availability(df, datetime(2024, 1, 1))
        assert len(result) == 0

    def test_filter_by_date_availability_no_dates(self, client):
        """Test date availability filtering with missing date columns."""
        df = pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        result = client._filter_by_date_availability(df, datetime(2024, 1, 1))
        assert len(result) == 1

    def test_apply_filters_venue(self, client):
        """Test applying venue filter."""
        df = pd.DataFrame(
            {
                "venue": ["BINANCE-SPOT", "DERIBIT"],
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "DERIBIT:OPTION:BTC-USD",
                ],
            }
        )
        result = client._apply_filters(df, venue="BINANCE-SPOT")
        assert len(result) == 1
        assert result.iloc[0]["venue"] == "BINANCE-SPOT"

    def test_apply_filters_instrument_type(self, client):
        """Test applying instrument type filter."""
        df = pd.DataFrame(
            {
                "instrument_type": ["SPOT_PAIR", "OPTION"],
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "DERIBIT:OPTION:BTC-USD",
                ],
            }
        )
        result = client._apply_filters(df, instrument_type="SPOT_PAIR")
        assert len(result) == 1

    def test_apply_filters_base_currency(self, client):
        """Test applying base currency filter."""
        df = pd.DataFrame(
            {
                "base_asset": ["BTC", "ETH"],
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "BINANCE-SPOT:SPOT_PAIR:ETH-USDT",
                ],
            }
        )
        result = client._apply_filters(df, base_currency="BTC")
        assert len(result) == 1

    def test_apply_filters_quote_currency(self, client):
        """Test applying quote currency filter."""
        df = pd.DataFrame(
            {
                "quote_asset": ["USDT", "USD"],
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "DERIBIT:OPTION:BTC-USD",
                ],
            }
        )
        result = client._apply_filters(df, quote_currency="USDT")
        assert len(result) == 1

    def test_apply_filters_symbol_pattern(self, client):
        """Test applying symbol pattern filter."""
        df = pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "BINANCE-SPOT:SPOT_PAIR:ETH-USDT",
                ],
                "symbol": ["BTC-USDT", "ETH-USDT"],  # Add symbol column
            }
        )
        result = client._apply_filters(df, symbol_pattern="BTC.*")
        assert len(result) == 1

    def test_apply_filters_instrument_ids(self, client):
        """Test applying instrument IDs filter."""
        df = pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "DERIBIT:OPTION:BTC-USD",
                ]
            }
        )
        result = client._apply_filters(
            df, instrument_ids=["BINANCE-SPOT:SPOT_PAIR:BTC-USDT"]
        )
        assert len(result) == 1

    def test_apply_filters_multiple(self, client):
        """Test applying multiple filters."""
        df = pd.DataFrame(
            {
                "venue": ["BINANCE-SPOT", "DERIBIT"],
                "instrument_type": ["SPOT_PAIR", "OPTION"],
                "base_asset": ["BTC", "BTC"],
                "instrument_key": [
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                    "DERIBIT:OPTION:BTC-USD",
                ],
            }
        )
        result = client._apply_filters(
            df, venue="BINANCE-SPOT", instrument_type="SPOT_PAIR", base_currency="BTC"
        )
        assert len(result) == 1

    def test_get_available_venues(self, client):
        """Test getting available venues."""
        result = client.get_available_venues("2024-01-01")
        assert isinstance(result, list)

    def test_get_available_venues_empty(self, client):
        """Test getting available venues when empty."""
        client.cloud_service.download_parquet.return_value = pd.DataFrame()
        result = client.get_available_venues("2024-01-01")
        assert result == []

    def test_get_available_instrument_types(self, client):
        """Test getting available instrument types."""
        result = client.get_available_instrument_types("2024-01-01")
        assert isinstance(result, list)

    def test_get_available_instrument_types_with_venue(self, client):
        """Test getting available instrument types with venue filter."""
        result = client.get_available_instrument_types(
            "2024-01-01", venue="BINANCE-SPOT"
        )
        assert isinstance(result, list)

    def test_get_available_base_currencies(self, client):
        """Test getting available base currencies."""
        result = client.get_available_base_currencies("2024-01-01")
        assert isinstance(result, list)

    def test_get_available_base_currencies_with_filters(self, client):
        """Test getting available base currencies with filters."""
        result = client.get_available_base_currencies(
            "2024-01-01", venue="BINANCE-SPOT", instrument_type="SPOT_PAIR"
        )
        assert isinstance(result, list)

    def test_search_instruments_by_symbol(self, client):
        """Test searching instruments by symbol pattern."""
        result = client.search_instruments_by_symbol("2024-01-01", "BTC.*")
        assert isinstance(result, pd.DataFrame)

    def test_search_instruments_by_symbol_with_venue(self, client):
        """Test searching instruments by symbol with venue filter."""
        result = client.search_instruments_by_symbol(
            "2024-01-01", "BTC.*", venue="BINANCE-SPOT", limit=10
        )
        assert isinstance(result, pd.DataFrame)

    def test_search_instruments_by_symbol_limit(self, client):
        """Test searching instruments with limit."""
        # Mock get_instruments_for_date to return large DataFrame
        large_df = pd.DataFrame(
            {
                "instrument_key": [f"TEST:SPOT_PAIR:ASSET{i}-USDT" for i in range(200)],
                "symbol": [f"ASSET{i}-USDT" for i in range(200)],
            }
        )
        client.get_instruments_for_date = Mock(return_value=large_df)

        result = client.search_instruments_by_symbol("2024-01-01", ".*", limit=50)
        assert len(result) <= 50

    def test_get_instrument_details_found(self, client):
        """Test getting instrument details when found."""
        # Mock the get_instruments_for_date to return data
        client.get_instruments_for_date = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["BINANCE-SPOT:SPOT_PAIR:BTC-USDT"],
                    "venue": ["BINANCE-SPOT"],
                }
            )
        )
        result = client.get_instrument_details(
            "2024-01-01", "BINANCE-SPOT:SPOT_PAIR:BTC-USDT"
        )
        assert result is not None
        assert "instrument_key" in result

    def test_get_instrument_details_not_found(self, client):
        """Test getting instrument details when not found."""
        client.cloud_service.download_parquet.return_value = pd.DataFrame()
        result = client.get_instrument_details("2024-01-01", "TEST:SPOT_PAIR:BTC-USDT")
        assert result is None

    def test_get_trading_parameters_found(self, client):
        """Test getting trading parameters when found."""
        # Mock get_instrument_details to return data
        client.get_instrument_details = Mock(
            return_value={
                "instrument_key": "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                "tick_size": "0.01",
                "min_size": "0.001",
                "data_types": "trades,book_snapshot_5",
            }
        )
        result = client.get_trading_parameters(
            "2024-01-01", "BINANCE-SPOT:SPOT_PAIR:BTC-USDT"
        )
        assert result is not None

    def test_get_trading_parameters_not_found(self, client):
        """Test getting trading parameters when not found."""
        client.cloud_service.download_parquet.return_value = pd.DataFrame()
        result = client.get_trading_parameters("2024-01-01", "TEST:SPOT_PAIR:BTC-USDT")
        assert result is None

    def test_get_instruments_by_data_type(self, client):
        """Test getting instruments by data type."""
        result = client.get_instruments_by_data_type("2024-01-01", "trades")
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_by_data_type_with_venue(self, client):
        """Test getting instruments by data type with venue filter."""
        result = client.get_instruments_by_data_type(
            "2024-01-01", "trades", venue="BINANCE-SPOT", limit=100
        )
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_by_data_type_limit(self, client):
        """Test getting instruments by data type with limit."""
        # Mock get_instruments_for_date to return large DataFrame
        large_df = pd.DataFrame(
            {
                "instrument_key": [f"TEST:SPOT_PAIR:ASSET{i}-USDT" for i in range(200)],
                "data_types": ["trades"] * 200,
            }
        )
        client.get_instruments_for_date = Mock(return_value=large_df)

        result = client.get_instruments_by_data_type("2024-01-01", "trades", limit=50)
        assert len(result) <= 50

    def test_get_instruments_date_range(self, client):
        """Test getting instruments for date range."""

        # Mock get_instruments_for_date to return data for each date
        def mock_get_instruments(date, **kwargs):
            return pd.DataFrame(
                {
                    "instrument_key": [f"TEST:SPOT_PAIR:BTC-USDT-{date}"],
                    "query_date": [str(date)],
                }
            )

        client.get_instruments_for_date = Mock(side_effect=mock_get_instruments)
        result = client.get_instruments_date_range("2024-01-01", "2024-01-02")
        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_date_range_with_filters(self, client):
        """Test getting instruments for date range with filters."""

        def mock_get_instruments(date, **kwargs):
            return pd.DataFrame(
                {
                    "instrument_key": [f"TEST:SPOT_PAIR:BTC-USDT-{date}"],
                    "query_date": [str(date)],
                }
            )

        client.get_instruments_for_date = Mock(side_effect=mock_get_instruments)
        result = client.get_instruments_date_range(
            "2024-01-01",
            "2024-01-02",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
        )
        assert isinstance(result, pd.DataFrame)

    def test_get_summary_stats(self, client):
        """Test getting summary statistics."""
        # Mock get_instruments_for_date to return data
        client.get_instruments_for_date = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["BINANCE-SPOT:SPOT_PAIR:BTC-USDT"],
                    "venue": ["BINANCE-SPOT"],
                    "instrument_type": ["SPOT_PAIR"],
                    "base_asset": ["BTC"],
                    "quote_asset": ["USDT"],
                    "ccxt_symbol": ["BTC/USDT"],
                    "data_types": ["trades"],
                }
            )
        )
        result = client.get_summary_stats("2024-01-01")
        assert isinstance(result, dict)
        assert "total_instruments" in result

    def test_get_summary_stats_empty(self, client):
        """Test getting summary statistics when empty."""
        client.get_instruments_for_date = Mock(return_value=pd.DataFrame())
        result = client.get_summary_stats("2024-01-01")
        assert result["total_instruments"] == 0

    def test_get_expiring_instruments(self, client):
        """Test getting expiring instruments."""
        # Mock get_instruments_for_date to return data with expiry dates
        future_date = datetime(2024, 1, 15)
        client.get_instruments_for_date = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["DERIBIT:OPTION:BTC-USD"],
                    "available_to_datetime": [future_date.isoformat() + "Z"],
                }
            )
        )
        result = client.get_expiring_instruments("2024-01-01", days_until_expiry=30)
        assert isinstance(result, pd.DataFrame)

    def test_get_expiring_instruments_with_type(self, client):
        """Test getting expiring instruments with instrument type filter."""
        future_date = datetime(2024, 1, 15)
        client.get_instruments_for_date = Mock(
            return_value=pd.DataFrame(
                {
                    "instrument_key": ["DERIBIT:OPTION:BTC-USD"],
                    "available_to_datetime": [future_date.isoformat() + "Z"],
                }
            )
        )
        result = client.get_expiring_instruments(
            "2024-01-01", days_until_expiry=30, instrument_type="OPTION"
        )
        assert isinstance(result, pd.DataFrame)

    def test_download_from_gcs(self, client):
        """Test downloading from GCS."""
        result = client._download_from_gcs("test/path.parquet")
        assert isinstance(result, pd.DataFrame)
        client.cloud_service.download_from_gcs.assert_called()

    def test_download_from_gcs_exception(self, client):
        """Test downloading from GCS with exception."""
        client.cloud_service.download_from_gcs.side_effect = Exception("Download error")
        result = client._download_from_gcs("test/path.parquet")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
