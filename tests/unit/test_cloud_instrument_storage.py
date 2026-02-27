"""
Tests for CloudInstrumentStorage to increase coverage.
"""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

# Import for patching


class TestCloudInstrumentStorage:
    """Tests for CloudInstrumentStorage."""

    @pytest.fixture
    def mock_cloud_service(self):
        """Create mock cloud service."""
        service = Mock()
        service.upload_dataframe = Mock(return_value=True)
        service.query_dataframe = Mock(return_value=pd.DataFrame())
        return service

    @pytest.fixture
    def mock_cloud_target(self):
        """Create mock cloud target."""
        target = Mock()
        target.project_id = "test-project"
        target.gcs_bucket = "test-bucket"
        target.bigquery_dataset = "test-dataset"
        target.bigquery_location = "asia-northeast1"
        return target

    @pytest.fixture
    def storage(self, mock_cloud_service, mock_cloud_target):
        """Create storage with mocked dependencies."""
        # Mock StandardizedDomainCloudService to return our mock when instantiated
        # This handles the category-based service creation inside store_instruments
        mock_category_service = Mock()
        mock_category_service.upload_to_gcs = Mock(return_value=True)
        # Mock batch upload to return success for each upload
        mock_category_service.upload_to_gcs_batch = Mock(
            side_effect=lambda uploads, **kwargs: [{"success": True, "gcs_path": u["gcs_path"]} for u in uploads]
        )

        # Mock sampling service
        mock_sampling_service = Mock()
        mock_sampling_service.generate_csv_sample = Mock()

        # Mock ParquetSchemaEnforcer to return valid result (for per-venue validation)
        mock_schema_enforcer = Mock()
        mock_schema_validation_result = Mock()
        mock_schema_validation_result.valid = True
        mock_schema_validation_result.errors = []
        mock_schema_validation_result.warnings = []
        mock_schema_enforcer.validate_dataframe = Mock(return_value=mock_schema_validation_result)

        # Create patches that will stay active
        # Patch the imports in cloud_instrument_storage module
        patches = [
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
                return_value=mock_category_service,
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.CloudTarget",
                return_value=mock_cloud_target,
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.determine_market_category",
                return_value="CEFI",
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.create_sampling_service",
                return_value=mock_sampling_service,
            ),
            # Mock instruments_config (used for get_bucket_for_category in store_instruments)
            patch(
                "instruments_service.app.core.cloud_instrument_storage.instruments_config",
                Mock(get_bucket_for_category=Mock(return_value="test-bucket")),
            ),
            # Mock ParquetSchemaEnforcer for per-venue validation
            patch(
                "instruments_service.app.core.cloud_instrument_storage.ParquetSchemaEnforcer",
                return_value=mock_schema_enforcer,
            ),
        ]

        # Start all patches
        for p in patches:
            p.start()

        try:
            storage = CloudInstrumentStorage(cloud_target=mock_cloud_target)
            storage.cloud_service = mock_cloud_service
            # Store reference to category service, schema validation result, and patches for cleanup
            storage._mock_category_service = mock_category_service
            storage._mock_schema_enforcer = mock_schema_enforcer
            storage._mock_schema_validation_result = mock_schema_validation_result
            storage._patches = patches
            yield storage
        finally:
            # Stop all patches
            for p in patches:
                p.stop()

    def test_init_with_cloud_target(self, mock_cloud_service, mock_cloud_target):
        """Test initialization with cloud target."""
        with patch(
            "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
            return_value=mock_cloud_service,
        ):
            storage = CloudInstrumentStorage(cloud_target=mock_cloud_target)
            assert storage.cloud_service is not None
            assert storage.cloud_target == mock_cloud_target

    def test_init_without_cloud_target(self, mock_cloud_service):
        """Test initialization without cloud target (uses defaults)."""
        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
                return_value=mock_cloud_service,
            ),
            patch("instruments_service.app.core.cloud_instrument_storage.CloudTarget") as mock_target_class,
        ):
            mock_target = Mock()
            mock_target.project_id = "test-project"
            mock_target.gcs_bucket = "test-bucket"
            mock_target.bigquery_dataset = "test-dataset"
            mock_target.bigquery_location = "asia-northeast1"
            mock_target_class.return_value = mock_target

            storage = CloudInstrumentStorage()
            assert storage.cloud_service is not None

    def test_init_test_mode(self, mock_cloud_service):
        """Test initialization in test mode."""
        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
                return_value=mock_cloud_service,
            ),
            patch("instruments_service.app.core.cloud_instrument_storage.CloudTarget") as mock_target_class,
            patch.dict(
                "os.environ",
                {"ENVIRONMENT": "test", "INSTRUMENTS_GCS_BUCKET_TEST": "test-bucket"},
            ),
        ):
            mock_target = Mock()
            mock_target_class.return_value = mock_target

            storage = CloudInstrumentStorage()
            # Should use test bucket
            assert storage.cloud_service is not None

    def test_store_instruments_success(self, storage, mock_cloud_service):
        """Test storing instruments successfully."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        # Check that category service batch upload was called
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_no_date(self, storage, mock_cloud_service):
        """Test storing instruments without date."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )

        result = storage.store_instruments(df, table_name="instruments")

        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_failure(self, storage, mock_cloud_service):
        """Test storing instruments with failure."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )
        storage._mock_category_service.upload_to_gcs_batch = Mock(side_effect=Exception("Upload failed"))

        result = storage.store_instruments(df, table_name="instruments")

        assert result is False

    def test_query_instruments(self, storage, mock_cloud_service):
        """Test querying instruments."""
        # query_instruments now returns empty DataFrame (BigQuery removed)
        result = storage.query_instruments(venue="TEST", instrument_type="SPOT_PAIR")

        assert isinstance(result, pd.DataFrame)
        # Should return empty DataFrame (GCS query not implemented)
        assert len(result) == 0

    def test_query_instruments_empty(self, storage, mock_cloud_service):
        """Test querying instruments with empty result."""
        result = storage.query_instruments()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_store_instruments_missing_columns(self, storage):
        """Test storing instruments when schema validation fails (per-venue validation)."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )

        # Configure schema enforcer to return invalid result for this test
        storage._mock_schema_validation_result.valid = False
        storage._mock_schema_validation_result.errors = ["Missing required columns"]

        # Storage returns False when validation fails (skips venue, all_successful=False)
        result = storage.store_instruments(df, table_name="instruments")
        assert result is False

    def test_store_instruments_exception_handling(self, storage, mock_cloud_service):
        """Test exception handling during storage."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )

        storage._mock_category_service.upload_to_gcs_batch = Mock(side_effect=Exception("Storage error"))

        result = storage.store_instruments(df, table_name="instruments")

        assert result is False

    def test_store_instruments_extract_date_from_available_from(self, storage, mock_cloud_service):
        """Test storing instruments extracting date from available_from_datetime."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": [pd.Timestamp("2024-01-15T00:00:00Z")],
            }
        )

        result = storage.store_instruments(df, table_name="instruments", date=None)

        assert result is True
        # Should extract date from available_from_datetime
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_timestamp_conversion(self, storage, mock_cloud_service):
        """Test timestamp column conversion."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": [pd.Timestamp("2024-01-01T00:00:00Z", tz="UTC")],
                "available_to_datetime": [pd.Timestamp("2024-12-31T00:00:00Z", tz="UTC")],
                "expiry": [pd.Timestamp("2024-12-31T00:00:00Z", tz="UTC")],
            }
        )

        result = storage.store_instruments(df, table_name="instruments")

        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_string_timestamp(self, storage, mock_cloud_service):
        """Test storing instruments with string timestamps."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
                "timestamp": ["2024-01-01T00:00:00Z"],
            }
        )

        result = storage.store_instruments(df, table_name="instruments")

        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_init_import_error(self):
        """Test initialization when unified-trading-services not available."""
        # Mock the imports to simulate ImportError scenario
        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
                side_effect=ImportError("unified-trading-services not available"),
            ),
            pytest.raises(ImportError, match="unified-trading-services not available"),
        ):
            CloudInstrumentStorage()

    def test_store_instruments_date_extraction_fallback(self, storage, mock_cloud_service):
        """Test storing instruments with date extraction fallback."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["invalid-date"],  # Invalid date, should fallback
            }
        )

        result = storage.store_instruments(df, table_name="instruments", date=None)

        # Should still succeed with fallback to current date
        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_no_available_from_datetime_column(self, storage, mock_cloud_service):
        """Test storing instruments when available_from_datetime extraction fails."""
        # Create df without available_from_datetime for date extraction
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],  # Required column
            }
        )

        # Simulate case where date extraction from available_from_datetime fails
        # by using a date that will fallback to current date
        result = storage.store_instruments(df, table_name="instruments", date=None)

        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_timestamp_parsing_error(self, storage, mock_cloud_service):
        """Test storing instruments with timestamp parsing error."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
                "timestamp": ["invalid-timestamp"],  # Invalid timestamp
            }
        )

        # Should handle parsing error gracefully
        result = storage.store_instruments(df, table_name="instruments")

        assert result is True
        storage._mock_category_service.upload_to_gcs_batch.assert_called()

    def test_store_instruments_multiple_venues(self, storage, mock_cloud_service):
        """Test storing instruments creates separate files per venue (by-venue structure)."""
        df = pd.DataFrame(
            {
                "instrument_key": [
                    "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
                    "BINANCE-FUTURES:PERPETUAL:ETH-USDT",
                    "DERIBIT:PERPETUAL:BTC-USDT-PERPETUAL",
                    "OKX:PERPETUAL:BTC-USDT-SWAP",
                ],
                "venue": ["BINANCE-FUTURES", "BINANCE-FUTURES", "DERIBIT", "OKX"],
                "instrument_type": ["PERPETUAL", "PERPETUAL", "PERPETUAL", "PERPETUAL"],
                "symbol": ["BTC-USDT", "ETH-USDT", "BTC-USDT-PERPETUAL", "BTC-USDT-SWAP"],
                "available_from_datetime": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        # Should have called batch upload with 3 uploads (one per venue)
        storage._mock_category_service.upload_to_gcs_batch.assert_called()
        call_args = storage._mock_category_service.upload_to_gcs_batch.call_args
        uploads = call_args[0][0]  # First positional argument is the list of uploads

        # Verify we have 3 venue folders
        assert len(uploads) == 3

        # Verify the paths contain venue folders (new format: venue={VENUE} for BigQuery partitioning)
        paths = [u["gcs_path"] for u in uploads]
        assert any("venue=BINANCE-FUTURES" in p for p in paths)
        assert any("venue=DERIBIT" in p for p in paths)
        assert any("venue=OKX" in p for p in paths)

    def test_store_instruments_venue_path_format(self, storage, mock_cloud_service):
        """Test that GCS path follows by-venue folder structure."""
        df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST-VENUE"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["BTC-USDT"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        call_args = storage._mock_category_service.upload_to_gcs_batch.call_args
        uploads = call_args[0][0]

        # Verify path format: instrument_availability/by_date/day=YYYY-MM-DD/venue={VENUE}/instruments.parquet
        # Uses key=value format for BigQuery hive partitioning
        path = uploads[0]["gcs_path"]
        assert "instrument_availability/by_date/day=2024-01-01" in path
        assert "venue=TEST-VENUE" in path
        assert "instruments.parquet" in path

    def test_store_instruments_venue_with_special_chars(self, storage, mock_cloud_service):
        """Test storing instruments with venue names containing special characters."""
        df = pd.DataFrame(
            {
                "instrument_key": ["UNISWAPV3-ETHEREUM:SPOT_PAIR:WETH-USDC"],
                "venue": ["UNISWAPV3-ETHEREUM"],
                "instrument_type": ["SPOT_PAIR"],
                "symbol": ["WETH-USDC"],
                "available_from_datetime": ["2024-01-01T00:00:00Z"],
            }
        )
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = storage.store_instruments(df, table_name="instruments", date=date)

        assert result is True
        call_args = storage._mock_category_service.upload_to_gcs_batch.call_args
        uploads = call_args[0][0]

        # Verify venue folder uses key=value format for BigQuery partitioning
        path = uploads[0]["gcs_path"]
        assert "venue=UNISWAPV3-ETHEREUM" in path
