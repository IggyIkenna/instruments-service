"""
Tests for CloudDataProvider to increase coverage.
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime, timezone
from instruments_service.app.core.cloud_data_provider import CloudDataProvider


class TestCloudDataProvider:
    """Tests for CloudDataProvider."""

    @pytest.fixture
    def mock_cloud_service(self):
        """Create mock cloud service."""
        service = Mock()
        service.download_from_gcs = Mock(
            return_value=pd.DataFrame(
                {"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"], "venue": ["TEST"]}
            )
        )
        service.query_bigquery = Mock(
            return_value=pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        )
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
    def provider(self, mock_cloud_service, mock_cloud_target):
        """Create provider with mocked dependencies."""
        with (
            patch(
                "instruments_service.app.core.cloud_data_provider.StandardizedDomainCloudService",
                return_value=mock_cloud_service,
            ),
            patch(
                "instruments_service.app.core.cloud_data_provider.CloudTarget",
                return_value=mock_cloud_target,
            ),
        ):
            provider = CloudDataProvider(cloud_target=mock_cloud_target)
            provider.cloud_service = mock_cloud_service
            return provider

    def test_init_with_cloud_target(self, mock_cloud_service, mock_cloud_target):
        """Test initialization with cloud target."""
        with patch(
            "instruments_service.app.core.cloud_data_provider.StandardizedDomainCloudService",
            return_value=mock_cloud_service,
        ):
            provider = CloudDataProvider(cloud_target=mock_cloud_target)
            assert provider.cloud_service is not None
            assert provider.cloud_target == mock_cloud_target

    def test_init_without_cloud_target(self, mock_cloud_service):
        """Test initialization without cloud target (uses defaults)."""
        with (
            patch(
                "instruments_service.app.core.cloud_data_provider.StandardizedDomainCloudService",
                return_value=mock_cloud_service,
            ),
            patch(
                "instruments_service.app.core.cloud_data_provider.CloudTarget"
            ) as mock_target_class,
        ):
            mock_target = Mock()
            mock_target.project_id = "test-project"
            mock_target.bigquery_dataset = "test-dataset"
            mock_target_class.return_value = mock_target

            provider = CloudDataProvider()
            assert provider.cloud_service is not None

    def test_get_instruments_from_gcs_success(self, provider, mock_cloud_service):
        """Test getting instruments from GCS successfully."""
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        mock_cloud_service.download_from_gcs.assert_called()

    def test_get_instruments_from_gcs_custom_path(self, provider, mock_cloud_service):
        """Test getting instruments from GCS with custom path."""
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        custom_path = "custom/path/instruments.parquet"

        result = provider.get_instruments_from_gcs(date, gcs_path=custom_path)

        assert isinstance(result, pd.DataFrame)
        mock_cloud_service.download_from_gcs.assert_called_with(
            gcs_path=custom_path, format="parquet", log_errors=False
        )

    def test_get_instruments_from_gcs_empty(self, provider, mock_cloud_service):
        """Test getting instruments from GCS when empty."""
        mock_cloud_service.download_from_gcs.return_value = pd.DataFrame()
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_gcs_exception(self, provider, mock_cloud_service):
        """Test getting instruments from GCS with exception."""
        mock_cloud_service.download_from_gcs.side_effect = Exception("Download error")
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_bigquery_success(self, provider, mock_cloud_service):
        """Test getting instruments from BigQuery successfully."""
        result = provider.get_instruments_from_bigquery(venue="TEST", instrument_type="SPOT_PAIR")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        mock_cloud_service.query_bigquery.assert_called()

    def test_get_instruments_from_bigquery_empty(self, provider, mock_cloud_service):
        """Test getting instruments from BigQuery when empty."""
        mock_cloud_service.query_bigquery.return_value = pd.DataFrame()

        result = provider.get_instruments_from_bigquery()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_bigquery_exception(self, provider, mock_cloud_service):
        """Test getting instruments from BigQuery with exception."""
        mock_cloud_service.query_bigquery.side_effect = Exception("Query error")

        result = provider.get_instruments_from_bigquery()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_check_instruments_exist_true(self, provider, mock_cloud_service):
        """Test checking if instruments exist when they do."""
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Mock get_instruments_from_category since check_instruments_exist creates
        # new cloud services for each category, bypassing the fixture mock
        with patch.object(
            provider,
            "get_instruments_from_category",
            return_value=pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"]})
        ):
            result = provider.check_instruments_exist(date)
            assert result is True

    def test_check_instruments_exist_false(self, provider, mock_cloud_service):
        """Test checking if instruments exist when they don't."""
        mock_cloud_service.download_from_gcs.return_value = pd.DataFrame()
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = provider.check_instruments_exist(date)

        assert result is False

    def test_check_instruments_exist_exception(self, provider, mock_cloud_service):
        """Test checking if instruments exist with exception."""
        mock_cloud_service.download_from_gcs.side_effect = Exception("Download error")
        date = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = provider.check_instruments_exist(date)

        assert result is False
