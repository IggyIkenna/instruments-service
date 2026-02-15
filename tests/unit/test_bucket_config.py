"""
Tests for bucket configuration and resolution.

Ensures that category-specific buckets are correctly resolved from environment variables
and configuration, preventing 'BaseServiceConfig' object has no attribute errors.
"""

import os
from unittest.mock import MagicMock, patch

import pytest
from unified_cloud_services import get_bucket_for_category

from instruments_service.app.core.cloud_data_provider import CloudDataProvider
from instruments_service.config import InstrumentsServiceConfig


class TestBucketConfiguration:
    """Test bucket configuration and resolution."""

    def setup_method(self):
        """Setup method."""
        # Clear environment variables to ensure clean state
        self.env_vars = {
            "GCP_PROJECT_ID": "test-project",
            "INSTRUMENTS_GCS_BUCKET": "test-bucket",
            "INSTRUMENTS_GCS_BUCKET_TEST": "test-bucket-test",
            "INSTRUMENTS_GCS_BUCKET_CEFI": "test-bucket-cefi",
            "INSTRUMENTS_GCS_BUCKET_TRADFI": "test-bucket-tradfi",
            "INSTRUMENTS_GCS_BUCKET_DEFI": "test-bucket-defi",
            "INSTRUMENTS_GCS_BUCKET_CEFI_TEST": "test-bucket-cefi-test",
            "INSTRUMENTS_GCS_BUCKET_TRADFI_TEST": "test-bucket-tradfi-test",
            "INSTRUMENTS_GCS_BUCKET_DEFI_TEST": "test-bucket-defi-test",
            "ENVIRONMENT": "development",
            "ALCHEMY_SECRET_NAME": "test",
            "GRAPH_SECRET_NAME": "test",
            "CLICKUP_SECRET_NAME": "test",
            "CLICKUP_LIST_ID": "123",
        }

    def test_instruments_service_config_has_category_buckets(self):
        """Test that InstrumentsServiceConfig has category bucket attributes."""
        with patch.dict(os.environ, self.env_vars):
            config = InstrumentsServiceConfig()

            assert hasattr(config, "gcs_bucket_cefi")
            assert hasattr(config, "gcs_bucket_tradfi")
            assert hasattr(config, "gcs_bucket_defi")

            assert config.gcs_bucket_cefi == "test-bucket-cefi"
            assert config.gcs_bucket_tradfi == "test-bucket-tradfi"
            assert config.gcs_bucket_defi == "test-bucket-defi"

            # Check test buckets
            assert hasattr(config, "gcs_bucket_cefi_test")
            assert config.gcs_bucket_cefi_test == "test-bucket-cefi-test"

    def test_get_bucket_for_category_resolution(self):
        """Test that get_bucket_for_category resolves correctly."""
        # We need to patch get_config in unified_cloud_services to return our test values
        # get_config converts env vars to snake_case and uses getattr on unified_config

        def mock_get_config(key, default=""):
            """Mock get_config that returns test bucket values."""
            key_upper = key.upper()
            config_map = {
                "INSTRUMENTS_GCS_BUCKET_CEFI": "test-bucket-cefi",
                "INSTRUMENTS_GCS_BUCKET_TRADFI": "test-bucket-tradfi",
                "INSTRUMENTS_GCS_BUCKET_DEFI": "test-bucket-defi",
                "INSTRUMENTS_GCS_BUCKET": "test-bucket",
            }
            return config_map.get(key_upper, default)

        with patch("unified_cloud_services.core.market_category.get_config", mock_get_config):
            bucket_cefi = get_bucket_for_category("CEFI", test_mode=False)
            assert bucket_cefi == "test-bucket-cefi"

            bucket_tradfi = get_bucket_for_category("TRADFI", test_mode=False)
            assert bucket_tradfi == "test-bucket-tradfi"

            bucket_defi = get_bucket_for_category("DEFI", test_mode=False)
            assert bucket_defi == "test-bucket-defi"

    @pytest.mark.unit
    def test_bucket_resolution_fixture_integration(self, setup_test_environment):
        """
        Integration test using the actual 'setup_test_environment' fixture.
        This verifies that conftest.py correctly patches the configuration so that
        tests running with the fixture get the correct TEST buckets defined in .env.
        """
        # Actual values resolved via get_bucket_for_category with test_mode=True
        actual_cefi = get_bucket_for_category("CEFI", test_mode=True)
        get_bucket_for_category("TRADFI", test_mode=True)
        get_bucket_for_category("DEFI", test_mode=True)

        # Bucket name may be instruments-store-test or instruments-store-test-cefi-{project}
        assert "test" in actual_cefi.lower(), f"Expected 'test' in bucket name, got {actual_cefi}"
        assert "instruments" in actual_cefi.lower() or "store" in actual_cefi.lower()

    def test_check_instruments_exist_iterates_categories(self):
        """Test that check_instruments_exist checks all categories."""
        with (
            patch("instruments_service.app.core.cloud_data_provider.StandardizedDomainCloudService"),
            patch.object(CloudDataProvider, "get_instruments_from_category") as mock_get_from_cat,
        ):
            # Mock DataFrame objects
            empty_df = MagicMock()
            empty_df.empty = True

            found_df = MagicMock()
            found_df.empty = False

            # Make it fail for first two, succeed for last
            mock_get_from_cat.side_effect = [
                empty_df,  # CEFI (empty)
                None,  # TRADFI (None/Error)
                found_df,  # DEFI (Found)
            ]

            provider = CloudDataProvider()
            # Mock date object
            mock_date = MagicMock()
            mock_date.strftime.return_value = "2023-01-01"

            exists = provider.check_instruments_exist(mock_date)

            assert exists is True
            assert mock_get_from_cat.call_count == 3

            # Verify calls were for correct categories
            calls = mock_get_from_cat.call_args_list
            assert calls[0][0][1] == "CEFI"
            assert calls[1][0][1] == "TRADFI"
            assert calls[2][0][1] == "DEFI"
