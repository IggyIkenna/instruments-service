"""
Tests for bucket configuration and resolution.

Ensures that category-specific buckets are correctly resolved from environment variables
and configuration, preventing 'BaseServiceConfig' object has no attribute errors.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from instruments_service.settings import InstrumentsServiceConfig
from unified_cloud_services import get_bucket_for_category
from instruments_service.app.core.cloud_data_provider import CloudDataProvider


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
        # We need to patch unified_config in unified_cloud_services to use InstrumentsServiceConfig
        # or ensure it has the attributes
        
        # Create a mock config that mimics InstrumentsServiceConfig
        mock_config = MagicMock()
        # getattr matches case-sensitive, get_bucket_for_category uses uppercase
        mock_config.INSTRUMENTS_GCS_BUCKET_CEFI = "test-bucket-cefi"
        mock_config.INSTRUMENTS_GCS_BUCKET_TRADFI = "test-bucket-tradfi"
        mock_config.INSTRUMENTS_GCS_BUCKET_DEFI = "test-bucket-defi"
        mock_config.INSTRUMENTS_GCS_BUCKET = "test-bucket"
        
        with patch("unified_cloud_services.core.market_category.unified_config", mock_config):
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
        # The values should match what is in the .env file (since we are running in the real environment context here)
        # Or specifically, check that we get the *-test-* buckets
        
        actual_cefi = get_bucket_for_category("CEFI", test_mode=True)
        actual_tradfi = get_bucket_for_category("TRADFI", test_mode=True)
        actual_defi = get_bucket_for_category("DEFI", test_mode=True)
        
        print(f"\nDEBUG: CEFI Test Bucket: {actual_cefi}")
        
        assert "test" in actual_cefi, f"Expected 'test' in bucket name, got {actual_cefi}"
        assert "instruments-store" in actual_cefi
        assert "cefi" in actual_cefi.lower()

        # Specifically verify against known .env values if possible, but general structure check is safer for portability
        # Verify it matches the pattern expected from .env
        # INSTRUMENTS_GCS_BUCKET_CEFI_TEST=instruments-store-test-cefi-central-element-323112
        
        if os.getenv("INSTRUMENTS_GCS_BUCKET_CEFI_TEST"):
             assert actual_cefi == os.getenv("INSTRUMENTS_GCS_BUCKET_CEFI_TEST")

    def test_check_instruments_exist_iterates_categories(self):
        """Test that check_instruments_exist checks all categories."""
        with patch("instruments_service.app.core.cloud_data_provider.StandardizedDomainCloudService"), \
             patch.object(CloudDataProvider, "get_instruments_from_category") as mock_get_from_cat:
            
            # Mock DataFrame objects
            empty_df = MagicMock()
            empty_df.empty = True
            
            found_df = MagicMock()
            found_df.empty = False
            
            # Make it fail for first two, succeed for last
            mock_get_from_cat.side_effect = [
                empty_df,  # CEFI (empty)
                None,      # TRADFI (None/Error)
                found_df   # DEFI (Found)
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
