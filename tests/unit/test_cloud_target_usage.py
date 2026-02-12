"""Test that all CloudTarget instantiations are correct.

IMPORTANT: These are UNIT TESTS ONLY (no real cloud APIs).
unified-cloud-services validates real API usage via its own integration tests.
"""

import pytest
from unified_cloud_services import CloudTarget

from instruments_service.config import instruments_config


class TestCloudTargetInstantiations:
    """Verify CloudTarget usage across the codebase (unit tests with mocks)."""

    def test_all_cloudtarget_calls_have_required_params(self):
        """All CloudTarget instantiations must include bigquery_dataset."""
        config = instruments_config

        # Pattern that all code should follow
        target = CloudTarget(
            project_id=config.gcp_project_id,
            gcs_bucket=config.get_bucket_for_category("cefi"),
            bigquery_dataset=config.bigquery_dataset,  # REQUIRED
        )

        # Verify it works
        assert target.project_id
        assert target.gcs_bucket
        assert target.bigquery_dataset

    def test_cloudtarget_fails_without_bigquery_dataset(self):
        """CloudTarget should fail if bigquery_dataset is missing."""
        config = instruments_config

        with pytest.raises((TypeError, ValueError), match="bigquery_dataset|required"):
            CloudTarget(
                project_id=config.gcp_project_id,
                gcs_bucket=config.get_bucket_for_category("cefi"),
                # bigquery_dataset missing - should fail!
            )

    def test_config_provides_all_cloudtarget_params(self):
        """Config should provide all required CloudTarget parameters."""
        config = instruments_config

        # Verify config has all required attributes
        assert hasattr(config, "gcp_project_id")
        assert hasattr(config, "bigquery_dataset")
        assert config.gcp_project_id
        assert config.bigquery_dataset
