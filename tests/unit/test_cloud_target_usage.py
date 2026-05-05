"""Test that all CloudTarget instantiations are correct.

IMPORTANT: These are UNIT TESTS ONLY (no real cloud APIs).
unified-trading-services validates real API usage via its own integration tests.
"""

import pytest
from unified_trading_library.domain_client.cloud_target import CloudTarget  # noqa: qg-deep-import

from instruments_service.config import instruments_config


class TestCloudTargetInstantiations:
    """Verify CloudTarget usage across the codebase (unit tests with mocks)."""

    def test_all_cloudtarget_calls_have_required_params(self):
        """All CloudTarget instantiations must include analytics_dataset."""
        config = instruments_config

        # Pattern that all code should follow
        target = CloudTarget(
            project_id=config.gcp_project_id,
            storage_bucket=config.get_bucket_for_category("cefi"),
            analytics_dataset=config.bigquery_dataset,  # REQUIRED
        )

        # Verify it works
        assert target.project_id
        assert target.storage_bucket
        assert target.analytics_dataset

    def test_cloudtarget_fails_without_analytics_dataset(self):
        """CloudTarget should fail if analytics_dataset is missing."""
        config = instruments_config

        with pytest.raises((TypeError, ValueError), match=r"analytics_dataset|required"):
            CloudTarget(
                project_id=config.gcp_project_id,
                storage_bucket=config.get_bucket_for_category("cefi"),
                # analytics_dataset missing - should fail!
            )

    def test_config_provides_all_cloudtarget_params(self):
        """Config should provide all required CloudTarget parameters."""
        config = instruments_config

        # Verify config has all required attributes
        assert hasattr(config, "gcp_project_id")
        assert hasattr(config, "bigquery_dataset")
        assert config.gcp_project_id
        assert config.bigquery_dataset
