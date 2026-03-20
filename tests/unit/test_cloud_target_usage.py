"""Test that cloud configuration attributes are present and valid.

CloudTarget has been removed (deprecated). These tests verify the config
provides all required cloud parameters for UCI data sink usage.
"""

from instruments_service.config import instruments_config


class TestCloudConfigParams:
    """Verify config has all required cloud parameters."""

    def test_config_provides_all_required_cloud_params(self):
        """Config should expose all attributes needed for UCI data sink."""
        config = instruments_config

        # Verify config has all required attributes
        assert hasattr(config, "gcp_project_id")
        assert hasattr(config, "bigquery_dataset")

    def test_config_cloud_params_are_non_empty(self):
        """Config attributes used for cloud access must be non-empty."""
        config = instruments_config

        assert config.gcp_project_id
        assert config.bigquery_dataset
