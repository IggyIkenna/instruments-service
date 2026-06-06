"""Test that all CloudTarget instantiations are correct.

IMPORTANT: These are UNIT TESTS ONLY (no real cloud APIs).
unified-trading-services validates real API usage via its own integration tests.

The canonical bucket helper is ``orchestrator._get_instruments_bucket(asset_group)``
which delegates to UTL ``resolve_bucket_name`` (bucket-name SSOT).
"""

from unittest.mock import patch

import pytest
from unified_trading_library.domain_client.cloud_target import CloudTarget  # noqa: qg-deep-import

from instruments_service.config import instruments_config
from instruments_service.engine.orchestrator import _get_instruments_bucket


class TestCloudTargetInstantiations:
    """Verify CloudTarget usage across the codebase (unit tests with mocks)."""

    def test_all_cloudtarget_calls_have_required_params(self):
        """All CloudTarget instantiations must include analytics_dataset."""
        config = instruments_config

        with patch(
            "instruments_service.engine.orchestrator.resolve_bucket_name",
            return_value="instruments-store-cefi-prd-test-project",
        ):
            target = CloudTarget(
                project_id=config.gcp_project_id,
                storage_bucket=_get_instruments_bucket("CEFI"),
                analytics_dataset=config.bigquery_dataset,  # REQUIRED
            )

        assert target.project_id
        assert target.storage_bucket
        assert target.analytics_dataset

    def test_cloudtarget_fails_without_analytics_dataset(self):
        """CloudTarget should fail if analytics_dataset is missing."""
        config = instruments_config

        with (
            patch(
                "instruments_service.engine.orchestrator.resolve_bucket_name",
                return_value="instruments-store-cefi-prd-test-project",
            ),
            pytest.raises((TypeError, ValueError), match=r"analytics_dataset|required"),
        ):
            CloudTarget(
                project_id=config.gcp_project_id,
                storage_bucket=_get_instruments_bucket("CEFI"),
                # analytics_dataset missing - should fail!
            )

    def test_config_provides_all_cloudtarget_params(self):
        """Config should provide all required CloudTarget parameters."""
        config = instruments_config

        assert hasattr(config, "gcp_project_id")
        assert hasattr(config, "bigquery_dataset")
        assert config.gcp_project_id
        assert config.bigquery_dataset


class TestInstrumentsBucketAssetGroupCasing:
    """Regression: `_get_instruments_bucket` must lowercase the asset_group before
    handing it to `resolve_bucket_name`, which is strict-lowercase since the Option B
    bucket-SSOT migration (library@6c8a1175). Uppercase raised
    `BucketNamingError: Unknown asset_group 'SPORTS'` on every sports/footystats
    short-circuit run (footystats-fwd exit=1 hourly, 2026-06-01).
    """

    @pytest.mark.parametrize("supplied", ["SPORTS", "Sports", "sports", "CEFI", "DeFi"])
    def test_asset_group_passed_lowercase_to_resolver(self, supplied: str):
        """Whatever case the caller supplies, the resolver receives lowercase."""
        with patch(
            "instruments_service.engine.orchestrator.resolve_bucket_name",
            return_value="instruments-store-x-prd-test-project",
        ) as mock_resolve:
            _get_instruments_bucket(supplied)

        assert mock_resolve.call_count == 1
        passed = mock_resolve.call_args.kwargs["asset_group"]
        assert passed == supplied.lower(), f"resolver got {passed!r}, expected lowercase"
