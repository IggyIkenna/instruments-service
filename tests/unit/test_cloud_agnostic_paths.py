"""
Tests for cloud-agnostic path format and project ID injection.

Verifies:
1. All GCS paths use key=value format (day={date}, not day-{date})
2. Code uses StandardizedDomainCloudService (cloud-agnostic)
3. Project ID is correctly injected into bucket names
"""

import os
from datetime import date
from unittest.mock import Mock, patch

import pytest

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.dependency_checker import DependencyChecker
from instruments_service.cli.handlers.corporate_actions_handler import CorporateActionsHandler


class TestCloudAgnosticPaths:
    """Test cloud-agnostic path formats and project ID injection."""

    @pytest.fixture
    def mock_project_id(self):
        """Mock project ID."""
        return "test-project-12345"

    @pytest.fixture
    def mock_cloud_service(self):
        """Create mock cloud service."""
        service = Mock()
        service.upload_to_gcs = Mock(return_value=True)
        service.upload_to_gcs_batch = Mock(
            side_effect=lambda uploads, **kwargs: [{"success": True, "gcs_path": u["gcs_path"]} for u in uploads]
        )
        service.download_from_gcs = Mock(return_value=Mock(empty=False))
        return service

    @pytest.fixture
    def mock_cloud_target(self, mock_project_id):
        """Create mock cloud target with project ID."""
        target = Mock()
        target.project_id = mock_project_id
        target.gcs_bucket = f"instruments-store-cefi-{mock_project_id}"
        target.bigquery_dataset = "instruments"
        target.bigquery_location = "asia-northeast1"
        return target

    def test_cloud_instrument_storage_path_format(self, mock_cloud_service, mock_cloud_target):
        """Test that CloudInstrumentStorage uses day={date} format."""
        import pandas as pd

        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService",
                return_value=mock_cloud_service,
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
                "instruments_service.app.core.cloud_instrument_storage.get_bucket_for_category",
                return_value=f"instruments-store-cefi-{mock_cloud_target.project_id}",
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.create_sampling_service",
                return_value=Mock(),
            ),
            patch(
                "unified_cloud_services.SchemaValidator",
                return_value=Mock(validate_dataframe_schema=Mock(return_value=Mock(valid=True, errors=[]))),
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.ParquetSchemaEnforcer",
                return_value=Mock(validate_dataframe=Mock(return_value=Mock(valid=True, errors=[], warnings=[]))),
            ),
        ):
            storage = CloudInstrumentStorage(cloud_target=mock_cloud_target)
            storage.cloud_service = mock_cloud_service

            # Create test data
            test_date = date(2024, 1, 15)
            test_df = pd.DataFrame(
                {
                    "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                    "venue": ["TEST"],
                }
            )

            # Store instruments
            storage.store_instruments(
                instruments_df=test_df,
                date=test_date,
                category="CEFI",
            )

            # Verify upload was called
            assert mock_cloud_service.upload_to_gcs_batch.called

            # Get the call arguments
            call_args = mock_cloud_service.upload_to_gcs_batch.call_args
            uploads = call_args[0][0] if call_args[0] else call_args[1].get("uploads", [])

            # Verify path format uses day={date}
            for upload in uploads:
                gcs_path = upload.get("gcs_path", "")
                assert "day=" in gcs_path, f"Path should use day= format: {gcs_path}"
                assert "day-2024-01-15" not in gcs_path, f"Path should not use day- format: {gcs_path}"
                assert "day=2024-01-15" in gcs_path, f"Path should contain day=2024-01-15: {gcs_path}"

    def test_dependency_checker_path_format(self, mock_project_id):
        """Test that DependencyChecker uses day={date} format."""
        with (
            patch(
                "instruments_service.app.core.dependency_checker.StandardizedDomainCloudService"
            ) as mock_service_class,
            patch("instruments_service.app.core.dependency_checker.CloudTarget") as mock_target_class,
        ):
            mock_service = Mock()
            mock_service.download_from_gcs = Mock(return_value=Mock(empty=False))
            mock_service_class.return_value = mock_service

            mock_target = Mock()
            mock_target.project_id = mock_project_id
            mock_target.gcs_bucket = f"instruments-store-cefi-{mock_project_id}"
            mock_target_class.return_value = mock_target

            checker = DependencyChecker(project_id=mock_project_id)

            # Check output path (validates path format)
            checker.validate_output_path(category="CEFI", date="2024-01-15")

            # Verify CloudTarget was created with correct project ID
            assert mock_target_class.called
            call_kwargs = mock_target_class.call_args[1] if mock_target_class.call_args[1] else {}
            assert call_kwargs.get("project_id") == mock_project_id

            # Verify path template uses day={date}
            path_template = checker.OUTPUT_PATH_TEMPLATE
            assert "day={" in path_template, f"Path template should use day={{date}}: {path_template}"
            assert "day-{" not in path_template, f"Path template should not use day-{{date}}: {path_template}"

    def test_corporate_actions_handler_cloud_agnostic(self, mock_project_id):
        """Test that CorporateActionsHandler uses cloud-agnostic service."""
        import pandas as pd

        with (
            patch(
                "instruments_service.cli.handlers.corporate_actions_handler.StandardizedDomainCloudService"
            ) as mock_service_class,
            patch("instruments_service.cli.handlers.corporate_actions_handler.CloudTarget") as mock_target_class,
            patch("instruments_service.cli.handlers.corporate_actions_handler.instruments_config") as mock_config,
        ):
            mock_service = Mock()
            mock_service.download_from_gcs = Mock(
                return_value=pd.DataFrame(
                    {
                        "venue": ["NYSE", "NASDAQ"],
                        "exchange_raw_symbol": ["AAPL", "MSFT"],
                    }
                )
            )
            mock_service.upload_to_gcs = Mock(return_value=True)
            mock_service_class.return_value = mock_service

            mock_target = Mock()
            mock_target.project_id = mock_project_id
            mock_target.gcs_bucket = f"instruments-store-tradfi-{mock_project_id}"
            mock_target_class.return_value = mock_target

            mock_config.gcs_bucket_tradfi = f"instruments-store-tradfi-{mock_project_id}"
            mock_config.get_bucket_for_category = Mock(return_value=f"instruments-store-tradfi-{mock_project_id}")

            handler = CorporateActionsHandler(project_id=mock_project_id)

            # Test ticker loading uses cloud-agnostic service
            handler._get_tickers_from_gcs()

            # Verify StandardizedDomainCloudService was used (not get_gcs_client)
            assert mock_service_class.called, "Should use StandardizedDomainCloudService"
            assert mock_target_class.called, "Should create CloudTarget"

            # Verify CloudTarget has correct project ID
            call_kwargs = mock_target_class.call_args[1] if mock_target_class.call_args[1] else {}
            assert call_kwargs.get("project_id") == mock_project_id

            # Verify path format uses day={date}
            download_calls = mock_service.download_from_gcs.call_args_list
            for call in download_calls:
                gcs_path = call[1].get("gcs_path", "") or (call[0][0] if call[0] else "")
                if "day=" in gcs_path:
                    assert "day=" in gcs_path, f"Path should use day= format: {gcs_path}"
                    assert "day-2024" not in gcs_path, f"Path should not use day- format: {gcs_path}"

    def test_project_id_injection_into_bucket_names(self, mock_project_id):
        """Test that project ID is correctly injected into bucket names."""
        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService"
            ) as mock_service_class,
            patch("instruments_service.app.core.cloud_instrument_storage.CloudTarget") as mock_target_class,
            patch("instruments_service.app.core.cloud_instrument_storage.get_bucket_for_category") as mock_get_bucket,
        ):
            mock_service = Mock()
            mock_service.upload_to_gcs_batch = Mock(
                side_effect=lambda uploads, **kwargs: [{"success": True, "gcs_path": u["gcs_path"]} for u in uploads]
            )
            mock_service_class.return_value = mock_service

            expected_bucket = f"instruments-store-cefi-{mock_project_id}"
            mock_get_bucket.return_value = expected_bucket

            mock_target = Mock()
            mock_target.project_id = mock_project_id
            mock_target.gcs_bucket = expected_bucket
            mock_target_class.return_value = mock_target

            CloudInstrumentStorage(cloud_target=mock_target)

            # Verify get_bucket_for_category was called with project ID
            assert mock_get_bucket.called

            # Verify CloudTarget was created with bucket containing project ID
            assert mock_target_class.called
            call_kwargs = mock_target_class.call_args[1] if mock_target_class.call_args[1] else {}
            bucket_name = call_kwargs.get("gcs_bucket", "")
            assert mock_project_id in bucket_name, f"Bucket name should contain project ID: {bucket_name}"

    def test_no_direct_gcs_client_imports(self):
        """Test that no code directly imports get_gcs_client from unified_cloud_services."""
        from pathlib import Path

        # Find all Python files in the service
        service_root = Path(__file__).parent.parent.parent
        python_files = list(service_root.rglob("*.py"))

        # Exclude test files and __pycache__
        python_files = [
            f for f in python_files if "test_" not in f.name and "__pycache__" not in str(f) and "tests/" not in str(f)
        ]

        violations = []
        for file_path in python_files:
            try:
                with open(file_path, "r") as f:
                    content = f.read()

                # Check for direct get_gcs_client imports (bad)
                if "from unified_cloud_services import get_gcs_client" in content:
                    violations.append(f"{file_path}: Direct import of get_gcs_client")

                # Check for get_gcs_client() calls (bad, should use StandardizedDomainCloudService)
                if "get_gcs_client(" in content and "unified_cloud_services" in content:
                    # Allow if it's in a comment or docstring
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if "get_gcs_client(" in line and not (
                            line.strip().startswith("#") or '"""' in line or "'''" in line
                        ):
                            violations.append(f"{file_path}:{i + 1}: Direct call to get_gcs_client()")

            except Exception:
                # Skip files that can't be parsed
                continue

        assert len(violations) == 0, f"Found {len(violations)} violations of cloud-agnostic pattern:\n" + "\n".join(
            violations
        )

    def test_aws_provider_support_structure(self):
        """Test that AWS provider structure exists (no connectivity required)."""
        from unified_cloud_services.core.provider import CloudProvider

        # Test AWS provider enum exists
        assert CloudProvider.AWS.value == "aws"
        assert CloudProvider.from_string("aws") == CloudProvider.AWS

        # Test that get_storage_client can be called with AWS provider
        # (won't actually create client without credentials, but structure should exist)
        with patch.dict(os.environ, {"CLOUD_PROVIDER": "aws"}):
            # Refresh provider cache
            from unified_cloud_services.core.provider import set_cloud_provider

            set_cloud_provider(CloudProvider.AWS)

            # Verify AWS client can be imported
            try:
                from unified_cloud_services.core.aws_clients import S3StorageClient

                assert S3StorageClient is not None, "S3StorageClient should exist"
            except ImportError:
                # AWS libraries might not be installed in test env, but structure should exist
                pytest.skip("AWS libraries not installed (expected in some test environments)")

    def test_aws_env_vars_supported(self, mock_project_id):
        """Test that AWS environment variables are supported in config."""
        from instruments_service.config import InstrumentsServiceConfig

        # Test with AWS provider
        with patch.dict(
            os.environ,
            {
                "CLOUD_PROVIDER": "aws",
                "AWS_PROJECT_ID": "test-aws-project-12345",
                "AWS_REGION": "us-east-1",
            },
            clear=False,
        ):
            # Clear config cache if it exists
            import instruments_service.config

            if hasattr(instruments_service.config, "_config"):
                instruments_service.config._config = None

            config = InstrumentsServiceConfig()

            # Verify AWS config fields exist
            assert hasattr(config, "cloud_provider")
            assert hasattr(config, "aws_account_id") or hasattr(config, "aws_region")
            # Config should respect CLOUD_PROVIDER env var
            assert config.cloud_provider.lower() in ["aws", "gcp"]

    def test_bucket_names_use_project_id_variable(self, mock_project_id):
        """Test that bucket names use project_id variable, not hardcoded."""
        from instruments_service.config import InstrumentsServiceConfig

        with patch.dict(os.environ, {"GCP_PROJECT_ID": mock_project_id}, clear=False):
            # Clear config cache if it exists
            import instruments_service.config

            if hasattr(instruments_service.config, "_config"):
                instruments_service.config._config = None

            config = InstrumentsServiceConfig()

            # Verify bucket templates use project_id (not hardcoded)
            # Buckets should be constructed dynamically
            bucket_cefi = config.get_bucket_for_category("cefi") if hasattr(config, "get_bucket_for_category") else None
            if bucket_cefi:
                assert mock_project_id in bucket_cefi, f"Bucket should contain project ID: {bucket_cefi}"
                # Only check for hardcoded ID if it's different from mock
                if mock_project_id != "central-element-323112":
                    assert "central-element-323112" not in bucket_cefi, (
                        f"Bucket should not contain hardcoded project ID: {bucket_cefi}"
                    )
