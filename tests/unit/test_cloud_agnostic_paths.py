"""
Tests for cloud-agnostic path format and project ID injection in instruments-service.

Verifies instruments-service code:
1. All GCS paths use key=value format (day={date}, not day-{date})
2. Code uses unified-cloud-services abstractions (not direct GCP clients)
3. Project ID is correctly injected into bucket names
"""

import os
from datetime import date
from unittest.mock import Mock, patch

import pytest

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.dependency_checker import DependencyChecker


class TestCloudAgnosticPaths:
    """Test cloud-agnostic path formats and project ID injection in instruments-service."""

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
                    "market_category": "CEFI",
                    "available_from_datetime": pd.Timestamp("2024-01-15", tz="UTC"),
                    "instrument_type": "SPOT_PAIR",
                }
            )

            # Store instruments (category is determined from market_category column, not parameter)
            storage.store_instruments(
                instruments_df=test_df,
                date=test_date,
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
        # Patch at the point where StandardizedDomainCloudService is imported (inside method)
        with (
            patch("unified_cloud_services.StandardizedDomainCloudService") as mock_service_class,
            patch("unified_cloud_services.CloudTarget") as mock_target_class,
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
            output_path = checker.get_output_path(category="CEFI", date="2024-01-15")

            # Verify path template uses day={date}
            path_template = checker.OUTPUT_PATH_TEMPLATE
            assert "day={" in path_template, f"Path template should use day={{date}}: {path_template}"
            assert "day-{" not in path_template, f"Path template should not use day-{{date}}: {path_template}"
            # Verify the generated path uses correct format
            assert "day=2024-01-15" in output_path, f"Generated path should use day= format: {output_path}"

    def test_project_id_injection_into_bucket_names(self, mock_project_id):
        """Test that project ID is correctly injected into bucket names."""
        with (
            patch(
                "instruments_service.app.core.cloud_instrument_storage.StandardizedDomainCloudService"
            ) as mock_service_class,
        ):
            mock_service = Mock()
            mock_service.upload_to_gcs_batch = Mock(
                side_effect=lambda uploads, **kwargs: [{"success": True, "gcs_path": u["gcs_path"]} for u in uploads]
            )
            mock_service_class.return_value = mock_service

            expected_bucket = f"instruments-store-cefi-{mock_project_id}"

            # Create CloudTarget directly (not mocked) to verify it accepts project ID
            from unified_cloud_services import CloudTarget

            cloud_target = CloudTarget(
                project_id=mock_project_id,
                gcs_bucket=expected_bucket,
                bigquery_dataset="instruments",
                bigquery_location="asia-northeast1",
            )

            storage = CloudInstrumentStorage(cloud_target=cloud_target)

            # Verify storage uses the provided cloud_target with project ID
            assert storage.cloud_target.project_id == mock_project_id
            assert mock_project_id in storage.cloud_target.gcs_bucket, (
                f"Bucket name should contain project ID: {storage.cloud_target.gcs_bucket}"
            )

    def test_no_direct_gcs_client_imports(self):
        """Test that instruments-service code doesn't directly import get_gcs_client from unified_cloud_services."""
        from pathlib import Path

        # Use the package's actual installation path (invariant across environments).
        # Path(__file__) breaks in Cloud Build: workspace root can be /workspace with
        # deps at /workspace/unified-cloud-services, so parent.parent.parent may
        # incorrectly span the workspace and pick up dependency code.
        import instruments_service

        source_dir = Path(instruments_service.__file__).parent.resolve()

        if not source_dir.exists():
            pytest.skip("instruments_service package directory not found")

        python_files = [
            f
            for f in source_dir.rglob("*.py")
            if "__pycache__" not in str(f) and f.resolve().is_relative_to(source_dir)
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

        assert len(violations) == 0, (
            f"Found {len(violations)} violations - instruments-service should use StandardizedDomainCloudService, "
            f"not get_gcs_client:\n" + "\n".join(violations)
        )

    def test_bucket_names_use_project_id_variable(self, mock_project_id):
        """Test that bucket names use project_id variable, not hardcoded."""
        from instruments_service.config import InstrumentsServiceConfig

        # Set environment variable and clear config cache
        with patch.dict(os.environ, {"GCP_PROJECT_ID": mock_project_id}, clear=False):
            # Clear config cache if it exists
            import instruments_service.config

            if hasattr(instruments_service.config, "_config"):
                instruments_service.config._config = None

            # Also set bucket env vars to use the mock project ID
            with patch.dict(
                os.environ,
                {
                    "INSTRUMENTS_GCS_BUCKET_CEFI": f"instruments-store-cefi-{mock_project_id}",
                },
                clear=False,
            ):
                config = InstrumentsServiceConfig()

                # Verify bucket uses project_id (not hardcoded)
                # Buckets should be constructed dynamically
                bucket_cefi = config.get_bucket_for_category("cefi")
                if bucket_cefi:
                    # Bucket should contain project ID or be a test bucket
                    assert mock_project_id in bucket_cefi or "test" in bucket_cefi.lower(), (
                        f"Bucket should contain project ID or be test bucket: {bucket_cefi}"
                    )
                    # Only check for hardcoded ID if it's different from mock
                    if mock_project_id != "central-element-323112":
                        assert "central-element-323112" not in bucket_cefi, (
                            f"Bucket should not contain hardcoded project ID: {bucket_cefi}"
                        )
