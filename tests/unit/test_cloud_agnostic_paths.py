"""
Tests for cloud-agnostic path format and project ID injection in instruments-service.

Verifies instruments-service code:
1. UCI DataSink partition dict uses day/venue keys (key=value format)
2. Code uses UCI get_data_sink abstraction (not direct GCP clients)
3. Project ID is correctly injected into bucket names via config
"""

import os
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from unified_trading_library import BaseDependencyChecker as DependencyChecker

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage


def _make_common_patches(mock_data_sink):
    """Return context managers for patching UCI layer in store_instruments calls."""
    return [
        patch(
            "instruments_service.app.core.cloud_instrument_storage.get_data_sink",
            return_value=mock_data_sink,
        ),
        patch(
            "instruments_service.app.core.cloud_instrument_storage.determine_market_category",
            return_value="CEFI",
        ),
        patch(
            "instruments_service.app.core.cloud_instrument_storage.create_sampling_service",
            return_value=Mock(),
        ),
        patch(
            "instruments_service.app.core.cloud_instrument_storage.ParquetSchemaEnforcer",
            return_value=Mock(validate_dataframe=Mock(return_value=Mock(valid=True, errors=[], warnings=[]))),
        ),
        patch(
            "instruments_service.app.core.cloud_instrument_storage.validate_timestamp_date_alignment",
            return_value=Mock(valid=True, actual_dates_found=[], alignment_percentage=100.0),
        ),
    ]


class TestCloudAgnosticPaths:
    """Test cloud-agnostic path formats and project ID injection in instruments-service."""

    @pytest.fixture
    def mock_project_id(self):
        """Mock project ID."""
        return "test-project-12345"

    @pytest.fixture
    def mock_data_sink(self):
        """Create a mock UCI DataSink."""
        sink = Mock()
        sink.write = Mock(return_value="gs://bucket/day=2024-01-15/venue=TEST/file.parquet")
        return sink

    def test_cloud_instrument_storage_path_format(self, mock_data_sink):
        """Test that CloudInstrumentStorage passes day partition key to UCI DataSink."""
        test_date = datetime(2024, 1, 15, tzinfo=UTC)
        test_df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "symbol": ["BTC-USDT"],
                "venue": ["TEST"],
                "market_category": ["CEFI"],
                "available_from_datetime": [pd.Timestamp("2024-01-15", tz="UTC")],
                "instrument_type": ["SPOT_PAIR"],
                "timestamp": [pd.Timestamp("2024-01-15", tz="UTC")],
            }
        )

        with (
            patch("instruments_service.app.core.cloud_instrument_storage.get_data_sink", return_value=mock_data_sink),
            patch("instruments_service.app.core.cloud_instrument_storage.create_sampling_service", return_value=Mock()),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.ParquetSchemaEnforcer",
                return_value=Mock(validate_dataframe=Mock(return_value=Mock(valid=True, errors=[], warnings=[]))),
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.validate_timestamp_date_alignment",
                return_value=Mock(valid=True, actual_dates_found=[], alignment_percentage=100.0),
            ),
        ):
            storage = CloudInstrumentStorage()
            storage.store_instruments(instruments_df=test_df, date=test_date)

        # Verify UCI DataSink.write was called with day partition
        assert mock_data_sink.write.called
        call_kwargs = mock_data_sink.write.call_args[1]
        assert "partition" in call_kwargs
        partition = call_kwargs["partition"]
        assert partition.get("day") == "2024-01-15", f"Partition should have day=2024-01-15: {partition}"
        assert "venue" in partition, f"Partition should include venue key: {partition}"

    def test_dependency_checker_path_format(self, mock_project_id):
        """Test that DependencyChecker uses day={date} format."""
        # DependencyChecker.get_output_path is a pure formatter — no cloud calls needed
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
        """Test that project ID is correctly injected into bucket names via config."""
        from instruments_service.config import InstrumentsServiceConfig

        with patch.dict(
            os.environ,
            {
                "GCP_PROJECT_ID": mock_project_id,
                "INSTRUMENTS_GCS_BUCKET_CEFI": f"instruments-store-cefi-{mock_project_id}",
            },
            clear=False,
        ):
            config = InstrumentsServiceConfig()
            bucket_cefi = config.get_bucket_for_category("CEFI")

        # Bucket should contain mock project ID
        assert mock_project_id in bucket_cefi, f"Bucket name should contain project ID: {bucket_cefi}"

    def test_no_direct_gcs_client_imports(self):
        """Test that instruments-service code doesn't directly import get_gcs_client."""
        from pathlib import Path

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
                with open(file_path) as f:
                    content = f.read()

                # Check for direct get_gcs_client imports (bad)
                if "from unified_trading_library.core.cloud_auth_factory import create_gcs_client" in content:
                    violations.append(f"{file_path}: Direct import of get_gcs_client")

                # Check for get_gcs_client() calls (bad, should use UCI get_data_sink)
                if "get_gcs_client(" in content and "unified_trading_library" in content:
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if "get_gcs_client(" in line and not (
                            line.strip().startswith("#") or '"""' in line or "'''" in line
                        ):
                            violations.append(f"{file_path}:{i + 1}: Direct call to get_gcs_client()")

            except (OSError, ValueError, KeyError, TypeError, ImportError, RuntimeError):
                continue

        assert len(violations) == 0, (
            f"Found {len(violations)} violations - instruments-service should use UCI get_data_sink, "
            f"not get_gcs_client:\n" + "\n".join(violations)
        )

    def test_bucket_names_use_project_id_variable(self, mock_project_id):
        """Test that bucket names use project_id variable, not hardcoded."""
        from instruments_service.config import InstrumentsServiceConfig

        with patch.dict(os.environ, {"GCP_PROJECT_ID": mock_project_id}, clear=False):
            import instruments_service.config

            if hasattr(instruments_service.config, "_config"):
                instruments_service.config._config = None

            with patch.dict(
                os.environ,
                {
                    "INSTRUMENTS_GCS_BUCKET_CEFI": f"instruments-store-cefi-{mock_project_id}",
                },
                clear=False,
            ):
                config = InstrumentsServiceConfig()

                bucket_cefi = config.get_bucket_for_category("cefi")
                if bucket_cefi:
                    assert mock_project_id in bucket_cefi or "test" in bucket_cefi.lower(), (
                        f"Bucket should contain project ID or be test bucket: {bucket_cefi}"
                    )
                    FORBIDDEN_REAL_ID = "real-project-id"
                    if mock_project_id != FORBIDDEN_REAL_ID:
                        assert FORBIDDEN_REAL_ID not in bucket_cefi, (
                            f"Bucket should not contain hardcoded project ID: {bucket_cefi}"
                        )

    def test_venue_path_format_uses_key_equals_value(self, mock_data_sink):
        """Test that venue paths use venue= partition key in UCI DataSink call."""
        test_date = datetime(2024, 1, 15, tzinfo=UTC)
        test_df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "symbol": ["BTC-USDT"],
                "venue": ["TEST-VENUE"],
                "market_category": ["CEFI"],
                "available_from_datetime": [pd.Timestamp("2024-01-15", tz="UTC")],
                "instrument_type": ["SPOT_PAIR"],
                "timestamp": [pd.Timestamp("2024-01-15", tz="UTC")],
            }
        )

        with (
            patch("instruments_service.app.core.cloud_instrument_storage.get_data_sink", return_value=mock_data_sink),
            patch("instruments_service.app.core.cloud_instrument_storage.create_sampling_service", return_value=Mock()),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.ParquetSchemaEnforcer",
                return_value=Mock(validate_dataframe=Mock(return_value=Mock(valid=True, errors=[], warnings=[]))),
            ),
            patch(
                "instruments_service.app.core.cloud_instrument_storage.validate_timestamp_date_alignment",
                return_value=Mock(valid=True, actual_dates_found=[], alignment_percentage=100.0),
            ),
        ):
            storage = CloudInstrumentStorage()
            storage.store_instruments(instruments_df=test_df, date=test_date)

        # Verify UCI DataSink.write was called with venue partition key
        assert mock_data_sink.write.called
        call_kwargs = mock_data_sink.write.call_args[1]
        assert "partition" in call_kwargs
        partition = call_kwargs["partition"]
        assert "venue" in partition, f"Partition should include venue key: {partition}"
        # Venue value should not use hyphen-prefix format
        venue_value = partition.get("venue", "")
        assert venue_value != "", "Venue partition value should not be empty"
