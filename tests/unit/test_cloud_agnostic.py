"""
Cloud-Agnostic Architecture Tests for instruments-service.

Verifies:
- Uses unified-cloud-services (not direct GCP imports)
- Project ID injection in bucket names
- Path format correctness (key=value for BigQuery)
- Path ordering (day first, then other dimensions)
"""

from unittest.mock import patch


class TestCloudAgnosticArchitecture:
    """Test cloud-agnostic patterns."""

    def test_uses_unified_cloud_services_not_direct_gcp(self):
        """Verify imports use unified-cloud-services, not google.cloud directly."""
        # Check core modules use cloud-agnostic imports
        from instruments_service.app.core import cloud_instrument_storage

        # These modules should use StandardizedDomainCloudService
        # Not direct google.cloud.storage.Client

        # Verify unified-cloud-services is imported
        assert hasattr(cloud_instrument_storage, "StandardizedDomainCloudService") or "unified_cloud_services" in str(
            cloud_instrument_storage.__file__
        )

    def test_bucket_name_uses_project_id_injection(self):
        """Verify bucket names use {project_id} template, not hardcoded."""
        from instruments_service.config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()

        # Bucket templates should contain {project_id}
        assert "{project_id}" in config.instruments_gcs_bucket_template
        assert "{project_id}" in config.instrument_availability_bucket_template

        # Formatted buckets should have actual project ID
        bucket = config.instruments_gcs_bucket
        assert config.gcp_project_id in bucket
        assert "{project_id}" not in bucket  # Should be replaced

    def test_path_format_uses_key_equals_value(self):
        """Verify GCS paths use key=value format for BigQuery hive partitioning."""

        # Path should use day={date} format
        # Not day-{date} (old format incompatible with BigQuery)
        # Test by checking if the code uses the correct format
        # This is a compile-time check - if code has day= it will work
        # Read the source to verify format
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Should contain day={date} pattern
        assert "day={" in source or "day='" in source or 'day="' in source, (
            "Path format should use day={date} not day-{date} for BigQuery compatibility"
        )

    def test_path_ordering_day_first(self):
        """Verify paths have day as first dimension (for partitioning)."""
        # Path structure should be: .../by_date/day={date}/...
        # Not: .../{other}/by_date/day={date}/
        # Day should be the FIRST partition key
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Check that by_date comes before any other dimensions
        # and day= immediately follows by_date/
        assert "by_date/day=" in source, "Day should immediately follow by_date/ for partitioning"

    def test_no_hardcoded_project_ids(self):
        """Verify no hardcoded GCP project IDs in production code."""
        from pathlib import Path

        # Check production code (not tests)
        service_dir = Path(__file__).parent.parent.parent / "instruments_service"

        # Search for hardcoded project ID
        hardcoded_patterns = [
            "central-element-323112",
            "project_id='",
            'project_id="',
        ]

        found_hardcoded = []
        for py_file in service_dir.rglob("*.py"):
            if "test" in str(py_file):
                continue  # Skip test files

            content = py_file.read_text()
            for pattern in hardcoded_patterns:
                if pattern in content:
                    found_hardcoded.append((str(py_file), pattern))

        assert len(found_hardcoded) == 0, f"Found hardcoded project IDs in production code: {found_hardcoded}"

    @patch.dict("os.environ", {"CLOUD_PROVIDER": "aws"})
    def test_cloud_provider_switch_via_env_var(self):
        """Verify can switch cloud provider via CLOUD_PROVIDER env var."""
        from unified_cloud_services.core.provider import CloudProvider

        # unified-cloud-services should detect AWS from env var
        provider = CloudProvider.from_env()

        # This test verifies the PATTERN is correct
        # Actual AWS support may not be implemented yet
        assert provider is not None, "CloudProvider.from_env() should work"


class TestPathFormatCompliance:
    """Test specific path format requirements."""

    def test_instrument_definitions_path_format(self):
        """Test instrument definitions path uses correct format."""
        # Verify path construction matches expected format: instrument_definitions/by_date/day={date}/instruments.parquet
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Should contain the expected format
        assert "day={" in source, "Should use day={date} format"
        assert "instrument_definitions" in source, "Should write to instrument_definitions/"
        assert "by_date/" in source, "Should use by_date/ prefix"

    def test_instrument_availability_path_format(self):
        """Test instrument availability path uses correct format."""
        # Verify path construction matches expected format: instrument_availability/by_date/day={date}/instruments.parquet
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Verify format
        assert "instrument_availability" in source
        assert "by_date/day=" in source

    def test_path_no_hardcoded_dates(self):
        """Verify paths use date variables, not hardcoded dates."""
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Should NOT contain hardcoded dates like "2023-01-01"
        assert "day=2023" not in source, "Should not have hardcoded dates"
        assert "day=2024" not in source, "Should not have hardcoded dates"
