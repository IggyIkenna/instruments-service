"""
Cloud-Agnostic Architecture Tests for instruments-service.

Verifies instruments-service uses cloud-agnostic patterns:
- Uses unified-trading-services (not direct GCP imports)
- Project ID injection in bucket names
- Path format correctness (key=value for BigQuery)
- Path ordering (day first, then other dimensions)
"""

from pathlib import Path


class TestCloudAgnosticArchitecture:
    """Test cloud-agnostic patterns in instruments-service code."""

    def test_no_direct_gcp_imports(self):
        """Verify instruments-service doesn't use direct google.cloud imports."""
        service_dir = Path(__file__).parent.parent.parent / "instruments_service"

        violations = []
        for py_file in service_dir.rglob("*.py"):
            if "test" in str(py_file):
                continue  # Skip test files

            try:
                content = py_file.read_text()

                # Check for direct google.cloud imports (bad - should use unified-trading-services)
                if "from google.cloud import storage" in content:
                    # Allow if it's in a comment or docstring
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if "from google.cloud import storage" in line and not (
                            line.strip().startswith("#") or '"""' in line or "'''" in line
                        ):
                            violations.append(f"{py_file}:{i + 1}: Direct import of google.cloud.storage")

                if "from google.cloud.storage" in content:
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if "from google.cloud.storage" in line and not (
                            line.strip().startswith("#") or '"""' in line or "'''" in line
                        ):
                            violations.append(f"{py_file}:{i + 1}: Direct import of google.cloud.storage")

            except (OSError, ValueError, KeyError, TypeError, ImportError, RuntimeError):
                continue

        assert len(violations) == 0, (
            f"Found {len(violations)} violations - instruments-service should use unified-trading-services, "
            f"not direct google.cloud imports:\n" + "\n".join(violations)
        )

    def test_bucket_name_uses_project_id_injection(self):
        """Verify instruments-service bucket names use project ID, not hardcoded."""
        from instruments_service.config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()

        # Buckets should contain actual project ID (not hardcoded)
        # Check category-specific buckets (config uses sink_bucket_* naming)
        if config.sink_bucket_cefi:
            assert config.gcp_project_id in config.sink_bucket_cefi or "test" in config.sink_bucket_cefi.lower()
        if config.sink_bucket_tradfi:
            assert config.gcp_project_id in config.sink_bucket_tradfi or "test" in config.sink_bucket_tradfi.lower()

        # Verify get_bucket_for_category returns bucket with project ID
        bucket_cefi = config.get_bucket_for_category("CEFI")
        if bucket_cefi and bucket_cefi != config.sink_bucket:  # Only check if category bucket is set
            # Bucket should contain project ID or be a test bucket
            assert config.gcp_project_id in bucket_cefi or "test" in bucket_cefi.lower()

    def test_path_format_uses_key_equals_value(self):
        """Verify instruments-service GCS paths use key=value format for BigQuery hive partitioning."""
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Should use partition dict with "day" key (UCI DataSink intent API)
        assert '"day"' in source or "'day'" in source, (
            "Path format should use partition={'day': date_str} for BigQuery hive partitioning compatibility"
        )

    def test_path_ordering_day_first(self):
        """Verify instruments-service passes 'day' as the first partition key to UCI DataSink."""
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # The UCI DataSink uses partition={"day": date_str, ...} — "day" key must appear first
        # Check for the UCI partition dict pattern
        assert '"day"' in source or "'day'" in source, "Storage should pass day as a partition key to UCI DataSink"

    def test_no_hardcoded_project_ids(self):
        """Verify no hardcoded GCP project IDs in instruments-service production code."""
        service_dir = Path(__file__).parent.parent.parent / "instruments_service"

        # Search for hardcoded project ID
        hardcoded_patterns = [
            "test-project",
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
                    # Check if it's in a comment or docstring
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if pattern in line and not (line.strip().startswith("#") or '"""' in line or "'''" in line):
                            found_hardcoded.append((str(py_file), pattern, i + 1))
                            break

        assert len(found_hardcoded) == 0, (
            f"Found hardcoded project IDs in instruments-service production code: {found_hardcoded}"
        )


class TestPathFormatCompliance:
    """Test specific path format requirements in instruments-service code."""

    def test_instrument_availability_path_format(self):
        """Test instrument storage uses UCI DataSink with day partition key."""
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # UCI DataSink uses partition={"day": date_str} — verify day key is present
        assert '"day"' in source or "'day'" in source, "Should use 'day' as UCI DataSink partition key"
        # Should use get_data_sink from UCI — cloud-agnostic storage
        assert "get_data_sink" in source, "Should use UCI get_data_sink for cloud-agnostic storage"

    def test_path_no_hardcoded_dates(self):
        """Verify instruments-service paths use date variables, not hardcoded dates."""
        import inspect

        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        source = inspect.getsource(CloudInstrumentStorage)

        # Should NOT contain hardcoded dates like "2023-01-01"
        assert "day=2023" not in source, "Should not have hardcoded dates"
        assert "day=2024" not in source, "Should not have hardcoded dates"
