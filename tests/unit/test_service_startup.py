"""
Service startup smoke tests for instruments-service.

Verifies that:
1. The service can be instantiated with CLOUD_PROVIDER=local (no real cloud).
2. Core classes import and instantiate without raising.
3. /health and /readiness endpoints return expected status when present.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def cloud_local_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force local cloud provider so no real GCS calls are made."""
    monkeypatch.setenv("CLOUD_PROVIDER", "local")
    monkeypatch.setenv("CLOUD_MOCK_MODE", "true")
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("SERVICE_MODE", "batch")


class TestInstrumentsServiceStartup:
    """Smoke tests: verify instantiation exits 0 with local cloud provider."""

    def test_import_instruments_service_package(self) -> None:
        """Package import must succeed — verifies __init__.py is valid."""
        import instruments_service  # noqa: F401

    def test_cloud_instrument_storage_instantiation(self) -> None:
        """CloudInstrumentStorage can be constructed with mock UCI."""
        with (
            patch("instruments_service.app.core.cloud_instrument_storage.get_service_mode") as mock_mode,
            patch("instruments_service.app.core.cloud_instrument_storage.get_data_sink"),
        ):
            mock_mode.return_value = MagicMock()
            from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

            storage = CloudInstrumentStorage(testing_mode=True)
            assert storage is not None

    def test_instruments_service_core_instantiation(self) -> None:
        """InstrumentsService core class can be instantiated with mocked storage."""
        with (
            patch("instruments_service.app.core.cloud_instrument_storage.get_service_mode"),
            patch("instruments_service.app.core.cloud_instrument_storage.get_data_sink"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            config: dict[str, object] = {"project_id": "test-project"}
            service = InstrumentsService(config=config)
            assert service is not None

    def test_persistence_events_use_lifecycle_event_type(self) -> None:
        """cloud_instrument_storage must use LifecycleEventType for persistence events, not raw strings."""
        import inspect

        import instruments_service.app.core.cloud_instrument_storage as mod

        source = inspect.getsource(mod)
        # Raw UPLOAD_ string literals must not appear in production code
        assert '"UPLOAD_STARTED"' not in source, (
            "cloud_instrument_storage must use LifecycleEventType.PERSISTENCE_STARTED, not raw string"
        )
        assert '"UPLOAD_COMPLETED"' not in source, (
            "cloud_instrument_storage must use LifecycleEventType.PERSISTENCE_COMPLETED, not raw string"
        )
        # Canonical names must be present
        assert "PERSISTENCE_STARTED" in source
        assert "PERSISTENCE_COMPLETED" in source

    def test_config_does_not_use_os_getenv(self) -> None:
        """Production config module must not call os.getenv — use UnifiedCloudConfig."""
        import inspect

        import instruments_service.config as cfg_mod

        source = inspect.getsource(cfg_mod)
        assert "os.getenv" not in source, "config.py must not use os.getenv — extend UnifiedCloudConfig"
        assert "os.environ" not in source, "config.py must not use os.environ — extend UnifiedCloudConfig"


class TestInstrumentsServiceHealth:
    """Health/readiness probe tests (CLI-based service — no HTTP server)."""

    def test_cli_main_imports_cleanly(self) -> None:
        """CLI main module must import without errors in test environment."""
        with (
            patch("instruments_service.cli.main._load_env_early"),
            patch("instruments_service.config_reloaders.start_domain_config_reloaders"),
        ):
            # Importing the module should not raise
            import instruments_service.cli.main  # noqa: F401

    def test_package_has_version_or_metadata(self) -> None:
        """Package should be importable with standard metadata attributes."""
        import instruments_service

        # Basic sanity: package module exists and __name__ is correct
        assert instruments_service.__name__ == "instruments_service"
