"""Unit tests for InstrumentsServiceConfig and config setup.

Verifies that:
- Config is importable and instantiable with test env vars
- Config reads from UnifiedCloudConfig (not os.getenv)
- Required fields have correct types
- get_config() returns an InstrumentsServiceConfig instance
"""

from unified_config_interface import UnifiedCloudConfig

from instruments_service.config import get_config
from instruments_service.config.service_config import InstrumentsServiceConfig


class TestInstrumentsServiceConfigImport:
    """Verify InstrumentsServiceConfig is importable and structurally correct."""

    def test_config_class_importable(self) -> None:
        """InstrumentsServiceConfig must be importable from instruments_service.config.service_config."""
        assert InstrumentsServiceConfig is not None

    def test_config_inherits_unified_cloud_config(self) -> None:
        """InstrumentsServiceConfig must extend UnifiedCloudConfig (not os.getenv)."""
        assert issubclass(InstrumentsServiceConfig, UnifiedCloudConfig)

    def test_config_has_required_fields(self) -> None:
        """Config class must declare the required service-level fields."""
        fields = InstrumentsServiceConfig.model_fields
        assert "service_name" in fields, "service_name field must be declared"


class TestGetConfig:
    """Verify get_config() returns a valid config singleton."""

    def test_get_config_returns_config_instance(self) -> None:
        """get_config() must return an InstrumentsServiceConfig instance."""
        config = get_config()
        assert isinstance(config, InstrumentsServiceConfig)

    def test_get_config_service_name(self) -> None:
        """Config service_name must be 'instruments-service'."""
        config = get_config()
        assert config.service_name == "instruments-service"

    def test_config_gcp_project_id_type(self) -> None:
        """gcp_project_id must be a string (set via GCP_PROJECT_ID env var in test env)."""
        config = get_config()
        # In test mode CLOUD_MOCK_MODE=true; gcp_project_id may be empty but must be str
        assert isinstance(config.gcp_project_id, str)

    def test_config_does_not_use_os_getenv(self) -> None:
        """Production config module must not use os.getenv — values come from UnifiedCloudConfig."""
        import inspect

        import instruments_service.config.service_config as cfg_module

        source = inspect.getsource(cfg_module)
        assert "os.getenv" not in source, "Config module must not use os.getenv — extend UnifiedCloudConfig instead"
        assert "os.environ" not in source, "Config module must not use os.environ — extend UnifiedCloudConfig instead"


class TestConfigCloudMockMode:
    """Verify config works in CLOUD_MOCK_MODE (used in CI and unit tests)."""

    def test_config_instantiable_without_credentials(self) -> None:
        """Config must be instantiable with CLOUD_MOCK_MODE=true and GCP_PROJECT_ID set."""
        # CLOUD_MOCK_MODE and GCP_PROJECT_ID are set in quality-gates.sh for test runs
        config = get_config()
        assert config is not None

    def test_config_environment_field(self) -> None:
        """Config environment field must be accessible."""
        config = get_config()
        # environment may be None in test mode; it must be a string or None
        assert config.environment is None or isinstance(config.environment, str)
