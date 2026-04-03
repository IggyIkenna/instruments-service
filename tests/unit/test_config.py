"""Config tests — verifies InstrumentsServiceConfig and config_reloaders work correctly.

instruments-service config is intentionally minimal: 4 fields only.
Everything else (buckets, DeFi URLs, deployment state) is owned by UTL/UCI/UAC.
"""

from __future__ import annotations


def test_service_config_has_expected_fields():
    """Service config has exactly the 4 declared fields (no more, no less service-specific fields)."""
    from instruments_service.config.service_config import InstrumentsServiceConfig

    cfg = InstrumentsServiceConfig()
    # 4 service-specific fields
    assert hasattr(cfg, "service_name")
    assert hasattr(cfg, "enable_ccxt_integration")
    assert hasattr(cfg, "config_store_bucket")
    assert hasattr(cfg, "catalogue_path_override")


def test_service_config_defaults():
    from instruments_service.config.service_config import InstrumentsServiceConfig

    cfg = InstrumentsServiceConfig()
    assert cfg.service_name == "instruments-service"
    assert cfg.enable_ccxt_integration is True
    assert cfg.config_store_bucket == ""
    assert cfg.catalogue_path_override == ""


def test_service_config_env_override(monkeypatch):
    """Service config reads env vars for field overrides."""
    monkeypatch.setenv("ENABLE_CCXT_INTEGRATION", "false")
    monkeypatch.setenv("CONFIG_STORE_BUCKET", "my-bucket")
    monkeypatch.setenv("INSTRUMENTS_CATALOGUE_PATH", "/override/path")

    # Reset singleton to pick up new env
    import instruments_service.config.service_config as sc

    sc._config = None

    cfg = sc.InstrumentsServiceConfig()
    assert cfg.enable_ccxt_integration is False
    assert cfg.config_store_bucket == "my-bucket"
    assert cfg.catalogue_path_override == "/override/path"

    # Reset singleton back
    sc._config = None


def test_get_config_returns_singleton():
    from instruments_service.config.service_config import get_config

    c1 = get_config()
    c2 = get_config()
    assert c1 is c2


def test_instruments_config_exported_from_package():
    """instruments_config singleton is importable from the config package."""
    from instruments_service.config import InstrumentsServiceConfig, instruments_config

    assert isinstance(instruments_config, InstrumentsServiceConfig)
    assert instruments_config.service_name == "instruments-service"


def test_config_extends_unified_cloud_config():
    """InstrumentsServiceConfig extends UnifiedCloudConfig — inherits cloud settings."""
    from unified_trading_library import UnifiedCloudConfig

    from instruments_service.config.service_config import InstrumentsServiceConfig

    assert issubclass(InstrumentsServiceConfig, UnifiedCloudConfig)
    cfg = InstrumentsServiceConfig()
    # Inherited fields from UnifiedCloudConfig
    assert hasattr(cfg, "gcp_project_id")
    assert hasattr(cfg, "cloud_provider")


def test_config_package_init_exports():
    """config/__init__.py exports the three expected names."""
    import instruments_service.config as config_pkg

    assert hasattr(config_pkg, "InstrumentsServiceConfig")
    assert hasattr(config_pkg, "get_config")
    assert hasattr(config_pkg, "instruments_config")
