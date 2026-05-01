"""Service-level configuration for instruments-service.

Four fields only. Everything else is resolved by UTL, UCI, or URDI:
- Bucket names: UTL ``get_bucket_name("instruments", <segment>)`` builds
  ``instruments-store-{asset_group}-{gcp_project_id}`` (``segment`` = asset group: cefi, defi, …).
- DeFi/venue API URLs: URDI adapters read their own URLs from UCI provider
  manifest at startup — not service config.
- Deployment state (deployment_id, shard_launched_at): UTL ServiceBootstrap.
"""

from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict
from unified_trading_library import UnifiedCloudConfig


class InstrumentsServiceConfig(UnifiedCloudConfig):
    """Service configuration — 4 fields only."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    service_name: str = Field(default="instruments-service", description="Service name")

    enable_ccxt_integration: bool = Field(
        default=True,
        validation_alias=AliasChoices("ENABLE_CCXT_INTEGRATION"),
        description="Enable post-URDI CCXT metadata enrichment (leverage/margin fields)",
    )

    config_store_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("CONFIG_STORE_BUCKET"),
        description="Cloud storage bucket for domain config store (hot reload)",
    )

    catalogue_path_override: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_CATALOGUE_PATH"),
        description="Override path to data catalogue (for CI tests)",
    )

    instruments_bucket_prefix: str = Field(
        default="instruments-store",
        validation_alias=AliasChoices("INSTRUMENTS_BUCKET_PREFIX"),
        description="Bucket name prefix; full name: {prefix}-{asset_group}-{gcp_project_id}",
    )

    is_test_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("IS_TEST_RUN"),
        description="Route writes to test bucket instead of prod (E2E test mode)",
    )


_config: InstrumentsServiceConfig | None = None


def get_config() -> InstrumentsServiceConfig:
    """Return the singleton service config instance."""
    global _config
    if _config is None:
        _config = InstrumentsServiceConfig()
    return _config


instruments_config = get_config()
