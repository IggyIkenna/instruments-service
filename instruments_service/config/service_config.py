"""
Service-level configuration for instruments-service.

InstrumentsServiceConfig (Pydantic BaseSettings) and singleton access.
"""

import logging
from typing import cast

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict
from unified_config_interface import UnifiedCloudConfig
from unified_trading_library import CloudTarget

logger = logging.getLogger(__name__)


class InstrumentsServiceConfig(UnifiedCloudConfig):
    """
    Service-level configuration for instruments-service.

    Extends UnifiedCloudConfig with instruments-specific settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    service_name: str = Field(default="instruments-service", description="Service name")

    gcs_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET"),
        description="Primary GCS bucket for instruments",
    )
    gcs_bucket_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TEST"),
        description="Test GCS bucket for instruments",
    )
    gcs_bucket_cefi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
        description="GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
        description="GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
        description="GCS bucket for DEFI instruments",
    )
    gcs_bucket_sports: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_SPORTS"),
        description="GCS bucket for SPORTS instruments",
    )
    gcs_bucket_cefi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI_TEST"),
        description="Test GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI_TEST"),
        description="Test GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI_TEST"),
        description="Test GCS bucket for DEFI instruments",
    )
    gcs_bucket_sports_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_SPORTS_TEST"),
        description="Test GCS bucket for SPORTS instruments",
    )

    bigquery_dataset: str = Field(
        default="instruments",
        validation_alias=AliasChoices("INSTRUMENTS_BIGQUERY_DATASET", "BIGQUERY_DATASET"),
        description="BigQuery dataset for instruments",
    )

    enable_ccxt_integration: bool = Field(default=True, description="Enable CCXT metadata enrichment")
    enable_metadata_caching: bool = Field(default=True, description="Enable metadata caching")
    cache_ttl_hours: int = Field(default=24, description="Cache TTL in hours")
    max_batch_size: int = Field(default=1000, description="Maximum batch size for processing")
    lookback_days: int = Field(default=0, description="Lookback days for batch processing")

    graph_secret_name: str = Field(
        default="graph-api-key",
        validation_alias=AliasChoices("GRAPH_SECRET_NAME"),
        description="Graph API key secret name",
    )

    aavescan_api_url: str = Field(
        default="https://api.aavescan.com/v2",
        validation_alias=AliasChoices("AAVESCAN_API_URL"),
        description="AaveScan Pro API base URL",
    )
    ethereum_rpc_url: str = Field(
        default="",
        validation_alias=AliasChoices("ETHEREUM_RPC_URL"),
        description="Ethereum RPC URL",
    )
    uniswap_v3_graph_url: str = Field(
        default="",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_URL"),
        description="Uniswap V3 Graph URL",
    )
    uniswap_v3_graph_arb_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-arbitrum/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_ARB_URL"),
        description="The Graph Uniswap V3 URL for Arbitrum",
    )
    uniswap_v3_graph_base_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-base/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_BASE_URL"),
        description="The Graph Uniswap V3 URL for Base",
    )
    envio_api_url: str = Field(
        default="",
        validation_alias=AliasChoices("ENVIO_API_URL"),
        description="Envio API URL",
    )
    hyperliquid_api_url: str = Field(
        default="https://api.hyperliquid.xyz",
        validation_alias=AliasChoices("HYPERLIQUID_API_URL"),
        description="Hyperliquid API base URL",
    )
    thegraph_gateway_url: str = Field(
        default="https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}",
        validation_alias=AliasChoices("THEGRAPH_GATEWAY_URL"),
        description="The Graph Gateway URL template",
    )
    uniswap_v3_mainnet_subgraph_id: str = Field(
        default="5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        validation_alias=AliasChoices("UNISWAP_V3_MAINNET_SUBGRAPH_ID"),
        description="Uniswap V3 Ethereum mainnet subgraph ID",
    )
    thegraph_uniswap_v3_studio_url: str = Field(
        default="https://api.studio.thegraph.com/query/48211/uniswap-v3-mainnet/version/latest",
        validation_alias=AliasChoices("THEGRAPH_UNISWAP_V3_STUDIO_URL"),
        description="TheGraph Uniswap V3 Studio URL (public, rate-limited)",
    )
    alchemy_mainnet_url_template: str = Field(
        default="https://eth-mainnet.g.alchemy.com/v2/{api_key}",
        validation_alias=AliasChoices("ALCHEMY_MAINNET_URL_TEMPLATE"),
        description="Alchemy Ethereum mainnet URL template",
    )
    defi_mvp_tokens: str = Field(
        default="",
        validation_alias=AliasChoices("DEFI_MVP_TOKENS"),
        description="Comma-separated list of DeFi MVP tokens",
    )

    # Deployment orchestration metadata (set by VM startup scripts)
    deployment_id: str = Field(
        default="",
        validation_alias=AliasChoices("DEPLOYMENT_ID"),
        description="Deployment ID for race condition detection",
    )
    shard_launched_at: str = Field(
        default="",
        validation_alias=AliasChoices("SHARD_LAUNCHED_AT"),
        description="Shard launch timestamp for race condition detection",
    )

    def get_cloud_target(self, category: str | None = None) -> CloudTarget:
        """Get CloudTarget for instruments service."""
        if category:
            category_upper = category.upper()
            if category_upper == "CEFI":
                bucket = self.gcs_bucket_cefi
            elif category_upper == "TRADFI":
                bucket = self.gcs_bucket_tradfi
            elif category_upper == "DEFI":
                bucket = self.gcs_bucket_defi
            elif category_upper == "SPORTS":
                bucket = self.gcs_bucket_sports
            else:
                raise ValueError(f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI, SPORTS")
        else:
            bucket = self.gcs_bucket

        return CloudTarget(
            project_id=self.gcp_project_id,
            gcs_bucket=bucket,
            bigquery_dataset=self.bigquery_dataset,
            bigquery_location=self.bigquery_location,
        )

    def is_test_environment(self) -> bool:
        """Check if the current environment is a test environment."""
        return self.environment.lower() in ["test", "testing"]

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI(self) -> str:
        return self.gcs_bucket_cefi

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI(self) -> str:
        return self.gcs_bucket_tradfi

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI(self) -> str:
        return self.gcs_bucket_defi

    @property
    def INSTRUMENTS_GCS_BUCKET_SPORTS(self) -> str:
        return self.gcs_bucket_sports

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI_TEST(self) -> str:
        return self.gcs_bucket_cefi_test or f"{self.gcs_bucket_cefi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI_TEST(self) -> str:
        return self.gcs_bucket_tradfi_test or f"{self.gcs_bucket_tradfi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI_TEST(self) -> str:
        return self.gcs_bucket_defi_test or f"{self.gcs_bucket_defi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_SPORTS_TEST(self) -> str:
        return self.gcs_bucket_sports_test or f"{self.gcs_bucket_sports}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET(self) -> str:
        return self.gcs_bucket

    @property
    def INSTRUMENTS_GCS_BUCKET_TEST(self) -> str:
        return self.gcs_bucket_test

    def get_bucket_for_category(self, category: str, test_mode: bool = False) -> str:
        """Get the GCS bucket name for a specific market category."""
        category_upper = category.upper()
        if category_upper not in ["CEFI", "TRADFI", "DEFI", "SPORTS"]:
            raise ValueError(f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI, SPORTS")
        bucket_name = (
            f"gcs_bucket_{category_upper.lower()}_test" if test_mode else f"gcs_bucket_{category_upper.lower()}"
        )
        bucket_raw: object = getattr(self, bucket_name, None)
        if bucket_raw:
            bucket: str = cast(str, bucket_raw)
            logger.debug("📦 Using bucket for %s: %s", category_upper, bucket)
            return bucket
        logger.warning("⚠️ Category-specific bucket not configured for %s. Using default bucket.", category_upper)
        return self.gcs_bucket_test if test_mode else self.gcs_bucket

    # =========================================================================
    # DOMAIN CONFIG PROTOCOL IMPLEMENTATION
    # =========================================================================

    @property
    def config(self) -> dict[str, object]:
        """Generic config dict for extensibility (implements DomainConfigProtocol)."""
        return self.model_dump()


_config: InstrumentsServiceConfig | None = None


def get_config() -> InstrumentsServiceConfig:
    """Get the singleton service config instance."""
    global _config
    if _config is None:
        _config = InstrumentsServiceConfig()
    return _config


instruments_config = get_config()
