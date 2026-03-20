"""
Service-level configuration for instruments-service.

InstrumentsServiceConfig (Pydantic BaseSettings) and singleton access.
"""

import logging
from typing import cast

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import SettingsConfigDict
from unified_config_interface import UnifiedCloudConfig

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

    sink_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET"),
        description="Primary sink bucket for instruments",
    )
    sink_bucket_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TEST"),
        description="Test sink bucket for instruments",
    )
    sink_bucket_cefi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
        description="Sink bucket for CEFI instruments",
    )
    sink_bucket_tradfi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
        description="Sink bucket for TRADFI instruments",
    )
    sink_bucket_defi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
        description="Sink bucket for DEFI instruments",
    )
    sink_bucket_sports: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_SPORTS"),
        description="Sink bucket for SPORTS instruments",
    )
    sink_bucket_cefi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI_TEST"),
        description="Test sink bucket for CEFI instruments",
    )
    sink_bucket_tradfi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI_TEST"),
        description="Test sink bucket for TRADFI instruments",
    )
    sink_bucket_defi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI_TEST"),
        description="Test sink bucket for DEFI instruments",
    )
    sink_bucket_sports_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_SPORTS_TEST"),
        description="Test sink bucket for SPORTS instruments",
    )

    analytics_dataset: str = Field(
        default="instruments",
        validation_alias=AliasChoices("INSTRUMENTS_BIGQUERY_DATASET", "BIGQUERY_DATASET"),
        description="Analytics dataset for instruments",
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
        default="",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_ARB_URL"),
        description="The Graph Uniswap V3 URL for Arbitrum (adapter auto-constructs from API key + subgraph ID)",
    )
    uniswap_v3_graph_base_url: str = Field(
        default="",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_BASE_URL"),
        description="The Graph Uniswap V3 URL for Base (adapter auto-constructs from API key + subgraph ID)",
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

    config_store_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("CONFIG_STORE_BUCKET"),
        description="Cloud storage bucket for dynamic domain config store",
    )

    catalogue_path_override: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_CATALOGUE_PATH"),
        description="Override path to data catalogue YAML (for tests/CI)",
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

    # =========================================================================
    # VALIDATORS
    # =========================================================================

    @model_validator(mode="after")
    def _compute_bucket_defaults(self) -> "InstrumentsServiceConfig":
        """Compute bucket names from gcp_project_id when not explicitly set via env vars.

        Convention:
          - Real:  instruments-store-{category}-{gcp_project_id}
          - Test:  instruments-store-{category}-test-{gcp_project_id}
          - Generic real:  instruments-store-{gcp_project_id}
          - Generic test:  instruments-store-test-{gcp_project_id}
        """
        pid = self.gcp_project_id
        if not pid:
            # In mock/test mode gcp_project_id may be empty — buckets stay as-is
            # (tests set explicit env vars). In production this is a hard error
            # caught by callers that need a bucket.
            return self

        _updates: dict[str, str] = {}

        if not self.sink_bucket:
            _updates["sink_bucket"] = f"instruments-store-{pid}"  # CORRECT-LOCAL
        if not self.sink_bucket_test:
            _updates["sink_bucket_test"] = f"instruments-store-test-{pid}"  # CORRECT-LOCAL

        _categories = ["cefi", "tradfi", "defi", "sports"]
        for cat in _categories:
            field_real = f"sink_bucket_{cat}"
            field_test = f"sink_bucket_{cat}_test"
            if not getattr(self, field_real):
                _updates[field_real] = f"instruments-store-{cat}-{pid}"  # CORRECT-LOCAL
            if not getattr(self, field_test):
                _updates[field_test] = f"instruments-store-{cat}-test-{pid}"  # CORRECT-LOCAL

        if _updates:
            return self.model_copy(update=_updates)
        return self

    # =========================================================================
    # DEFI CONFIG VALIDATION
    # =========================================================================

    def validate_defi_config(self) -> None:
        """Validate that DeFi-specific configuration fields are set.

        Call this before any operation that requires DeFi RPC/Graph access.
        Raises ValueError if ethereum_rpc_url or uniswap_v3_graph_url is empty.
        """
        missing: list[str] = []
        if not self.ethereum_rpc_url:
            missing.append("ETHEREUM_RPC_URL")
        if not self.uniswap_v3_graph_url:
            missing.append("UNISWAP_V3_GRAPH_URL")
        if missing:
            raise ValueError(
                f"DeFi configuration incomplete — missing env vars: {', '.join(missing)}. "
                "These are required for DeFi instrument generation."
            )

    def is_test_environment(self) -> bool:
        """Check if the current environment is a test environment."""
        return self.environment.lower() in ["test", "testing"]

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI(self) -> str:
        return self.sink_bucket_cefi

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI(self) -> str:
        return self.sink_bucket_tradfi

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI(self) -> str:
        return self.sink_bucket_defi

    @property
    def INSTRUMENTS_GCS_BUCKET_SPORTS(self) -> str:
        return self.sink_bucket_sports

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI_TEST(self) -> str:
        return self.sink_bucket_cefi_test

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI_TEST(self) -> str:
        return self.sink_bucket_tradfi_test

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI_TEST(self) -> str:
        return self.sink_bucket_defi_test

    @property
    def INSTRUMENTS_GCS_BUCKET_SPORTS_TEST(self) -> str:
        return self.sink_bucket_sports_test

    @property
    def INSTRUMENTS_GCS_BUCKET(self) -> str:
        return self.sink_bucket

    @property
    def INSTRUMENTS_GCS_BUCKET_TEST(self) -> str:
        return self.sink_bucket_test

    def get_bucket_for_category(self, category: str, test_mode: bool = False) -> str:
        """Get the sink bucket name for a specific market category.

        After model_validator, all category buckets are computed from gcp_project_id.
        If a bucket is still empty, gcp_project_id was never set — raise immediately.
        """
        category_upper = category.upper()
        if category_upper not in ["CEFI", "TRADFI", "DEFI", "SPORTS"]:  # CORRECT-LOCAL
            raise ValueError(f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI, SPORTS")
        field_name = (
            f"sink_bucket_{category_upper.lower()}_test" if test_mode else f"sink_bucket_{category_upper.lower()}"
        )
        bucket: str = cast(str, getattr(self, field_name, ""))
        if not bucket:
            raise ValueError(
                f"Bucket for category {category_upper} (test_mode={test_mode}) is not configured. "
                "Set GCP_PROJECT_ID or the category-specific env var "
                f"(INSTRUMENTS_GCS_BUCKET_{category_upper}{'_TEST' if test_mode else ''})."
            )
        logger.debug("Using bucket for %s: %s", category_upper, bucket)
        return bucket

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
