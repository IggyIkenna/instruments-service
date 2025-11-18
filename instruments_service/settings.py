"""
Configuration for Instruments Service

Unified instrument configuration with all instruments, mappings, and metadata in one place.
"""

from typing import Optional
from pydantic import Field, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

from unified_cloud_services import CloudTarget


class InstrumentsServiceConfig(BaseSettings):
    """
    Service-level configuration for instruments-service.

    Extends BaseServiceConfig with instruments-specific settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields from .env
    )

    service_name: str = Field(default="instruments-service", description="Service name")
    environment: str = Field(
        default="development",
        validation_alias=AliasChoices("ENVIRONMENT"),
        description="Environment (development, test, production)",
    )

    # GCP configuration (common across all services)
    google_application_credentials_path: str = Field(
        validation_alias=AliasChoices("GOOGLE_APPLICATION_CREDENTIALS"),
        description="Filepath to GCP credentials JSON file",
    )
    gcp_project_id: str = Field(
        validation_alias=AliasChoices("GCP_PROJECT_ID"),
        description="GCP project ID",
    )

    # GCS configuration
    gcs_region: str = Field(
        validation_alias=AliasChoices("GCS_REGION"),
        description="GCS region",
    )
    gcs_location: str = Field(
        validation_alias=AliasChoices("GCS_LOCATION"),
        description="GCS location",
    )
    gcs_bucket: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET"),
        description="GCS bucket for instruments",
    )
    gcs_bucket_test: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TEST"),
        description="GCS bucket for instruments",
    )
    # Category-specific buckets for independent batch processing
    gcs_bucket_cefi: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
        description="GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
        description="GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
        description="GCS bucket for DEFI instruments",
    )

    # BigQuery configuration
    bigquery_dataset: str = Field(
        validation_alias=AliasChoices("INSTRUMENTS_BIGQUERY_DATASET"),
        description="BigQuery dataset for instruments",
    )
    bigquery_location: str = Field(
        validation_alias=AliasChoices("BIGQUERY_LOCATION"),
        description="BigQuery dataset location",
    )

    # Instruments-specific configuration
    enable_ccxt_integration: bool = Field(
        default=True, description="Enable CCXT metadata enrichment"
    )
    enable_metadata_caching: bool = Field(default=True, description="Enable metadata caching")
    cache_ttl_hours: int = Field(default=24, description="Cache TTL in hours")
    max_batch_size: int = Field(default=1000, description="Maximum batch size for processing")
    lookback_days: int = Field(default=0, description="Lookback days for batch processing")
    # CSV Sampling Configuration (only used in development mode)
    enable_csv_sampling: bool = Field(default=True, description="Enable CSV sampling")
    csv_sample_size: int = Field(default=20000, description="CSV sample size")
    csv_sample_dir: str = Field(default="./data/samples", description="CSV sample directory")

    # API Key Configuration (Secret Manager)
    # All API keys are stored in GCP Secret Manager for security
    # The service automatically retrieves keys using secret names below
    # NEVER commit actual API keys to this file

    # Secret Manager secret names (keys stored in GCP Secret Manager)
    tardis_secret_name: str = Field(
        validation_alias=AliasChoices("TARDIS_SECRET_NAME"),
        description="Tardis API key secret name",
    )
    databento_secret_name: str = Field(
        validation_alias=AliasChoices("DATABENTO_SECRET_NAME"),
        description="Databento API key secret name",
    )
    aavescan_secret_name: str = Field(
        validation_alias=AliasChoices("AAVESCAN_SECRET_NAME"),
        description="Aavescan API key secret name",
    )
    alchemy_secret_name: str = Field(
        validation_alias=AliasChoices("ALCHEMY_SECRET_NAME"),
        description="Alchemy API key secret name",
    )
    graph_seceret_name: str = Field(
        validation_alias=AliasChoices("GRAPH_SECRET_NAME"), description="Graph API key secret name"
    )

    # URLS
    ethereum_rpc_url: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ETHEREUM_RPC_URL",
        ),
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
        description="The Graph Uniswap V3 URL for Base",
    )

    # DeFi MVP tokens configuration
    defi_mvp_tokens: str = Field(
        default="",
        validation_alias=AliasChoices("DEFI_MVP_TOKENS"),
        description="Comma-separated list of DeFi MVP tokens",
    )

    # ClickUp Configuration
    # User IDs for assignees
    clickup_secret_name: str = Field(
        validation_alias=AliasChoices("CLICKUP_SECRET_NAME"),
        description="ClickUp API key secret name",
    )
    clickup_list_id: Optional[str] = Field(
        validation_alias=AliasChoices("clickup_list_id_instruments_service"),
        description="ClickUp List ID",
    )
    clickup_user_id_ikenna: str = Field(
        default="254573729",
        validation_alias=AliasChoices("clickup_user_id_ikenna"),
        description="ClickUp User ID for Ikenna",
    )
    clickup_user_id_harsh: str = Field(
        default="100698878",
        validation_alias=AliasChoices("clickup_user_id_harsh"),
        description="ClickUp User ID for Harsh",
    )
    clickup_user_id_femi: str = Field(
        default="100698756",
        validation_alias=AliasChoices("clickup_user_id_femi"),
        description="ClickUp User ID for Femi",
    )
    clickup_user_id_daniel: str = Field(
        default="36559682",
        validation_alias=AliasChoices("clickup_user_id_daniel"),
        description="ClickUp User ID for Daniel",
    )

    def get_cloud_target(self, category: Optional[str] = None):
        """
        Get CloudTarget for instruments service.

        Args:
            category: Optional market category ("CEFI", "TRADFI", "DEFI") to use category-specific bucket

        Returns:
            CloudTarget with appropriate bucket for category
        """

        # Determine bucket based on category
        if category:
            category_upper = category.upper()
            if category_upper == "CEFI":
                bucket = self.gcs_bucket_cefi
            elif category_upper == "TRADFI":
                bucket = self.gcs_bucket_tradfi
            elif category_upper == "DEFI":
                bucket = self.gcs_bucket_defi
            else:
                raise ValueError(
                    f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI"
                )
        else:
            bucket = self.gcs_bucket

        return CloudTarget(
            project_id=self.gcp_project_id,
            gcs_bucket=bucket,
            bigquery_dataset=self.bigquery_dataset,
            bigquery_location=self.bigquery_location,
        )

    def is_test_environment(self) -> bool:
        """
        Check if the current environment is a test environment.

        Returns:
            True if environment is "test", False otherwise
        """
        return self.environment.lower() in ["test", "testing"]


env_configs = InstrumentsServiceConfig()
