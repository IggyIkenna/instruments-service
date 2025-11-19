"""
Cloud Instrument Storage using unified-cloud-services.

Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
Uses unified-cloud-services directly for cloud operations.
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime, timezone

from instruments_service.settings import instruments_config
from instruments_service.schemas.parquet import get_required_columns
from unified_cloud_services import (
    determine_market_category,
    get_bucket_for_category,
    StandardizedDomainCloudService,
    CloudTarget,
    create_sampling_service,
    SchemaValidator,
)

logger = logging.getLogger(__name__)


class CloudInstrumentStorage:
    """
    Cloud instrument storage using unified-cloud-services.

    Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
    Uses unified-cloud-services directly (not MarketDataClient).
    Per unified architecture plan specification.
    """

    def __init__(self, cloud_target: CloudTarget = None):
        """Initialize cloud instrument storage with unified-cloud-services."""

        # Configure CloudTarget for market_data domain (instruments are part of market_data)
        # Use asia-northeast1 location per .env configuration (GCS: asia-northeast1-c, BigQuery: asia-northeast1)
        # Detect test environment and use test bucket if applicable
        if cloud_target is None:
            # Priority: ENVIRONMENT=test > pytest detection > default to prod
            # Check if we're in test mode (pytest or test environment)
            # Use test bucket if in test mode, otherwise use prod bucket
            # Only use test bucket if explicitly in test environment
            if instruments_config.is_test_environment():
                bucket_name = instruments_config.gcs_bucket_test
                logger.info(f"🧪 Test mode detected: Using test bucket {bucket_name}")
            else:
                bucket_name = instruments_config.gcs_bucket

            cloud_target = CloudTarget(
                project_id=instruments_config.gcp_project_id,
                gcs_bucket=bucket_name,
                bigquery_dataset=instruments_config.bigquery_dataset,
                bigquery_location=instruments_config.bigquery_location,
            )

        # Create instruments service using direct instantiation (canonical pattern)
        # Each domain has its own bucket and dataset (instruments domain)
        self.cloud_service = StandardizedDomainCloudService(
            domain="instruments", cloud_target=cloud_target
        )
        self.cloud_target = cloud_target

        logger.info(
            f"Initialized CloudInstrumentStorage: "
            f"project={cloud_target.project_id}, "
            f"bucket={cloud_target.gcs_bucket}, "
            f"dataset={cloud_target.bigquery_dataset}, "
            f"location={cloud_target.bigquery_location}"
        )

    def store_instruments(
        self,
        instruments_df: pd.DataFrame,
        table_name: str = "instruments",
        date: Optional[datetime] = None,
    ) -> bool:
        """
        Store instrument definitions to GCS (batch historical data only).

        BigQuery uploads removed - batch data now goes to GCS only.
        Live streaming data (analytics mode) will upload to BigQuery separately.

        Args:
            instruments_df: DataFrame with instrument definitions
            table_name: Table name (kept for compatibility, not used for BigQuery)
            date: Optional date for CSV sample filename (defaults to current date)

        Returns:
            True if storage successful

        Schema Reference:
            See `instruments_service.schemas.parquet` for complete Parquet schema definition.
            Expected columns are based on `InstrumentDefinition` Pydantic model.
            Parquet files are stored with headers (column names) and implicit schema.
        """
        try:
            # Generate CSV sample using centralized service (only in non-production)
            if instruments_df is not None and not instruments_df.empty:
                sampling_service = create_sampling_service()
                sample_date = date if date else datetime.now(timezone.utc)
                sampling_service.generate_csv_sample(
                    df=instruments_df,
                    filename_prefix="instruments",
                    metadata={"date": sample_date},
                )

            # Copy DataFrame to avoid SettingWithCopyWarning
            instruments_df = instruments_df.copy()

            # Add generation timestamp BEFORE validation (if not present)
            if "timestamp" not in instruments_df.columns:
                instruments_df["timestamp"] = datetime.now(timezone.utc)

            # Validate schema using unified-cloud-services SchemaValidator (DRY)
            # Domain-specific schema definition provides required columns list
            validator = SchemaValidator()
            required_columns = get_required_columns()
            result = validator.validate_dataframe_schema(
                df=instruments_df, required_columns=required_columns
            )

            if not result.valid:
                raise ValueError(f"Schema validation failed: {result.errors}")

            # Convert timestamp columns for GCS storage
            timestamp_columns = [
                "timestamp",
                "available_from_datetime",
                "available_to_datetime",
                "expiry",
            ]
            for ts_col in timestamp_columns:
                if ts_col in instruments_df.columns:
                    # Convert to timezone-naive UTC if needed
                    if instruments_df[ts_col].dtype.name.startswith("datetime64"):
                        ts_series = pd.to_datetime(instruments_df[ts_col])
                        if ts_series.dt.tz is not None:
                            instruments_df[ts_col] = ts_series.dt.tz_convert("UTC").dt.tz_localize(
                                None
                            )
                        instruments_df[ts_col] = instruments_df[ts_col].astype("datetime64[ns]")
                    elif instruments_df[ts_col].dtype == "object":
                        # Try to parse string timestamps
                        try:
                            instruments_df[ts_col] = pd.to_datetime(
                                instruments_df[ts_col]
                            ).dt.tz_localize(None)
                        except:
                            pass

            # Determine date string for GCS path
            if date:
                date_str = date.strftime("%Y-%m-%d")
            else:
                # Extract date from available_from_datetime if available
                if "available_from_datetime" in instruments_df.columns:
                    try:
                        first_date = pd.to_datetime(
                            instruments_df["available_from_datetime"].iloc[0]
                        )
                        date_str = first_date.strftime("%Y-%m-%d")
                    except:
                        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                else:
                    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Ensure market_category is populated for all instruments
            logger.info(
                f"📊 Ensuring market_category is populated for {len(instruments_df)} instruments..."
            )
            if "market_category" not in instruments_df.columns:
                instruments_df["market_category"] = ""
            # Populate market_category for instruments that don't have it or have empty value
            mask = (instruments_df["market_category"].isna()) | (
                instruments_df["market_category"] == ""
            )
            if mask.any():
                instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                    lambda row: determine_market_category(row.to_dict()), axis=1
                )

            # Group by category
            category_groups = instruments_df.groupby("market_category")
            total_stored = 0
            all_successful = True

            # Detect test mode for bucket selection
            is_test = instruments_config.is_test_environment()

            # Upload each category group to its respective bucket
            for category, category_df in category_groups:
                try:
                    # Get bucket for this category
                    category_bucket = get_bucket_for_category(category, test_mode=is_test)

                    # Create cloud service for this category bucket
                    category_cloud_target = CloudTarget(
                        project_id=self.cloud_target.project_id,
                        gcs_bucket=category_bucket,
                        bigquery_dataset=self.cloud_target.bigquery_dataset,
                        bigquery_location=self.cloud_target.bigquery_location,
                    )
                    category_cloud_service = StandardizedDomainCloudService(
                        domain="instruments", cloud_target=category_cloud_target
                    )

                    # market_category is now a real field, keep it in the stored data
                    category_df_to_store = category_df.copy()

                    # Upload to GCS in instrument_availability/by_date structure
                    # Path format: instrument_availability/by_date/day-YYYY-MM-DD/instruments.parquet
                    gcs_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"

                    category_cloud_service.upload_to_gcs(
                        data=category_df_to_store, gcs_path=gcs_path, format="parquet"
                    )

                    logger.info(
                        f"✅ Uploaded {len(category_df)} {category} instruments to GCS: "
                        f"{category_bucket}/{gcs_path}"
                    )
                    total_stored += len(category_df)

                except Exception as gcs_error:
                    logger.error(f"❌ GCS upload failed for {category} category: {gcs_error}")
                    all_successful = False

            if all_successful:
                logger.info(
                    f"✅ Stored {total_stored} instruments to category-specific buckets "
                    f"(batch historical data)"
                )
            else:
                logger.warning(
                    f"⚠️ Some category uploads failed. Total stored: {total_stored}/{len(instruments_df)}"
                )

            return all_successful

        except Exception as e:
            logger.error(f"Failed to store instruments: {e}")
            return False

    def query_instruments(
        self,
        venue: Optional[str] = None,
        instrument_type: Optional[str] = None,
        table_name: str = "instruments",
    ) -> pd.DataFrame:
        """
        Query stored instruments from GCS (batch historical data).

        Note: BigQuery queries removed - batch data is now in GCS only.
        For live streaming data queries, use analytics mode endpoints.

        Args:
            venue: Optional venue filter (e.g., "BINANCE-FUTURES")
            instrument_type: Optional instrument type filter (e.g., "PERPETUAL")
            table_name: Table name (kept for compatibility, not used)

        Returns:
            DataFrame with stored instruments (empty DataFrame - GCS query not implemented)
        """
        logger.warning(
            "⚠️ BigQuery query removed - batch instruments are stored in GCS only. "
            "Use GCS download methods or live streaming analytics endpoints for queries."
        )
        return pd.DataFrame()
