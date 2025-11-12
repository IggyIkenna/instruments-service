"""
Cloud Instrument Storage using unified-cloud-services.

Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
Uses unified-cloud-services directly for cloud operations.
"""

import pandas as pd
import logging
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone
import os

logger = logging.getLogger(__name__)

# Import unified-cloud-services (direct dependency)
try:
    from unified_cloud_services import StandardizedDomainCloudService, CloudTarget

    UNIFIED_CLOUD_SERVICES_AVAILABLE = True
    logger.info("unified-cloud-services is available")
except ImportError:
    UNIFIED_CLOUD_SERVICES_AVAILABLE = False
    logger.warning("unified-cloud-services not available")

# Import centralized sampling service from unified-cloud-services
try:
    from unified_cloud_services import create_sampling_service

    SAMPLING_SERVICE_AVAILABLE = True
except ImportError:
    SAMPLING_SERVICE_AVAILABLE = False
    logger.debug("Sampling service not available")


class CloudInstrumentStorage:
    """
    Cloud instrument storage using unified-cloud-services.

    Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
    Uses unified-cloud-services directly (not MarketDataClient).
    Per unified architecture plan specification.
    """

    def __init__(self, cloud_target: CloudTarget = None):
        """Initialize cloud instrument storage with unified-cloud-services."""
        if not UNIFIED_CLOUD_SERVICES_AVAILABLE:
            raise ImportError(
                "unified-cloud-services not available. "
                "Install unified-cloud-services package: "
                "pip install -e ../unified-cloud-services"
            )

        # Configure CloudTarget for market_data domain (instruments are part of market_data)
        # Use asia-northeast1 location per .env configuration (GCS: asia-northeast1-c, BigQuery: asia-northeast1)
        # Detect test environment and use test bucket if applicable
        if cloud_target is None:
            # Check if we're in test mode (pytest or test environment)
            # Priority: ENVIRONMENT=test > pytest detection > default to prod
            environment = os.getenv("ENVIRONMENT", "development").lower()
            test_bucket = os.getenv("INSTRUMENTS_GCS_BUCKET_TEST")
            prod_bucket = os.getenv("INSTRUMENTS_GCS_BUCKET", "instruments-store")

            # Only use test bucket if explicitly in test environment
            is_test = (
                environment in ["test", "testing"]  # Explicit test environment
                or "pytest" in os.environ.get("_", "")
                or os.getenv("PYTEST_CURRENT_TEST") is not None
            )

            # Use test bucket if in test mode, otherwise use prod bucket
            if is_test:
                bucket_name = test_bucket or "instruments-store-test"
                logger.info(f"🧪 Test mode detected: Using test bucket {bucket_name}")
            else:
                bucket_name = prod_bucket

            cloud_target = CloudTarget(
                project_id=os.getenv("GCP_PROJECT_ID", "central-element-323112"),
                gcs_bucket=bucket_name,
                bigquery_dataset=os.getenv("INSTRUMENTS_BIGQUERY_DATASET", "instruments"),
                bigquery_location=os.getenv(
                    "BIGQUERY_LOCATION", "asia-northeast1"
                ),  # Default to asia-northeast1 per .env
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
            if (
                SAMPLING_SERVICE_AVAILABLE
                and instruments_df is not None
                and not instruments_df.empty
            ):
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
            try:
                from unified_cloud_services import SchemaValidator
                from instruments_service.schemas.parquet import get_required_columns

                validator = SchemaValidator()
                required_columns = get_required_columns()
                result = validator.validate_dataframe_schema(
                    df=instruments_df, required_columns=required_columns
                )

                if not result.valid:
                    raise ValueError(f"Schema validation failed: {result.errors}")
            except ImportError:
                # Fallback if schema modules not available
                required_columns = [
                    "instrument_key",
                    "venue",
                    "instrument_type",
                    "available_from_datetime",
                ]
                missing_columns = [
                    col for col in required_columns if col not in instruments_df.columns
                ]
                if missing_columns:
                    raise ValueError(f"Missing required columns: {missing_columns}")

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

            # Upload to GCS in instrument_availability/by_date structure
            # Path format: instrument_availability/by_date/day-YYYY-MM-DD/instruments.parquet
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

            gcs_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"

            try:
                self.cloud_service.upload_to_gcs(
                    data=instruments_df, gcs_path=gcs_path, format="parquet"
                )
                logger.info(f"✅ Uploaded {len(instruments_df)} instruments to GCS: {gcs_path}")
            except Exception as gcs_error:
                logger.error(f"❌ GCS upload failed: {gcs_error}")
                return False

            logger.info(
                f"✅ Stored {len(instruments_df)} instruments to GCS (batch historical data)"
            )

            return True

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
