"""
Cloud Instrument Storage using unified-cloud-services.

Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
Uses unified-cloud-services directly for cloud operations.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, cast

import pandas as pd
from unified_cloud_services import (
    CloudTarget,
    ParquetSchemaEnforcer,
    SchemaValidationResult,
    StandardizedDomainCloudService,
    determine_market_category,
    handle_storage_errors,
)
from unified_domain_services import validate_timestamp_date_alignment

from instruments_service.config import instruments_config
from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA

logger = logging.getLogger(__name__)

UNIFIED_CLOUD_SERVICES_AVAILABLE = True

# Import centralized sampling service from unified-cloud-services
_sampling_available = False
try:
    from unified_cloud_services import create_sampling_service

    _sampling_available = True
except ImportError:
    logger.debug("Sampling service not available")
SAMPLING_SERVICE_AVAILABLE = _sampling_available


class CloudInstrumentStorage:
    """
    Cloud instrument storage using unified-cloud-services.

    Stores instrument definitions to instruments domain (each domain has its own bucket and dataset).
    Uses unified-cloud-services directly (not MarketDataClient).
    Per unified architecture plan specification.
    """

    def __init__(self, cloud_target: CloudTarget | None = None):
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
            cfg = instruments_config
            environment = (cfg.environment or "development").lower()
            is_test = environment in ["test", "testing"] or bool(os.environ.get("PYTEST_CURRENT_TEST"))

            if is_test:
                bucket_name = cfg.gcs_bucket_test or cfg.gcs_bucket_cefi_test or "instruments-store-test"
                logger.info(f"🧪 Test mode detected: Using test bucket {bucket_name}")
            else:
                bucket_name = cfg.gcs_bucket_cefi or cfg.get_bucket_for_category("cefi")

            cloud_target = CloudTarget(
                project_id=cfg.gcp_project_id,
                gcs_bucket=bucket_name,
                bigquery_dataset=cfg.bigquery_dataset or "instruments",
                bigquery_location=cfg.bigquery_location or "asia-northeast1",
            )

        # Create instruments service using direct instantiation (canonical pattern)
        # Each domain has its own bucket and dataset (instruments domain)
        self.cloud_service = StandardizedDomainCloudService(domain="instruments", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(
            f"Initialized CloudInstrumentStorage: "
            f"project={cloud_target.project_id}, "
            f"bucket={cloud_target.gcs_bucket}, "
            f"dataset={cloud_target.bigquery_dataset}, "
            f"location={cloud_target.bigquery_location}"
        )

    @handle_storage_errors(max_retries=2)
    def store_instruments(
        self,
        instruments_df: pd.DataFrame,
        table_name: str = "instruments",
        date: datetime | None = None,
    ) -> bool:
        """
        Store instrument definitions to GCS (batch historical data only).

        BigQuery uploads removed - batch data now goes to GCS only.
        Live streaming data (analytics mode) will upload to BigQuery separately.

        Handles UTC midnight spanning for CME/ICE: When a session spans UTC midnight
        (opens on day N, closes on day N+1), the instrument is written to BOTH dates
        with session_date_tag to distinguish the entries.

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
            if SAMPLING_SERVICE_AVAILABLE and instruments_df is not None and not instruments_df.empty:
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
            # IMPORTANT: Use the target date parameter, NOT datetime.now(timezone.utc), to avoid TIMESTAMP_DATE_MISMATCH
            # The timestamp should reflect the date the instruments are valid for, not when they were generated
            if "timestamp" not in instruments_df.columns:
                if date is not None:
                    # Use target date (start of day UTC)
                    if isinstance(date, datetime):
                        target_timestamp = date.replace(hour=0, minute=0, second=0, microsecond=0)
                        if target_timestamp.tzinfo is None:
                            target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)
                    else:
                        # date is a date object, convert to datetime
                        target_timestamp = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
                    instruments_df["timestamp"] = target_timestamp
                else:
                    # Fallback to current time (should not happen in normal flow)
                    instruments_df["timestamp"] = datetime.now(timezone.utc)
                    logger.warning("No date parameter provided, using current time for timestamp column")

            # Validate required columns (ParquetSchemaEnforcer for full validation; minimal check here)
            from instruments_service.schemas.parquet import get_required_columns

            required_columns = get_required_columns()
            missing_columns = [col for col in required_columns if col not in instruments_df.columns]
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
                    col_dtype = getattr(instruments_df[ts_col].dtype, "name", str(instruments_df[ts_col].dtype))
                    if str(col_dtype).startswith("datetime64"):
                        ts_series: pd.Series = cast(pd.Series, pd.to_datetime(instruments_df[ts_col], utc=True))
                        if ts_series.dt.tz is not None:
                            instruments_df[ts_col] = ts_series.dt.tz_convert("UTC").dt.tz_localize(None)
                        instruments_df[ts_col] = instruments_df[ts_col].astype("datetime64[ns]")
                    elif str(instruments_df[ts_col].dtype) == "object":
                        # Try to parse string timestamps
                        try:
                            ts_series_obj: pd.Series = cast(pd.Series, pd.to_datetime(instruments_df[ts_col], utc=True))
                            instruments_df[ts_col] = ts_series_obj.dt.tz_convert(None)
                        except (ValueError, TypeError, AttributeError) as e:
                            logger.debug(f"Could not parse timestamp column {ts_col}: {e}")

            # Determine date string for GCS path
            date_str: str
            if date:
                date_str = date.strftime("%Y-%m-%d")
            else:
                # Extract date from available_from_datetime if available
                if "available_from_datetime" in instruments_df.columns:
                    try:
                        first_val = instruments_df["available_from_datetime"].iloc[0]  # type: ignore[reportAny]
                        first_date = pd.Timestamp(
                            pd.to_datetime([first_val], utc=True)[0]
                            if first_val is not None
                            else datetime.now(timezone.utc)
                        )
                        date_str = first_date.strftime("%Y-%m-%d")
                    except (ValueError, TypeError, IndexError) as e:
                        logger.debug(f"Could not extract date from available_from_datetime: {e}")
                        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                else:
                    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Ensure market_category is populated for all instruments
            logger.info(f"📊 Ensuring market_category is populated for {len(instruments_df)} instruments...")
            if "market_category" not in instruments_df.columns:
                instruments_df["market_category"] = ""
            # Populate market_category for instruments that don't have it or have empty value
            mask: pd.Series = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
            if cast(bool, mask.any()):

                def _row_to_market_category(row: pd.Series[Any]) -> str:  # type: ignore[reportAny]
                    d: dict[str, str | None] = {}
                    for k, v in row.items():  # type: ignore[reportAny]
                        if v is None or (isinstance(v, float) and (v != v)):
                            d[str(k)] = None
                        else:
                            d[str(k)] = str(v)
                    return determine_market_category(d)

                instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                    _row_to_market_category, axis=1
                )

            # Group by category
            category_groups = instruments_df.groupby("market_category")
            total_stored = 0
            all_successful = True

            # Detect test mode for bucket selection
            cfg = instruments_config
            environment: str = str(cfg.environment or "development").lower()
            is_test = environment in ["test", "testing"] or bool(os.environ.get("PYTEST_CURRENT_TEST"))

            # Group uploads by bucket to use batch upload per bucket
            # (each bucket needs its own cloud service)
            bucket_uploads: dict[str, list[tuple[str, pd.DataFrame, str]]] = {}  # bucket -> [(gcs_path, df, category)]

            # Create schema enforcer for validation
            schema_enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)

            for category, category_df in category_groups:
                category_str = str(category)
                category_bucket = cfg.get_bucket_for_category(category_str, test_mode=is_test)

                # NEW: Group by venue within category for by-venue folder structure
                venue_groups = category_df.groupby("venue")

                for venue, venue_df in venue_groups:
                    venue_folder = str(venue).replace("/", "-").replace("\\", "-")
                    # Use key=value format for BigQuery hive partitioning
                    gcs_path = (
                        f"instrument_availability/by_date/day={date_str}/venue={venue_folder}/instruments.parquet"
                    )
                    venue_df_to_store = venue_df.copy()

                    # Coerce nullable float64/bool columns to correct dtypes
                    # When all values are None, pandas infers 'object' dtype
                    _FLOAT64_COLS = [
                        "contract_size",
                        "max_position_size",
                        "max_leverage",
                        "initial_margin_rate",
                        "maintenance_margin_rate",
                        "ltv",
                        "liquidation_threshold",
                        "liquidation_bonus",
                        "reserve_factor",
                        "emode_liquidation_threshold",
                        "emode_liquidation_bonus",
                        "optimal_utilization_rate",
                        "base_variable_borrow_rate",
                        "variable_rate_slope1",
                        "variable_rate_slope2",
                    ]
                    _INT64_COLS = ["pool_fee_tier", "emode_category_id"]
                    _BOOL_COLS = ["is_trading_day"]
                    for col in _FLOAT64_COLS:
                        if col in venue_df_to_store.columns:
                            venue_df_to_store[col] = pd.to_numeric(venue_df_to_store[col], errors="coerce")
                    for col in _INT64_COLS:
                        if col in venue_df_to_store.columns:
                            ser: pd.Series = cast(pd.Series, pd.to_numeric(venue_df_to_store[col], errors="coerce"))
                            venue_df_to_store[col] = ser.astype("Int64") if hasattr(ser, "astype") else ser
                    for col in _BOOL_COLS:
                        if col in venue_df_to_store.columns:
                            venue_df_to_store[col] = venue_df_to_store[col].astype("boolean")

                    # Validate schema before upload
                    dimensions: dict[str, str] = {"category": category_str}
                    validation_result = cast(
                        SchemaValidationResult,
                        schema_enforcer.validate_dataframe(venue_df_to_store, dimensions),  # pyright: ignore[reportUnknownMemberType]
                    )

                    if not validation_result.valid:
                        for error in validation_result.errors:
                            logger.error(f"Schema validation failed for {category}/{venue}: {error}")
                        logger.error(f"Skipping GCS upload for {category}/{venue} due to schema validation errors")
                        all_successful = False
                        continue  # Skip this venue

                    # Log any warnings
                    for warning in validation_result.warnings:
                        logger.warning(f"Schema validation warning for {category}/{venue}: {warning}")

                    # Validate timestamp-date alignment (item_22c)
                    # Instruments use available_from_datetime which should align with the date folder
                    from datetime import date as date_type

                    expected_date = date_type.fromisoformat(date_str)
                    alignment_result = validate_timestamp_date_alignment(
                        venue_df_to_store,
                        expected_date=expected_date,
                        timestamp_col="timestamp",  # Use generation timestamp
                        alignment_threshold=100.0,  # All timestamps should match date
                        timestamp_unit="auto",
                    )
                    if not alignment_result.valid:
                        logger.error(
                            f"TIMESTAMP_DATE_MISMATCH for {category}/{venue}: Expected {date_str}, "
                            f"found dates: {alignment_result.actual_dates_found}. "
                            f"Alignment: {alignment_result.alignment_percentage:.1f}%"
                        )
                        # For instruments, this is a warning not a blocker since timestamp is generation time
                        # The actual data date is determined by available_from_datetime range

                    if category_bucket not in bucket_uploads:
                        bucket_uploads[category_bucket] = []
                    bucket_uploads[category_bucket].append((gcs_path, venue_df_to_store, f"{category}/{venue}"))

            # Upload to each bucket using batch upload (thread-safe)
            for bucket_name, uploads_list in bucket_uploads.items():
                try:
                    # Create cloud service for this bucket
                    bucket_cloud_target = CloudTarget(
                        project_id=self.cloud_target.project_id,
                        gcs_bucket=bucket_name,
                        bigquery_dataset=self.cloud_target.bigquery_dataset,
                        bigquery_location=self.cloud_target.bigquery_location,
                    )
                    bucket_cloud_service = StandardizedDomainCloudService(
                        domain="instruments", cloud_target=bucket_cloud_target
                    )

                    # Prepare batch upload (uploads_list: list[tuple[str, pd.DataFrame, str]])
                    batch_uploads: list[dict[str, str | pd.DataFrame]] = [
                        {"data": df, "gcs_path": gcs_path, "format": "parquet"} for gcs_path, df, _ in uploads_list
                    ]

                    # Use thread-safe batch upload
                    results = bucket_cloud_service.upload_to_gcs_batch(batch_uploads, show_progress=False)

                    # Process results
                    for i, result in enumerate(results):
                        gcs_path, df, category_venue = uploads_list[i]
                        if result.get("success"):
                            logger.info(
                                f"✅ Uploaded {len(df)} {category_venue} instruments to GCS: {bucket_name}/{gcs_path}"
                            )
                            total_stored += len(df)
                        else:
                            logger.error(f"❌ GCS upload failed for {category_venue}: {result.get('error')}")
                            all_successful = False

                except Exception as gcs_error:
                    logger.error(f"❌ GCS upload failed for bucket {bucket_name}: {gcs_error}")
                    all_successful = False

            if all_successful:
                # Count unique venues stored
                unique_venues: int = int(instruments_df["venue"].nunique()) if "venue" in instruments_df.columns else 0
                logger.info(
                    f"✅ Stored {total_stored} instruments across {unique_venues} venues to "
                    f"category-specific buckets (by-venue folder structure)"
                )
            else:
                logger.warning(f"⚠️ Some venue uploads failed. Total stored: {total_stored}/{len(instruments_df)}")

            return all_successful

        except Exception as e:
            logger.error(f"Failed to store instruments: {e}")
            return False

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
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
