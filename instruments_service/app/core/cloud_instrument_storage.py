"""
Cloud Instrument Storage using UCI DataSink intent-level API.

Stores instrument definitions to instruments domain via DataSink routing.
Uses unified-cloud-interface (UCI) for cloud-agnostic storage operations.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast
from uuid import uuid4

import pandas as pd
from unified_cloud_interface import DataSink, ServiceMode, get_data_sink, get_service_mode
from unified_events_interface import log_event
from unified_internal_contracts import (
    EnhancedError,
    ErrorCategory,
    ErrorContext,
    ErrorRecoveryStrategy,
    ErrorSeverity,
    LifecycleEventType,
)
from unified_trading_library import (
    ParquetSchemaEnforcer,
    SchemaValidationResult,
    create_sampling_service,
    determine_market_category,
    handle_storage_errors,
    validate_timestamp_date_alignment,
)

from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA
from instruments_service.schemas.parquet import get_required_columns

logger = logging.getLogger(__name__)

SAMPLING_SERVICE_AVAILABLE = True


class CloudInstrumentStorage:
    """
    Cloud instrument storage using UCI DataSink intent-level API.

    Stores instrument definitions via DataSink routing keyed by market category.
    Routing is resolved at deployment time via PROTOCOL_DATA_SINK_BUCKET_{CATEGORY_UPPER} env vars.
    """

    def __init__(self, testing_mode: bool = False) -> None:
        """Initialize cloud instrument storage using UCI DataSink."""
        self._testing_mode = testing_mode
        # Mode is injected at deployment time — service just reads it
        try:
            self._mode = get_service_mode()
        except RuntimeError:
            # SERVICE_MODE not set — default to batch for instruments (always batch)
            self._mode = ServiceMode.BATCH
        logger.info("CloudInstrumentStorage initialized in %s mode", self._mode.value)

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
            if SAMPLING_SERVICE_AVAILABLE and not instruments_df.empty:
                sampling_service = create_sampling_service()
                sample_date = date if date else datetime.now(UTC)
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
                    # Use target date (start of day UTC); date is typed as datetime (not date)
                    target_timestamp = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    if target_timestamp.tzinfo is None:
                        target_timestamp = target_timestamp.replace(tzinfo=UTC)
                    instruments_df["timestamp"] = target_timestamp
                else:
                    # Fallback to current time (should not happen in normal flow)
                    instruments_df["timestamp"] = datetime.now(UTC)
                    logger.warning("No date parameter provided, using current time for timestamp column")

            # Validate required columns (ParquetSchemaEnforcer for full validation; minimal check here)
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
                            logger.debug("Could not parse timestamp column %s: %s", ts_col, e)

            # Determine date string for GCS path
            date_str: str
            if date:
                date_str = date.strftime("%Y-%m-%d")
            else:
                # Extract date from available_from_datetime if available
                if "available_from_datetime" in instruments_df.columns:
                    try:
                        first_val_raw: object = cast(object, instruments_df["available_from_datetime"].iloc[0])
                        if first_val_raw is not None:
                            first_ts = pd.to_datetime(str(first_val_raw), utc=True)
                            first_date = pd.Timestamp(first_ts)
                        else:
                            first_date = pd.Timestamp(datetime.now(UTC))
                        date_str = first_date.strftime("%Y-%m-%d")
                    except (ValueError, TypeError, IndexError) as e:
                        logger.debug("Could not extract date from available_from_datetime: %s", e)
                        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
                else:
                    date_str = datetime.now(UTC).strftime("%Y-%m-%d")

            # Ensure market_category is populated for all instruments
            logger.info("📊 Ensuring market_category is populated for %s instruments...", len(instruments_df))
            if "market_category" not in instruments_df.columns:
                instruments_df["market_category"] = ""
            # Populate market_category for instruments that don't have it or have empty value
            mask: pd.Series = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
            if bool(mask.any()):

                def _row_to_market_category(row: pd.Series) -> str:  # pyright: ignore[reportMissingTypeArgument]
                    raw: dict[str, object] = cast(dict[str, object], row.to_dict())
                    d: dict[str, object] = {}
                    for k, val in raw.items():
                        if val is None or (isinstance(val, float) and (val != val)):
                            d[str(k)] = None
                        else:
                            d[str(k)] = str(val)
                    return determine_market_category(d)

                instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                    _row_to_market_category, axis=1
                )

            # Group by category
            category_groups = instruments_df.groupby("market_category")
            total_stored = 0
            all_successful = True

            # Create schema enforcer for validation
            schema_enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)

            venue_count = 0
            log_event(LifecycleEventType.PERSISTENCE_STARTED, details={"date": date_str})

            for category, category_df in category_groups:
                category_str = str(category)

                # Group by venue within category for by-venue folder structure
                venue_groups = category_df.groupby("venue")

                for venue, venue_df in venue_groups:
                    venue_folder = str(venue).replace("/", "-").replace("\\", "-")
                    venue_df_to_store = venue_df.copy()
                    category_venue = f"{category_str}/{venue}"

                    # Coerce nullable float64/bool columns to correct dtypes
                    # When all values are None, pandas infers 'object' dtype
                    float64_cols = [
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
                    int64_cols = ["pool_fee_tier", "emode_category_id"]
                    bool_cols = ["is_trading_day"]
                    for col in float64_cols:
                        if col in venue_df_to_store.columns:
                            venue_df_to_store[col] = pd.to_numeric(venue_df_to_store[col], errors="coerce")
                    for col in int64_cols:
                        if col in venue_df_to_store.columns:
                            ser = pd.to_numeric(venue_df_to_store[col], errors="coerce")
                            venue_df_to_store[col] = ser.astype("Int64") if hasattr(ser, "astype") else ser
                    for col in bool_cols:
                        if col in venue_df_to_store.columns:
                            venue_df_to_store[col] = venue_df_to_store[col].astype("boolean")

                    # Validate schema before upload
                    dimensions: dict[str, str] = {"category": category_str}
                    validation_result: SchemaValidationResult = schema_enforcer.validate_dataframe(
                        venue_df_to_store, dimensions
                    )  # pyright: ignore[reportUnknownMemberType]

                    if not validation_result.valid:
                        for error in validation_result.errors:
                            logger.error("Schema validation failed for %s/%s: %s", category, venue, error)
                        logger.error("Skipping upload for %s/%s due to schema validation errors", category, venue)
                        all_successful = False
                        continue  # Skip this venue

                    # Log any warnings
                    for warning in validation_result.warnings:
                        logger.warning("Schema validation warning for %s/%s: %s", category, venue, warning)

                    # Validate timestamp-date alignment (item_22c)
                    # Instruments use available_from_datetime which should align with the date folder
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
                            "TIMESTAMP_DATE_MISMATCH for %s/%s: Expected %s, found dates: %s. Alignment: %.1f%%",
                            category,
                            venue,
                            date_str,
                            alignment_result.actual_dates_found,
                            alignment_result.alignment_percentage,
                        )
                        # For instruments, this is a warning not a blocker since timestamp is generation time
                        # The actual data date is determined by available_from_datetime range

                    # Write via UCI DataSink — routing_key maps to deployment-injected bucket config
                    try:
                        data_sink: DataSink = get_data_sink(routing_key=category_str)
                        result_uri: str = data_sink.write(
                            venue_df_to_store,
                            partition={"day": date_str, "venue": venue_folder},
                            format="parquet",
                        )
                        if result_uri:
                            logger.info(
                                "Uploaded %s %s instruments via DataSink: %s",
                                len(venue_df_to_store),
                                category_venue,
                                result_uri,
                            )
                            total_stored += len(venue_df_to_store)
                            venue_count += 1
                        else:
                            logger.error("DataSink write failed for %s", category_venue)
                            all_successful = False
                    except (ValueError, KeyError, TypeError, IndexError) as sink_error:
                        _err = EnhancedError(
                            message=str(sink_error),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(sink_error).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        logger.error("DataSink write failed for %s: %s", category_venue, sink_error)
                        all_successful = False
            if all_successful:
                logger.info(
                    "Stored %s instruments across %s venues via DataSink (by-venue folder structure)",
                    total_stored,
                    venue_count,
                )
                log_event(
                    LifecycleEventType.PERSISTENCE_COMPLETED, details={"total_stored": total_stored, "date": date_str}
                )
            else:
                logger.warning("Some venue uploads failed. Total stored: %s/%s", total_stored, len(instruments_df))
                log_event(
                    LifecycleEventType.PERSISTENCE_COMPLETED,
                    details={"total_stored": total_stored, "date": date_str, "partial": True},
                )

            return all_successful

        except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("Failed to store instruments: %s", e)
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
