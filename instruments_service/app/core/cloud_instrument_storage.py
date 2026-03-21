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
from unified_cloud_interface import DataSink, RuntimeMode, get_data_sink, get_service_mode
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
            self._mode = RuntimeMode.BATCH
        logger.info("CloudInstrumentStorage initialized in %s mode", self._mode.value)

    def _generate_csv_sample(self, instruments_df: pd.DataFrame, date: datetime | None) -> None:
        """Generate CSV sample using centralized service (only in non-production)."""
        if SAMPLING_SERVICE_AVAILABLE and not instruments_df.empty:
            sampling_service = create_sampling_service()
            sample_date = date if date else datetime.now(UTC)
            sampling_service.generate_csv_sample(
                df=instruments_df,
                filename_prefix="instruments",
                metadata={"date": sample_date},
            )

    def _add_generation_timestamp(self, instruments_df: pd.DataFrame, date: datetime | None) -> None:
        """Add generation timestamp column if not present, using target date."""
        if "timestamp" in instruments_df.columns:
            return
        if date is not None:
            target_timestamp = date.replace(hour=0, minute=0, second=0, microsecond=0)
            if target_timestamp.tzinfo is None:
                target_timestamp = target_timestamp.replace(tzinfo=UTC)
            instruments_df["timestamp"] = target_timestamp
        else:
            instruments_df["timestamp"] = datetime.now(UTC)
            logger.warning("No date parameter provided, using current time for timestamp column")

    def _validate_required_columns(self, instruments_df: pd.DataFrame) -> None:
        """Validate that all required columns are present in the DataFrame."""
        required_columns = get_required_columns()
        missing_columns = [col for col in required_columns if col not in instruments_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    def _convert_timestamp_columns(self, instruments_df: pd.DataFrame) -> None:
        """Convert timestamp columns to timezone-naive UTC for GCS storage."""
        timestamp_columns = [
            "timestamp",
            "available_from_datetime",
            "available_to_datetime",
            "expiry",
        ]
        for ts_col in timestamp_columns:
            if ts_col not in instruments_df.columns:
                continue
            col_dtype = getattr(instruments_df[ts_col].dtype, "name", str(instruments_df[ts_col].dtype))
            if str(col_dtype).startswith("datetime64"):
                ts_series: pd.Series = cast(pd.Series, pd.to_datetime(instruments_df[ts_col], utc=True))
                if ts_series.dt.tz is not None:
                    instruments_df[ts_col] = ts_series.dt.tz_convert("UTC").dt.tz_localize(None)
                instruments_df[ts_col] = instruments_df[ts_col].astype("datetime64[ns]")
            elif str(instruments_df[ts_col].dtype) == "object":
                try:
                    ts_series_obj: pd.Series = cast(pd.Series, pd.to_datetime(instruments_df[ts_col], utc=True))
                    instruments_df[ts_col] = ts_series_obj.dt.tz_convert(None)
                except (ValueError, TypeError, AttributeError) as e:
                    logger.debug("Could not parse timestamp column %s: %s", ts_col, e)

    def _resolve_date_string(self, instruments_df: pd.DataFrame, date: datetime | None) -> str:
        """Determine date string for GCS path from date param or DataFrame content."""
        if date:
            return date.strftime("%Y-%m-%d")
        if "available_from_datetime" in instruments_df.columns:
            try:
                first_val_raw: object = cast(object, instruments_df["available_from_datetime"].iloc[0])
                if first_val_raw is not None:
                    first_ts = pd.to_datetime(str(first_val_raw), utc=True)
                    first_date = pd.Timestamp(first_ts)
                else:
                    first_date = pd.Timestamp(datetime.now(UTC))
                return first_date.strftime("%Y-%m-%d")
            except (ValueError, TypeError, IndexError) as e:
                logger.debug("Could not extract date from available_from_datetime: %s", e)
        return datetime.now(UTC).strftime("%Y-%m-%d")

    def _populate_market_category(self, instruments_df: pd.DataFrame) -> None:
        """Ensure market_category is populated for all instruments."""
        logger.info("Ensuring market_category is populated for %s instruments...", len(instruments_df))
        if "market_category" not in instruments_df.columns:
            instruments_df["market_category"] = ""
        mask: pd.Series = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
        if not bool(mask.any()):
            return

        def _row_to_market_category(row: pd.Series) -> str:  # pyright: ignore[reportMissingTypeArgument]
            raw: dict[str, object] = cast(dict[str, object], row.to_dict())
            d: dict[str, object] = {}
            for k, val in raw.items():
                if val is None or (isinstance(val, float) and (val != val)):
                    d[str(k)] = None
                else:
                    d[str(k)] = str(val)
            return determine_market_category(d)

        instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(_row_to_market_category, axis=1)

    def _coerce_venue_dtypes(self, venue_df: pd.DataFrame) -> None:
        """Coerce nullable float64/bool columns to correct dtypes for a venue DataFrame."""
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
            if col in venue_df.columns:
                venue_df[col] = pd.to_numeric(venue_df[col], errors="coerce")
        for col in int64_cols:
            if col in venue_df.columns:
                ser = pd.to_numeric(venue_df[col], errors="coerce")
                venue_df[col] = ser.astype("Int64") if hasattr(ser, "astype") else ser
        for col in bool_cols:
            if col in venue_df.columns:
                venue_df[col] = venue_df[col].astype("boolean")

    def _validate_venue_schema(
        self,
        schema_enforcer: ParquetSchemaEnforcer,
        venue_df: pd.DataFrame,
        category_str: str,
        venue: str,
    ) -> bool:
        """Validate schema and log warnings/errors. Returns True if valid."""
        dimensions: dict[str, str] = {"category": category_str}
        validation_result: SchemaValidationResult = schema_enforcer.validate_dataframe(venue_df, dimensions)  # pyright: ignore[reportUnknownMemberType]

        if not validation_result.valid:
            for error in validation_result.errors:
                logger.error("Schema validation failed for %s/%s: %s", category_str, venue, error)
            logger.error("Skipping upload for %s/%s due to schema validation errors", category_str, venue)
            return False

        for warning in validation_result.warnings:
            logger.warning("Schema validation warning for %s/%s: %s", category_str, venue, warning)
        return True

    def _validate_timestamp_alignment(
        self, venue_df: pd.DataFrame, date_str: str, category_str: str, venue: str
    ) -> None:
        """Validate timestamp-date alignment and log any mismatches."""
        expected_date = date_type.fromisoformat(date_str)
        alignment_result = validate_timestamp_date_alignment(
            venue_df,
            expected_date=expected_date,
            timestamp_col="timestamp",
            alignment_threshold=100.0,
            timestamp_unit="auto",
        )
        if not alignment_result.valid:
            logger.error(
                "TIMESTAMP_DATE_MISMATCH for %s/%s: Expected %s, found dates: %s. Alignment: %.1f%%",
                category_str,
                venue,
                date_str,
                alignment_result.actual_dates_found,
                alignment_result.alignment_percentage,
            )

    def _upload_venue_to_datasink(
        self,
        venue_df: pd.DataFrame,
        category_str: str,
        venue_folder: str,
        date_str: str,
        category_venue: str,
    ) -> tuple[bool, int]:
        """Upload a single venue DataFrame via UCI DataSink. Returns (success, count)."""
        try:
            from unified_cloud_interface.constants import get_bucket_name, get_project_id_optional

            # Resolve bucket from UCI convention — no PROTOCOL_DATA_SINK_BUCKET_* env vars needed
            project_id = get_project_id_optional()
            if project_id:
                bucket = get_bucket_name("instruments", category_str.upper())
                logger.debug("Resolved bucket: %s (project=%s, category=%s)", bucket, project_id, category_str)
            else:
                # No project ID → cannot resolve GCS bucket.
                # In mock mode, LocalDataSink is expected. Outside mock mode, this is a
                # configuration error that silently writes to local filesystem instead of GCS.
                from unified_config_interface import UnifiedCloudConfig

                _cfg = UnifiedCloudConfig()
                if not _cfg.is_mock_mode():
                    logger.error(
                        "GCP_PROJECT_ID not set — cannot resolve GCS bucket for %s. "
                        "Writes will go to LocalDataSink (local filesystem), NOT GCS. "
                        "Set GCP_PROJECT_ID in env or .env to enable cloud writes.",
                        category_venue,
                    )
                bucket = None
            data_sink: DataSink = get_data_sink(bucket=bucket)
            logger.info(
                "Writing %s %s instruments via %s (bucket=%s)",
                len(venue_df),
                category_venue,
                type(data_sink).__name__,
                bucket,
            )
            result_uri: str = data_sink.write(
                venue_df,
                partition={"day": date_str, "venue": venue_folder},
                format="parquet",
            )
            if result_uri:
                logger.info(
                    "Uploaded %s %s instruments via DataSink: %s",
                    len(venue_df),
                    category_venue,
                    result_uri,
                )
                return True, len(venue_df)
            logger.error("DataSink write failed for %s (result_uri empty)", category_venue)
            return False, 0
        except (ValueError, KeyError, TypeError, IndexError) as sink_error:
            _err = EnhancedError(
                message=str(sink_error),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(sink_error).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("DataSink write failed for %s: %s", category_venue, sink_error)
            return False, 0

    def _prepare_dataframe(self, instruments_df: pd.DataFrame, date: datetime | None) -> tuple[pd.DataFrame, str]:
        """Prepare DataFrame for storage: copy, add timestamp, validate, convert, resolve date."""
        self._generate_csv_sample(instruments_df, date)
        df = instruments_df.copy()
        self._add_generation_timestamp(df, date)
        self._validate_required_columns(df)
        self._convert_timestamp_columns(df)
        date_str = self._resolve_date_string(df, date)
        self._populate_market_category(df)
        return df, date_str

    def _store_all_venues(
        self,
        instruments_df: pd.DataFrame,
        schema_enforcer: ParquetSchemaEnforcer,
        date_str: str,
    ) -> tuple[bool, int, int]:
        """Iterate over category/venue groups, validate, and upload each. Returns (all_ok, stored, venues)."""
        total_stored = 0
        venue_count = 0
        all_successful = True

        for category, category_df in instruments_df.groupby("market_category"):
            category_str = str(category)
            for venue, venue_df in category_df.groupby("venue"):
                venue_folder = str(venue).replace("/", "-").replace("\\", "-")
                venue_df_to_store = venue_df.copy()
                category_venue = f"{category_str}/{venue}"

                self._coerce_venue_dtypes(venue_df_to_store)

                if not self._validate_venue_schema(schema_enforcer, venue_df_to_store, category_str, str(venue)):
                    all_successful = False
                    continue

                self._validate_timestamp_alignment(venue_df_to_store, date_str, category_str, str(venue))

                success, count = self._upload_venue_to_datasink(
                    venue_df_to_store, category_str, venue_folder, date_str, category_venue
                )
                if success:
                    total_stored += count
                    venue_count += 1
                else:
                    all_successful = False

        return all_successful, total_stored, venue_count

    def _log_storage_result(
        self,
        all_successful: bool,
        total_stored: int,
        venue_count: int,
        total_instruments: int,
        date_str: str,
    ) -> None:
        """Log final storage metrics and emit lifecycle events."""
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
            logger.warning("Some venue uploads failed. Total stored: %s/%s", total_stored, total_instruments)
            log_event(
                LifecycleEventType.PERSISTENCE_COMPLETED,
                details={"total_stored": total_stored, "date": date_str, "partial": True},
            )

    @handle_storage_errors(max_retries=2)
    def store_instruments(
        self,
        instruments_df: pd.DataFrame,
        table_name: str = "instruments",
        date: datetime | None = None,
    ) -> bool:
        """
        Store instrument definitions to GCS via DataSink (batch historical data only).

        Handles UTC midnight spanning for CME/ICE by writing to both dates.
        Schema: see ``instruments_service.schemas.parquet``.

        Args:
            instruments_df: DataFrame with instrument definitions.
            table_name: Kept for compatibility (unused).
            date: Optional target date for partitioning and CSV sample.

        Returns:
            True if all venue uploads succeeded.
        """
        try:
            # Pre-flight: validate we can resolve a GCS bucket before doing any work
            from unified_cloud_interface.constants import get_project_id_optional
            from unified_config_interface import UnifiedCloudConfig

            _pre_cfg = UnifiedCloudConfig()
            if not _pre_cfg.is_mock_mode() and not get_project_id_optional():
                logger.error(
                    "STORAGE PRE-FLIGHT FAILED: GCP_PROJECT_ID not set. "
                    "Cannot write to GCS. Set GCP_PROJECT_ID or use CLOUD_MOCK_MODE=true."
                )
                return False

            instruments_df, date_str = self._prepare_dataframe(instruments_df, date)

            schema_enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
            log_event(LifecycleEventType.PERSISTENCE_STARTED, details={"date": date_str})

            all_successful, total_stored, venue_count = self._store_all_venues(
                instruments_df, schema_enforcer, date_str
            )

            self._log_storage_result(all_successful, total_stored, venue_count, len(instruments_df), date_str)
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
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
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
            "BigQuery query removed - batch instruments are stored in GCS only. "
            "Use GCS download methods or live streaming analytics endpoints for queries."
        )
        return pd.DataFrame()
