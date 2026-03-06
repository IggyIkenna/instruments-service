"""
Main Orchestration Service for Instruments Service

Coordinates instrument processing, storage, and batch operations.
Follows unified repository structure pattern.

This module is the main entry point. Implementation is split across:
- instrument_crud.py — query, batch processing, stats, cleanup, ErrorWarningCounter
- instrument_sync.py — CEFI/TRADFI/DEFI venue processing
- instrument_validation.py — venue validation and filtering
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast
from uuid import uuid4

import pandas as pd
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import VenueMapping

from instruments_service.app.core.batch_processor import InstrumentBatchProcessor
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

# Import mixins for this file's SRP split
from instruments_service.app.core.instrument_crud import ErrorWarningCounter, InstrumentCrudMixin
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from instruments_service.app.core.instrument_sync import InstrumentSyncMixin
from instruments_service.app.core.instrument_validation import InstrumentValidationMixin
from instruments_service.models import InstrumentDefinition

__all__ = [
    "ErrorWarningCounter",
    "InstrumentsService",
]

logger = logging.getLogger(__name__)


class InstrumentsService(
    InstrumentCrudMixin,
    InstrumentSyncMixin,
    InstrumentValidationMixin,
):
    """
    Main orchestration service that coordinates:
    - Instrument processing (fetching from APIs, generating canonical keys)
    - Cloud storage (GCS and BigQuery)
    - Batch operations (date range processing, memory estimation)

    Functionality is composed from mixins:
    - InstrumentCrudMixin: query, batch processing, stats, cleanup
    - InstrumentSyncMixin: CEFI/TRADFI/DEFI venue processing
    - InstrumentValidationMixin: venue validation and filtering
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestration service.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - gcs_bucket: GCS bucket name (optional, auto-detected)
                - bigquery_dataset: BigQuery dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        # Initialize processing service
        from instruments_service.config import instruments_config

        processing_config: dict[str, object] = {
            "project_id": str(cast(str | None, config.get("project_id")) or instruments_config.gcp_project_id),
            "enable_ccxt_integration": cast(bool, config.get("enable_ccxt_integration", True)),
            "enable_metadata_caching": cast(bool, config.get("enable_metadata_caching", True)),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping
        self.venue_mapping: VenueMapping = VenueMapping()

        logger.info("✅ InstrumentsService initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        sports: bool = False,
        venues: list[str] | str | None = None,
        instrument_ids: list[str] | str | None = None,
        tradfi_venues: list[str] | None = None,
        skip_storage: bool = False,
    ) -> dict[str, object]:
        """
        Generate instruments for a specific date.

        Args:
            date: Target date for instrument generation
            exchanges: Optional list of exchanges to process (for CeFi mode)
            force: Force regeneration even if instruments exist
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            sports: Process Sports fixtures
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include
            tradfi_venues: Optional list of specific TradFi venues to process
            skip_storage: Skip cloud storage (live mode)

        Returns:
            Dictionary with generation results including error_count and warning_count
        """
        error_warning_counter = ErrorWarningCounter()
        root_logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)
        error_warning_counter.reset()

        success = False
        instruments_df = None
        date_str: str = date.strftime("%Y-%m-%d")

        try:
            date_str = date.strftime("%Y-%m-%d")
            logger.info(
                "📅 Generating instruments for %s (CeFi=%s, TradFi=%s, DeFi=%s, Sports=%s)",
                date_str,
                cefi,
                tradfi,
                defi,
                sports,
            )

            # Normalize venues filter
            _venues_raw: list[str] | str = venues if venues is not None else []
            if isinstance(_venues_raw, str):
                _venues_raw = [_venues_raw]
            venues_filter: list[str] = [str(v).upper() for v in _venues_raw] if _venues_raw else []

            # Extract venues from instrument_ids if provided
            venues_filter = self._extract_venues_from_instrument_ids(instrument_ids, venues_filter)

            # Validate venues against allowed venues
            if venues_filter:
                valid_venues_by_type = self._validate_venues_filter(venues_filter, cefi, tradfi, defi)

                if valid_venues_by_type is not None and not any(valid_venues_by_type.values()):
                    error_count = error_warning_counter.error_count
                    warning_count = error_warning_counter.warning_count
                    root_logger.removeHandler(error_warning_counter)
                    return {
                        "status": "warning",
                        "date": date_str,
                        "instruments_generated": 0,
                        "message": "No valid venues found after filtering",
                        "error_count": error_count,
                        "warning_count": warning_count,
                    }

            all_instruments: dict[str, InstrumentDefinition] = {}

            # Process CEFI (Tardis) exchanges
            cefi_exchanges: list[str] = []
            if cefi:
                cefi_exchanges = (
                    cast(list[str], exchanges)
                    if exchanges is not None
                    else cast(list[str], self.venue_mapping.all_tardis_exchanges)
                )
                try:
                    cefi_exchanges, cefi_instruments = await self._process_cefi_exchanges(
                        cefi_exchanges=cefi_exchanges,
                        venues_filter=venues_filter,
                        date=date,
                        force=force,
                    )
                    all_instruments.update(cefi_instruments)
                except (OSError, ValueError, RuntimeError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e

            # Process TRADFI (Databento) exchanges
            if tradfi:
                try:
                    tradfi_instruments = await self._process_tradfi_exchanges(
                        venues_filter=venues_filter,
                        tradfi_venues=tradfi_venues,
                        date=date,
                    )
                    all_instruments.update(tradfi_instruments)
                except (ValueError, KeyError, TypeError, IndexError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e

            # Process DEFI protocols
            if defi:
                try:
                    defi_instruments = await self._process_defi_protocols(
                        venues_filter=venues_filter,
                        date=date,
                    )
                    all_instruments.update(defi_instruments)
                except (OSError, ValueError, RuntimeError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e

            # Process SPORTS fixtures
            if sports:
                from instruments_service.engine.operations.instruments.orchestration.sports_orchestration import (
                    SportsOrchestrator,
                )

                sports_orchestrator = SportsOrchestrator()
                sports_instruments = await sports_orchestrator.process_sports(date, venues_filter)
                all_instruments.update(sports_instruments)

            # If no mode flags specified, process all four modes
            if not cefi and not tradfi and not defi and not sports:
                logger.info("📋 No mode flags specified, processing all modes (CeFi, TradFi, DeFi, Sports)")
                return await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=True,
                    tradfi=True,
                    defi=True,
                    sports=True,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )

            # Apply instrument_id filtering if specified
            all_instruments = self._filter_instruments_by_ids(all_instruments, instrument_ids)

            # Convert to DataFrame
            instruments_list: list[dict[str, object]] = []
            for inst_key, inst_obj in all_instruments.items():
                _ = inst_key
                if hasattr(inst_obj, "model_dump"):
                    instruments_list.append(cast(dict[str, object], inst_obj.model_dump()))
                else:
                    instruments_list.append(cast(dict[str, object], cast(object, inst_obj)))

            instruments_df = pd.DataFrame(instruments_list)

            # Ensure market_category populated for live mode
            if skip_storage and not instruments_df.empty:
                from unified_trading_library import determine_market_category

                def _row_to_market_category(row: pd.Series) -> str:  # pyright: ignore[reportMissingTypeArgument]
                    raw: dict[str, object] = cast(dict[str, object], row.to_dict())
                    row_dict: dict[str, object] = {}
                    for k, val in raw.items():
                        if val is None or (isinstance(val, float) and val != val):
                            row_dict[str(k)] = None
                        else:
                            row_dict[str(k)] = str(val)
                    return determine_market_category(row_dict)

                if "market_category" not in instruments_df.columns:
                    instruments_df["market_category"] = ""
                mask = (instruments_df["market_category"].isna()) | (instruments_df["market_category"] == "")
                if mask.any():
                    instruments_df.loc[mask, "market_category"] = instruments_df.loc[mask].apply(
                        _row_to_market_category, axis=1
                    )

            # Handle case where no instruments generated
            if not all_instruments:
                if tradfi and venues_filter:
                    logger.warning(
                        "⚠️ No instruments generated from adapters for %s. Placeholder logic will ensure files are written for requested TradFi venues.",
                        date_str,
                    )
                else:
                    logger.error("No instruments generated for %s.", date_str)
                    error_count = error_warning_counter.error_count
                    warning_count = error_warning_counter.warning_count
                    root_logger.removeHandler(error_warning_counter)
                    return {
                        "status": "error",
                        "date": date_str,
                        "instruments_generated": 0,
                        "message": "No instruments generated - check adapter/API connectivity",
                        "error_count": error_count,
                        "warning_count": warning_count,
                    }

            # Add TradFi placeholders for missing venues
            if tradfi and venues_filter:
                instruments_df = self._add_tradfi_placeholders(instruments_df, venues_filter, date, date_str)

            # Tag instruments with session_date_tag
            if not instruments_df.empty and "regular_open_utc" in instruments_df.columns:
                instruments_df["session_date_tag"] = "close_date"

            # Store to cloud
            if skip_storage:
                success = True
                logger.info("📤 Skipping batch storage (live mode) - %s instruments", len(instruments_df))
            else:
                logger.info("📤 Storing %s instruments to cloud...", len(instruments_df))
                success = cast(
                    bool,
                    self.cloud_storage.store_instruments(  # pyright: ignore[reportUnknownMemberType]
                        instruments_df=instruments_df, table_name="instruments", date=date
                    ),
                )

            # Handle UTC midnight spanning
            if (
                success
                and not skip_storage
                and not instruments_df.empty
                and "regular_open_utc" in instruments_df.columns
            ):
                self._handle_utc_spanning(instruments_df, date, date_str)

            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count

            if success:
                logger.info("✅ Successfully stored instruments for %s", date_str)
                result: dict[str, object] = {
                    "status": "success",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "exchanges_processed": len(cefi_exchanges) if cefi else 0,
                    "venues": cast(list[str], instruments_df["venue"].unique().tolist())
                    if "venue" in instruments_df.columns
                    else [],
                    "error_count": error_count,
                    "warning_count": warning_count,
                }
                if skip_storage and not instruments_df.empty and "market_category" in instruments_df.columns:
                    result["instruments_by_category"] = {
                        str(cat): grp for cat, grp in instruments_df.groupby("market_category")
                    }
                return result
            else:
                logger.error("Failed to store instruments for %s", date_str)
                return {
                    "status": "error",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "message": "Storage failed",
                    "error_count": error_count,
                    "warning_count": warning_count,
                }

        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count
            logger.exception("Exception during instrument generation: %s", e)
            return {
                "status": "error",
                "date": date_str,
                "instruments_generated": 0,
                "message": f"Exception during processing: {e!s}",
                "error_count": error_count,
                "warning_count": warning_count,
            }
        finally:
            if error_warning_counter in root_logger.handlers:
                root_logger.removeHandler(error_warning_counter)

    def _add_tradfi_placeholders(
        self, instruments_df: pd.DataFrame, venues_filter: list[str], date: datetime, date_str: str
    ) -> pd.DataFrame:
        """Add holiday/closed placeholder rows for TradFi venues without instruments."""
        requested_tradfi: list[str] = [
            v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_databento_venues)
        ]
        if not requested_tradfi:
            return instruments_df

        target_dt = (
            date.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(date, datetime)
            else datetime.combine(date, datetime.min.time(), tzinfo=UTC)
        )
        if target_dt.tzinfo is None:
            target_dt = target_dt.replace(tzinfo=UTC)
        date_iso = target_dt.isoformat().replace("+00:00", "Z")

        venues_present: set[str] = set()
        if "venue" in instruments_df.columns and not instruments_df.empty:
            venues_present = {str(v) for v in cast(list[object], instruments_df["venue"].unique().tolist())}

        for venue_name in requested_tradfi:
            if venue_name not in venues_present:
                placeholder: dict[str, object] = {
                    "instrument_key": f"{venue_name}:MARKET_CLOSED:PLACEHOLDER",
                    "venue": venue_name,
                    "instrument_type": "MARKET_CLOSED",
                    "symbol": "PLACEHOLDER",
                    "available_from_datetime": date_iso,
                    "timestamp": target_dt,
                    "market_category": "TRADFI",
                    "data_provider": "databento",
                    "asset_class": "traditional",
                    "venue_type": "exchange",
                    "chain": "off-chain",
                    "databento_symbol": "PLACEHOLDER",
                    "tardis_exchange": "",
                    "is_trading_day": False,
                    "trading_hours_open": "holiday",
                    "trading_hours_close": "holiday",
                    "holiday_calendar": venue_name,
                    "trading_session": "regular",
                    "regular_open_utc": None,
                    "regular_close_utc": None,
                    "auction_open_utc": None,
                    "auction_close_utc": None,
                    "early_close_utc": None,
                }
                logger.info("📅 Adding holiday/closed placeholder for %s on %s", venue_name, date_str)
                instruments_df = pd.concat([instruments_df, pd.DataFrame([placeholder])], ignore_index=True)
        return instruments_df

    def _handle_utc_spanning(self, instruments_df: pd.DataFrame, date: datetime, date_str: str) -> None:
        """Handle UTC midnight spanning for CME and ICE instruments."""
        utc_spanning_instruments: list[pd.Series] = []
        file_date: date_type = date.date() if isinstance(date, datetime) else date

        for idx, row in instruments_df.iterrows():
            _ = idx
            if row.get("regular_open_utc") and row.get("regular_close_utc"):
                try:
                    open_dt = cast(pd.Timestamp, pd.to_datetime(cast(str, row["regular_open_utc"])))
                    close_dt = cast(pd.Timestamp, pd.to_datetime(cast(str, row["regular_close_utc"])))
                    if open_dt.date() != file_date and close_dt.date() == file_date:
                        utc_spanning_instruments.append(row)
                except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e

        if utc_spanning_instruments:
            spanning_df = pd.DataFrame(utc_spanning_instruments)
            spanning_df["session_date_tag"] = "open_date"
            open_date_val = cast(date_type, pd.to_datetime(cast(str, spanning_df.iloc[0]["regular_open_utc"])).date())
            logger.info("🌙 Writing %s UTC-spanning instruments to open-date file: %s", len(spanning_df), open_date_val)
            open_date_dt = datetime.combine(open_date_val, datetime.min.time(), tzinfo=UTC)
            span_success = cast(
                bool,
                self.cloud_storage.store_instruments(  # pyright: ignore[reportUnknownMemberType]
                    instruments_df=spanning_df, table_name="instruments", date=open_date_dt
                ),
            )
            if span_success:
                logger.info("✅ Successfully wrote UTC-spanning instruments to %s", open_date_val)
            else:
                logger.warning("⚠️ Failed to write UTC-spanning instruments to %s", open_date_val)
