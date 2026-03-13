"""
Main Instruments Orchestrator

Coordinates instrument generation workflow across market types (CeFi, TradFi, DeFi).
Handles venue filtering, parallel processing, and storage coordination.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Protocol, cast
from uuid import uuid4

import pandas as pd
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import VenueMapping

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from instruments_service.config import (
    instruments_config,
)
from instruments_service.engine.operations.instruments.batch_orchestrator import InstrumentBatchProcessor
from instruments_service.metrics import PROCESSING_LATENCY, RECORDS_PROCESSED
from instruments_service.models import InstrumentDefinition
from instruments_service.utils import ErrorWarningCounter

from .cefi_orchestration import CeFiOrchestrator
from .defi_orchestration import DeFiOrchestrator
from .instrument_utils import InstrumentUtils
from .sports_orchestration import SportsOrchestrator
from .tradfi_orchestration import TradFiOrchestrator


class InstrumentStorageProtocol(Protocol):
    """Protocol for storage adapters with store_instruments and query_instruments."""

    def store_instruments(
        self,
        instruments_df: pd.DataFrame,
        table_name: str = "instruments",
        date: datetime | None = None,
    ) -> bool: ...

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        table_name: str | None = None,
    ) -> pd.DataFrame: ...


logger = logging.getLogger(__name__)


class InstrumentsOrchestrator:
    """
    Orchestrates instrument generation workflow.

    Responsibilities:
    - Coordinate processing across market types (CeFi, TradFi, DeFi)
    - Handle venue filtering and validation
    - Manage parallel processing of exchanges/venues
    - Coordinate storage operations
    - Track errors and warnings
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestrator.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - sink_bucket: Sink bucket name (optional, auto-detected)
                - analytics_dataset: Analytics dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        processing_config: dict[str, object] = {
            "project_id": str(cast(str | None, config.get("project_id")) or instruments_config.gcp_project_id),
            "enable_ccxt_integration": cast(bool, config.get("enable_ccxt_integration", True)),
            "enable_metadata_caching": cast(bool, config.get("enable_metadata_caching", True)),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage: InstrumentStorageProtocol = CloudInstrumentStorage()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Initialize specialized orchestrators
        self.cefi_orchestrator = CeFiOrchestrator(self.processing_service)
        self.tradfi_orchestrator = TradFiOrchestrator(self.processing_service)
        self.defi_orchestrator = DeFiOrchestrator(self.processing_service)
        self.sports_orchestrator = SportsOrchestrator()

        # Initialize utility helper
        self.utils = InstrumentUtils()

        # Venue mapping
        self.venue_mapping: VenueMapping = VenueMapping()

        logger.info("✅ InstrumentsOrchestrator initialized")

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
            tradfi_venues: Optional list of specific TradFi venues to process (e.g., ["NASDAQ", "NYSE"])
            skip_storage: Skip storage (for live mode)

        Returns:
            Dictionary with generation results including error_count and warning_count
        """
        # Set up error/warning counter for this operation
        error_warning_counter: ErrorWarningCounter = ErrorWarningCounter()
        root_logger: logging.Logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)  # pyright: ignore[reportArgumentType]
        error_warning_counter.reset()

        # Initialize variables that may be used in finally block
        success = False
        instruments_df = None
        date_str = date.strftime("%Y-%m-%d")
        _prom_start = time.perf_counter()

        try:
            logger.info(
                "📅 Generating instruments for %s (CeFi=%s, TradFi=%s, DeFi=%s, Sports=%s)",
                date_str,
                cefi,
                tradfi,
                defi,
                sports,
            )

            # Normalize venues filter
            venues_filter = self.utils.normalize_venues_filter(venues)

            # Extract venues from instrument_ids if provided
            if instrument_ids:
                venues_filter = self.utils.extract_venues_from_instrument_ids(instrument_ids, venues_filter)

            # Validate venues against allowed venues for each market type
            if venues_filter:
                validation_result = self.utils.validate_venues(venues_filter, cefi, tradfi, defi)
                if validation_result:
                    return validation_result

            all_instruments: dict[str, InstrumentDefinition] = {}

            # Process CEFI (Tardis) exchanges
            if cefi:
                cefi_instruments = await self.cefi_orchestrator.process_cefi(date, exchanges, force, venues_filter)
                all_instruments.update(cefi_instruments)

            # Process TRADFI (Databento) exchanges
            if tradfi:
                tradfi_instruments = await self.tradfi_orchestrator.process_tradfi(date, venues_filter, tradfi_venues)
                all_instruments.update(tradfi_instruments)

            # Process DEFI protocols
            if defi:
                defi_instruments = await self.defi_orchestrator.process_defi(date, venues_filter)
                all_instruments.update(defi_instruments)

            # Process SPORTS fixtures
            if sports:
                sports_instruments = await self.sports_orchestrator.process_sports(date, venues_filter)
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
            if instrument_ids:
                all_instruments = self.utils.filter_by_instrument_ids(all_instruments, instrument_ids)

            # Convert to DataFrame
            if all_instruments:
                instruments_df = self.utils.convert_to_dataframe(all_instruments, date)
            else:
                logger.warning("⚠️ No instruments found or generated")
                return self.utils.handle_no_instruments(date, error_warning_counter)

            # Add trading hours for TradFi instruments
            self.utils.add_tradfi_placeholders(instruments_df)

            # Handle UTC midnight spanning instruments
            self.utils.handle_utc_midnight_spanning(instruments_df, date)

            # Store results unless skipped
            if not skip_storage:
                success = self.cloud_storage.store_instruments(instruments_df, date=date)
                if success:
                    logger.info("✅ Successfully stored %s instruments for %s", len(instruments_df), date_str)
                else:
                    logger.error("❌ Failed to store instruments for %s", date_str)

            RECORDS_PROCESSED.labels(status="success").inc()
            return {
                "success": success,
                "count": len(instruments_df),
                "date": date_str,
                "error_count": error_warning_counter.error_count,
                "warning_count": error_warning_counter.warning_count,
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
            logger.error("❌ Error in generate_instruments_for_date: %s", e)
            RECORDS_PROCESSED.labels(status="error").inc()
            return {
                "success": False,
                "error": str(e),
                "date": date_str if "date_str" in locals() else date.strftime("%Y-%m-%d"),
                "error_count": error_warning_counter.error_count,
                "warning_count": error_warning_counter.warning_count,
            }
        finally:
            PROCESSING_LATENCY.observe(time.perf_counter() - _prom_start)
            # Remove handler to avoid memory leaks
            root_logger.removeHandler(error_warning_counter)  # pyright: ignore[reportArgumentType]

    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        sports: bool = False,
        venues: list[str] | str | None = None,
        instrument_ids: list[str] | str | None = None,
        parallel: bool = True,
        max_concurrent: int = 3,
        skip_storage: bool = False,
    ) -> dict[str, object]:
        """
        Generate instruments for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            exchanges: Optional list of exchanges to process (for CeFi mode)
            force: Force regeneration even if instruments exist
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            sports: Process Sports fixtures
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include
            parallel: Process dates in parallel (default: True)
            max_concurrent: Maximum concurrent date processing (default: 3)
            skip_storage: Skip storage (for live mode)

        Returns:
            Dictionary with aggregated results
        """
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        dates = [d.to_pydatetime() for d in date_range]

        if parallel:
            # Process dates in parallel with concurrency limit
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_date_with_semaphore(date: datetime) -> dict[str, object]:
                async with semaphore:
                    return await self.generate_instruments_for_date(
                        date=date,
                        exchanges=exchanges,
                        force=force,
                        cefi=cefi,
                        tradfi=tradfi,
                        defi=defi,
                        sports=sports,
                        venues=venues,
                        instrument_ids=instrument_ids,
                        skip_storage=skip_storage,
                    )

            results = await asyncio.gather(*[process_date_with_semaphore(date) for date in dates])
        else:
            # Process dates sequentially
            results = []
            for date in dates:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=cefi,
                    tradfi=tradfi,
                    defi=defi,
                    sports=sports,
                    venues=venues,
                    instrument_ids=instrument_ids,
                    skip_storage=skip_storage,
                )
                results.append(result)

        # Aggregate results
        def _to_int(val: object) -> int:
            return int(val) if isinstance(val, (int, float, str)) else 0

        total_count = sum(_to_int(r.get("count")) for r in results)
        total_errors = sum(_to_int(r.get("error_count")) for r in results)
        total_warnings = sum(_to_int(r.get("warning_count")) for r in results)
        successful_dates = sum(1 for r in results if r.get("success", False))

        return {
            "success": successful_dates > 0,
            "processed_dates": len(dates),
            "successful_dates": successful_dates,
            "total_instruments": total_count,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "date_results": results,
        }

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        table_name: str | None = None,
    ) -> pd.DataFrame:
        """
        Query stored instruments.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            table_name: Optional table name override

        Returns:
            DataFrame with matching instruments
        """
        return self.cloud_storage.query_instruments(venue, instrument_type, table_name)

    def get_processing_stats(self) -> dict[str, object]:
        """Get processing statistics."""
        return {
            "processor": "InstrumentsOrchestrator",
            "config": self.config,
            "batch_processor_config": getattr(self.batch_processor, "config", None),  # pyright: ignore[reportUnknownMemberType]
        }

    def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("🧹 Cleaning up InstrumentsOrchestrator")
        if hasattr(self.processing_service, "cleanup"):
            self.processing_service.cleanup()
        if hasattr(self.batch_processor, "cleanup"):
            self.batch_processor.cleanup()  # pyright: ignore[reportUnknownMemberType]
