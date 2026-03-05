"""
Instrument CRUD Operations and Batch Processing.

Extracted from instruments_service.py — contains query operations,
date range processing, processing stats, and the ErrorWarningCounter helper.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pandas as pd
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

if TYPE_CHECKING:
    from instruments_service.app.core.batch_processor import InstrumentBatchProcessor
    from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

logger = logging.getLogger(__name__)


class ErrorWarningCounter(logging.Handler):
    """Custom logging handler to count ERROR and WARNING messages."""

    def __init__(self):
        super().__init__()
        self.error_count = 0
        self.warning_count = 0

    def emit(self, record: logging.LogRecord) -> None:
        """Count errors and warnings."""
        if record.levelno == logging.ERROR:
            self.error_count += 1
        elif record.levelno == logging.WARNING:
            self.warning_count += 1

    def reset(self):
        """Reset counters."""
        self.error_count = 0
        self.warning_count = 0


class InstrumentCrudMixin:
    """
    Mixin providing instrument query, batch processing, stats, and cleanup methods.

    Requires the host class to have:
        - self.cloud_storage: CloudInstrumentStorage
        - self.batch_processor: InstrumentBatchProcessor
        - self.processing_service: InstrumentProcessingService
        - self.generate_instruments_for_date(...) -> dict[str, object]
    """

    # Attribute stubs provided by the concrete host class via composition.
    # The cast(T, cast(object, None)) pattern satisfies reportUninitializedInstanceVariable
    # while keeping the correct type annotation visible to basedpyright.
    batch_processor: InstrumentBatchProcessor = cast("InstrumentBatchProcessor", cast(object, None))
    cloud_storage: CloudInstrumentStorage = cast("CloudInstrumentStorage", cast(object, None))
    processing_service: InstrumentProcessingService = cast("InstrumentProcessingService", cast(object, None))

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
    ) -> dict[str, object]: ...

    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: list[str] | None = None,
        instrument_ids: list[str] | None = None,
    ) -> dict[str, object]:
        """
        Generate instruments for a date range.

        Args:
            start_date: Start date
            end_date: End date
            exchanges: Optional list of exchanges to process
            force: Force regeneration
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include

        Returns:
            Dictionary with batch processing results
        """
        # Get date range from batch processor
        date_range = self.batch_processor.get_required_periods(start_date)

        # Filter to requested range
        date_range = [d for d in date_range if start_date <= d <= end_date]

        logger.info("📅 Processing %s dates from %s to %s", len(date_range), start_date.date(), end_date.date())

        results: list[dict[str, object]] = []
        total_generated: int = 0
        total_errors: int = 0

        for date in date_range:
            try:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=cefi,
                    tradfi=tradfi,
                    defi=defi,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )
                results.append(result)
                if result.get("status") == "success":
                    total_generated += cast(int, result.get("instruments_generated", 0))
                else:
                    total_errors += 1
            except (ValueError, KeyError, TypeError, IndexError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                logger.error(
                    "❌ Failed to process %s: %s",
                    date.strftime("%Y-%m-%d"),
                    e,
                    exc_info=True,
                )
                total_errors += 1
                results.append(
                    {
                        "status": "error",
                        "date": date.strftime("%Y-%m-%d"),
                        "error": str(e),
                    }
                )
        success_count: int = len([r for r in results if r.get("status") == "success"])
        success_rate = (success_count / len(date_range) * 100) if date_range else 0

        logger.info(
            "📊 Batch processing complete: %s/%s successful (%..1f%)", success_count, len(date_range), success_rate
        )
        logger.info("   Total instruments generated: %s", total_generated)
        logger.info("   Errors: %s", total_errors)

        return {
            "status": "success" if total_errors == 0 else "partial",
            "dates_processed": len(date_range),
            "dates_successful": success_count,
            "dates_failed": total_errors,
            "success_rate_percent": success_rate,
            "total_instruments_generated": total_generated,
            "results": results,
        }

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
    ) -> pd.DataFrame:
        """
        Query stored instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            base_asset: Optional base asset filter
            quote_asset: Optional quote asset filter

        Returns:
            DataFrame with instruments
        """
        result: pd.DataFrame = self.cloud_storage.query_instruments(venue=venue, instrument_type=instrument_type)
        # Filter by base_asset and quote_asset if provided
        if base_asset is not None and not result.empty and "base_asset" in result.columns:
            result = result[result["base_asset"] == base_asset]
        if quote_asset is not None and not result.empty and "quote_asset" in result.columns:
            result = result[result["quote_asset"] == quote_asset]
        return result

    def get_processing_stats(self) -> dict[str, object]:
        """Get processing statistics."""
        return {
            "processing_service": self.processing_service.get_processing_stats(),
            "batch_processor": {
                "max_batch_size": self.batch_processor.max_batch_size,
                "lookback_days": self.batch_processor.lookback_days,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "processing_service"):
            self.processing_service.cleanup()
        logger.info("🧹 InstrumentsService cleanup completed")
