"""
Instrument Batch Processor

Uses unified-trading-library utils (generate_date_range, split_into_batches) per UCS v1.6.0.
Replaces GenericBatchProcessor with composition over inheritance.
"""

import logging
from datetime import datetime, timedelta

from unified_trading_library import generate_date_range, split_into_batches

logger = logging.getLogger(__name__)


class InstrumentBatchProcessor:
    """
    Instrument-specific batch processor.

    Uses UCS utils for date range and batching (UCS v1.6.0 migration).
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize instrument batch processor.

        Args:
            config: Configuration with batch processing settings
        """
        max_batch_raw = config.get("max_batch_size")
        self.max_batch_size: int = int(max_batch_raw) if isinstance(max_batch_raw, (int, float, str)) else 1000
        lookback_raw = config.get("lookback_days")
        self.lookback_days: int = int(lookback_raw) if isinstance(lookback_raw, (int, float, str)) else 0

    def calculate_date_range(
        self, target_date: datetime, lookback_days: int | None = None
    ) -> tuple[datetime, datetime]:
        """
        Calculate date range for batch processing.

        Args:
            target_date: Target date for processing
            lookback_days: Optional override for lookback days

        Returns:
            tuple: (start_date, end_date)
        """
        if lookback_days is None:
            lookback_days = self.lookback_days

        start_date = target_date - timedelta(days=lookback_days)
        end_date = target_date

        logger.info(
            "Calculated date range: %s to %s (lookback: %s days)", start_date.date(), end_date.date(), lookback_days
        )

        return start_date, end_date

    def estimate_memory_requirements(self, num_instruments: int, date_range_days: int) -> dict[str, int | float]:
        """
        Estimate memory requirements for batch processing.

        Args:
            num_instruments: Number of instruments to process
            date_range_days: Number of days in date range

        Returns:
            Dictionary with memory estimates
        """
        # Rough estimate: ~1KB per instrument definition
        bytes_per_instrument = 1024
        estimated_bytes = num_instruments * bytes_per_instrument
        estimated_mb = estimated_bytes / (1024 * 1024)

        estimate: dict[str, int | float] = {
            "num_instruments": num_instruments,
            "date_range_days": date_range_days,
            "estimated_bytes": estimated_bytes,
            "estimated_mb": round(estimated_mb, 2),
            "estimated_gb": round(estimated_mb / 1024, 2),
        }

        logger.info(
            "Memory estimate: %s MB (%s GB) for %s instruments",
            estimate["estimated_mb"],
            estimate["estimated_gb"],
            num_instruments,
        )

        return estimate

    def get_required_periods(self, target_date: datetime, lookback_days: int | None = None) -> list[datetime]:
        """
        Get list of required periods (dates) for processing.

        Uses UCS generate_date_range (replaces GenericBatchProcessor.get_required_periods).

        Args:
            target_date: Target date for processing
            lookback_days: Optional override for lookback days

        Returns:
            list of datetime objects for each period
        """
        start_date, end_date = self.calculate_date_range(target_date, lookback_days)
        periods: list[datetime] = generate_date_range(start_date, end_date)
        logger.info("Generated %s periods for processing", len(periods))
        return periods

    def process_batch(
        self, instruments: list[dict[str, object]], batch_size: int | None = None
    ) -> list[list[dict[str, object]]]:
        """
        Split instruments into batches for processing.

        Uses UCS split_into_batches (replaces GenericBatchProcessor.process_batch).

        Args:
            instruments: list of instrument dictionaries
            batch_size: Optional batch size override

        Returns:
            list of batches (each batch is a list of instruments)
        """
        if batch_size is None:
            batch_size = self.max_batch_size

        batches: list[list[dict[str, object]]] = split_into_batches(instruments, batch_size)
        logger.info("Split %s instruments into %s batches (batch size: %s)", len(instruments), len(batches), batch_size)
        return batches
