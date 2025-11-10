"""
Instrument Batch Processor

Handles batch processing of instrument downloads with lookback computation,
date range calculation, and memory estimation.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class InstrumentBatchProcessor:
    """
    Batch processor for instrument downloads.

    Handles:
    - Lookback computation (determining historical data requirements)
    - Date range calculation
    - Memory estimation
    - Batch processing orchestration
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize batch processor.

        Args:
            config: Configuration with batch processing settings
        """
        self.config = config
        self.max_batch_size = config.get("max_batch_size", 1000)
        self.lookback_days = config.get("lookback_days", 0)  # Default: no lookback

    def calculate_date_range(
        self, target_date: datetime, lookback_days: Optional[int] = None
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
            f"Calculated date range: {start_date.date()} to {end_date.date()} "
            f"(lookback: {lookback_days} days)"
        )

        return start_date, end_date

    def estimate_memory_requirements(
        self, num_instruments: int, date_range_days: int
    ) -> Dict[str, Any]:
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

        estimate = {
            "num_instruments": num_instruments,
            "date_range_days": date_range_days,
            "estimated_bytes": estimated_bytes,
            "estimated_mb": round(estimated_mb, 2),
            "estimated_gb": round(estimated_mb / 1024, 2),
        }

        logger.info(
            f"Memory estimate: {estimate['estimated_mb']} MB "
            f"({estimate['estimated_gb']} GB) for {num_instruments} instruments"
        )

        return estimate

    def get_required_periods(
        self, target_date: datetime, lookback_days: Optional[int] = None
    ) -> List[datetime]:
        """
        Get list of required periods (dates) for processing.

        Args:
            target_date: Target date for processing
            lookback_days: Optional override for lookback days

        Returns:
            List of datetime objects for each period
        """
        start_date, end_date = self.calculate_date_range(target_date, lookback_days)

        periods = []
        current_date = start_date

        while current_date <= end_date:
            periods.append(current_date)
            current_date += timedelta(days=1)

        logger.info(f"Generated {len(periods)} periods for processing")

        return periods

    def process_batch(
        self, instruments: List[Dict[str, Any]], batch_size: Optional[int] = None
    ) -> List[List[Dict[str, Any]]]:
        """
        Split instruments into batches for processing.

        Args:
            instruments: List of instrument dictionaries
            batch_size: Optional batch size override

        Returns:
            List of batches (each batch is a list of instruments)
        """
        if batch_size is None:
            batch_size = self.max_batch_size

        batches = []
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i : i + batch_size]
            batches.append(batch)

        logger.info(
            f"Split {len(instruments)} instruments into {len(batches)} batches "
            f"(batch size: {batch_size})"
        )

        return batches
