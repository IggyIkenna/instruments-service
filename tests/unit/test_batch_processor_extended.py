"""
Extended unit tests for batch processor to increase coverage to 80%+.
"""

import pytest
from datetime import datetime, timezone, timedelta
from instruments_service.app.core.batch_processor import InstrumentBatchProcessor


class TestInstrumentBatchProcessorExtended:
    """Extended tests for InstrumentBatchProcessor."""

    def test_batch_processor_initialization(self):
        """Test batch processor initialization."""
        config = {"max_batch_size": 1000, "lookback_days": 5}
        processor = InstrumentBatchProcessor(config)
        assert processor.max_batch_size == 1000
        assert processor.lookback_days == 5

    def test_estimate_memory_requirements_single_date(self):
        """Test memory estimation for single date."""
        config = {"max_batch_size": 1000, "lookback_days": 5}
        processor = InstrumentBatchProcessor(config)

        estimate = processor.estimate_memory_requirements(num_instruments=1000, date_range_days=1)
        assert estimate["num_instruments"] == 1000
        assert estimate["date_range_days"] == 1
        assert estimate["estimated_mb"] > 0

    def test_estimate_memory_requirements_date_range(self):
        """Test memory estimation for date range."""
        config = {"max_batch_size": 1000, "lookback_days": 5}
        processor = InstrumentBatchProcessor(config)

        estimate = processor.estimate_memory_requirements(num_instruments=5000, date_range_days=31)
        assert estimate["num_instruments"] == 5000
        assert estimate["date_range_days"] == 31
        assert estimate["estimated_mb"] > 0

    def test_process_batch_small(self):
        """Test processing small batch."""
        config = {"max_batch_size": 1000, "lookback_days": 5}
        processor = InstrumentBatchProcessor(config)

        instruments = [{"id": i} for i in range(500)]
        batches = processor.process_batch(instruments)
        assert len(batches) == 1
        assert len(batches[0]) == 500

    def test_process_batch_large(self):
        """Test processing large batch that needs splitting."""
        config = {"max_batch_size": 100, "lookback_days": 5}  # Changed to 100 to force splitting
        processor = InstrumentBatchProcessor(config)

        instruments = [{"id": i} for i in range(250)]
        batches = processor.process_batch(instruments)
        assert len(batches) > 1  # Should split into multiple batches (250/100 = 3 batches)
        assert len(batches[0]) == 100  # First batch should be exactly 100
