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
        config = {"test": "config"}
        processor = InstrumentBatchProcessor(config)
        assert processor.config == config

    def test_estimate_memory_requirements_single_date(self):
        """Test memory estimation for single date."""
        config = {"test": "config"}
        processor = InstrumentBatchProcessor(config)

        estimate = processor.estimate_memory_requirements(
            num_instruments=1000, date_range_days=1
        )
        assert estimate["num_instruments"] == 1000
        assert estimate["date_range_days"] == 1
        assert estimate["estimated_mb"] > 0

    def test_estimate_memory_requirements_date_range(self):
        """Test memory estimation for date range."""
        config = {"test": "config"}
        processor = InstrumentBatchProcessor(config)

        estimate = processor.estimate_memory_requirements(
            num_instruments=5000, date_range_days=31
        )
        assert estimate["num_instruments"] == 5000
        assert estimate["date_range_days"] == 31
        assert estimate["estimated_mb"] > 0

    def test_process_batch_small(self):
        """Test processing small batch."""
        config = {"test": "config", "max_batch_size": 1000}
        processor = InstrumentBatchProcessor(config)

        instruments = [{"id": i} for i in range(500)]
        batches = processor.process_batch(instruments)
        assert len(batches) == 1
        assert len(batches[0]) == 500

    def test_process_batch_large(self):
        """Test processing large batch that needs splitting."""
        config = {"test": "config", "max_batch_size": 100}
        processor = InstrumentBatchProcessor(config)

        instruments = [{"id": i} for i in range(250)]
        batches = processor.process_batch(instruments)
        assert len(batches) > 1  # Should split into multiple batches
        assert len(batches[0]) == 100
