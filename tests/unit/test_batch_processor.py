"""
Unit tests for InstrumentBatchProcessor.
"""

from datetime import UTC, datetime, timedelta

from instruments_service.app.core.batch_processor import InstrumentBatchProcessor


class TestInstrumentBatchProcessor:
    """Test InstrumentBatchProcessor."""

    def test_batch_processor_creation(self):
        """Test creating InstrumentBatchProcessor with defaults."""
        config = {}
        processor = InstrumentBatchProcessor(config)
        assert processor.max_batch_size == 1000
        assert processor.lookback_days == 0

    def test_batch_processor_initialization_with_config(self):
        """Test batch processor initialization with custom config."""
        config = {"max_batch_size": 500, "lookback_days": 7}
        processor = InstrumentBatchProcessor(config)
        assert processor.max_batch_size == 500
        assert processor.lookback_days == 7

    def test_calculate_date_range_no_lookback(self):
        """Test date range calculation without lookback."""
        config = {}
        processor = InstrumentBatchProcessor(config)
        target_date = datetime(2023, 5, 23, tzinfo=UTC)
        start_date, end_date = processor.calculate_date_range(target_date)
        assert start_date == target_date
        assert end_date == target_date

    def test_calculate_date_range_with_lookback(self):
        """Test date range calculation with lookback."""
        config = {"lookback_days": 7}
        processor = InstrumentBatchProcessor(config)
        target_date = datetime(2023, 5, 23, tzinfo=UTC)
        start_date, end_date = processor.calculate_date_range(target_date)
        assert start_date == target_date - timedelta(days=7)
        assert end_date == target_date

    def test_get_required_periods(self):
        """Test getting required periods."""
        config = {}
        processor = InstrumentBatchProcessor(config)
        target_date = datetime(2023, 5, 23, tzinfo=UTC)
        periods = processor.get_required_periods(target_date)
        assert len(periods) == 1
        assert periods[0] == target_date

    def test_get_required_periods_multiple_days(self):
        """Test getting required periods for multiple days."""
        config = {"lookback_days": 2}
        processor = InstrumentBatchProcessor(config)
        target_date = datetime(2023, 5, 23, tzinfo=UTC)
        periods = processor.get_required_periods(target_date)
        assert len(periods) == 3  # 2 lookback + 1 target

    def test_estimate_memory_requirements_single_date(self):
        """Test memory estimation for single date."""
        config = {}
        processor = InstrumentBatchProcessor(config)
        estimate = processor.estimate_memory_requirements(num_instruments=1000, date_range_days=1)
        assert estimate["num_instruments"] == 1000
        assert estimate["date_range_days"] == 1
        assert estimate["estimated_mb"] > 0

    def test_estimate_memory_requirements_date_range(self):
        """Test memory estimation for date range."""
        config = {}
        processor = InstrumentBatchProcessor(config)
        estimate = processor.estimate_memory_requirements(num_instruments=5000, date_range_days=31)
        assert estimate["num_instruments"] == 5000
        assert estimate["date_range_days"] == 31
        assert estimate["estimated_mb"] > 0

    def test_process_batch_small(self):
        """Test processing small batch."""
        config = {"max_batch_size": 1000}
        processor = InstrumentBatchProcessor(config)
        instruments = [{"id": i} for i in range(500)]
        batches = processor.process_batch(instruments)
        assert len(batches) == 1
        assert len(batches[0]) == 500

    def test_process_batch_large(self):
        """Test batch processing that needs splitting."""
        config = {"max_batch_size": 100}
        processor = InstrumentBatchProcessor(config)
        instruments = [{"id": i} for i in range(250)]
        batches = processor.process_batch(instruments)
        assert len(batches) == 3  # 250 / 100 = 3 batches
        assert len(batches[0]) == 100
        assert len(batches[2]) == 50  # Last batch has remainder
