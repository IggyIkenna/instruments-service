"""
Unit tests for DateFilterService.

Tests uniform date filtering logic for instruments.
"""

import pytest
from datetime import datetime, timezone

from instruments_service.utils.date_filter_service import DateFilterService


class TestDateFilterService:
    """Test DateFilterService functionality."""

    @pytest.fixture
    def date_filter_service(self):
        """Create DateFilterService fixture."""
        return DateFilterService()

    @pytest.fixture
    def sample_instruments(self):
        """Create sample instruments for testing."""
        return {
            "INST1": {
                "instrument_key": "INST1",
                "available_from_datetime": "2024-01-01T00:00:00Z",
                "available_to_datetime": None,
            },
            "INST2": {
                "instrument_key": "INST2",
                "available_from_datetime": "2023-01-01T00:00:00Z",
                "available_to_datetime": "2023-12-31T23:59:59Z",
            },
            "INST3": {
                "instrument_key": "INST3",
                "available_from_datetime": None,
                "available_to_datetime": None,
            },
        }

    def test_init(self, date_filter_service):
        """Test DateFilterService initialization."""
        assert date_filter_service._protocol_defaults is not None
        assert "uniswap_v3" in date_filter_service._protocol_defaults
        assert "ethena" in date_filter_service._protocol_defaults

    def test_filter_instruments_by_date_before_launch(
        self, date_filter_service, sample_instruments
    ):
        """Test filtering instruments before launch date."""
        target_date = datetime(2023, 6, 1, tzinfo=timezone.utc)
        filtered = date_filter_service.filter_instruments_by_date(
            instruments=sample_instruments,
            target_date=target_date,
        )

        # INST1 should be filtered out (launched 2024-01-01)
        # INST2 should be included (available in 2023)
        # INST3 should be included (no date restriction)
        assert "INST1" not in filtered
        assert "INST2" in filtered
        assert "INST3" in filtered

    def test_filter_instruments_by_date_after_launch(self, date_filter_service, sample_instruments):
        """Test filtering instruments after launch date."""
        target_date = datetime(2024, 6, 1, tzinfo=timezone.utc)
        filtered = date_filter_service.filter_instruments_by_date(
            instruments=sample_instruments,
            target_date=target_date,
        )

        # All instruments should be included
        assert "INST1" in filtered
        assert "INST2" not in filtered  # Expired
        assert "INST3" in filtered

    def test_filter_instruments_with_protocol_default(self, date_filter_service):
        """Test filtering with protocol default dates."""
        instruments = {
            "INST1": {
                "instrument_key": "INST1",
                "available_from_datetime": None,  # No date set
            }
        }

        # Filter for date before protocol launch
        target_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        filtered = date_filter_service.filter_instruments_by_date(
            instruments=instruments,
            target_date=target_date,
            protocol="uniswap_v3",  # Launched 2021-05-05
        )

        # Should be filtered out (protocol launched after target date)
        assert "INST1" not in filtered

    def test_get_protocol_default_date(self, date_filter_service):
        """Test getting protocol default dates."""
        date = date_filter_service.get_protocol_default_date("uniswap_v3", "available_from")
        assert date == "2021-05-05T00:00:00Z"

        date = date_filter_service.get_protocol_default_date("ethena", "available_from")
        assert date == "2024-02-16T00:00:00Z"

        # Non-existent protocol
        date = date_filter_service.get_protocol_default_date("unknown", "available_from")
        assert date is None

    def test_set_protocol_default_date(self, date_filter_service):
        """Test setting protocol default dates."""
        date_filter_service.set_protocol_default_date(
            "test_protocol", "available_from", "2024-01-01T00:00:00Z"
        )

        date = date_filter_service.get_protocol_default_date("test_protocol", "available_from")
        assert date == "2024-01-01T00:00:00Z"

    def test_filter_empty_instruments(self, date_filter_service):
        """Test filtering empty instrument dictionary."""
        filtered = date_filter_service.filter_instruments_by_date(
            instruments={},
            target_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert filtered == {}

    def test_filter_timezone_aware_date(self, date_filter_service, sample_instruments):
        """Test filtering with timezone-aware dates."""
        # Test with timezone-aware date
        target_date = datetime(2024, 6, 1, tzinfo=timezone.utc)
        filtered = date_filter_service.filter_instruments_by_date(
            instruments=sample_instruments,
            target_date=target_date,
        )
        assert len(filtered) > 0

        # Test with naive date (should be converted to UTC)
        target_date_naive = datetime(2024, 6, 1)
        filtered_naive = date_filter_service.filter_instruments_by_date(
            instruments=sample_instruments,
            target_date=target_date_naive,
        )
        assert len(filtered_naive) > 0
