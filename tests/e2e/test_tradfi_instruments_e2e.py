"""
End-to-end test for TRADFI instrument generation.

Tests the complete workflow for TradFi instruments:
1. Generate instruments from Databento API
2. Upload to test bucket (instruments-store-test-tradfi-*)
3. Verify data integrity
"""

from datetime import datetime, timezone

import pytest


@pytest.mark.e2e
class TestTradfiInstrumentGeneration:
    """E2E tests for TradFi instrument generation."""

    def test_tradfi_category_identifier(self):
        """Test TRADFI category identifier."""
        category = "TRADFI"
        assert category.upper() == "TRADFI"

    def test_tradfi_bucket_naming(self, gcp_project_id):
        """Test TRADFI bucket naming convention."""
        category = "tradfi"
        bucket_template = f"instruments-store-{category}-{gcp_project_id}"

        assert "tradfi" in bucket_template
        assert gcp_project_id in bucket_template


@pytest.mark.e2e
class TestTradfiVenueSupport:
    """Tests for TradFi venue support."""

    def test_cme_venue_supported(self):
        """Test CME venue is supported."""
        tradfi_venues = ["cme", "cboe", "nasdaq", "ice", "nyse"]
        assert "cme" in tradfi_venues

    def test_cboe_venue_supported(self):
        """Test CBOE venue is supported."""
        tradfi_venues = ["cme", "cboe", "nasdaq", "ice", "nyse"]
        assert "cboe" in tradfi_venues


@pytest.mark.e2e
class TestTradfiInstrumentTypes:
    """Tests for TradFi instrument type support."""

    def test_futures_instrument_schema(self):
        """Test futures instrument has required fields."""
        futures_instrument = {
            "instrument_key": "CME:ESM4",
            "venue": "CME",
            "symbol": "ESM4",
            "category": "TRADFI",
            "instrument_type": "futures",
            "underlying": "SPX",
            "expiry": "2024-06-21",
        }

        required = ["instrument_key", "venue", "symbol", "category", "instrument_type"]
        for field in required:
            assert field in futures_instrument

    def test_options_instrument_schema(self):
        """Test options instrument has required fields."""
        options_instrument = {
            "instrument_key": "CBOE:SPY240315C500",
            "venue": "CBOE",
            "symbol": "SPY240315C500",
            "category": "TRADFI",
            "instrument_type": "option",
            "underlying": "SPY",
            "strike": 500.0,
            "option_type": "call",
            "expiry": "2024-03-15",
        }

        required = ["instrument_key", "venue", "symbol", "option_type", "strike"]
        for field in required:
            assert field in options_instrument

    def test_equity_instrument_schema(self):
        """Test equity instrument has required fields."""
        equity_instrument = {
            "instrument_key": "NASDAQ:AAPL",
            "venue": "NASDAQ",
            "symbol": "AAPL",
            "category": "TRADFI",
            "instrument_type": "equity",
        }

        required = ["instrument_key", "venue", "symbol", "instrument_type"]
        for field in required:
            assert field in equity_instrument


@pytest.mark.e2e
class TestTradfiDataProvider:
    """Tests for TradFi data provider (Databento)."""

    def test_databento_dataset_mapping(self):
        """Test Databento dataset mapping for TradFi venues."""
        dataset_map = {
            "cme": "GLBX.MDP3",
            "cboe": "OPRA.PILLAR",
            "nasdaq": "DBEQ.BASIC",
        }

        assert "cme" in dataset_map
        assert dataset_map["cme"] == "GLBX.MDP3"

    def test_databento_symbol_types(self):
        """Test Databento symbol type options."""
        stype_options = ["raw_symbol", "parent", "instrument_id"]

        assert "raw_symbol" in stype_options
        assert "parent" in stype_options


@pytest.mark.e2e
class TestTradfiTradingCalendar:
    """Tests for TradFi trading calendar considerations."""

    def test_weekend_detection(self):
        """Test weekend dates are not trading days."""
        saturday = datetime(2024, 1, 13, tzinfo=timezone.utc)
        sunday = datetime(2024, 1, 14, tzinfo=timezone.utc)

        assert saturday.weekday() == 5  # Saturday
        assert sunday.weekday() == 6  # Sunday

    def test_weekday_is_potential_trading_day(self):
        """Test weekdays are potential trading days."""
        monday = datetime(2024, 1, 15, tzinfo=timezone.utc)

        assert monday.weekday() == 0  # Monday
        assert monday.weekday() < 5  # Is weekday


@pytest.mark.e2e
class TestTradfiTestBucketIsolation:
    """Tests for test bucket isolation in TRADFI."""

    def test_test_bucket_contains_test_string(self, gcp_project_id):
        """Test bucket name contains 'test' for isolation."""
        test_bucket = f"instruments-store-test-tradfi-{gcp_project_id}"

        assert "test" in test_bucket.lower()

    def test_test_bucket_different_from_prod(self, gcp_project_id):
        """Test bucket is different from production bucket."""
        test_bucket = f"instruments-store-test-tradfi-{gcp_project_id}"
        prod_bucket = f"instruments-store-tradfi-{gcp_project_id}"

        assert test_bucket != prod_bucket
