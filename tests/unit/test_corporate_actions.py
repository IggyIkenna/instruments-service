"""
Unit Tests for Corporate Actions Module

Tests for corporate actions models and adapter.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.corporate_actions.adapter import CorporateActionsAdapter
from instruments_service.corporate_actions.models import (
    CorporateActionsBundle,
    DividendRecord,
    DividendType,
    EarningsRecord,
    StockSplitRecord,
)

# =============================================================================
# Model Tests
# =============================================================================


class TestDividendRecord:
    """Tests for DividendRecord model."""

    def test_basic_dividend(self):
        """Test creating a basic dividend record."""
        div = DividendRecord(
            ticker="AAPL",
            ex_date=date(2024, 5, 10),
            amount=0.24,
        )
        assert div.ticker == "AAPL"
        assert div.ex_date == date(2024, 5, 10)
        assert div.amount == 0.24
        assert div.dividend_type == DividendType.UNSPECIFIED
        assert div.currency == "USD"
        assert div.source == "yfinance"

    def test_full_dividend(self):
        """Test creating a dividend with all fields."""
        div = DividendRecord(
            ticker="msft",  # lowercase should be uppercased
            ex_date=date(2024, 5, 10),
            pay_date=date(2024, 6, 15),
            record_date=date(2024, 5, 11),
            declaration_date=date(2024, 4, 20),
            amount=0.75,
            dividend_type=DividendType.REGULAR,
            currency="USD",
            source="alpha_vantage",
            instrument_key="NASDAQ:EQUITY:MSFT",
        )
        assert div.ticker == "MSFT"
        assert div.pay_date == date(2024, 6, 15)
        assert div.dividend_type == DividendType.REGULAR
        assert div.instrument_key == "NASDAQ:EQUITY:MSFT"

    def test_dividend_nan_amount(self):
        """Test that NaN amounts are converted to 0."""
        div = DividendRecord(
            ticker="AAPL",
            ex_date=date(2024, 5, 10),
            amount=float("nan"),
        )
        assert div.amount == 0.0

    def test_dividend_none_amount(self):
        """Test that None amounts are converted to 0."""
        div = DividendRecord(
            ticker="AAPL",
            ex_date=date(2024, 5, 10),
            amount=None,
        )
        assert div.amount == 0.0

    def test_dividend_to_dict(self):
        """Test conversion to dictionary."""
        div = DividendRecord(
            ticker="AAPL",
            ex_date=date(2024, 5, 10),
            amount=0.24,
        )
        d = div.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["ex_date"] == "2024-05-10"
        assert d["amount"] == 0.24
        assert "fetched_at" in d

    def test_empty_ticker_raises(self):
        """Test that empty ticker raises error."""
        with pytest.raises(ValueError):
            DividendRecord(
                ticker="",
                ex_date=date(2024, 5, 10),
                amount=0.24,
            )


class TestStockSplitRecord:
    """Tests for StockSplitRecord model."""

    def test_forward_split(self):
        """Test creating a forward split (e.g., 4:1)."""
        split = StockSplitRecord(
            ticker="AAPL",
            effective_date=date(2020, 8, 31),
            ratio=4.0,
            split_from=1,
            split_to=4,
        )
        assert split.ticker == "AAPL"
        assert split.ratio == 4.0
        assert split.is_reverse_split is False
        assert split.adjustment_factor == 0.25

    def test_reverse_split(self):
        """Test creating a reverse split (e.g., 1:10)."""
        split = StockSplitRecord(
            ticker="GE",
            effective_date=date(2021, 8, 2),
            ratio=0.125,  # 1:8 reverse split
            split_from=8,
            split_to=1,
        )
        assert split.ticker == "GE"
        assert split.ratio == 0.125
        assert split.is_reverse_split is True
        assert split.adjustment_factor == 8.0

    def test_ratio_string_parsing(self):
        """Test that string ratios like '4:1' are parsed."""
        split = StockSplitRecord(
            ticker="NVDA",
            effective_date=date(2024, 6, 10),
            ratio="10:1",
        )
        assert split.ratio == 10.0

    def test_split_to_dict(self):
        """Test conversion to dictionary."""
        split = StockSplitRecord(
            ticker="AAPL",
            effective_date=date(2020, 8, 31),
            ratio=4.0,
        )
        d = split.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["effective_date"] == "2020-08-31"
        assert d["ratio"] == 4.0
        assert d["is_reverse_split"] is False
        assert d["adjustment_factor"] == 0.25


class TestEarningsRecord:
    """Tests for EarningsRecord model."""

    def test_basic_earnings(self):
        """Test creating a basic earnings record."""
        earn = EarningsRecord(
            ticker="AAPL",
            earnings_date=date(2024, 5, 2),
        )
        assert earn.ticker == "AAPL"
        assert earn.earnings_date == date(2024, 5, 2)
        assert earn.reported_eps is None
        assert earn.estimated_eps is None

    def test_full_earnings(self):
        """Test creating earnings with all fields."""
        earn = EarningsRecord(
            ticker="AAPL",
            earnings_date=date(2024, 5, 2),
            earnings_time="AMC",
            fiscal_quarter="Q2 2024",
            fiscal_year=2024,
            reported_eps=1.53,
            estimated_eps=1.50,
            surprise_pct=2.0,
            revenue=90753000000,
            estimated_revenue=90000000000,
        )
        assert earn.earnings_time == "AMC"
        assert earn.reported_eps == 1.53
        assert earn.surprise_pct == 2.0

    def test_earnings_time_validation(self):
        """Test earnings time validation."""
        # Valid times
        earn = EarningsRecord(ticker="AAPL", earnings_date=date(2024, 1, 1), earnings_time="bmo")
        assert earn.earnings_time == "BMO"

        earn = EarningsRecord(ticker="AAPL", earnings_date=date(2024, 1, 1), earnings_time="amc")
        assert earn.earnings_time == "AMC"

        # Invalid time returns None
        earn = EarningsRecord(ticker="AAPL", earnings_date=date(2024, 1, 1), earnings_time="invalid")
        assert earn.earnings_time is None

    def test_earnings_to_dict(self):
        """Test conversion to dictionary."""
        earn = EarningsRecord(
            ticker="AAPL",
            earnings_date=date(2024, 5, 2),
            reported_eps=1.53,
        )
        d = earn.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["earnings_date"] == "2024-05-02"
        assert d["reported_eps"] == 1.53


class TestCorporateActionsBundle:
    """Tests for CorporateActionsBundle model."""

    def test_empty_bundle(self):
        """Test creating an empty bundle."""
        bundle = CorporateActionsBundle(
            ticker="AAPL",
            start_date=date(2020, 1, 1),
            end_date=date(2024, 12, 31),
        )
        assert bundle.ticker == "AAPL"
        assert bundle.total_records == 0

    def test_bundle_with_records(self):
        """Test bundle with records."""
        div1 = DividendRecord(ticker="AAPL", ex_date=date(2024, 5, 10), amount=0.24)
        div2 = DividendRecord(ticker="AAPL", ex_date=date(2024, 8, 10), amount=0.24)
        split1 = StockSplitRecord(ticker="AAPL", effective_date=date(2020, 8, 31), ratio=4.0)

        bundle = CorporateActionsBundle(
            ticker="AAPL",
            dividends=[div1, div2],
            splits=[split1],
            start_date=date(2020, 1, 1),
            end_date=date(2024, 12, 31),
        )
        assert bundle.total_records == 3


# =============================================================================
# Adapter Tests (with mocking)
# =============================================================================


class TestCorporateActionsAdapter:
    """Tests for CorporateActionsAdapter."""

    @patch("instruments_service.corporate_actions.adapter.CorporateActionsAdapter.yf")
    def test_fetch_dividends(self, mock_yf):
        """Test fetching dividends with mocked yfinance."""
        # Setup mock
        mock_ticker = MagicMock()
        mock_yf.Ticker.return_value = mock_ticker

        # Create sample dividend data
        div_index = pd.to_datetime(["2024-05-10", "2024-08-12"], utc=True)
        mock_ticker.dividends = pd.Series([0.24, 0.25], index=div_index)

        # Test adapter
        adapter = CorporateActionsAdapter()
        adapter._yf = mock_yf

        dividends = adapter.fetch_dividends(
            ticker="AAPL",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )

        assert len(dividends) == 2
        assert dividends[0].ticker == "AAPL"
        assert dividends[0].amount == 0.24

    @patch("instruments_service.corporate_actions.adapter.CorporateActionsAdapter.yf")
    def test_fetch_splits(self, mock_yf):
        """Test fetching splits with mocked yfinance."""
        # Setup mock
        mock_ticker = MagicMock()
        mock_yf.Ticker.return_value = mock_ticker

        # Create sample split data
        split_index = pd.to_datetime(["2020-08-31", "2014-06-09"], utc=True)
        mock_ticker.splits = pd.Series([4.0, 7.0], index=split_index)

        # Test adapter
        adapter = CorporateActionsAdapter()
        adapter._yf = mock_yf

        splits = adapter.fetch_splits(
            ticker="AAPL",
            start_date=date(2010, 1, 1),
            end_date=date(2024, 12, 31),
        )

        assert len(splits) == 2
        assert splits[0].ratio == 4.0
        assert splits[1].ratio == 7.0

    @patch("instruments_service.corporate_actions.adapter.CorporateActionsAdapter.yf")
    def test_fetch_empty_dividends(self, mock_yf):
        """Test fetching dividends when none exist."""
        mock_ticker = MagicMock()
        mock_yf.Ticker.return_value = mock_ticker
        mock_ticker.dividends = pd.Series([], dtype=float)

        adapter = CorporateActionsAdapter()
        adapter._yf = mock_yf

        dividends = adapter.fetch_dividends(
            ticker="NVDA",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )

        assert len(dividends) == 0

    @patch("instruments_service.corporate_actions.adapter.CorporateActionsAdapter.yf")
    def test_to_dataframes(self, mock_yf):
        """Test converting bundles to DataFrames."""
        # Create sample bundles
        bundles = {
            "AAPL": CorporateActionsBundle(
                ticker="AAPL",
                dividends=[
                    DividendRecord(ticker="AAPL", ex_date=date(2024, 5, 10), amount=0.24),
                ],
                splits=[
                    StockSplitRecord(ticker="AAPL", effective_date=date(2020, 8, 31), ratio=4.0),
                ],
                earnings=[],
                start_date=date(2020, 1, 1),
                end_date=date(2024, 12, 31),
            ),
            "MSFT": CorporateActionsBundle(
                ticker="MSFT",
                dividends=[
                    DividendRecord(ticker="MSFT", ex_date=date(2024, 5, 15), amount=0.75),
                ],
                splits=[],
                earnings=[],
                start_date=date(2020, 1, 1),
                end_date=date(2024, 12, 31),
            ),
        }

        adapter = CorporateActionsAdapter()
        dividends_df, splits_df, earnings_df = adapter.to_dataframes(bundles)

        assert len(dividends_df) == 2
        assert len(splits_df) == 1
        assert len(earnings_df) == 0

        assert "ticker" in dividends_df.columns
        assert "ex_date" in dividends_df.columns
        assert "amount" in dividends_df.columns


# =============================================================================
# Integration Tests (real yfinance, skip in CI)
# =============================================================================


@pytest.mark.integration
@pytest.mark.skipif(
    True,  # Skip by default, enable for manual testing
    reason="Integration test - requires internet and yfinance",
)
class TestCorporateActionsIntegration:
    """Integration tests with real yfinance calls."""

    def test_fetch_apple_dividends(self):
        """Fetch real AAPL dividends."""
        adapter = CorporateActionsAdapter()
        dividends = adapter.fetch_dividends(
            ticker="AAPL",
            start_date=date(2023, 1, 1),
            end_date=date(2024, 12, 31),
        )

        # AAPL pays quarterly dividends
        assert len(dividends) >= 4
        for div in dividends:
            assert div.ticker == "AAPL"
            assert div.amount > 0

    def test_fetch_nvidia_split(self):
        """Fetch real NVDA 10:1 split (June 2024)."""
        adapter = CorporateActionsAdapter()
        splits = adapter.fetch_splits(
            ticker="NVDA",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )

        # NVDA had a 10:1 split in June 2024
        assert len(splits) >= 1
        # Find the 10:1 split
        ten_for_one = [s for s in splits if s.ratio == 10.0]
        assert len(ten_for_one) >= 1
