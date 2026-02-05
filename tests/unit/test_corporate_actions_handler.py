"""
Unit Tests for Corporate Actions Handler

Tests for the CorporateActionsHandler class methods including:
- Date normalization
- Date filtering
- File saving with daily partitioning
- Schema constants
"""

import pytest
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock
import tempfile
import shutil

import pandas as pd

from instruments_service.cli.handlers.corporate_actions_handler import (
    CorporateActionsHandler,
    DIVIDENDS_SCHEMA,
    SPLITS_SCHEMA,
    EARNINGS_SCHEMA,
    parse_date,
)


# =============================================================================
# Schema Tests
# =============================================================================


class TestSchemaConstants:
    """Tests for schema constant definitions."""

    def test_dividends_schema_has_required_columns(self):
        """Verify dividends schema contains all required columns."""
        required = ["ticker", "ex_date", "amount", "dividend_type", "source"]
        for col in required:
            assert col in DIVIDENDS_SCHEMA, f"Missing required column: {col}"

    def test_splits_schema_has_required_columns(self):
        """Verify splits schema contains all required columns."""
        required = ["ticker", "effective_date", "ratio", "is_reverse_split", "source"]
        for col in required:
            assert col in SPLITS_SCHEMA, f"Missing required column: {col}"

    def test_earnings_schema_has_required_columns(self):
        """Verify earnings schema contains all required columns."""
        required = ["ticker", "earnings_date", "source"]
        for col in required:
            assert col in EARNINGS_SCHEMA, f"Missing required column: {col}"

    def test_all_schemas_have_instrument_key(self):
        """All schemas should have instrument_key for linking."""
        assert "instrument_key" in DIVIDENDS_SCHEMA
        assert "instrument_key" in SPLITS_SCHEMA
        assert "instrument_key" in EARNINGS_SCHEMA

    def test_all_schemas_have_fetched_at(self):
        """All schemas should have fetched_at for audit."""
        assert "fetched_at" in DIVIDENDS_SCHEMA
        assert "fetched_at" in SPLITS_SCHEMA
        assert "fetched_at" in EARNINGS_SCHEMA


# =============================================================================
# Date Parsing Tests
# =============================================================================


class TestParseDate:
    """Tests for the parse_date function."""

    def test_valid_date_string(self):
        """Parse valid YYYY-MM-DD string."""
        result = parse_date("2024-05-10")
        assert result == date(2024, 5, 10)

    def test_invalid_format_raises(self):
        """Invalid format should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("05-10-2024")

    def test_invalid_date_raises(self):
        """Invalid date values should raise ValueError."""
        with pytest.raises(ValueError):
            parse_date("2024-13-01")  # Invalid month


# =============================================================================
# Handler Method Tests
# =============================================================================


class TestCorporateActionsHandler:
    """Tests for CorporateActionsHandler methods."""

    @pytest.fixture
    def handler(self):
        """Create a handler instance with mocked dependencies."""
        with patch("instruments_service.cli.handlers.corporate_actions_handler.CorporateActionsAdapter"):
            handler = CorporateActionsHandler({"project_id": "test-project"})
            yield handler

    @pytest.fixture
    def temp_output_dir(self, handler):
        """Create a temporary output directory for tests."""
        temp_dir = Path(tempfile.mkdtemp())
        handler.output_dir = temp_dir
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    # -------------------------------------------------------------------------
    # _normalize_date_column tests
    # -------------------------------------------------------------------------

    def test_normalize_date_column_with_valid_dates(self, handler):
        """Normalize datetime column to date objects."""
        df = pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "ex_date": pd.to_datetime(["2024-05-10", "2024-05-15"]),
        })

        result = handler._normalize_date_column(df, "ex_date")

        assert result["ex_date"].iloc[0] == date(2024, 5, 10)
        assert result["ex_date"].iloc[1] == date(2024, 5, 15)

    def test_normalize_date_column_with_string_dates(self, handler):
        """Normalize string dates to date objects."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": ["2024-05-10"],
        })

        result = handler._normalize_date_column(df, "ex_date")

        assert result["ex_date"].iloc[0] == date(2024, 5, 10)

    def test_normalize_date_column_empty_df(self, handler):
        """Return empty df unchanged."""
        df = pd.DataFrame(columns=["ticker", "ex_date"])
        result = handler._normalize_date_column(df, "ex_date")
        assert result.empty

    def test_normalize_date_column_missing_column(self, handler):
        """Return df unchanged if column doesn't exist."""
        df = pd.DataFrame({"ticker": ["AAPL"]})
        result = handler._normalize_date_column(df, "ex_date")
        assert "ex_date" not in result.columns

    def test_normalize_date_column_preserves_original(self, handler):
        """Original dataframe should not be modified."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": pd.to_datetime(["2024-05-10"]),
        })
        original_type = type(df["ex_date"].iloc[0])

        handler._normalize_date_column(df, "ex_date")

        # Original should be unchanged
        assert type(df["ex_date"].iloc[0]) == original_type

    # -------------------------------------------------------------------------
    # _filter_by_date tests
    # -------------------------------------------------------------------------

    def test_filter_by_date_matching_records(self, handler):
        """Filter returns only matching records."""
        df = pd.DataFrame({
            "ticker": ["AAPL", "MSFT", "GOOGL"],
            "ex_date": [date(2024, 5, 10), date(2024, 5, 10), date(2024, 5, 15)],
            "amount": [0.24, 0.75, 0.50],
        })

        result = handler._filter_by_date(
            df, "ex_date", date(2024, 5, 10), ["ticker", "ex_date", "amount"]
        )

        assert len(result) == 2
        assert set(result["ticker"].tolist()) == {"AAPL", "MSFT"}

    def test_filter_by_date_no_matches(self, handler):
        """Filter returns empty df with schema when no matches."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })

        result = handler._filter_by_date(
            df, "ex_date", date(2024, 5, 15), ["ticker", "ex_date", "amount"]
        )

        assert result.empty
        assert list(result.columns) == ["ticker", "ex_date", "amount"]

    def test_filter_by_date_empty_input(self, handler):
        """Filter empty df returns empty df with schema."""
        df = pd.DataFrame(columns=["ticker", "ex_date", "amount"])

        result = handler._filter_by_date(
            df, "ex_date", date(2024, 5, 10), ["ticker", "ex_date", "amount"]
        )

        assert result.empty
        assert list(result.columns) == ["ticker", "ex_date", "amount"]

    def test_filter_by_date_missing_column(self, handler):
        """Filter with missing column returns empty df with schema."""
        df = pd.DataFrame({"ticker": ["AAPL"]})

        result = handler._filter_by_date(
            df, "ex_date", date(2024, 5, 10), ["ticker", "ex_date", "amount"]
        )

        assert result.empty
        assert list(result.columns) == ["ticker", "ex_date", "amount"]

    def test_filter_by_date_partial_schema(self, handler):
        """Filter only includes available schema columns."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            # 'amount' missing
        })

        result = handler._filter_by_date(
            df, "ex_date", date(2024, 5, 10), ["ticker", "ex_date", "amount"]
        )

        assert len(result) == 1
        assert "ticker" in result.columns
        assert "ex_date" in result.columns
        assert "amount" not in result.columns  # Not in source df

    # -------------------------------------------------------------------------
    # _save_results tests
    # -------------------------------------------------------------------------

    def test_save_results_creates_daily_directories(self, handler, temp_output_dir):
        """Save results creates by_date/day-{date}/ structure."""
        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 10), "parquet"
        )

        day_dir = temp_output_dir / "by_date" / "day-2024-05-10"
        assert day_dir.exists()
        assert (day_dir / "dividends.parquet").exists()

    def test_save_results_skips_empty_days(self, handler, temp_output_dir):
        """Save results skips days with no corporate actions."""
        # Only has data for 2024-05-10, not 2024-05-11
        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        result = handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 12), "parquet"
        )

        # Should only have files for 2024-05-10
        assert any(f["date"] == "2024-05-10" for f in result)
        assert not any(f["date"] == "2024-05-11" for f in result)
        assert not any(f["date"] == "2024-05-12" for f in result)

        # Directories for empty days should not exist
        assert not (temp_output_dir / "by_date" / "day-2024-05-11").exists()
        assert not (temp_output_dir / "by_date" / "day-2024-05-12").exists()

    def test_save_results_multiple_action_types_same_day(self, handler, temp_output_dir):
        """Save results handles multiple action types on same day."""
        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })
        splits_df = pd.DataFrame({
            "ticker": ["NVDA"],
            "effective_date": [date(2024, 5, 10)],
            "ratio": [10.0],
        })
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        result = handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 10), "parquet"
        )

        # Should have both dividends and splits for same day
        day_types = {f["action_type"] for f in result if f["date"] == "2024-05-10"}
        assert "dividends" in day_types
        assert "splits" in day_types

    def test_save_results_csv_format(self, handler, temp_output_dir):
        """Save results works with CSV format."""
        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        result = handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 10), "csv"
        )

        assert len(result) == 1
        assert result[0]["path"].endswith(".csv")
        assert (temp_output_dir / "by_date" / "day-2024-05-10" / "dividends.csv").exists()

    def test_save_results_returns_correct_structure(self, handler, temp_output_dir):
        """Save results returns list of dicts with required keys."""
        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
            "amount": [0.24],
        })
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        result = handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 10), "parquet"
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert "action_type" in result[0]
        assert "date" in result[0]
        assert "path" in result[0]
        assert result[0]["action_type"] == "dividends"
        assert result[0]["date"] == "2024-05-10"

    def test_save_results_all_empty_returns_empty_list(self, handler, temp_output_dir):
        """Save results with all empty dataframes returns empty list."""
        dividends_df = pd.DataFrame(columns=DIVIDENDS_SCHEMA)
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)

        result = handler._save_results(
            dividends_df, splits_df, earnings_df,
            date(2024, 5, 10), date(2024, 5, 12), "parquet"
        )

        assert result == []
        # No directories should be created
        assert not (temp_output_dir / "by_date").exists()

    # -------------------------------------------------------------------------
    # _write_day_files tests
    # -------------------------------------------------------------------------

    def test_write_day_files_only_writes_non_empty(self, handler, temp_output_dir):
        """Write day files only creates files for non-empty dataframes."""
        day_dir = temp_output_dir / "by_date" / "day-2024-05-10"
        day_dir.mkdir(parents=True)

        dividends_df = pd.DataFrame({
            "ticker": ["AAPL"],
            "ex_date": [date(2024, 5, 10)],
        })
        splits_df = pd.DataFrame(columns=SPLITS_SCHEMA)  # Empty
        earnings_df = pd.DataFrame(columns=EARNINGS_SCHEMA)  # Empty

        result = handler._write_day_files(
            day_dir, "2024-05-10", "parquet",
            dividends_df, splits_df, earnings_df
        )

        # Only dividends file should be created
        assert len(result) == 1
        assert result[0]["action_type"] == "dividends"
        assert (day_dir / "dividends.parquet").exists()
        assert not (day_dir / "splits.parquet").exists()
        assert not (day_dir / "earnings.parquet").exists()
