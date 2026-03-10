"""
Unit tests for GenerateDateViewsHandler.

Tests _load_all_tickers_data, _generate_date_partitions, and run()
using tmp_path fixtures to avoid any cloud I/O.
"""

import pandas as pd
import pytest

from instruments_service.cli.handlers.generate_date_views_handler import (
    GenerateDateViewsHandler,
)


def _make_handler():
    return GenerateDateViewsHandler(config={"mode": "generate_date_views"})


# ─── _load_all_tickers_data ───────────────────────────────────────────────────


class TestLoadAllTickersData:
    def test_empty_dir_returns_empty_df(self, tmp_path):
        handler = _make_handler()
        result = handler._load_all_tickers_data(tmp_path, "dividends")
        assert result.empty

    def test_loads_csv_from_single_ticker_dir(self, tmp_path):
        ticker_dir = tmp_path / "AAPL"
        ticker_dir.mkdir()
        df = pd.DataFrame([{"ticker": "AAPL", "ex_date": "2025-03-01", "amount": 0.25}])
        df.to_csv(ticker_dir / "dividends.csv", index=False)

        handler = _make_handler()
        result = handler._load_all_tickers_data(tmp_path, "dividends")
        assert not result.empty
        assert "ticker" in result.columns

    def test_skips_ticker_without_action_type_csv(self, tmp_path):
        ticker_dir = tmp_path / "MSFT"
        ticker_dir.mkdir()
        # Only splits.csv, no dividends.csv
        pd.DataFrame([{"ticker": "MSFT", "effective_date": "2025-01-01"}]).to_csv(
            ticker_dir / "splits.csv", index=False
        )

        handler = _make_handler()
        result = handler._load_all_tickers_data(tmp_path, "dividends")
        assert result.empty

    def test_loads_multiple_tickers(self, tmp_path):
        for ticker in ["AAPL", "MSFT", "GOOG"]:
            d = tmp_path / ticker
            d.mkdir()
            pd.DataFrame(
                [{"ticker": ticker, "ex_date": f"2025-03-0{i + 1}", "amount": 0.1 * (i + 1)} for i in range(2)]
            ).to_csv(d / "dividends.csv", index=False)

        handler = _make_handler()
        result = handler._load_all_tickers_data(tmp_path, "dividends")
        assert len(result) == 6  # 3 tickers * 2 rows each

    def test_skips_files_not_dirs(self, tmp_path):
        # Put a file in the by_ticker dir (not a dir)
        (tmp_path / "metadata.json").write_text("{}")
        handler = _make_handler()
        result = handler._load_all_tickers_data(tmp_path, "dividends")
        assert result.empty


# ─── _generate_date_partitions ────────────────────────────────────────────────


class TestGenerateDatePartitions:
    def test_empty_df_returns_empty_dict(self, tmp_path):
        handler = _make_handler()
        result = handler._generate_date_partitions(pd.DataFrame(), "ex_date", "dividends", tmp_path)
        assert result == {}

    def test_creates_parquet_files_per_date(self, tmp_path):
        handler = _make_handler()
        df = pd.DataFrame(
            [
                {"ticker": "AAPL", "ex_date": "2025-03-01", "amount": 0.25},
                {"ticker": "MSFT", "ex_date": "2025-03-05", "amount": 0.30},
            ]
        )
        result = handler._generate_date_partitions(df, "ex_date", "dividends", tmp_path)
        assert len(result) == 2
        assert "2025-03-01" in result
        assert "2025-03-05" in result
        # Check parquet files created
        assert (tmp_path / "day=2025-03-01" / "dividends.parquet").exists()
        assert (tmp_path / "day=2025-03-05" / "dividends.parquet").exists()

    def test_multiple_records_per_date(self, tmp_path):
        handler = _make_handler()
        df = pd.DataFrame(
            [
                {"ticker": "AAPL", "ex_date": "2025-03-01", "amount": 0.25},
                {"ticker": "MSFT", "ex_date": "2025-03-01", "amount": 0.30},
            ]
        )
        result = handler._generate_date_partitions(df, "ex_date", "dividends", tmp_path)
        assert result["2025-03-01"] == 2


# ─── run() ───────────────────────────────────────────────────────────────────


class TestGenerateDateViewsHandlerRun:
    def test_raises_when_input_dir_missing(self, tmp_path):
        handler = _make_handler()
        with pytest.raises(ValueError, match="Input directory not found"):
            handler.run(input_dir=str(tmp_path / "nonexistent"), output_dir=str(tmp_path / "output"))

    def test_processes_empty_dirs_successfully(self, tmp_path):
        by_ticker = tmp_path / "by_ticker"
        by_ticker.mkdir()
        by_date = tmp_path / "by_date"

        handler = _make_handler()
        result = handler.run(input_dir=str(by_ticker), output_dir=str(by_date))
        assert result["status"] == "success"
        assert result["success"] is True

    def test_processes_dividend_data(self, tmp_path):
        by_ticker = tmp_path / "by_ticker"
        by_ticker.mkdir()
        # Add dividend data
        aapl = by_ticker / "AAPL"
        aapl.mkdir()
        pd.DataFrame(
            [
                {"ticker": "AAPL", "ex_date": "2025-03-28", "amount": 0.25},
            ]
        ).to_csv(aapl / "dividends.csv", index=False)

        by_date = tmp_path / "by_date"
        handler = _make_handler()
        result = handler.run(input_dir=str(by_ticker), output_dir=str(by_date))
        assert result["status"] == "success"
        stats = result["statistics"]["action_types"]
        assert stats["dividends"]["total_records"] == 1

    def test_creates_output_dir_if_missing(self, tmp_path):
        by_ticker = tmp_path / "by_ticker"
        by_ticker.mkdir()
        by_date = tmp_path / "new_output_dir"
        assert not by_date.exists()

        handler = _make_handler()
        handler.run(input_dir=str(by_ticker), output_dir=str(by_date))
        assert by_date.exists()

    def test_processes_multiple_action_types(self, tmp_path):
        by_ticker = tmp_path / "by_ticker"
        by_ticker.mkdir()
        aapl = by_ticker / "AAPL"
        aapl.mkdir()

        pd.DataFrame([{"ticker": "AAPL", "ex_date": "2025-03-28"}]).to_csv(aapl / "dividends.csv", index=False)
        pd.DataFrame([{"ticker": "AAPL", "effective_date": "2025-04-01", "ratio": "2:1"}]).to_csv(
            aapl / "splits.csv", index=False
        )
        pd.DataFrame([{"ticker": "AAPL", "earnings_date": "2025-05-01", "eps_estimate": 1.5}]).to_csv(
            aapl / "earnings.csv", index=False
        )

        by_date = tmp_path / "by_date"
        handler = _make_handler()
        result = handler.run(input_dir=str(by_ticker), output_dir=str(by_date))
        stats = result["statistics"]["action_types"]
        assert stats["dividends"]["total_records"] == 1
        assert stats["splits"]["total_records"] == 1
        assert stats["earnings"]["total_records"] == 1


# ─── cleanup ─────────────────────────────────────────────────────────────────


class TestGenerateDateViewsHandlerCleanup:
    def test_cleanup_does_not_raise(self):
        handler = _make_handler()
        handler.cleanup()  # Should not raise
