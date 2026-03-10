"""
Unit tests for CorporateActionsUpdateHandler.

Tests _load_ticker_registry, _get_outdated_tickers, and run()
using tmp_path fixtures for YAML registry files.
"""

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from instruments_service.cli.handlers.corporate_actions_update_handler import (
    CorporateActionsUpdateHandler,
)


def _make_handler(tmp_path: Path) -> CorporateActionsUpdateHandler:
    """Build a handler with mocked sub-handlers."""
    with (
        patch(
            "instruments_service.cli.handlers.corporate_actions_update_handler.CorporateActionsBackfillHandler"
        ) as mock_backfill,
        patch(
            "instruments_service.cli.handlers.corporate_actions_update_handler.GenerateDateViewsHandler"
        ) as mock_views,
    ):
        mock_backfill.return_value = MagicMock()
        mock_views.return_value = MagicMock()
        handler = CorporateActionsUpdateHandler(config={"mode": "corporate_actions_update"})
    return handler


# ─── _load_ticker_registry ───────────────────────────────────────────────────


class TestLoadTickerRegistry:
    def test_returns_default_when_registry_missing(self, tmp_path):
        handler = _make_handler(tmp_path)
        result = handler._load_ticker_registry(tmp_path / "nonexistent_dir")
        assert "version" in result
        assert "tickers" in result
        assert result["tickers"] == {}

    def test_loads_existing_registry(self, tmp_path):
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        registry = {
            "version": "1.0",
            "tickers": {
                "AAPL": {"last_download_date": "2025-03-01", "status": "success"},
                "MSFT": {"last_download_date": "2025-03-01", "status": "success"},
            },
        }
        (metadata_dir / "ticker_registry.yaml").write_text(yaml.dump(registry), encoding="utf-8")

        handler = _make_handler(tmp_path)
        result = handler._load_ticker_registry(metadata_dir)
        assert result["version"] == "1.0"
        tickers = result.get("tickers")
        assert isinstance(tickers, dict)
        assert "AAPL" in tickers

    def test_empty_registry_returns_empty_dict(self, tmp_path):
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "ticker_registry.yaml").write_text("", encoding="utf-8")

        handler = _make_handler(tmp_path)
        result = handler._load_ticker_registry(metadata_dir)
        assert result == {}


# ─── _get_outdated_tickers ───────────────────────────────────────────────────


class TestGetOutdatedTickers:
    def test_empty_registry_returns_empty_list(self, tmp_path):
        handler = _make_handler(tmp_path)
        result = handler._get_outdated_tickers({}, days_threshold=7)
        assert result == []

    def test_never_fetched_ticker_is_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        registry = {"tickers": {"AAPL": {"last_download_date": None}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "AAPL" in result

    def test_ticker_missing_last_date_is_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        registry = {"tickers": {"MSFT": {}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "MSFT" in result

    def test_recent_ticker_not_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        registry = {"tickers": {"AAPL": {"last_download_date": yesterday, "status": "success"}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "AAPL" not in result

    def test_old_ticker_is_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        old_date = (date.today() - timedelta(days=10)).isoformat()
        registry = {"tickers": {"AAPL": {"last_download_date": old_date, "status": "success"}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "AAPL" in result

    def test_failed_ticker_retried_after_one_day(self, tmp_path):
        handler = _make_handler(tmp_path)
        yesterday = (date.today() - timedelta(days=2)).isoformat()
        registry = {"tickers": {"GOOG": {"last_download_date": yesterday, "status": "error"}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "GOOG" in result

    def test_invalid_date_format_marks_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        registry = {"tickers": {"TSLA": {"last_download_date": "not-a-date"}}}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "TSLA" in result

    def test_mixed_tickers_returns_only_outdated(self, tmp_path):
        handler = _make_handler(tmp_path)
        fresh_date = (date.today() - timedelta(days=1)).isoformat()
        stale_date = (date.today() - timedelta(days=30)).isoformat()
        registry = {
            "tickers": {
                "AAPL": {"last_download_date": fresh_date, "status": "success"},
                "MSFT": {"last_download_date": stale_date, "status": "success"},
            }
        }
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "AAPL" not in result
        assert "MSFT" in result

    def test_non_dict_tickers_returns_empty(self, tmp_path):
        handler = _make_handler(tmp_path)
        registry = {"tickers": "not_a_dict"}
        result = handler._get_outdated_tickers(registry, days_threshold=7)
        assert result == []

    def test_custom_threshold_respected(self, tmp_path):
        handler = _make_handler(tmp_path)
        three_days_ago = (date.today() - timedelta(days=3)).isoformat()
        registry = {"tickers": {"NVDA": {"last_download_date": three_days_ago, "status": "success"}}}

        # threshold=7: not outdated (3 < 7)
        result_7 = handler._get_outdated_tickers(registry, days_threshold=7)
        assert "NVDA" not in result_7

        # threshold=2: outdated (3 >= 2)
        result_2 = handler._get_outdated_tickers(registry, days_threshold=2)
        assert "NVDA" in result_2


# ─── run() ───────────────────────────────────────────────────────────────────


class TestCorporateActionsUpdateHandlerRun:
    def test_run_returns_success_when_all_tickers_current(self, tmp_path):
        handler = _make_handler(tmp_path)
        fresh_date = (date.today() - timedelta(days=1)).isoformat()
        registry = {
            "version": "1.0",
            "tickers": {"AAPL": {"last_download_date": fresh_date, "status": "success"}},
        }
        with patch.object(handler, "_load_ticker_registry", return_value=registry):
            result = handler.run(days_threshold=7)
        assert result["status"] == "success"
        stats = result["statistics"]
        assert isinstance(stats, dict)
        assert stats["tickers_outdated"] == 0

    def test_run_calls_backfill_when_outdated_tickers_found(self, tmp_path):
        handler = _make_handler(tmp_path)
        stale_date = (date.today() - timedelta(days=30)).isoformat()
        registry = {
            "version": "1.0",
            "tickers": {"AAPL": {"last_download_date": stale_date, "status": "success"}},
        }
        mock_backfill_result = {
            "status": "success",
            "statistics": {
                "tickers_successful": 1,
                "tickers_failed": 0,
                "total_dividends": 2,
                "total_splits": 0,
                "total_earnings": 1,
            },
        }
        handler.backfill_handler.run.return_value = mock_backfill_result
        handler.date_views_handler.run.return_value = {"status": "success", "statistics": {}}
        with patch.object(handler, "_load_ticker_registry", return_value=registry):
            result = handler.run(days_threshold=7, regenerate_date_views=True)
        handler.backfill_handler.run.assert_called_once()
        assert result["status"] == "success"
        stats = result["statistics"]
        assert isinstance(stats, dict)
        assert stats["tickers_outdated"] == 1

    def test_run_skips_date_views_when_disabled(self, tmp_path):
        handler = _make_handler(tmp_path)
        stale_date = (date.today() - timedelta(days=30)).isoformat()
        registry = {
            "version": "1.0",
            "tickers": {"AAPL": {"last_download_date": stale_date, "status": "success"}},
        }
        mock_backfill_result = {
            "status": "success",
            "statistics": {
                "tickers_successful": 1,
                "tickers_failed": 0,
                "total_dividends": 0,
                "total_splits": 0,
                "total_earnings": 0,
            },
        }
        handler.backfill_handler.run.return_value = mock_backfill_result
        with patch.object(handler, "_load_ticker_registry", return_value=registry):
            result = handler.run(days_threshold=7, regenerate_date_views=False)
        handler.date_views_handler.run.assert_not_called()
        assert result["status"] == "success"

    def test_run_empty_registry_returns_success(self, tmp_path):
        handler = _make_handler(tmp_path)
        empty_registry = {"version": "1.0", "tickers": {}}
        with patch.object(handler, "_load_ticker_registry", return_value=empty_registry):
            result = handler.run(days_threshold=7)
        assert result["status"] == "success"
        stats = result["statistics"]
        assert isinstance(stats, dict)
        assert stats["tickers_checked"] == 0

    def test_cleanup_delegates_to_subhandlers(self, tmp_path):
        handler = _make_handler(tmp_path)
        handler.cleanup()
        handler.backfill_handler.cleanup.assert_called_once()
        handler.date_views_handler.cleanup.assert_called_once()
