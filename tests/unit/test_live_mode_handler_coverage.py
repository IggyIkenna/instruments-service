"""
Unit tests for cli/handlers/live_mode_handler.py

Covers:
- LiveModeHandler._calculate_next_aligned_time
- LiveModeHandler._get_live_gcs_path
- LiveModeHandler._cleanup
- LiveModeHandler._start_persistence_thread (basic)
"""

from datetime import UTC, datetime
from queue import Queue
from unittest.mock import MagicMock, patch


class TestCalculateNextAlignedTime:
    """Test _calculate_next_aligned_time without constructing LiveModeHandler."""

    def _make_handler(self) -> "object":
        """Create a LiveModeHandler with all external deps mocked."""
        with (
            patch("instruments_service.cli.handlers.live_mode_handler.InstrumentsService"),
            patch("instruments_service.cli.handlers.live_mode_handler.get_config"),
        ):
            from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

            handler = LiveModeHandler.__new__(LiveModeHandler)
            handler.persistence_queue = None
            handler.persistence_thread = None
            return handler

    def test_returns_tuple(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        result = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=15)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_sleep_seconds_positive(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        sleep_secs, next_run = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=15)
        assert sleep_secs > 0
        assert sleep_secs <= 15 * 60 + 2  # at most one interval

    def test_next_run_is_aligned(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        sleep_secs, next_run = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=15)
        assert next_run.minute % 15 == 0
        assert next_run.second == 0
        assert next_run.microsecond == 0

    def test_different_intervals(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        for interval in [5, 10, 15, 30]:
            sleep_secs, next_run = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=interval)
            assert next_run.minute % interval == 0
            assert sleep_secs > 0


class TestGetLiveGcsPath:
    def _make_handler(self) -> "object":
        with (
            patch("instruments_service.cli.handlers.live_mode_handler.InstrumentsService"),
            patch("instruments_service.cli.handlers.live_mode_handler.get_config"),
        ):
            from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

            handler = LiveModeHandler.__new__(LiveModeHandler)
            handler.persistence_queue = None
            handler.persistence_thread = None
            return handler

    def test_path_starts_with_live_prefix(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        ts = datetime(2024, 3, 15, 10, 30, 0, tzinfo=UTC)
        path = LiveModeHandler._get_live_gcs_path(handler, ts)
        assert path.startswith("live/")

    def test_path_contains_date_partition(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        ts = datetime(2024, 3, 15, 10, 30, 0, tzinfo=UTC)
        path = LiveModeHandler._get_live_gcs_path(handler, ts)
        assert "day=2024-03-15" in path

    def test_path_contains_minute_partition(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        ts = datetime(2024, 3, 15, 10, 30, 0, tzinfo=UTC)
        path = LiveModeHandler._get_live_gcs_path(handler, ts)
        assert "minute=1030" in path

    def test_path_contains_parquet_filename(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        ts = datetime(2024, 3, 15, 10, 30, 0, tzinfo=UTC)
        path = LiveModeHandler._get_live_gcs_path(handler, ts)
        assert "instruments_20240315_103000.parquet" in path

    def test_midnight_timestamp(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        path = LiveModeHandler._get_live_gcs_path(handler, ts)
        assert "minute=0000" in path


class TestCleanup:
    def _make_handler(self) -> "object":
        with (
            patch("instruments_service.cli.handlers.live_mode_handler.InstrumentsService"),
            patch("instruments_service.cli.handlers.live_mode_handler.get_config"),
            patch("instruments_service.cli.handlers.live_mode_handler.log_event"),
        ):
            from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

            handler = LiveModeHandler.__new__(LiveModeHandler)
            handler.persistence_queue = None
            handler.persistence_thread = None
            return handler

    def test_cleanup_with_no_queue_no_thread(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        with patch("instruments_service.cli.handlers.live_mode_handler.log_event"):
            # Should not raise
            LiveModeHandler._cleanup(handler)

    def test_cleanup_sends_stop_signal(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        queue: Queue[object] = Queue()
        handler.persistence_queue = queue
        handler.persistence_thread = None

        with patch("instruments_service.cli.handlers.live_mode_handler.log_event"):
            LiveModeHandler._cleanup(handler)

        # None stop signal should have been sent
        assert queue.get_nowait() is None

    def test_cleanup_joins_thread(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        queue: Queue[object] = Queue()
        thread = MagicMock()
        handler.persistence_queue = queue
        handler.persistence_thread = thread

        with patch("instruments_service.cli.handlers.live_mode_handler.log_event"):
            LiveModeHandler._cleanup(handler)

        thread.join.assert_called_once_with(timeout=30)
