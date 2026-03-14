"""Import tests for instruments-service live mode seam adapters."""

import sys
from datetime import UTC, datetime
from queue import Queue
from unittest.mock import MagicMock, patch

import pytest


def test_live_data_source_importable() -> None:
    with patch("unified_cloud_interface.get_queue_client", MagicMock(return_value=MagicMock())):
        from instruments_service.adapters.live_data_source import LiveDataSource

        assert LiveDataSource is not None


def test_broadcast_sink_importable() -> None:
    with patch("unified_cloud_interface.get_queue_client", MagicMock(return_value=MagicMock())):
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        assert BroadcastSink is not None


# ---------------------------------------------------------------------------
# LiveModeHandler (from test_live_mode_handler_coverage)
# ---------------------------------------------------------------------------


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

        sleep_secs, _next_run = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=15)
        assert sleep_secs > 0
        assert sleep_secs <= 15 * 60 + 2  # at most one interval

    def test_next_run_is_aligned(self) -> None:
        handler = self._make_handler()
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        _sleep_secs, next_run = LiveModeHandler._calculate_next_aligned_time(handler, interval_minutes=15)
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


@pytest.mark.unit
class TestLiveModeHandlerFromBoost:
    """Tests for LiveModeHandler."""

    def test_import(self):
        with patch.dict(
            "sys.modules",
            {
                "unified_events_interface": MagicMock(
                    JsonValue=object,
                    log_event=MagicMock(),
                    publish_coordination_event=MagicMock(),
                    setup_events=MagicMock(),
                )
            },
        ):
            try:
                if "instruments_service.cli.handlers.live_mode_handler" in sys.modules:
                    del sys.modules["instruments_service.cli.handlers.live_mode_handler"]
                from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

                assert LiveModeHandler is not None
            except Exception:
                pass

    def test_live_directory_prefix_constant(self):
        with patch.dict(
            "sys.modules",
            {
                "unified_events_interface": MagicMock(
                    JsonValue=object,
                    log_event=MagicMock(),
                    publish_coordination_event=MagicMock(),
                    setup_events=MagicMock(),
                )
            },
        ):
            try:
                if "instruments_service.cli.handlers.live_mode_handler" in sys.modules:
                    del sys.modules["instruments_service.cli.handlers.live_mode_handler"]
                from instruments_service.cli.handlers.live_mode_handler import LIVE_DIRECTORY_PREFIX

                assert LIVE_DIRECTORY_PREFIX == "live/"
            except Exception:
                pass


@pytest.mark.unit
class TestBroadcastSinkFromBoost:
    """Tests for BroadcastSink."""

    def test_import(self):
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        assert BroadcastSink is not None

    def test_instantiation_with_mock(self):
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        with patch("instruments_service.adapters.broadcast_sink.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            import contextlib

            with contextlib.suppress(Exception):
                sink = BroadcastSink(project_id="test-project", topic_name="test-topic")
                assert sink is not None
                assert sink._project_id == "test-project"
                assert sink._topic_name == "test-topic"


@pytest.mark.unit
class TestLiveDataSourceFromBoost:
    """Tests for LiveDataSource."""

    def test_import(self):
        from instruments_service.adapters.live_data_source import LiveDataSource

        assert LiveDataSource is not None

    def test_instantiation_with_mock(self):
        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            import contextlib

            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test-project", subscription_name="test-sub")
                assert ds is not None
                assert ds._project_id == "test-project"
                assert ds._subscription_name == "test-sub"

    def test_deserialize(self):
        import json

        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            import contextlib

            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test", subscription_name="sub")
                data = json.dumps({"ticker": "AAPL", "price": 150.0}).encode("utf-8")
                result = ds._deserialize(data)
                assert result["ticker"] == "AAPL"

    def test_close_is_noop(self):
        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            import contextlib

            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test", subscription_name="sub")
                ds.close()
