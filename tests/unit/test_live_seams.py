"""Import tests for instruments-service live mode seam adapters."""

from unittest.mock import MagicMock, patch


def test_live_data_source_importable() -> None:
    with patch("unified_cloud_interface.get_queue_client", MagicMock(return_value=MagicMock())):
        from instruments_service.adapters.live_data_source import LiveDataSource

        assert LiveDataSource is not None


def test_broadcast_sink_importable() -> None:
    with patch("unified_cloud_interface.get_queue_client", MagicMock(return_value=MagicMock())):
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        assert BroadcastSink is not None
