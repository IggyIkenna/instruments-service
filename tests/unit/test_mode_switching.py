"""Tests for mode-based transport switching — instruments-service.

batch mode → upload_to_storage (GCS) used for persistence
live mode  → LiveModeHandler initialises and uses GCSEventSink + upload_to_storage
             (IS uses GCS pull pattern for reference data, not PubSub streaming)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestBatchModeTransport:
    """InstrumentHandler (batch) uses GCS-based storage client."""

    @patch("instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage")
    @patch("instruments_service.cli.handlers.instrument_handler.InstrumentsService")
    @patch("instruments_service.cli.handlers.instrument_handler.VenueMapping")
    def test_batch_handler_instantiates_gcs_storage(
        self,
        mock_venue_mapping: MagicMock,
        mock_instruments_service: MagicMock,
        mock_cloud_storage: MagicMock,
    ) -> None:
        """batch mode handler creates CloudInstrumentStorage (GCS-backed)."""
        from instruments_service.cli.handlers.instrument_handler import InstrumentHandler

        with patch("instruments_service.cli.handlers.instrument_handler.get_config") as mock_cfg:
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            mock_instruments_service.return_value = MagicMock()
            mock_cloud_storage.return_value = MagicMock()

            handler = InstrumentHandler(config={"project_id": "test-project"})

        assert handler is not None
        mock_cloud_storage.assert_called_once()

    @patch("instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage")
    @patch("instruments_service.cli.handlers.instrument_handler.InstrumentsService")
    @patch("instruments_service.cli.handlers.instrument_handler.VenueMapping")
    def test_batch_handler_does_not_use_pubsub(
        self,
        mock_venue_mapping: MagicMock,
        mock_instruments_service: MagicMock,
        mock_cloud_storage: MagicMock,
    ) -> None:
        """batch mode handler does not instantiate any PubSub client at construction."""
        from instruments_service.cli.handlers.instrument_handler import InstrumentHandler

        with patch("instruments_service.cli.handlers.instrument_handler.get_config") as mock_cfg:
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            mock_instruments_service.return_value = MagicMock()
            mock_cloud_storage.return_value = MagicMock()

            with patch("unified_cloud_interface.get_queue_client") as mock_pubsub:
                InstrumentHandler(config={"project_id": "test-project"})

        mock_pubsub.assert_not_called()


class TestLiveModeTransport:
    """LiveModeHandler uses GCS persistence thread (IS uses GCS pull pattern for reference data)."""

    def test_live_handler_file_exists(self) -> None:
        """live_mode_handler.py is present in instruments_service/cli/handlers/."""
        import os

        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "instruments_service",
            "cli",
            "handlers",
            "live_mode_handler.py",
        )
        assert os.path.exists(path), "live_mode_handler.py must exist"

    def test_live_handler_source_uses_upload_to_storage(self) -> None:
        """live_mode_handler.py references upload_to_storage (GCS) not get_queue_client (PubSub)."""
        import os

        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "instruments_service",
            "cli",
            "handlers",
            "live_mode_handler.py",
        )
        with open(path) as f:
            source = f.read()
        assert "upload_to_storage" in source, "live_mode_handler must use upload_to_storage (GCS)"
        assert "get_queue_client" not in source, "live_mode_handler must not use PubSub queue client"

    def test_live_handler_source_has_persistence_queue(self) -> None:
        """live_mode_handler.py defines persistence_queue (async GCS write pattern)."""
        import os

        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "instruments_service",
            "cli",
            "handlers",
            "live_mode_handler.py",
        )
        with open(path) as f:
            source = f.read()
        assert "persistence_queue" in source, "live_mode_handler must define persistence_queue"
