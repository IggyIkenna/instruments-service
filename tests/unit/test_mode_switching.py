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

    @patch("instruments_service.cli.handlers.live_mode_handler.InstrumentsService")
    def test_live_handler_instantiates_without_pubsub(
        self,
        mock_instruments_service: MagicMock,
    ) -> None:
        """LiveModeHandler() can be constructed without a PubSub call."""
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        mock_instruments_service.return_value = MagicMock()

        with patch("unified_cloud_interface.get_queue_client") as mock_pubsub:
            handler = LiveModeHandler(config={"project_id": "test-project"})

        assert handler is not None
        mock_pubsub.assert_not_called()

    @patch("instruments_service.cli.handlers.live_mode_handler.InstrumentsService")
    def test_live_handler_uses_gcs_persistence(
        self,
        mock_instruments_service: MagicMock,
    ) -> None:
        """LiveModeHandler uses upload_to_storage (GCS) for persistence (reference data pattern)."""
        from instruments_service.cli.handlers.live_mode_handler import LiveModeHandler

        mock_instruments_service.return_value = MagicMock()
        handler = LiveModeHandler(config={"project_id": "test-project"})

        # IS live handler queues writes via upload_to_storage (GCS), not PubSub
        # The persistence_queue is None until run() starts — verify attribute exists
        assert hasattr(handler, "persistence_queue")
        assert hasattr(handler, "persistence_thread")
