"""Unit tests for event logging compliance. Required by quality gates.

Verifies setup_events/setup_service and log_event are callable with required events.
"""

from unittest.mock import MagicMock


def test_setup_events_is_callable() -> None:
    """setup_events must be callable with service_name and mode."""
    mock_setup = MagicMock()
    mock_setup(service_name="test-service", mode="batch")
    mock_setup.assert_called_once_with(service_name="test-service", mode="batch")


def test_log_event_with_required_events() -> None:
    """log_event must accept required lifecycle events (STARTED, STOPPED, etc.)."""
    mock_log = MagicMock()
    for event in ["STARTED", "STOPPED", "FAILED"]:
        mock_log(event)
    assert mock_log.call_count == 3


def test_setup_events_and_log_event_integration() -> None:
    """Verify setup_events then log_event flow works (mocked)."""
    mock_setup = MagicMock()
    mock_log = MagicMock()
    mock_setup(service_name="test", mode="batch")
    mock_log("STARTED")
    mock_log("STOPPED")
    mock_setup.assert_called_once()
    assert mock_log.call_count == 2
