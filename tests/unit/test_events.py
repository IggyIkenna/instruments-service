"""
Unit tests for events module (Task 110 coverage).
"""

from unittest.mock import patch

from instruments_service.events import log_event


class TestLogEvent:
    """Tests for log_event wrapper."""

    def test_log_event_with_message(self):
        """Test log_event with message passes details."""
        with patch("instruments_service.events._log_event") as mock:
            log_event("STARTED", "Processing instruments")
            mock.assert_called_once_with("STARTED", details={"message": "Processing instruments"})

    def test_log_event_without_message(self):
        """Test log_event without message calls with no details."""
        with patch("instruments_service.events._log_event") as mock:
            log_event("COMPLETED")
            mock.assert_called_once_with("COMPLETED")

    def test_log_event_with_kwargs(self):
        """Test log_event passes through kwargs."""
        with patch("instruments_service.events._log_event") as mock:
            log_event("FAILED", "Error occurred", severity="error")
            mock.assert_called_once()
            call_kwargs = mock.call_args[1]
            assert call_kwargs.get("details") == {"message": "Error occurred"}
            assert call_kwargs.get("severity") == "error"
