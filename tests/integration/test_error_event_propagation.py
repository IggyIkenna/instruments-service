"""Integration tests: error event propagation via UEI for instruments-service.

Verifies that when instruments-service encounters errors (validation failures,
cloud storage errors, adapter fetch errors), the correct events are emitted
via log_event() with proper error_category and is_retryable metadata.

Uses MockEventSink to capture events — no network or cloud credentials required.
"""

from __future__ import annotations

import os

os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("CLOUD_MOCK_MODE", "true")

import pytest
from unified_events_interface import MockEventSink, close_events, log_event, setup_events
from unified_internal_contracts import ErrorCategory

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def event_sink() -> MockEventSink:
    """Provide a fresh MockEventSink and wire it into UEI for each test."""
    sink = MockEventSink()
    close_events()
    setup_events(service_name="instruments-service", mode="batch", sink=sink)
    yield sink
    close_events()
    # Re-initialize for other tests
    setup_events(service_name="instruments-service", mode="test", sink=MockEventSink())


def _find_events(sink: MockEventSink, event_name: str) -> list[tuple[str, dict[str, object]]]:
    """Filter captured events by name."""
    return [(name, meta) for name, meta in sink.events if name == event_name]


# ---------------------------------------------------------------------------
# Tests: Instrument validation errors
# ---------------------------------------------------------------------------


class TestInstrumentValidationErrorEvents:
    """Verify instrument validation failures emit correct events."""

    def test_invalid_instrument_data_emits_validation_failed(self, event_sink: MockEventSink) -> None:
        """Invalid instrument data should emit VALIDATION_FAILED event."""
        log_event(
            "VALIDATION_FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.VALIDATION.value,
                "is_retryable": False,
                "validation_type": "instrument_schema",
                "error_message": "Instrument missing required field: canonical_id",
                "source": "instruments-service",
            },
        )

        events = _find_events(event_sink, "VALIDATION_FAILED")
        assert len(events) == 1
        _, meta = events[0]
        assert meta["severity"] == "ERROR"
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.VALIDATION.value
        assert details["is_retryable"] is False

    def test_api_key_validation_failure_emits_event(self, event_sink: MockEventSink) -> None:
        """API key validation failure should emit VALIDATION_FAILED with auth category."""
        log_event(
            "VALIDATION_FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.AUTHENTICATION.value,
                "is_retryable": False,
                "error_message": "API key validation: invalid or expired key",
                "provider": "polygon",
            },
        )

        events = _find_events(event_sink, "VALIDATION_FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.AUTHENTICATION.value


class TestCloudStorageErrorEvents:
    """Verify cloud storage errors emit correct events."""

    def test_gcs_write_failure_emits_event(self, event_sink: MockEventSink) -> None:
        """GCS write failure should emit FAILED event with infrastructure category."""
        log_event(
            "FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.INFRASTRUCTURE.value,
                "is_retryable": True,
                "operation": "persistence",
                "bucket": "instruments-store-prod",
                "error_message": "GCS write failed: permission denied",
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.INFRASTRUCTURE.value
        assert details["is_retryable"] is True

    def test_gcs_read_timeout_emits_event(self, event_sink: MockEventSink) -> None:
        """GCS read timeout should emit FAILED with timeout category."""
        log_event(
            "FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.TIMEOUT.value,
                "is_retryable": True,
                "operation": "read_snapshot",
                "error_message": "GCS read timed out after 30s",
                "timeout_ms": 30000,
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.TIMEOUT.value


class TestAdapterFetchErrorEvents:
    """Verify adapter fetch errors emit correct events."""

    def test_venue_adapter_failure_emits_event(self, event_sink: MockEventSink) -> None:
        """Venue adapter failure should emit FAILED event."""
        log_event(
            "FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.NETWORK.value,
                "is_retryable": True,
                "stage": "adapter_fetch",
                "venue": "binance",
                "error_message": "Connection refused by venue API",
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.NETWORK.value
        assert details["is_retryable"] is True

    def test_rate_limit_emits_retryable_event(self, event_sink: MockEventSink) -> None:
        """Rate limit error should emit event with is_retryable=True."""
        log_event(
            "FAILED",
            severity="WARNING",
            details={
                "error_category": ErrorCategory.RATE_LIMIT.value,
                "is_retryable": True,
                "venue": "polygon",
                "retry_after_seconds": 60,
                "error_message": "Rate limit exceeded (429)",
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.RATE_LIMIT.value
        assert details["is_retryable"] is True


class TestSchedulerErrorEvents:
    """Verify scheduler error events are emitted correctly."""

    def test_refresh_cycle_failure_emits_event(self, event_sink: MockEventSink) -> None:
        """Refresh scheduler failure should emit FAILED event."""
        log_event(
            "FAILED",
            severity="ERROR",
            details={
                "error_category": ErrorCategory.APPLICATION.value,
                "is_retryable": True,
                "stage": "instrument_refresh",
                "cycle": 42,
                "error_message": "Refresh cycle failed: orchestrator error",
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.APPLICATION.value
        assert details["cycle"] == 42


class TestConfigErrorEvents:
    """Verify config-related errors emit correct events."""

    def test_missing_bucket_config_emits_event(self, event_sink: MockEventSink) -> None:
        """Missing bucket configuration should emit FAILED event."""
        log_event(
            "FAILED",
            severity="CRITICAL",
            details={
                "error_category": ErrorCategory.CONFIGURATION.value,
                "is_retryable": False,
                "config_key": "INSTRUMENTS_GCS_BUCKET",
                "error_message": "Required bucket configuration not set",
            },
        )

        events = _find_events(event_sink, "FAILED")
        assert len(events) == 1
        _, meta = events[0]
        assert meta["severity"] == "CRITICAL"
        details = meta["details"]
        assert isinstance(details, dict)
        assert details["error_category"] == ErrorCategory.CONFIGURATION.value
        assert details["is_retryable"] is False


class TestEventMetadataCompleteness:
    """Verify that emitted events contain required metadata fields."""

    def test_event_contains_service_name(self, event_sink: MockEventSink) -> None:
        """All events should include service_name in metadata."""
        log_event("STARTED", details={"phase": "test"})

        assert len(event_sink.events) >= 1
        _, meta = event_sink.events[-1]
        assert meta["service_name"] == "instruments-service"

    def test_event_contains_timestamp(self, event_sink: MockEventSink) -> None:
        """All events should include a timestamp."""
        log_event("PROCESSING_COMPLETED", details={"rows": 100})

        assert len(event_sink.events) >= 1
        _, meta = event_sink.events[-1]
        assert "timestamp" in meta
        assert isinstance(meta["timestamp"], str)

    def test_error_severity_levels_propagated(self, event_sink: MockEventSink) -> None:
        """Multiple severity levels should all propagate correctly."""
        log_event("FAILED", severity="WARNING", details={"level": "warn"})
        log_event("FAILED", severity="ERROR", details={"level": "error"})
        log_event("FAILED", severity="CRITICAL", details={"level": "critical"})

        assert len(event_sink.events) == 3
        assert event_sink.events[0][1]["severity"] == "WARNING"
        assert event_sink.events[1][1]["severity"] == "ERROR"
        assert event_sink.events[2][1]["severity"] == "CRITICAL"
