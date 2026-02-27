"""
Unit tests for events module - updated for unified-trading-services pattern.
"""

from unified_events_interface import MockEventSink
from unified_trading_services import log_event, setup_service


def test_setup_service_uses_mock_sink():
    """Test that setup_service works with MockEventSink for testing."""
    sink = MockEventSink()
    setup_service(service_name="instruments-service", mode="test", sink=sink)
    log_event("STARTED")
    assert any(name == "STARTED" for name, _ in sink.events)


def test_lifecycle_events_logged():
    """Test that standard lifecycle events can be logged."""
    sink = MockEventSink()
    setup_service(service_name="instruments-service", mode="test", sink=sink)

    # Log standard lifecycle events
    log_event("STARTED")
    log_event("VALIDATION_STARTED")
    log_event("VALIDATION_COMPLETED")
    log_event("DATA_INGESTION_STARTED")
    log_event("DATA_INGESTION_COMPLETED")
    log_event("PROCESSING_STARTED")
    log_event("PROCESSING_COMPLETED")
    log_event("UPLOAD_STARTED")
    log_event("UPLOAD_COMPLETED")
    log_event("STOPPED")

    event_names = [name for name, _ in sink.events]
    assert "STARTED" in event_names
    assert "VALIDATION_STARTED" in event_names
    assert "VALIDATION_COMPLETED" in event_names
    assert "DATA_INGESTION_STARTED" in event_names
    assert "DATA_INGESTION_COMPLETED" in event_names
    assert "PROCESSING_STARTED" in event_names
    assert "PROCESSING_COMPLETED" in event_names
    assert "UPLOAD_STARTED" in event_names
    assert "UPLOAD_COMPLETED" in event_names
    assert "STOPPED" in event_names


def test_service_specific_events_logged():
    """Test that service-specific events can be logged."""
    sink = MockEventSink()
    setup_service(service_name="instruments-service", mode="test", sink=sink)

    # Log service-specific events
    log_event("DATE_PROCESSING_STARTED")
    log_event("DATE_PROCESSING_COMPLETED")
    log_event("VENUE_PROCESSING_STARTED")
    log_event("VENUE_PROCESSING_COMPLETED")
    log_event("ADAPTER_FETCH_STARTED")
    log_event("ADAPTER_FETCH_COMPLETED")
    log_event("CLASSIFICATION_STARTED")
    log_event("CLASSIFICATION_COMPLETED")

    event_names = [name for name, _ in sink.events]
    assert "DATE_PROCESSING_STARTED" in event_names
    assert "DATE_PROCESSING_COMPLETED" in event_names
    assert "VENUE_PROCESSING_STARTED" in event_names
    assert "VENUE_PROCESSING_COMPLETED" in event_names
    assert "ADAPTER_FETCH_STARTED" in event_names
    assert "ADAPTER_FETCH_COMPLETED" in event_names
    assert "CLASSIFICATION_STARTED" in event_names
    assert "CLASSIFICATION_COMPLETED" in event_names
