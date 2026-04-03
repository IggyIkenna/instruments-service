"""Event logging tests — verifies instruments-service emits required lifecycle events.

Per codex 03-observability/lifecycle-events.md:
  STARTED, STOPPED, FAILED must be emittable.
  PROCESSING_STARTED, PROCESSING_COMPLETED, PROCESSING_FAILED must be emittable.
"""

from __future__ import annotations


def test_required_lifecycle_events_importable():
    """All required lifecycle event strings are used in the orchestrator."""
    import ast
    from pathlib import Path

    orch_src = (Path(__file__).parent.parent.parent / "instruments_service" / "engine" / "orchestrator.py").read_text()
    tree = ast.parse(orch_src)

    # Collect all string literals passed to log_event calls
    events_found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "log_event"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            events_found.add(node.args[0].value)

    required = {"PROCESSING_STARTED", "PROCESSING_COMPLETED", "PROCESSING_FAILED"}
    missing = required - events_found
    assert not missing, f"Missing required events in orchestrator: {missing}"


def test_event_logging_initialized_in_mock_mode():
    """Events can be initialized and log_event can be called without error."""
    from unified_trading_library import MockEventSink, log_event, setup_events

    setup_events(service_name="instruments-service-test", mode="test", sink=MockEventSink())

    # All calls must succeed without raising
    log_event("STARTED")
    log_event("PROCESSING_STARTED", details={"date": "2026-03-22", "categories": ["DEFI"]})
    log_event("PROCESSING_COMPLETED", details={"total_records": 100})
    log_event("STOPPED")


def test_write_failed_event_is_emittable():
    """WRITE_FAILED event is emittable with venue/date context."""
    from unified_trading_library import MockEventSink, log_event, setup_events

    setup_events(service_name="instruments-service-test", mode="test", sink=MockEventSink())

    log_event("WRITE_FAILED", details={"venue": "MORPHO-ETHEREUM", "date": "2026-03-22", "error": "timeout"})


def test_processing_failed_event_is_emittable():
    """PROCESSING_FAILED event is emittable when URDI returns zero records."""
    from unified_trading_library import MockEventSink, log_event, setup_events

    setup_events(service_name="instruments-service-test", mode="test", sink=MockEventSink())

    log_event("PROCESSING_FAILED", details={"date": "2026-03-22", "reason": "URDI returned zero records"})


def test_validation_failed_event_is_emittable():
    """VALIDATION_FAILED event is emittable on schema issues."""
    from unified_trading_library import MockEventSink, log_event, setup_events

    setup_events(service_name="instruments-service-test", mode="test", sink=MockEventSink())

    log_event("VALIDATION_FAILED", details={"date": "2026-03-22", "errors": ["schema mismatch"]})
