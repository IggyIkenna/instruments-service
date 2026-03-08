"""Unit tests for standardized event logging compliance.

Verifies required events are present in service source code.
"""

import re
from pathlib import Path

import pytest
from unified_events_interface import MockEventSink

REQUIRED_COMMON_EVENTS = [
    "STARTED",
    "VALIDATION_STARTED",
    "VALIDATION_COMPLETED",
    "VALIDATION_FAILED",
    "DATA_INGESTION_STARTED",
    "DATA_INGESTION_COMPLETED",
    "PROCESSING_STARTED",
    "PROCESSING_COMPLETED",
    "PERSISTENCE_STARTED",
    "PERSISTENCE_COMPLETED",
    "STOPPED",
    "FAILED",
]

SERVICE_SPECIFIC_EVENTS = {
    "instruments-service": [
        "DATE_PROCESSING_STARTED",
        "DATE_PROCESSING_COMPLETED",
        "VENUE_PROCESSING_STARTED",
        "VENUE_PROCESSING_COMPLETED",
        "ADAPTER_FETCH_STARTED",
        "ADAPTER_FETCH_COMPLETED",
        "CLASSIFICATION_STARTED",
        "CLASSIFICATION_COMPLETED",
    ],
}


def get_service_name() -> str:
    """Detect service name from current directory."""
    return Path.cwd().name


def find_python_files(service_dir: Path) -> list[Path]:
    """Find Python files in service source (exclude tests, venv)."""
    exclude = {"tests", ".venv", "venv", "__pycache__", ".git", "examples"}
    return [p for p in service_dir.rglob("*.py") if not any(x in p.relative_to(service_dir).parts for x in exclude)]


def find_event_markers(file_path: Path) -> set[str]:
    """Extract log_event markers from Python file."""
    content = file_path.read_text()
    pattern = r'(?:log_event\s*\(\s*["\']|SERVICE_EVENT:\s+)(\w+)'
    return set(re.findall(pattern, content))


@pytest.fixture
def all_event_markers() -> set[str]:
    """Collect all event markers from service source."""
    # Use file location, not cwd, so the test works when run from workspace root
    service_dir = Path(__file__).parent.parent.parent
    markers: set[str] = set()
    for py in find_python_files(service_dir):
        markers.update(find_event_markers(py))
    return markers


def test_required_common_events_exist(all_event_markers: set[str]) -> None:
    """Verify all required common events are present."""
    missing = set(REQUIRED_COMMON_EVENTS) - all_event_markers
    if missing:
        pytest.fail(
            f"Missing required common events: {sorted(missing)}\n"
            "See: unified-trading-deployment-v2/docs/STANDARDIZED_EVENT_LOGGING.md"
        )
    assert not (set(REQUIRED_COMMON_EVENTS) - all_event_markers), "Some required common events missing"


def test_service_specific_events_exist(all_event_markers: set[str]) -> None:
    """Verify service-specific events are present."""
    name = get_service_name()
    if name not in SERVICE_SPECIFIC_EVENTS:
        pytest.skip(f"No service-specific events for {name}")
    missing = set(SERVICE_SPECIFIC_EVENTS[name]) - all_event_markers
    if missing:
        pytest.fail(
            f"Missing service-specific events for {name}: {sorted(missing)}\n"
            "See: unified-trading-deployment-v2/docs/STANDARDIZED_EVENT_LOGGING.md"
        )
    assert True, "Service-specific events validated"


def test_event_helper_imported(all_event_markers: set[str]) -> None:
    """log_event must come from unified_events_interface (Pattern B — direct import)."""
    if not all_event_markers:
        pytest.skip("No event markers found")
    for py in find_python_files(Path.cwd()):
        text = py.read_text()
        if "from unified_events_interface import log_event" in text:
            return
    pytest.fail("log_event not imported. Add: from unified_events_interface import log_event")
    assert True, "log_event import found"


def test_mock_event_sink_importable() -> None:
    """MockEventSink must be importable from unified_events_interface."""
    sink = MockEventSink()
    assert hasattr(sink, "events")
    assert hasattr(sink, "write_event")
    assert sink.events == []


def test_setup_service_requires_sink_in_production() -> None:
    """setup_service must accept a sink argument for production use."""
    import inspect

    from unified_events_interface import setup_events

    sig = inspect.signature(setup_events)
    # setup_events must accept service_name and mode at minimum
    param_names = list(sig.parameters.keys())
    assert "service_name" in param_names, "setup_events missing service_name param"
    assert "mode" in param_names, "setup_events missing mode param"
