"""Unit tests for standardized event logging compliance.

Verifies required events are present in service source code.
"""

import re
from pathlib import Path

import pytest

REQUIRED_COMMON_EVENTS = [
    "STARTED",
    "VALIDATION_STARTED",
    "VALIDATION_COMPLETED",
    "VALIDATION_FAILED",
    "DATA_INGESTION_STARTED",
    "DATA_INGESTION_COMPLETED",
    "PROCESSING_STARTED",
    "PROCESSING_COMPLETED",
    "UPLOAD_STARTED",
    "UPLOAD_COMPLETED",
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
    found = []
    for py in service_dir.rglob("*.py"):
        parts = py.relative_to(service_dir).parts
        if any(p in exclude for p in parts):
            continue
        found.append(py)
    return found


def find_event_markers(file_path: Path) -> set[str]:
    """Extract SERVICE_EVENT markers from Python file."""
    content = file_path.read_text()
    # Match log_event("EVENT") or log_event('EVENT') or SERVICE_EVENT: EVENT
    pattern = r'(?:log_event\s*\(\s*["\']|SERVICE_EVENT:\s+)(\w+)'
    return set(re.findall(pattern, content))


@pytest.fixture
def all_event_markers() -> set[str]:
    """Collect all event markers from service source."""
    service_dir = Path.cwd()
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


def test_service_specific_events_exist(all_event_markers: set[str]) -> None:
    """Verify service-specific events are present."""
    name = get_service_name()
    if name not in SERVICE_SPECIFIC_EVENTS:
        pytest.skip(f"No service-specific events for {name}")
    required = set(SERVICE_SPECIFIC_EVENTS[name])
    missing = required - all_event_markers
    if missing:
        pytest.fail(
            f"Missing service-specific events for {name}: {sorted(missing)}\n"
            "See: unified-trading-deployment-v2/docs/STANDARDIZED_EVENT_LOGGING.md"
        )


def test_event_helper_imported(all_event_markers: set[str]) -> None:
    """Verify log_event is imported when events are used."""
    if not all_event_markers:
        pytest.skip("No event markers found")
    for py in find_python_files(Path.cwd()):
        text = py.read_text()
        if "from unified_events_interface import log_event" in text:
            return
        if "from instruments_service.events import log_event" in text:
            return
        if "from unified_cloud_services.observability import log_event" in text:
            return
    pytest.fail(
        "log_event not imported. Add: from unified_events_interface import log_event "
        "or from instruments_service.events import log_event"
    )
