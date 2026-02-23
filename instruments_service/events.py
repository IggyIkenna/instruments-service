"""Event logging wrapper for instruments-service.

Provides log_event(event_name, message="") compatible with UEI's
log_event(event_name, severity, details).
"""

from typing import Any

from unified_events_interface import log_event as _log_event


def log_event(event_name: str, message: str = "", **kwargs: Any) -> None:  # type: ignore[reportAny]
    """Log a lifecycle event. Message is passed as details["message"]."""
    if message:
        _log_event(event_name, details={"message": message}, **kwargs)
    else:
        _log_event(event_name, **kwargs)
