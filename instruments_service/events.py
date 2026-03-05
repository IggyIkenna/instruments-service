"""Event logging wrapper for instruments-service.

Provides log_event(event_name, message="") compatible with UEI's
log_event(event_name, severity, details).
"""

from unified_events_interface import JSONDict
from unified_events_interface import log_event as _log_event


def log_event(
    event_name: str,
    message: str = "",
    severity: str = "INFO",
    details: JSONDict | None = None,
    client_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    """Log a lifecycle event. Message is passed as details["message"]."""
    if message:
        merged_details: JSONDict = {"message": message}
        if details:
            merged_details.update(details)
        _log_event(
            event_name,
            severity=severity,
            details=merged_details,
            client_id=client_id,
            correlation_id=correlation_id,
        )
    else:
        _log_event(
            event_name,
            severity=severity,
            details=details,
            client_id=client_id,
            correlation_id=correlation_id,
        )
