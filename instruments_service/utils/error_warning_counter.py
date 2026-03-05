"""
Error and Warning Counter

A logging handler that counts errors and warnings.
"""

import logging


class ErrorWarningCounter(logging.Handler):
    """Simple error/warning counter for logging."""

    def __init__(self):
        super().__init__()
        self.error_count = 0
        self.warning_count = 0

    def emit(self, record: logging.LogRecord) -> None:
        """Handle log record and count errors/warnings."""
        if record.levelno >= logging.ERROR:
            self.error_count += 1
        elif record.levelno >= logging.WARNING:
            self.warning_count += 1

    def reset(self) -> None:
        """Reset counters."""
        self.error_count = 0
        self.warning_count = 0
