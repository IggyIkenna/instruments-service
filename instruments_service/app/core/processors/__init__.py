"""Processors extracted from InstrumentProcessingService."""

from instruments_service.app.core.processors.canonical_key_generator import (
    generate_canonical_key,
)
from instruments_service.app.core.processors.defi_processor import (
    fetch_defi_instruments,
)

__all__ = [
    "fetch_defi_instruments",
    "generate_canonical_key",
]


__all__ = [
    "fetch_defi_instruments",
    "generate_canonical_key",
]
