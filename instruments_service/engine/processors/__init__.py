"""Processors extracted from InstrumentProcessingService."""

from instruments_service.engine.processors.canonical_key_generator import (
    generate_canonical_key,
)
from instruments_service.engine.processors.defi_processor import (
    fetch_defi_instruments,
)

__all__ = [
    "fetch_defi_instruments",
    "generate_canonical_key",
]
