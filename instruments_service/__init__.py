"""
Instruments Service

Service for generating canonical instrument definitions from exchange APIs.

Rebuild: Feb 19, 2026 - Pull latest UCS base image with bucket configuration fix.
"""

from typing import Any, cast

__version__ = "0.1.0"

# Use lazy imports to avoid circular dependencies
# Import only what's needed at module level, defer heavy imports
# Lazy-loaded attrs (InstrumentProcessingService, etc.) are available via __getattr__
# Only non-lazy exports in __all__ to satisfy pyright reportUnsupportedDunderAll
__all__: list[str] = ["__version__"]


def __getattr__(name: str) -> Any:  # type: ignore[reportAny]
    """Lazy import to avoid circular dependencies."""
    if name == "cli":
        import instruments_service.cli  # noqa: F401

        return instruments_service.cli
    elif name == "InstrumentProcessingService":
        from instruments_service.app.core.instrument_processing_service import (
            InstrumentProcessingService,
        )

        return InstrumentProcessingService
    elif name == "CloudInstrumentStorage":
        from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage

        return CloudInstrumentStorage
    elif name == "InstrumentBatchProcessor":
        from instruments_service.app.core.batch_processor import InstrumentBatchProcessor

        return InstrumentBatchProcessor
    elif name in ("InstrumentDefinition", "InstrumentKey", "Venue", "InstrumentType"):
        from instruments_service.models import (
            InstrumentDefinition,
            InstrumentKey,
            InstrumentType,
            Venue,
        )

        return cast(
            Any,
            {
                "InstrumentDefinition": InstrumentDefinition,
                "InstrumentKey": InstrumentKey,
                "Venue": Venue,
                "InstrumentType": InstrumentType,
            }[name],
        )
    elif name in (
        "VenueMapping",
        "ExchangeInstrumentConfig",
        "DataTypeConfig",
        "UnifiedInstrumentConfig",
    ):
        from instruments_service.config import (
            DataTypeConfig,
            ExchangeInstrumentConfig,
            UnifiedInstrumentConfig,
            VenueMapping,
        )

        return cast(
            Any,
            {
                "VenueMapping": VenueMapping,
                "ExchangeInstrumentConfig": ExchangeInstrumentConfig,
                "DataTypeConfig": DataTypeConfig,
                "UnifiedInstrumentConfig": UnifiedInstrumentConfig,
            }[name],
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
