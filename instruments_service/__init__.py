"""
Instruments Service

Service for generating canonical instrument definitions from exchange APIs.
"""

__version__ = "0.1.0"

# Use lazy imports to avoid circular dependencies
# Import only what's needed at module level, defer heavy imports

__all__ = [
    "InstrumentProcessingService",
    "CloudInstrumentStorage",
    "InstrumentBatchProcessor",
    "InstrumentDefinition",
    "InstrumentKey",
    "Venue",
    "InstrumentType",
    "VenueMapping",
    "ExchangeInstrumentConfig",
    "DataTypeConfig",
    "UnifiedInstrumentConfig",
]


def __getattr__(name):
    """Lazy import to avoid circular dependencies."""
    if name == "InstrumentProcessingService":
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
            Venue,
            InstrumentType,
        )

        return {
            "InstrumentDefinition": InstrumentDefinition,
            "InstrumentKey": InstrumentKey,
            "Venue": Venue,
            "InstrumentType": InstrumentType,
        }[name]
    elif name in (
        "VenueMapping",
        "ExchangeInstrumentConfig",
        "DataTypeConfig",
        "UnifiedInstrumentConfig",
    ):
        from instruments_service.config import (
            VenueMapping,
            ExchangeInstrumentConfig,
            DataTypeConfig,
            UnifiedInstrumentConfig,
        )

        return {
            "VenueMapping": VenueMapping,
            "ExchangeInstrumentConfig": ExchangeInstrumentConfig,
            "DataTypeConfig": DataTypeConfig,
            "UnifiedInstrumentConfig": UnifiedInstrumentConfig,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
