"""Configuration for instruments-service.

Exports the 4-field service config only. Reference data (venue names,
instruments lists, ticker universes) comes from UAC.
"""

from instruments_service.config.service_config import (
    InstrumentsServiceConfig,
    get_config,
    instruments_config,
)

__all__ = [
    "InstrumentsServiceConfig",
    "get_config",
    "instruments_config",
]
