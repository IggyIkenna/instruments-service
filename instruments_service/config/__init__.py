"""
Configuration for Instruments Service

Unified instrument configuration. Re-exports from submodules.

Submodules:
- venue_config: TradFi tickers, instruments, exchange mappings
- api_keys: API key and secret defaults (see docs/API_KEYS_STANDARDIZED_PROCESS.md)
- data_type_config: Processing defaults
- service_config: InstrumentsServiceConfig (Pydantic), get_config, instruments_config
- instrument_definitions: DEFI_PROTOCOLS, ETF_TICKERS, SP500_TICKERS, etc.
- tradfi_exchange_mappings: Databento symbol mappings
"""

import logging

from unified_api_contracts import INSTRUMENT_TYPES_BY_VENUE
from unified_config_interface import (
    DataTypeConfig,
    ExchangeInstrumentConfig,
    VenueMapping,
)

from instruments_service.config.instrument_definitions import (
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    ETF_TICKERS,
    NASDAQ_TICKERS,
    SP500_TICKERS,
    TRADFI_INSTRUMENTS_CONFIG,
    corporate_actions_start_date,
)
from instruments_service.config.service_config import (
    InstrumentsServiceConfig,
    get_config,
    instruments_config,
)
from instruments_service.config.tradfi_exchange_mappings import (
    DATABENTO_VALID_OPTIONS_SYMBOLS,
    DATABENTO_VALID_PARENT_SYMBOLS,
    EXCHANGE_CODE_TO_NAME,
)
from instruments_service.config.venue_config import (
    InstrumentDefinition,
    TradFiInstrument,
    UnifiedInstrumentConfig,
)

_logger = logging.getLogger(__name__)


def _validate_venues_against_uac() -> None:
    """Log warnings for venues handled by instruments-service but absent from UAC INSTRUMENT_TYPES_BY_VENUE."""
    from instruments_service.config.defi_definitions import (
        DEFI_VENUE_TO_PROTOCOL as _DEFI_VENUES,
    )

    service_venues: set[str] = set(_DEFI_VENUES.keys())
    # Add TradFi venues from instrument definitions
    for inst in TRADFI_INSTRUMENTS_CONFIG:
        venue = inst.get("venue")
        if venue is not None:
            service_venues.add(venue)

    uac_venues = set(INSTRUMENT_TYPES_BY_VENUE.keys())
    unrecognized = service_venues - uac_venues
    if unrecognized:
        _logger.warning(
            "instruments-service handles %d venue(s) not in UAC INSTRUMENT_TYPES_BY_VENUE: %s",
            len(unrecognized),
            sorted(unrecognized),
        )


_validate_venues_against_uac()

__all__ = [
    "DATABENTO_VALID_OPTIONS_SYMBOLS",
    "DATABENTO_VALID_PARENT_SYMBOLS",
    "DEFI_PROTOCOLS",
    "DEFI_VENUE_TO_PROTOCOL",
    "ETF_TICKERS",
    "EXCHANGE_CODE_TO_NAME",
    "INSTRUMENT_TYPES_BY_VENUE",
    "NASDAQ_TICKERS",
    "SP500_TICKERS",
    "TRADFI_INSTRUMENTS_CONFIG",
    "DataTypeConfig",
    "ExchangeInstrumentConfig",
    "InstrumentDefinition",
    "InstrumentsServiceConfig",
    "TradFiInstrument",
    "UnifiedInstrumentConfig",
    "VenueMapping",
    "corporate_actions_start_date",
    "get_config",
    "instruments_config",
]
