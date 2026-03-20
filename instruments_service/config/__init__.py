"""
Configuration for Instruments Service

Unified instrument configuration. Re-exports from submodules.

Submodules:
- venue_config: TradFi tickers, instruments, exchange mappings
- api_keys: API key and secret defaults (see docs/API_KEYS_STANDARDIZED_PROCESS.md)
- data_type_config: Processing defaults
- service_config: InstrumentsServiceConfig (Pydantic), get_config, instruments_config
- instrument_definitions: ETF_TICKERS, SP500_TICKERS, etc.

Static reference data (TRADFI_INSTRUMENTS_CONFIG, DEFI_VENUE_TO_PROTOCOL, DEFI_PROTOCOLS,
DATABENTO_VALID_PARENT_SYMBOLS, DATABENTO_VALID_OPTIONS_SYMBOLS, EXCHANGE_CODE_TO_NAME,
KNOWN_ETFS, SPACE_TO_DOT_SYMBOLS) is imported from UAC (SSOT).
"""

import logging

from unified_api_contracts import (
    DATABENTO_VALID_OPTIONS_SYMBOLS,
    DATABENTO_VALID_PARENT_SYMBOLS,
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    EXCHANGE_CODE_TO_NAME,
    INSTRUMENT_TYPES_BY_VENUE,
    KNOWN_ETFS,
    SPACE_TO_DOT_SYMBOLS,
    TRADFI_INSTRUMENTS_CONFIG,
)
from unified_config_interface import (
    DataTypeConfig,
    ExchangeInstrumentConfig,
    VenueMapping,
)

from instruments_service.config.instrument_definitions import (
    ETF_TICKERS,
    NASDAQ_TICKERS,
    SP500_TICKERS,
    corporate_actions_start_date,
)
from instruments_service.config.service_config import (
    InstrumentsServiceConfig,
    get_config,
    instruments_config,
)
from instruments_service.config.venue_config import (
    InstrumentDefinition,
    TradFiInstrument,
    UnifiedInstrumentConfig,
)

_logger = logging.getLogger(__name__)


def _validate_venues_against_uac() -> None:
    """Log warnings for venues handled by instruments-service but absent from UAC INSTRUMENT_TYPES_BY_VENUE."""
    service_venues: set[str] = set(DEFI_VENUE_TO_PROTOCOL.keys())
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
    "KNOWN_ETFS",
    "NASDAQ_TICKERS",
    "SP500_TICKERS",
    "SPACE_TO_DOT_SYMBOLS",
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
