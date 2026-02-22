"""
Configuration for Instruments Service

Unified instrument configuration. Re-exports from submodules for backward compatibility.

Submodules:
- venue_config: TradFi tickers, instruments, exchange mappings
- api_keys: API key and secret defaults
- data_type_config: Processing defaults
- service_config: InstrumentsServiceConfig (Pydantic), get_config, instruments_config
"""

from unified_config_interface import (
    DataTypeConfig,
    ExchangeInstrumentConfig,
    VenueMapping,
)

from instruments_service.config.instrument_definitions import (
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    SP500_TICKERS,
    TRADFI_INSTRUMENTS_CONFIG,
)
from instruments_service.config.service_config import (
    InstrumentsServiceConfig,
    get_config,
    instruments_config,
)
from instruments_service.config.venue_config import (
    DATABENTO_VALID_OPTIONS_SYMBOLS,
    DATABENTO_VALID_PARENT_SYMBOLS,
    ETF_TICKERS,
    EXCHANGE_CODE_TO_NAME,
    NASDAQ_TICKERS,
    InstrumentDefinition,
    TradFiInstrument,
    UnifiedInstrumentConfig,
    corporate_actions_start_date,
)

# Alias for backward compatibility (used by io/writer.py)
get_service_config = get_config

__all__ = [
    "DATABENTO_VALID_OPTIONS_SYMBOLS",
    "DATABENTO_VALID_PARENT_SYMBOLS",
    "DEFI_PROTOCOLS",
    "DEFI_VENUE_TO_PROTOCOL",
    "DataTypeConfig",
    "EXCHANGE_CODE_TO_NAME",
    "ETF_TICKERS",
    "ExchangeInstrumentConfig",
    "InstrumentsServiceConfig",
    "InstrumentDefinition",
    "NASDAQ_TICKERS",
    "SP500_TICKERS",
    "TRADFI_INSTRUMENTS_CONFIG",
    "TradFiInstrument",
    "UnifiedInstrumentConfig",
    "VenueMapping",
    "corporate_actions_start_date",
    "get_config",
    "get_service_config",
    "instruments_config",
]
