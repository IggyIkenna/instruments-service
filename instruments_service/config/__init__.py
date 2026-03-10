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

from pathlib import Path as _Path

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
from instruments_service.config.venue_config import (
    _load_sp500_tickers as _load_sp500_tickers,
)
from instruments_service.config.venue_config import (
    _load_tradfi_instruments as _load_tradfi_instruments,
)


def _get_data_dir() -> _Path:
    """Get the data directory for the config package."""
    return _Path(__file__).parent / "data"


__all__ = [
    "DATABENTO_VALID_OPTIONS_SYMBOLS",
    "DATABENTO_VALID_PARENT_SYMBOLS",
    "DEFI_PROTOCOLS",
    "DEFI_VENUE_TO_PROTOCOL",
    "ETF_TICKERS",
    "EXCHANGE_CODE_TO_NAME",
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
