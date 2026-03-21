"""
Venue configuration for instruments-service.

TradFi UnifiedInstrumentConfig, TradFiInstrument, and loaders.
Ticker/instrument data lives in instrument_definitions; exchange mappings in UAC.
"""

import logging
from dataclasses import dataclass, field

from unified_api_contracts import (
    EXCHANGE_CODE_TO_NAME,
    KNOWN_ETFS,
    SPACE_TO_DOT_SYMBOLS,
    TRADFI_INSTRUMENTS_CONFIG,
)

from instruments_service.config.instrument_definitions import (
    ETF_TICKERS,
    NASDAQ_TICKERS,
    SP500_TICKERS,
)

logger = logging.getLogger(__name__)

__all__ = [
    "InstrumentDefinition",
    "TradFiInstrument",
    "UnifiedInstrumentConfig",
]

# Caches for loaded data
_sp500_tickers_cache: list[str] | None = None
_nasdaq_tickers_cache: list[str] | None = None
_tradfi_instruments_cache: list[dict[str, str | None]] | None = None
_exchange_code_to_name_cache: dict[str, str] | None = None


def _load_sp500_tickers() -> tuple[list[str], list[str]]:
    """Load S&P 500 tickers and ETF tickers from embedded constants.

    Returns both regular tickers and ETF tickers (including Bitcoin ETFs like IBIT, FBTC)
    combined into a single list for instrument generation.

    Note: Data is in instrument_definitions (SP500_TICKERS, ETF_TICKERS, NASDAQ_TICKERS).
    """
    global _sp500_tickers_cache, _nasdaq_tickers_cache

    if _sp500_tickers_cache is not None:
        return _sp500_tickers_cache, _nasdaq_tickers_cache or []

    _nasdaq_tickers_cache = list(NASDAQ_TICKERS)
    all_tickers = list(SP500_TICKERS)
    for etf in ETF_TICKERS:
        if etf not in all_tickers:
            all_tickers.append(etf)

    _sp500_tickers_cache = all_tickers
    logger.debug(
        "Loaded %s S&P 500 tickers + %s ETF tickers = %s total from embedded config",
        len(SP500_TICKERS),
        len(ETF_TICKERS),
        len(_sp500_tickers_cache),
    )

    return _sp500_tickers_cache, _nasdaq_tickers_cache


def _load_tradfi_instruments() -> tuple[list[dict[str, str | None]], dict[str, str]]:
    """Load TradFi instruments and exchange code mappings from UAC."""
    global _tradfi_instruments_cache, _exchange_code_to_name_cache

    if _tradfi_instruments_cache is not None:
        return _tradfi_instruments_cache, _exchange_code_to_name_cache or {}

    _tradfi_instruments_cache = TRADFI_INSTRUMENTS_CONFIG
    _exchange_code_to_name_cache = EXCHANGE_CODE_TO_NAME
    logger.debug("Loaded %s TradFi instruments from UAC", len(_tradfi_instruments_cache))

    return _tradfi_instruments_cache, _exchange_code_to_name_cache


@dataclass
class TradFiInstrument:
    """
    Single TradFi instrument definition with metadata.

    Note: This is different from instruments_service.models.InstrumentDefinition (Pydantic model).
    This dataclass is for static TradFi instrument configuration (Databento symbols).
    """

    symbol: str  # Databento symbol (e.g., "ES.FUT", "SPY", "BRN.FUT", "SPY.OPT")
    venue: str  # Canonical venue (e.g., "CME", "NASDAQ", "ICE")
    instrument_type: str  # "FUTURE", "EQUITY", "OPTION", "ETF"
    dataset: str  # Databento dataset (e.g., "GLBX.MDP3", "DBEQ.BASIC")
    stype_in: str  # "parent" for futures/options, "raw_symbol" for equities/ETFs
    base_asset: str | None = None  # Human-readable base asset name
    quote_asset: str = "USD"  # Quote currency (default USD for TradFi)
    exchange_code: str | None = None  # Databento exchange code (e.g., "ES", "CL")
    underlying: str | None = None  # Underlying asset (e.g., "BTC" for Bitcoin ETFs)


# Backward compatibility alias
InstrumentDefinition = TradFiInstrument


@dataclass
class UnifiedInstrumentConfig:
    """
    Unified instrument configuration - single source of truth for all TradFi instruments.

    Loads instruments and exchange code mappings from UAC.
    """

    # Cached instruments loaded from UAC (initialized lazily)
    _instruments: list[TradFiInstrument] | None = field(default=None, repr=False)
    _exchange_code_to_name: dict[str, str] | None = field(default=None, repr=False)

    def __post_init__(self):
        """Load instruments on first access."""
        self._load_data()

    def _load_data(self) -> None:
        """Load TradFi instruments and exchange mappings from UAC."""
        if self._instruments is not None:
            return

        raw_instruments, exchange_mappings = _load_tradfi_instruments()

        # Convert raw dicts to TradFiInstrument objects
        self._instruments = []
        for inst in raw_instruments:
            self._instruments.append(
                TradFiInstrument(
                    symbol=inst["symbol"] or "",
                    venue=inst["venue"] or "",
                    instrument_type=inst["type"] or "",
                    dataset=inst.get("dataset") or "",
                    stype_in=inst.get("stype") or "",
                    base_asset=inst.get("base"),
                    quote_asset="USD",
                    exchange_code=inst.get("code"),
                    underlying=inst.get("underlying"),
                )
            )

        self._exchange_code_to_name = exchange_mappings

    @property
    def instruments(self) -> list[TradFiInstrument]:
        """Get base TradFi instruments (futures, options, ETFs)."""
        if self._instruments is None:
            self._load_data()
        return self._instruments or []

    @property
    def exchange_code_to_name(self) -> dict[str, str]:
        """Get exchange code to human-readable name mapping."""
        if self._exchange_code_to_name is None:
            self._load_data()
        return self._exchange_code_to_name or {}

    def get_symbols_for_venue(self, venue: str) -> list[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.venue == venue.upper()]

    def get_symbols_for_dataset(self, dataset: str) -> list[str]:
        """Get all symbols for a dataset (e.g., 'GLBX.MDP3', 'DBEQ.BASIC')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.dataset == dataset]

    def get_symbols_by_type(self, instrument_type: str) -> list[str]:
        """Get all symbols for an instrument type (e.g., 'FUTURE', 'EQUITY', 'OPTION')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.instrument_type == instrument_type.upper()]

    def get_dataset_and_stype(self, symbol: str) -> tuple[str, str] | None:
        """Get dataset and stype_in for a symbol"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
            if inst.symbol == symbol:
                return (inst.dataset, inst.stype_in)
        return None

    def get_instrument(self, symbol: str, venue: str | None = None) -> InstrumentDefinition | None:
        """Get instrument definition by symbol (optionally filtered by venue)"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
            if inst.symbol == symbol and (venue is None or inst.venue == venue.upper()):
                return inst
        return None

    def get_human_readable_name(self, exchange_code: str) -> str:
        """Convert Databento exchange code to human-readable name"""
        if exchange_code in self.exchange_code_to_name:
            return self.exchange_code_to_name[exchange_code]
        # Check micro version (M prefix)
        if exchange_code.startswith("M") and len(exchange_code) > 1:
            base_code = exchange_code[1:]
            if base_code in self.exchange_code_to_name:
                return self.exchange_code_to_name[base_code]
        return exchange_code

    def _get_sp500_equities(self) -> list[TradFiInstrument]:
        """Generate S&P 500 equity/ETF instrument definitions from external data file."""
        sp500_tickers, nasdaq_tickers = _load_sp500_tickers()

        if not sp500_tickers:
            logger.warning("No S&P 500 tickers loaded - returning empty list")
            return []

        instruments: list[TradFiInstrument] = []
        for ticker in sp500_tickers:
            # Convert space symbols to dot format for Databento
            databento_symbol = SPACE_TO_DOT_SYMBOLS.get(ticker, ticker)

            # Determine venue (NASDAQ for known tech stocks, NYSE for others)
            venue = "NASDAQ" if ticker in nasdaq_tickers else "NYSE"

            # Determine instrument type (ETF vs EQUITY)
            instrument_type = "ETF" if ticker in KNOWN_ETFS else "EQUITY"

            instruments.append(
                TradFiInstrument(
                    symbol=databento_symbol,  # Use Databento-compatible symbol (BRK.B not BRK B)
                    venue=venue,
                    instrument_type=instrument_type,  # ETF or EQUITY
                    dataset="DBEQ.BASIC",
                    stype_in="raw_symbol",
                    base_asset=ticker,  # Keep original ticker as base_asset for display
                    quote_asset="USD",
                )
            )
        return instruments

    def get_all_instruments(self) -> list[TradFiInstrument]:
        """Get all instruments (base instruments + dynamically generated S&P 500 equities)"""
        # Combine base instruments with dynamically generated S&P 500 equities
        all_insts: list[TradFiInstrument] = list(self.instruments)
        sp500_equities: list[TradFiInstrument] = self._get_sp500_equities()
        all_insts.extend(sp500_equities)
        return all_insts
