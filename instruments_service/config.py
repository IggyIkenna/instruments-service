"""
Configuration for Instruments Service

Unified instrument configuration with all instruments, mappings, and metadata in one place.
Includes both domain-specific instrument definitions and service-level runtime configuration.

Note: For InstrumentDefinition Pydantic model (with full validation), use instruments_service.models.
This file contains TradFiInstrument dataclass for static TradFi instrument configuration.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

# Import from unified-cloud-services (required dependency)
from instruments_service.config.instrument_definitions import (
    SP500_TICKERS,
    TRADFI_INSTRUMENTS_CONFIG,
)

logger = logging.getLogger(__name__)

# ETF tickers including Bitcoin ETFs
ETF_TICKERS = [
    "SPY",
    "QQQ",
    "IVV",
    "VOO",
    "VTI",
    "DIA",
    "IWM",
    "IBIT",
    "FBTC",
    "ARKB",
    "GBTC",
    "BITO",  # Bitcoin ETFs
    "GLD",
    "SLV",
    "USO",  # Commodity ETFs
    "XLF",
    "XLE",
    "XLK",
    "XLV",  # Sector ETFs
]

# NASDAQ-listed tickers (subset of S&P 500 that trade on NASDAQ vs NYSE)
NASDAQ_TICKERS = [
    "AAPL",
    "ADBE",
    "ADI",
    "ADP",
    "ADSK",
    "AEP",
    "ALGN",
    "AMAT",
    "AMD",
    "AMGN",
    "AMZN",
    "ANSS",
    "ASML",
    "AVGO",
    "AZN",
    "BIDU",
    "BIIB",
    "BKNG",
    "CDNS",
    "CDW",
    "CEG",
    "CHTR",
    "CMCSA",
    "COST",
    "CPRT",
    "CRWD",
    "CSCO",
    "CSGP",
    "CSX",
    "CTAS",
    "CTSH",
    "DDOG",
    "DLTR",
    "DXCM",
    "EA",
    "EBAY",
    "ENPH",
    "EXC",
    "FANG",
    "FAST",
    "FTNT",
    "GEHC",
    "GFS",
    "GILD",
    "GOOG",
    "GOOGL",
    "HON",
    "IDXX",
    "ILMN",
    "INTC",
    "INTU",
    "ISRG",
    "JD",
    "KDP",
    "KHC",
    "KLAC",
    "LRCX",
    "LULU",
    "MAR",
    "MCHP",
    "MDLZ",
    "MELI",
    "META",
    "MNST",
    "MRNA",
    "MRVL",
    "MSFT",
    "MU",
    "NFLX",
    "NVDA",
    "NXPI",
    "ODFL",
    "ON",
    "ORLY",
    "PANW",
    "PAYX",
    "PCAR",
    "PDD",
    "PEP",
    "PYPL",
    "QCOM",
    "REGN",
    "RIVN",
    "ROST",
    "SBUX",
    "SIRI",
    "SNPS",
    "TEAM",
    "TMUS",
    "TSLA",
    "TTD",
    "TXN",
    "VRSK",
    "VRTX",
    "WBA",
    "WDAY",
    "XEL",
    "ZM",
    "ZS",
    # Bitcoin ETFs also trade on NASDAQ
    "IBIT",
    "FBTC",
    "ARKB",
]

# ============================================================================
# DATA LOADING (now uses embedded constants, no external JSON)
# ============================================================================

# Caches for loaded data (for backward compatibility)
_sp500_tickers_cache: list[str] | None = None
_nasdaq_tickers_cache: list[str] | None = None
_tradfi_instruments_cache: list[dict[str, str | None]] | None = None
_exchange_code_to_name_cache: dict[str, str] | None = None


def _get_data_dir() -> Path:
    """Get the data directory path."""
    return Path(__file__).parent / "data"


# ============================================================================
# TRADFI INSTRUMENTS CONFIG (Inline - no external JSON file needed)
# ============================================================================
# This maps Databento symbols to venues and datasets for TradFi instruments.
#
# DATASETS:
# - GLBX.MDP3: CME Globex (ES, NQ, CL, NG, GC, SI, HG, ZC, ZW, ZS, currencies)
# - IFUS.IMPACT: ICE Futures US (Cotton, Coffee, Sugar, Cocoa, OJ, Dollar Index)
# - IFEU.IMPACT: ICE Futures Europe (Brent Crude, Gasoil, etc.)
# - NDEX.IMPACT: ICE Endex (European natural gas)
# - OPRA.PILLAR: US Options (equities, indices)
# - DBEQ.BASIC: US Equities
#
# PARENT SYMBOLOGY: Use [ROOT].FUT for futures, [ROOT].OPT for options
# See: https://databento.com/docs/standards-and-conventions/symbology
# TRADFI_INSTRUMENTS_CONFIG moved to instrument_definitions.py (Issue #90)

# ============================================================================
# VALID DATABENTO PARENT SYMBOLS - Used for validation in market-tick-data-handler
# ============================================================================
# Maps canonical underlying -> (databento_parent_symbol, dataset)
# Only symbols in this map are valid for parent symbology downloads
DATABENTO_VALID_PARENT_SYMBOLS: dict[str, tuple[str, str]] = {
    # CME Index Futures
    "ES": ("ES.FUT", "GLBX.MDP3"),
    "SP500": ("ES.FUT", "GLBX.MDP3"),
    "NQ": ("NQ.FUT", "GLBX.MDP3"),
    "NASDAQ100": ("NQ.FUT", "GLBX.MDP3"),
    "RTY": ("RTY.FUT", "GLBX.MDP3"),
    "RUSSELL2000": ("RTY.FUT", "GLBX.MDP3"),
    "YM": ("YM.FUT", "GLBX.MDP3"),
    "DOW": ("YM.FUT", "GLBX.MDP3"),
    # CME Energy
    "CL": ("CL.FUT", "GLBX.MDP3"),
    "CRUDE": ("CL.FUT", "GLBX.MDP3"),
    "NG": ("NG.FUT", "GLBX.MDP3"),
    "NATGAS": ("NG.FUT", "GLBX.MDP3"),
    "RB": ("RB.FUT", "GLBX.MDP3"),
    "GASOLINE": ("RB.FUT", "GLBX.MDP3"),
    "HO": ("HO.FUT", "GLBX.MDP3"),
    "HEATINGOIL": ("HO.FUT", "GLBX.MDP3"),
    # CME Metals
    "GC": ("GC.FUT", "GLBX.MDP3"),
    "GOLD": ("GC.FUT", "GLBX.MDP3"),
    "SI": ("SI.FUT", "GLBX.MDP3"),
    "SILVER": ("SI.FUT", "GLBX.MDP3"),
    "HG": ("HG.FUT", "GLBX.MDP3"),
    "COPPER": ("HG.FUT", "GLBX.MDP3"),
    "PL": ("PL.FUT", "GLBX.MDP3"),
    "PLATINUM": ("PL.FUT", "GLBX.MDP3"),
    # CME Grains
    "ZC": ("ZC.FUT", "GLBX.MDP3"),
    "CORN": ("ZC.FUT", "GLBX.MDP3"),
    "ZW": ("ZW.FUT", "GLBX.MDP3"),
    "WHEAT": ("ZW.FUT", "GLBX.MDP3"),
    "ZS": ("ZS.FUT", "GLBX.MDP3"),
    "SOYBEAN": ("ZS.FUT", "GLBX.MDP3"),
    "ZM": ("ZM.FUT", "GLBX.MDP3"),
    "SOYMEAL": ("ZM.FUT", "GLBX.MDP3"),
    "ZL": ("ZL.FUT", "GLBX.MDP3"),
    "SOYOIL": ("ZL.FUT", "GLBX.MDP3"),
    # CME Interest Rates
    "ZB": ("ZB.FUT", "GLBX.MDP3"),
    "TBOND": ("ZB.FUT", "GLBX.MDP3"),
    "ZN": ("ZN.FUT", "GLBX.MDP3"),
    "TNOTE10Y": ("ZN.FUT", "GLBX.MDP3"),
    "ZF": ("ZF.FUT", "GLBX.MDP3"),
    "TNOTE5Y": ("ZF.FUT", "GLBX.MDP3"),
    "ZT": ("ZT.FUT", "GLBX.MDP3"),
    "TNOTE2Y": ("ZT.FUT", "GLBX.MDP3"),
    # CME Currencies
    "6E": ("6E.FUT", "GLBX.MDP3"),
    "EUR": ("6E.FUT", "GLBX.MDP3"),
    "6J": ("6J.FUT", "GLBX.MDP3"),
    "JPY": ("6J.FUT", "GLBX.MDP3"),
    "6B": ("6B.FUT", "GLBX.MDP3"),
    "GBP": ("6B.FUT", "GLBX.MDP3"),
    "6C": ("6C.FUT", "GLBX.MDP3"),
    "CAD": ("6C.FUT", "GLBX.MDP3"),
    "6A": ("6A.FUT", "GLBX.MDP3"),
    "AUD": ("6A.FUT", "GLBX.MDP3"),
    "6S": ("6S.FUT", "GLBX.MDP3"),
    "CHF": ("6S.FUT", "GLBX.MDP3"),
    "6M": ("6M.FUT", "GLBX.MDP3"),
    "MXN": ("6M.FUT", "GLBX.MDP3"),
    "6N": ("6N.FUT", "GLBX.MDP3"),
    "NZD": ("6N.FUT", "GLBX.MDP3"),
    "6L": ("6L.FUT", "GLBX.MDP3"),
    "BRL": ("6L.FUT", "GLBX.MDP3"),
    "6Z": ("6Z.FUT", "GLBX.MDP3"),
    "ZAR": ("6Z.FUT", "GLBX.MDP3"),
    # ICE Futures US (IFUS.IMPACT) - Available from 2018-12-23
    "CT": ("CT.FUT", "IFUS.IMPACT"),
    "COTTON": ("CT.FUT", "IFUS.IMPACT"),
    "CC": ("CC.FUT", "IFUS.IMPACT"),
    "COCOA": ("CC.FUT", "IFUS.IMPACT"),
    "KC": ("KC.FUT", "IFUS.IMPACT"),
    "COFFEE": ("KC.FUT", "IFUS.IMPACT"),
    "SB": ("SB.FUT", "IFUS.IMPACT"),
    "SUGAR": ("SB.FUT", "IFUS.IMPACT"),
    "OJ": ("OJ.FUT", "IFUS.IMPACT"),
    "ORANGEJUICE": ("OJ.FUT", "IFUS.IMPACT"),
    "DX": ("DX.FUT", "IFUS.IMPACT"),
    "DOLLARINDEX": ("DX.FUT", "IFUS.IMPACT"),
    # ICE Futures Europe - Energy (available from Dec 23, 2018)
    "BRN": ("BRN.FUT", "IFEU.IMPACT"),
    "BRENT": ("BRN.FUT", "IFEU.IMPACT"),
    "G": ("G.FUT", "IFEU.IMPACT"),
    "GASOIL": ("G.FUT", "IFEU.IMPACT"),
    "T": ("T.FUT", "IFEU.IMPACT"),
    "WTI": ("T.FUT", "IFEU.IMPACT"),
}

# Options parent symbol mapping
DATABENTO_VALID_OPTIONS_SYMBOLS: dict[str, tuple[str, str]] = {
    "ES": ("ES.OPT", "GLBX.MDP3"),
    "SP500": ("ES.OPT", "GLBX.MDP3"),
    "NQ": ("NQ.OPT", "GLBX.MDP3"),
    "NASDAQ100": ("NQ.OPT", "GLBX.MDP3"),
    "CL": ("CL.OPT", "GLBX.MDP3"),
    "CRUDE": ("CL.OPT", "GLBX.MDP3"),
    "GC": ("GC.OPT", "GLBX.MDP3"),
    "GOLD": ("GC.OPT", "GLBX.MDP3"),
}

EXCHANGE_CODE_TO_NAME: dict[str, str] = {
    # CME Index
    "ES": "SP500",
    "NQ": "NASDAQ100",
    "RTY": "RUSSELL2000",
    "YM": "DOW",
    # CME Energy
    "CL": "CRUDE",
    "NG": "NATGAS",
    "RB": "GASOLINE",
    "HO": "HEATINGOIL",
    # CME Metals
    "GC": "GOLD",
    "SI": "SILVER",
    "HG": "COPPER",
    "PL": "PLATINUM",
    # CME Grains
    "ZC": "CORN",
    "ZW": "WHEAT",
    "ZS": "SOYBEAN",
    "ZM": "SOYMEAL",
    "ZL": "SOYOIL",
    # CME Interest Rates
    "ZB": "TBOND",
    "ZN": "TNOTE10Y",
    "ZF": "TNOTE5Y",
    "ZT": "TNOTE2Y",
    # CME Currencies
    "6E": "EUR",
    "6J": "JPY",
    "6B": "GBP",
    "6C": "CAD",
    "6A": "AUD",
    "6S": "CHF",
    "6L": "BRL",
    "6N": "NZD",
    "6Z": "ZAR",
    "6M": "MXN",
    # CME Options (weekly)
    "EW1": "SP500",
    "EW2": "SP500",
    "EW3": "SP500",
    "EW4": "SP500",
    # ICE US
    "CT": "COTTON",
    "CC": "COCOA",
    "KC": "COFFEE",
    "SB": "SUGAR",
    "OJ": "ORANGEJUICE",
    "DX": "DOLLARINDEX",
    # ICE Europe
    "BRN": "BRENT",
    "G": "GASOIL",
    "T": "WTI",
}


def _load_sp500_tickers() -> tuple[list[str], list[str]]:
    """Load S&P 500 tickers and ETF tickers from embedded constants.

    Returns both regular tickers and ETF tickers (including Bitcoin ETFs like IBIT, FBTC)
    combined into a single list for instrument generation.

    Note: Data is now embedded in config.py (SP500_TICKERS, ETF_TICKERS, NASDAQ_TICKERS)
    instead of loading from sp500_tickers.json to ensure it's version-controlled
    and available in all deployment environments (per centralized config checklist).
    """
    global _sp500_tickers_cache, _nasdaq_tickers_cache

    if _sp500_tickers_cache is not None:
        return _sp500_tickers_cache, _nasdaq_tickers_cache or []

    # Use embedded constants instead of JSON file
    _nasdaq_tickers_cache = list(NASDAQ_TICKERS)

    # Combine S&P 500 tickers and ETF tickers, avoiding duplicates
    all_tickers = list(SP500_TICKERS)
    for etf in ETF_TICKERS:
        if etf not in all_tickers:
            all_tickers.append(etf)

    _sp500_tickers_cache = all_tickers
    logger.debug(
        f"Loaded {len(SP500_TICKERS)} S&P 500 tickers + {len(ETF_TICKERS)} ETF tickers = "
        f"{len(_sp500_tickers_cache)} total from embedded config"
    )

    return _sp500_tickers_cache, _nasdaq_tickers_cache


def _load_tradfi_instruments() -> tuple[list[dict[str, str | None]], dict[str, str]]:
    """Load TradFi instruments and exchange code mappings from inline config.

    Previously loaded from data/tradfi_instruments.json, now uses inline
    TRADFI_INSTRUMENTS_CONFIG and EXCHANGE_CODE_TO_NAME for version control.
    """
    global _tradfi_instruments_cache, _exchange_code_to_name_cache

    if _tradfi_instruments_cache is not None:
        return _tradfi_instruments_cache, _exchange_code_to_name_cache or {}

    # Use inline config instead of external JSON file
    _tradfi_instruments_cache = TRADFI_INSTRUMENTS_CONFIG
    _exchange_code_to_name_cache = EXCHANGE_CODE_TO_NAME
    logger.debug(f"Loaded {len(_tradfi_instruments_cache)} TradFi instruments from inline config")

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

    Loads instruments and exchange code mappings from external JSON files.
    All TradFi instruments are loaded from data/tradfi_instruments.json.
    """

    # Cached instruments loaded from JSON (initialized lazily)
    _instruments: list[TradFiInstrument] | None = field(default=None, repr=False)
    _exchange_code_to_name: dict[str, str] | None = field(default=None, repr=False)

    def __post_init__(self):
        """Load instruments from JSON on first access."""
        self._load_data()

    def _load_data(self) -> None:
        """Load TradFi instruments and exchange mappings from JSON file."""
        if self._instruments is not None:
            return

        raw_instruments, exchange_mappings = _load_tradfi_instruments()

        # Convert raw JSON to TradFiInstrument objects
        self._instruments = []
        for inst in raw_instruments:
            self._instruments.append(
                TradFiInstrument(
                    symbol=inst["symbol"] or "",
                    venue=inst["venue"] or "",
                    instrument_type=inst["type"] or "",
                    dataset=inst["dataset"] or "",
                    stype_in=inst["stype"] or "",
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

    # ETFs that are in the S&P 500 or commonly traded (should NOT be classified as EQUITY)
    KNOWN_ETFS = {
        "SPY",
        "QQQ",
        "IVV",
        "VOO",
        "VTI",
        "DIA",
        "IWM",
        "EEM",
        "VEA",
        "VWO",
        "GLD",
        "SLV",
        "USO",
        "UNG",
        "TLT",
        "IEF",
        "SHY",
        "LQD",
        "HYG",
        "JNK",
        "XLF",
        "XLE",
        "XLK",
        "XLV",
        "XLI",
        "XLY",
        "XLP",
        "XLB",
        "XLU",
        "XLRE",
        "VNQ",
        "IBB",
        "SMH",
        "ARKK",
        "ARKG",
        "ARKW",
        "ARKF",
        "ARKQ",
        "IBIT",
        "FBTC",
        "ARKB",
        "GBTC",
        "BITO",  # Bitcoin ETFs
    }

    # Symbols with spaces that need dot format for Databento API
    # Databento uses "BRK.B" not "BRK B"
    SPACE_TO_DOT_SYMBOLS = {
        "BRK B": "BRK.B",  # Berkshire Hathaway Class B
        "BF B": "BF.B",  # Brown-Forman Class B
        "BRK A": "BRK.A",  # Berkshire Hathaway Class A
        "BF A": "BF.A",  # Brown-Forman Class A
    }

    def _get_sp500_equities(self) -> list[TradFiInstrument]:
        """Generate S&P 500 equity/ETF instrument definitions from external data file."""
        sp500_tickers, nasdaq_tickers = _load_sp500_tickers()

        if not sp500_tickers:
            logger.warning("No S&P 500 tickers loaded - returning empty list")
            return []

        instruments: list[TradFiInstrument] = []
        for ticker in sp500_tickers:
            # Convert space symbols to dot format for Databento
            databento_symbol = self.SPACE_TO_DOT_SYMBOLS.get(ticker, ticker)

            # Determine venue (NASDAQ for known tech stocks, NYSE for others)
            venue = "NASDAQ" if ticker in nasdaq_tickers else "NYSE"

            # Determine instrument type (ETF vs EQUITY)
            instrument_type = "ETF" if ticker in self.KNOWN_ETFS else "EQUITY"

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


# ============================================================================
# SERVICE-LEVEL CONFIGURATION (Pydantic BaseSettings)
# InstrumentsServiceConfig, get_config, instruments_config live in service_config.py
# ============================================================================
