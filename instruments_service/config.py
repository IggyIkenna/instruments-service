"""
Configuration for Instruments Service

Unified instrument configuration with all instruments, mappings, and metadata in one place.
Includes both domain-specific instrument definitions and service-level runtime configuration.

Note: For InstrumentDefinition Pydantic model (with full validation), use instruments_service.models.
This file contains TradFiInstrument dataclass for static TradFi instrument configuration.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from pydantic import Field, AliasChoices
from pydantic_settings import SettingsConfigDict

# Import from unified-cloud-services (required dependency)
from unified_cloud_services import (
    UnifiedCloudServicesConfig,
    CloudTarget,
)

logger = logging.getLogger(__name__)

# ============================================================================
# DATA LOADING FROM JSON FILES
# ============================================================================

# Caches for loaded data
_sp500_tickers_cache: Optional[List[str]] = None
_nasdaq_tickers_cache: Optional[List[str]] = None
_tradfi_instruments_cache: Optional[List[Dict]] = None
_exchange_code_to_name_cache: Optional[Dict[str, str]] = None


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

TRADFI_INSTRUMENTS_CONFIG: List[Dict] = [
    # =========================================================================
    # CME Globex Futures (GLBX.MDP3) - Index, Energy, Metals, Grains, Currencies
    # =========================================================================
    {"symbol": "ES.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "ES"},
    {"symbol": "NQ.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "NASDAQ100", "code": "NQ"},
    {"symbol": "RTY.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "RUSSELL2000", "code": "RTY"},
    {"symbol": "YM.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "DOW", "code": "YM"},
    # Energy (CME/NYMEX)
    {"symbol": "CL.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "CRUDE", "code": "CL"},
    {"symbol": "NG.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "NATGAS", "code": "NG"},
    {"symbol": "RB.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "GASOLINE", "code": "RB"},
    {"symbol": "HO.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "HEATINGOIL", "code": "HO"},
    # Metals (CME/COMEX)
    {"symbol": "GC.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "GOLD", "code": "GC"},
    {"symbol": "SI.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SILVER", "code": "SI"},
    {"symbol": "HG.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "COPPER", "code": "HG"},
    {"symbol": "PL.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "PLATINUM", "code": "PL"},
    # Grains (CME/CBOT)
    {"symbol": "ZC.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "CORN", "code": "ZC"},
    {"symbol": "ZW.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "WHEAT", "code": "ZW"},
    {"symbol": "ZS.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SOYBEAN", "code": "ZS"},
    {"symbol": "ZM.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SOYMEAL", "code": "ZM"},
    {"symbol": "ZL.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SOYOIL", "code": "ZL"},
    # Interest Rates (CME/CBOT)
    {"symbol": "ZB.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "TBOND", "code": "ZB"},
    {"symbol": "ZN.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "TNOTE10Y", "code": "ZN"},
    {"symbol": "ZF.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "TNOTE5Y", "code": "ZF"},
    {"symbol": "ZT.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "TNOTE2Y", "code": "ZT"},
    # Currencies (CME)
    {"symbol": "6E.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "EUR", "code": "6E"},
    {"symbol": "6J.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "JPY", "code": "6J"},
    {"symbol": "6B.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "GBP", "code": "6B"},
    {"symbol": "6C.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "CAD", "code": "6C"},
    {"symbol": "6A.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "AUD", "code": "6A"},
    {"symbol": "6S.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "CHF", "code": "6S"},
    {"symbol": "6L.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "BRL", "code": "6L"},
    {"symbol": "6N.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "NZD", "code": "6N"},
    {"symbol": "6Z.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "ZAR", "code": "6Z"},
    {"symbol": "6M.FUT", "venue": "CME", "type": "FUTURE", "dataset": "GLBX.MDP3", "stype": "parent", "base": "MXN", "code": "6M"},
    # =========================================================================
    # CME Options (GLBX.MDP3)
    # =========================================================================
    {"symbol": "ES.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "ES"},
    {"symbol": "NQ.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "NASDAQ100", "code": "NQ"},
    {"symbol": "CL.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "CRUDE", "code": "CL"},
    {"symbol": "GC.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "GOLD", "code": "GC"},
    # Weekly ES options (EW1-EW4)
    {"symbol": "EW1.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "EW1", "underlying": "ES"},
    {"symbol": "EW2.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "EW2", "underlying": "ES"},
    {"symbol": "EW3.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "EW3", "underlying": "ES"},
    {"symbol": "EW4.OPT", "venue": "CME", "type": "OPTION", "dataset": "GLBX.MDP3", "stype": "parent", "base": "SP500", "code": "EW4", "underlying": "ES"},
    # =========================================================================
    # ICE Futures US (IFUS.IMPACT) - Soft Commodities & Dollar Index
    # Available from: December 23, 2018 (historical coverage start)
    # https://databento.com/blog/introducing-ice-futures-us
    # =========================================================================
    {"symbol": "CT.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "COTTON", "code": "CT"},
    {"symbol": "CC.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "COCOA", "code": "CC"},
    {"symbol": "KC.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "COFFEE", "code": "KC"},
    {"symbol": "SB.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "SUGAR", "code": "SB"},
    {"symbol": "OJ.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "ORANGEJUICE", "code": "OJ"},
    {"symbol": "DX.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFUS.IMPACT", "stype": "parent", "base": "DOLLARINDEX", "code": "DX"},
    # =========================================================================
    # ICE Futures Europe (IFEU.IMPACT) - Energy commodities
    # Available from: December 23, 2018 (same as ICE US)
    # NOTE: Brent crude symbol on ICE Europe is "BRN", not "B"
    # =========================================================================
    {"symbol": "BRN.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFEU.IMPACT", "stype": "parent", "base": "BRENT", "code": "BRN"},
    {"symbol": "G.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFEU.IMPACT", "stype": "parent", "base": "GASOIL", "code": "G"},
    {"symbol": "T.FUT", "venue": "ICE", "type": "FUTURE", "dataset": "IFEU.IMPACT", "stype": "parent", "base": "WTI", "code": "T"},
]

# ============================================================================
# VALID DATABENTO PARENT SYMBOLS - Used for validation in market-tick-data-handler
# ============================================================================
# Maps canonical underlying -> (databento_parent_symbol, dataset)
# Only symbols in this map are valid for parent symbology downloads
DATABENTO_VALID_PARENT_SYMBOLS: Dict[str, Tuple[str, str]] = {
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
DATABENTO_VALID_OPTIONS_SYMBOLS: Dict[str, Tuple[str, str]] = {
    "ES": ("ES.OPT", "GLBX.MDP3"),
    "SP500": ("ES.OPT", "GLBX.MDP3"),
    "NQ": ("NQ.OPT", "GLBX.MDP3"),
    "NASDAQ100": ("NQ.OPT", "GLBX.MDP3"),
    "CL": ("CL.OPT", "GLBX.MDP3"),
    "CRUDE": ("CL.OPT", "GLBX.MDP3"),
    "GC": ("GC.OPT", "GLBX.MDP3"),
    "GOLD": ("GC.OPT", "GLBX.MDP3"),
}

EXCHANGE_CODE_TO_NAME: Dict[str, str] = {
    # CME Index
    "ES": "SP500", "NQ": "NASDAQ100", "RTY": "RUSSELL2000", "YM": "DOW",
    # CME Energy
    "CL": "CRUDE", "NG": "NATGAS", "RB": "GASOLINE", "HO": "HEATINGOIL",
    # CME Metals
    "GC": "GOLD", "SI": "SILVER", "HG": "COPPER", "PL": "PLATINUM",
    # CME Grains
    "ZC": "CORN", "ZW": "WHEAT", "ZS": "SOYBEAN", "ZM": "SOYMEAL", "ZL": "SOYOIL",
    # CME Interest Rates
    "ZB": "TBOND", "ZN": "TNOTE10Y", "ZF": "TNOTE5Y", "ZT": "TNOTE2Y",
    # CME Currencies
    "6E": "EUR", "6J": "JPY", "6B": "GBP", "6C": "CAD", "6A": "AUD", "6S": "CHF",
    "6L": "BRL", "6N": "NZD", "6Z": "ZAR", "6M": "MXN",
    # CME Options (weekly)
    "EW1": "SP500", "EW2": "SP500", "EW3": "SP500", "EW4": "SP500",
    # ICE US
    "CT": "COTTON", "CC": "COCOA", "KC": "COFFEE", "SB": "SUGAR",
    "OJ": "ORANGEJUICE", "DX": "DOLLARINDEX",
    # ICE Europe
    "BRN": "BRENT", "G": "GASOIL", "T": "WTI",
}


def _load_sp500_tickers() -> Tuple[List[str], List[str]]:
    """Load S&P 500 tickers and ETF tickers from JSON file.

    Returns both regular tickers and ETF tickers (including Bitcoin ETFs like IBIT, FBTC)
    combined into a single list for instrument generation.
    """
    global _sp500_tickers_cache, _nasdaq_tickers_cache

    if _sp500_tickers_cache is not None:
        return _sp500_tickers_cache, _nasdaq_tickers_cache or []

    try:
        data_file = _get_data_dir() / "sp500_tickers.json"
        if data_file.exists():
            with open(data_file, "r") as f:
                data = json.load(f)
                base_tickers = data.get("tickers", [])
                etf_tickers = data.get("etf_tickers", [])  # Includes Bitcoin ETFs (IBIT, FBTC, etc.)
                _nasdaq_tickers_cache = data.get("nasdaq_tickers", [])

                # Combine tickers and ETF tickers, avoiding duplicates
                # ETFs like IBIT, FBTC, ARKB, GBTC, BITO need to be included for Bitcoin ETF support
                all_tickers = list(base_tickers)
                for etf in etf_tickers:
                    if etf not in all_tickers:
                        all_tickers.append(etf)

                _sp500_tickers_cache = all_tickers
                logger.debug(f"Loaded {len(base_tickers)} base tickers + {len(etf_tickers)} ETF tickers = {len(_sp500_tickers_cache)} total from {data_file}")
        else:
            logger.warning(f"S&P 500 tickers file not found: {data_file}")
            _sp500_tickers_cache = []
            _nasdaq_tickers_cache = []
    except Exception as e:
        logger.error(f"Failed to load S&P 500 tickers: {e}")
        _sp500_tickers_cache = []
        _nasdaq_tickers_cache = []

    return _sp500_tickers_cache, _nasdaq_tickers_cache


def _load_tradfi_instruments() -> Tuple[List[Dict], Dict[str, str]]:
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
    base_asset: Optional[str] = None  # Human-readable base asset name
    quote_asset: str = "USD"  # Quote currency (default USD for TradFi)
    exchange_code: Optional[str] = None  # Databento exchange code (e.g., "ES", "CL")
    underlying: Optional[str] = None  # Underlying asset (e.g., "BTC" for Bitcoin ETFs)


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
    _instruments: Optional[List[TradFiInstrument]] = field(default=None, repr=False)
    _exchange_code_to_name: Optional[Dict[str, str]] = field(default=None, repr=False)

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
            self._instruments.append(TradFiInstrument(
                symbol=inst["symbol"],
                venue=inst["venue"],
                instrument_type=inst["type"],
                dataset=inst["dataset"],
                stype_in=inst["stype"],
                base_asset=inst.get("base"),
                quote_asset="USD",
                exchange_code=inst.get("code"),
                underlying=inst.get("underlying"),
            ))

        self._exchange_code_to_name = exchange_mappings

    @property
    def instruments(self) -> List[TradFiInstrument]:
        """Get base TradFi instruments (futures, options, ETFs)."""
        if self._instruments is None:
            self._load_data()
        return self._instruments or []

    @property
    def exchange_code_to_name(self) -> Dict[str, str]:
        """Get exchange code to human-readable name mapping."""
        if self._exchange_code_to_name is None:
            self._load_data()
        return self._exchange_code_to_name or {}

    def get_symbols_for_venue(self, venue: str) -> List[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.venue == venue.upper()]

    def get_symbols_for_dataset(self, dataset: str) -> List[str]:
        """Get all symbols for a dataset (e.g., 'GLBX.MDP3', 'DBEQ.BASIC')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.dataset == dataset]

    def get_symbols_by_type(self, instrument_type: str) -> List[str]:
        """Get all symbols for an instrument type (e.g., 'FUTURE', 'EQUITY', 'OPTION')"""
        all_insts = self.get_all_instruments()
        return [
            inst.symbol for inst in all_insts if inst.instrument_type == instrument_type.upper()
        ]

    def get_dataset_and_stype(self, symbol: str) -> Optional[Tuple[str, str]]:
        """Get dataset and stype_in for a symbol"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
            if inst.symbol == symbol:
                return (inst.dataset, inst.stype_in)
        return None

    def get_instrument(
        self, symbol: str, venue: Optional[str] = None
    ) -> Optional[InstrumentDefinition]:
        """Get instrument definition by symbol (optionally filtered by venue)"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
            if inst.symbol == symbol:
                if venue is None or inst.venue == venue.upper():
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
        'SPY', 'QQQ', 'IVV', 'VOO', 'VTI', 'DIA', 'IWM', 'EEM', 'VEA', 'VWO',
        'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'IEF', 'SHY', 'LQD', 'HYG', 'JNK',
        'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLU', 'XLRE',
        'VNQ', 'IBB', 'SMH', 'ARKK', 'ARKG', 'ARKW', 'ARKF', 'ARKQ',
        'IBIT', 'FBTC', 'ARKB', 'GBTC', 'BITO'  # Bitcoin ETFs
    }

    # Symbols with spaces that need dot format for Databento API
    # Databento uses "BRK.B" not "BRK B"
    SPACE_TO_DOT_SYMBOLS = {
        'BRK B': 'BRK.B',   # Berkshire Hathaway Class B
        'BF B': 'BF.B',     # Brown-Forman Class B
        'BRK A': 'BRK.A',   # Berkshire Hathaway Class A
        'BF A': 'BF.A'      # Brown-Forman Class A
    }

    def _get_sp500_equities(self) -> List[TradFiInstrument]:
        """Generate S&P 500 equity/ETF instrument definitions from external data file."""
        sp500_tickers, nasdaq_tickers = _load_sp500_tickers()

        if not sp500_tickers:
            logger.warning("No S&P 500 tickers loaded - returning empty list")
            return []

        instruments = []
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
                    quote_asset="USD"
                )
            )
        return instruments

    def get_all_instruments(self) -> List[TradFiInstrument]:
        """Get all instruments (base instruments + dynamically generated S&P 500 equities)"""
        # Combine base instruments with dynamically generated S&P 500 equities
        all_insts = list(self.instruments)
        sp500_equities = self._get_sp500_equities()
        all_insts.extend(sp500_equities)
        return all_insts


# ============================================================================
# SERVICE-LEVEL CONFIGURATION (Pydantic BaseSettings)
# VenueMapping, DataTypeConfig, ExchangeInstrumentConfig
# are imported from unified_cloud_services (see imports at top of file)
# ============================================================================


class InstrumentsServiceConfig(UnifiedCloudServicesConfig):
    """
    Service-level configuration for instruments-service.

    Extends UnifiedCloudServicesConfig with instruments-specific settings.
    Inherited from parent: gcp_project_id, google_application_credentials_path,
    gcs_region, gcs_location, bigquery_location, tardis_secret_name,
    databento_secret_name, alchemy_secret_name, aavescan_secret_name,
    enable_csv_sampling, csv_sample_size, csv_sample_dir, log_level, etc.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields from .env
    )

    # =========================================================================
    # SERVICE IDENTIFICATION (override parent default)
    # =========================================================================
    service_name: str = Field(default="instruments-service", description="Service name")

    # =========================================================================
    # INSTRUMENTS-SPECIFIC GCS BUCKETS
    # These override the parent's generic bucket with instruments-specific env vars
    # =========================================================================
    gcs_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET"),
        description="Primary GCS bucket for instruments",
    )
    gcs_bucket_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TEST"),
        description="Test GCS bucket for instruments",
    )
    # Category-specific buckets for independent batch processing
    gcs_bucket_cefi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
        description="GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
        description="GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
        description="GCS bucket for DEFI instruments",
    )
    # Category-specific TEST buckets
    gcs_bucket_cefi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI_TEST"),
        description="Test GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI_TEST"),
        description="Test GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI_TEST"),
        description="Test GCS bucket for DEFI instruments",
    )

    # =========================================================================
    # INSTRUMENTS-SPECIFIC BIGQUERY (override parent with instruments-specific env var)
    # =========================================================================
    bigquery_dataset: str = Field(
        default="instruments",
        validation_alias=AliasChoices("INSTRUMENTS_BIGQUERY_DATASET", "BIGQUERY_DATASET"),
        description="BigQuery dataset for instruments",
    )

    # =========================================================================
    # INSTRUMENTS-SPECIFIC PROCESSING CONFIGURATION
    # =========================================================================
    enable_ccxt_integration: bool = Field(
        default=True, description="Enable CCXT metadata enrichment"
    )
    enable_metadata_caching: bool = Field(default=True, description="Enable metadata caching")
    cache_ttl_hours: int = Field(default=24, description="Cache TTL in hours")
    max_batch_size: int = Field(default=1000, description="Maximum batch size for processing")
    lookback_days: int = Field(default=0, description="Lookback days for batch processing")

    # =========================================================================
    # THE GRAPH SECRET NAME (different name than parent's thegraph_secret_name)
    # =========================================================================
    graph_secret_name: str = Field(
        default="graph-api-key",
        validation_alias=AliasChoices("GRAPH_SECRET_NAME"),
        description="Graph API key secret name",
    )

    # =========================================================================
    # DEFI URLS (instruments-specific)
    # =========================================================================
    aavescan_api_url: str = Field(
        default="https://api.aavescan.com/v2",
        validation_alias=AliasChoices("AAVESCAN_API_URL"),
        description="AaveScan Pro API base URL",
    )
    ethereum_rpc_url: str = Field(
        default="",
        validation_alias=AliasChoices("ETHEREUM_RPC_URL"),
        description="Ethereum RPC URL",
    )
    uniswap_v3_graph_url: str = Field(
        default="",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_URL"),
        description="Uniswap V3 Graph URL",
    )
    uniswap_v3_graph_arb_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-arbitrum/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_ARB_URL"),
        description="The Graph Uniswap V3 URL for Arbitrum",
    )
    uniswap_v3_graph_base_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-base/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_BASE_URL"),
        description="The Graph Uniswap V3 URL for Base",
    )
    envio_api_url: str = Field(
        default="",
        validation_alias=AliasChoices("ENVIO_API_URL"),
        description="Envio API URL",
    )

    # Hyperliquid API URL
    hyperliquid_api_url: str = Field(
        default="https://api.hyperliquid.xyz",
        validation_alias=AliasChoices("HYPERLIQUID_API_URL"),
        description="Hyperliquid API base URL",
    )

    # The Graph Gateway URL template
    thegraph_gateway_url: str = Field(
        default="https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}",
        validation_alias=AliasChoices("THEGRAPH_GATEWAY_URL"),
        description="The Graph Gateway URL template",
    )

    # Uniswap V3 Mainnet Subgraph ID (for The Graph Gateway)
    uniswap_v3_mainnet_subgraph_id: str = Field(
        default="5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        validation_alias=AliasChoices("UNISWAP_V3_MAINNET_SUBGRAPH_ID"),
        description="Uniswap V3 Ethereum mainnet subgraph ID",
    )

    # Default TheGraph Uniswap V3 Studio URL (fallback when no API key)
    thegraph_uniswap_v3_studio_url: str = Field(
        default="https://api.studio.thegraph.com/query/48211/uniswap-v3-mainnet/version/latest",
        validation_alias=AliasChoices("THEGRAPH_UNISWAP_V3_STUDIO_URL"),
        description="TheGraph Uniswap V3 Studio URL (public, rate-limited)",
    )

    # Alchemy mainnet URL template
    alchemy_mainnet_url_template: str = Field(
        default="https://eth-mainnet.g.alchemy.com/v2/{api_key}",
        validation_alias=AliasChoices("ALCHEMY_MAINNET_URL_TEMPLATE"),
        description="Alchemy Ethereum mainnet URL template",
    )

    # DeFi MVP tokens configuration
    defi_mvp_tokens: str = Field(
        default="",
        validation_alias=AliasChoices("DEFI_MVP_TOKENS"),
        description="Comma-separated list of DeFi MVP tokens",
    )

    # ClickUp Configuration
    clickup_secret_name: str = Field(
        default="clickup-api-key",
        validation_alias=AliasChoices("CLICKUP_SECRET_NAME"),
        description="ClickUp API key secret name",
    )
    clickup_list_id: str = Field(
        default="",
        validation_alias=AliasChoices("clickup_list_id_instruments_service"),
        description="ClickUp List ID",
    )
    clickup_user_id_ikenna: str = Field(
        default="254573729",
        validation_alias=AliasChoices("clickup_user_id_ikenna"),
        description="ClickUp User ID for Ikenna",
    )
    clickup_user_id_harsh: str = Field(
        default="100698878",
        validation_alias=AliasChoices("clickup_user_id_harsh"),
        description="ClickUp User ID for Harsh",
    )
    clickup_user_id_femi: str = Field(
        default="100698756",
        validation_alias=AliasChoices("clickup_user_id_femi"),
        description="ClickUp User ID for Femi",
    )
    clickup_user_id_daniel: str = Field(
        default="36559682",
        validation_alias=AliasChoices("clickup_user_id_daniel"),
        description="ClickUp User ID for Daniel",
    )

    def get_cloud_target(self, category: str | None = None) -> CloudTarget:
        """
        Get CloudTarget for instruments service.

        Args:
            category: Optional market category ("CEFI", "TRADFI", "DEFI") to use category-specific bucket

        Returns:
            CloudTarget with appropriate bucket for category
        """
        # Determine bucket based on category
        if category:
            category_upper = category.upper()
            if category_upper == "CEFI":
                bucket = self.gcs_bucket_cefi
            elif category_upper == "TRADFI":
                bucket = self.gcs_bucket_tradfi
            elif category_upper == "DEFI":
                bucket = self.gcs_bucket_defi
            else:
                raise ValueError(
                    f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI"
                )
        else:
            bucket = self.gcs_bucket

        return CloudTarget(
            project_id=self.gcp_project_id,
            gcs_bucket=bucket,
            bigquery_dataset=self.bigquery_dataset,
            bigquery_location=self.bigquery_location,
        )

    def is_test_environment(self) -> bool:
        """Check if the current environment is a test environment."""
        return self.environment.lower() in ["test", "testing"]

    # Properties for uppercase bucket names (compatibility with unified-cloud-services)
    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI(self) -> str:
        return self.gcs_bucket_cefi

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI(self) -> str:
        return self.gcs_bucket_tradfi

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI(self) -> str:
        return self.gcs_bucket_defi

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI_TEST(self) -> str:
        return self.gcs_bucket_cefi_test or f"{self.gcs_bucket_cefi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI_TEST(self) -> str:
        return self.gcs_bucket_tradfi_test or f"{self.gcs_bucket_tradfi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI_TEST(self) -> str:
        return self.gcs_bucket_defi_test or f"{self.gcs_bucket_defi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET(self) -> str:
        return self.gcs_bucket

    @property
    def INSTRUMENTS_GCS_BUCKET_TEST(self) -> str:
        return self.gcs_bucket_test

    def get_bucket_for_category(self, category: str, test_mode: bool = False) -> str:
        """
        Get the GCS bucket name for a specific market category.

        Args:
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            test_mode: Whether to use test bucket (default: False)

        Returns:
            Bucket name from environment variables

        Raises:
            ValueError: If category is invalid or bucket not configured
        """
        category_upper = category.upper()

        if category_upper not in ["CEFI", "TRADFI", "DEFI"]:
            raise ValueError(f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI")

        # Determine environment variable name
        if test_mode:
            bucket_name = f"gcs_bucket_{category_upper.lower()}_test"
        else:
            bucket_name = f"gcs_bucket_{category_upper.lower()}"

        # Get bucket from attribute
        bucket = getattr(self, bucket_name, None)

        if bucket:
            logger.debug(f"📦 Using bucket for {category_upper}: {bucket}")
            return bucket

        # Fallback to default bucket if category-specific bucket not configured
        logger.warning(
            f"⚠️ Category-specific bucket not configured for {category_upper}. "
            f"Using default bucket."
        )
        return self.gcs_bucket_test if test_mode else self.gcs_bucket


# Singleton pattern for config access
_config: Optional[InstrumentsServiceConfig] = None


def get_config() -> InstrumentsServiceConfig:
    """Get the singleton service config instance."""
    global _config
    if _config is None:
        _config = InstrumentsServiceConfig()
    return _config


# Create singleton instance (backward compatibility)
instruments_config = get_config()
