"""
Instrument definitions for instruments-service.

Extracted from config.py per Task 1.1.1 (Issue #89), Task 1.1.2 (Issue #90).
DeFi protocol configs per Task 1.1.3 (Issue #91).

Ticker data (SP500, ETF, NASDAQ) is loaded from data/tickers.json (ISS-049).

Static reference data (TRADFI_VENUE_MAPPINGS, DEFI_VENUE_TO_PROTOCOL, DEFI_PROTOCOLS)
has been moved to unified-api-contracts (UAC) as the SSOT.
Import from `unified_api_contracts` instead.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

_DATA_DIR = Path(__file__).parent / "data"


def _load_tickers() -> dict[str, list[str] | str]:
    """Load ticker lists from the consolidated data/tickers.json file.

    Returns a dict with keys: sp500_tickers, etf_tickers, nasdaq_tickers,
    corporate_actions_start_date.
    """
    tickers_path = _DATA_DIR / "tickers.json"
    with open(tickers_path) as f:
        return cast(dict[str, list[str] | str], cast(object, json.load(f)))


_TICKERS = _load_tickers()

# Source: data/tickers.json (consolidated from ticker_lists.py, equity_definitions.py)
# Last updated: 2026-01-31
corporate_actions_start_date: str = cast(str, _TICKERS["corporate_actions_start_date"])

SP500_TICKERS: list[str] = cast(list[str], _TICKERS["sp500_tickers"])

ETF_TICKERS: list[str] = cast(list[str], _TICKERS["etf_tickers"])

NASDAQ_TICKERS: list[str] = cast(list[str], _TICKERS["nasdaq_tickers"])
