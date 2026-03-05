"""
Ticker lists for various exchanges and indices.

DEPRECATED: This module re-exports from instrument_definitions.py.
All ticker data is consolidated in data/tickers.json (ISS-049).
Import from instruments_service.config.instrument_definitions instead.
"""

from instruments_service.config.instrument_definitions import (
    ETF_TICKERS,
    NASDAQ_TICKERS,
    SP500_TICKERS,
    corporate_actions_start_date,
)

__all__ = [
    "ETF_TICKERS",
    "NASDAQ_TICKERS",
    "SP500_TICKERS",
    "corporate_actions_start_date",
]
