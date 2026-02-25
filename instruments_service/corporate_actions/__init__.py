"""
Corporate Actions Module (TRADFI only)

Provides corporate action data (dividends, splits, earnings) for US equities.
Part of instruments-service reference data.

Note: Corporate actions are TRADFI-only. Crypto (CEFI) and DeFi do not have
traditional corporate actions like dividends or stock splits.

Data source: yfinance (free, no API key required, 20+ years history)
"""

from instruments_service.corporate_actions.adapter import CorporateActionsAdapter
from instruments_service.corporate_actions.models import (
    CorporateActionType,
    DividendRecord,
    DividendType,
    EarningsRecord,
    StockSplitRecord,
)

__all__ = [
    "CorporateActionType",
    "CorporateActionsAdapter",
    "DividendRecord",
    "DividendType",
    "EarningsRecord",
    "StockSplitRecord",
]
