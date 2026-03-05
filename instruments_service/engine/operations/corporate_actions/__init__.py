"""
Corporate Actions Operations

Provides adapters, models, and utilities for fetching and processing
corporate actions data (dividends, splits, earnings) for TRADFI equities.
"""

from instruments_service.engine.operations.corporate_actions.adapter import CorporateActionsAdapter
from instruments_service.engine.operations.corporate_actions.models import (
    CorporateActionsBundle,
    CorporateActionType,
    DividendRecord,
    DividendType,
    EarningsRecord,
    StockSplitRecord,
)

__all__ = [
    "CorporateActionType",
    "CorporateActionsAdapter",
    "CorporateActionsBundle",
    "DividendRecord",
    "DividendType",
    "EarningsRecord",
    "StockSplitRecord",
]
