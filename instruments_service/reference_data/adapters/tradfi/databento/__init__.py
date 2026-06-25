"""Databento reference data adapter — institutional market data provider.

Databento provides normalized historical and reference data for equities, futures, options.
API key required: store in Secret Manager as databento-api-key.

Fetches ONLY the curated instruments declared in UAC's TRADFI_DATABENTO_INSTRUMENTS
registry (futures/options via ``stype_in=parent``) plus S&P 500 / ETF equities from
TRADFI_TICKER_UNIVERSE (via ``stype_in=raw_symbol``).  This avoids dumping the entire
Databento dataset (millions of rows) and returns only the ~600 instruments the system
actually trades.

FX spot pairs (KRW/USD etc.) are created as static InstrumentRecords from
UAC's FX_SPOT_PAIRS — they don't come from Databento.

PACKAGE LAYOUT (split 2026-06-12 from the 1,222-line monolith; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``):
  - ``symbology.py`` — dataset/venue/asset-group mappings, spread legs, error codes
  - ``sessions.py``  — exchange calendar / trading-hours session enrichment
  - ``adapter.py``   — ``DatabentoReferenceDataAdapter``

The package keeps the original module's single-namespace semantics: every
shared symbol (including the ``db`` / ``xcals`` / ``pd`` module aliases) is an
attribute of THIS package module and submodules resolve collaborators through
``_pkg_ref.databento_namespace`` at call time, so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.tradfi.databento.<name>")``
targets behave exactly as before the split.
"""

# pyright: reportPrivateUsage=false, reportImportCycles=false

import contextlib as contextlib
import logging
from datetime import UTC as UTC
from datetime import date as date
from datetime import datetime as datetime
from datetime import timedelta as timedelta
from decimal import Decimal as Decimal
from zoneinfo import ZoneInfo as ZoneInfo

import databento as db
import exchange_calendars as xcals
import pandas as pd
from unified_api_contracts import (
    FX_SPOT_PAIRS,
    KNOWN_ETFS,
    TRADFI_DATABENTO_INSTRUMENTS,
    TRADFI_TICKER_UNIVERSE,
    CanonicalFuturesContract,
    FuturesContractLifecyclePhase,
    VenueMapping,
    classify_venue_error,
)
from unified_api_contracts.internal import AssetClass, InstrumentLeg, InstrumentRecord, InstrumentType, OptionType
from unified_trading_library import log_event

from ....base_adapter import BaseReferenceDataAdapter
from ....schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# ── cohesion submodules: import + re-export (public surface unchanged) ──────
from instruments_service.reference_data.adapters.tradfi.databento import adapter as adapter
from instruments_service.reference_data.adapters.tradfi.databento import sessions as sessions
from instruments_service.reference_data.adapters.tradfi.databento import symbology as symbology
from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
    DatabentoReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.tradfi.databento.sessions import (
    _EXCHANGE_HOURS,
    _FX_VENUES_24_7,
    _XCAL_CACHE,
    _XCAL_MAPPING,
    EXCHANGE_HOURS,
    UndeclaredTradfiVenueError,
    _apply_early_close,
    _compute_utc_hours,
    _get_session_metadata,
    _get_xcal,
    _is_trading_holiday,
    _non_trading_result,
    _resolve_trading_status,
    get_session_metadata,
    is_non_trading_day,
    non_trading_day_reason,
)
from instruments_service.reference_data.adapters.tradfi.databento.symbology import (
    _CLASS_TO_TYPE,
    _DATASET_TO_VENUE,
    _DEFAULT_TRADFI_FLOOR,
    _FUTURES_DATASETS,
    _SORTED_EXCHANGE_CODES,
    _VENUE_FLOOR_DATES,
    _VENUE_MAPPING,
    _classify_bento_error,
    _DATASET_TO_asset_group,
    _EXCHANGE_CODE_asset_group,
    _extract_underlying_from_symbol,
    _parse_cme_calendar_spread_legs,
    _resolve_product_root,
)

__all__ = [
    "EXCHANGE_HOURS",
    "FX_SPOT_PAIRS",
    "KNOWN_ETFS",
    "TRADFI_DATABENTO_INSTRUMENTS",
    "TRADFI_TICKER_UNIVERSE",
    "UTC",
    "_CLASS_TO_TYPE",
    "_DATASET_TO_VENUE",
    "_DEFAULT_TRADFI_FLOOR",
    "_EXCHANGE_HOURS",
    "_FUTURES_DATASETS",
    "_FX_VENUES_24_7",
    "_SORTED_EXCHANGE_CODES",
    "_VENUE_FLOOR_DATES",
    "_VENUE_MAPPING",
    "_XCAL_CACHE",
    "_XCAL_MAPPING",
    "AssetClass",
    "BaseReferenceDataAdapter",
    "CanonicalExpiryCalendar",
    "CanonicalFuturesContract",
    "CanonicalOptionsChain",
    "DatabentoReferenceDataAdapter",
    "Decimal",
    "FundingRateRef",
    "FuturesContractLifecyclePhase",
    "InstrumentLeg",
    "InstrumentRecord",
    "InstrumentType",
    "OHLCVRef",
    "OptionType",
    "UndeclaredTradfiVenueError",
    "VenueMapping",
    "ZoneInfo",
    "_DATASET_TO_asset_group",
    "_EXCHANGE_CODE_asset_group",
    "_apply_early_close",
    "_classify_bento_error",
    "_compute_utc_hours",
    "_extract_underlying_from_symbol",
    "_get_session_metadata",
    "_get_xcal",
    "_is_trading_holiday",
    "_non_trading_result",
    "_parse_cme_calendar_spread_legs",
    "_resolve_product_root",
    "_resolve_trading_status",
    "adapter",
    "classify_venue_error",
    "contextlib",
    "date",
    "datetime",
    "db",
    "get_session_metadata",
    "is_non_trading_day",
    "log_event",
    "logger",
    "logging",
    "non_trading_day_reason",
    "pd",
    "sessions",
    "symbology",
    "timedelta",
    "xcals",
]
