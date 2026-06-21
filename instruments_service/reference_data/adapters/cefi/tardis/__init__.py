"""Tardis reference data adapter — historical tick data provider.

Tardis provides historical trades/orderbook data for crypto derivatives.
This adapter retrieves instrument metadata via the public exchanges REST endpoint.
API key optional for public instrument listing; required for higher rate limits.

API key: store in Secret Manager as TARDIS_API_KEY.
Auth: Authorization: Bearer {api_key} (header).
Base URL: https://api.tardis.dev/v1

Supported exchanges (configurable):
  binance-futures, bybit, okex, deribit

Not applicable: crypto funding rates (historical; use tardis-client library),
OHLCV bars (requires /replay endpoint — out of scope for URDI REST adapter).

PACKAGE LAYOUT (split 2026-06-12 from the 1,348-line monolith; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``):
  - ``parsing.py`` — symbol / expiry / quote / margin parsing helpers
  - ``combos.py``  — Deribit combo/spread leg parser
  - ``adapter.py`` — ``TardisReferenceDataAdapter`` + venue universe constants

The package keeps the original module's single-namespace semantics: every
shared symbol is an attribute of THIS package module and submodules resolve
collaborators through ``_pkg_ref.tardis_namespace`` at call time, so
``unittest.mock.patch("instruments_service.reference_data.adapters.cefi.tardis.<name>")``
targets behave exactly as before the split.
"""

# pyright: reportPrivateUsage=false, reportImportCycles=false

import asyncio as asyncio
import contextlib as contextlib
import json as json
import logging
import re as re
import time as time
from datetime import UTC as UTC
from datetime import datetime as datetime
from decimal import Decimal as Decimal
from typing import cast as cast

import aiohttp as aiohttp
from unified_api_contracts import (
    CEFI_ACCEPTED_QUOTE_ASSETS,
    CEFI_BASE_ASSET_UNIVERSE,
    CEFI_OPTIONS_UNDERLYINGS,
    TardisExchangeDetail,
    TardisInstrumentDetail,
    VenueMapping,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentLeg, InstrumentRecord, InstrumentType, MarginType, OptionType
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
from instruments_service.reference_data.adapters.cefi.tardis import adapter as adapter
from instruments_service.reference_data.adapters.cefi.tardis import combos as combos
from instruments_service.reference_data.adapters.cefi.tardis import parsing as parsing
from instruments_service.reference_data.adapters.cefi.tardis.adapter import (
    _DEFAULT_EXCHANGES,
    _DERIVATIVES_ONLY_EXCHANGES,
    _TARDIS_BASE,
    _TARDIS_RETRY_ATTEMPTS,
    _TARDIS_RETRY_BASE_DELAY,
    _TARDIS_RETRYABLE_CODES,
    _TYPE_MAP,
    _VENUE_MAPPING,
    TardisReferenceDataAdapter,
    _classify_tardis_error,
)
from instruments_service.reference_data.adapters.cefi.tardis.combos import (
    _DERIBIT_COMBO_STRUCTURES,
    _DERIBIT_DUAL_EXPIRY_CODES,
    _parse_deribit_combo_legs,
)
from instruments_service.reference_data.adapters.cefi.tardis.parsing import (
    _DERIBIT_MONTHS,
    _QUOTE_CURRENCIES,
    _QUOTE_CURRENCIES_SET,
    _infer_derivative_quote,
    _infer_margin_type,
    _normalize_option_type,
    _parse_ddmmmyy,
    _parse_deribit_symbol_expiry,
    _parse_expiry,
    _parse_underscore_yymmdd_symbol_expiry,
    _parse_yymmdd_symbol_expiry,
    _passes_asset_filter,
    _resolve_base_quote,
    _resolve_option_fields,
    _split_kraken_symbol,
    _split_symbol,
)

__all__ = [
    "CEFI_ACCEPTED_QUOTE_ASSETS",
    "CEFI_BASE_ASSET_UNIVERSE",
    "CEFI_OPTIONS_UNDERLYINGS",
    "UTC",
    "_DEFAULT_EXCHANGES",
    "_DERIBIT_COMBO_STRUCTURES",
    "_DERIBIT_DUAL_EXPIRY_CODES",
    "_DERIBIT_MONTHS",
    "_DERIVATIVES_ONLY_EXCHANGES",
    "_QUOTE_CURRENCIES",
    "_QUOTE_CURRENCIES_SET",
    "_TARDIS_BASE",
    "_TARDIS_RETRYABLE_CODES",
    "_TARDIS_RETRY_ATTEMPTS",
    "_TARDIS_RETRY_BASE_DELAY",
    "_TYPE_MAP",
    "_VENUE_MAPPING",
    "BaseReferenceDataAdapter",
    "CanonicalExpiryCalendar",
    "CanonicalOptionsChain",
    "Decimal",
    "FundingRateRef",
    "InstrumentLeg",
    "InstrumentRecord",
    "InstrumentType",
    "MarginType",
    "OHLCVRef",
    "OptionType",
    "TardisExchangeDetail",
    "TardisInstrumentDetail",
    "TardisReferenceDataAdapter",
    "VenueMapping",
    "_classify_tardis_error",
    "_infer_derivative_quote",
    "_infer_margin_type",
    "_normalize_option_type",
    "_parse_ddmmmyy",
    "_parse_deribit_combo_legs",
    "_parse_deribit_symbol_expiry",
    "_parse_expiry",
    "_parse_underscore_yymmdd_symbol_expiry",
    "_parse_yymmdd_symbol_expiry",
    "_passes_asset_filter",
    "_resolve_base_quote",
    "_resolve_option_fields",
    "_split_kraken_symbol",
    "_split_symbol",
    "adapter",
    "aiohttp",
    "asyncio",
    "cast",
    "classify_venue_error",
    "combos",
    "contextlib",
    "datetime",
    "json",
    "log_event",
    "logger",
    "logging",
    "parsing",
    "re",
    "time",
]
