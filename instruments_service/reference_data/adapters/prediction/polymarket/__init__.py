"""Polymarket reference data adapter — prediction market instrument listing.

Polymarket Gamma API provides public market metadata without authentication.
Each active market is returned as an InstrumentRecord so services can treat
prediction markets as tradeable instruments alongside crypto and sports.

Base URL: https://gamma-api.polymarket.com
No auth required for market listing.

Supported endpoint:
  GET /markets?closed=false&active=true&limit=100&offset={n}

PACKAGE LAYOUT (split 2026-06-12 from the 1,184-line monolith; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``):
  - ``markets.py`` — question/team/keyword classification helpers + Gamma constants
  - ``clob.py``    — ``PolymarketClobMixin`` (CLOB price-history + market scan)
  - ``parsing.py`` — ``PolymarketParsingMixin`` (InstrumentRecord / lifecycle parsing)
  - ``adapter.py`` — ``PolymarketReferenceDataAdapter`` (mixin composition)

The package keeps the original module's single-namespace semantics: every
shared symbol is an attribute of THIS package module and submodules resolve
collaborators through ``_pkg_ref.polymarket_namespace`` at call time, so
``unittest.mock.patch(
"instruments_service.reference_data.adapters.prediction.polymarket.<name>")``
targets behave exactly as before the split.
"""

# pyright: reportPrivateUsage=false, reportImportCycles=false

import logging
import re as re
import time as time
from datetime import UTC as UTC
from datetime import datetime as datetime
from decimal import Decimal as Decimal
from typing import TYPE_CHECKING
from typing import ClassVar as ClassVar
from typing import cast as cast

if TYPE_CHECKING:
    import pandas as pd

import aiohttp as aiohttp
from unified_api_contracts import (
    PolymarketGammaMarket,
    classify_venue_error,
)
from unified_api_contracts.external.polymarket import (
    POLYMARKET_PREDICTION_LEAGUES,
    get_canonical_league_for_polymarket_series,
    get_canonical_team_for_polymarket,
)
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.prediction import (
    PredictionMarketMapper,
)
from unified_api_contracts.predictions import (
    CANONICAL_GROUP_METADATA,
    CanonicalQuestionGroup,
    MarketLifecycle,
    classify_polymarket_to_canonical_group,
)
from unified_api_contracts.sports import (
    POLYMARKET_MARKET_TO_CANONICAL,
    build_crypto_prediction_id,
    build_macro_prediction_id,
    build_prediction_instrument_id,
    slugify_canonical_name,
)
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
from instruments_service.reference_data.adapters.prediction.polymarket import adapter as adapter
from instruments_service.reference_data.adapters.prediction.polymarket import clob as clob
from instruments_service.reference_data.adapters.prediction.polymarket import markets as markets
from instruments_service.reference_data.adapters.prediction.polymarket import parsing as parsing
from instruments_service.reference_data.adapters.prediction.polymarket.adapter import (
    PolymarketReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.prediction.polymarket.clob import (
    PolymarketClobMixin,
)
from instruments_service.reference_data.adapters.prediction.polymarket.markets import (
    _CRYPTO_KEYWORDS,
    _CRYPTO_LONG_FORMS,
    _CRYPTO_SHORT_PATTERNS,
    _CRYPTO_SHORT_TICKERS,
    _GAMMA_BASE,
    _GENERIC_OUTCOMES,
    _MACRO_KEYWORDS,
    _MAPPER,
    _MAX_PAGES_ACTIVE,
    _PAGE_LIMIT,
    _QUESTION_SUFFIX,
    _SINGLE_TEAM_PATTERN,
    _VS_PATTERN,
    _classify_polymarket_error,
    _enrich_raw_event_fields,
    _extract_series_slug,
    _get_first_event,
    _match_crypto_asset,
    _match_macro_index,
    _normalize_team_pair,
    _parse_single_team_question,
    _parse_vs_string,
    _selection_from_outcomes,
)
from instruments_service.reference_data.adapters.prediction.polymarket.parsing import (
    PolymarketParsingMixin,
)

__all__ = [
    "CANONICAL_GROUP_METADATA",
    "POLYMARKET_MARKET_TO_CANONICAL",
    "POLYMARKET_PREDICTION_LEAGUES",
    "UTC",
    "_CRYPTO_KEYWORDS",
    "_CRYPTO_LONG_FORMS",
    "_CRYPTO_SHORT_PATTERNS",
    "_CRYPTO_SHORT_TICKERS",
    "_GAMMA_BASE",
    "_GENERIC_OUTCOMES",
    "_MACRO_KEYWORDS",
    "_MAPPER",
    "_MAX_PAGES_ACTIVE",
    "_PAGE_LIMIT",
    "_QUESTION_SUFFIX",
    "_SINGLE_TEAM_PATTERN",
    "_VS_PATTERN",
    "BaseReferenceDataAdapter",
    "CanonicalExpiryCalendar",
    "CanonicalOptionsChain",
    "CanonicalQuestionGroup",
    "ClassVar",
    "Decimal",
    "FundingRateRef",
    "InstrumentRecord",
    "MarketLifecycle",
    "OHLCVRef",
    "PolymarketClobMixin",
    "PolymarketGammaMarket",
    "PolymarketParsingMixin",
    "PolymarketReferenceDataAdapter",
    "PredictionMarketMapper",
    "_classify_polymarket_error",
    "_enrich_raw_event_fields",
    "_extract_series_slug",
    "_get_first_event",
    "_match_crypto_asset",
    "_match_macro_index",
    "_normalize_team_pair",
    "_parse_single_team_question",
    "_parse_vs_string",
    "_selection_from_outcomes",
    "adapter",
    "aiohttp",
    "build_crypto_prediction_id",
    "build_macro_prediction_id",
    "build_prediction_instrument_id",
    "cast",
    "classify_polymarket_to_canonical_group",
    "classify_venue_error",
    "clob",
    "datetime",
    "get_canonical_league_for_polymarket_series",
    "get_canonical_team_for_polymarket",
    "log_event",
    "logger",
    "logging",
    "markets",
    "parsing",
    "re",
    "slugify_canonical_name",
    "time",
]
