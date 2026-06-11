"""Databento dataset / symbol mappings + spread-leg and error classification.

Cohesion module of the ``adapters.tradfi.databento`` package (split from the
former monolithic ``adapters/tradfi/databento.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators resolve through ``_db`` — the live package namespace — so
``unittest.mock.patch("instruments_service.reference_data.adapters.tradfi.databento.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split.
"""

# Package-internal access: the databento package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from unified_api_contracts import TRADFI_DATABENTO_INSTRUMENTS, VenueMapping
from unified_api_contracts.internal import AssetClass, InstrumentLeg, InstrumentType

if TYPE_CHECKING:
    from instruments_service.reference_data.adapters.tradfi import databento as _db
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.tradfi.databento._pkg_ref import databento_namespace as _db

__all__ = [
    "_CLASS_TO_TYPE",
    "_DATASET_TO_VENUE",
    "_DEFAULT_TRADFI_FLOOR",
    "_FUTURES_DATASETS",
    "_SORTED_EXCHANGE_CODES",
    "_VENUE_FLOOR_DATES",
    "_VENUE_MAPPING",
    "_DATASET_TO_asset_group",
    "_EXCHANGE_CODE_asset_group",
    "_classify_bento_error",
    "_extract_underlying_from_symbol",
    "_parse_cme_calendar_spread_legs",
]

# Databento instrument_class → canonical InstrumentType
_CLASS_TO_TYPE: dict[str, InstrumentType] = {
    "B": InstrumentType.SPOT_PAIR,  # Bond
    "C": InstrumentType.OPTION,  # Call option (CME/ICE)
    "E": InstrumentType.EQUITY,  # Equity
    "F": InstrumentType.FUTURE,  # Future
    "K": InstrumentType.SPOT_PAIR,  # Forex spot
    "M": InstrumentType.FUTURE,  # Monthly future (CME)
    "N": InstrumentType.ETF,  # ETF / Fund / Index
    "O": InstrumentType.OPTION,  # Option (generic)
    "P": InstrumentType.OPTION,  # Put option (CME/ICE)
    "S": InstrumentType.SPOT_PAIR,  # FX Spot / Equity spot
    "T": InstrumentType.COMBO,  # Exchange-defined spread / combo
    "X": InstrumentType.SPOT_PAIR,  # Index
}

# Dataset → canonical venue mapping
_DATASET_TO_VENUE: dict[str, str] = {
    "GLBX.MDP3": "CME",
    "XNAS.ITCH": "NASDAQ",
    "XNAS.BASIC": "NASDAQ",
    "XNYS.PILLAR": "NYSE",
    "DBEQ.BASIC": "NYSE",
    "IFEU.IMPACT": "ICE",
    "IFUS.IMPACT": "ICE",
    "OPRA.PILLAR": "CBOE",
    "XCBF.PITCH": "CBOE",
}

# Dataset → fallback asset class (used when no per-instrument match exists)
_DATASET_TO_asset_group: dict[str, AssetClass] = {
    "GLBX.MDP3": AssetClass.COMMODITY,
    "XNAS.ITCH": AssetClass.EQUITY,
    "XNAS.BASIC": AssetClass.EQUITY,
    "XNYS.PILLAR": AssetClass.EQUITY,
    "DBEQ.BASIC": AssetClass.EQUITY,
    "IFEU.IMPACT": AssetClass.COMMODITY,
    "IFUS.IMPACT": AssetClass.COMMODITY,
    "OPRA.PILLAR": AssetClass.EQUITY,
    "XCBF.PITCH": AssetClass.EQUITY,
}

# Per-instrument asset class from UAC registry.
# Maps exchange_code → asset_group (e.g. "ES" → equity, "CL" → commodity).
# Build from both exchange_code AND the root symbol (part before ".FUT"/".OPT")
# so that commodities like GC, CL, SI (which have exchange_code=None) are included.
_EXCHANGE_CODE_asset_group: dict[str, str] = {}
for _inst in TRADFI_DATABENTO_INSTRUMENTS:
    if _inst.exchange_code:
        _EXCHANGE_CODE_asset_group[_inst.exchange_code] = _inst.asset_group
    # Also register the root symbol (e.g. "GC" from "GC.FUT", "BRN" from "BRN.FUT")
    _root = _inst.symbol.split(".")[0] if "." in _inst.symbol else ""
    if _root and _root not in _EXCHANGE_CODE_asset_group:
        _EXCHANGE_CODE_asset_group[_root] = _inst.asset_group

_VENUE_MAPPING = VenueMapping()

# Futures datasets where class "S" = exchange-defined calendar spread (not equity spot).
_FUTURES_DATASETS = frozenset({"GLBX.MDP3", "IFEU.IMPACT", "IFUS.IMPACT"})

# Floor dates for venues where Databento doesn't populate the activation field.
# These are conservative "data available from" dates based on Databento coverage.
_DEFAULT_TRADFI_FLOOR = datetime(2020, 1, 1, tzinfo=UTC)
_VENUE_FLOOR_DATES: dict[str, datetime] = {
    "CME": datetime(2010, 1, 1, tzinfo=UTC),
    "ICE": datetime(2015, 1, 1, tzinfo=UTC),
    "NASDAQ": datetime(2015, 1, 1, tzinfo=UTC),
    "NYSE": datetime(2015, 1, 1, tzinfo=UTC),
    "CBOE": datetime(2015, 1, 1, tzinfo=UTC),
    "FX": datetime(2020, 1, 1, tzinfo=UTC),
}

# Sorted exchange codes longest-first for greedy prefix matching.
_SORTED_EXCHANGE_CODES: list[str] = sorted(_EXCHANGE_CODE_asset_group.keys(), key=len, reverse=True)


def _extract_underlying_from_symbol(raw_symbol: str) -> str:
    """Derive underlying (parent/root symbol) from a futures raw symbol.

    Matches the longest registered exchange_code that is a prefix of raw_symbol.
    E.g. "ESH6" → "ES", "6MJ6" → "6M", "CLZ26" → "CL".
    """
    for code in _SORTED_EXCHANGE_CODES:
        if raw_symbol.startswith(code) and len(raw_symbol) > len(code):
            return code
    return ""


def _parse_cme_calendar_spread_legs(raw_symbol: str, venue: str) -> list[InstrumentLeg] | None:
    """Parse CME exchange-defined calendar spread legs from raw_symbol.

    Format: ``ROOTMONTHYEAR-ROOTMONTHYEAR`` (e.g. ``ESM6-ESU6``, ``CLZ26-CLF27``).
    Returns [BUY front_leg, SELL back_leg] or None if unparseable.
    """
    parts = raw_symbol.split("-")
    if len(parts) != 2:
        return None
    front, back = parts[0].strip(), parts[1].strip()
    if not front or not back:
        return None
    # Both legs must resolve to a known underlying (registered exchange code)
    front_und = _db._extract_underlying_from_symbol(front)
    back_und = _db._extract_underlying_from_symbol(back)
    if not front_und or not back_und:
        return None
    return [
        InstrumentLeg(instrument_key=f"{venue}:FUTURE:{front}", side="BUY", ratio=1),
        InstrumentLeg(instrument_key=f"{venue}:FUTURE:{back}", side="SELL", ratio=1),
    ]


def _classify_bento_error(exc: Exception) -> str:
    """Map a Databento SDK error to a UAC error code for classification.

    ``exc`` is a ``db.common.error.BentoError`` at every call site; the
    parameter is typed ``Exception`` because the SDK module is untyped
    (``db.common`` is opaque to basedpyright) and the body only needs
    ``str(exc)``.
    """
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "401" in msg or "auth" in msg or "unauthorized" in msg:
        return "AUTH_FAILURE"
    if "connection" in msg or "reset" in msg or "timeout" in msg:
        return "CONNECTION_RESET"
    if "422" in msg:
        return "VALIDATION_ERROR"
    if "404" in msg or "not found" in msg:
        return "NOT_FOUND"
    if "500" in msg or "internal" in msg:
        return "SERVER_ERROR"
    return "UNKNOWN"
