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

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from unified_api_contracts import TRADFI_DATABENTO_INSTRUMENTS, VenueMapping
from unified_api_contracts.internal import AssetClass, InstrumentLeg, InstrumentType
from unified_api_contracts.registry import EXCHANGE_CODE_TO_NAME

if TYPE_CHECKING:
    from instruments_service.reference_data.adapters.tradfi import databento as _db
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.tradfi.databento._pkg_ref import databento_namespace as _db

logger = logging.getLogger(__name__)

# Hard cap on real combo/spread leg count (operator spec, 2026-07-09 —
# canonical_id_p1_tradfi_combo_leg_canonicalization_2026_07_08.md): 1-4 legs
# supported; a real combo with 5+ legs is DROPPED (not captured, not
# truncated) — logged with the real leg count rather than silently lost.
# Covers every real combo shape confirmed in the 2026-07-08 catalog audit
# (2-leg calendar spreads, 3-leg butterflies) with headroom.
_MAX_COMBO_LEGS = 4

__all__ = [
    "_CLASS_TO_TYPE",
    "_DATASET_TO_VENUE",
    "_DEFAULT_TRADFI_FLOOR",
    "_EXCHANGE_CODE_TO_PRODUCT_ROOT",
    "_FUTURES_DATASETS",
    "_SORTED_EXCHANGE_CODES",
    "_SPREAD_LEG_PARSERS",
    "_VENUE_FLOOR_DATES",
    "_VENUE_MAPPING",
    "_DATASET_TO_asset_group",
    "_EXCHANGE_CODE_asset_group",
    "_build_leg_key",
    "_classify_bento_error",
    "_extract_underlying_from_symbol",
    "_parse_cboe_spread_legs",
    "_parse_cme_calendar_spread_legs",
    "_resolve_product_root",
    "_sanitize_symbol_for_key",
]

# Databento instrument_class → canonical InstrumentType.
# 2026-07-08 fix: "K" was mapped to SPOT_PAIR ("Forex spot") — WRONG. Confirmed via
# the real ``databento`` SDK's ``InstrumentClass`` enum (``InstrumentClass.STOCK.value
# == "K"``) AND a live Databento definition-schema call for AAPL/SPY/IBIT on
# DBEQ.BASIC (2026-07-08) — all 3 real securities return instrument_class="K". This
# was the real, live, ongoing root cause of "224 securities double-keyed as EQUITY
# and SPOT_PAIR" (finding, instrument_id_format_canonicalization_2026_07_08.md):
# every DBEQ.BASIC single-stock/ETF row defaults through this map, so 100% of
# fresh NASDAQ/NYSE equity captures were mistyped SPOT_PAIR (verified: the
# 2026-07-08 by_date snapshot showed 100/100 NASDAQ rows as SPOT_PAIR, zero EQUITY).
# The G1.d "class S → EQUITY" override (below) is UNRELATED and unaffected — a
# separate, smaller population of DBEQ.BASIC rows that happen to arrive tagged "S".
# ETFs are unaffected by this fix: the downstream ``raw_symbol in KNOWN_ETFS`` check
# (adapter.py) reclassifies them ETF regardless of this map's EQUITY/SPOT_PAIR value.
_CLASS_TO_TYPE: dict[str, InstrumentType] = {
    "B": InstrumentType.SPOT_PAIR,  # Bond
    "C": InstrumentType.OPTION,  # Call option (CME/ICE)
    "E": InstrumentType.EQUITY,  # Equity
    "F": InstrumentType.FUTURE,  # Future
    "K": InstrumentType.EQUITY,  # Stock (real Databento InstrumentClass.STOCK == "K")
    "M": InstrumentType.FUTURE,  # Monthly future (CME)
    "N": InstrumentType.ETF,  # ETF / Fund / Index
    "O": InstrumentType.OPTION,  # Option (generic)
    "P": InstrumentType.OPTION,  # Put option (CME/ICE)
    "S": InstrumentType.SPOT_PAIR,  # FX Spot / Equity spot
    "T": InstrumentType.COMBO,  # Exchange-defined spread / combo
    "X": InstrumentType.SPOT_PAIR,  # Index
}

# Dataset → canonical venue mapping.
# §7.1 BILLABLE-VENUE GUARD (operator 2026-06-18 subscription lockdown + 2026-06-25 cleanup):
# ONLY the 3 subscribed Databento datasets are mapped. The non-billable datasets
# (XNAS.ITCH / XNAS.BASIC / XNYS.PILLAR / IFEU.IMPACT / IFUS.IMPACT / OPRA.PILLAR) are
# REMOVED so a stray off-allowlist definition row can never resolve to a (polluting) venue —
# they were the source of the stale ICE-Brent (IFEU/IFUS→ICE) + CBOE-OPRA-SPX (OPRA→CBOE)
# catalogue pollution. ICE is databento-billing-blocked → purged everywhere; the Yahoo-sourced
# DXY currency index re-homes to venue=FX (NOT ICE). Enumeration is curated-list-driven
# (TRADFI_DATABENTO_INSTRUMENTS, all billable) and the fetch path asserts
# assert_databento_request_allowed; this map is the last-mile canonical-venue resolve.
_DATASET_TO_VENUE: dict[str, str] = {
    "GLBX.MDP3": "CME",
    "DBEQ.BASIC": "NYSE",  # equity dataset; adapter re-routes NASDAQ vs NYSE by ticker
    "XCBF.PITCH": "CBOE",  # Cboe Futures Exchange — VX/VIX futures
}

# Dataset → fallback asset class (used when no per-instrument match exists).
# Billable datasets only (see _DATASET_TO_VENUE). XCBF.PITCH = COMMODITY (VX/VIX volatility
# futures — was the mis-classified EQUITY fallback; G1.c 2026-06-25).
_DATASET_TO_asset_group: dict[str, AssetClass] = {
    "GLBX.MDP3": AssetClass.COMMODITY,
    "DBEQ.BASIC": AssetClass.EQUITY,
    "XCBF.PITCH": AssetClass.COMMODITY,
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

# Futures datasets where class "S" = exchange-defined calendar spread → COMBO (not equity spot).
# GLBX.MDP3 (CME) + XCBF.PITCH (CBOE/VX) — IFEU/IFUS removed (ICE billing-blocked, purged).
# 2026-07-08 canonicalization fix: XCBF.PITCH class-"S" VX calendar spreads used to be dropped
# entirely (G1.c, "outright-only universe") because the raw rows were landing mis-typed as
# SPOT_PAIR with a whitespace-padded-dash leg separator — the real fix is to DECOMPOSE them via
# _parse_cboe_spread_legs (below), not to drop them; the outright-only universe was a workaround,
# not the intended target state. DBEQ.BASIC class-"S" is equity-spot → EQUITY (G1.d), unrelated.
_FUTURES_DATASETS = frozenset({"GLBX.MDP3", "XCBF.PITCH"})

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
    """Derive underlying (parent/root exchange code) from a futures/option raw symbol.

    Matches the longest registered exchange_code that is a prefix of raw_symbol.
    E.g. "ESH6" → "ES", "6MJ6" → "6M", "CLZ26" → "CL". Spaced option contract
    codes resolve on the leading futures-style token, e.g. "E5AH0 C2510" → "E5A".
    """
    for code in _SORTED_EXCHANGE_CODES:
        if raw_symbol.startswith(code) and len(raw_symbol) > len(code):
            return code
    return ""


# Exchange code → human-canonical product root (e.g. "ES" → "SP500", "E5A" →
# "SP500"). Built from the UAC ``EXCHANGE_CODE_TO_NAME`` registry plus every
# curated ``DatabentoInstrumentDef.exchange_code`` → ``.base_asset`` (which
# covers the daily/weekly option roots E1A..E5A / EW1.. that the display map
# omits). No Databento API call — purely the existing static registry.
_EXCHANGE_CODE_TO_PRODUCT_ROOT: dict[str, str] = dict(EXCHANGE_CODE_TO_NAME)
for _inst in TRADFI_DATABENTO_INSTRUMENTS:
    if _inst.exchange_code:
        _EXCHANGE_CODE_TO_PRODUCT_ROOT.setdefault(_inst.exchange_code, _inst.base_asset)


def _resolve_product_root(raw_symbol: str) -> str | None:
    """Resolve the human-canonical product root from a raw exchange contract code.

    Extracts the registered exchange-code prefix (incl. spaced options like
    "E5AH0 C2510" → "E5A") then maps it to the human product root via the UAC
    registry (e.g. "ES" → "SP500"). Returns ``None`` when no mapping resolves.
    """
    if not raw_symbol:
        return None
    # Direct hit (raw_symbol IS an exchange code, e.g. parent-stype "ES").
    root = _EXCHANGE_CODE_TO_PRODUCT_ROOT.get(raw_symbol)
    if root:
        return root
    code = _extract_underlying_from_symbol(raw_symbol)
    if code:
        return _EXCHANGE_CODE_TO_PRODUCT_ROOT.get(code)
    return None


# Matches a whitespace-padded dash (the combo/spread inter-leg separator,
# e.g. " - " in "VX/F1:1:S - VX/G1:1:B") so it collapses to a single "-"
# instead of leaving stray dashes once the surrounding whitespace is also
# replaced by _sanitize_symbol_for_key.
_WS_PADDED_DASH_RE = re.compile(r"\s*-\s*")
_WHITESPACE_RUN_RE = re.compile(r"\s+")


def _sanitize_symbol_for_key(raw_symbol: str) -> str:
    """Replace whitespace inside a raw_symbol with ``-`` for canonical-key use.

    Whitespace is never an acceptable delimiter inside a canonical
    ``instrument_id``/``instrument_key`` (operator-decided workspace-wide rule,
    2026-07-08) — real Databento raw_symbols embed a literal space for several
    TradFi shapes: CME/ICE option contracts (``"OBZ4 C15500"``), some
    user-defined strategy futures (``"CL:SA 02M M6"``), and a handful of
    equity share-class tickers (``"BRK B"``). This is additive/cosmetic on the
    CANONICAL key only — ``raw_symbol`` itself is never modified, it stays the
    verbatim vendor code (needed for exact-match lookups against Databento).
    A pre-existing whitespace-padded dash (the combo inter-leg separator, e.g.
    ``" - "``) collapses to one dash rather than leaving a run of dashes.
    """
    collapsed = _WS_PADDED_DASH_RE.sub("-", raw_symbol)
    return _WHITESPACE_RUN_RE.sub("-", collapsed).strip("-")


def _build_leg_key(leg_type: InstrumentType, raw_symbol: str) -> str:
    """Build a combo leg's ``instrument_key`` — ``TYPE:SYMBOL`` only, no venue.

    The venue is already carried once at the combo's own top-level
    ``VENUE:COMBO:...`` id, so repeating it per leg is redundant (2026-07-08
    canonicalization decision — same "no redundant venue" call already made
    for the position-id margin-marker ``@LIN``/``@INV`` suffix). ``SYMBOL`` is
    the human-canonical product root resolved via :func:`_resolve_product_root`
    (e.g. ``VIX``, ``SP500``) — falls back to the raw exchange ticker only when
    no product-root mapping exists, so a leg never silently loses identity for
    an unmapped root.
    """
    symbol = _resolve_product_root(raw_symbol) or raw_symbol
    return f"{leg_type}:{symbol}"


def _parse_cme_calendar_spread_legs(raw_symbol: str) -> list[InstrumentLeg] | None:
    """Parse CME exchange-defined calendar spread legs from raw_symbol.

    Format: ``ROOTMONTHYEAR-ROOTMONTHYEAR`` (e.g. ``ESM6-ESU6``, ``CLZ26-CLF27``).
    Returns [BUY front_leg, SELL back_leg] or None if unparseable. Leg keys are
    venue-free human names (``FUTURE:SP500``, not ``CME:FUTURE:ESM6``) via
    :func:`_build_leg_key`.
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
        InstrumentLeg(instrument_key=_build_leg_key(InstrumentType.FUTURE, front), side="BUY", ratio=1),
        InstrumentLeg(instrument_key=_build_leg_key(InstrumentType.FUTURE, back), side="SELL", ratio=1),
    ]


# CBOE/XCBF.PITCH raw-symbol leg-side codes: "S" (sell) / "B" (buy).
_CBOE_LEG_SIDE: dict[str, str] = {"S": "SELL", "B": "BUY"}


def _parse_cboe_spread_legs(raw_symbol: str) -> list[InstrumentLeg] | None:
    """Parse CBOE/XCBF.PITCH exchange-defined spread legs from raw_symbol.

    Format: ``TICKER:RATIO:SIDE`` per leg, legs joined by ``" - "`` (a real,
    confirmed shape — e.g. ``VX/F1:1:S - VX/G1:1:B`` for a 2-leg calendar
    spread, ``VX/H1:1:B - VX/J1:2:S - VX/K1:1:B`` for a 3-leg butterfly).
    Unlike CME's concatenated-ticker form (which has no embedded side/ratio
    and always assumes BUY-front/SELL-back), CBOE's raw_symbol carries an
    explicit ratio + side per leg, so no leg-count special-casing is needed
    for the parse itself — an arbitrary number of ``" - "``-joined legs
    parses the same way, subject to the ``_MAX_COMBO_LEGS`` hard cap below.
    Returns ``None`` if any leg segment fails to parse (wrong field count, a
    non-numeric ratio, a side outside ``{S, B}``, or a ticker that doesn't
    resolve to a registered underlying), or if the real leg count is outside
    the operator-decided 1-4 range — the caller drops the combo entirely
    rather than emit a partially-decomposed spread or a truncated one.
    """
    leg_strs = raw_symbol.split(" - ")
    if len(leg_strs) < 2:
        return None
    if len(leg_strs) > _MAX_COMBO_LEGS:
        logger.warning(
            "dropping CBOE/XCBF.PITCH combo with %d legs (hard cap is %d, real symbol=%r)",
            len(leg_strs),
            _MAX_COMBO_LEGS,
            raw_symbol,
        )
        return None
    legs: list[InstrumentLeg] = []
    for leg_str in leg_strs:
        fields = leg_str.strip().split(":")
        if len(fields) != 3:
            return None
        ticker, ratio_raw, side_raw = (f.strip() for f in fields)
        side = _CBOE_LEG_SIDE.get(side_raw.upper())
        if not ticker or side is None:
            return None
        try:
            ratio = int(ratio_raw)
        except ValueError:
            return None
        if ratio <= 0:
            return None
        if not _db._extract_underlying_from_symbol(ticker):
            return None
        legs.append(InstrumentLeg(instrument_key=_build_leg_key(InstrumentType.FUTURE, ticker), side=side, ratio=ratio))
    return legs


# Dataset → spread-leg parser. Both CME and CBOE/VX exchange-defined class-"S"
# spreads route through here (see _FUTURES_DATASETS) — the raw_symbol SHAPES
# differ per venue (CME: concatenated ticker, no side/ratio; CBOE: colon-
# delimited ticker:ratio:side), so each dataset gets its own parser rather
# than one regex trying to cover both.
_SPREAD_LEG_PARSERS: dict[str, Callable[[str], list[InstrumentLeg] | None]] = {
    "GLBX.MDP3": _parse_cme_calendar_spread_legs,
    "XCBF.PITCH": _parse_cboe_spread_legs,
}


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
