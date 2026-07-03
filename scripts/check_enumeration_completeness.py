# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-1 enumeration-completeness check for Honest Coverage v2.

For each asset_group, builds the EXPECTED matrix of (venue, instrument_type,
data_type) tuples purely from UAC authorities, compares it to the ENUMERATED
matrix (distinct tuples present in the manifest skeleton), and returns per-node
completeness metrics including missing_tuples (Layer-1 holes) and stray_tuples
(tuples in ENUMERATED but not EXPECTED — warnings, not holes).

This is the CK2 implementation per the SSOT:
  codex/02-data/honest-coverage-model.md § Layer-1 enumeration-completeness matrix

Authorities used (exact UAC symbols):
  - valid_data_types_for_venue_instrument_type(ag, venue, itype) — DeFi protocol-grain (narrowed)
  - valid_data_types_for_instrument_type(ag, itype) — cefi/tradfi/sports/prediction + defi union
  - VENUE_DATA_TYPE_CAPABILITIES[venue][data_type] — venue capability gate (cefi/tradfi only)
  - get_mvp_data_types_for_cefi_venue(venue) — cefi per-venue MVP override (data_type set)
  - FUTURE_BUNDLE_VENUES — bundle grain for options_chain/futures_chain
  - bundle_instrument_type_for_leaf(ag, itype, venue) — leaf→bundle roll-up
  - VENUES_BY_ASSET_GROUP[ag] — the set of venues tracked for each AG (implicit MVP gate)

CRITICAL: expected data_types come from the UAC FUNCTIONS, NOT the raw
VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE dict.  That dict has ZERO defi keys
(defi validity is built dynamically from PROTOCOL_CAPABILITIES).  Indexing the
dict directly returns frozenset() for every defi tuple → EXPECTED=0 → false
"100% complete" (the empty-denominator failure mode — see Bug 1, 2026-06-29).

VENUE_CAPABILITY_AGS restricts the VENUE_DATA_TYPE_CAPABILITIES skip-filter to
cefi/tradfi only.  DeFi/sports/prediction capability is already encoded in their
protocol/league validity functions; that table is keyed by cefi/tradfi venues.

Note on MVP gate:
  is_mvp() requires a base_ccy to return True for CeFi/TradFi (it is an
  instrument-grain helper, not a schema-grain helper).  For the schema-level
  EXPECTED matrix we instead use:
    - CeFi: get_mvp_data_types_for_cefi_venue(venue) — per-venue data_type set
    - TradFi: VENUES_BY_ASSET_GROUP contains only MVP tradfi venues (e.g. CME)
    - DeFi/Sports/Prediction: validity functions handle scope already

Carve-outs (sourced from UAC, NOT hardcoded):
  1. Venue cannot produce data_type → absent from VENUE_DATA_TYPE_CAPABILITIES[venue]
     (cefi/tradfi only; not applied to defi/sports/prediction)
  2. Out of CeFi-MVP scope → get_mvp_data_types_for_cefi_venue(venue) does not include dt
  3. Bundle roll-up grain → leaf OPTION/FUTURE for FUTURE_BUNDLE_VENUES venues
     yields options_chain/futures_chain bundles, not per-leg rows
  4. frozenset() from validity functions → no rows for this (ag, itype) combination

EMPTY-DENOMINATOR GUARD (HARD RULE, 2026-06-29):
  When EXPECTED == 0 for an AG, denominator_status = "UNDEFINED",
  denominator_complete = False, completeness_pct = None.  Reporting 100% over an
  empty set reproduces the v1 dishonesty v2 exists to kill.

Returns:
  Layer1Result with per-AG + per-venue breakdown.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd
from unified_api_contracts import (
    VENUES_BY_ASSET_GROUP,
    bundle_instrument_type_for_leaf,
    get_mvp_data_types_for_cefi_venue,
)
from unified_api_contracts.registry import TRADFI_VENUE_INSTRUMENT_TYPES
from unified_api_contracts.registry.market_data_categories import (
    VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE,
    VENUE_DATA_TYPE_CAPABILITIES,
    valid_data_types_for_instrument_type,
    valid_data_types_for_venue_instrument_type,
)
from unified_api_contracts.registry.venue_mapping import VenueMapping

logger = logging.getLogger(__name__)

# AGs for which VENUE_DATA_TYPE_CAPABILITIES applies as a skip-filter.
# DeFi/sports/prediction capability is encoded in their validity functions;
# the VENUE_DATA_TYPE_CAPABILITIES dict is keyed by cefi/tradfi venues only.
VENUE_CAPABILITY_AGS: frozenset[str] = frozenset({"cefi", "tradfi"})

# ---------------------------------------------------------------------------
# VOCABULARY/GRAIN ALIGNMENT (HARD RULE, codex honest-coverage-model.md
# § Layer-1 enumeration-completeness matrix).
#
# EXPECTED is built in UAC vocabulary; ENUMERATED is the manifest's *written*
# vocabulary. They diverge on three axes — casing, instrument_type vocabulary,
# and venue format — so an un-normalised intersection collapses to artificial
# 0%/low completeness. The check intersects on a CANONICAL COMPARISON KEY
# derived identically from both sides; the original (display) tuples are kept
# for the missing/stray lists, but the membership test uses the canonical key.
#
# The normalisers REUSE the SSOT helpers the enumerator uses (not ad-hoc maps):
#   - instrument_type: UAC _INSTRUMENT_TYPE_ALIASES + bundle_instrument_type_for_leaf
#     (the same pair _canonical_writer_instrument_type / _rollup_bundle_grain use)
#   - venue (defi): VenueMapping._canonicalise_defi_protocol_spelling + chain strip
#     (the same canonicalisation _enumerate_v2_defi applies)
# ---------------------------------------------------------------------------

# Lazily imported from UAC (the SSOT alias map the enumerator uses).
_INSTRUMENT_TYPE_ALIASES: dict[str, str] | None = None


def _get_instrument_type_aliases() -> dict[str, str]:
    """Return the UAC SSOT instrument_type alias map (lowercase token → canonical)."""
    global _INSTRUMENT_TYPE_ALIASES
    if _INSTRUMENT_TYPE_ALIASES is None:
        from unified_api_contracts.registry.market_data_categories import (  # noqa: TID251
            _INSTRUMENT_TYPE_ALIASES as _aliases,
        )

        _INSTRUMENT_TYPE_ALIASES = dict(_aliases)
    return _INSTRUMENT_TYPE_ALIASES


# DeFi fine lending grains the writer emits at sub-instrument granularity; UAC
# models lending at the coarser canonical `lending` instrument_type
# (honest_coverage_uac_writer_matrix_reconciliation Decision 3/4, operator
# 2026-06-29: "Roll a_token/debt_token → lending" — a GRAIN mismatch, not
# missing data; the lending data_types are already declared in _LENDING_DATA).
_DEFI_LENDING_FINE_GRAINS: frozenset[str] = frozenset({"a_token", "debt_token", "liquidation"})


def _canon_instrument_type(asset_group: str, venue: str, instrument_type: str) -> str:
    """Canonicalise an instrument_type token to the comparison grain.

    Steps (reuse the enumerator's SSOT canonicalisation, no ad-hoc maps):
      1. strip + lowercase (case-fold — manifest carries BOTH PERPETUAL/perpetual);
      2. UAC _INSTRUMENT_TYPE_ALIASES (spot→spot_pair, perp→perpetual, …);
      3. bundle roll-up via bundle_instrument_type_for_leaf (OPTION→options_chain,
         COMBO→combo, FUTURE@bundle-venue→futures_chain) — the same helper
         _canonical_writer_instrument_type uses, so a leaf and its bundle meet.
    """
    norm = (instrument_type or "").strip().lower()
    if not norm:
        return ""  # blank stays blank — a blank-instrument_type row is a REAL hole
    aliases = _get_instrument_type_aliases()
    norm = aliases.get(norm, norm)
    # Prediction grain: the writers stamp prediction / prediction_market /
    # PREDICTION_MARKET; the UAC alias map has no entry for the bare `prediction`
    # token (Kalshi), so fold it to the canonical `prediction_market` grain
    # (the matrix key) — both name the same could-exist grain.
    if asset_group == "prediction" and norm == "prediction":
        norm = "prediction_market"
    # DeFi lending grain roll-up: writer-side fine grains (a_token/debt_token/
    # liquidation) meet UAC's canonical `lending` grain — same could-exist cell.
    if asset_group == "defi" and norm in _DEFI_LENDING_FINE_GRAINS:
        norm = "lending"
    bundle = bundle_instrument_type_for_leaf(asset_group, norm, venue)
    if bundle is not None:
        norm = bundle.strip().lower()
        norm = aliases.get(norm, norm)
    return norm


# CeFi venue-dialect fold (honest_coverage_uac_writer_matrix_reconciliation
# Decision 6, implemented as the todo's "check folds suffixes" option): the
# writer captures under Tardis-grain suffixed venues (OKX-SPOT/-SWAP/-FUTURES
# from expand_cefi_tardis_endpoints; legacy raw Tardis exchange ids on older
# rows) while UAC keys those venues at the bare canonical grain (OKX; COINBASE
# for spot). Fold BOTH sides to the UAC-canonical venue so a suffix dialect can
# never manufacture a false hole or a false stray. Venues that are themselves
# UAC-canonical suffixed forms (BYBIT-SPOT, KRAKEN-FUTURES, BITFINEX-*, …) are
# deliberately NOT folded.
_CEFI_VENUE_FOLD: dict[str, str] = {
    # Tardis-grain splits emitted by expand_cefi_tardis_endpoints()
    "OKX-SPOT": "OKX",
    "OKX-SWAP": "OKX",
    "OKX-FUTURES": "OKX",
    "COINBASE-SPOT": "COINBASE",
    # Writer-side names for venues UAC keys differently
    "BYBIT-FUTURES": "BYBIT",
    "COINBASE-INTERNATIONAL": "COINBASE-FUTURES",
    # Legacy raw Tardis exchange ids (pre-canonicalisation manifest rows)
    "OKEX": "OKX",
    "OKEX-SWAP": "OKX",
    "OKEX-FUTURES": "OKX",
    "CRYPTOFACILITIES": "KRAKEN-FUTURES",
    "BITFINEX-DERIVATIVES": "BITFINEX-FUTURES",
}


def _canon_venue(asset_group: str, venue: str) -> str:
    """Canonicalise a venue token to the comparison grain.

    - defi: canonicalise protocol spelling (AAVEV3→AAVE_V3) via the SAME
      VenueMapping helper _enumerate_v2_defi uses, then strip the -CHAIN suffix
      so the EXPECTED PROTOCOL-CHAIN id (AAVE_V3-ETHEREUM) and the ENUMERATED
      PROTOCOL-only id (AAVE_V3) meet at the PROTOCOL grain.
    - cefi: fold Tardis-suffix/legacy venue dialects to the UAC canonical venue
      (see _CEFI_VENUE_FOLD).
    - all AGs: upper-case (venues are upper-case canonical; manifest carries
      stray lower-case e.g. kalshi).
    """
    v = (venue or "").strip()
    if not v:
        return ""
    if asset_group == "defi":
        v = VenueMapping._canonicalise_defi_protocol_spelling(v.upper())
        if "-" in v:
            v = v.rsplit("-", 1)[0]  # strip -CHAIN suffix → PROTOCOL grain
        return v
    v = v.upper()
    if asset_group == "cefi":
        return _CEFI_VENUE_FOLD.get(v, v)
    return v


def _canon_data_type(asset_group: str, data_type: str) -> str:
    """Canonicalise a data_type token (case-fold — manifest carries ODDS/odds).

    defi: `rate_indices` is the non-canonical writer name for `lending_indices`
    (reconciliation Decision 3 evidence, 2026-06-29) — pure dialect, same cell.
    """
    dt = (data_type or "").strip().lower()
    if asset_group == "defi" and dt == "rate_indices":
        return "lending_indices"
    return dt


def _canon_key(asset_group: str, venue: str, instrument_type: str, data_type: str) -> tuple[str, str, str]:
    """Build the canonical comparison key for a (venue, itype, data_type) tuple."""
    return (
        _canon_venue(asset_group, venue),
        _canon_instrument_type(asset_group, venue, instrument_type),
        _canon_data_type(asset_group, data_type),
    )


# The full set of lowercase canonical instrument_types we consider for each AG.
# For cefi/tradfi/sports/prediction: derived from VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE keys.
# For defi: derived dynamically from PROTOCOL_CAPABILITIES via valid_data_types_for_instrument_type.
_AG_INSTRUMENT_TYPES: dict[str, frozenset[str]] = {}

# Known defi instrument types (from PROTOCOL_CAPABILITIES — populated lazily).
_DEFI_INSTRUMENT_TYPES: frozenset[str] | None = None


def _get_defi_instrument_types() -> frozenset[str]:
    """Return all canonical instrument_types present in any DeFi protocol.

    Lazily built from PROTOCOL_CAPABILITIES via the valid_data_types_for_instrument_type
    function which also caches the defi mapping internally.
    """
    global _DEFI_INSTRUMENT_TYPES
    if _DEFI_INSTRUMENT_TYPES is None:
        from unified_api_contracts.registry.capability_declarations._defi import (  # noqa: TID251
            PROTOCOL_CAPABILITIES,
        )

        itypes: set[str] = set()
        for cap in PROTOCOL_CAPABILITIES.values():
            for it in cap.instrument_types:
                itypes.add(it.strip().lower())
        _DEFI_INSTRUMENT_TYPES = frozenset(itypes)
    return _DEFI_INSTRUMENT_TYPES


# Valid (venue, instrument_type) pairs per AG — the could-exist universe gate
# that prevents the cross-product over-generation (BINANCE-FUTURES × spot_pair,
# AAVE(lending) × pool, …). Lazily built from the AUTHORITATIVE UAC sources.
_CEFI_VENUE_ITYPES: dict[str, frozenset[str]] | None = None


def _get_cefi_venue_itypes() -> dict[str, frozenset[str]]:
    """Return cefi venue → frozenset of valid (lowercase, alias-canonical) itypes.

    AUTHORITY: VenueMapping.venue_instrument_type_to_tardis keys (the
    (venue, INSTRUMENT_TYPE) pairs that resolve to a real tardis endpoint) PLUS
    the FUTURE_BUNDLE_VENUES bundle types (options_chain/futures_chain) for the
    bundle venues (DERIBIT/OKX) — these are the per-underlying chain bundles the
    writer stamps, not leaf OPTION/FUTURE.
    """
    global _CEFI_VENUE_ITYPES
    if _CEFI_VENUE_ITYPES is None:
        from unified_api_contracts.registry.market_data_categories import (  # noqa: TID251
            FUTURE_BUNDLE_VENUES,
        )

        aliases = _get_instrument_type_aliases()
        vm = VenueMapping()
        out: dict[str, set[str]] = {}
        for venue, itype in vm.venue_instrument_type_to_tardis:
            norm = aliases.get(itype.strip().lower(), itype.strip().lower())
            # roll leaf option/future to bundle at bundle venues
            bundle = bundle_instrument_type_for_leaf("cefi", norm, venue)
            if bundle is not None:
                norm = aliases.get(bundle.strip().lower(), bundle.strip().lower())
            out.setdefault(venue.strip().upper(), set()).add(norm)
        # Ensure the bundle venues carry the bundle itypes explicitly.
        for bv in FUTURE_BUNDLE_VENUES.get("cefi", frozenset()):
            out.setdefault(bv, set()).update({"options_chain", "futures_chain"})
        _CEFI_VENUE_ITYPES = {v: frozenset(its) for v, its in out.items()}
    return _CEFI_VENUE_ITYPES


# TradFi venue → writer-grain instrument_types: UAC TRADFI_VENUE_INSTRUMENT_TYPES
# (promoted there 2026-07-03 from the replica that used to live here —
# honest_coverage_uac_writer_matrix_reconciliation "SSOT-placement" finding; the
# MTDS writer-side counterpart is symbol_rules._VENUE_INSTRUMENT_TYPE).
# YAHOO_FINANCE/KRX stamp no itype (legacy source-as-venue) → absent from the
# map → not gated here (fall through to data_type-capability only).

_DEFI_PROTOCOL_ITYPES: dict[str, frozenset[str]] | None = None


def _get_defi_protocol_itypes() -> dict[str, frozenset[str]]:
    """Return defi PROTOCOL (chain-stripped, upper) → valid (alias-canonical) itypes.

    AUTHORITY: PROTOCOL_CAPABILITIES[protocol].instrument_types. This narrows the
    defi cross-product to only the itypes each protocol actually declares (so a
    lending-only protocol like AAVE never expects pool/perp tuples).
    """
    global _DEFI_PROTOCOL_ITYPES
    if _DEFI_PROTOCOL_ITYPES is None:
        from unified_api_contracts.registry.capability_declarations._defi import (  # noqa: TID251
            PROTOCOL_CAPABILITIES,
        )

        aliases = _get_instrument_type_aliases()
        out: dict[str, set[str]] = {}
        for protocol, cap in PROTOCOL_CAPABILITIES.items():
            its = {aliases.get(it.strip().lower(), it.strip().lower()) for it in cap.instrument_types}
            out[protocol.strip().upper()] = its
        _DEFI_PROTOCOL_ITYPES = {p: frozenset(its) for p, its in out.items()}
    return _DEFI_PROTOCOL_ITYPES


def _venue_itype_is_valid(asset_group: str, venue: str, itype_canon: str) -> bool:
    """Gate: does (venue, instrument_type) genuinely co-occur for this AG?

    Prevents the cross-product over-generation. itype_canon is the lowercase
    alias-canonical instrument_type. For AGs without a codified venue→itype
    authority (tradfi/sports/prediction) returns True (no gate) — those are
    handled by the data_type-level validity functions + venue capability table.
    """
    if asset_group == "cefi":
        valid = _get_cefi_venue_itypes().get(venue.strip().upper())
        return valid is not None and itype_canon in valid
    if asset_group == "defi":
        # venue here is the EXPECTED-side PROTOCOL-CHAIN id; strip to protocol.
        proto = VenueMapping._canonicalise_defi_protocol_spelling(venue.strip().upper())
        if "-" in proto:
            proto = proto.rsplit("-", 1)[0]
        valid = _get_defi_protocol_itypes().get(proto)
        return valid is not None and itype_canon in valid
    if asset_group == "tradfi":
        valid = TRADFI_VENUE_INSTRUMENT_TYPES.get(venue.strip().upper())
        # Venues without a stamped itype (YAHOO_FINANCE/KRX) are not gated here.
        return valid is None or itype_canon in valid
    return True


def _get_ag_instrument_types(asset_group: str) -> frozenset[str]:
    """Return all canonical (lowercase) instrument_types for an asset_group.

    For defi: dynamically from PROTOCOL_CAPABILITIES (the static dict has no defi keys).
    For others: from VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE keys (post-bundle-roll-up
    types that actually appear in the manifest).
    Leaf types with empty valid_data_types are included and filtered out during the
    tuple-building loop.
    """
    if asset_group not in _AG_INSTRUMENT_TYPES:
        if asset_group == "defi":
            _AG_INSTRUMENT_TYPES[asset_group] = _get_defi_instrument_types()
        else:
            itypes: set[str] = set()
            for ag, itype in VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE:
                if ag == asset_group:
                    itypes.add(itype)
            _AG_INSTRUMENT_TYPES[asset_group] = frozenset(itypes)
    return _AG_INSTRUMENT_TYPES[asset_group]


@dataclass
class MissingTuple:
    venue: str
    instrument_type: str
    data_type: str

    def as_dict(self) -> dict[str, str]:
        return {
            "venue": self.venue,
            "instrument_type": self.instrument_type,
            "data_type": self.data_type,
        }


@dataclass
class StrayTuple:
    venue: str
    instrument_type: str
    data_type: str

    def as_dict(self) -> dict[str, str]:
        return {
            "venue": self.venue,
            "instrument_type": self.instrument_type,
            "data_type": self.data_type,
        }


@dataclass
class VenueCompleteness:
    venue: str
    expected_tuples: int
    present_tuples: int
    completeness_pct: float
    missing: list[MissingTuple] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "completeness_pct": self.completeness_pct,
            "expected_tuples": self.expected_tuples,
            "present_tuples": self.present_tuples,
            "missing": [m.as_dict() for m in self.missing],
        }


@dataclass
class DiagnosticSamples:
    """Sample keys per AG so a residual hole is provably REAL vs a dialect artifact.

    Each sample is the CANONICAL comparison key (post-alignment) plus, for the
    matched/expected-only/enumerated-only buckets, an example original tuple.
    """

    expected_only: list[tuple[str, str, str]]  # canonical keys in EXPECTED − ENUMERATED
    enumerated_only: list[tuple[str, str, str]]  # canonical keys in ENUMERATED − EXPECTED
    matched: list[tuple[str, str, str]]  # canonical keys in EXPECTED ∩ ENUMERATED
    matched_count: int
    expected_only_count: int
    enumerated_only_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "matched_count": self.matched_count,
            "expected_only_count": self.expected_only_count,
            "enumerated_only_count": self.enumerated_only_count,
            "expected_only_samples": [list(t) for t in self.expected_only],
            "enumerated_only_samples": [list(t) for t in self.enumerated_only],
            "matched_samples": [list(t) for t in self.matched],
        }


@dataclass
class AgLayer1Result:
    asset_group: str
    denominator_complete: bool
    denominator_status: str  # "COMPLETE" | "INCOMPLETE" | "UNDEFINED"
    completeness_pct: float | None  # None when EXPECTED==0 (UNDEFINED)
    expected_tuples: int
    present_tuples: int
    missing_tuples: list[MissingTuple]
    stray_tuples: list[StrayTuple]
    by_venue: dict[str, VenueCompleteness]
    diagnostics: DiagnosticSamples | None = None

    def as_dict(self) -> dict[str, object]:
        out: dict[str, object] = {
            "denominator_complete": self.denominator_complete,
            "denominator_status": self.denominator_status,
            "completeness_pct": self.completeness_pct,
            "expected_tuples": self.expected_tuples,
            "present_tuples": self.present_tuples,
            "missing_tuples": [m.as_dict() for m in self.missing_tuples],
            "stray_tuples": [s.as_dict() for s in self.stray_tuples],
            "by_venue": {v: vc.as_dict() for v, vc in self.by_venue.items()},
        }
        if self.diagnostics is not None:
            out["diagnostics"] = self.diagnostics.as_dict()
        return out


@dataclass
class Layer1Result:
    by_asset_group: dict[str, AgLayer1Result]

    def as_dict(self) -> dict[str, object]:
        return {
            "by_asset_group": {ag: r.as_dict() for ag, r in self.by_asset_group.items()},
        }


def _build_expected_tuples_sports() -> set[tuple[str, str, str]]:
    """Build sports EXPECTED at the WRITER grain (instrument_type=odds).

    Sports has TWO surfaces (research 2026-06-29):
      (1) IS reference-data: instrument_type="league", UPPERCASE data_types
          (SPORTS_DATA_TYPE_TO_SOURCE: MATCHES/STANDINGS/FIXTURES/XG/…) — lives
          in the instruments-store reference bucket, NOT the market-data-tick
          sports bucket this harness reads. Its data_types do NOT appear in the
          market-tick manifest (which carries only odds/trades/odds_* etc.), so
          including the league surface here would manufacture false holes. It is
          therefore OUT OF SCOPE for the market-tick Layer-1 (Layer-1 of the
          reference store is a separate audit).
      (2) MTDS odds market-tick: instrument_type="odds", real bookmaker venues,
          data_types from VENUE_DATA_TYPE_CAPABILITIES[venue] (markets /
          odds_snapshot / odds_movement / outcomes / settlements / odds /
          arbitrage_opportunity / odds_horizon_bucket / trades).

    The MTDS writer stamps instrument_type=odds (SSOT: MTDS venue_fetch.py /
    sentinels.py) at data_type=trades for bookmaker ticks + the ODDS_API
    snapshot data_types. EXPECTED is the odds grain per bookmaker venue (the
    surface that actually lands in this bucket); `trades` is the canonical
    bookmaker tick data_type so it is admitted for every venue.

    AUTHORITY: VENUE_DATA_TYPE_CAPABILITIES (per-venue odds capabilities).
    """
    expected: set[tuple[str, str, str]] = set()
    venues = VENUES_BY_ASSET_GROUP.get("sports", [])
    for venue in venues:
        caps = VENUE_DATA_TYPE_CAPABILITIES.get(venue, {})
        for dt in caps:
            expected.add((venue, "odds", dt))
        # Every bookmaker venue produces the canonical `trades` odds tick
        # (MTDS sentinels stamp data_type=trades) even when the capability table
        # only lists the richer odds_* snapshot types.
        expected.add((venue, "odds", "trades"))
    return expected


def _build_expected_tuples(asset_group: str) -> set[tuple[str, str, str]]:
    """Build the EXPECTED set of (venue, instrument_type, data_type) from UAC.

    Per the CK2 pseudocode from codex/02-data/honest-coverage-model.md:

      for venue in venues_in_ag(ag):
        for instrument_type in itypes_present(ag, venue):
          # AUTHORITY = UAC FUNCTIONS, never the raw dict (dict has no defi keys)
          expected_dts = (
              valid_data_types_for_venue_instrument_type(ag, venue, itype)
              or valid_data_types_for_instrument_type(ag, itype)
              or frozenset()
          )
          for dt in expected_dts:
            if ag in VENUE_CAPABILITY_AGS and dt not in VENUE_DATA_TYPE_CAPABILITIES[venue]:
                continue  # venue cannot produce dt → carve-out (cefi/tradfi only)
            if not is_mvp(ag, venue, itype, dt): continue
            EXPECTED.add((venue, itype, dt))

    VENUE_DATA_TYPE_CAPABILITIES is only applied as a skip-filter for
    VENUE_CAPABILITY_AGS = {"cefi", "tradfi"} — DeFi/sports/prediction capability
    is already encoded in valid_data_types_for_venue_instrument_type /
    valid_data_types_for_instrument_type.  Applying it to defi would wrongly
    exclude all defi tuples (the dict has no defi venue keys).

    Bundle grain: leaf OPTION/FUTURE for FUTURE_BUNDLE_VENUES venues are rolled
    up to options_chain/futures_chain bundles.  The UAC functions / static dict
    already carry the bundle instrument_types as first-class keys; leaf types
    that resolve to frozenset() are skipped.
    """
    ag = asset_group.lower()
    if ag == "sports":
        return _build_expected_tuples_sports()
    venues = VENUES_BY_ASSET_GROUP.get(ag, [])
    instrument_types = _get_ag_instrument_types(ag)
    expected: set[tuple[str, str, str]] = set()
    in_capability_ag = ag in VENUE_CAPABILITY_AGS

    for venue in venues:
        venue_caps = VENUE_DATA_TYPE_CAPABILITIES.get(venue, {}) if in_capability_ag else {}
        # For cefi: pre-compute MVP data_types override for this venue.
        cefi_mvp_dts: frozenset[str] | None = None
        if ag == "cefi":
            cefi_mvp_dts = get_mvp_data_types_for_cefi_venue(venue)

        for itype in instrument_types:
            # (venue, itype) validity GATE — prevents the cross-product
            # over-generation (BINANCE-FUTURES × spot_pair, AAVE(lending) × pool).
            # AUTHORITY: cefi=venue_instrument_type_to_tardis keys;
            # defi=PROTOCOL_CAPABILITIES[protocol].instrument_types.
            itype_canon = _get_instrument_type_aliases().get(itype.strip().lower(), itype.strip().lower())
            if not _venue_itype_is_valid(ag, venue, itype_canon):
                continue

            # AUTHORITY: UAC functions, not the raw dict (bug fix 2026-06-29).
            # valid_data_types_for_venue_instrument_type narrows DeFi to the
            # specific protocol named by the PROTOCOL segment of venue id.
            # Falls back to valid_data_types_for_instrument_type when the
            # venue-specific narrowing is not applicable (non-defi or unmapped).
            valid_dts = (
                valid_data_types_for_venue_instrument_type(ag, venue, itype)
                or valid_data_types_for_instrument_type(ag, itype)
                or frozenset()
            )

            if len(valid_dts) == 0:
                # frozenset() → no expected rows for this (ag, venue, itype).
                # For cefi this means the leaf type (OPTION) rolls up to a bundle
                # (options_chain) which is iterated as its own first-class itype.
                continue

            # Check if the bundle_instrument_type_for_leaf maps this itype to a
            # bundle at this venue.  If so, skip the leaf — the bundle type is
            # iterated separately.
            bundle_it = bundle_instrument_type_for_leaf(ag, itype, venue)
            if bundle_it is not None and bundle_it != itype:
                continue

            for dt in valid_dts:
                # Carve-out 1: venue cannot produce this data_type.
                # Only apply for cefi/tradfi (VENUE_CAPABILITY_AGS); defi/sports/
                # prediction capability is already in the validity functions.
                if in_capability_ag and dt not in venue_caps:
                    continue

                # Carve-out 2: cefi per-venue MVP override.
                # NOTE: is_mvp() is NOT used here because it requires a base_ccy
                # (instrument grain) to return True for CeFi/TradFi.  The schema-
                # level MVP gate for CeFi is get_mvp_data_types_for_cefi_venue.
                # TradFi: VENUES_BY_ASSET_GROUP already contains only MVP tradfi
                # venues (CME); non-MVP venues are absent from the AG's venue set.
                if ag == "cefi" and cefi_mvp_dts is not None and dt not in cefi_mvp_dts:
                    continue

                expected.add((venue, itype, dt))

    return expected


def _build_enumerated_tuples(asset_group: str, df: pd.DataFrame) -> set[tuple[str, str, str]]:
    """Build the ENUMERATED set from the manifest skeleton.

    Distinct (venue, instrument_type, data_type) tuples present in the manifest,
    across all 4 capture_status states. Rows with blank instrument_type are
    excluded (they count as missing from ENUMERATED, surfacing as Layer-1 holes
    until the writer stamps the correct instrument_type).
    """
    if "instrument_type" not in df.columns:
        logger.warning(
            "  Layer-1 [%s]: 'instrument_type' column absent — ENUMERATED set will be empty",
            asset_group,
        )
        return set()

    enumerated: set[tuple[str, str, str]] = set()
    # Only iterate unique (venue, instrument_type, data_type) triples.
    cols = ["venue", "instrument_type", "data_type"]
    available = [c for c in cols if c in df.columns]
    if len(available) < 3:
        logger.warning(
            "  Layer-1 [%s]: missing columns %s — cannot build ENUMERATED set",
            asset_group,
            set(cols) - set(available),
        )
        return enumerated

    # Drop rows with blank instrument_type (they are Layer-1 holes by definition).
    mask = df["instrument_type"].notna() & (df["instrument_type"].astype(str).str.strip() != "")
    sub = df.loc[mask, ["venue", "instrument_type", "data_type"]]
    for row in sub.drop_duplicates().itertuples(index=False):
        enumerated.add((str(row.venue), str(row.instrument_type), str(row.data_type)))
    return enumerated


def _canonicalise_tuple_set(
    asset_group: str, tuples: set[tuple[str, str, str]]
) -> dict[tuple[str, str, str], tuple[str, str, str]]:
    """Map canonical comparison key → a representative original tuple.

    Blank-instrument_type keys are EXCLUDED (a blank itype is a genuine hole on
    the ENUMERATED side, and on the EXPECTED side EXPECTED never has blanks).
    The first original tuple seen for a canonical key wins (deterministic via
    the sorted input).
    """
    out: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    for orig in sorted(tuples):
        v, it, dt = orig
        key = _canon_key(asset_group, v, it, dt)
        # A blank canonical instrument_type means the row carried no usable
        # itype — keep it as a distinct key so it surfaces as a REAL hole/stray,
        # never silently collapsing into a valid bucket.
        if key not in out:
            out[key] = orig
    return out


def check_enumeration_completeness(
    asset_group: str,
    df: pd.DataFrame,
    *,
    diagnose: bool = False,
    sample_n: int = 15,
) -> AgLayer1Result:
    """Check Layer-1 enumeration completeness for a single asset_group.

    The intersection is computed on a CANONICAL COMPARISON KEY (case-folded +
    UAC instrument_type aliases + bundle roll-up + defi protocol/chain
    canonicalisation) so EXPECTED (UAC vocabulary) and ENUMERATED (manifest
    written vocabulary) meet at one grain — see _canon_key / the VOCABULARY/GRAIN
    ALIGNMENT HARD RULE. Only REAL post-alignment holes count toward
    missing_tuples; pure casing/format/vocabulary differences do NOT.

    Args:
        asset_group: One of cefi/defi/tradfi/sports/prediction.
        df: Merged manifest DataFrame for the asset_group (all 4 capture_status
            states present). Must include 'venue', 'data_type', 'capture_status'.
            'instrument_type' is required for a meaningful ENUMERATED set.
        diagnose: when True, populate AgLayer1Result.diagnostics with sample
            EXPECTED-only / ENUMERATED-only / matched canonical keys.
        sample_n: number of samples per bucket in diagnostic mode.

    Returns:
        AgLayer1Result with completeness_pct, missing_tuples, stray_tuples,
        denominator_complete, and per-venue breakdown.
    """
    ag = asset_group.lower()
    logger.info("  Layer-1: building EXPECTED matrix for %s …", ag)
    expected = _build_expected_tuples(ag)
    logger.info("  Layer-1 [%s]: EXPECTED = %d tuples (raw, pre-align)", ag, len(expected))

    logger.info("  Layer-1: building ENUMERATED matrix for %s …", ag)
    enumerated = _build_enumerated_tuples(ag, df)
    logger.info("  Layer-1 [%s]: ENUMERATED = %d tuples (raw, pre-align)", ag, len(enumerated))

    # VOCABULARY/GRAIN ALIGNMENT: canonicalise both sides, intersect on the key.
    exp_by_key = _canonicalise_tuple_set(ag, expected)
    enum_by_key = _canonicalise_tuple_set(ag, enumerated)
    exp_keys = set(exp_by_key)
    enum_keys = set(enum_by_key)

    present_keys = exp_keys & enum_keys
    missing_keys = exp_keys - enum_keys
    stray_keys = enum_keys - exp_keys

    logger.info(
        "  Layer-1 [%s]: aligned EXPECTED=%d ENUMERATED=%d matched=%d missing=%d stray=%d",
        ag,
        len(exp_keys),
        len(enum_keys),
        len(present_keys),
        len(missing_keys),
        len(stray_keys),
    )

    n_expected = len(exp_keys)
    n_present = len(present_keys)

    # EMPTY-DENOMINATOR GUARD (HARD RULE, 2026-06-29).
    # EXPECTED == 0 is NOT 100% complete — it means the AG's validity authority
    # is not wired or no venues/instruments were enumerated.  Fail CLOSED.
    if n_expected == 0:
        logger.error(
            "  Layer-1 [%s]: EXPECTED == 0 — denominator UNDEFINED (not wired or no venues). "
            "This is certification-blocking. Check UAC function imports and VENUES_BY_ASSET_GROUP.",
            ag,
        )
        completeness_pct: float | None = None
        denominator_complete = False
        denominator_status = "UNDEFINED"
    else:
        completeness_pct = round(n_present / n_expected * 100, 2)
        denominator_complete = len(missing_keys) == 0
        denominator_status = "COMPLETE" if denominator_complete else "INCOMPLETE"

    # Map canonical missing/stray keys back to a representative original tuple.
    missing_tuples = [
        MissingTuple(venue=exp_by_key[k][0], instrument_type=exp_by_key[k][1], data_type=exp_by_key[k][2])
        for k in sorted(missing_keys)
    ]
    stray_tuples = [
        StrayTuple(venue=enum_by_key[k][0], instrument_type=enum_by_key[k][1], data_type=enum_by_key[k][2])
        for k in sorted(stray_keys)
    ]

    if stray_tuples:
        logger.warning(
            "  Layer-1 [%s]: %d stray tuples (post-align — writer emits something UAC does not sanction): %s",
            ag,
            len(stray_tuples),
            [(s.venue, s.instrument_type, s.data_type) for s in stray_tuples[:5]],
        )

    if denominator_status == "UNDEFINED":
        pass  # already logged as ERROR above
    elif missing_tuples:
        logger.warning(
            "  Layer-1 [%s]: %d MISSING tuples (post-align Layer-1 holes): first 5: %s",
            ag,
            len(missing_tuples),
            [(m.venue, m.instrument_type, m.data_type) for m in missing_tuples[:5]],
        )
    else:
        logger.info("  Layer-1 [%s]: denominator COMPLETE (0 holes)", ag)

    # Per-venue breakdown (canonical venue grain).
    expected_by_venue: dict[str, set[tuple[str, str]]] = {}
    for cv, cit, cdt in exp_keys:
        expected_by_venue.setdefault(cv, set()).add((cit, cdt))

    enumerated_by_venue: dict[str, set[tuple[str, str]]] = {}
    for cv, cit, cdt in enum_keys:
        enumerated_by_venue.setdefault(cv, set()).add((cit, cdt))

    by_venue: dict[str, VenueCompleteness] = {}
    all_venues = sorted(set(expected_by_venue) | set(enumerated_by_venue))
    for venue in all_venues:
        v_exp = expected_by_venue.get(venue, set())
        v_enum = enumerated_by_venue.get(venue, set())
        v_present = v_exp & v_enum
        v_missing = v_exp - v_enum
        n_v_exp = len(v_exp)
        n_v_present = len(v_present)
        # Per-venue empty-denominator: return 0.0 (conservative) rather than 100.0
        v_pct = round(n_v_present / n_v_exp * 100, 2) if n_v_exp else 0.0
        by_venue[venue] = VenueCompleteness(
            venue=venue,
            expected_tuples=n_v_exp,
            present_tuples=n_v_present,
            completeness_pct=v_pct,
            missing=[MissingTuple(venue=venue, instrument_type=it, data_type=dt) for (it, dt) in sorted(v_missing)],
        )

    diagnostics: DiagnosticSamples | None = None
    if diagnose:
        diagnostics = DiagnosticSamples(
            expected_only=sorted(missing_keys)[:sample_n],
            enumerated_only=sorted(stray_keys)[:sample_n],
            matched=sorted(present_keys)[:sample_n],
            matched_count=len(present_keys),
            expected_only_count=len(missing_keys),
            enumerated_only_count=len(stray_keys),
        )

    return AgLayer1Result(
        asset_group=ag,
        denominator_complete=denominator_complete,
        denominator_status=denominator_status,
        completeness_pct=completeness_pct,
        expected_tuples=n_expected,
        present_tuples=n_present,
        missing_tuples=missing_tuples,
        stray_tuples=stray_tuples,
        by_venue=by_venue,
        diagnostics=diagnostics,
    )


def check_all_asset_groups(
    dfs: dict[str, pd.DataFrame],
) -> Layer1Result:
    """Run Layer-1 enumeration-completeness check for all loaded asset_groups.

    Args:
        dfs: mapping of asset_group → merged manifest DataFrame.

    Returns:
        Layer1Result with per-AG AgLayer1Result.
    """
    results: dict[str, AgLayer1Result] = {}
    for ag, df in dfs.items():
        results[ag] = check_enumeration_completeness(ag, df)
    return Layer1Result(by_asset_group=results)
