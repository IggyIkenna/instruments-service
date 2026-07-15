# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Single public producer of the EXPECTED Layer-1 universe.

`build_expected(asset_group) -> set[tuple[venue, instrument_type, data_type]]`
is THE entry point.  Every caller — Layer-1 completeness (Layer-1 matrix),
Layer-2 measurement (measure_honest_coverage via the completeness module),
audits — routes through it.  Two independent producers = two chances at silent
denominator drift, which is exactly the failure mode Honest Coverage v2
exists to kill.

Per-AG strategies share ONE interface (a callable returning the tuple set)
but preserve the genuinely different grains the SSOT calls out:

  cefi:       INSTRUMENT_TYPES_BY_VENUE  +  VENUE_DATA_TYPE_CAPABILITIES  +
              get_mvp_data_types_for_cefi_venue_itype  (per-(venue,itype) MVP override)
  defi:       PROTOCOL_CAPABILITIES  per-protocol  (chain-genesis grain)
  tradfi:     TRADFI_VENUE_INSTRUMENT_TYPES  +  VENUE_DATA_TYPE_CAPABILITIES
  sports:     VENUE_DATA_TYPE_CAPABILITIES  odds  grain per bookmaker venue
  prediction: VENUES_BY_ASSET_GROUP  +  UAC  validity  functions

D2a declarative-gate authority is BAKED IN — cefi uses INSTRUMENT_TYPES_BY_VENUE
(the SAME declarative axis defi uses via PROTOCOL_CAPABILITIES and tradfi uses
via TRADFI_VENUE_INSTRUMENT_TYPES), NOT the Tardis fetch-routing map that
silently omits non-Tardis venues.

Byte-identical golden regression per AG lives at
`tests/unit/scripts/goldens/expected_universe/<ag>.json` — any producer edit
that changes the tuple set fails that gate loudly (drift never silent).

SSOT: codex/02-data/honest-coverage-model.md
      § Layer-1 enumeration-completeness matrix
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from unified_api_contracts import (
    INSTRUMENT_TYPES_BY_VENUE,
    VENUES_BY_ASSET_GROUP,
    bundle_instrument_type_for_leaf,
    get_mvp_data_types_for_cefi_venue_itype,
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

# AGs where VENUE_DATA_TYPE_CAPABILITIES applies as a skip-filter.
# defi/sports/prediction capability is already encoded in their protocol/league
# validity functions; that table is keyed by cefi/tradfi venues only.
VENUE_CAPABILITY_AGS: frozenset[str] = frozenset({"cefi", "tradfi"})

KNOWN_ASSET_GROUPS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# ---------------------------------------------------------------------------
# Lazy caches (module-level; safe under Python's GIL for read-mostly access).
# ---------------------------------------------------------------------------

_INSTRUMENT_TYPE_ALIASES: dict[str, str] | None = None
_CEFI_VENUE_ITYPES: dict[str, frozenset[str]] | None = None
_DEFI_PROTOCOL_ITYPES: dict[str, frozenset[str]] | None = None
_DEFI_INSTRUMENT_TYPES: frozenset[str] | None = None
_AG_INSTRUMENT_TYPES: dict[str, frozenset[str]] = {}


def _get_instrument_type_aliases() -> dict[str, str]:
    """Return the UAC SSOT instrument_type alias map (lowercase token → canonical)."""
    global _INSTRUMENT_TYPE_ALIASES
    if _INSTRUMENT_TYPE_ALIASES is None:
        from unified_api_contracts.registry.market_data_categories import (
            _INSTRUMENT_TYPE_ALIASES as _INSTRUMENT_TYPE_ALIASES_SRC,
        )

        _INSTRUMENT_TYPE_ALIASES = dict(_INSTRUMENT_TYPE_ALIASES_SRC)
    return _INSTRUMENT_TYPE_ALIASES


def _get_defi_instrument_types() -> frozenset[str]:
    """Return all canonical instrument_types present in any DeFi protocol.

    Lazily built from PROTOCOL_CAPABILITIES.  The static
    VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE dict has ZERO defi keys — defi
    validity is entirely dynamic from PROTOCOL_CAPABILITIES.
    """
    global _DEFI_INSTRUMENT_TYPES
    if _DEFI_INSTRUMENT_TYPES is None:
        from unified_api_contracts.registry.capability_declarations._defi import (
            PROTOCOL_CAPABILITIES,
        )

        itypes: set[str] = set()
        for cap in PROTOCOL_CAPABILITIES.values():
            for it in cap.instrument_types:
                itypes.add(it.strip().lower())
        _DEFI_INSTRUMENT_TYPES = frozenset(itypes)
    return _DEFI_INSTRUMENT_TYPES


def _get_cefi_venue_itypes() -> dict[str, frozenset[str]]:
    """Return cefi venue → frozenset of valid (lowercase, alias-canonical) itypes.

    AUTHORITY (D2a gate-authority fix, 2026-07-06): UAC's DECLARATIVE
    INSTRUMENT_TYPES_BY_VENUE, restricted to the declared cefi venues
    (VENUES_BY_ASSET_GROUP["cefi"]) — the SAME kind of authority defi uses via
    PROTOCOL_CAPABILITIES.instrument_types and tradfi uses via
    TRADFI_VENUE_INSTRUMENT_TYPES — PLUS the FUTURE_BUNDLE_VENUES bundle types
    (options_chain/futures_chain) for bundle venues (DERIBIT/OKX) — the
    per-underlying chain bundles the writer stamps, not leaf OPTION/FUTURE.

    Previously sourced from VenueMapping.venue_instrument_type_to_tardis — a
    tardis-fetch ROUTING table (which (venue, INSTRUMENT_TYPE) pairs resolve to
    a real tardis endpoint), NOT an existence declaration.  A declared cefi
    venue with no tardis fetch route (e.g. on-chain-CLOB DEX perps that fetch
    via native REST APIs, not Tardis) was therefore SILENTLY ABSENT from the
    EXPECTED set — sourcing != existence.  D2a switches the authority to the
    declarative dict so a declared-but-unrouted venue still counts.
    """
    global _CEFI_VENUE_ITYPES
    if _CEFI_VENUE_ITYPES is None:
        from unified_api_contracts.registry.market_data_categories import (
            FUTURE_BUNDLE_VENUES,
        )

        aliases = _get_instrument_type_aliases()
        out: dict[str, set[str]] = {}
        for venue in VENUES_BY_ASSET_GROUP.get("cefi", []):
            venue_key = venue.strip().upper()
            for itype in INSTRUMENT_TYPES_BY_VENUE.get(venue_key, set()):
                norm = aliases.get(itype.strip().lower(), itype.strip().lower())
                # roll leaf option/future to bundle at bundle venues
                bundle = bundle_instrument_type_for_leaf("cefi", norm, venue_key)
                if bundle is not None:
                    norm = aliases.get(bundle.strip().lower(), bundle.strip().lower())
                out.setdefault(venue_key, set()).add(norm)
        # Ensure the bundle venues carry the bundle itypes explicitly.
        for bv in FUTURE_BUNDLE_VENUES.get("cefi", frozenset()):
            out.setdefault(bv, set()).update({"options_chain", "futures_chain"})
        _CEFI_VENUE_ITYPES = {v: frozenset(its) for v, its in out.items()}
    return _CEFI_VENUE_ITYPES


def _get_defi_protocol_itypes() -> dict[str, frozenset[str]]:
    """Return defi PROTOCOL (chain-stripped, upper) → valid (alias-canonical) itypes.

    AUTHORITY: PROTOCOL_CAPABILITIES[protocol].instrument_types.  This narrows
    the defi cross-product to only the itypes each protocol actually declares
    (so a lending-only protocol like AAVE never expects pool/perp tuples).
    """
    global _DEFI_PROTOCOL_ITYPES
    if _DEFI_PROTOCOL_ITYPES is None:
        from unified_api_contracts.registry.capability_declarations._defi import (
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

    Prevents the cross-product over-generation (BINANCE-FUTURES x spot_pair,
    AAVE(lending) x pool, …).  itype_canon is lowercase, alias-canonical.
    For AGs without a codified venue→itype authority (sports/prediction)
    returns True (no gate) — those are handled by the data_type-level validity
    functions + venue capability table.
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
    types that actually appear in the manifest).  Leaf types with empty valid_data_types
    are included and filtered out during the tuple-building loop.
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


# ---------------------------------------------------------------------------
# Per-AG strategies — share one interface (a `() -> set[tuple[str, str, str]]`
# callable) but each preserves the genuinely different grain the SSOT calls
# out.  build_expected() dispatches through _STRATEGIES.
# ---------------------------------------------------------------------------


def _expected_sports() -> set[tuple[str, str, str]]:
    """Sports strategy — instrument_type=odds per bookmaker venue.

    Sports has TWO surfaces (research 2026-06-29):
      (1) IS reference-data: instrument_type="league", UPPERCASE data_types
          (SPORTS_DATA_TYPE_TO_SOURCE: MATCHES/STANDINGS/FIXTURES/XG/…) — lives
          in the instruments-store reference bucket, NOT the market-data-tick
          sports bucket the Layer-2 harness reads.  Its data_types do not
          appear in the market-tick manifest (odds/trades/odds_*), so including
          the league surface here would manufacture false holes.  Out of scope
          for the market-tick Layer-1 (reference-store Layer-1 is a separate
          audit).
      (2) MTDS odds market-tick: instrument_type="odds", real bookmaker venues,
          data_types from VENUE_DATA_TYPE_CAPABILITIES[venue].

    AUTHORITY: VENUE_DATA_TYPE_CAPABILITIES (per-venue odds capabilities).
    Every bookmaker venue admits the canonical `trades` odds tick (MTDS
    sentinels stamp data_type=trades) even when the capability table only
    lists richer odds_* snapshot types.
    """
    expected: set[tuple[str, str, str]] = set()
    for venue in VENUES_BY_ASSET_GROUP.get("sports", []):
        caps = VENUE_DATA_TYPE_CAPABILITIES.get(venue, {})
        for dt in caps:
            expected.add((venue, "odds", dt))
        expected.add((venue, "odds", "trades"))
    return expected


def _expected_generic(asset_group: str) -> set[tuple[str, str, str]]:
    """Generic per-AG producer for cefi / defi / tradfi / prediction.

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
    VENUE_CAPABILITY_AGS = {"cefi", "tradfi"} — DeFi/prediction capability is
    already encoded in their validity functions.  Applying it to defi would
    wrongly exclude all defi tuples (the dict has no defi venue keys).

    Bundle grain: leaf OPTION/FUTURE for FUTURE_BUNDLE_VENUES venues are rolled
    up to options_chain/futures_chain bundles.  The UAC functions / static
    dict already carry the bundle instrument_types as first-class keys; leaf
    types that resolve to frozenset() are skipped.
    """
    ag = asset_group
    venues = VENUES_BY_ASSET_GROUP.get(ag, [])
    instrument_types = _get_ag_instrument_types(ag)
    expected: set[tuple[str, str, str]] = set()
    in_capability_ag = ag in VENUE_CAPABILITY_AGS

    for venue in venues:
        venue_caps = VENUE_DATA_TYPE_CAPABILITIES.get(venue, {}) if in_capability_ag else {}

        for itype in instrument_types:
            # (venue, itype) validity GATE — prevents the cross-product
            # over-generation (BINANCE-FUTURES x spot_pair, AAVE(lending) x pool).
            # AUTHORITY: cefi=INSTRUMENT_TYPES_BY_VENUE keys (D2a, 2026-07-06);
            # defi=PROTOCOL_CAPABILITIES[protocol].instrument_types;
            # tradfi=TRADFI_VENUE_INSTRUMENT_TYPES.
            itype_canon = _get_instrument_type_aliases().get(itype.strip().lower(), itype.strip().lower())
            if not _venue_itype_is_valid(ag, venue, itype_canon):
                continue

            # For cefi: the MVP data_type set is per-(venue, INSTRUMENT_TYPE) — the
            # PERPETUAL override adds `liquidations` for perp cells ONLY (v15,
            # 2026-07-15), the OPTION override narrows to options_chain, and the
            # per-venue `venue_data_types` override (COINBASE-FUTURES → {trades})
            # still wins. Computed per-itype (NOT venue-wide) via the itype-aware
            # helper so liquidations lands on PERPETUAL only, never FUTURE/spot —
            # matching the enumerate_expected_universe.py denominator producer.
            cefi_mvp_dts: frozenset[str] | None = None
            if ag == "cefi":
                cefi_mvp_dts = get_mvp_data_types_for_cefi_venue_itype(venue, itype)

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
                # For cefi this means the leaf type (OPTION) rolls up to a
                # bundle (options_chain) which is iterated as its own itype.
                continue

            # Check if bundle_instrument_type_for_leaf maps this itype to a
            # bundle at this venue.  If so, skip the leaf — the bundle type is
            # iterated separately.
            bundle_it = bundle_instrument_type_for_leaf(ag, itype, venue)
            if bundle_it is not None and bundle_it != itype:
                continue

            for dt in valid_dts:
                # Carve-out 1: venue cannot produce this data_type.
                # Only apply for cefi/tradfi (VENUE_CAPABILITY_AGS); defi/
                # prediction capability is already in the validity functions.
                if in_capability_ag and dt not in venue_caps:
                    continue

                # Carve-out 2: cefi per-(venue, instrument_type) MVP override.
                # NOTE: is_mvp() is NOT used here because it requires a
                # base_ccy (instrument grain) to return True for CeFi/TradFi.
                # The schema-level MVP gate for CeFi is the itype-aware
                # get_mvp_data_types_for_cefi_venue_itype (computed per-itype
                # above).  TradFi: VENUES_BY_ASSET_GROUP already contains only
                # MVP tradfi venues; non-MVP venues are absent from the AG's set.
                if ag == "cefi" and cefi_mvp_dts is not None and dt not in cefi_mvp_dts:
                    continue

                expected.add((venue, itype, dt))

    return expected


# Per-AG dispatch — every strategy has the same signature `() -> set[…]`.
# Registering an AG here is the ONLY way build_expected() can produce for it.
_STRATEGIES: dict[str, Callable[[], set[tuple[str, str, str]]]] = {
    "cefi": lambda: _expected_generic("cefi"),
    "defi": lambda: _expected_generic("defi"),
    "tradfi": lambda: _expected_generic("tradfi"),
    "sports": _expected_sports,
    "prediction": lambda: _expected_generic("prediction"),
}


def build_expected(asset_group: str) -> set[tuple[str, str, str]]:
    """THE single public producer of the EXPECTED Layer-1 universe.

    Returns the set of (venue, instrument_type, data_type) tuples that UAC
    authoritatively says should exist for this asset_group — the denominator
    Layer-1 audits and Layer-2 measures against.

    Consumers (all callers MUST route through here — do not duplicate):
      • check_enumeration_completeness (Layer-1 matrix)
      • measure_honest_coverage (via the completeness module)
      • test_expected_universe_golden (byte-identical per-AG regression)

    Unknown asset_group → empty set + ERROR log.  A fictitious AG is a
    misconfiguration, not a valid empty universe; downstream the
    empty-denominator guard in `check_enumeration_completeness` catches it
    (denominator_status = UNDEFINED, fail-CLOSED).  We log LOUDLY rather than
    raise so a typo doesn't kill the whole batch when only one AG is broken.

    HARD RULE (codex/02-data/honest-coverage-model.md): the expected universe
    is the cross-product of the IS catalogue and the UAC matrix -- NEVER derive
    it from the manifest (the manifest is the write ledger; using it as both
    numerator and denominator is a circular reference that hides real holes).
    """
    ag = asset_group.lower()
    strategy = _STRATEGIES.get(ag)
    if strategy is None:
        logger.error(
            "build_expected: unknown asset_group %r (known: %s) — returning empty set; "
            "empty-denominator guard will fail-CLOSED downstream",
            asset_group,
            sorted(_STRATEGIES),
        )
        return set()
    return strategy()
