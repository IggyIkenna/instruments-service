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
  - VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE[(ag, instrument_type)] — per-(ag,itype) expected dtypes
  - VENUE_DATA_TYPE_CAPABILITIES[venue][data_type] — venue capability gate
  - get_mvp_data_types_for_cefi_venue(venue) — cefi per-venue MVP override (data_type set)
  - FUTURE_BUNDLE_VENUES — bundle grain for options_chain/futures_chain
  - bundle_instrument_type_for_leaf(ag, itype, venue) — leaf→bundle roll-up
  - VENUES_BY_ASSET_GROUP[ag] — the set of venues tracked for each AG (implicit MVP gate)

Note on MVP gate:
  is_mvp() requires a base_ccy to return True for CeFi/TradFi (it is an instrument-grain
  helper, not a schema-grain helper).  For the schema-level EXPECTED matrix we instead use:
    - CeFi: get_mvp_data_types_for_cefi_venue(venue) — per-venue data_type set
    - TradFi: VENUES_BY_ASSET_GROUP contains only MVP tradfi venues (e.g. CME)
    - DeFi/Sports/Prediction: VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE has no entries

Carve-outs (sourced from UAC, NOT hardcoded):
  1. Venue cannot produce data_type → absent from VENUE_DATA_TYPE_CAPABILITIES[venue]
  2. Out of CeFi-MVP scope → get_mvp_data_types_for_cefi_venue(venue) does not include dt
  3. Bundle roll-up grain → leaf OPTION/FUTURE for FUTURE_BUNDLE_VENUES venues
     yields options_chain/futures_chain bundles, not per-leg rows
  4. frozenset() in VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE → no per-leaf rows

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
from unified_api_contracts.registry.market_data_categories import (
    VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE,
    VENUE_DATA_TYPE_CAPABILITIES,
)

logger = logging.getLogger(__name__)

# Instrument types that exist in the manifest at a coarser / bundle grain.
# For FUTURE_BUNDLE_VENUES this is resolved via bundle_instrument_type_for_leaf;
# for leaf types with frozenset() valid_data_types they are skipped entirely.

# The full set of lowercase canonical instrument_types we consider for each AG.
# We derive this by iterating VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE keys for
# the AG PLUS the bundle types (options_chain, futures_chain, combo) that are
# real instrument_types in the manifest.
_AG_INSTRUMENT_TYPES: dict[str, frozenset[str]] = {}


def _get_ag_instrument_types(asset_group: str) -> frozenset[str]:
    """Return all canonical (lowercase) instrument_types for an asset_group.

    Derived from the VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE keys — these
    are the post-bundle-roll-up types that actually appear in the manifest.
    Leaf types with frozenset() are included (they are filtered out when their
    valid_data_types set is empty).
    """
    if asset_group not in _AG_INSTRUMENT_TYPES:
        itypes: set[str] = set()
        for (ag, itype) in VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE:
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
class AgLayer1Result:
    asset_group: str
    denominator_complete: bool
    completeness_pct: float
    expected_tuples: int
    present_tuples: int
    missing_tuples: list[MissingTuple]
    stray_tuples: list[StrayTuple]
    by_venue: dict[str, VenueCompleteness]

    def as_dict(self) -> dict[str, object]:
        return {
            "denominator_complete": self.denominator_complete,
            "completeness_pct": self.completeness_pct,
            "expected_tuples": self.expected_tuples,
            "present_tuples": self.present_tuples,
            "missing_tuples": [m.as_dict() for m in self.missing_tuples],
            "stray_tuples": [s.as_dict() for s in self.stray_tuples],
            "by_venue": {v: vc.as_dict() for v, vc in self.by_venue.items()},
        }


@dataclass
class Layer1Result:
    by_asset_group: dict[str, AgLayer1Result]

    def as_dict(self) -> dict[str, object]:
        return {
            "by_asset_group": {ag: r.as_dict() for ag, r in self.by_asset_group.items()},
        }


def _build_expected_tuples(asset_group: str) -> set[tuple[str, str, str]]:
    """Build the EXPECTED set of (venue, instrument_type, data_type) from UAC.

    This is the CK2 pseudocode from codex/02-data/honest-coverage-model.md:
      for venue in venues_in_ag(ag):
        for instrument_type in itypes_present(ag, venue):
          expected_dts = VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE[(ag, instrument_type)]
          for dt in expected_dts:
            if dt not in VENUE_DATA_TYPE_CAPABILITIES.get(venue, {}): skip
            if not is_mvp(ag, venue, instrument_type, dt): skip
            EXPECTED.add((venue, instrument_type, dt))

    Bundle grain: leaf OPTION/FUTURE for FUTURE_BUNDLE_VENUES venues are rolled
    up to options_chain/futures_chain bundles via bundle_instrument_type_for_leaf.
    The VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE already carries the bundle
    instrument_types (options_chain/futures_chain) as first-class keys, so we
    iterate those directly. Leaf types with frozenset() are skipped (no rows).
    """
    ag = asset_group.lower()
    venues = VENUES_BY_ASSET_GROUP.get(ag, [])
    instrument_types = _get_ag_instrument_types(ag)
    expected: set[tuple[str, str, str]] = set()

    for venue in venues:
        venue_caps = VENUE_DATA_TYPE_CAPABILITIES.get(venue, {})
        # For cefi: pre-compute MVP data_types override for this venue.
        cefi_mvp_dts: frozenset[str] | None = None
        if ag == "cefi":
            cefi_mvp_dts = get_mvp_data_types_for_cefi_venue(venue)

        for itype in instrument_types:
            valid_dts = VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE.get((ag, itype))
            if valid_dts is None:
                # unmapped instrument_type — skip (conservative; no over-fanning)
                continue
            if len(valid_dts) == 0:
                # frozenset() → this is a leaf type that rolls up to a bundle.
                # The bundle is already keyed under options_chain/futures_chain/combo
                # — iterating those directly handles it.  Do NOT emit any tuple here.
                continue

            # Check if the bundle_instrument_type_for_leaf maps this itype to a
            # bundle at this venue.  If so, we should only emit the bundle type,
            # not the leaf.  For cefi OPTION the valid_dts is already frozenset()
            # so we hit the continue above.  For cefi FUTURE at DERIBIT/OKX we
            # need the venue-specific check.
            bundle_it = bundle_instrument_type_for_leaf(ag, itype, venue)
            if bundle_it is not None and bundle_it != itype:
                # This leaf rolls up to a different bundle type — skip the leaf.
                # The bundle type (options_chain, futures_chain, combo) is iterated
                # separately under its own itype key.
                continue

            for dt in valid_dts:
                # Carve-out 1: venue cannot produce this data_type
                if dt not in venue_caps:
                    continue

                # Carve-out 2: cefi per-venue MVP override via get_mvp_data_types_for_cefi_venue.
                # NOTE: is_mvp() is NOT used here because it requires a base_ccy (instrument
                # grain) to return True for CeFi/TradFi.  The schema-level MVP gate is:
                #   - CeFi: get_mvp_data_types_for_cefi_venue(venue) → per-venue data_type set
                #   - TradFi: VENUES_BY_ASSET_GROUP already contains only MVP tradfi venues
                #             (CME); non-MVP venues are absent from the AG's venue set
                #   - DeFi/Sports/Prediction: VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE has
                #             no entries for these AGs → instrument_types is empty → no tuples
                if ag == "cefi" and cefi_mvp_dts is not None and dt not in cefi_mvp_dts:
                    continue

                expected.add((venue, itype, dt))

    return expected


def _build_enumerated_tuples(
    asset_group: str, df: pd.DataFrame
) -> set[tuple[str, str, str]]:
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


def check_enumeration_completeness(
    asset_group: str,
    df: pd.DataFrame,
) -> AgLayer1Result:
    """Check Layer-1 enumeration completeness for a single asset_group.

    Args:
        asset_group: One of cefi/defi/tradfi/sports/prediction.
        df: Merged manifest DataFrame for the asset_group (all 4 capture_status
            states present). Must include 'venue', 'data_type', 'capture_status'.
            'instrument_type' is required for a meaningful ENUMERATED set; if
            absent, all expected tuples will be reported as missing.

    Returns:
        AgLayer1Result with completeness_pct, missing_tuples, stray_tuples,
        denominator_complete, and per-venue breakdown.
    """
    ag = asset_group.lower()
    logger.info("  Layer-1: building EXPECTED matrix for %s …", ag)
    expected = _build_expected_tuples(ag)
    logger.info("  Layer-1 [%s]: EXPECTED = %d tuples", ag, len(expected))

    logger.info("  Layer-1: building ENUMERATED matrix for %s …", ag)
    enumerated = _build_enumerated_tuples(ag, df)
    logger.info("  Layer-1 [%s]: ENUMERATED = %d tuples", ag, len(enumerated))

    present = expected & enumerated
    missing_set = expected - enumerated
    stray_set = enumerated - expected

    n_expected = len(expected)
    n_present = len(present)
    completeness_pct = round(n_present / n_expected * 100, 2) if n_expected else 100.0
    denominator_complete = len(missing_set) == 0

    missing_tuples = [
        MissingTuple(venue=v, instrument_type=it, data_type=dt)
        for (v, it, dt) in sorted(missing_set)
    ]
    stray_tuples = [
        StrayTuple(venue=v, instrument_type=it, data_type=dt)
        for (v, it, dt) in sorted(stray_set)
    ]

    if stray_tuples:
        logger.warning(
            "  Layer-1 [%s]: %d stray tuples (writer emitting something UAC does not sanction): %s",
            ag,
            len(stray_tuples),
            [(s.venue, s.instrument_type, s.data_type) for s in stray_tuples[:5]],
        )

    if missing_tuples:
        logger.warning(
            "  Layer-1 [%s]: %d MISSING tuples (Layer-1 holes): first 5: %s",
            ag,
            len(missing_tuples),
            [(m.venue, m.instrument_type, m.data_type) for m in missing_tuples[:5]],
        )
    else:
        logger.info("  Layer-1 [%s]: denominator COMPLETE (0 holes)", ag)

    # Per-venue breakdown
    # Group expected and enumerated tuples by venue
    expected_by_venue: dict[str, set[tuple[str, str]]] = {}
    for v, it, dt in expected:
        expected_by_venue.setdefault(v, set()).add((it, dt))

    enumerated_by_venue: dict[str, set[tuple[str, str]]] = {}
    for v, it, dt in enumerated:
        enumerated_by_venue.setdefault(v, set()).add((it, dt))

    by_venue: dict[str, VenueCompleteness] = {}
    all_venues = sorted(set(expected_by_venue) | set(enumerated_by_venue))
    for venue in all_venues:
        v_exp = expected_by_venue.get(venue, set())
        v_enum = enumerated_by_venue.get(venue, set())
        v_present = v_exp & v_enum
        v_missing = v_exp - v_enum
        n_v_exp = len(v_exp)
        n_v_present = len(v_present)
        v_pct = round(n_v_present / n_v_exp * 100, 2) if n_v_exp else 100.0
        by_venue[venue] = VenueCompleteness(
            venue=venue,
            expected_tuples=n_v_exp,
            present_tuples=n_v_present,
            completeness_pct=v_pct,
            missing=[MissingTuple(venue=venue, instrument_type=it, data_type=dt) for (it, dt) in sorted(v_missing)],
        )

    return AgLayer1Result(
        asset_group=ag,
        denominator_complete=denominator_complete,
        completeness_pct=completeness_pct,
        expected_tuples=n_expected,
        present_tuples=n_present,
        missing_tuples=missing_tuples,
        stray_tuples=stray_tuples,
        by_venue=by_venue,
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
