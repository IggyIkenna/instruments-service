# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Layer-1 enumeration-completeness check for Honest Coverage v2.

For each asset_group, builds the EXPECTED matrix via
`expected_universe.build_expected(ag)` — the SINGLE public producer —
compares it to the ENUMERATED matrix (distinct tuples present in the
manifest skeleton), and returns per-node completeness metrics including
missing_tuples (Layer-1 holes) and stray_tuples (tuples in ENUMERATED but
not EXPECTED — warnings, not holes).

This is the CK2 implementation per the SSOT:
  codex/02-data/honest-coverage-model.md § Layer-1 enumeration-completeness matrix

EXPECTED-side authority + carve-outs live in `scripts/expected_universe.py`
(the D2a declarative-gate authority, VENUE_CAPABILITY_AGS carve-out,
per-venue MVP override, bundle-grain roll-up — all in that single module).
This file focuses on: (a) the vocabulary/grain alignment normaliser
(EXPECTED-side vs ENUMERATED-side canonicalisation), (b) the intersection +
diagnostics, (c) the empty-denominator UNDEFINED guard.

EMPTY-DENOMINATOR GUARD (HARD RULE, 2026-06-29):
  When EXPECTED == 0 for an AG, denominator_status = "UNDEFINED",
  denominator_complete = False, completeness_pct = None.  Reporting 100% over
  an empty set reproduces the v1 dishonesty v2 exists to kill.

Returns:
  Layer1Result with per-AG + per-venue breakdown.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

import pandas as pd
from unified_api_contracts import bundle_instrument_type_for_leaf
from unified_api_contracts.registry.venue_mapping import VenueMapping

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sibling-load expected_universe.py — the SINGLE public producer of the
# EXPECTED Layer-1 universe.  Every caller of `_build_expected_tuples` /
# `build_expected` in this file routes through the sibling module, so there
# is exactly ONE producer feeding the Layer-1 matrix (no drift).
#
# Mirrors measure_honest_coverage._load_completeness_module — the local
# convention for cross-script imports (scripts/ is not a python package).
# ---------------------------------------------------------------------------


def _load_expected_universe() -> ModuleType:
    """Load expected_universe.py from the sibling scripts/ directory."""
    module_name = "_expected_universe"
    if module_name in sys.modules:
        return sys.modules[module_name]
    script_dir = Path(__file__).resolve().parent
    path = script_dir / "expected_universe.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_EU = _load_expected_universe()

# Public re-exports so callers can import from either module.
build_expected = _EU.build_expected
VENUE_CAPABILITY_AGS: frozenset[str] = _EU.VENUE_CAPABILITY_AGS

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

def _get_instrument_type_aliases() -> dict[str, str]:
    """Return the UAC SSOT instrument_type alias map (lowercase token → canonical).

    Delegator — see expected_universe._get_instrument_type_aliases.  The cache
    lives on the sibling module so both sides of the intersection (EXPECTED
    builder + ENUMERATED-side canonicalisation here) share one source.
    """
    return _EU._get_instrument_type_aliases()


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


# The producer-side helpers (_get_defi_instrument_types, _get_cefi_venue_itypes,
# _get_defi_protocol_itypes, _venue_itype_is_valid, _get_ag_instrument_types)
# moved to expected_universe.py as part of the single-producer consolidation
# (A17 / cefi Layer-1 plan 2a).  Callers here go through `build_expected` (the
# _EU re-export at the top of this file); nothing in the codebase referenced
# those private helpers outside the EXPECTED-side path.


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

    expected_only: list[tuple[str, str, str]]  # canonical keys in EXPECTED - ENUMERATED
    enumerated_only: list[tuple[str, str, str]]  # canonical keys in ENUMERATED - EXPECTED
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
    """Delegator — see expected_universe._expected_sports (via build_expected).

    Kept for API compat; direct sports access folds into build_expected("sports").
    """
    return build_expected("sports")


def _build_expected_tuples(asset_group: str) -> set[tuple[str, str, str]]:
    """Delegator — see expected_universe.build_expected.

    Kept for API compat with the existing test suite; new code should call
    build_expected() directly.  The producer body moved to expected_universe.py
    as part of the single-producer consolidation (A17 / cefi Layer-1 plan 2a) —
    two producers = two chances at silent denominator drift, which is the
    failure mode Honest Coverage v2 exists to kill.
    """
    return build_expected(asset_group)


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


def filter_manifest_to_expected(
    asset_group: str,
    df: pd.DataFrame,
    *,
    expected: set[tuple[str, str, str]] | None = None,
) -> pd.DataFrame:
    """MVP read-time gate — keep only manifest rows whose canonical
    ``(venue, instrument_type, data_type)`` key is in ``EXPECTED``.

    The retired manifest-pruning script `reclassify_cefi_manifest_mvp_universe_2026_06_23.py`
    is the WRONG shape — its `_derive_base` mis-parses Bitfinex `ADAF0:USTF0`
    + Kraken `PF_/PI_` wire-forms and would DELETE ~380k legit captured
    BITFINEX/KRAKEN rows, and it derives the denominator from the manifest
    (honest-coverage-v2 forbids this circular reference).

    This function is the READ-TIME replacement: no manifest rows are ever
    mutated (the returned DataFrame is a filtered VIEW of the input; the
    input is unchanged). The gate uses `build_expected(asset_group)` as the
    oracle, and `build_expected` already applies the CeFi MVP filter via
    `get_mvp_data_types_for_cefi_venue`, plus the D2a `INSTRUMENT_TYPES_BY_VENUE`
    gate + `VENUE_DATA_TYPE_CAPABILITIES` carve-out. So calling
    `filter_manifest_to_expected("cefi", df)` yields a Layer-2 denominator
    that matches the Layer-1 EXPECTED denominator — MVP-scoped, honestly.

    Comparison is on the SAME canonical key `check_enumeration_completeness`
    uses (`_canon_key`) so a manifest row whose (venue, itype, dt) folds to
    the same key as an EXPECTED tuple is kept even if the raw tokens differ
    (e.g. OKX-SPOT manifest row folds to OKX canonical venue).

    Args:
        asset_group: cefi/defi/tradfi/sports/prediction.
        df: the manifest DataFrame (must have ``venue``, ``instrument_type``,
            ``data_type`` columns; degrades gracefully when a column is missing
            by returning the df unchanged + a WARNING log).
        expected: optional pre-computed EXPECTED tuple set. When None,
            ``build_expected(asset_group)`` is called.

    Returns:
        Filtered DataFrame containing only in-scope rows (input df is
        unchanged; rows are neither reordered nor mutated).
    """
    required_cols = {"venue", "instrument_type", "data_type"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        logger.warning(
            "  filter_manifest_to_expected [%s]: missing columns %s — "
            "returning df unchanged (gate cannot apply)",
            asset_group,
            sorted(missing_cols),
        )
        return df

    exp = expected if expected is not None else _build_expected_tuples(asset_group)
    exp_by_key = _canonicalise_tuple_set(asset_group, exp)
    exp_keys = set(exp_by_key)

    # Canonicalise ONLY the unique (venue, itype, dt) triples in the manifest —
    # single-pass computation, O(unique_triples) not O(rows).  The cefi index
    # is tens-of-millions of rows but only a few hundred unique triples.
    triples = df[["venue", "instrument_type", "data_type"]].drop_duplicates()
    in_scope: set[tuple[str, str, str]] = set()
    for row in triples.itertuples(index=False):
        venue = "" if pd.isna(row.venue) else str(row.venue)
        itype = "" if pd.isna(row.instrument_type) else str(row.instrument_type)
        dt = "" if pd.isna(row.data_type) else str(row.data_type)
        key = _canon_key(asset_group, venue, itype, dt)
        if key in exp_keys:
            in_scope.add((row.venue, row.instrument_type, row.data_type))

    if not in_scope:
        logger.warning(
            "  filter_manifest_to_expected [%s]: no manifest triples in EXPECTED — "
            "gate would drop every row.  Returning EMPTY df (0 rows).",
            asset_group,
        )
        return df.iloc[0:0].copy()

    # Row-level mask via O(1) set membership.  itertuples-over-3-cols is fast
    # in CPython (list-of-tuples membership check) and avoids the merge-reorder.
    mask = pd.Series(
        [(v, it, dt) in in_scope for v, it, dt in zip(df["venue"], df["instrument_type"], df["data_type"], strict=False)],
        index=df.index,
    )
    filtered = df.loc[mask].reset_index(drop=True)
    logger.info(
        "  filter_manifest_to_expected [%s]: kept %d/%d rows (%.1f%%) — "
        "%d/%d unique triples in EXPECTED",
        asset_group,
        len(filtered),
        len(df),
        (len(filtered) / len(df) * 100) if len(df) else 100.0,
        len(in_scope),
        len(triples),
    )
    return filtered
