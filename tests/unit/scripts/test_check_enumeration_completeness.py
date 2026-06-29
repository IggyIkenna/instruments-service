"""Unit tests for check_enumeration_completeness.py — Layer-1 enumeration completeness.

Tests cover:
  - Carve-out table assertions (each expected-absence row from the codex table is NOT a hole)
  - Deribit options_chain bundle IS a hole when absent from ENUMERATED
  - ASTER missing book_snapshot_5 / liquidations is NOT a hole
  - HYPERLIQUID missing liquidations is NOT a hole
  - UPBIT derivative_ticker absent (capability carve-out) is NOT a hole
  - completeness_pct and denominator_complete behaviour
  - stray_tuples logged as warnings (not holes)
  - Empty-denominator guard: EXPECTED==0 → denominator_status UNDEFINED, completeness_pct None
  - DeFi regression: EXPECTED > 0 after switching from raw dict to UAC functions

Plan: honest_coverage_v2_instrument_denominator_2026_06_28.md Phase 1 IMPL
SSOT: codex/02-data/honest-coverage-model.md § Carve-outs
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module loader (mirrors the enumerator test pattern)
# ---------------------------------------------------------------------------

def _load_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "check_enumeration_completeness.py"
    module_name = "_check_enumeration_completeness_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod() -> ModuleType:
    return _load_module()


# ---------------------------------------------------------------------------
# Helper: make a minimal manifest DataFrame for ENUMERATED
# ---------------------------------------------------------------------------

def _make_manifest(rows: list[dict[str, str]]) -> pd.DataFrame:
    """Return a DataFrame with the columns the checker expects."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Test: carve-out — DERIBIT options → options_chain bundle, NOT per-leg
#
# The codex table says:
#   cefi | DERIBIT options → ``options_chain`` bundle (data_type=trades), no per-leg
#   Source: VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE[(cefi,option)]=frozenset()
#           + FUTURE_BUNDLE_VENUES
#
# Layer-1 check consequence:
#   The EXPECTED matrix for cefi NEVER contains (DERIBIT, option, *) tuples
#   (because frozenset() → skip leaf entirely and the leaf rolls up to options_chain).
#   It SHOULD contain (DERIBIT, options_chain, trades).
# ---------------------------------------------------------------------------

class TestDeribitOptionsChainCarveOut:
    def test_deribit_option_leaf_is_not_in_expected(self, mod: ModuleType) -> None:
        """OPTION leaf rows never appear in EXPECTED for DERIBIT — they roll up to options_chain."""
        expected = mod._build_expected_tuples("cefi")
        # No (DERIBIT, option, *) tuples should be in EXPECTED
        deribit_option_tuples = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "DERIBIT" and it == "option"
        }
        assert len(deribit_option_tuples) == 0, (
            f"DERIBIT leaf option tuples should NOT be in EXPECTED (roll up to options_chain bundle): "
            f"{deribit_option_tuples}"
        )

    def test_deribit_options_chain_trades_is_in_expected(self, mod: ModuleType) -> None:
        """(DERIBIT, options_chain, trades) MUST be in EXPECTED."""
        expected = mod._build_expected_tuples("cefi")
        assert ("DERIBIT", "options_chain", "trades") in expected, (
            "DERIBIT options_chain bundle (data_type=trades) must be in EXPECTED"
        )

    def test_deribit_options_chain_is_a_hole_when_absent_from_enumerated(
        self, mod: ModuleType
    ) -> None:
        """(DERIBIT, options_chain, trades) is a Layer-1 hole when absent from ENUMERATED."""
        # Manifest has NO options_chain rows for DERIBIT
        df = _make_manifest([
            {"capture_status": "captured", "venue": "DERIBIT", "instrument_type": "spot_pair", "data_type": "trades"},
            {"capture_status": "captured", "venue": "BINANCE-FUTURES", "instrument_type": "perpetual", "data_type": "trades"},
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        # options_chain for DERIBIT should be a missing tuple
        missing = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert ("DERIBIT", "options_chain", "trades") in missing, (
            f"(DERIBIT, options_chain, trades) must be a Layer-1 hole when absent. "
            f"Missing tuples (sample): {list(missing)[:5]}"
        )
        assert not result.denominator_complete

    def test_deribit_options_chain_not_a_hole_when_present_in_enumerated(
        self, mod: ModuleType
    ) -> None:
        """(DERIBIT, options_chain, trades) is NOT a hole when present in ENUMERATED."""
        # Manifest has options_chain row for DERIBIT
        df = _make_manifest([
            {
                "capture_status": "captured",
                "venue": "DERIBIT",
                "instrument_type": "options_chain",
                "data_type": "trades",
            },
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        missing = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert ("DERIBIT", "options_chain", "trades") not in missing, (
            "(DERIBIT, options_chain, trades) must NOT be a hole when present in ENUMERATED"
        )


# ---------------------------------------------------------------------------
# Test: carve-out — ASTER has no book_snapshot_5, no liquidations
#   Source: absent from VENUE_DATA_TYPE_CAPABILITIES["ASTER"]
# ---------------------------------------------------------------------------

class TestAsterCarveOut:
    def test_aster_book_snapshot_5_not_in_expected(self, mod: ModuleType) -> None:
        """book_snapshot_5 is NOT expected for ASTER (absent from VENUE_DATA_TYPE_CAPABILITIES)."""
        expected = mod._build_expected_tuples("cefi")
        aster_bs5 = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "ASTER" and dt == "book_snapshot_5"
        }
        assert len(aster_bs5) == 0, (
            f"ASTER book_snapshot_5 should NOT be in EXPECTED (venue capability absent): {aster_bs5}"
        )

    def test_aster_liquidations_not_in_expected(self, mod: ModuleType) -> None:
        """liquidations is NOT expected for ASTER (absent from VENUE_DATA_TYPE_CAPABILITIES)."""
        expected = mod._build_expected_tuples("cefi")
        aster_liq = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "ASTER" and dt == "liquidations"
        }
        assert len(aster_liq) == 0, (
            f"ASTER liquidations should NOT be in EXPECTED (venue capability absent): {aster_liq}"
        )

    def test_aster_book_snapshot_5_absent_from_manifest_is_not_a_hole(
        self, mod: ModuleType
    ) -> None:
        """A manifest with no ASTER book_snapshot_5 rows has 0 missing for that tuple."""
        df = _make_manifest([
            {"capture_status": "captured", "venue": "ASTER", "instrument_type": "perpetual", "data_type": "trades"},
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        aster_bs5_missing = [
            m for m in result.missing_tuples
            if m.venue == "ASTER" and m.data_type == "book_snapshot_5"
        ]
        assert len(aster_bs5_missing) == 0, (
            "ASTER book_snapshot_5 should NOT be a Layer-1 hole (it is a carve-out)"
        )


# ---------------------------------------------------------------------------
# Test: carve-out — HYPERLIQUID has no liquidations
#   Source: absent from VENUE_DATA_TYPE_CAPABILITIES["HYPERLIQUID"]
# ---------------------------------------------------------------------------

class TestHyperliquidCarveOut:
    def test_hyperliquid_liquidations_not_in_expected(self, mod: ModuleType) -> None:
        """liquidations is NOT expected for HYPERLIQUID (absent from VENUE_DATA_TYPE_CAPABILITIES)."""
        expected = mod._build_expected_tuples("cefi")
        hl_liq = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "HYPERLIQUID" and dt == "liquidations"
        }
        assert len(hl_liq) == 0, (
            f"HYPERLIQUID liquidations should NOT be in EXPECTED (venue capability absent): {hl_liq}"
        )

    def test_hyperliquid_liquidations_absent_from_manifest_is_not_a_hole(
        self, mod: ModuleType
    ) -> None:
        """No Layer-1 hole for HYPERLIQUID liquidations even when absent from manifest."""
        df = _make_manifest([
            {
                "capture_status": "captured",
                "venue": "HYPERLIQUID",
                "instrument_type": "perpetual",
                "data_type": "trades",
            },
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        hl_liq_missing = [
            m for m in result.missing_tuples
            if m.venue == "HYPERLIQUID" and m.data_type == "liquidations"
        ]
        assert len(hl_liq_missing) == 0, (
            "HYPERLIQUID liquidations must NOT be a Layer-1 hole (carve-out from VENUE_DATA_TYPE_CAPABILITIES)"
        )


# ---------------------------------------------------------------------------
# Test: carve-out — venue MVP override via get_mvp_data_types_for_cefi_venue
#
# UPBIT: capability set = {book_snapshot_5, trades} only.  derivative_ticker and
#   funding_rate are in the global MVP data_types but NOT in UPBIT capabilities →
#   filtered by Carve-out 1 (venue capability absent).
# BINANCE-DELIVERY: get_mvp_data_types_for_cefi_venue returns frozenset() →
#   ALL data_types filtered by Carve-out 2 (empty per-venue MVP override).
#   Source: get_mvp_data_types_for_cefi_venue
# ---------------------------------------------------------------------------

class TestCoinbaseSpotCarveOut:
    def test_upbit_derivative_ticker_not_in_expected(self, mod: ModuleType) -> None:
        """derivative_ticker is NOT expected for UPBIT (absent from VENUE_DATA_TYPE_CAPABILITIES)."""
        expected = mod._build_expected_tuples("cefi")
        upbit_dt = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "UPBIT" and dt == "derivative_ticker"
        }
        assert len(upbit_dt) == 0, (
            f"UPBIT derivative_ticker should NOT be in EXPECTED (venue capability absent): {upbit_dt}"
        )

    def test_upbit_trades_is_in_expected(self, mod: ModuleType) -> None:
        """trades IS expected for UPBIT (in both venue capabilities and MVP)."""
        expected = mod._build_expected_tuples("cefi")
        upbit_trades = {
            (v, it, dt) for (v, it, dt) in expected
            if v == "UPBIT" and dt == "trades"
        }
        assert len(upbit_trades) > 0, "UPBIT should have at least one (venue, itype, trades) tuple in EXPECTED"

    def test_upbit_derivative_ticker_absent_is_not_a_hole(self, mod: ModuleType) -> None:
        """A manifest without UPBIT derivative_ticker is NOT a Layer-1 hole."""
        df = _make_manifest([
            {
                "capture_status": "captured",
                "venue": "UPBIT",
                "instrument_type": "spot_pair",
                "data_type": "trades",
            },
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        upbit_dt_missing = [
            m for m in result.missing_tuples
            if m.venue == "UPBIT" and m.data_type == "derivative_ticker"
        ]
        assert len(upbit_dt_missing) == 0, (
            "UPBIT derivative_ticker must NOT be a Layer-1 hole (venue capability carve-out)"
        )


# ---------------------------------------------------------------------------
# Test: denominator_complete and completeness_pct semantics
# ---------------------------------------------------------------------------

class TestCompletenessMetrics:
    def test_denominator_complete_when_all_expected_present(self, mod: ModuleType) -> None:
        """denominator_complete = True when ENUMERATED ⊇ EXPECTED (no holes)."""
        # Build the full expected set and provide all of them in the manifest.
        expected = mod._build_expected_tuples("cefi")
        rows = [
            {
                "capture_status": "captured",
                "venue": v,
                "instrument_type": it,
                "data_type": dt,
            }
            for (v, it, dt) in expected
        ]
        if not rows:
            pytest.skip("cefi expected set is empty — cannot assert completeness")
        df = _make_manifest(rows)
        result = mod.check_enumeration_completeness("cefi", df)
        assert result.denominator_complete, (
            f"denominator_complete must be True when all expected tuples are present. "
            f"Missing: {result.missing_tuples[:3]}"
        )
        assert result.completeness_pct == 100.0
        assert len(result.missing_tuples) == 0

    def test_denominator_incomplete_when_expected_tuple_missing(self, mod: ModuleType) -> None:
        """denominator_complete = False when any expected tuple is absent from ENUMERATED."""
        # Empty manifest → all expected tuples are missing
        df = _make_manifest([])
        result = mod.check_enumeration_completeness("cefi", df)
        # cefi has a non-trivial EXPECTED set; if it's empty, skip
        if mod._build_expected_tuples("cefi"):
            assert not result.denominator_complete
            assert result.completeness_pct < 100.0
            assert len(result.missing_tuples) > 0

    def test_completeness_pct_is_correct_fraction(self, mod: ModuleType) -> None:
        """completeness_pct = |EXPECTED ∩ ENUMERATED| / |EXPECTED| * 100."""
        expected = mod._build_expected_tuples("cefi")
        if not expected:
            pytest.skip("Empty EXPECTED — cannot compute pct")

        # Take half the expected tuples
        half = list(expected)[: len(expected) // 2]
        rows = [
            {
                "capture_status": "expected_unattempted",
                "venue": v,
                "instrument_type": it,
                "data_type": dt,
            }
            for (v, it, dt) in half
        ]
        df = _make_manifest(rows)
        result = mod.check_enumeration_completeness("cefi", df)
        expected_pct = round(len(half) / len(expected) * 100, 2)
        assert abs(result.completeness_pct - expected_pct) < 0.1, (
            f"completeness_pct {result.completeness_pct} != expected {expected_pct}"
        )

    def test_missing_instrument_type_column_yields_empty_enumerated(
        self, mod: ModuleType
    ) -> None:
        """If the 'instrument_type' column is absent, ENUMERATED = empty → all expected missing."""
        df = pd.DataFrame([
            {"capture_status": "captured", "venue": "DERIBIT", "data_type": "trades"},
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        # Without instrument_type, ENUMERATED is empty → all expected are holes
        expected = mod._build_expected_tuples("cefi")
        if expected:
            assert len(result.missing_tuples) == len(expected)
            assert not result.denominator_complete


# ---------------------------------------------------------------------------
# Test: stray tuples are logged as warnings, NOT counted as holes
# ---------------------------------------------------------------------------

class TestStrayTuples:
    def test_stray_tuple_not_counted_as_hole(self, mod: ModuleType) -> None:
        """A tuple in ENUMERATED but not in EXPECTED is a stray, NOT a missing_tuple."""
        # Fabricate a tuple that UAC cannot sanction (e.g. ASTER + liquidations)
        df = _make_manifest([
            # Stray: ASTER has no liquidations capability
            {
                "capture_status": "captured",
                "venue": "ASTER",
                "instrument_type": "perpetual",
                "data_type": "liquidations",
            },
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        # The stray must NOT appear in missing_tuples
        missing_dtypes = {(m.venue, m.data_type) for m in result.missing_tuples}
        assert ("ASTER", "liquidations") not in missing_dtypes, (
            "Stray tuple (ASTER, perpetual, liquidations) must NOT be in missing_tuples"
        )
        # It should appear in stray_tuples
        stray_dtypes = {(s.venue, s.data_type) for s in result.stray_tuples}
        assert ("ASTER", "liquidations") in stray_dtypes, (
            "Stray tuple (ASTER, perpetual, liquidations) must appear in stray_tuples"
        )

    def test_valid_tuple_not_in_stray(self, mod: ModuleType) -> None:
        """A tuple that IS in EXPECTED is never a stray (even if also in ENUMERATED)."""
        # Pick a known expected tuple for cefi
        expected = mod._build_expected_tuples("cefi")
        if not expected:
            pytest.skip("EXPECTED is empty")
        v, it, dt = next(iter(expected))
        df = _make_manifest([
            {"capture_status": "captured", "venue": v, "instrument_type": it, "data_type": dt},
        ])
        result = mod.check_enumeration_completeness("cefi", df)
        stray_keys = {(s.venue, s.instrument_type, s.data_type) for s in result.stray_tuples}
        assert (v, it, dt) not in stray_keys, f"Expected tuple ({v},{it},{dt}) must not be a stray"


# ---------------------------------------------------------------------------
# Test: per-venue breakdown is consistent with top-level
# ---------------------------------------------------------------------------

class TestVenueBreakdown:
    def test_by_venue_sums_correctly(self, mod: ModuleType) -> None:
        """Sum of per-venue expected_tuples equals the AG expected_tuples."""
        expected = mod._build_expected_tuples("cefi")
        rows = [
            {
                "capture_status": "captured",
                "venue": v,
                "instrument_type": it,
                "data_type": dt,
            }
            for (v, it, dt) in expected
        ]
        if not rows:
            pytest.skip("Empty EXPECTED")
        df = _make_manifest(rows)
        result = mod.check_enumeration_completeness("cefi", df)
        venue_total = sum(vc.expected_tuples for vc in result.by_venue.values())
        assert venue_total == result.expected_tuples, (
            f"Venue expected_tuples sum {venue_total} != AG expected_tuples {result.expected_tuples}"
        )


# ---------------------------------------------------------------------------
# Test: empty-denominator guard — EXPECTED==0 → UNDEFINED, not 100% (Bug 2)
# ---------------------------------------------------------------------------

class TestEmptyDenominatorGuard:
    def test_undefined_status_when_expected_is_empty(self, mod: ModuleType) -> None:
        """When _build_expected_tuples returns empty, check_enumeration_completeness
        must set denominator_status='UNDEFINED', denominator_complete=False, and
        completeness_pct=None.  A fictitious AG name guarantees EXPECTED==0.
        """
        # Use a non-existent AG so both VENUES_BY_ASSET_GROUP and
        # VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE return nothing → EXPECTED=0.
        df = _make_manifest([
            {"capture_status": "captured", "venue": "MOCK", "instrument_type": "spot", "data_type": "trades"},
        ])
        result = mod.check_enumeration_completeness("_nonexistent_ag_for_test_", df)
        assert result.expected_tuples == 0, "Expected 0 tuples for nonexistent AG"
        assert result.denominator_status == "UNDEFINED", (
            f"denominator_status must be UNDEFINED when EXPECTED==0, got {result.denominator_status!r}"
        )
        assert result.denominator_complete is False, (
            "denominator_complete must be False for UNDEFINED denominator"
        )
        assert result.completeness_pct is None, (
            f"completeness_pct must be None (not 100.0) for UNDEFINED denominator, "
            f"got {result.completeness_pct}"
        )

    def test_complete_status_when_all_present(self, mod: ModuleType) -> None:
        """denominator_status='COMPLETE' when all expected tuples present."""
        expected = mod._build_expected_tuples("cefi")
        if not expected:
            pytest.skip("cefi EXPECTED is empty — UAC not loaded")
        rows = [
            {"capture_status": "captured", "venue": v, "instrument_type": it, "data_type": dt}
            for (v, it, dt) in expected
        ]
        df = _make_manifest(rows)
        result = mod.check_enumeration_completeness("cefi", df)
        assert result.denominator_status == "COMPLETE", (
            f"expected 'COMPLETE' got {result.denominator_status!r}"
        )
        assert result.completeness_pct == 100.0
        assert result.denominator_complete is True

    def test_incomplete_status_when_tuples_missing(self, mod: ModuleType) -> None:
        """denominator_status='INCOMPLETE' when expected tuples are missing."""
        expected = mod._build_expected_tuples("cefi")
        if not expected:
            pytest.skip("cefi EXPECTED is empty — UAC not loaded")
        df = _make_manifest([])  # Empty manifest → all expected missing
        result = mod.check_enumeration_completeness("cefi", df)
        assert result.denominator_status == "INCOMPLETE", (
            f"expected 'INCOMPLETE' got {result.denominator_status!r}"
        )
        assert result.denominator_complete is False
        assert result.completeness_pct is not None
        assert result.completeness_pct == 0.0


# ---------------------------------------------------------------------------
# Test: DeFi regression — EXPECTED > 0 after UAC function switch (Bug 1)
# ---------------------------------------------------------------------------

class TestDefiExpectedNotEmpty:
    def test_defi_expected_tuples_gt_zero(self, mod: ModuleType) -> None:
        """Regression: _build_expected_tuples('defi') must return a non-empty set.

        The old implementation indexed VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE
        (no defi keys → EXPECTED=0 → false 100% complete).  The fix uses the UAC
        valid_data_types_for_venue_instrument_type / valid_data_types_for_instrument_type
        functions which build defi validity from PROTOCOL_CAPABILITIES dynamically.
        """
        expected = mod._build_expected_tuples("defi")
        assert len(expected) > 0, (
            f"BUG 1 REGRESSION: defi EXPECTED is empty (got {len(expected)}). "
            "Check that valid_data_types_for_venue_instrument_type / "
            "valid_data_types_for_instrument_type are used (not the raw dict)."
        )

    def test_defi_check_completeness_not_undefined(self, mod: ModuleType) -> None:
        """When defi EXPECTED > 0, check_enumeration_completeness must not set UNDEFINED."""
        from unified_api_contracts import VENUES_BY_ASSET_GROUP
        defi_venues = list(VENUES_BY_ASSET_GROUP.get("defi", []))
        if not defi_venues:
            pytest.skip("No defi venues in UAC — cannot build manifest rows")
        # Provide an empty manifest — should be INCOMPLETE (missing tuples), not UNDEFINED
        df = _make_manifest([])
        result = mod.check_enumeration_completeness("defi", df)
        assert result.denominator_status != "UNDEFINED", (
            f"defi should never be UNDEFINED (EXPECTED={result.expected_tuples}), "
            f"got denominator_status={result.denominator_status!r}"
        )
        assert result.expected_tuples > 0, (
            f"defi EXPECTED must be > 0, got {result.expected_tuples}"
        )
