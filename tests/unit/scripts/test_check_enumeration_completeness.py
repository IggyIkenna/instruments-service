"""Unit tests for check_enumeration_completeness.py — Layer-1 enumeration completeness.

Tests cover:
  - Carve-out table assertions (each expected-absence row from the codex table is NOT a hole)
  - Deribit options_chain bundle IS a hole when absent from ENUMERATED
  - ASTER book_snapshot_5 IS a hole when absent (live-wire capability, uac@3652f99f); ASTER liquidations still not in MVP
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
        deribit_option_tuples = {(v, it, dt) for (v, it, dt) in expected if v == "DERIBIT" and it == "option"}
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

    def test_deribit_options_chain_is_a_hole_when_absent_from_enumerated(self, mod: ModuleType) -> None:
        """(DERIBIT, options_chain, trades) is a Layer-1 hole when absent from ENUMERATED."""
        # Manifest has NO options_chain rows for DERIBIT
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "DERIBIT",
                    "instrument_type": "spot_pair",
                    "data_type": "trades",
                },
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        # options_chain for DERIBIT should be a missing tuple
        missing = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert ("DERIBIT", "options_chain", "trades") in missing, (
            f"(DERIBIT, options_chain, trades) must be a Layer-1 hole when absent. "
            f"Missing tuples (sample): {list(missing)[:5]}"
        )
        assert not result.denominator_complete

    def test_deribit_options_chain_not_a_hole_when_present_in_enumerated(self, mod: ModuleType) -> None:
        """(DERIBIT, options_chain, trades) is NOT a hole when present in ENUMERATED."""
        # Manifest has options_chain row for DERIBIT
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "DERIBIT",
                    "instrument_type": "options_chain",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        missing = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert ("DERIBIT", "options_chain", "trades") not in missing, (
            "(DERIBIT, options_chain, trades) must NOT be a hole when present in ENUMERATED"
        )


# ---------------------------------------------------------------------------
# Test: ASTER capability profile — book_snapshot_5 is live-wire from 2026-06-23,
# liquidations still carved out (not in MVP scope).
#   Source: VENUE_DATA_TYPE_CAPABILITIES["ASTER"] (uac@3652f99f 2026-07-07,
#   cefi_layer1_denominator_gaps-008 UAC ASTER capability flip).
# ---------------------------------------------------------------------------


class TestAsterCapabilities:
    def test_aster_book_snapshot_5_is_in_expected(self, mod: ModuleType) -> None:
        """book_snapshot_5 IS expected for ASTER (live-wire capability landed uac@3652f99f, MVP includes it)."""
        expected = mod._build_expected_tuples("cefi")
        aster_bs5 = {(v, it, dt) for (v, it, dt) in expected if v == "ASTER" and dt == "book_snapshot_5"}
        assert aster_bs5 == {("ASTER", "perpetual", "book_snapshot_5")}, (
            f"ASTER book_snapshot_5 should be in EXPECTED (live-wire capability from 2026-06-23): {aster_bs5}"
        )

    def test_aster_liquidations_not_in_expected(self, mod: ModuleType) -> None:
        """liquidations is NOT expected for ASTER — capability present but liquidations not in MVP scope."""
        expected = mod._build_expected_tuples("cefi")
        aster_liq = {(v, it, dt) for (v, it, dt) in expected if v == "ASTER" and dt == "liquidations"}
        assert len(aster_liq) == 0, f"ASTER liquidations should NOT be in EXPECTED (not in MVP scope): {aster_liq}"

    def test_aster_book_snapshot_5_absent_from_manifest_is_a_hole(self, mod: ModuleType) -> None:
        """A manifest with no ASTER book_snapshot_5 rows now surfaces as a Layer-1 hole (live-wire capability)."""
        df = _make_manifest(
            [
                {"capture_status": "captured", "venue": "ASTER", "instrument_type": "perpetual", "data_type": "trades"},
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        aster_bs5_missing = [
            m for m in result.missing_tuples if m.venue == "ASTER" and m.data_type == "book_snapshot_5"
        ]
        assert len(aster_bs5_missing) == 1, "ASTER book_snapshot_5 IS a Layer-1 hole when absent from manifest"


# ---------------------------------------------------------------------------
# Test: carve-out — HYPERLIQUID has no liquidations
#   Source: absent from VENUE_DATA_TYPE_CAPABILITIES["HYPERLIQUID"]
# ---------------------------------------------------------------------------


class TestHyperliquidCarveOut:
    def test_hyperliquid_liquidations_not_in_expected(self, mod: ModuleType) -> None:
        """liquidations is NOT expected for HYPERLIQUID (absent from VENUE_DATA_TYPE_CAPABILITIES)."""
        expected = mod._build_expected_tuples("cefi")
        hl_liq = {(v, it, dt) for (v, it, dt) in expected if v == "HYPERLIQUID" and dt == "liquidations"}
        assert len(hl_liq) == 0, (
            f"HYPERLIQUID liquidations should NOT be in EXPECTED (venue capability absent): {hl_liq}"
        )

    def test_hyperliquid_liquidations_absent_from_manifest_is_not_a_hole(self, mod: ModuleType) -> None:
        """No Layer-1 hole for HYPERLIQUID liquidations even when absent from manifest."""
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "HYPERLIQUID",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        hl_liq_missing = [
            m for m in result.missing_tuples if m.venue == "HYPERLIQUID" and m.data_type == "liquidations"
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
        upbit_dt = {(v, it, dt) for (v, it, dt) in expected if v == "UPBIT" and dt == "derivative_ticker"}
        assert len(upbit_dt) == 0, (
            f"UPBIT derivative_ticker should NOT be in EXPECTED (venue capability absent): {upbit_dt}"
        )

    def test_upbit_trades_is_in_expected(self, mod: ModuleType) -> None:
        """trades IS expected for UPBIT (in both venue capabilities and MVP)."""
        expected = mod._build_expected_tuples("cefi")
        upbit_trades = {(v, it, dt) for (v, it, dt) in expected if v == "UPBIT" and dt == "trades"}
        assert len(upbit_trades) > 0, "UPBIT should have at least one (venue, itype, trades) tuple in EXPECTED"

    def test_upbit_derivative_ticker_absent_is_not_a_hole(self, mod: ModuleType) -> None:
        """A manifest without UPBIT derivative_ticker is NOT a Layer-1 hole."""
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "UPBIT",
                    "instrument_type": "spot_pair",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        upbit_dt_missing = [
            m for m in result.missing_tuples if m.venue == "UPBIT" and m.data_type == "derivative_ticker"
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

    def test_missing_instrument_type_column_yields_empty_enumerated(self, mod: ModuleType) -> None:
        """If the 'instrument_type' column is absent, ENUMERATED = empty → all expected missing."""
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "venue": "DERIBIT", "data_type": "trades"},
            ]
        )
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
        df = _make_manifest(
            [
                # Stray: ASTER has no liquidations capability
                {
                    "capture_status": "captured",
                    "venue": "ASTER",
                    "instrument_type": "perpetual",
                    "data_type": "liquidations",
                },
            ]
        )
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
        df = _make_manifest(
            [
                {"capture_status": "captured", "venue": v, "instrument_type": it, "data_type": dt},
            ]
        )
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
        df = _make_manifest(
            [
                {"capture_status": "captured", "venue": "MOCK", "instrument_type": "spot", "data_type": "trades"},
            ]
        )
        result = mod.check_enumeration_completeness("_nonexistent_ag_for_test_", df)
        assert result.expected_tuples == 0, "Expected 0 tuples for nonexistent AG"
        assert result.denominator_status == "UNDEFINED", (
            f"denominator_status must be UNDEFINED when EXPECTED==0, got {result.denominator_status!r}"
        )
        assert result.denominator_complete is False, "denominator_complete must be False for UNDEFINED denominator"
        assert result.completeness_pct is None, (
            f"completeness_pct must be None (not 100.0) for UNDEFINED denominator, got {result.completeness_pct}"
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
        assert result.denominator_status == "COMPLETE", f"expected 'COMPLETE' got {result.denominator_status!r}"
        assert result.completeness_pct == 100.0
        assert result.denominator_complete is True

    def test_incomplete_status_when_tuples_missing(self, mod: ModuleType) -> None:
        """denominator_status='INCOMPLETE' when expected tuples are missing."""
        expected = mod._build_expected_tuples("cefi")
        if not expected:
            pytest.skip("cefi EXPECTED is empty — UAC not loaded")
        df = _make_manifest([])  # Empty manifest → all expected missing
        result = mod.check_enumeration_completeness("cefi", df)
        assert result.denominator_status == "INCOMPLETE", f"expected 'INCOMPLETE' got {result.denominator_status!r}"
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
        assert result.expected_tuples > 0, f"defi EXPECTED must be > 0, got {result.expected_tuples}"


# ---------------------------------------------------------------------------
# Test: VOCABULARY/GRAIN ALIGNMENT (Bug 3) — both sides normalised before
# intersect; casing/format/vocab differences are NOT holes; only REAL holes.
# ---------------------------------------------------------------------------


class TestCanonNormalisers:
    def test_case_fold_instrument_type(self, mod: ModuleType) -> None:
        """UPPERCASE and lowercase instrument_type canonicalise to the same key."""
        assert mod._canon_instrument_type("cefi", "BINANCE-SPOT", "SPOT_PAIR") == mod._canon_instrument_type(
            "cefi", "BINANCE-SPOT", "spot_pair"
        )

    def test_alias_spot_to_spot_pair(self, mod: ModuleType) -> None:
        """Writer-grain `spot` aliases to UAC `spot_pair`."""
        assert mod._canon_instrument_type("cefi", "BINANCE-SPOT", "spot") == "spot_pair"

    def test_defi_venue_chain_strip_and_spelling(self, mod: ModuleType) -> None:
        """defi AAVEV3-ETHEREUM / AAVE_V3-ETHEREUM / AAVE_V3 all canonicalise to AAVE_V3."""
        a = mod._canon_venue("defi", "AAVEV3-ETHEREUM")
        b = mod._canon_venue("defi", "AAVE_V3-ETHEREUM")
        d = mod._canon_venue("defi", "AAVE_V3")
        assert a == b == d == "AAVE_V3", f"got {a!r} {b!r} {d!r}"

    def test_data_type_case_fold(self, mod: ModuleType) -> None:
        """ODDS and odds canonicalise to the same data_type."""
        assert mod._canon_data_type("sports", "ODDS") == mod._canon_data_type("sports", "odds") == "odds"

    def test_defi_rate_indices_folds_to_lending_indices(self, mod: ModuleType) -> None:
        """defi `rate_indices` is the non-canonical writer name for `lending_indices`
        (uac_writer_matrix_reconciliation Decision 3); other AGs are NOT folded."""
        assert mod._canon_data_type("defi", "rate_indices") == "lending_indices"
        assert mod._canon_data_type("cefi", "rate_indices") == "rate_indices"

    def test_defi_lending_fine_grains_fold_to_lending(self, mod: ModuleType) -> None:
        """a_token/debt_token/liquidation writer grains roll up to UAC `lending`
        (Decision 3/4: grain mismatch, not missing data); other AGs unaffected."""
        for fine in ("a_token", "DEBT_TOKEN", "liquidation"):
            assert mod._canon_instrument_type("defi", "AAVE_V3-ETHEREUM", fine) == "lending"
        assert mod._canon_instrument_type("cefi", "BYBIT", "a_token") == "a_token"

    def test_cefi_venue_suffix_fold(self, mod: ModuleType) -> None:
        """Tardis-suffix + legacy venue dialects fold to the UAC canonical venue
        (Decision 6, check-folds-suffixes); UAC-canonical suffixed venues do NOT fold."""
        assert mod._canon_venue("cefi", "OKX-SWAP") == "OKX"
        assert mod._canon_venue("cefi", "OKX-SPOT") == "OKX"
        assert mod._canon_venue("cefi", "okex-futures") == "OKX"
        assert mod._canon_venue("cefi", "CRYPTOFACILITIES") == "KRAKEN-FUTURES"
        assert mod._canon_venue("cefi", "BITFINEX-DERIVATIVES") == "BITFINEX-FUTURES"
        assert mod._canon_venue("cefi", "BYBIT-SPOT") == "BYBIT-SPOT"
        assert mod._canon_venue("cefi", "KRAKEN-FUTURES") == "KRAKEN-FUTURES"

    def test_cefi_bare_coinbase_folds_up_to_coinbase_spot(self, mod: ModuleType) -> None:
        """coinbase_bare_name_migration_2026_07_06 S1 (Option A fold invert):
        the canonical EXPECTED token for spot Coinbase is COINBASE-SPOT, so
        legacy bare-COINBASE writer/EXPECTED tokens fold UP to COINBASE-SPOT
        instead of COINBASE-SPOT folding DOWN to bare COINBASE."""
        assert mod._canon_venue("cefi", "COINBASE") == "COINBASE-SPOT"
        assert mod._canon_venue("cefi", "COINBASE-SPOT") == "COINBASE-SPOT"

    def test_legacy_bare_coinbase_and_coinbase_spot_writer_rows_are_equivalent(self, mod: ModuleType) -> None:
        """Regression guard (plan §3): a manifest row stamped bare COINBASE
        (legacy pre-2026-06-23 writer rows) and one stamped COINBASE-SPOT (the
        current writer token) for the same (itype, dt) MUST canonicalise to
        the identical comparison key. This is the exact D2a-safety property
        the S1 fold invert exists for — a writer-side dialect difference must
        never manufacture a spurious stray or a spurious hole, regardless of
        which token happens to be canonical."""
        key_bare = mod._canon_key("cefi", "COINBASE", "spot_pair", "trades")
        key_qualified = mod._canon_key("cefi", "COINBASE-SPOT", "spot_pair", "trades")
        assert key_bare == key_qualified == ("COINBASE-SPOT", "spot_pair", "trades")

    def test_prediction_token_folds_to_prediction_market(self, mod: ModuleType) -> None:
        """Kalshi `prediction` itype folds to the canonical `prediction_market` grain."""
        assert mod._canon_instrument_type("prediction", "KALSHI", "prediction") == "prediction_market"
        assert mod._canon_instrument_type("prediction", "KALSHI", "PREDICTION_MARKET") == "prediction_market"


class TestAlignmentNotArtifact:
    """A casing/format/vocab difference must NOT be a hole after alignment."""

    def test_uppercase_manifest_matches_lowercase_expected(self, mod: ModuleType) -> None:
        """UPPERCASE manifest instrument_type matches the lowercase EXPECTED grain.

        Pre-alignment this collapsed to 0% (the Bug-3 artifact). Build a manifest
        whose itype is UPPERCASE for a known-expected cefi tuple and assert it is
        MATCHED (not a hole).
        """
        expected = mod._build_expected_tuples("cefi")
        # Pick an expected (venue, itype, dt) and present it UPPERCASE in manifest.
        target = next(
            (t for t in sorted(expected) if t[1] in {"perpetual", "spot_pair", "future"}),
            None,
        )
        if target is None:
            pytest.skip("No suitable cefi expected tuple")
        v, it, dt = target
        df = _make_manifest(
            [
                {"capture_status": "captured", "venue": v, "instrument_type": it.upper(), "data_type": dt.upper()},
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        missing_keys = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert (v, it, dt) not in missing_keys, (
            f"UPPERCASE manifest row {(v, it.upper(), dt.upper())} must MATCH lowercase "
            f"EXPECTED {(v, it, dt)} after alignment — not be a hole"
        )

    def test_defi_uppercase_lending_matches(self, mod: ModuleType) -> None:
        """defi LENDING (uppercase) + chain-stripped venue matches lowercase EXPECTED."""
        expected = mod._build_expected_tuples("defi")
        target = next((t for t in sorted(expected) if t[1] == "lending"), None)
        if target is None:
            pytest.skip("No defi lending tuple in EXPECTED")
        v, it, dt = target  # v is PROTOCOL grain e.g. AAVE_V3
        df = _make_manifest(
            [
                {"capture_status": "captured", "venue": v, "instrument_type": "LENDING", "data_type": dt},
            ]
        )
        result = mod.check_enumeration_completeness("defi", df)
        missing_keys = {(m.venue, m.instrument_type, m.data_type) for m in result.missing_tuples}
        assert (v, it, dt) not in missing_keys, (
            f"defi LENDING uppercase must match lowercase EXPECTED {(v, it, dt)} — not a hole"
        )


class TestPerAgAlignmentRegression:
    """Coordinator requirement #4: after normalisation, an AG with real captured
    data MUST NOT show 0% / 0 matched purely from dialect mismatch."""

    def _ag_df(self, mod: ModuleType, ag: str, rows: list[dict[str, str]]) -> pd.DataFrame:
        return _make_manifest(rows)

    def test_defi_not_zero_when_present(self, mod: ModuleType) -> None:
        """defi: a manifest carrying an expected (chain-stripped, lowercase) tuple
        yields matched > 0 (not the pre-fix 0%)."""
        expected = mod._build_expected_tuples("defi")
        sample = sorted(expected)[:5]
        if not sample:
            pytest.skip("defi EXPECTED empty")
        rows = [
            {"capture_status": "captured", "venue": v, "instrument_type": it, "data_type": dt} for (v, it, dt) in sample
        ]
        result = mod.check_enumeration_completeness("defi", df := _make_manifest(rows), diagnose=True)
        assert result.present_tuples > 0, "defi matched must be > 0 with present expected tuples"
        assert result.completeness_pct is not None and result.completeness_pct > 0.0

    def test_sports_not_zero_when_odds_trades_present(self, mod: ModuleType) -> None:
        """sports: odds-grain (venue, odds, trades) present → matched > 0 (was 0% pre-fix)."""
        from unified_api_contracts import VENUES_BY_ASSET_GROUP

        venues = list(VENUES_BY_ASSET_GROUP.get("sports", []))
        if not venues:
            pytest.skip("No sports venues")
        rows = [
            {"capture_status": "captured", "venue": v, "instrument_type": "odds", "data_type": "trades"} for v in venues
        ]
        result = mod.check_enumeration_completeness("sports", _make_manifest(rows), diagnose=True)
        assert result.present_tuples > 0, (
            "sports (venue, odds, trades) must MATCH the odds-grain EXPECTED — "
            "the pre-fix exchange_odds/fixed_odds grain gave 0 matched"
        )

    def test_diagnostics_populated_when_requested(self, mod: ModuleType) -> None:
        """diagnose=True populates DiagnosticSamples with the three buckets + counts."""
        expected = mod._build_expected_tuples("cefi")
        rows = [
            {"capture_status": "captured", "venue": v, "instrument_type": it, "data_type": dt}
            for (v, it, dt) in sorted(expected)[:3]
        ]
        result = mod.check_enumeration_completeness("cefi", _make_manifest(rows), diagnose=True)
        assert result.diagnostics is not None
        d = result.diagnostics
        assert d.matched_count >= 0
        assert isinstance(d.expected_only, list)
        assert isinstance(d.enumerated_only, list)
        assert isinstance(d.matched, list)
        # as_dict carries the diagnostics block
        assert "diagnostics" in result.as_dict()

    def test_no_diagnostics_by_default(self, mod: ModuleType) -> None:
        """Without diagnose, diagnostics is None and absent from as_dict."""
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-SPOT",
                    "instrument_type": "spot_pair",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.check_enumeration_completeness("cefi", df)
        assert result.diagnostics is None
        assert "diagnostics" not in result.as_dict()


class TestVenueItypeGate:
    """The (venue, itype) validity gate prevents cross-product over-generation."""

    def test_cefi_futures_venue_no_spot_pair(self, mod: ModuleType) -> None:
        """BINANCE-FUTURES must NOT expect spot_pair (futures-only venue)."""
        expected = mod._build_expected_tuples("cefi")
        bad = {(v, it, dt) for (v, it, dt) in expected if v == "BINANCE-FUTURES" and it == "spot_pair"}
        assert not bad, f"BINANCE-FUTURES should not expect spot_pair: {bad}"

    def test_defi_lending_protocol_no_pool(self, mod: ModuleType) -> None:
        """A lending protocol (AAVE_V3) must NOT expect pool itype."""
        expected = mod._build_expected_tuples("defi")
        aave_itypes = {it for (v, it, dt) in expected if v == "AAVE_V3"}
        # AAVE_V3 is a lending protocol — pool should be absent
        assert "pool" not in aave_itypes, f"AAVE_V3 (lending) should not expect pool itype; got itypes={aave_itypes}"

    def test_tradfi_cme_no_equity(self, mod: ModuleType) -> None:
        """CME (futures venue) must NOT expect equity itype."""
        expected = mod._build_expected_tuples("tradfi")
        cme_itypes = {it for (v, it, dt) in expected if v == "CME"}
        assert "equity" not in cme_itypes, f"CME should not expect equity; got itypes={cme_itypes}"
