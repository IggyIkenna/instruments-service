"""Unit tests for check_enumeration_completeness.filter_manifest_to_expected.

The MVP read-time gate: filter a manifest DataFrame to only rows whose
canonical (venue, instrument_type, data_type) key is in the EXPECTED tuple
set from build_expected(asset_group).  Manifest rows are never mutated —
only a filtered view is returned.

Plan: plans/active/issues/cefi_layer1_denominator_gaps_2026_07_03.md task 2c
SSOT: codex/02-data/honest-coverage-model.md § Carve-outs (MVP filter row)
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "check_enumeration_completeness.py"
    module_name = "_check_enumeration_completeness_filter_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod() -> ModuleType:
    return _load_module()


def _make_manifest(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestFilterKeepsInScopeRows:
    """In-scope rows (present in EXPECTED) are kept."""

    def test_cefi_in_scope_binance_futures_trades_kept(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 1
        assert filtered.iloc[0]["venue"] == "BINANCE-FUTURES"

    def test_input_df_never_mutated(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
            ]
        )
        pre_len = len(df)
        pre_venues = list(df["venue"])
        _ = mod.filter_manifest_to_expected("cefi", df)
        assert len(df) == pre_len
        assert list(df["venue"]) == pre_venues


class TestFilterRemovesOutOfScopeRows:
    """Out-of-scope rows (not in EXPECTED) are dropped."""

    def test_cefi_bybit_spot_perpetual_stamp_dropped(self, mod: ModuleType) -> None:
        """BYBIT-SPOT rows mis-stamped as PERPETUAL are OUT OF SCOPE for cefi.

        The plan documents this exact writer defect (BYBIT-SPOT rows carry
        instrument_type=PERPETUAL because MTDS _VENUE_INSTRUMENT_TYPE lacks
        the BYBIT-SPOT entry).  The read-time gate correctly drops these
        because (BYBIT-SPOT, perpetual, *) is not in EXPECTED — BYBIT-SPOT
        is expected under spot_pair.
        """
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BYBIT-SPOT",
                    "instrument_type": "perpetual",
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
        filtered = mod.filter_manifest_to_expected("cefi", df)
        # Only BINANCE-FUTURES/perpetual/trades survives.
        assert len(filtered) == 1
        assert filtered.iloc[0]["venue"] == "BINANCE-FUTURES"

    def test_cefi_non_mvp_data_type_dropped(self, mod: ModuleType) -> None:
        """COINBASE-FUTURES MVP override is trades-only.  A book_snapshot_5
        row is dropped; a trades row survives.  This exercises the MVP
        per-venue override path (`get_mvp_data_types_for_cefi_venue`).
        """
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "COINBASE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "book_snapshot_5",  # non-MVP for COINBASE-FUTURES
                },
                {
                    "capture_status": "captured",
                    "venue": "COINBASE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",  # MVP for COINBASE-FUTURES
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        # Only the trades row survives.
        assert len(filtered) == 1
        assert filtered.iloc[0]["data_type"] == "trades"


class TestFilterCanonicalisation:
    """Filter uses the same canonical grain check_enumeration_completeness uses."""

    def test_cefi_okx_spot_matches_own_expected_entry(self, mod: ModuleType) -> None:
        """OKX-SPOT venue in manifest matches directly (no fold, 2026-07-10) —
        kept because (OKX-SPOT, spot_pair, trades) is its own EXPECTED entry.
        """
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "OKX-SPOT",  # writer-side Tardis split
                    "instrument_type": "spot_pair",
                    "data_type": "trades",
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 1
        assert filtered.iloc[0]["venue"] == "OKX-SPOT"  # original preserved

    def test_case_folding_perpetual_uppercase_kept(self, mod: ModuleType) -> None:
        """The manifest carries both PERPETUAL and perpetual (uppercase +
        lowercase).  Both canonicalise to `perpetual`, so both are kept
        when (venue, perpetual, dt) is in EXPECTED.
        """
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",  # uppercase
                    "data_type": "trades",
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 1


class TestFilterDegradesGracefully:
    """Missing columns → return input df unchanged (no gate applied)."""

    def test_missing_instrument_type_column_returns_unchanged(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "data_type": "trades",
                    # no instrument_type
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        # Should return the input unchanged (identity) so measurement continues.
        assert len(filtered) == 1
        assert list(filtered.columns) == list(df.columns)

    def test_missing_venue_column_returns_unchanged(self, mod: ModuleType) -> None:
        df = pd.DataFrame(
            [
                {
                    "capture_status": "captured",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 1


class TestFilterEmptyResult:
    """When zero manifest triples land in EXPECTED, return an empty df."""

    def test_all_out_of_scope_yields_empty_df(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "NOT_A_REAL_VENUE",
                    "instrument_type": "not_a_real_type",
                    "data_type": "trades",
                },
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 0
        # Columns preserved for downstream groupby correctness.
        assert set(filtered.columns) == set(df.columns)


class TestFilterExplicitExpected:
    """The gate accepts an explicit `expected` set (bypasses build_expected)."""

    def test_explicit_expected_only_keeps_matches(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                },
                {
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "book_snapshot_5",
                },
            ]
        )
        # Only the trades tuple in the explicit expected set.
        explicit_expected = {("BINANCE-FUTURES", "perpetual", "trades")}
        filtered = mod.filter_manifest_to_expected("cefi", df, expected=explicit_expected)
        assert len(filtered) == 1
        assert filtered.iloc[0]["data_type"] == "trades"


class TestFilterAllStatuses:
    """The gate is agnostic to capture_status — filters all 4 states equally."""

    def test_all_four_statuses_survive_when_in_scope(self, mod: ModuleType) -> None:
        df = _make_manifest(
            [
                {
                    "capture_status": s,
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "perpetual",
                    "data_type": "trades",
                }
                for s in ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")
            ]
        )
        filtered = mod.filter_manifest_to_expected("cefi", df)
        assert len(filtered) == 4
        assert set(filtered["capture_status"]) == {
            "captured",
            "empty_confirmed",
            "attempted_failed",
            "expected_unattempted",
        }
