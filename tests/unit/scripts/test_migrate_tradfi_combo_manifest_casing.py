"""Unit tests — migrate_tradfi_combo_manifest_casing.py.

Pure relabel/census logic only — no GCS or network access. Covers:
  1. Normal relabel: every instrument_type=="COMBO" row becomes "combo",
     row identity (date/venue/instrument_id/capture_status) untouched.
  2. 4-capture_status coverage: relabels captured / attempted_failed /
     empty_confirmed / expected_unattempted rows alike.
  3. Idempotency: re-running on an already-migrated frame is a no-op
     (0 candidates, frame unchanged).
  4. Non-COMBO / already-canonical / other-instrument_type rows survive
     untouched.
  5. _census: per-capture_status breakdown for both casings.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_tradfi_combo_manifest_casing.py"
    module_name = "_migrate_tradfi_combo_manifest_casing_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_script()

pytestmark = pytest.mark.unit


def _fixture_df() -> pd.DataFrame:
    """Mirrors the real distribution: legacy COMBO across all 4 capture_status
    values, a handful of already-canonical combo rows, and unrelated
    instrument_type rows that must never be touched."""
    return pd.DataFrame(
        [
            {"date": "2024-01-02", "venue": "CME", "capture_status": "captured", "instrument_type": "COMBO"},
            {
                "date": "2024-01-03",
                "venue": "CME",
                "capture_status": "attempted_failed",
                "instrument_type": "COMBO",
            },
            {
                "date": "2024-01-04",
                "venue": "CME",
                "capture_status": "empty_confirmed",
                "instrument_type": "COMBO",
            },
            {
                "date": "2024-01-05",
                "venue": "CME",
                "capture_status": "expected_unattempted",
                "instrument_type": "COMBO",
            },
            # Already-canonical — must be left exactly as-is.
            {"date": "2024-01-06", "venue": "CME", "capture_status": "captured", "instrument_type": "combo"},
            # A different instrument_type entirely — must never be touched.
            {"date": "2024-01-07", "venue": "CME", "capture_status": "captured", "instrument_type": "future"},
            # A lowercase-but-different value that happens to contain "combo"
            # as a substring must NOT match (exact-value comparison only).
            {
                "date": "2024-01-08",
                "venue": "CME",
                "capture_status": "captured",
                "instrument_type": "combo_spread",
            },
        ]
    )


class TestRelabelComboCasing:
    def test_relabels_every_uppercase_combo_row(self) -> None:
        df = _fixture_df()
        relabeled, stats = _mod._relabel_combo_casing(df)
        assert stats["total_relabeled"] == 4
        assert int((relabeled["instrument_type"] == "COMBO").sum()) == 0
        # 4 newly-relabeled + 1 already-canonical = 5.
        assert int((relabeled["instrument_type"] == "combo").sum()) == 5

    def test_covers_all_four_capture_status_values(self) -> None:
        df = _fixture_df()
        _relabeled, stats = _mod._relabel_combo_casing(df)
        assert stats["captured"] == 1
        assert stats["attempted_failed"] == 1
        assert stats["empty_confirmed"] == 1
        assert stats["expected_unattempted"] == 1

    def test_row_identity_preserved_across_relabel(self) -> None:
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        row = relabeled.loc[relabeled["date"] == "2024-01-03"].iloc[0]
        assert row["venue"] == "CME"
        assert row["capture_status"] == "attempted_failed"
        assert row["instrument_type"] == "combo"

    def test_already_canonical_row_untouched(self) -> None:
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        row = relabeled.loc[relabeled["date"] == "2024-01-06"].iloc[0]
        assert row["instrument_type"] == "combo"

    def test_unrelated_instrument_type_untouched(self) -> None:
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        row = relabeled.loc[relabeled["date"] == "2024-01-07"].iloc[0]
        assert row["instrument_type"] == "future"

    def test_substring_match_not_relabeled(self) -> None:
        """combo_spread must NOT be caught by an exact-value comparison."""
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        row = relabeled.loc[relabeled["date"] == "2024-01-08"].iloc[0]
        assert row["instrument_type"] == "combo_spread"

    def test_row_count_unchanged(self) -> None:
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        assert len(relabeled) == len(df)

    def test_idempotent_on_already_migrated_frame(self) -> None:
        df = _fixture_df()
        relabeled_once, _stats = _mod._relabel_combo_casing(df)
        relabeled_twice, stats_twice = _mod._relabel_combo_casing(relabeled_once)
        assert stats_twice["total_relabeled"] == 0
        pd.testing.assert_frame_equal(relabeled_once, relabeled_twice)

    def test_no_candidates_returns_zero_stats(self) -> None:
        df = _fixture_df()
        df = df[df["instrument_type"] != "COMBO"].reset_index(drop=True)
        _relabeled, stats = _mod._relabel_combo_casing(df)
        assert stats["total_relabeled"] == 0

    def test_missing_instrument_type_column_is_a_noop(self) -> None:
        df = pd.DataFrame([{"date": "2024-01-02", "venue": "CME", "capture_status": "captured"}])
        relabeled, stats = _mod._relabel_combo_casing(df)
        assert stats["total_relabeled"] == 0
        assert relabeled is df


class TestCensus:
    def test_pre_migration_census_matches_fixture(self) -> None:
        df = _fixture_df()
        census = _mod._census(df)
        assert census["COMBO"] == {
            "captured": 1,
            "attempted_failed": 1,
            "empty_confirmed": 1,
            "expected_unattempted": 1,
        }
        assert census["combo"] == {"captured": 1}

    def test_post_migration_census_shows_zero_uppercase(self) -> None:
        df = _fixture_df()
        relabeled, _stats = _mod._relabel_combo_casing(df)
        census = _mod._census(relabeled)
        assert census["COMBO"] == {}
        # 1 originally-canonical captured row + 1 newly-relabeled captured row.
        assert census["combo"]["captured"] == 2
        assert census["combo"]["attempted_failed"] == 1
        assert census["combo"]["empty_confirmed"] == 1
        assert census["combo"]["expected_unattempted"] == 1

    def test_census_missing_columns_returns_empty(self) -> None:
        df = pd.DataFrame([{"date": "2024-01-02"}])
        census = _mod._census(df)
        assert census == {"COMBO": {}, "combo": {}}
