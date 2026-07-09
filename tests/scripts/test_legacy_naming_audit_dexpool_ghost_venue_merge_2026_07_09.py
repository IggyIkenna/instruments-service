"""Unit tests for legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py.

Credential-free + GCS-free — exercises the pure ``_rewrite_ghost_venue_columns``
and ``_merge_frames`` helpers.

Regression coverage for the 2026-07-09 adversarial-verification finding: the
FIRST shipped version of this script (``instruments-service@11192be2``) fixed a
ghost-only row's GCS *path* but never rewrote the row's own ``venue``/
``instrument_key`` COLUMN values, so a merged row could survive with a
canonical path but ghost-spelled data inside it (e.g.
``instrument_key='AAVEV3-OPTIMISM:A_TOKEN:ALINK'``, no underscore). These tests
assert the surviving rows' COLUMN VALUES are canonical-spelled after merge, not
merely that the row count/identity survived.

Plan: unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py"
    module_name = "_legacy_naming_audit_dexpool_ghost_venue_merge_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mig() -> ModuleType:
    return _load_script()


GHOST = "AAVEV3-OPTIMISM"
CANON = "AAVE_V3-OPTIMISM"


class TestRewriteGhostVenueColumns:
    def test_exact_match_venue_column_rewritten(self, mig: ModuleType) -> None:
        df = pd.DataFrame({"venue": [GHOST, GHOST], "raw_symbol": ["A", "B"]})
        out = mig._rewrite_ghost_venue_columns(df, GHOST, CANON)
        assert out["venue"].tolist() == [CANON, CANON]

    def test_prefixed_instrument_key_rewritten_preserving_suffix(self, mig: ModuleType) -> None:
        df = pd.DataFrame(
            {
                "instrument_key": [f"{GHOST}:A_TOKEN:ALINK", f"{GHOST}:A_TOKEN:AAAVE"],
                "raw_symbol": ["ALINK", "AAAVE"],
            }
        )
        out = mig._rewrite_ghost_venue_columns(df, GHOST, CANON)
        assert out["instrument_key"].tolist() == [f"{CANON}:A_TOKEN:ALINK", f"{CANON}:A_TOKEN:AAAVE"]

    def test_non_ghost_values_untouched(self, mig: ModuleType) -> None:
        df = pd.DataFrame({"venue": [CANON], "instrument_key": [f"{CANON}:A_TOKEN:AUSDC"]})
        out = mig._rewrite_ghost_venue_columns(df, GHOST, CANON)
        assert out["venue"].tolist() == [CANON]
        assert out["instrument_key"].tolist() == [f"{CANON}:A_TOKEN:AUSDC"]

    def test_non_string_values_pass_through(self, mig: ModuleType) -> None:
        df = pd.DataFrame({"venue": [GHOST], "tick_size": [0.01], "expiry": [None]})
        out = mig._rewrite_ghost_venue_columns(df, GHOST, CANON)
        assert out["venue"].tolist() == [CANON]
        assert out["tick_size"].tolist() == [0.01]
        assert out["expiry"].tolist() == [None]

    def test_empty_frame_noop(self, mig: ModuleType) -> None:
        df = pd.DataFrame(columns=["venue", "instrument_key"])
        out = mig._rewrite_ghost_venue_columns(df, GHOST, CANON)
        assert out.empty

    def test_same_ghost_and_canon_noop(self, mig: ModuleType) -> None:
        df = pd.DataFrame({"venue": [CANON]})
        out = mig._rewrite_ghost_venue_columns(df, CANON, CANON)
        assert out["venue"].tolist() == [CANON]


class TestMergeFramesContamination:
    """Regression: a ghost-only row's own venue/instrument_key must be canonical
    post-merge, not just its GCS path (the real 2026-07-09 bug)."""

    def test_collision_ghost_only_row_gets_canonical_columns(self, mig: ModuleType) -> None:
        canon_df = pd.DataFrame(
            {
                "raw_symbol": ["ASUSD"],
                "venue": [CANON],
                "instrument_key": [f"{CANON}:A_TOKEN:ASUSD"],
            }
        )
        ghost_df = pd.DataFrame(
            {
                "raw_symbol": ["ALINK", "AAAVE"],
                "venue": [GHOST, GHOST],
                "instrument_key": [f"{GHOST}:A_TOKEN:ALINK", f"{GHOST}:A_TOKEN:AAAVE"],
            }
        )
        merged = mig._merge_frames(ghost_df, canon_df, GHOST, CANON)

        assert len(merged) == 3  # 1 canonical + 2 ghost-only, none dropped
        assert set(merged["venue"].tolist()) == {CANON}  # no ghost spelling survives ANYWHERE
        ghost_only_rows = merged[merged["raw_symbol"].isin(["ALINK", "AAAVE"])]
        assert sorted(ghost_only_rows["instrument_key"].tolist()) == [
            f"{CANON}:A_TOKEN:AAAVE",
            f"{CANON}:A_TOKEN:ALINK",
        ]
        # Real duplicate-of-canonical row (ASUSD) is untouched/kept exactly once.
        assert merged[merged["raw_symbol"] == "ASUSD"]["instrument_key"].tolist() == [f"{CANON}:A_TOKEN:ASUSD"]

    def test_pure_orphan_row_gets_canonical_columns(self, mig: ModuleType) -> None:
        """canon_df is None (100%-orphan day, e.g. PANCAKESWAPV3-ZKSYNC) — every
        row comes straight from the ghost side and must still be relabeled."""
        ghost_df = pd.DataFrame(
            {
                "raw_symbol": ["USDC-USDT"],
                "venue": [GHOST],
                "instrument_key": [f"{GHOST}:POOL:USDC-USDT:100"],
            }
        )
        merged = mig._merge_frames(ghost_df, None, GHOST, CANON)
        assert merged["venue"].tolist() == [CANON]
        assert merged["instrument_key"].tolist() == [f"{CANON}:POOL:USDC-USDT:100"]

    def test_no_shared_identity_column_branch_still_relabels(self, mig: ModuleType) -> None:
        canon_df = pd.DataFrame({"venue": [CANON], "instrument_key": [f"{CANON}:POOL:X"]})
        ghost_df = pd.DataFrame({"venue": [GHOST], "instrument_key": [f"{GHOST}:POOL:Y"]})
        merged = mig._merge_frames(ghost_df, canon_df, GHOST, CANON)
        assert set(merged["venue"].tolist()) == {CANON}
        assert sorted(merged["instrument_key"].tolist()) == [f"{CANON}:POOL:X", f"{CANON}:POOL:Y"]

    def test_idempotent_when_already_canonical(self, mig: ModuleType) -> None:
        canon_df = pd.DataFrame({"raw_symbol": ["A"], "venue": [CANON], "instrument_key": [f"{CANON}:A_TOKEN:A"]})
        merged = mig._merge_frames(canon_df.copy(), canon_df.copy(), CANON, CANON)
        assert merged["venue"].tolist() == [CANON]
        assert merged["instrument_key"].tolist() == [f"{CANON}:A_TOKEN:A"]
