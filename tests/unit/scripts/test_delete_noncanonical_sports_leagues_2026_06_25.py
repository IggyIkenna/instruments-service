"""Unit tests — delete_noncanonical_sports_leagues_2026_06_25.py (``_delete_noncanonical_rows``).

Credential-free: ``_delete_noncanonical_rows`` is pure (no GCS / no network) — tested directly on synthetic
DataFrames shaped like the manifest ``availability_index``/legacy-seed rows.

Coverage: the 2026-07-27 scope-bug regression guard
(``sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md``). A live census found the G1 non-canonical-
league wipe deleting real, un-migrated canonical-league ``trades``/``odds_horizon_bucket`` rows (a SEPARATE,
still-in-flight Track V casing migration) because the football-data-type scope (``_FOOTBALL_DATA_TYPES``) was
defined but never wired into the actual filter. This pins that a non-football row with a non-canonical
``league_id`` now SURVIVES, while a football row with a non-canonical ``league_id`` is still deleted.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "delete_noncanonical_sports_leagues_2026_06_25.py"
    module_name = "_delete_noncanonical_sports_leagues_2026_06_25_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = _load_script()

_CANONICAL_IDS = frozenset({"EPL", "LA_LIGA"})


def test_non_football_row_with_noncanonical_league_id_survives() -> None:
    """A `trades`/`odds_horizon_bucket` row carrying a stale non-canonical league_id must NOT be deleted —
    it belongs to a separate migration, not G1 NOISE."""
    df = pd.DataFrame(
        {
            "league_id": ["PREMIER_LEAGUE", "CHAMPIONSHIP"],
            "data_type": ["trades", "odds_horizon_bucket"],
        }
    )
    out, n_deleted, deleted_ids = MOD._delete_noncanonical_rows(df, _CANONICAL_IDS, "test")
    assert n_deleted == 0
    assert deleted_ids == frozenset()
    assert len(out) == 2


def test_football_row_with_noncanonical_league_id_still_deleted() -> None:
    """A genuine football row (FIXTURES) with a non-canonical league_id is still removed — the fix must not
    over-correct into never deleting anything."""
    df = pd.DataFrame(
        {
            "league_id": ["ALBANIA_SUPERLIGA", "EPL"],
            "data_type": ["FIXTURES", "FIXTURES"],
        }
    )
    out, n_deleted, deleted_ids = MOD._delete_noncanonical_rows(df, _CANONICAL_IDS, "test")
    assert n_deleted == 1
    assert deleted_ids == frozenset({"ALBANIA_SUPERLIGA"})
    assert len(out) == 1
    assert out["league_id"].tolist() == ["EPL"]


def test_mixed_football_and_non_football_only_football_deleted() -> None:
    """Same non-canonical league_id appearing in both a football and a non-football data_type: only the
    football-typed row is deleted."""
    df = pd.DataFrame(
        {
            "league_id": ["PREMIER_LEAGUE", "PREMIER_LEAGUE"],
            "data_type": ["FIXTURES", "trades"],
        }
    )
    out, n_deleted, deleted_ids = MOD._delete_noncanonical_rows(df, _CANONICAL_IDS, "test")
    assert n_deleted == 1
    assert deleted_ids == frozenset({"PREMIER_LEAGUE"})
    assert len(out) == 1
    assert out["data_type"].tolist() == ["trades"]


def test_missing_data_type_column_falls_back_to_prior_behavior() -> None:
    """If a frame has no `data_type` column at all (legacy seed shape), the football-scope filter must not
    crash — every non-canonical row is still treated as deletable (prior behavior preserved)."""
    df = pd.DataFrame({"league_id": ["ALBANIA_SUPERLIGA", "EPL"]})
    out, n_deleted, deleted_ids = MOD._delete_noncanonical_rows(df, _CANONICAL_IDS, "test")
    assert n_deleted == 1
    assert deleted_ids == frozenset({"ALBANIA_SUPERLIGA"})
    assert len(out) == 1
