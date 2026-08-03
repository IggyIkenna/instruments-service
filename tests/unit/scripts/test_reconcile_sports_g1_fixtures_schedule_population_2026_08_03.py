"""Unit tests — reconcile_sports_g1_fixtures_schedule_population_2026_08_03.py.

Credential-free: pure logic only (no GCS / no network). Coverage:
`sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md` todo 2 — §U's FIXTURES_SCHEDULE
non-registry population vs the G1 delete script's manifest-index census.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "reconcile_sports_g1_fixtures_schedule_population_2026_08_03.py"
    module_name = "_reconcile_sports_g1_fixtures_schedule_population_2026_08_03_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = _load_script()
is_g1_census_disjoint_from_fixtures_schedule = MOD.is_g1_census_disjoint_from_fixtures_schedule
league_from_blob_path = MOD.league_from_blob_path
tally_frame = MOD.tally_frame


def test_g1_census_scope_excludes_fixtures_schedule() -> None:
    """Pins the decisive structural finding: the G1 delete script's own `_FOOTBALL_DATA_TYPES` frozenset
    (duplicated here) does not include FIXTURES_SCHEDULE/FIXTURES_OUTCOMES, so §U's population (entirely
    FIXTURES_SCHEDULE) can never overlap with the G1 manifest-index cut."""
    assert is_g1_census_disjoint_from_fixtures_schedule() is True


def test_league_from_blob_path_extracts_partition_value() -> None:
    path = (
        "sports_reference/by_date/day=2024-03-09/pipeline_mode=batch_api_football/"
        "entity=fixtures_schedule/league=1301/fixtures_schedule.parquet"
    )
    assert league_from_blob_path(path) == "1301"


def test_league_from_blob_path_returns_none_for_bare_day_file() -> None:
    """The bare multi-league file (no `/league=` segment) is never read by the live path — must be
    skipped, not misclassified as some league."""
    path = (
        "sports_reference/by_date/day=2024-03-09/pipeline_mode=batch_api_football/"
        "entity=fixtures_schedule/fixtures_schedule.parquet"
    )
    assert league_from_blob_path(path) is None


def test_tally_frame_counts_blank_round_rows() -> None:
    df = pd.DataFrame({"af_league_id": [1301, 1301, 1301], "round": ["Regular Season - 3", "", None]})
    total, blank = tally_frame(df)
    assert total == 3
    assert blank == 2


def test_tally_frame_handles_missing_round_column() -> None:
    df = pd.DataFrame({"af_league_id": [1301]})
    total, blank = tally_frame(df)
    assert total == 1
    assert blank == 0
