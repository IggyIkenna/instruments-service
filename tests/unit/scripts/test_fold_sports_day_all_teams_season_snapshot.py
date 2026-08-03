"""Unit test for scripts/fold_sports_day_all_teams_season_snapshot_2026_08_03.py.

Pure-Python: no GCS calls, no network. Covers ``_season_dst_path`` -- the one function
with no I/O, which resolves the destination path via the UAC SSOT
(``candidate_parquet_paths``) rather than a hand-rolled string. The GCS-touching
``_fold_season``/``main`` flow was verified live against prod (dry-run then --apply,
30,069/30,069 rows accounted for, 0 aborted -- see the parent issue doc's Progress Log),
mirroring the boundary the sibling one-off migration-script tests draw.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).parent.parent.parent.parent / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from fold_sports_day_all_teams_season_snapshot_2026_08_03 import (
    _season_dst_path,  # type: ignore[import-not-found]
)

pytestmark = pytest.mark.unit


def test_season_dst_path_matches_flat_per_season_convention() -> None:
    assert _season_dst_path("2024") == "sports_reference/teams/season=2024/teams.parquet"


def test_season_dst_path_different_seasons_distinct() -> None:
    paths = {_season_dst_path(s) for s in ("2019", "2020", "2021", "2022", "2023", "2024", "2025")}
    assert len(paths) == 7
    assert all("by_date" not in p and "day=" not in p for p in paths)
