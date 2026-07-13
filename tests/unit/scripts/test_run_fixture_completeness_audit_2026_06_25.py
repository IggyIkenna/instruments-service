"""Unit tests — run_fixture_completeness_audit_2026_06_25.py (``_compute_targeted_refetch``).

Credential-free: ``_compute_targeted_refetch`` is pure (no GCS / no network) — tested directly on
synthetic DataFrames built in the same shape ``_build_fixtures_index`` produces.

Coverage:
  1. Normal case: shortfall-league rows that are ``attempted_failed`` (or any non-captured /
     non-``empty_confirmed`` status) are included; ``captured``/``empty_confirmed`` rows and
     non-shortfall-league rows are excluded.
  2. Fail-fast regression guard (2026-07-09 empty-string-fallback fix): ``capture_status`` is a
     guaranteed manifest column (already accessed unconditionally via bracket notation in
     ``_compute_season_summary``'s ``grp["capture_status"]``). A row-wise dict-style fallback here
     would have silently turned a structurally-missing column into an empty-string status instead
     of failing loud — this pins that a missing column now raises ``KeyError`` instead.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "run_fixture_completeness_audit_2026_06_25.py"
    module_name = "_run_fixture_completeness_audit_2026_06_25_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = _load_script()


def _filt_frame() -> pd.DataFrame:
    """A ``_build_fixtures_index``-shaped FIXTURES slice: one shortfall league (EPL) with a
    failed shard + a missing-shard-equivalent row, one fully-captured shortfall-league shard,
    and one row for a league with no shortfall (must never appear in the refetch output)."""
    return pd.DataFrame(
        [
            {
                "league_id": "EPL",
                "_date_str": "2025-09-01",
                "season_year": 2025,
                "capture_status": "attempted_failed",
                "row_count": 0,
            },
            {
                "league_id": "EPL",
                "_date_str": "2025-09-02",
                "season_year": 2025,
                "capture_status": "captured",
                "row_count": 10,
            },
            {
                "league_id": "EPL",
                "_date_str": "2025-09-03",
                "season_year": 2025,
                "capture_status": "empty_confirmed",
                "row_count": 0,
            },
            {
                "league_id": "BUNDESLIGA",
                "_date_str": "2025-09-01",
                "season_year": 2025,
                "capture_status": "attempted_failed",
                "row_count": 0,
            },
        ]
    )


def _summary_rows() -> list[dict[str, object]]:
    return [
        {"league_id": "EPL", "in_registry": True, "shortfall": 3},
        {"league_id": "BUNDESLIGA", "in_registry": True, "shortfall": 0},
    ]


def test_targeted_refetch_includes_failed_shortfall_rows_only() -> None:
    refetch = MOD._compute_targeted_refetch(_filt_frame(), _summary_rows())

    assert len(refetch) == 1
    row = refetch[0]
    assert row["league_id"] == "EPL"
    assert row["date"] == "2025-09-01"
    assert row["capture_status"] == "attempted_failed"


def test_targeted_refetch_excludes_non_shortfall_league() -> None:
    refetch = MOD._compute_targeted_refetch(_filt_frame(), _summary_rows())

    assert all(r["league_id"] != "BUNDESLIGA" for r in refetch)


def _raw_pending_frame() -> pd.DataFrame:
    """Index-shaped FIXTURES slice for ``_count_raw_pending_fetch``.

    Atom A appears twice — an older blank-reason expected_unattempted row that a
    NEWER empty_confirmed(EXPECTED_NO_FIXTURE) write superseded (consolidator
    latest-``written_at`` convention) → must NOT count. Atom B is a live blank
    pending row → counts. Atom C is a typed EXPECTED_* expected_unattempted row
    (honest absence, not pending) → must NOT count. Atom D is captured → no.
    """
    base = {
        "data_type": "FIXTURES",
        "source": "api_football",
        "pipeline_mode": "batch_api_football",
        "venue": "",
        "asset_group": "sports",
    }
    return pd.DataFrame(
        [
            # Atom A — superseded pending row (older written_at).
            {
                **base,
                "league_id": "EPL",
                "_date_str": "2025-09-01",
                "capture_status": "expected_unattempted",
                "error_reason": "",
                "written_at": "2026-06-28T21:31:49Z",
            },
            # Atom A — newer typed empty_confirmed write wins the dedup.
            {
                **base,
                "league_id": "EPL",
                "_date_str": "2025-09-01",
                "capture_status": "empty_confirmed",
                "error_reason": "EXPECTED_NO_FIXTURE",
                "written_at": "2026-07-10T01:30:00Z",
            },
            # Atom B — live blank pending row (counts).
            {
                **base,
                "league_id": "EPL",
                "_date_str": "2025-09-02",
                "capture_status": "expected_unattempted",
                "error_reason": "",
                "written_at": "2026-07-10T01:30:00Z",
            },
            # Atom C — typed EXPECTED_* expected_unattempted (not pending).
            {
                **base,
                "league_id": "BUNDESLIGA",
                "_date_str": "2025-09-01",
                "capture_status": "expected_unattempted",
                "error_reason": "EXPECTED_NO_FIXTURE",
                "written_at": "2026-07-10T01:30:00Z",
            },
            # Atom D — captured.
            {
                **base,
                "league_id": "EPL",
                "_date_str": "2025-09-03",
                "capture_status": "captured",
                "error_reason": "",
                "written_at": "2026-07-10T01:30:00Z",
            },
        ]
    )


def test_count_raw_pending_fetch_dedups_and_filters_expected_reasons() -> None:
    """Only atom B survives: dedup keeps the latest ``written_at`` per shard atom
    (A's newer empty_confirmed supersedes its older pending row) and EXPECTED_*
    typed reasons are honest absence, not pending fetch."""
    assert MOD._count_raw_pending_fetch(_raw_pending_frame()) == 1


def test_count_raw_pending_fetch_counts_blank_pending_without_written_at() -> None:
    """No ``written_at`` column → no dedup ordering available; raw rows still counted
    (never silently zero the metric on a schema-lean frame)."""
    df = _raw_pending_frame().drop(columns=["written_at"])
    # Without written_at ordering, drop_duplicates keeps the last occurrence per
    # atom — atom A's typed row (listed after the pending row) still wins here.
    assert MOD._count_raw_pending_fetch(df) == 1


def test_targeted_refetch_missing_capture_status_column_fails_fast() -> None:
    """capture_status is guaranteed present on real manifest rows (schema v9, already
    accessed unconditionally elsewhere in this script) — a row missing it entirely
    signals a structural problem upstream and must raise, not silently report ""."""
    filt = _filt_frame().drop(columns=["capture_status"])

    with pytest.raises(KeyError):
        MOD._compute_targeted_refetch(filt, _summary_rows())
