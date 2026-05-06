"""Unit tests for ``scripts/migrate_sports_available_at_column.py``.

Covers the pure migration logic for all 4 schema cases (A/B/C/D) plus the
case-classification helper. GCS I/O paths (``_migrate_one``, ``main``) are not
exercised here — they are integration concerns for the operator-driven
``--dry-run`` step.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pytest

# Load the script as a module (it lives under scripts/, which is not on the
# default test import path).
_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "migrate_sports_available_at_column.py"
_SPEC = importlib.util.spec_from_file_location("migrate_sports_available_at_column", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules["migrate_sports_available_at_column"] = _MOD
_SPEC.loader.exec_module(_MOD)


CANONICAL_COL = _MOD.CANONICAL_COL
LEGACY_COL = _MOD.LEGACY_COL
_classify_schema = _MOD._classify_schema
_apply_migration = _MOD._apply_migration


# ---------------------------------------------------------------------------
# _classify_schema — pure function, 4 cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        (("fixture_id", LEGACY_COL, "kickoff_utc"), "A_renamed"),
        (("fixture_id", LEGACY_COL, CANONICAL_COL, "kickoff_utc"), "B_dedup"),
        (("fixture_id", CANONICAL_COL, "kickoff_utc"), "C_skip_already_canonical"),
        (("fixture_id", "kickoff_utc"), "D_skip_neither"),
    ],
)
def test_classify_schema_covers_all_four_cases(columns: tuple[str, ...], expected: str) -> None:
    assert _classify_schema(columns) == expected


def test_classify_schema_handles_empty_columns() -> None:
    assert _classify_schema(()) == "D_skip_neither"


# ---------------------------------------------------------------------------
# _apply_migration — pure function, transforms pyarrow tables
# ---------------------------------------------------------------------------


def _make_table(columns: dict[str, list[object]]) -> pa.Table:
    return pa.table(columns)  # type: ignore[arg-type]


def test_case_a_renames_legacy_to_canonical() -> None:
    table = _make_table(
        {
            "fixture_id": ["F1", "F2"],
            LEGACY_COL: ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
            "kickoff_utc": ["2024-01-01T15:00:00Z", "2024-01-02T15:00:00Z"],
        }
    )
    new_table, case = _apply_migration(table)
    assert case == "A_renamed"
    assert new_table.column_names == ["fixture_id", CANONICAL_COL, "kickoff_utc"]
    # Cell values for the renamed column survive verbatim.
    assert new_table.column(CANONICAL_COL).to_pylist() == [
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
    ]


def test_case_b_drops_legacy_keeps_canonical_when_both_present() -> None:
    # Mid-migration restart shape — both columns exist; canonical is the
    # post-rename source-of-truth, legacy is the pre-rename leftover.
    table = _make_table(
        {
            "fixture_id": ["F1"],
            LEGACY_COL: ["2024-01-01T00:00:00Z"],
            CANONICAL_COL: ["2024-01-01T00:00:00Z"],
            "kickoff_utc": ["2024-01-01T15:00:00Z"],
        }
    )
    new_table, case = _apply_migration(table)
    assert case == "B_dedup"
    assert LEGACY_COL not in new_table.column_names
    assert CANONICAL_COL in new_table.column_names


def test_case_c_already_canonical_is_a_noop() -> None:
    table = _make_table(
        {
            "fixture_id": ["F1"],
            CANONICAL_COL: ["2024-01-01T00:00:00Z"],
            "kickoff_utc": ["2024-01-01T15:00:00Z"],
        }
    )
    new_table, case = _apply_migration(table)
    assert case == "C_skip_already_canonical"
    # Same-object identity guarantees we don't pay re-serialisation cost on
    # already-migrated parquets.
    assert new_table is table


def test_case_d_neither_column_is_a_noop() -> None:
    table = _make_table({"fixture_id": ["F1"], "kickoff_utc": ["2024-01-01T15:00:00Z"]})
    new_table, case = _apply_migration(table)
    assert case == "D_skip_neither"
    assert new_table is table


def test_case_a_preserves_row_count_and_other_columns() -> None:
    table = _make_table(
        {
            "fixture_id": ["F1", "F2", "F3"],
            "league_id": ["L1", "L2", "L3"],
            LEGACY_COL: ["t1", "t2", "t3"],
            "extra_col": [1, 2, 3],
        }
    )
    new_table, _ = _apply_migration(table)
    assert new_table.num_rows == 3
    assert set(new_table.column_names) == {"fixture_id", "league_id", CANONICAL_COL, "extra_col"}
    assert new_table.column("fixture_id").to_pylist() == ["F1", "F2", "F3"]


def test_case_a_idempotent_when_rerun_through_apply_migration() -> None:
    # Running the migration on already-migrated output must classify as C and
    # be a no-op. Guards against double-rename corruption on rerun.
    table = _make_table(
        {
            "fixture_id": ["F1"],
            LEGACY_COL: ["t1"],
            "kickoff_utc": ["k1"],
        }
    )
    once, _ = _apply_migration(table)
    twice, case_twice = _apply_migration(once)
    assert case_twice == "C_skip_already_canonical"
    assert twice is once
