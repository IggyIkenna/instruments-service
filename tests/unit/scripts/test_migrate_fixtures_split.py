"""Unit tests for ``scripts/migrate_fixtures_split.py``.

Covers pure split logic (_split_source, _target_names) and all migration cases
(A_split, B_partial, D_skip, F_read_failed). GCS I/O paths (_migrate_one with
real bucket calls) are integration concerns; this file only tests the pure
functions and the mock-bucket migration path.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import PreconditionFailed

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "migrate_fixtures_split.py"
_SPEC = importlib.util.spec_from_file_location("migrate_fixtures_split", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules["migrate_fixtures_split"] = _MOD
_SPEC.loader.exec_module(_MOD)

_split_source = _MOD._split_source
_target_names = _MOD._target_names
_migrate_one = _MOD._migrate_one
_ENTITY_SEGMENT = _MOD._ENTITY_SEGMENT
_ENTITY_SCHED = _MOD._ENTITY_SCHED
_ENTITY_OUTCOMES = _MOD._ENTITY_OUTCOMES
_SCHEDULE_KEEP = _MOD._SCHEDULE_KEEP
_OUTCOMES_KEEP = _MOD._OUTCOMES_KEEP
_serialise = _MOD._serialise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source_table(extra_cols: dict[str, list[object]] | None = None) -> pa.Table:
    """Minimal source entity=fixtures table with core schedule + outcomes cols."""
    base: dict[str, list[object]] = {
        "af_fixture_id": [1001, 1002],
        "date": ["2026-06-16", "2026-06-16"],
        "timestamp": ["2026-06-16T15:00:00+00:00", "2026-06-16T17:30:00+00:00"],
        "league_id": ["ENG1", "ESP1"],
        "day": ["2026-06-16", "2026-06-16"],
        "af_league_id": [39, 140],
        "af_home_id": [33, 541],
        "af_away_id": [34, 529],
        "af_home_name": ["Man Utd", "Real Madrid"],
        "af_away_name": ["Man City", "Barcelona"],
        "status_long": ["Not Started", "Match Finished"],
        "status_short": ["NS", "FT"],
        "available_at": ["2026-06-09T15:00:00+00:00", "2026-06-09T17:30:00+00:00"],
        "announced_at": ["2026-06-09T15:00:00+00:00", "2026-06-09T17:30:00+00:00"],
        "home_score": [None, 2],
        "away_score": [None, 1],
        "home_score_halftime": [None, 1],
        "away_score_halftime": [None, 0],
        "home_score_fulltime": [None, 2],
        "away_score_fulltime": [None, 1],
        "match_end_time": [None, "2026-06-16T19:10:00+00:00"],
        "report_time": [None, "2026-06-16T19:15:00+00:00"],
    }
    if extra_cols:
        base.update(extra_cols)
    return pa.table(base)


# ---------------------------------------------------------------------------
# _target_names
# ---------------------------------------------------------------------------

def test_target_names_legacy_path() -> None:
    src = "sports_reference/by_date/day=2026-06-16/entity=fixtures/league=ENG1/fixtures.parquet"
    sched, out = _target_names(src)
    assert sched == "sports_reference/by_date/day=2026-06-16/entity=fixtures_schedule/league=ENG1/fixtures.parquet"
    assert out == "sports_reference/by_date/day=2026-06-16/entity=fixtures_outcomes/league=ENG1/fixtures.parquet"


def test_target_names_pipeline_mode_path() -> None:
    src = "sports_reference/by_date/day=2026-06-16/pipeline_mode=batch_api_football/entity=fixtures/league=ENG1/fixtures.parquet"
    sched, out = _target_names(src)
    assert "pipeline_mode=batch_api_football/entity=fixtures_schedule/" in sched
    assert "pipeline_mode=batch_api_football/entity=fixtures_outcomes/" in out


def test_entity_segment_does_not_match_already_split_schedule() -> None:
    schedule_path = "sports_reference/by_date/day=2026-06-16/entity=fixtures_schedule/league=ENG1/fixtures.parquet"
    assert _ENTITY_SEGMENT not in schedule_path


def test_entity_segment_does_not_match_already_split_outcomes() -> None:
    outcomes_path = "sports_reference/by_date/day=2026-06-16/entity=fixtures_outcomes/league=ENG1/fixtures.parquet"
    assert _ENTITY_SEGMENT not in outcomes_path


# ---------------------------------------------------------------------------
# _split_source — column routing
# ---------------------------------------------------------------------------

def test_split_source_schedule_cols_are_subset_of_keep() -> None:
    table = _make_source_table()
    sched, _ = _split_source(table)
    for col in sched.column_names:
        assert col in _SCHEDULE_KEEP, f"Unexpected schedule column: {col}"


def test_split_source_outcomes_cols_are_subset_of_keep() -> None:
    table = _make_source_table()
    _, out = _split_source(table)
    for col in out.column_names:
        assert col in _OUTCOMES_KEEP, f"Unexpected outcomes column: {col}"


def test_split_source_no_column_appears_in_both_outputs() -> None:
    # Join keys (af_fixture_id, league_id, day) and available_at are intentionally
    # shared — available_at is required in every entity for the writegate check.
    allowed_shared = {"af_fixture_id", "league_id", "day", "available_at"}
    table = _make_source_table()
    sched, out = _split_source(table)
    shared = set(sched.column_names) & set(out.column_names)
    unexpected_shared = shared - allowed_shared
    assert not unexpected_shared, f"Unexpected shared columns: {unexpected_shared}"


def test_split_source_schedule_contains_core_schedule_cols() -> None:
    table = _make_source_table()
    sched, _ = _split_source(table)
    for col in ("af_fixture_id", "league_id", "day", "timestamp", "announced_at", "available_at"):
        assert col in sched.column_names, f"Missing schedule column: {col}"


def test_split_source_outcomes_contains_core_outcomes_cols() -> None:
    table = _make_source_table()
    _, out = _split_source(table)
    for col in ("af_fixture_id", "league_id", "day", "home_score", "away_score", "match_end_time"):
        assert col in out.column_names, f"Missing outcomes column: {col}"


def test_split_source_preserves_row_count() -> None:
    table = _make_source_table()
    sched, out = _split_source(table)
    assert sched.num_rows == table.num_rows
    assert out.num_rows == table.num_rows


# ---------------------------------------------------------------------------
# _split_source — outcomes available_at overwrite (lookahead-bias fix)
# ---------------------------------------------------------------------------

def test_split_source_outcomes_available_at_equals_match_end_time() -> None:
    table = _make_source_table()
    _, out = _split_source(table)
    assert "available_at" in out.column_names
    assert "match_end_time" in out.column_names
    # Row 0: match_end_time is None → available_at should also be None.
    # Row 1: match_end_time is set → available_at should mirror it.
    met = out.column("match_end_time").to_pylist()
    avail = out.column("available_at").to_pylist()
    assert met[0] is None
    assert avail[0] is None
    # Row 1: both should carry the same match end value.
    assert avail[1] == met[1]


def test_split_source_schedule_available_at_is_original_announced_at_value() -> None:
    # Schedule available_at must NOT be replaced with match_end_time.
    table = _make_source_table()
    sched, _ = _split_source(table)
    orig_avail = table.column("available_at").to_pylist()
    sched_avail = sched.column("available_at").to_pylist()
    assert sched_avail == orig_avail


# ---------------------------------------------------------------------------
# _split_source — Q5/Q6 optional columns
# ---------------------------------------------------------------------------

def test_split_source_includes_q5_cols_when_present() -> None:
    q5 = {
        "halftime_start_time": ["2026-06-16T15:45:00+00:00", "2026-06-16T17:45:00+00:00"],
        "halftime_end_time": ["2026-06-16T16:00:00+00:00", "2026-06-16T18:00:00+00:00"],
        "whistle_full_time_at": [None, "2026-06-16T19:00:00+00:00"],
    }
    table = _make_source_table(extra_cols=q5)
    sched, _ = _split_source(table)
    for col in q5:
        assert col in sched.column_names, f"Q5 column missing from schedule: {col}"


def test_split_source_includes_q6_cols_when_present() -> None:
    q6 = {
        "home_score_regulation": [None, 2],
        "away_score_regulation": [None, 1],
        "went_to_extra_time": [False, False],
        "went_to_penalties": [False, False],
        "match_result": [None, "HOME_WIN"],
    }
    table = _make_source_table(extra_cols=q6)
    _, out = _split_source(table)
    for col in q6:
        assert col in out.column_names, f"Q6 column missing from outcomes: {col}"


def test_split_source_silently_skips_absent_q5_cols() -> None:
    # Older legacy rows without Q5 columns → no KeyError, just absent columns.
    table = _make_source_table()
    sched, _ = _split_source(table)
    assert "halftime_start_time" not in sched.column_names


def test_split_source_silently_skips_absent_q6_cols() -> None:
    table = _make_source_table()
    _, out = _split_source(table)
    assert "home_score_regulation" not in out.column_names


# ---------------------------------------------------------------------------
# _migrate_one — case classification via mock bucket
# ---------------------------------------------------------------------------

def _make_bucket_mock(
    sched_exists: bool = False,
    out_exists: bool = False,
    source_table: pa.Table | None = None,
    upload_raises: Exception | None = None,
) -> MagicMock:
    """Create a minimal bucket mock for _migrate_one unit tests."""
    bucket = MagicMock()

    if source_table is None:
        source_table = _make_source_table()
    raw = _serialise(source_table)

    source_blob = MagicMock()
    source_blob.download_as_bytes.return_value = raw

    sched_blob = MagicMock()
    sched_blob.exists.return_value = sched_exists
    if upload_raises is not None:
        sched_blob.upload_from_string.side_effect = upload_raises
    else:
        sched_blob.upload_from_string.return_value = None

    out_blob = MagicMock()
    out_blob.exists.return_value = out_exists
    if upload_raises is not None:
        out_blob.upload_from_string.side_effect = upload_raises
    else:
        out_blob.upload_from_string.return_value = None

    def _blob_factory(name: str) -> MagicMock:
        if _ENTITY_SCHED in name:
            return sched_blob
        if _ENTITY_OUTCOMES in name:
            return out_blob
        return source_blob

    bucket.blob.side_effect = _blob_factory
    return bucket


_SOURCE_BLOB = "sports_reference/by_date/day=2026-06-16/entity=fixtures/league=ENG1/fixtures.parquet"


def test_migrate_one_d_skip_when_both_targets_exist() -> None:
    bucket = _make_bucket_mock(sched_exists=True, out_exists=True)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    assert result.case == "D_skip"
    assert result.rows == 0
    assert result.error is None


def test_migrate_one_a_split_writes_both_targets() -> None:
    bucket = _make_bucket_mock(sched_exists=False, out_exists=False)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    assert result.case == "A_split"
    assert result.rows == 2
    assert result.error is None


def test_migrate_one_b_partial_when_only_schedule_exists() -> None:
    bucket = _make_bucket_mock(sched_exists=True, out_exists=False)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    # Only outcomes was written → B_partial
    assert result.case == "B_partial"


def test_migrate_one_b_partial_when_only_outcomes_exists() -> None:
    bucket = _make_bucket_mock(sched_exists=False, out_exists=True)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    assert result.case == "B_partial"


def test_migrate_one_dry_run_a_split_no_upload() -> None:
    bucket = _make_bucket_mock(sched_exists=False, out_exists=False)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=True)
    assert result.case == "A_split"
    assert "dry-run" in (result.error or "")
    # Confirm rows were counted (source was read) but no upload was attempted.
    assert result.rows == 2


def test_migrate_one_dry_run_b_partial_when_one_target_exists() -> None:
    bucket = _make_bucket_mock(sched_exists=True, out_exists=False)
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=True)
    assert result.case == "B_partial"


def test_migrate_one_f_read_failed_on_download_error() -> None:
    bucket = MagicMock()
    source_blob = MagicMock()
    source_blob.download_as_bytes.side_effect = OSError("network failure")

    def _blob_factory(name: str) -> MagicMock:
        if _ENTITY_SCHED in name or _ENTITY_OUTCOMES in name:
            b = MagicMock()
            b.exists.return_value = False
            return b
        return source_blob

    bucket.blob.side_effect = _blob_factory
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    assert result.case == "F_read_failed"
    assert "read:" in (result.error or "")


def test_migrate_one_d_skip_when_upload_returns_precondition_failed() -> None:
    # PreconditionFailed on if_generation_match=0 means the target already existed
    # (another concurrent migrator wrote it first). Both uploads fail → D_skip.
    bucket = _make_bucket_mock(
        sched_exists=False,
        out_exists=False,
        upload_raises=PreconditionFailed("already exists"),
    )
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=False)
    # _upload_if_new catches PreconditionFailed and returns False (exists).
    # Neither sched_written nor out_written → D_skip.
    assert result.case == "D_skip"


def test_migrate_one_target_names_embedded_in_result() -> None:
    bucket = _make_bucket_mock()
    result = _migrate_one(bucket, _SOURCE_BLOB, dry_run=True)
    assert _ENTITY_SCHED in result.sched_name
    assert _ENTITY_OUTCOMES in result.out_name


# ---------------------------------------------------------------------------
# Round-trip: serialise → read_table preserves split column sets
# ---------------------------------------------------------------------------

def test_serialised_schedule_is_valid_parquet() -> None:
    table = _make_source_table()
    sched, _ = _split_source(table)
    raw = _serialise(sched)
    recovered = pq.read_table(io.BytesIO(raw))
    assert recovered.num_rows == sched.num_rows
    assert set(recovered.column_names) == set(sched.column_names)


def test_serialised_outcomes_is_valid_parquet() -> None:
    table = _make_source_table()
    _, out = _split_source(table)
    raw = _serialise(out)
    recovered = pq.read_table(io.BytesIO(raw))
    assert recovered.num_rows == out.num_rows
    assert set(recovered.column_names) == set(out.column_names)
