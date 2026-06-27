"""Unit tests for the Phase-3 cross-source announced_at backfill helpers in
``scripts/migrate_fixtures_split.py``.

Covers:
    * _parse_date_league_from_path — pure path parser
    * _fill_null_announced_at — UAC-floor + footystats min() logic

GCS I/O paths (``_load_footystats_map``, ``_migrate_one``, ``main``) are
integration concerns for the operator-driven --dry-run step; they are not
exercised here.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Load script as a module (lives under scripts/, not on the test import path).
# ---------------------------------------------------------------------------
_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "migrate_fixtures_split.py"
_SPEC = importlib.util.spec_from_file_location("migrate_fixtures_split", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules["migrate_fixtures_split"] = _MOD
_SPEC.loader.exec_module(_MOD)

_parse_date_league = _MOD._parse_date_league_from_path
_fill_null = _MOD._fill_null_announced_at

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------
_KICKOFF = datetime(2025, 9, 14, 14, 0, 0, tzinfo=UTC)
_FIXTURE_ID = "EPL:ARSENAL_v_CHELSEA:2025-09-14"
_EPL_AF_LEAGUE_ID = 39  # EPL floor = 21 days in UAC
_EPL_FLOOR_DAYS = 21
_DEFAULT_FLOOR_DAYS = 14


def _table_with_announced_at(
    announced_at_val: datetime | None,
    *,
    af_league_id: int | None = _EPL_AF_LEAGUE_ID,
    kickoff_str: str | None = _KICKOFF.isoformat(),
) -> pa.Table:
    return pa.table(
        {
            "fixture_id": pa.array([_FIXTURE_ID], type=pa.string()),
            "available_at": pa.array(
                [_KICKOFF - timedelta(days=7)], type=pa.timestamp("us", tz="UTC")
            ),
            "timestamp": pa.array([kickoff_str], type=pa.string()),
            "af_league_id": pa.array(
                [af_league_id], type=pa.int64() if af_league_id is not None else pa.null()
            ),
            "announced_at": pa.array(
                [announced_at_val], type=pa.timestamp("us", tz="UTC")
            ),
        }
    )


# ---------------------------------------------------------------------------
# _parse_date_league_from_path
# ---------------------------------------------------------------------------


class TestParseDateLeagueFromPath:
    def test_per_league_path(self) -> None:
        blob = "sports_reference/by_date/day=2025-09-14/entity=fixtures/league=EPL/fixtures.parquet"
        assert _parse_date_league(blob) == ("2025-09-14", "EPL")

    def test_bare_legacy_path_returns_empty_strings(self) -> None:
        blob = "sports_reference/by_date/day=2025-09-14/entity=fixtures/fixtures.parquet"
        date, league = _parse_date_league(blob)
        assert date == "2025-09-14"
        assert league == ""  # no league= segment

    def test_different_league(self) -> None:
        blob = "sports_reference/by_date/day=2024-11-02/entity=fixtures/league=BUNDESLIGA/fixtures.parquet"
        assert _parse_date_league(blob) == ("2024-11-02", "BUNDESLIGA")


# ---------------------------------------------------------------------------
# _fill_null_announced_at
# ---------------------------------------------------------------------------


class TestFillNullAnnouncedAt:
    def test_non_null_rows_unchanged(self) -> None:
        existing_ts = datetime(2025, 8, 24, tzinfo=UTC)
        table = _table_with_announced_at(existing_ts)
        result = _fill_null(table, footystats_map=None)
        assert result.column("announced_at")[0].as_py() == existing_ts

    def test_null_filled_with_uac_floor_epl(self) -> None:
        """EPL (league 39) → 21-day floor."""
        table = _table_with_announced_at(None)
        result = _fill_null(table, footystats_map=None)
        filled = result.column("announced_at")[0].as_py()
        assert filled is not None
        expected = _KICKOFF - timedelta(days=_EPL_FLOOR_DAYS)
        assert filled == expected

    def test_null_filled_with_default_floor_when_no_league(self) -> None:
        """Unknown af_league_id → default 14-day floor."""
        table = _table_with_announced_at(None, af_league_id=None)
        result = _fill_null(table, footystats_map=None)
        filled = result.column("announced_at")[0].as_py()
        assert filled is not None
        expected = _KICKOFF - timedelta(days=_DEFAULT_FLOOR_DAYS)
        assert filled == expected

    def test_footystats_earlier_than_uac_floor_wins(self) -> None:
        """footystats available_at < UAC floor → footystats value used."""
        ft_early = _KICKOFF - timedelta(days=25)  # 25d > 21d (EPL floor)
        ft_map = {_FIXTURE_ID: ft_early}
        table = _table_with_announced_at(None)
        result = _fill_null(table, footystats_map=ft_map)
        filled = result.column("announced_at")[0].as_py()
        assert filled == ft_early

    def test_footystats_later_than_uac_floor_uac_wins(self) -> None:
        """footystats available_at > UAC floor → min() picks UAC floor."""
        ft_late = _KICKOFF - timedelta(hours=72)  # 3d < 21d (EPL floor)
        ft_map = {_FIXTURE_ID: ft_late}
        table = _table_with_announced_at(None)
        result = _fill_null(table, footystats_map=ft_map)
        filled = result.column("announced_at")[0].as_py()
        expected = _KICKOFF - timedelta(days=_EPL_FLOOR_DAYS)
        assert filled == expected

    def test_footystats_map_miss_falls_back_to_uac_floor(self) -> None:
        """Fixture absent from footystats_map → UAC floor applied."""
        ft_map = {"OTHER_LEAGUE:X_v_Y:2025-09-14": _KICKOFF - timedelta(days=25)}
        table = _table_with_announced_at(None)
        result = _fill_null(table, footystats_map=ft_map)
        filled = result.column("announced_at")[0].as_py()
        expected = _KICKOFF - timedelta(days=_EPL_FLOOR_DAYS)
        assert filled == expected

    def test_null_kickoff_row_left_unchanged(self) -> None:
        """Row with null timestamp is not modified (can't compute fallback)."""
        table = _table_with_announced_at(None, kickoff_str=None)
        result = _fill_null(table, footystats_map=None)
        assert result.column("announced_at")[0].as_py() is None

    def test_no_announced_at_column_returns_unchanged(self) -> None:
        """Table without the announced_at column is returned as-is."""
        table = pa.table(
            {
                "fixture_id": pa.array([_FIXTURE_ID]),
                "available_at": pa.array(
                    [_KICKOFF - timedelta(days=7)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        )
        result = _fill_null(table, footystats_map=None)
        assert "announced_at" not in result.column_names

    def test_no_null_rows_table_returned_unchanged(self) -> None:
        """Table with no null announced_at rows is returned without modification."""
        ts = datetime(2025, 8, 20, tzinfo=UTC)
        table = _table_with_announced_at(ts)
        result = _fill_null(table, footystats_map=None)
        # Should be identical column values.
        assert result.column("announced_at")[0].as_py() == ts
        assert result.num_columns == table.num_columns
