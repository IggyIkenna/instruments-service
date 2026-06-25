"""Unit tests for instruments_service.sports.fixture_completeness.

Covers the five completeness checks:
  1. MISSING_FIXTURES
  2. TEAM_COUNT_MISMATCH
  3. UNEXPECTED_GAP
  4. SEASON_WINDOW_DRIFT
  5. RESCHEDULE_STALE_TIME

Uses the real UAC SeasonStructure registry (EPL: 20 teams, 380 fixtures,
DOUBLE_ROUND_ROBIN) as the ground truth so the tests pin against the actual
registry values.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from instruments_service.sports.fixture_completeness import (
    CompletenessReport,
    FixtureDefect,
    FixtureDefectKind,
    validate_fixture_completeness,
)

# ---------------------------------------------------------------------------
# Helpers for building minimal fixture rows
# ---------------------------------------------------------------------------

_N_EPL_TEAMS = 20
_EPL_EXPECTED_FIXTURES = 380  # 20 * 19
_EPL_SEASON = 2024


def _make_rows(
    n_fixtures: int = _EPL_EXPECTED_FIXTURES,
    n_teams: int = _N_EPL_TEAMS,
    start_date: date = date(2024, 8, 17),
    *,
    include_available_at: bool = False,
) -> list[dict[str, object]]:
    """Build minimal fixture rows for EPL 2024-25 with no defects."""
    rows: list[dict[str, object]] = []
    teams = list(range(1, n_teams + 1))

    # Produce n_fixtures rows with evenly-spaced dates
    for i in range(n_fixtures):
        home_idx = i % n_teams
        away_idx = (i + 1) % n_teams
        # Avoid same team playing itself
        if home_idx == away_idx:
            away_idx = (away_idx + 1) % n_teams
        kickoff_dt = datetime(start_date.year, start_date.month, start_date.day, 15, 0, tzinfo=UTC) + timedelta(
            days=(i * 7 // 10)
        )
        row: dict[str, object] = {
            "af_fixture_id": i + 1,
            "af_home_id": teams[home_idx],
            "af_away_id": teams[away_idx],
            "timestamp": kickoff_dt.isoformat(),
            "status_short": "FT",
        }
        if include_available_at:
            row["available_at"] = (kickoff_dt - timedelta(days=7)).isoformat()
        rows.append(row)
    return rows


def _complete_epl_rows() -> list[dict[str, object]]:
    """380 EPL 2024-25 rows: 20 teams, every DRR pair, no defects."""
    teams = list(range(1, _N_EPL_TEAMS + 1))
    rows: list[dict[str, object]] = []
    fid = 1
    # Build all home/away pairs
    pairs = [(h, a) for h in teams for a in teams if h != a]
    assert len(pairs) == _EPL_EXPECTED_FIXTURES

    for idx, (home, away) in enumerate(pairs):
        d = date(2024, 8, 17) + timedelta(days=(idx * 7 // 10))
        rows.append(
            {
                "af_fixture_id": fid,
                "af_home_id": home,
                "af_away_id": away,
                "timestamp": datetime(d.year, d.month, d.day, 15, 0, tzinfo=UTC).isoformat(),
                "status_short": "FT",
                "available_at": datetime(
                    d.year, d.month, d.day, 15, 0, tzinfo=UTC - timedelta(days=7) if False else UTC
                )
                .replace(tzinfo=UTC)
                .isoformat(),  # will be overridden below
            }
        )
        # Set available_at = kickoff - 7 days correctly
        kickoff_dt = datetime(d.year, d.month, d.day, 15, 0, tzinfo=UTC)
        rows[-1]["available_at"] = (kickoff_dt - timedelta(days=7)).isoformat()
        fid += 1
    return rows


# ---------------------------------------------------------------------------
# Basic smoke tests
# ---------------------------------------------------------------------------


class TestCompletenessReportIsComplete:
    def test_perfect_epl_season_is_complete(self) -> None:
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)

        assert report.is_complete is True
        assert report.defects == []
        assert report.captured_fixture_count == _EPL_EXPECTED_FIXTURES
        assert report.expected_fixture_count == _EPL_EXPECTED_FIXTURES
        assert report.distinct_team_count == _N_EPL_TEAMS
        assert report.expected_team_count == _N_EPL_TEAMS

    def test_report_fields(self) -> None:
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert report.league_id == "EPL"
        assert report.season_year == _EPL_SEASON

    def test_empty_rows_not_complete(self) -> None:
        report = validate_fixture_completeness("EPL", _EPL_SEASON, [])
        assert report.is_complete is False
        assert report.captured_fixture_count == 0

    def test_unknown_league_returns_report_with_none_expected(self) -> None:
        report = validate_fixture_completeness("UNKNOWN_LEAGUE_XYZ", 2024, [])
        assert report.expected_fixture_count is None
        assert report.expected_team_count is None
        assert report.is_complete is False  # no registry entry


# ---------------------------------------------------------------------------
# Check 1: MISSING_FIXTURES
# ---------------------------------------------------------------------------


class TestMissingFixtures:
    def test_shortfall_produces_defect(self) -> None:
        rows = _make_rows(n_fixtures=200, n_teams=_N_EPL_TEAMS)
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        kinds = {d.kind for d in report.defects}
        assert FixtureDefectKind.MISSING_FIXTURES in kinds

    def test_shortfall_defect_detail(self) -> None:
        rows = _make_rows(n_fixtures=300, n_teams=_N_EPL_TEAMS)
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        defect = next(d for d in report.defects if d.kind == FixtureDefectKind.MISSING_FIXTURES)
        assert defect.detail["captured"] == 300
        assert defect.detail["expected"] == _EPL_EXPECTED_FIXTURES
        assert defect.detail["shortfall"] == 80

    def test_exact_expected_no_missing_defect(self) -> None:
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.MISSING_FIXTURES for d in report.defects)

    def test_excess_fixtures_produces_defect(self) -> None:
        """Captured > expected + promo_extra → MISSING_FIXTURES (excess variant)."""
        rows = _make_rows(n_fixtures=_EPL_EXPECTED_FIXTURES + 50, n_teams=_N_EPL_TEAMS)
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        # EPL has promo_extra=0, so 380+50 > 380+0
        assert any(d.kind == FixtureDefectKind.MISSING_FIXTURES for d in report.defects)

    def test_bundesliga_expected_count(self) -> None:
        """Bundesliga: 18 teams → 306 fixtures."""
        rows = _make_rows(n_fixtures=306, n_teams=18, start_date=date(2024, 8, 16))
        # 306 == expected — should not flag MISSING_FIXTURES
        report = validate_fixture_completeness("BUNDESLIGA", 2024, rows)
        assert not any(d.kind == FixtureDefectKind.MISSING_FIXTURES for d in report.defects)


# ---------------------------------------------------------------------------
# Check 2: TEAM_COUNT_MISMATCH
# ---------------------------------------------------------------------------


class TestTeamCountMismatch:
    def test_fewer_teams_flags_defect(self) -> None:
        rows = _make_rows(n_fixtures=100, n_teams=18)  # EPL expects 20
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert any(d.kind == FixtureDefectKind.TEAM_COUNT_MISMATCH for d in report.defects)

    def test_correct_team_count_no_mismatch_defect(self) -> None:
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.TEAM_COUNT_MISMATCH for d in report.defects)

    def test_per_team_game_count_defect(self) -> None:
        """One team missing games should trigger TEAM_COUNT_MISMATCH."""
        rows = _complete_epl_rows()
        # Remove all fixtures involving team 1
        rows_filtered = [r for r in rows if r["af_home_id"] != 1 and r["af_away_id"] != 1]
        # 20 distinct teams still (team 1 won't appear anymore) — actually no,
        # if we remove team 1, it won't show up in the teams set either.
        # Let's instead remove only HOME fixtures of team 1 to keep 20 teams
        # but reduce team 1's game count
        rows_partial = [r for r in rows if r["af_home_id"] != 1]
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows_partial)
        # Team 1 now appears only as away team (19 times), not as home team (0)
        # Total games = 19 < expected 38
        assert any(d.kind == FixtureDefectKind.TEAM_COUNT_MISMATCH for d in report.defects)


# ---------------------------------------------------------------------------
# Check 3: UNEXPECTED_GAP
# ---------------------------------------------------------------------------


class TestUnexpectedGap:
    def test_large_unexplained_gap_flags_defect(self) -> None:
        """A 40-day gap in June (not an expected EPL break) should be flagged."""
        rows = _complete_epl_rows()
        # Move half the rows to be much later (simulates a big mid-season gap)
        mid = len(rows) // 2
        for i in range(mid, len(rows)):
            old_ts = rows[i]["timestamp"]
            assert isinstance(old_ts, str)
            dt = datetime.fromisoformat(old_ts.replace("Z", "+00:00"))
            # Push 40 days later to create an unexplained gap
            new_dt = dt + timedelta(days=40)
            rows[i]["timestamp"] = new_dt.isoformat()
            if "available_at" in rows[i]:
                rows[i]["available_at"] = (new_dt - timedelta(days=7)).isoformat()

        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert any(d.kind == FixtureDefectKind.UNEXPECTED_GAP for d in report.defects)

    def test_short_gaps_not_flagged(self) -> None:
        """A 7-day gap is normal (weekly matches) — should NOT be flagged."""
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.UNEXPECTED_GAP for d in report.defects)


# ---------------------------------------------------------------------------
# Check 4: SEASON_WINDOW_DRIFT
# ---------------------------------------------------------------------------


class TestSeasonWindowDrift:
    def test_fixtures_well_within_window_no_drift(self) -> None:
        rows = _complete_epl_rows()
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.SEASON_WINDOW_DRIFT for d in report.defects)

    def test_late_start_flags_drift(self) -> None:
        """All fixtures starting 60 days after expected season start → drift."""
        rows = _make_rows(
            n_fixtures=_EPL_EXPECTED_FIXTURES, n_teams=_N_EPL_TEAMS, start_date=date(2024, 11, 1)
        )  # EPL starts Aug; 75 days late
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert any(d.kind == FixtureDefectKind.SEASON_WINDOW_DRIFT for d in report.defects)

    def test_season_end_overrun_flags_drift(self) -> None:
        """Fixtures running into September 2026 for 2024 season → drift."""
        rows = _make_rows(
            n_fixtures=_EPL_EXPECTED_FIXTURES, n_teams=_N_EPL_TEAMS, start_date=date(2026, 7, 1)
        )  # far beyond May 2025 end
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert any(d.kind == FixtureDefectKind.SEASON_WINDOW_DRIFT for d in report.defects)

    def test_empty_rows_no_window_defect(self) -> None:
        report = validate_fixture_completeness("EPL", _EPL_SEASON, [])
        assert not any(d.kind == FixtureDefectKind.SEASON_WINDOW_DRIFT for d in report.defects)


# ---------------------------------------------------------------------------
# Check 5: RESCHEDULE_STALE_TIME
# ---------------------------------------------------------------------------


class TestRescheduleStaleTime:
    def test_correct_available_at_no_defect(self) -> None:
        rows = _complete_epl_rows()  # available_at = kickoff - 7 days (exact)
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.RESCHEDULE_STALE_TIME for d in report.defects)

    def test_stale_available_at_flags_defect(self) -> None:
        """available_at that is 10 days off the expected kickoff - 7 days → stale."""
        rows = _complete_epl_rows()
        # Make the first row's available_at stale (10 days off)
        ts = rows[0]["timestamp"]
        assert isinstance(ts, str)
        kickoff_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        rows[0]["available_at"] = (kickoff_dt - timedelta(days=17)).isoformat()  # 10 days extra

        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        stale = [d for d in report.defects if d.kind == FixtureDefectKind.RESCHEDULE_STALE_TIME]
        assert len(stale) >= 1
        assert stale[0].fixture_id is not None

    def test_no_available_at_no_defect(self) -> None:
        """Rows without available_at should not be flagged."""
        rows = _make_rows(n_fixtures=10, n_teams=_N_EPL_TEAMS)
        # No available_at field in these rows
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert not any(d.kind == FixtureDefectKind.RESCHEDULE_STALE_TIME for d in report.defects)

    def test_stale_detail_fields(self) -> None:
        rows = _complete_epl_rows()
        ts = rows[0]["timestamp"]
        assert isinstance(ts, str)
        kickoff_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        # Set available_at 15 days earlier than expected (kickoff - 22 days)
        rows[0]["available_at"] = (kickoff_dt - timedelta(days=22)).isoformat()

        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        stale = next(d for d in report.defects if d.kind == FixtureDefectKind.RESCHEDULE_STALE_TIME)
        assert "fixture_id" in stale.detail
        assert "drift_hours" in stale.detail
        assert stale.detail["drift_hours"] > 0


# ---------------------------------------------------------------------------
# Alternate row key names (fixture_id / home_team_id / away_team_id fallbacks)
# ---------------------------------------------------------------------------


class TestAlternateRowKeys:
    def test_fixture_id_fallback(self) -> None:
        rows = [
            {
                "fixture_id": str(i),
                "home_team_id": i % 20 + 1,
                "away_team_id": (i + 1) % 20 + 1,
                "timestamp": datetime(2024, 9, 1, 15, 0, tzinfo=UTC).isoformat(),
            }
            for i in range(50)
        ]
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert report.captured_fixture_count == 50

    def test_kickoff_utc_column_fallback(self) -> None:
        rows = [
            {
                "af_fixture_id": i,
                "af_home_id": i % 20 + 1,
                "af_away_id": (i + 1) % 20 + 1,
                "kickoff_utc": datetime(2024, 9, 1, 15, 0, tzinfo=UTC).isoformat(),
            }
            for i in range(50)
        ]
        report = validate_fixture_completeness("EPL", _EPL_SEASON, rows)
        assert report.captured_fixture_count == 50


# ---------------------------------------------------------------------------
# FixtureDefect dataclass
# ---------------------------------------------------------------------------


class TestFixtureDefectDataclass:
    def test_frozen(self) -> None:
        d = FixtureDefect(kind=FixtureDefectKind.MISSING_FIXTURES, description="test")
        with pytest.raises((AttributeError, TypeError)):
            d.kind = FixtureDefectKind.TEAM_COUNT_MISMATCH  # type: ignore[misc]

    def test_default_detail_is_empty_dict(self) -> None:
        d = FixtureDefect(kind=FixtureDefectKind.MISSING_FIXTURES, description="test")
        assert d.detail == {}
        assert d.fixture_id is None
