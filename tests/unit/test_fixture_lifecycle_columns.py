"""Tests for the Q5/Q6 lifecycle-column overlay on ``_flatten_canonical_fixture_for_disk``.

Phase 3 of the fixture-schedule-split epic (``plans/epics/sports_master.md`` §
"Match HT/ET/PEN timestamps + score-distinction columns"). The flatten function
takes an optional ``af_response`` raw api-football dict; when supplied it
populates the Q5 HT/ET/PEN phase-timestamp columns + the Q6 score-distinction
columns via the UTL ``extract_match_lifecycle`` SSOT. When absent the columns
take honest defaults (None scores/timestamps, False ``went_to_*``).

These tests pin: (a) the new columns appear on the flatten output; (b) a
regulation match leaves ET/PEN columns NULL + ``went_to_*`` False; (c) an
ET-only match populates ET scores + ``went_to_extra_time`` but NOT penalty
columns; (d) an ET+PEN match populates the full penalty distinction +
``match_result`` = ``home_win_after_pens`` / ``away_win_after_pens``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd

from instruments_service.engine.orchestrator import (
    _Q5_SCHEDULE_COLUMNS,
    _Q6_OUTCOME_COLUMNS,
    _flatten_canonical_fixture_for_disk,
)

_KICKOFF = datetime(2024, 8, 17, 11, 30, tzinfo=UTC)
_SECOND_HALF = datetime(2024, 8, 17, 12, 30, tzinfo=UTC)


def _make_canonical_fixture(*, status: str = "FT", home_goals: int = 2, away_goals: int = 1) -> SimpleNamespace:
    """SimpleNamespace shaped like CanonicalFixture (flatten uses getattr only)."""
    return SimpleNamespace(
        fixture_id="1208022",
        source_fixture_id="1208022",
        home_team=SimpleNamespace(api_football_id=57, name="Ipswich"),
        away_team=SimpleNamespace(api_football_id=40, name="Liverpool"),
        league=SimpleNamespace(api_football_id=39, league_id="EPL"),
        venue=SimpleNamespace(api_football_id=545, name="Portman Road", city="Ipswich"),
        referee=SimpleNamespace(name="T. Robinson"),
        kickoff_utc=_KICKOFF,
        season="2024-25",
        status=status,
        home_goals=home_goals,
        away_goals=away_goals,
        home_goals_halftime=0,
        away_goals_halftime=0,
    )


def _af_response(
    *,
    status_short: str,
    fulltime: dict[str, int] | None,
    extratime: dict[str, int] | None = None,
    penalty: dict[str, int] | None = None,
    goals: dict[str, int] | None = None,
    with_periods: bool = True,
) -> dict[str, object]:
    """Build a minimal raw api-football fixture response item."""
    fixture: dict[str, object] = {
        "id": 1208022,
        "timestamp": int(_KICKOFF.timestamp()),
        "status": {"short": status_short},
        "venue": {"name": "Portman Road"},
    }
    if with_periods:
        fixture["periods"] = {"first": int(_KICKOFF.timestamp()), "second": int(_SECOND_HALF.timestamp())}
    score: dict[str, object] = {}
    if fulltime is not None:
        score["fulltime"] = fulltime
    if extratime is not None:
        score["extratime"] = extratime
    if penalty is not None:
        score["penalty"] = penalty
    return {
        "fixture": fixture,
        "league": {"id": 39},
        "teams": {"home": {"id": 57}, "away": {"id": 40}},
        "goals": goals if goals is not None else fulltime,
        "score": score,
    }


_ALL_LIFECYCLE_COLUMNS = set(_Q5_SCHEDULE_COLUMNS) | set(_Q6_OUTCOME_COLUMNS)


def test_lifecycle_columns_present_on_flatten_output() -> None:
    """Every Q5/Q6 column appears on the flatten output (with and without af_response)."""
    fx = _make_canonical_fixture()
    without = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    af = _af_response(status_short="FT", fulltime={"home": 2, "away": 1})
    with_resp = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response=af)
    assert _ALL_LIFECYCLE_COLUMNS.issubset(without.keys())
    assert _ALL_LIFECYCLE_COLUMNS.issubset(with_resp.keys())


def test_no_af_response_defaults_lifecycle_columns() -> None:
    """Without af_response: timestamps/scores None, went_to_* False."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    for col in _Q5_SCHEDULE_COLUMNS:
        assert out[col] is None, f"{col} should default None"
    assert out["home_score_regulation"] is None
    assert out["home_score_after_extra_time"] is None
    assert out["home_penalty_shootout_score"] is None
    assert out["went_to_extra_time"] is False
    assert out["went_to_penalties"] is False
    assert out["match_result"] is None


def test_regulation_match_et_pen_columns_null() -> None:
    """A regulation (FT) match: ET/PEN columns NULL, went_to_* False, regulation result set."""
    fx = _make_canonical_fixture(status="FT", home_goals=2, away_goals=1)
    af = _af_response(status_short="FT", fulltime={"home": 2, "away": 1})
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response=af)

    # ET/PEN timestamps + scores stay NULL on a regulation match.
    assert out["extra_time_first_half_start_time"] is None
    assert out["penalty_shootout_start_time"] is None
    assert out["home_score_after_extra_time"] is None
    assert out["away_score_after_extra_time"] is None
    assert out["home_penalty_shootout_score"] is None
    assert out["away_penalty_shootout_score"] is None
    assert out["home_score_after_penalty_shootout"] is None
    assert out["away_score_after_penalty_shootout"] is None

    assert out["went_to_extra_time"] is False
    assert out["went_to_penalties"] is False
    assert out["home_score_regulation"] == 2
    assert out["away_score_regulation"] == 1
    assert out["match_result"] == "home_win"
    # HT timestamp + full-time whistle DO populate (periods present).
    assert out["halftime_start_time"] is not None
    assert out["whistle_full_time_at"] is not None


def test_extra_time_only_match_populates_et_not_pen() -> None:
    """An ET-only (AET) match: ET scores set + went_to_extra_time True, NO penalty columns."""
    fx = _make_canonical_fixture(status="AET", home_goals=3, away_goals=2)
    af = _af_response(
        status_short="AET",
        fulltime={"home": 2, "away": 2},
        extratime={"home": 3, "away": 2},
        goals={"home": 3, "away": 2},
    )
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response=af)

    assert out["went_to_extra_time"] is True
    assert out["went_to_penalties"] is False
    assert out["home_score_regulation"] == 2
    assert out["away_score_regulation"] == 2
    assert out["home_score_after_extra_time"] == 3
    assert out["away_score_after_extra_time"] == 2
    # No shootout → penalty columns NULL.
    assert out["home_penalty_shootout_score"] is None
    assert out["away_penalty_shootout_score"] is None
    assert out["home_score_after_penalty_shootout"] is None
    assert out["match_result"] == "home_win_after_et"


def test_extra_time_plus_penalty_match_populates_full_distinction() -> None:
    """An ET+PEN match: full penalty distinction + match_result home_win_after_pens."""
    fx = _make_canonical_fixture(status="PEN", home_goals=4, away_goals=3)
    af = _af_response(
        status_short="PEN",
        fulltime={"home": 2, "away": 2},
        extratime={"home": 2, "away": 2},
        penalty={"home": 4, "away": 3},
        goals={"home": 4, "away": 3},
    )
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response=af)

    assert out["went_to_extra_time"] is True
    assert out["went_to_penalties"] is True
    assert out["home_score_regulation"] == 2
    assert out["home_score_after_extra_time"] == 2
    # Shootout tally alone is distinct from the post-shootout aggregate.
    assert out["home_penalty_shootout_score"] == 4
    assert out["away_penalty_shootout_score"] == 3
    assert out["home_score_after_penalty_shootout"] == 4
    assert out["away_score_after_penalty_shootout"] == 3
    assert out["match_result"] == "home_win_after_pens"


def test_away_win_after_pens() -> None:
    """Shootout winner is decided by the shootout tally, not the aggregate."""
    fx = _make_canonical_fixture(status="PEN")
    af = _af_response(
        status_short="PEN",
        fulltime={"home": 1, "away": 1},
        extratime={"home": 1, "away": 1},
        penalty={"home": 3, "away": 5},
        goals={"home": 1, "away": 1},
    )
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response=af)
    assert out["match_result"] == "away_win_after_pens"
    assert out["away_penalty_shootout_score"] == 5


def test_malformed_af_response_falls_back_to_defaults() -> None:
    """A malformed af_response (no fixture id) must NOT fail the shard — defaults instead."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17", af_response={"fixture": {}})
    # extract_match_lifecycle raises on missing fixture.id → overlay swallows → defaults.
    assert out["went_to_extra_time"] is False
    assert out["went_to_penalties"] is False
    assert out["match_result"] is None
    assert out["whistle_full_time_at"] is None


def test_lifecycle_rows_assemble_homogeneous_dataframe() -> None:
    """Mixed regulation + ET + PEN rows assemble into one homogeneous DataFrame."""
    fx = _make_canonical_fixture()
    rows = [
        _flatten_canonical_fixture_for_disk(fx, "2024-08-17"),
        _flatten_canonical_fixture_for_disk(
            fx, "2024-08-17", af_response=_af_response(status_short="FT", fulltime={"home": 1, "away": 0})
        ),
        _flatten_canonical_fixture_for_disk(
            fx,
            "2024-08-17",
            af_response=_af_response(
                status_short="PEN",
                fulltime={"home": 0, "away": 0},
                extratime={"home": 0, "away": 0},
                penalty={"home": 5, "away": 4},
                goals={"home": 0, "away": 0},
            ),
        ),
    ]
    df = pd.DataFrame(rows)
    assert len(df) == 3
    assert _ALL_LIFECYCLE_COLUMNS.issubset(set(df.columns))
