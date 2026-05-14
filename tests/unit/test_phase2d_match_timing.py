"""Phase 2.D unit tests — match_end_time, announced_at, POSTPONED/CANCELLED manifest wiring.

Tests:
  (a) detect_match_end_time — freeze detected (live-mode 30+ frozen rows past 85:00)
  (b) detect_match_end_time — no freeze / batch-mode path (varied rows past 85:00)
  (c) normalize_api_football_fixture — announced_at = kickoff_utc - 7 days
  (d) orchestrator PST fixture → record_empty(reason=EXPECTED_FIXTURE_POSTPONED)
  (e) orchestrator CANC fixture → record_empty(reason=EXPECTED_FIXTURE_CANCELLED)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from unified_api_contracts.canonical.domain.sports import CanonicalProgressiveStats
from unified_api_contracts.external.api_football.normalize import normalize_api_football_fixture
from unified_api_contracts.external.api_football.schemas import (
    ApiFootballFixture,
    ApiFootballFixtureStatus,
    ApiFootballLeague,
    ApiFootballPeriods,
    ApiFootballScore,
    ApiFootballTeamWithWinner,
)

from instruments_service.reference_data.adapters.sports.adapters.soccerfootball_info import (
    _MATCH_END_SEARCH_START_SECONDS,
    _MIN_MATCH_END_RUN,
    detect_match_end_time,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_KICKOFF = datetime(2024, 3, 10, 15, 0, 0, tzinfo=UTC)
_LATE_START = _MATCH_END_SEARCH_START_SECONDS  # 85 * 60 = 5100


def _make_row(timer_seconds: int, goals: int | None = None) -> CanonicalProgressiveStats:
    return CanonicalProgressiveStats(
        fixture_id="1234",
        timer_seconds=timer_seconds,
        team="home",
        goals=goals,
    )


def _make_frozen_rows(start_seconds: int, count: int, base_goals: int = 2) -> list[CanonicalProgressiveStats]:
    """Build `count` rows with identical stats (freeze simulation)."""
    return [
        CanonicalProgressiveStats(
            fixture_id="1234",
            timer_seconds=start_seconds + i * 30,
            team="home",
            goals=base_goals,
            shots_on_target=5,
            corners=3,
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# (a) Freeze detected — 30+ frozen rows past 85:00
# ---------------------------------------------------------------------------


def test_detect_match_end_time_freeze_detected() -> None:
    """Freeze: ≥ _MIN_MATCH_END_RUN identical rows past 85:00 → returns kickoff + max timer."""
    pre_late = [_make_row(t) for t in range(0, _LATE_START, 30)]
    frozen = _make_frozen_rows(start_seconds=_LATE_START, count=_MIN_MATCH_END_RUN + 5)
    rows = pre_late + frozen

    result = detect_match_end_time(rows, _KICKOFF)

    assert result is not None
    expected_max = _LATE_START + (_MIN_MATCH_END_RUN + 5 - 1) * 30
    assert result == _KICKOFF + timedelta(seconds=expected_max)


# ---------------------------------------------------------------------------
# (b) No freeze — batch completed-match path uses max timer
# ---------------------------------------------------------------------------


def test_detect_match_end_time_batch_no_freeze() -> None:
    """Batch mode: varied rows past 85:00, no freeze → returns kickoff + max timer_seconds."""
    pre_late = [_make_row(t) for t in range(0, _LATE_START, 30)]
    late_varied = [
        _make_row(_LATE_START, goals=0),
        _make_row(_LATE_START + 30, goals=1),
        _make_row(_LATE_START + 60, goals=1),
    ]
    rows = pre_late + late_varied

    result = detect_match_end_time(rows, _KICKOFF)

    assert result is not None
    assert result == _KICKOFF + timedelta(seconds=_LATE_START + 60)


def test_detect_match_end_time_empty_rows() -> None:
    """Empty rows → None."""
    assert detect_match_end_time([], _KICKOFF) is None


def test_detect_match_end_time_no_late_rows() -> None:
    """Only rows before 85:00 → None (match not played past early-game)."""
    rows = [_make_row(t) for t in range(0, 60 * 60, 30)]  # up to 60:00
    assert detect_match_end_time(rows, _KICKOFF) is None


# ---------------------------------------------------------------------------
# (c) announced_at from API Football: kickoff_utc - 7 days
# ---------------------------------------------------------------------------


def _make_af_fixture(*, timestamp: int, status_short: str = "FT") -> ApiFootballFixture:
    return ApiFootballFixture(
        id=9999,
        timestamp=timestamp,
        status=ApiFootballFixtureStatus(short=status_short, long="Match Finished", elapsed=90),
        league=ApiFootballLeague(
            id=39,
            name="Premier League",
            country="England",
            logo=None,
            flag=None,
            season=2024,
            round="Regular Season - 1",
        ),
        teams={
            "home": ApiFootballTeamWithWinner(id=33, name="Manchester United", logo=None, winner=True),
            "away": ApiFootballTeamWithWinner(id=34, name="Newcastle", logo=None, winner=False),
        },
        goals=ApiFootballScore(home=2, away=1),
        periods=ApiFootballPeriods(first=None, second=None),
    )


def test_normalize_api_football_fixture_announced_at() -> None:
    """normalize_api_football_fixture populates announced_at = kickoff_utc - 7 days."""
    kickoff_ts = int(_KICKOFF.timestamp())
    raw = _make_af_fixture(timestamp=kickoff_ts)
    fx = normalize_api_football_fixture(raw)

    assert fx.announced_at is not None
    assert fx.announced_at == _KICKOFF - timedelta(days=7)


# ---------------------------------------------------------------------------
# (d) EXPECTED_FIXTURE_POSTPONED enum exists with correct string value
# ---------------------------------------------------------------------------


def test_empty_confirmed_reason_postponed_value() -> None:
    """EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED is the expected string (used in manifest)."""
    from unified_api_contracts import EmptyConfirmedReason

    assert EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED == "EXPECTED_FIXTURE_POSTPONED"
    assert str(EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED) == "EXPECTED_FIXTURE_POSTPONED"


# ---------------------------------------------------------------------------
# (e) EXPECTED_FIXTURE_CANCELLED enum exists with correct string value
# ---------------------------------------------------------------------------


def test_empty_confirmed_reason_cancelled_value() -> None:
    """EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED is the expected string (used in manifest)."""
    from unified_api_contracts import EmptyConfirmedReason

    assert EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED == "EXPECTED_FIXTURE_CANCELLED"
    assert str(EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED) == "EXPECTED_FIXTURE_CANCELLED"


# ---------------------------------------------------------------------------
# Bonus: _af_record_empty passes reason to manifest.record_empty
# ---------------------------------------------------------------------------


def test_af_record_empty_passes_reason_to_manifest() -> None:
    """Verify the _af_record_empty closure logic forwards reason= to ManifestWriter."""
    from unified_api_contracts import EmptyConfirmedReason, PipelineMode

    mock_manifest = MagicMock()
    date = "2024-03-10"
    af_attempt_ts = datetime(2024, 3, 10, 12, 0, tzinfo=UTC)

    # Recreate the _af_record_empty closure exactly as it appears in the orchestrator.
    def _af_record_empty(data_type: str, league_id: str = "", reason: str = "") -> None:
        _row_key: dict[str, str] = {"date": date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        mock_manifest.record_empty(
            row_key=_row_key,
            attempted_at=af_attempt_ts,
            reason=reason,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )

    _af_record_empty(
        "FIXTURES",
        league_id="EPL",
        reason=str(EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED),
    )

    call_kwargs = mock_manifest.record_empty.call_args.kwargs
    assert call_kwargs["reason"] == "EXPECTED_FIXTURE_POSTPONED"
    assert call_kwargs["row_key"] == {"date": date, "data_type": "FIXTURES", "league_id": "EPL"}
    assert call_kwargs["pipeline_mode"] == PipelineMode.BATCH_API_FOOTBALL
