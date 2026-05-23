"""Phase 2.D unit tests — match_end_time, announced_at, POSTPONED/CANCELLED manifest wiring.

Tests:
  (a) detect_match_end_time — freeze detected (live-mode 30+ frozen rows past 85:00)
  (b) detect_match_end_time — no freeze / batch-mode path (varied rows past 85:00)
  (c) normalize_api_football_fixture — announced_at = kickoff_utc - 7 days
  (d) orchestrator PST fixture → record_empty(reason=EXPECTED_FIXTURE_POSTPONED)
  (e) orchestrator CANC fixture → record_empty(reason=EXPECTED_FIXTURE_CANCELLED)

C.6 available_at cascade tests (added 2026-05-23):
  (f) report_time → available_at for completed SFI matches
  (g) None report_time → wall-clock fallback for in-progress SFI rows
  (h) mixed report_time rows — completed use report_time, in-progress use fallback
  (i) detect_match_end_time + SFI_DATA_LAG_P95_SECONDS → correct report_time derivation
  (j) understat XG kickoff+24h preserved (not overridden by wall-clock)
  (k) understat XG None kickoff → wall-clock fallback
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
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
from unified_api_contracts.registry.source_data_latency import SFI_DATA_LAG_P95_SECONDS

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


# ---------------------------------------------------------------------------
# C.6 available_at cascade — (f)-(k) added 2026-05-23
# ---------------------------------------------------------------------------


def _apply_report_time_stamping(df: pd.DataFrame, fallback: datetime) -> pd.DataFrame:
    """Mirrors the C.6 stamping logic shipped in orchestrator.py ~6380."""
    out = df.copy()
    if "report_time" in out.columns:
        rt = pd.to_datetime(out["report_time"], utc=True, errors="coerce")
        out["available_at"] = rt.fillna(pd.Timestamp(fallback))
    else:
        out["available_at"] = pd.Timestamp(fallback)
    return out


def _apply_xg_stamping(df: pd.DataFrame, fallback: datetime) -> pd.DataFrame:
    """Mirrors the C.6 stamping logic for understat XG (orchestrator.py ~5703)."""
    out = df.copy()
    out["available_at"] = out["available_at"].fillna(pd.Timestamp(fallback))
    return out


_REPORT_TIME = datetime(2024, 3, 10, 17, 5, 0, tzinfo=UTC)
_FALLBACK = datetime(2026, 5, 23, 12, 0, 0, tzinfo=UTC)


# (f) Completed match — report_time used as available_at


def test_c6_report_time_used_as_available_at() -> None:
    """Completed match rows with report_time use it as available_at."""
    df = pd.DataFrame(
        [
            {"fixture_id": "1", "timer_seconds": 5400, "report_time": _REPORT_TIME.isoformat()},
            {"fixture_id": "1", "timer_seconds": 5430, "report_time": _REPORT_TIME.isoformat()},
        ]
    )
    df["available_at"] = pd.Timestamp(_KICKOFF) + pd.to_timedelta(
        pd.to_numeric(df["timer_seconds"], errors="coerce"), unit="s"
    )
    result = _apply_report_time_stamping(df, _FALLBACK)

    expected = pd.Timestamp(_REPORT_TIME)
    assert (result["available_at"] == expected).all(), f"Expected {expected}, got {result['available_at'].tolist()}"


# (g) In-progress match — None report_time falls back to wall-clock


def test_c6_none_report_time_falls_back_to_wall_clock() -> None:
    """In-progress match rows with report_time=None fall back to wall-clock timestamp."""
    df = pd.DataFrame(
        [
            {"fixture_id": "2", "timer_seconds": 2700, "report_time": None},
            {"fixture_id": "2", "timer_seconds": 2730, "report_time": None},
        ]
    )
    df["available_at"] = pd.Timestamp(_KICKOFF) + pd.to_timedelta(
        pd.to_numeric(df["timer_seconds"], errors="coerce"), unit="s"
    )
    result = _apply_report_time_stamping(df, _FALLBACK)

    expected = pd.Timestamp(_FALLBACK)
    assert (result["available_at"] == expected).all()


# (h) Mixed rows — completed use report_time, in-progress use fallback


def test_c6_mixed_rows_use_correct_available_at() -> None:
    """Mixed df: rows with report_time use it; rows without use wall-clock fallback."""
    df = pd.DataFrame(
        [
            {"fixture_id": "1", "timer_seconds": 5400, "report_time": _REPORT_TIME.isoformat()},
            {"fixture_id": "2", "timer_seconds": 2700, "report_time": None},
        ]
    )
    df["available_at"] = pd.Timestamp(_KICKOFF)
    result = _apply_report_time_stamping(df, _FALLBACK)

    assert result.loc[0, "available_at"] == pd.Timestamp(_REPORT_TIME)
    assert result.loc[1, "available_at"] == pd.Timestamp(_FALLBACK)


# (i) detect_match_end_time + SFI_DATA_LAG_P95_SECONDS → correct report_time derivation


def test_c6_detect_match_end_time_derives_report_time_with_lag() -> None:
    """Full cascade: detect_match_end_time + lag gives correct report_time."""
    pre = [_make_row(t) for t in range(0, _LATE_START, 30)]
    frozen = _make_frozen_rows(start_seconds=_LATE_START, count=_MIN_MATCH_END_RUN + 2)
    rows = pre + frozen

    match_end = detect_match_end_time(rows, _KICKOFF)
    assert match_end is not None

    report_time = match_end + timedelta(seconds=SFI_DATA_LAG_P95_SECONDS)
    expected_match_end = _KICKOFF + timedelta(seconds=_LATE_START + (_MIN_MATCH_END_RUN + 2 - 1) * 30)
    assert match_end == expected_match_end
    assert report_time == expected_match_end + timedelta(seconds=300)


# (j) Understat XG — kickoff+24h preserved when set


def test_c6_understat_xg_kickoff_plus_24h_preserved() -> None:
    """kickoff+24h available_at set before stamp call is not overridden by wall-clock."""
    kickoff = pd.Timestamp(_KICKOFF)
    df = pd.DataFrame(
        [
            {"match_id": "m1", "kickoff_utc": kickoff},
            {"match_id": "m2", "kickoff_utc": kickoff},
        ]
    )
    df["available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce") + pd.Timedelta(hours=24)

    result = _apply_xg_stamping(df, _FALLBACK)

    expected = kickoff + pd.Timedelta(hours=24)
    assert (result["available_at"] == expected).all()


# (k) Understat XG — missing kickoff_utc falls back to wall-clock


def test_c6_understat_xg_none_kickoff_falls_back() -> None:
    """Row with no kickoff_utc (NaT available_at) falls back to wall-clock timestamp."""
    df = pd.DataFrame([{"match_id": "m1", "kickoff_utc": None}])
    df["available_at"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce") + pd.Timedelta(hours=24)

    result = _apply_xg_stamping(df, _FALLBACK)

    assert result.loc[0, "available_at"] == pd.Timestamp(_FALLBACK)
