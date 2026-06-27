# SCHEMA_PROVENANCE_EXEMPT: CompletenessReport/FixtureDefect are internal report types, not cross-service schemas.
"""Fixture-completeness validator — depth_coverage Tier-B denominator for sports.

Reads the captured fixtures catalogue for a (league_id, season_year) and
validates it against the UAC season-structure registry (external truth).
Emits a per-(league, season) ``CompletenessReport`` with typed ``FixtureDefect``
instances.

Five checks (per plan ``sports_fixture_completeness_oracle_2026_06_24.md``):

  1. **MISSING_FIXTURES** — captured distinct fixtures < expected_fixtures (or
     captured > expected + promotion_relegation_extra for excess detection).
  2. **TEAM_COUNT_MISMATCH** — distinct teams ≠ n_teams, or any team has fewer
     home+away games than ``(n_teams - 1) * 2`` for DOUBLE_ROUND_ROBIN leagues.
  3. **UNEXPECTED_GAP** — a calendar gap in fixture dates that does not map to
     any ``expected_break`` window for the league.
  4. **SEASON_WINDOW_DRIFT** — first/last fixture dates are materially outside
     the league's expected season window.
  5. **RESCHEDULE_STALE_TIME** — a fixture's ``available_at`` diverges from
     ``kickoff_utc - 7 days`` by more than the staleness threshold, indicating
     the catalogue still holds the original (pre-reschedule) kickoff time.

The validator is a **pure function** — it takes the already-loaded rows and
looks up the registry; it never touches GCS itself.  The caller loads the
parquet and passes the rows in (list of dicts or any ``Sequence[Mapping]``).

Expected row keys (union of ``fixtures.parquet`` and ``fixtures_schedule.parquet``
schemas — any missing key is treated as ``None``):
  - ``af_fixture_id`` or ``fixture_id`` — unique fixture identifier
  - ``af_home_id`` or ``home_team_id`` — home team identifier
  - ``af_away_id`` or ``away_team_id`` — away team identifier
  - ``timestamp`` or ``kickoff_utc`` — scheduled kickoff (ISO str or datetime)
  - ``available_at`` — fixture-availability timestamp (optional; used for
    RESCHEDULE_STALE_TIME only)
  - ``status_short`` — fixture status string (optional; future use)

SSOT: ``instruments_service.sports.fixture_completeness``.
See ``unified_api_contracts.canonical.domain.sports.season_structure`` for the
season-structure registry (Phase 1 of the oracle plan).
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

from unified_api_contracts.sports import (
    FixtureFormat,
    SeasonBreak,
    SeasonStructure,
    get_season_end,
    get_season_start,
    get_season_structure,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# A gap in fixture dates (in days) must exceed this threshold to be considered
# a "gap" for the UNEXPECTED_GAP check.  Short 7-day windows (mid-week →
# next fixture) are expected even within a normal run of play.
_GAP_THRESHOLD_DAYS: int = 14

# SEASON_WINDOW_DRIFT: how many days outside the expected season window the
# first/last fixture is allowed to be before flagging.
_WINDOW_DRIFT_TOLERANCE_DAYS: int = 21

# RESCHEDULE_STALE_TIME: available_at is expected to be kickoff_utc - 7 days.
# Flag if |available_at - (kickoff_utc - 7 days)| > this tolerance.
_RESCHEDULE_STALE_THRESHOLD_DAYS: int = 2
_AVAILABILITY_LAG_DAYS: int = 7

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class FixtureDefectKind(StrEnum):
    """Typed defect codes emitted by the fixture-completeness validator."""

    MISSING_FIXTURES = "MISSING_FIXTURES"
    """Captured fixture count is below expected (or exceeds expected + playoff
    extras, suggesting wrong-league leakage)."""

    TEAM_COUNT_MISMATCH = "TEAM_COUNT_MISMATCH"
    """Distinct team count differs from n_teams, or a team's game count
    diverges from the expected per-team schedule for the league format."""

    UNEXPECTED_GAP = "UNEXPECTED_GAP"
    """A calendar gap in fixture dates does not correspond to any known
    expected break (international window, winter break, etc.)."""

    SEASON_WINDOW_DRIFT = "SEASON_WINDOW_DRIFT"
    """The first or last fixture date falls materially outside the expected
    season window for the league."""

    RESCHEDULE_STALE_TIME = "RESCHEDULE_STALE_TIME"
    """A fixture's available_at timestamp diverges from kickoff_utc - 7 days
    by more than the staleness threshold, suggesting the catalogue still holds
    the original pre-reschedule kickoff time."""


@dataclass(frozen=True)
class FixtureDefect:
    """A single defect found by the completeness validator.

    Attributes:
        kind: Typed defect code.
        description: Human-readable description of the defect.
        fixture_id: Affected fixture identifier, if applicable.
        detail: Additional structured detail (counts, dates, etc.).
    """

    kind: FixtureDefectKind
    description: str
    fixture_id: str | None = None
    detail: dict[str, object] = field(default_factory=dict)


@dataclass
class CompletenessReport:
    """Per-(league, season) fixture-completeness report.

    Attributes:
        league_id: Canonical league identifier (e.g. ``"EPL"``).
        season_year: Season start year.
        captured_fixture_count: Distinct fixtures found in the catalogue.
        expected_fixture_count: Total expected from the season-structure
            registry (``None`` if the league/season is not registered).
        distinct_team_count: Distinct teams observed in the captured data.
        expected_team_count: Expected from the registry (``None`` if unknown).
        defects: List of ``FixtureDefect`` instances found.
        is_complete: ``True`` iff no defects were found AND the registry has
            an entry for this league/season.
    """

    league_id: str
    season_year: int
    captured_fixture_count: int
    expected_fixture_count: int | None
    distinct_team_count: int
    expected_team_count: int | None
    defects: list[FixtureDefect] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return len(self.defects) == 0 and self.expected_fixture_count is not None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _row_val(row: Mapping[str, object], *keys: str) -> object:
    """Return the first non-None value for the given key candidates."""
    for k in keys:
        v = row.get(k)
        if v is not None:
            return v
    return None


def _parse_date(val: object) -> date | None:
    """Parse a fixture date from a timestamp string, date, or datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            # Try ISO datetime first, then ISO date
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(val[:19], fmt[: len(fmt)]).date()
                except ValueError:
                    pass
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
        except (ValueError, AttributeError):
            pass
    return None


def _parse_datetime(val: object) -> datetime | None:
    """Parse a datetime from various formats."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo is not None else val.replace(tzinfo=UTC)
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
        except (ValueError, AttributeError):
            pass
    return None


def _month_in_break(month: int, brk: SeasonBreak) -> bool:
    """Return True if ``month`` falls within the break window."""
    s, e = brk.month_start, brk.month_end
    if s <= e:
        return s <= month <= e
    # Cross-year break (e.g. Dec-Jan): month is in [s..12] or [1..e]
    return month >= s or month <= e


def _gap_is_explained(gap_start: date, gap_end: date, breaks: list[SeasonBreak]) -> bool:
    """Return True if the date gap is covered by at least one expected break."""
    # Check every month in the gap window
    current = gap_start
    while current <= gap_end:
        if any(_month_in_break(current.month, brk) for brk in breaks):
            return True
        # Advance by one month
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    return False


# ---------------------------------------------------------------------------
# The five checks
# ---------------------------------------------------------------------------


def _check_fixture_count(
    structure: SeasonStructure,
    distinct_fixture_ids: set[object],
) -> list[FixtureDefect]:
    defects: list[FixtureDefect] = []
    captured = len(distinct_fixture_ids)
    expected = structure.expected_fixtures
    promo_extra = structure.promotion_relegation_extra
    max_expected = expected + promo_extra

    if captured < expected:
        defects.append(
            FixtureDefect(
                kind=FixtureDefectKind.MISSING_FIXTURES,
                description=(
                    f"Captured {captured} distinct fixtures; expected {expected} (shortfall: {expected - captured})"
                ),
                detail={
                    "captured": captured,
                    "expected": expected,
                    "shortfall": expected - captured,
                    "promo_extra_allowed": promo_extra,
                },
            )
        )
    elif captured > max_expected:
        defects.append(
            FixtureDefect(
                kind=FixtureDefectKind.MISSING_FIXTURES,
                description=(
                    f"Captured {captured} distinct fixtures exceeds expected "
                    f"{expected} + promotion_relegation_extra {promo_extra} "
                    f"= {max_expected} — possible duplicates or wrong-league leakage"
                ),
                detail={
                    "captured": captured,
                    "expected": expected,
                    "max_expected": max_expected,
                    "excess": captured - max_expected,
                },
            )
        )
    return defects


def _check_team_count(
    structure: SeasonStructure,
    home_team_ids: list[object],
    away_team_ids: list[object],
) -> list[FixtureDefect]:
    defects: list[FixtureDefect] = []
    all_team_ids = set(home_team_ids) | set(away_team_ids)
    distinct_teams = len(all_team_ids)
    n_teams = structure.n_teams

    if distinct_teams != n_teams:
        defects.append(
            FixtureDefect(
                kind=FixtureDefectKind.TEAM_COUNT_MISMATCH,
                description=(f"Found {distinct_teams} distinct teams; expected {n_teams}"),
                detail={
                    "captured_teams": distinct_teams,
                    "expected_teams": n_teams,
                },
            )
        )
        return defects  # per-team game-count check requires correct team set

    # Per-team game count check for DOUBLE_ROUND_ROBIN leagues only (the
    # rule is unambiguous: each team plays exactly (n_teams - 1) home games
    # and (n_teams - 1) away games = 2 * (n_teams - 1) total).
    if structure.format == FixtureFormat.DOUBLE_ROUND_ROBIN:
        expected_games_per_team = 2 * (n_teams - 1)
        home_counts: Counter[object] = Counter(home_team_ids)
        away_counts: Counter[object] = Counter(away_team_ids)
        under_scheduled: list[dict[str, object]] = []
        for tid in all_team_ids:
            total = home_counts.get(tid, 0) + away_counts.get(tid, 0)
            if total < expected_games_per_team:
                under_scheduled.append(
                    {
                        "team_id": tid,
                        "captured_games": total,
                        "expected_games": expected_games_per_team,
                    }
                )
        if under_scheduled:
            defects.append(
                FixtureDefect(
                    kind=FixtureDefectKind.TEAM_COUNT_MISMATCH,
                    description=(
                        f"{len(under_scheduled)} team(s) have fewer than the expected "
                        f"{expected_games_per_team} games (n_teams={n_teams}, DRR)"
                    ),
                    detail={"under_scheduled": under_scheduled},
                )
            )

    return defects


def _check_unexpected_gaps(
    structure: SeasonStructure,
    fixture_dates: list[date],
) -> list[FixtureDefect]:
    if len(fixture_dates) < 2:
        return []

    defects: list[FixtureDefect] = []
    sorted_dates = sorted(set(fixture_dates))
    breaks = structure.expected_breaks

    for i in range(1, len(sorted_dates)):
        gap_days = (sorted_dates[i] - sorted_dates[i - 1]).days
        if gap_days <= _GAP_THRESHOLD_DAYS:
            continue
        gap_start = sorted_dates[i - 1] + timedelta(days=1)
        gap_end = sorted_dates[i] - timedelta(days=1)
        if not _gap_is_explained(gap_start, gap_end, breaks):
            defects.append(
                FixtureDefect(
                    kind=FixtureDefectKind.UNEXPECTED_GAP,
                    description=(
                        f"Unexplained gap of {gap_days} days between "
                        f"{sorted_dates[i - 1].isoformat()} and "
                        f"{sorted_dates[i].isoformat()}"
                    ),
                    detail={
                        "gap_start": sorted_dates[i - 1].isoformat(),
                        "gap_end": sorted_dates[i].isoformat(),
                        "gap_days": gap_days,
                    },
                )
            )

    return defects


def _check_season_window(
    league_id: str,
    season_year: int,
    fixture_dates: list[date],
) -> list[FixtureDefect]:
    if not fixture_dates:
        return []

    defects: list[FixtureDefect] = []
    expected_start = get_season_start(league_id, season_year)
    expected_end = get_season_end(league_id, season_year)
    first_date = min(fixture_dates)
    last_date = max(fixture_dates)

    # Late start: first fixture arrives materially after expected season start
    if first_date > expected_start + timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS):
        defects.append(
            FixtureDefect(
                kind=FixtureDefectKind.SEASON_WINDOW_DRIFT,
                description=(
                    f"First fixture {first_date.isoformat()} is "
                    f"{(first_date - expected_start).days} days after expected "
                    f"season start {expected_start.isoformat()}"
                ),
                detail={
                    "first_fixture_date": first_date.isoformat(),
                    "expected_start": expected_start.isoformat(),
                    "drift_days": (first_date - expected_start).days,
                },
            )
        )

    # Overrun end: last fixture falls materially after expected season end
    if last_date > expected_end + timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS):
        defects.append(
            FixtureDefect(
                kind=FixtureDefectKind.SEASON_WINDOW_DRIFT,
                description=(
                    f"Last fixture {last_date.isoformat()} is "
                    f"{(last_date - expected_end).days} days after expected "
                    f"season end {expected_end.isoformat()}"
                ),
                detail={
                    "last_fixture_date": last_date.isoformat(),
                    "expected_end": expected_end.isoformat(),
                    "drift_days": (last_date - expected_end).days,
                },
            )
        )

    return defects


def _check_reschedule_stale(
    rows: Sequence[Mapping[str, object]],
) -> list[FixtureDefect]:
    """Flag fixtures where available_at diverges from kickoff_utc - 7 days."""
    defects: list[FixtureDefect] = []
    threshold = timedelta(days=_RESCHEDULE_STALE_THRESHOLD_DAYS)

    for row in rows:
        available_at_raw = row.get("available_at")
        kickoff_raw = _row_val(row, "timestamp", "kickoff_utc")
        if available_at_raw is None or kickoff_raw is None:
            continue

        available_at = _parse_datetime(available_at_raw)
        kickoff = _parse_datetime(kickoff_raw)
        if available_at is None or kickoff is None:
            continue

        expected_available_at = kickoff - timedelta(days=_AVAILABILITY_LAG_DAYS)
        drift = abs(available_at - expected_available_at)
        if drift > threshold:
            fid = str(_row_val(row, "af_fixture_id", "fixture_id") or "")
            defects.append(
                FixtureDefect(
                    kind=FixtureDefectKind.RESCHEDULE_STALE_TIME,
                    description=(
                        f"Fixture {fid}: available_at {available_at.isoformat()} "
                        f"differs from expected {expected_available_at.isoformat()} "
                        f"by {drift.days}d {drift.seconds // 3600}h — "
                        f"possible stale pre-reschedule kickoff time"
                    ),
                    fixture_id=fid or None,
                    detail={
                        "fixture_id": fid,
                        "kickoff_utc": kickoff.isoformat(),
                        "available_at": available_at.isoformat(),
                        "expected_available_at": expected_available_at.isoformat(),
                        "drift_hours": drift.total_seconds() / 3600,
                    },
                )
            )

    return defects


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def validate_fixture_completeness(
    league_id: str,
    season_year: int,
    fixture_rows: Sequence[Mapping[str, object]],
) -> CompletenessReport:
    """Validate captured fixture rows against the season-structure registry.

    Args:
        league_id: Canonical league identifier (e.g. ``"EPL"``).
        season_year: The calendar year in which the season STARTS (e.g. 2024
            for the 2024-25 EPL season).
        fixture_rows: Captured fixture rows loaded from the catalogue parquet.
            Each row must be a ``Mapping`` with at minimum the keys described
            in the module docstring.

    Returns:
        A ``CompletenessReport`` with per-(league, season) counts and any
        ``FixtureDefect`` instances found.
    """
    structure: SeasonStructure | None = get_season_structure(league_id, season_year)

    # Collect per-row data in one pass
    distinct_fixture_ids: set[object] = set()
    home_team_ids: list[object] = []
    away_team_ids: list[object] = []
    fixture_dates: list[date] = []

    for row in fixture_rows:
        fid = _row_val(row, "af_fixture_id", "fixture_id")
        if fid is not None:
            distinct_fixture_ids.add(fid)

        htid = _row_val(row, "af_home_id", "home_team_id")
        atid = _row_val(row, "af_away_id", "away_team_id")
        if htid is not None:
            home_team_ids.append(htid)
        if atid is not None:
            away_team_ids.append(atid)

        kickoff_raw = _row_val(row, "timestamp", "kickoff_utc", "date")
        d = _parse_date(kickoff_raw)
        if d is not None:
            fixture_dates.append(d)

    captured = len(distinct_fixture_ids)
    distinct_teams = len(set(home_team_ids) | set(away_team_ids))

    defects: list[FixtureDefect] = []

    if structure is None:
        logger.warning(
            "No season structure registered for league=%s season=%d — "
            "skipping checks 1-4; only RESCHEDULE_STALE_TIME is checked.",
            league_id,
            season_year,
        )
    else:
        defects.extend(_check_fixture_count(structure, distinct_fixture_ids))
        defects.extend(_check_team_count(structure, home_team_ids, away_team_ids))
        defects.extend(_check_unexpected_gaps(structure, fixture_dates))
        defects.extend(_check_season_window(league_id, season_year, fixture_dates))

    defects.extend(_check_reschedule_stale(fixture_rows))

    return CompletenessReport(
        league_id=league_id,
        season_year=season_year,
        captured_fixture_count=captured,
        expected_fixture_count=structure.expected_fixtures if structure else None,
        distinct_team_count=distinct_teams,
        expected_team_count=structure.n_teams if structure else None,
        defects=defects,
    )


__all__ = [
    "CompletenessReport",
    "FixtureDefect",
    "FixtureDefectKind",
    "validate_fixture_completeness",
]
