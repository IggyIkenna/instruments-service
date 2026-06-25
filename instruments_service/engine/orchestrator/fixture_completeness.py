"""Fixture-completeness oracle (oracle-002) — Tier-B denominator for sports depth_coverage.

Per (league_id, season_year), reads captured fixtures_schedule parquets from GCS
and validates against SEASON_STRUCTURE_REGISTRY (external truth). Emits typed defects.

5 checks per (league, season):
  1. FIXTURE_COUNT   — captured distinct af_fixture_ids == expected_fixtures
                       (± promotion_relegation_extra)
  2. TEAM_COUNT      — distinct team IDs == n_teams; each plays (n_teams-1)*2
                       home+away games (catches a team with missing fixtures
                       even when the total count is right)
  3. Promo/relegate  — excess fixtures up to promotion_relegation_extra are OK,
                       not false-flagged as MISSING_FIXTURES excess
  4. SEASON_WINDOW   — first/last fixture dates within ±30-day tolerance of the
                       expected season window; unexplained calendar gaps (not in
                       expected_breaks) are flagged as UNEXPECTED_GAP
  5. RESCHEDULE      — PST fixtures with stale timestamp (past kickoff date, never
                       rescheduled to a new time) flagged as RESCHEDULE_STALE_TIME

GCS fixture_schedule parquets live at:
  sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/
    entity=fixtures_schedule/league={league_id}/fixtures_schedule.parquet

SSOT: unified-trading-pm/plans/active/sports_fixture_completeness_oracle_2026_06_24.md
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from datetime import UTC, date as date_type, datetime, timedelta
from enum import StrEnum

import pandas as pd

from unified_api_contracts.canonical.domain.sports.season_structure import (
    SeasonBreak,
    SeasonStructure,
    get_all_league_ids,
    get_season_structure,
)
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

__all__ = [
    "FixtureCompletenessReport",
    "FixtureDefect",
    "FixtureDefectCode",
    "scan_all_leagues",
    "validate_fixture_completeness",
]

# GCS path templates for fixtures_schedule parquets.
# Canonical (post-pipeline_mode migration) and legacy (pre-migration) shapes.
_CANONICAL_PATH_TPL = (
    "sports_reference/by_date/day={date}/pipeline_mode=batch_api_football"
    "/entity=fixtures_schedule/league={league_id}/fixtures_schedule.parquet"
)
_LEGACY_PATH_TPL = (
    "sports_reference/by_date/day={date}"
    "/entity=fixtures_schedule/league={league_id}/fixtures_schedule.parquet"
)

# Season window tolerance for SEASON_WINDOW_DRIFT check (days).
_WINDOW_DRIFT_TOLERANCE_DAYS = 30

# Minimum gap length (days) before flagging as UNEXPECTED_GAP.
_MIN_GAP_DAYS = 10

# Stale-timestamp threshold: PST fixture whose timestamp is this many days
# in the past is considered never-rescheduled (RESCHEDULE_STALE_TIME).
_STALE_PST_THRESHOLD_DAYS = 30


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class FixtureDefectCode(StrEnum):
    """Typed defect codes emitted by the fixture-completeness validator."""

    MISSING_FIXTURES = "MISSING_FIXTURES"
    TEAM_COUNT_MISMATCH = "TEAM_COUNT_MISMATCH"
    UNEXPECTED_GAP = "UNEXPECTED_GAP"
    SEASON_WINDOW_DRIFT = "SEASON_WINDOW_DRIFT"
    RESCHEDULE_STALE_TIME = "RESCHEDULE_STALE_TIME"


@dataclass(frozen=True)
class FixtureDefect:
    """A single typed defect from the fixture-completeness oracle."""

    code: FixtureDefectCode
    detail: str
    league_id: str
    season_year: int


@dataclass
class FixtureCompletenessReport:
    """Per-(league_id, season_year) completeness report."""

    league_id: str
    season_year: int
    expected_fixtures: int
    captured_fixtures: int
    promotion_relegation_extra: int
    defects: list[FixtureDefect] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return len(self.defects) == 0

    @property
    def completeness_pct(self) -> float:
        if self.expected_fixtures == 0:
            return 100.0
        return min(100.0, self.captured_fixtures * 100.0 / self.expected_fixtures)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_cross_year_league(structure: SeasonStructure) -> bool:
    """True if the season spans two calendar years (European-style, Aug-Jun).

    Detected by the presence of a winter break (months 11-12 start or 1-3 end)
    in the expected_breaks.  Calendar-year leagues (MLS, Liga MX, Nordic) have
    a summer break instead.
    """
    for b in structure.expected_breaks:
        if b.month_start in (11, 12) or b.month_end in (1, 2, 3):
            return True
    return False


def _season_date_range(structure: SeasonStructure) -> tuple[date_type, date_type]:
    """Derive an inclusive (start, end) date range to scan for fixtures.

    European cross-year leagues: August 1 of season_year → July 31 of season_year+1.
    Calendar-year leagues (Americas, MLS, Nordic): February 1 → December 31.
    """
    y = structure.season_year
    if _is_cross_year_league(structure):
        return date_type(y, 8, 1), date_type(y + 1, 7, 31)
    return date_type(y, 2, 1), date_type(y, 12, 31)


def _expected_season_bounds(structure: SeasonStructure) -> tuple[date_type, date_type]:
    """Return the expected (start_bound, end_bound) for the season window drift check.

    These are the outer bounds used in CHECK 4.  A first fixture date before
    (start_bound - tolerance) or a last fixture date after (end_bound + tolerance)
    triggers SEASON_WINDOW_DRIFT.
    """
    return _season_date_range(structure)


def _date_in_break(d: date_type, season_year: int, brk: SeasonBreak) -> bool:
    """True if ``d`` falls within the month range of ``brk``.

    Handles cross-year breaks (e.g. winter_break Dec→Jan where month_end < month_start).
    """
    m = d.month
    if brk.month_end >= brk.month_start:
        return brk.month_start <= m <= brk.month_end
    # Cross-year break: e.g. month_start=12, month_end=1 → Dec or Jan
    return m >= brk.month_start or m <= brk.month_end


def _gap_in_expected_breaks(
    gap_start: date_type,
    gap_end: date_type,
    structure: SeasonStructure,
) -> bool:
    """True if every date in [gap_start, gap_end] falls within some expected_break."""
    d = gap_start
    while d <= gap_end:
        if not any(_date_in_break(d, structure.season_year, b) for b in structure.expected_breaks):
            return False
        d += timedelta(days=1)
    return True


def _read_fixtures_for_date_league(
    storage_client: object,
    bucket: str,
    date_str: str,
    league_id: str,
) -> pd.DataFrame | None:
    """Read fixtures_schedule parquet for (date, league) from GCS.

    Probes the canonical path (pipeline_mode= in prefix) first, then falls back
    to the legacy path for pre-migration objects.  Returns None on miss.
    """
    canonical = _CANONICAL_PATH_TPL.format(date=date_str, league_id=league_id)
    legacy = _LEGACY_PATH_TPL.format(date=date_str, league_id=league_id)
    for path in (canonical, legacy):
        try:
            # storage_client is the unified cloud-interface client; it exposes
            # a GCS-compatible bucket().blob().exists() + download_bytes API.
            blob = storage_client.bucket(bucket).blob(path)  # type: ignore[union-attr]
            if blob.exists():
                raw = storage_client.download_bytes(  # type: ignore[union-attr]
                    bucket=bucket, blob_path=path
                )
                return pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.debug(
                "fixture_completeness: read failed gs://%s/%s: %s", bucket, path, exc
            )
    return None


def _collect_season_fixtures(
    bucket: str,
    league_id: str,
    structure: SeasonStructure,
) -> pd.DataFrame:
    """Read and aggregate all fixtures_schedule parquets for (league, season).

    Iterates dates in the season scan window, reads each per-date per-league
    parquet, and concatenates into a single DataFrame.  Dates with no parquet
    (no data captured) are skipped silently.
    """
    storage = get_storage_client()
    start, end = _season_date_range(structure)
    frames: list[pd.DataFrame] = []
    d = start
    while d <= end:
        date_str = d.isoformat()
        df = _read_fixtures_for_date_league(storage, bucket, date_str, league_id)
        if df is not None and not df.empty:
            frames.append(df)
        d += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    # Deduplicate by af_fixture_id — the same fixture may appear on multiple
    # calendar dates (rescheduled, or written on both the original and new date).
    if "af_fixture_id" in combined.columns:
        combined = combined.drop_duplicates(subset=["af_fixture_id"], keep="last")
    return combined


# ---------------------------------------------------------------------------
# 5-check validators
# ---------------------------------------------------------------------------


def _check_fixture_count(
    df: pd.DataFrame,
    structure: SeasonStructure,
    league_id: str,
) -> list[FixtureDefect]:
    """CHECK 1 + 3: fixture count vs expected, with promotion/relegation tolerance."""
    if "af_fixture_id" not in df.columns:
        return [
            FixtureDefect(
                FixtureDefectCode.MISSING_FIXTURES,
                "fixtures_schedule parquet missing af_fixture_id column — "
                "cannot validate fixture count",
                league_id,
                structure.season_year,
            )
        ]
    captured = df["af_fixture_id"].nunique()
    expected = structure.expected_fixtures
    promo_extra = structure.promotion_relegation_extra
    defects: list[FixtureDefect] = []
    if captured < expected:
        shortfall = expected - captured
        defects.append(
            FixtureDefect(
                FixtureDefectCode.MISSING_FIXTURES,
                f"captured {captured} distinct fixtures, expected {expected} "
                f"(shortfall={shortfall}; promo_extra={promo_extra})",
                league_id,
                structure.season_year,
            )
        )
    elif captured > expected + promo_extra:
        excess = captured - expected - promo_extra
        defects.append(
            FixtureDefect(
                FixtureDefectCode.MISSING_FIXTURES,
                f"captured {captured} distinct fixtures exceeds expected "
                f"{expected} + promo_extra {promo_extra} by {excess} "
                "(possible duplicate fixtures or wrong-league leakage)",
                league_id,
                structure.season_year,
            )
        )
    return defects


def _check_team_count(
    df: pd.DataFrame,
    structure: SeasonStructure,
    league_id: str,
) -> list[FixtureDefect]:
    """CHECK 2: distinct team count and per-team game count."""
    home_col = "af_home_id" if "af_home_id" in df.columns else None
    away_col = "af_away_id" if "af_away_id" in df.columns else None
    if home_col is None or away_col is None:
        return [
            FixtureDefect(
                FixtureDefectCode.TEAM_COUNT_MISMATCH,
                "fixtures_schedule parquet missing af_home_id/af_away_id — "
                "cannot validate team count",
                league_id,
                structure.season_year,
            )
        ]
    home_ids = pd.to_numeric(df[home_col], errors="coerce").dropna().astype(int)
    away_ids = pd.to_numeric(df[away_col], errors="coerce").dropna().astype(int)
    all_teams = set(home_ids.tolist()) | set(away_ids.tolist())
    captured_n = len(all_teams)
    expected_n = structure.n_teams
    defects: list[FixtureDefect] = []
    if captured_n != expected_n:
        defects.append(
            FixtureDefect(
                FixtureDefectCode.TEAM_COUNT_MISMATCH,
                f"captured {captured_n} distinct teams, expected {expected_n}",
                league_id,
                structure.season_year,
            )
        )
        return defects
    # Each team should appear in exactly (n_teams - 1) * 2 fixtures.
    expected_games_per_team = (expected_n - 1) * 2
    team_home_counts = home_ids.value_counts()
    team_away_counts = away_ids.value_counts()
    anomalous: list[str] = []
    for team_id in all_teams:
        home_c = int(team_home_counts.get(team_id, 0))
        away_c = int(team_away_counts.get(team_id, 0))
        total = home_c + away_c
        if total != expected_games_per_team:
            anomalous.append(
                f"team_id={team_id} has {total} fixtures "
                f"(home={home_c}, away={away_c}), expected {expected_games_per_team}"
            )
    if anomalous:
        sample = anomalous[:3]
        suffix = f"... and {len(anomalous) - 3} more" if len(anomalous) > 3 else ""
        defects.append(
            FixtureDefect(
                FixtureDefectCode.TEAM_COUNT_MISMATCH,
                "per-team game count anomaly: "
                + "; ".join(sample)
                + suffix,
                league_id,
                structure.season_year,
            )
        )
    return defects


def _check_season_window_and_gaps(
    df: pd.DataFrame,
    structure: SeasonStructure,
    league_id: str,
) -> list[FixtureDefect]:
    """CHECK 4: season window drift and unexplained calendar gaps."""
    if "date" not in df.columns or df.empty:
        return []
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    if dates.empty:
        return []
    first_dt = dates.min().date()
    last_dt = dates.max().date()
    expected_start, expected_end = _expected_season_bounds(structure)
    defects: list[FixtureDefect] = []
    # Window drift check.
    early_bound = expected_start - timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS)
    late_start_bound = expected_start + timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS)
    early_end_bound = expected_end - timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS)
    late_bound = expected_end + timedelta(days=_WINDOW_DRIFT_TOLERANCE_DAYS)
    if first_dt < early_bound or first_dt > late_start_bound:
        defects.append(
            FixtureDefect(
                FixtureDefectCode.SEASON_WINDOW_DRIFT,
                f"first fixture date {first_dt} is outside expected window start "
                f"{expected_start} ± {_WINDOW_DRIFT_TOLERANCE_DAYS}d",
                league_id,
                structure.season_year,
            )
        )
    if last_dt < early_end_bound or last_dt > late_bound:
        defects.append(
            FixtureDefect(
                FixtureDefectCode.SEASON_WINDOW_DRIFT,
                f"last fixture date {last_dt} is outside expected window end "
                f"{expected_end} ± {_WINDOW_DRIFT_TOLERANCE_DAYS}d",
                league_id,
                structure.season_year,
            )
        )
    # Gap check: find gaps > _MIN_GAP_DAYS that don't map to any expected_break.
    if not structure.expected_breaks:
        return defects
    sorted_dates = sorted(dates.dt.date.unique())
    for i in range(len(sorted_dates) - 1):
        gap_days = (sorted_dates[i + 1] - sorted_dates[i]).days
        if gap_days > _MIN_GAP_DAYS:
            gap_start = sorted_dates[i] + timedelta(days=1)
            gap_end = sorted_dates[i + 1] - timedelta(days=1)
            if not _gap_in_expected_breaks(gap_start, gap_end, structure):
                defects.append(
                    FixtureDefect(
                        FixtureDefectCode.UNEXPECTED_GAP,
                        f"unexplained gap {gap_days}d from {sorted_dates[i]} to "
                        f"{sorted_dates[i + 1]} (gap window {gap_start}–{gap_end} "
                        "not covered by any expected_break)",
                        league_id,
                        structure.season_year,
                    )
                )
    return defects


def _check_reschedule(
    df: pd.DataFrame,
    structure: SeasonStructure,
    league_id: str,
) -> list[FixtureDefect]:
    """CHECK 5: PST fixtures with stale timestamp (past kickoff, never rescheduled).

    A postponed fixture (status_short=PST) whose timestamp is more than
    STALE_PST_THRESHOLD_DAYS in the past has almost certainly been rescheduled
    to a new date in the real world, but our catalogue still holds the original
    (stale) kickoff time.  Flag these as RESCHEDULE_STALE_TIME.
    """
    if "status_short" not in df.columns or "timestamp" not in df.columns:
        return []
    pst_mask = df["status_short"] == "PST"
    pst_df = df[pst_mask].copy()
    if pst_df.empty:
        return []
    pst_df["_ts_utc"] = pd.to_datetime(pst_df["timestamp"], utc=True, errors="coerce")
    now_utc = datetime.now(UTC)
    stale_threshold = now_utc - timedelta(days=_STALE_PST_THRESHOLD_DAYS)
    stale_mask = pst_df["_ts_utc"] < stale_threshold
    stale_count = int(stale_mask.sum())
    if stale_count == 0:
        return []
    # Sample a few fixture IDs for the defect detail.
    fid_col = "af_fixture_id" if "af_fixture_id" in pst_df.columns else None
    if fid_col:
        sample_fids = pst_df.loc[stale_mask, fid_col].dropna().astype(int).tolist()[:5]
        sample_str = str(sample_fids[:3])
        if len(sample_fids) > 3:
            sample_str += "..."
    else:
        sample_str = "(no fixture IDs)"
    return [
        FixtureDefect(
            FixtureDefectCode.RESCHEDULE_STALE_TIME,
            f"{stale_count} PST fixture(s) have timestamp >{_STALE_PST_THRESHOLD_DAYS}d "
            f"in the past (stale kickoff time, likely rescheduled without catalogue "
            f"update). Sample fixture IDs: {sample_str}",
            league_id,
            structure.season_year,
        )
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_fixture_completeness(
    league_id: str,
    season_year: int,
    bucket: str,
) -> FixtureCompletenessReport | None:
    """Validate fixture completeness for one (league_id, season_year) pair.

    Returns ``None`` if the league/season is not in the registry (unknown).
    Returns a ``FixtureCompletenessReport`` with zero defects when complete.
    """
    canonical_lid = league_id.upper()
    structure = get_season_structure(canonical_lid, season_year)
    if structure is None:
        logger.warning(
            "fixture_completeness: no season structure for league=%s season=%d — skipping",
            canonical_lid,
            season_year,
        )
        return None

    df = _collect_season_fixtures(bucket, canonical_lid, structure)
    captured_count = df["af_fixture_id"].nunique() if "af_fixture_id" in df.columns else 0

    defects: list[FixtureDefect] = []
    if df.empty:
        defects.append(
            FixtureDefect(
                FixtureDefectCode.MISSING_FIXTURES,
                f"no fixture data found in GCS for league={canonical_lid} "
                f"season={season_year} (expected {structure.expected_fixtures} fixtures)",
                canonical_lid,
                season_year,
            )
        )
    else:
        defects += _check_fixture_count(df, structure, canonical_lid)
        defects += _check_team_count(df, structure, canonical_lid)
        defects += _check_season_window_and_gaps(df, structure, canonical_lid)
        defects += _check_reschedule(df, structure, canonical_lid)

    report = FixtureCompletenessReport(
        league_id=canonical_lid,
        season_year=season_year,
        expected_fixtures=structure.expected_fixtures,
        captured_fixtures=captured_count,
        promotion_relegation_extra=structure.promotion_relegation_extra,
        defects=defects,
    )
    logger.info(
        "fixture_completeness: league=%s season=%d "
        "captured=%d expected=%d defects=%d complete=%s",
        canonical_lid,
        season_year,
        captured_count,
        structure.expected_fixtures,
        len(defects),
        report.is_complete,
    )
    return report


def scan_all_leagues(
    bucket: str,
    season_year: int,
) -> list[FixtureCompletenessReport]:
    """Run the fixture-completeness validator over all registered league IDs.

    Skips leagues with no registered structure for ``season_year``.  Returns
    one ``FixtureCompletenessReport`` per (league, season) with at least one
    registered season structure.
    """
    reports: list[FixtureCompletenessReport] = []
    for lid in get_all_league_ids():
        report = validate_fixture_completeness(lid, season_year, bucket)
        if report is not None:
            reports.append(report)
    return reports
