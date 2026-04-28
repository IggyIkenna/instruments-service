"""Map legacy sports fixtures parquet → new flat schema (Phase 1 of migration plan).

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``.

Twin of ``instruments_service.engine.orchestrator._flatten_canonical_fixture_for_disk``.
That helper flattens **CanonicalFixture objects** at write time. THIS helper flattens
**already-written legacy parquets** (DataFrame with nested ``league`` /
``home_team`` / ``away_team`` / ``venue`` struct cells) for the one-shot
historical rewrite.

The audit (2026-04-28) classified 594 of 3,627 day partitions as LEGACY. Phase 3
of the plan reads each LEGACY parquet, calls :func:`map_legacy_to_new`, and
writes side-by-side to ``sports_reference_v2/`` for atomic rename.

Output split: legacy fixtures parquets bundle match-stats columns inline
(``home_xg``, ``home_corners``, ``home_possession``, …). The new layout splits
those out into ``entity=fixture_stats/fixture_stats.parquet``. So
:func:`map_legacy_to_new` returns BOTH DataFrames — fixtures (32 cols) +
fixture_stats (~18 cols) — that the migration job writes to the two distinct
parquet paths.
"""

from __future__ import annotations

import re

import pandas as pd

# Regex: API-Football logo URLs end ``/leagues/{N}.png`` or ``/teams/{N}.png``.
# Legacy fixtures parquets carry ``league``, ``home_team``, ``away_team`` as
# nested struct cells; the api_football_id only survives via this URL.
_AF_LOGO_RE = re.compile(r"/(?:leagues|teams)/(\d+)\.png")


_FIXTURES_COLUMNS: tuple[str, ...] = (
    "af_fixture_id",
    "referee_name",
    "date",
    "timestamp",
    "periods_first",
    "periods_second",
    "venue_id",
    "venue_name",
    "venue_city",
    "status_long",
    "status_short",
    "status_elapsed_time",
    "af_league_id",
    "season",
    "round",
    "af_home_id",
    "af_away_id",
    "af_winner_id",
    "af_home_name",
    "af_away_name",
    "home_score",
    "away_score",
    "home_score_halftime",
    "away_score_halftime",
    "home_score_fulltime",
    "away_score_fulltime",
    "home_score_extratime",
    "away_score_extratime",
    "home_score_penalty",
    "away_score_penalty",
    "day",
    "data_available_at",
)


_FIXTURE_STATS_COLUMNS: tuple[str, ...] = (
    "af_fixture_id",
    "home_xg",
    "away_xg",
    "home_total_shots",
    "away_total_shots",
    "home_shots_on_target",
    "away_shots_on_target",
    "home_shots_blocked",
    "away_shots_blocked",
    "home_corners",
    "away_corners",
    "home_fouls",
    "away_fouls",
    "home_offsides",
    "away_offsides",
    "home_possession",
    "away_possession",
    "home_passes_total",
    "away_passes_total",
    "home_passes_accuracy",
    "away_passes_accuracy",
    "home_yellow_cards",
    "away_yellow_cards",
    "home_red_cards",
    "away_red_cards",
    "match_week",
    "data_available_at",
)


def is_legacy_schema(df: pd.DataFrame) -> bool:
    """Return True if df is a legacy fixtures parquet (nested struct schema).

    Heuristic: ``af_league_id`` column missing AND ``league`` struct column present.
    Both conditions matter — empty/malformed parquets that have neither return False.
    """
    if df.empty:
        return False
    return "af_league_id" not in df.columns and "league" in df.columns


def _parse_af_id_from_struct(cell: object) -> int | None:
    """Extract API-Football numeric ID from a legacy struct cell.

    Legacy ``league`` / ``home_team`` / ``away_team`` / ``venue`` cells are
    dicts with ``logo_url`` ending in ``/leagues/{N}.png`` or ``/teams/{N}.png``.
    Tries logo URL first, falls back to a direct ``api_football_id`` field
    if present (rare in practice).
    """
    if not isinstance(cell, dict):
        return None
    af = cell.get("api_football_id")
    if isinstance(af, int):
        return af
    if af is not None:
        try:
            return int(af)
        except (TypeError, ValueError):
            pass
    url = cell.get("logo_url")
    if isinstance(url, str):
        match = _AF_LOGO_RE.search(url)
        if match is not None:
            return int(match.group(1))
    return None


def _struct_field(cell: object, key: str) -> object | None:
    """Return ``cell[key]`` if ``cell`` is a dict, else None."""
    if isinstance(cell, dict):
        return cell.get(key)
    return None


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _derive_winner_id(
    home_score: object, away_score: object, af_home_id: int | None, af_away_id: int | None
) -> int | None:
    h = _coerce_int(home_score)
    a = _coerce_int(away_score)
    if h is None or a is None or h == a:
        return None
    return af_home_id if h > a else af_away_id


def _legacy_fixtures_row(row: pd.Series, day: str) -> dict[str, object]:
    """Map one legacy fixtures row to the new flat fixtures schema."""
    league = row.get("league")
    home_team = row.get("home_team")
    away_team = row.get("away_team")
    venue = row.get("venue")
    af_home_id = _parse_af_id_from_struct(home_team)
    af_away_id = _parse_af_id_from_struct(away_team)

    af_fixture_id = _coerce_int(row.get("source_fixture_id") or row.get("fixture_id"))
    home_score = row.get("home_goals")
    away_score = row.get("away_goals")
    af_winner_id = _derive_winner_id(home_score, away_score, af_home_id, af_away_id)

    kickoff = row.get("kickoff_utc")
    if kickoff is not None:
        ts = pd.Timestamp(kickoff)
        timestamp_str = ts.isoformat()
        date_str = ts.date().isoformat()
    else:
        timestamp_str = None
        date_str = day

    season_raw = row.get("season")
    season_int = _coerce_int(str(season_raw).split("-")[0] if season_raw is not None else None)

    return {
        "af_fixture_id": af_fixture_id,
        "referee_name": row.get("referee"),
        "date": date_str,
        "timestamp": timestamp_str,
        "periods_first": None,
        "periods_second": None,
        "venue_id": _parse_af_id_from_struct(venue),
        "venue_name": _struct_field(venue, "name"),
        "venue_city": _struct_field(venue, "city"),
        "status_long": row.get("status") or "Unknown",
        "status_short": row.get("status") or "NS",
        "status_elapsed_time": None,
        "af_league_id": _parse_af_id_from_struct(league),
        "season": season_int,
        "round": "" if row.get("match_week") is None else str(row.get("match_week")),
        "af_home_id": af_home_id,
        "af_away_id": af_away_id,
        "af_winner_id": af_winner_id,
        "af_home_name": _struct_field(home_team, "name") or "",
        "af_away_name": _struct_field(away_team, "name") or "",
        "home_score": home_score,
        "away_score": away_score,
        "home_score_halftime": row.get("home_goals_halftime"),
        "away_score_halftime": row.get("away_goals_halftime"),
        "home_score_fulltime": home_score,
        "away_score_fulltime": away_score,
        "home_score_extratime": None,
        "away_score_extratime": None,
        "home_score_penalty": None,
        "away_score_penalty": None,
        "day": day,
        "data_available_at": row.get("data_available_at"),
    }


def _legacy_stats_row(row: pd.Series) -> dict[str, object]:
    """Extract per-fixture match-stats from a legacy fixtures row."""
    return {
        "af_fixture_id": _coerce_int(row.get("source_fixture_id") or row.get("fixture_id")),
        "home_xg": row.get("home_xg"),
        "away_xg": row.get("away_xg"),
        "home_total_shots": row.get("home_total_shots"),
        "away_total_shots": row.get("away_total_shots"),
        "home_shots_on_target": row.get("home_shots_on_target"),
        "away_shots_on_target": row.get("away_shots_on_target"),
        "home_shots_blocked": row.get("home_shots_blocked"),
        "away_shots_blocked": row.get("away_shots_blocked"),
        "home_corners": row.get("home_corners"),
        "away_corners": row.get("away_corners"),
        "home_fouls": row.get("home_fouls"),
        "away_fouls": row.get("away_fouls"),
        "home_offsides": row.get("home_offsides"),
        "away_offsides": row.get("away_offsides"),
        "home_possession": row.get("home_possession"),
        "away_possession": row.get("away_possession"),
        "home_passes_total": row.get("home_passes_total"),
        "away_passes_total": row.get("away_passes_total"),
        "home_passes_accuracy": row.get("home_passes_accuracy"),
        "away_passes_accuracy": row.get("away_passes_accuracy"),
        "home_yellow_cards": row.get("home_yellow_cards"),
        "away_yellow_cards": row.get("away_yellow_cards"),
        "home_red_cards": row.get("home_red_cards"),
        "away_red_cards": row.get("away_red_cards"),
        "match_week": row.get("match_week"),
        "data_available_at": row.get("data_available_at"),
    }


def map_legacy_to_new(df_legacy: pd.DataFrame, *, day: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map a legacy fixtures DataFrame to ``(fixtures_new_df, fixture_stats_df)``.

    Args:
        df_legacy: DataFrame with legacy schema (nested ``league`` /
            ``home_team`` / ``away_team`` / ``venue`` struct cells; match
            stats inline as flat columns).
        day: Partition date this DataFrame came from (``YYYY-MM-DD``). Used
            as the ``day`` column when ``kickoff_utc`` is absent.

    Returns:
        ``(fixtures, fixture_stats)`` — two DataFrames matching the
        ``SPORTS_FIXTURES`` and ``SPORTS_FIXTURE_STATS`` SchemaContracts.
        Empty DataFrames preserve the column order via the explicit
        column list, so the migration job always writes the same parquet
        schema even on legitimately-zero-fixture days.
    """
    if df_legacy.empty:
        return (
            pd.DataFrame(columns=list(_FIXTURES_COLUMNS)),
            pd.DataFrame(columns=list(_FIXTURE_STATS_COLUMNS)),
        )

    fixtures_rows = [_legacy_fixtures_row(row, day) for _, row in df_legacy.iterrows()]
    stats_rows = [_legacy_stats_row(row) for _, row in df_legacy.iterrows()]

    fixtures_df = pd.DataFrame(fixtures_rows, columns=list(_FIXTURES_COLUMNS))
    stats_df = pd.DataFrame(stats_rows, columns=list(_FIXTURE_STATS_COLUMNS))
    return fixtures_df, stats_df


__all__ = [
    "is_legacy_schema",
    "map_legacy_to_new",
]
