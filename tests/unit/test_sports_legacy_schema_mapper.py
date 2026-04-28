"""Tests for ``instruments_service.sports.legacy_schema_mapper``.

Phase 2 of the sports fixtures legacy schema migration plan
(``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``).

Pin the mapper output column set + a few representative field mappings so
the Phase 3 VM-shard rewrite can trust the helper. The fixture mirrors the
shape of a real legacy parquet row (probed against
``gs://instruments-store-sports-…/sports_reference/by_date/day=2018-04-01/
entity=fixtures/fixtures.parquet`` during plan authoring).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from instruments_service.sports.legacy_schema_mapper import (
    is_legacy_schema,
    map_legacy_to_new,
)


def _legacy_fixture_row(
    *,
    source_fixture_id: str = "11599",
    af_home_id: int = 42,
    af_away_id: int = 49,
    af_league_id: int = 39,
    home_goals: int | None = 2,
    away_goals: int | None = 0,
    home_xg: float = 1.8,
) -> dict[str, object]:
    """Build one row matching the real 2018-04-01 fixtures.parquet shape."""
    return {
        "fixture_id": source_fixture_id,
        "source_fixture_id": source_fixture_id,
        "league": {
            "country": "England",
            "league_id": "ENGLAND_PREMIER_LEAGUE",
            "league_type": None,
            "logo_url": f"https://media.api-sports.io/football/leagues/{af_league_id}.png",
            "name": "Premier League",
        },
        "home_team": {
            "name": "Arsenal",
            "logo_url": f"https://media.api-sports.io/football/teams/{af_home_id}.png",
        },
        "away_team": {
            "name": "Stoke City",
            "logo_url": f"https://media.api-sports.io/football/teams/{af_away_id}.png",
        },
        "venue": {"name": "Emirates Stadium", "city": "London", "venue_id": 494},
        "kickoff_utc": datetime(2018, 4, 1, 14, 30, tzinfo=UTC),
        "season": "2017",
        "match_week": 32,
        "referee": "M. Atkinson",
        "status": "FT",
        "home_goals": home_goals,
        "away_goals": away_goals,
        "home_goals_halftime": 1,
        "away_goals_halftime": 0,
        "home_xg": home_xg,
        "away_xg": 0.4,
        "home_total_shots": 18,
        "away_total_shots": 6,
        "home_shots_on_target": 9,
        "away_shots_on_target": 1,
        "home_shots_blocked": 3,
        "away_shots_blocked": 2,
        "home_corners": 7,
        "away_corners": 1,
        "home_fouls": 9,
        "away_fouls": 14,
        "home_offsides": 2,
        "away_offsides": 0,
        "home_possession": 64,
        "away_possession": 36,
        "home_passes_total": 600,
        "away_passes_total": 320,
        "home_passes_accuracy": 87,
        "away_passes_accuracy": 73,
        "home_yellow_cards": 1,
        "away_yellow_cards": 4,
        "home_red_cards": 0,
        "away_red_cards": 0,
        "data_available_at": datetime(2018, 3, 25, 14, 30, tzinfo=UTC),
    }


_NEW_FIXTURES_COLUMNS = {
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
}


_NEW_FIXTURE_STATS_COLUMNS = {
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
}


def test_is_legacy_schema_true_for_struct_cells() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    assert is_legacy_schema(df) is True


def test_is_legacy_schema_false_for_new_flat_schema() -> None:
    df = pd.DataFrame([{"af_league_id": 39, "af_fixture_id": 11599}])
    assert is_legacy_schema(df) is False


def test_is_legacy_schema_false_for_empty_df() -> None:
    df = pd.DataFrame()
    assert is_legacy_schema(df) is False


def test_map_legacy_to_new_emits_full_fixtures_column_set() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    fixtures_df, _stats_df = map_legacy_to_new(df, day="2018-04-01")
    assert set(fixtures_df.columns) == _NEW_FIXTURES_COLUMNS


def test_map_legacy_to_new_emits_full_fixture_stats_column_set() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    _fixtures_df, stats_df = map_legacy_to_new(df, day="2018-04-01")
    assert set(stats_df.columns) == _NEW_FIXTURE_STATS_COLUMNS


def test_map_legacy_to_new_no_nested_struct_cells_in_fixtures() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    for col in fixtures_df.columns:
        for value in fixtures_df[col]:
            assert not isinstance(value, dict), f"{col}={value!r} is nested"


def test_af_ids_extracted_from_logo_urls() -> None:
    df = pd.DataFrame([_legacy_fixture_row(af_home_id=42, af_away_id=49, af_league_id=39)])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    row = fixtures_df.iloc[0]
    assert row["af_home_id"] == 42
    assert row["af_away_id"] == 49
    assert row["af_league_id"] == 39


def test_winner_id_home_win() -> None:
    df = pd.DataFrame([_legacy_fixture_row(af_home_id=42, af_away_id=49, home_goals=3, away_goals=1)])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["af_winner_id"] == 42


def test_winner_id_away_win() -> None:
    df = pd.DataFrame([_legacy_fixture_row(af_home_id=42, af_away_id=49, home_goals=0, away_goals=2)])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["af_winner_id"] == 49


def test_winner_id_null_on_draw() -> None:
    df = pd.DataFrame([_legacy_fixture_row(home_goals=1, away_goals=1)])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["af_winner_id"] is None


def test_winner_id_null_on_unplayed() -> None:
    df = pd.DataFrame([_legacy_fixture_row(home_goals=None, away_goals=None)])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["af_winner_id"] is None


def test_extratime_penalty_period_columns_default_null() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    row = fixtures_df.iloc[0]
    assert row["home_score_extratime"] is None
    assert row["away_score_extratime"] is None
    assert row["home_score_penalty"] is None
    assert row["away_score_penalty"] is None
    assert row["periods_first"] is None
    assert row["periods_second"] is None
    assert row["status_elapsed_time"] is None


def test_match_stats_routed_to_fixture_stats_df() -> None:
    df = pd.DataFrame([_legacy_fixture_row(home_xg=1.8)])
    fixtures_df, stats_df = map_legacy_to_new(df, day="2018-04-01")
    # Stats columns are NOT in fixtures_df
    assert "home_xg" not in fixtures_df.columns
    assert "home_corners" not in fixtures_df.columns
    # Stats columns ARE in stats_df
    assert stats_df.iloc[0]["home_xg"] == 1.8
    assert stats_df.iloc[0]["home_corners"] == 7


def test_join_key_consistency_between_fixtures_and_stats() -> None:
    """``af_fixture_id`` must be identical in both DataFrames so they can join."""
    df = pd.DataFrame(
        [
            _legacy_fixture_row(source_fixture_id="11599"),
            _legacy_fixture_row(source_fixture_id="11600"),
            _legacy_fixture_row(source_fixture_id="11601"),
        ]
    )
    fixtures_df, stats_df = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df["af_fixture_id"].tolist() == [11599, 11600, 11601]
    assert stats_df["af_fixture_id"].tolist() == [11599, 11600, 11601]


def test_empty_df_preserves_column_order() -> None:
    df = pd.DataFrame()
    fixtures_df, stats_df = map_legacy_to_new(df, day="2018-04-01")
    assert set(fixtures_df.columns) == _NEW_FIXTURES_COLUMNS
    assert set(stats_df.columns) == _NEW_FIXTURE_STATS_COLUMNS
    assert len(fixtures_df) == 0
    assert len(stats_df) == 0


def test_logo_url_parser_handles_missing_urls() -> None:
    """Malformed struct cells (no logo_url) → af_id is None, not a crash."""
    row = _legacy_fixture_row()
    row["league"] = {"country": "England", "name": "Premier League"}  # no logo_url
    df = pd.DataFrame([row])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["af_league_id"] is None


def test_kickoff_utc_drives_date_and_timestamp() -> None:
    df = pd.DataFrame([_legacy_fixture_row()])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    row = fixtures_df.iloc[0]
    assert row["date"] == "2018-04-01"
    assert row["timestamp"].startswith("2018-04-01T14:30:00")


def test_day_partition_kept_when_kickoff_missing() -> None:
    row = _legacy_fixture_row()
    row["kickoff_utc"] = None
    df = pd.DataFrame([row])
    fixtures_df, _ = map_legacy_to_new(df, day="2018-04-01")
    assert fixtures_df.iloc[0]["date"] == "2018-04-01"
    assert fixtures_df.iloc[0]["timestamp"] is None
    assert fixtures_df.iloc[0]["day"] == "2018-04-01"
