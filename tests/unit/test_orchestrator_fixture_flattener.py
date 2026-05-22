"""Tests for ``_flatten_canonical_fixture_for_disk``.

Phase 0.5 of the sports fixtures legacy schema migration plan
(``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.md``).

Pre-fix, the orchestrator wrote raw ``CanonicalFixture.model_dump()``
output to ``sports_reference/by_date/day=…/entity=fixtures/fixtures.parquet``,
which produced parquets with nested ``league`` / ``home_team`` / ``away_team``
struct cells (the legacy schema). The fix flattens to the on-disk
``SPORTS_FIXTURES`` SchemaContract — 32 ``af_*`` prefixed flat columns.

These tests pin the flattener output column set + a few representative
field mappings so any future regression on the writer surfaces here
before it pollutes production parquets.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd

from instruments_service.engine.orchestrator import (
    _af_id_from_canonical,
    _flatten_canonical_fixture_for_disk,
)


def _make_canonical_fixture(
    *,
    fixture_id: str = "1208022",
    home_id: int = 57,
    away_id: int = 40,
    league_id: int = 39,
    home_goals: int | None = 0,
    away_goals: int | None = 2,
) -> SimpleNamespace:
    """Build a SimpleNamespace shaped like CanonicalFixture for tests.

    Avoids importing the real Pydantic model so this test stays decoupled
    from UAC schema evolution. The flattener uses ``getattr`` everywhere,
    so duck-typing is sufficient.
    """
    return SimpleNamespace(
        fixture_id=fixture_id,
        source_fixture_id=fixture_id,
        home_team=SimpleNamespace(
            api_football_id=home_id,
            name="Ipswich",
            logo_url=f"https://media.api-sports.io/football/teams/{home_id}.png",
        ),
        away_team=SimpleNamespace(
            api_football_id=away_id,
            name="Liverpool",
            logo_url=f"https://media.api-sports.io/football/teams/{away_id}.png",
        ),
        league=SimpleNamespace(
            api_football_id=league_id,
            league_id="EPL",
            logo_url=f"https://media.api-sports.io/football/leagues/{league_id}.png",
        ),
        venue=SimpleNamespace(api_football_id=545, name="Portman Road", city="Ipswich, Suffolk"),
        referee=SimpleNamespace(name="T. Robinson"),
        kickoff_utc=datetime(2024, 8, 17, 11, 30, tzinfo=UTC),
        season="2024-25",
        status="FT",
        home_goals=home_goals,
        away_goals=away_goals,
        home_goals_halftime=0,
        away_goals_halftime=0,
    )


_EXPECTED_COLUMNS = {
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
    "available_at",
    "match_end_time",
    "announced_at",
    "report_time",
}


def test_flattener_emits_full_sports_fixtures_column_set() -> None:
    """The flattener output covers every SPORTS_FIXTURES SchemaContract column."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert set(out.keys()) == _EXPECTED_COLUMNS


def test_flattener_no_nested_struct_cells() -> None:
    """No output value should be a dict / Pydantic model — every cell flat."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    for key, value in out.items():
        assert not isinstance(value, dict), f"{key}={value!r} is nested (legacy schema regression)"


def test_flattener_af_ids_use_api_football_id_attribute() -> None:
    fx = _make_canonical_fixture(home_id=42, away_id=49, league_id=39)
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["af_home_id"] == 42
    assert out["af_away_id"] == 49
    assert out["af_league_id"] == 39


def test_flattener_winner_id_derivation_home_win() -> None:
    fx = _make_canonical_fixture(home_id=10, away_id=20, home_goals=3, away_goals=1)
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["af_winner_id"] == 10


def test_flattener_winner_id_derivation_away_win() -> None:
    fx = _make_canonical_fixture(home_id=10, away_id=20, home_goals=0, away_goals=2)
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["af_winner_id"] == 20


def test_flattener_winner_id_null_on_draw() -> None:
    fx = _make_canonical_fixture(home_goals=1, away_goals=1)
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["af_winner_id"] is None


def test_flattener_winner_id_null_on_unplayed() -> None:
    fx = _make_canonical_fixture(home_goals=None, away_goals=None)
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["af_winner_id"] is None


def test_flattener_extratime_penalty_fields_default_null() -> None:
    """Canonical model doesn't capture ET / pens — flattener defaults to None."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["home_score_extratime"] is None
    assert out["away_score_extratime"] is None
    assert out["home_score_penalty"] is None
    assert out["away_score_penalty"] is None


def test_flattener_round_defaults_empty_string_when_missing() -> None:
    """SchemaContract: round is non-null. Default to empty string when canonical model lacks it."""
    fx = _make_canonical_fixture()
    out = _flatten_canonical_fixture_for_disk(fx, "2024-08-17")
    assert out["round"] == ""


def test_flattener_dataframe_assemblable() -> None:
    """Multiple flattened fixtures cleanly construct a homogeneous DataFrame."""
    fixtures = [_make_canonical_fixture(fixture_id=str(1000 + i)) for i in range(3)]
    rows = [_flatten_canonical_fixture_for_disk(fx, "2024-08-17") for fx in fixtures]
    df = pd.DataFrame(rows)
    assert len(df) == 3
    assert set(df.columns) == _EXPECTED_COLUMNS
    assert df["af_fixture_id"].tolist() == [1000, 1001, 1002]


def test_af_id_from_canonical_attribute_path() -> None:
    obj = SimpleNamespace(api_football_id=39)
    assert _af_id_from_canonical(obj) == 39


def test_af_id_from_canonical_logo_url_fallback() -> None:
    obj = SimpleNamespace(
        api_football_id=None,
        logo_url="https://media.api-sports.io/football/leagues/39.png",
    )
    assert _af_id_from_canonical(obj) == 39


def test_af_id_from_canonical_returns_none_when_unparseable() -> None:
    obj = SimpleNamespace(api_football_id=None, logo_url="not a logo url")
    assert _af_id_from_canonical(obj) is None
