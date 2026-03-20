"""
Unit tests for the fixture parser.

Tests cover:
- Instrument key generation from league/team/kickoff
- Multi-league parsing (EPL, NBA, Bundesliga, La Liga, Serie A)
- Validation errors for unknown leagues/teams
- Case-insensitive league lookup
- Custom TeamNormalizer injection
- API-Football ID resolution
- Batch parsing
- data_provider field
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from instruments_service.sports.fixture_parser import FixtureParser
from instruments_service.sports.team_normalizer import TeamNormalizer


class TestFixtureParser:
    """Tests for the fixture parser."""

    def test_parse_valid_fixture(self) -> None:
        """Valid fixture produces correct instrument dict."""
        parser = FixtureParser()
        kickoff = datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="EPL",
            home_team="Arsenal",
            away_team="Chelsea",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "EPL:FIXTURE:ARS-v-CHE@20260315"
        assert result["venue"] == "EPL"
        assert result["instrument_type"] == "FIXTURE"
        assert result["symbol"] == "ARS-v-CHE"
        assert result["available_from_datetime"] == "2026-03-15T15:00:00Z"
        assert result["market_category"] == "SPORTS"
        assert result["asset_class"] == "FOOTBALL"
        assert result["base_asset"] == "Arsenal"
        assert result["quote_asset"] == "Chelsea"
        assert result["data_types"] == "odds,scores"

    def test_parse_nba_fixture(self) -> None:
        """NBA fixture produces correct instrument key."""
        parser = FixtureParser()
        kickoff = datetime(2026, 1, 10, 1, 30, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="NBA",
            home_team="Lakers",
            away_team="Celtics",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "NBA:FIXTURE:LAL-v-BOS@20260110"
        assert result["asset_class"] == "BASKETBALL"

    def test_parse_bundesliga_fixture(self) -> None:
        """Bundesliga fixture with new teams works."""
        parser = FixtureParser()
        kickoff = datetime(2026, 9, 20, 14, 30, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="BUNDESLIGA",
            home_team="Bayern Munich",
            away_team="Borussia Dortmund",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "BUNDESLIGA:FIXTURE:BAY-v-BVB@20260920"
        assert result["venue"] == "BUNDESLIGA"
        assert result["asset_class"] == "FOOTBALL"

    def test_parse_la_liga_fixture(self) -> None:
        """La Liga fixture works."""
        parser = FixtureParser()
        kickoff = datetime(2026, 10, 5, 19, 0, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="LA_LIGA",
            home_team="Real Madrid",
            away_team="Barcelona",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "LA_LIGA:FIXTURE:RMA-v-BAR@20261005"

    def test_parse_serie_a_fixture(self) -> None:
        """Serie A fixture works."""
        parser = FixtureParser()
        kickoff = datetime(2026, 11, 1, 17, 45, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="SERIE_A",
            home_team="Juventus",
            away_team="Inter Milan",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "SERIE_A:FIXTURE:JUV-v-INT@20261101"

    def test_parse_unknown_league_raises(self) -> None:
        """Unknown league raises ValueError."""
        parser = FixtureParser()
        with pytest.raises(ValueError, match="Unknown league"):
            parser.parse_fixture(
                league_id="UNKNOWN_LEAGUE",
                home_team="Arsenal",
                away_team="Chelsea",
                kickoff_utc=datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC),
            )

    def test_parse_unknown_home_team_raises(self) -> None:
        """Unknown home team raises ValueError."""
        parser = FixtureParser()
        with pytest.raises(ValueError, match="Cannot resolve home team"):
            parser.parse_fixture(
                league_id="EPL",
                home_team="Nonexistent FC",
                away_team="Chelsea",
                kickoff_utc=datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC),
            )

    def test_parse_unknown_away_team_raises(self) -> None:
        """Unknown away team raises ValueError."""
        parser = FixtureParser()
        with pytest.raises(ValueError, match="Cannot resolve away team"):
            parser.parse_fixture(
                league_id="EPL",
                home_team="Arsenal",
                away_team="Nonexistent FC",
                kickoff_utc=datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC),
            )

    def test_parse_case_insensitive_league(self) -> None:
        """League ID is case-insensitive."""
        parser = FixtureParser()
        kickoff = datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="epl",
            home_team="Arsenal",
            away_team="Chelsea",
            kickoff_utc=kickoff,
        )
        assert result["venue"] == "EPL"

    def test_parse_custom_normalizer(self) -> None:
        """Parser accepts a custom TeamNormalizer."""
        normalizer = TeamNormalizer()
        normalizer.register_alias("Team A", "TA", "Team Alpha")
        normalizer.register_alias("Team B", "TB", "Team Beta")

        parser = FixtureParser(team_normalizer=normalizer)
        kickoff = datetime(2026, 6, 1, 20, 0, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="NFL",
            home_team="Team A",
            away_team="Team B",
            kickoff_utc=kickoff,
        )
        assert result["instrument_key"] == "NFL:FIXTURE:TA-v-TB@20260601"

    def test_parse_by_api_football_id(self) -> None:
        """Fixture parser can resolve league by API-Football numeric ID."""
        parser = FixtureParser()
        kickoff = datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC)
        # "39" is EPL's API-Football ID
        result = parser.parse_fixture(
            league_id="39",
            home_team="Arsenal",
            away_team="Chelsea",
            kickoff_utc=kickoff,
        )
        assert result["venue"] == "EPL"
        assert result["instrument_key"] == "EPL:FIXTURE:ARS-v-CHE@20260315"

    def test_parse_fixtures_batch(self) -> None:
        """Batch parsing processes valid fixtures and skips invalid ones."""
        parser = FixtureParser()
        kickoff = datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC)
        fixtures = (
            ("EPL", "Arsenal", "Chelsea", kickoff),
            ("EPL", "NoTeam", "Chelsea", kickoff),  # Will fail
            ("BUNDESLIGA", "Bayern Munich", "Borussia Dortmund", kickoff),
        )
        results = parser.parse_fixtures(fixtures)
        assert len(results) == 2
        assert results[0]["venue"] == "EPL"
        assert results[1]["venue"] == "BUNDESLIGA"

    def test_resolve_league_for_api_football_id(self) -> None:
        """resolve_league_for_api_football_id returns canonical league_id."""
        parser = FixtureParser()
        assert parser.resolve_league_for_api_football_id(39) == "EPL"
        assert parser.resolve_league_for_api_football_id(78) == "BUNDESLIGA"
        assert parser.resolve_league_for_api_football_id(99999) is None

    def test_data_provider_populated(self) -> None:
        """data_provider field reflects the league's data sources."""
        parser = FixtureParser()
        kickoff = datetime(2026, 3, 15, 15, 0, 0, tzinfo=UTC)
        result = parser.parse_fixture(
            league_id="EPL",
            home_team="Arsenal",
            away_team="Chelsea",
            kickoff_utc=kickoff,
        )
        # EPL has all 7 data sources
        data_provider = result["data_provider"]
        assert isinstance(data_provider, str)
        assert "api_football" in data_provider
        assert "odds_api" in data_provider
