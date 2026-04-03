"""Integration tests for the sports sub-package.

Verifies sports adapters and factory function are importable
and that the sports sub-package integrates with UAC types.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalTeam,
    CanonicalVenue,
)


@pytest.mark.integration
class TestSportsSubpackageImports:
    """Sports sub-package exports are available and correct."""

    def test_factory_creates_known_adapter(self) -> None:
        from unittest.mock import patch

        from instruments_service.reference_data.adapters.sports import (
            ApiFootballAdapter,
            create_sports_reference_adapter,
        )

        with patch.object(ApiFootballAdapter, "__init__", return_value=None):
            adapter = create_sports_reference_adapter("api_football", api_key="test")
            assert isinstance(adapter, ApiFootballAdapter)

    def test_factory_rejects_unknown_venue(self) -> None:
        from instruments_service.reference_data.adapters.sports import (
            create_sports_reference_adapter,
        )

        with pytest.raises(ValueError, match="not_a_venue"):
            create_sports_reference_adapter("not_a_venue")


@pytest.mark.integration
class TestCanonicalFixtureIntegration:
    """CanonicalFixture: construct nested fixture with teams, league, venue."""

    def test_construct_full_fixture(self) -> None:
        venue = CanonicalVenue(
            venue_id="VEN-001",
            name="Old Trafford",
            city="Manchester",
            country="England",
            capacity=74310,
        )
        home = CanonicalTeam(
            team_id="TEAM-001",
            name="Manchester United",
            venue=venue,
        )
        away = CanonicalTeam(team_id="TEAM-002", name="Liverpool")
        league = CanonicalLeague(
            league_id="PL-2026",
            name="Premier League",
            country="England",
        )
        fixture = CanonicalFixture(
            fixture_id="FIX-001",
            home_team=home,
            away_team=away,
            league=league,
            kickoff_utc=datetime(2026, 3, 16, 15, 0, 0, tzinfo=UTC),
            venue=venue,
            season="2025-2026",
            source="api-football",
            home_goals=2,
            away_goals=1,
        )
        assert fixture.fixture_id == "FIX-001"
        assert fixture.home_team.name == "Manchester United"
        assert fixture.away_team.name == "Liverpool"
        assert fixture.league.country == "England"
        assert fixture.venue is not None
        assert fixture.venue.capacity == 74310
        assert fixture.home_goals == 2
        assert fixture.away_goals == 1
