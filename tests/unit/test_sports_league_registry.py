"""
Unit tests for the league registry (LEAGUE_REGISTRY dict and LeagueDefinition).

Tests cover:
- Registry size and known leagues
- LeagueDefinition fields (data_sources, api_football_id, tier, classification)
- Lookup helpers: by ID, sport, classification, country, API-Football ID
- Immutability
"""

from __future__ import annotations

import pytest

from instruments_service.sports.league_registry import (
    LEAGUE_REGISTRY,
    LeagueDefinition,
    get_league,
    get_league_by_api_football_id,
    get_leagues_by_classification,
    get_leagues_by_country,
    get_leagues_for_sport,
    get_prediction_leagues,
)


class TestLeagueRegistry:
    """Tests for the league registry module."""

    def test_registry_not_empty(self) -> None:
        """Registry has at least one entry."""
        assert len(LEAGUE_REGISTRY) > 0

    def test_registry_has_60_plus_leagues(self) -> None:
        """Registry has 60+ leagues after expansion."""
        assert len(LEAGUE_REGISTRY) >= 60

    def test_known_leagues_present(self) -> None:
        """Core leagues are present in the registry."""
        for league_id in ("EPL", "NBA", "NFL", "MLB", "NHL"):
            assert league_id in LEAGUE_REGISTRY, f"{league_id} missing from registry"

    def test_new_football_leagues_present(self) -> None:
        """Expanded football leagues from classification config are present."""
        expanded = (
            "ENG_CHAMPIONSHIP",
            "ENG_LEAGUE_ONE",
            "ENG_LEAGUE_TWO",
            "SEGUNDA_DIVISION",
            "BUNDESLIGA_2",
            "LIGA_3",
            "SERIE_B",
            "LIGUE_2",
            "EREDIVISIE",
            "PRIMEIRA_LIGA",
            "JUPILER_PRO",
            "SUPER_LIG",
            "GREEK_SUPER_LEAGUE",
            "SCOTTISH_PREMIERSHIP",
            "AUSTRIAN_BUNDESLIGA",
            "SWISS_SUPER_LEAGUE",
            "DANISH_SUPERLIGA",
            "ELITESERIEN",
            "ALLSVENSKAN",
            "EKSTRAKLASA",
            "ARGENTINA_PRIMERA",
            "BRASILEIRAO",
            "CHILE_PRIMERA",
            "LIGA_MX",
            "J1_LEAGUE",
            "K_LEAGUE_1",
            "A_LEAGUE",
        )
        for league_id in expanded:
            assert league_id in LEAGUE_REGISTRY, f"{league_id} missing from registry"

    def test_cup_competitions_present(self) -> None:
        """Reference/cup competitions are present."""
        cups = (
            "UCL",
            "UEL",
            "UECL",
            "COPA_LIBERTADORES",
            "CARABAO_CUP",
            "DFB_POKAL",
            "COPA_DEL_REY",
            "COPPA_ITALIA",
            "COUPE_DE_FRANCE",
        )
        for league_id in cups:
            assert league_id in LEAGUE_REGISTRY, f"{league_id} missing from registry"

    def test_league_definition_fields(self) -> None:
        """Each league definition has the expected attributes including new fields."""
        epl = LEAGUE_REGISTRY["EPL"]
        assert isinstance(epl, LeagueDefinition)
        assert epl.league_id == "EPL"
        assert epl.display_name == "English Premier League"
        assert epl.sport == "FOOTBALL"
        assert epl.country == "GB"
        assert epl.season_months == (8, 5)
        assert epl.has_playoffs is False
        # New fields
        assert isinstance(epl.data_sources, frozenset)
        assert "api_football" in epl.data_sources
        assert "understat" in epl.data_sources  # EPL has Understat
        assert epl.api_football_id == 39
        assert epl.tier == 1
        assert epl.classification == "Prediction"

    def test_non_football_league_no_api_football_id(self) -> None:
        """Non-football leagues have api_football_id=None."""
        nba = LEAGUE_REGISTRY["NBA"]
        assert nba.api_football_id is None
        assert nba.sport == "BASKETBALL"
        assert len(nba.data_sources) == 0

    def test_data_sources_frozenset(self) -> None:
        """data_sources is a frozenset for all leagues."""
        for league in LEAGUE_REGISTRY.values():
            assert isinstance(league.data_sources, frozenset), f"{league.league_id} data_sources is not frozenset"

    def test_tier_values(self) -> None:
        """Tier values are non-negative integers."""
        for league in LEAGUE_REGISTRY.values():
            assert isinstance(league.tier, int)
            assert league.tier >= 0

    def test_classification_values(self) -> None:
        """Classification is one of the known labels."""
        valid = {"Prediction", "Features", "Reference", "Other"}
        for league in LEAGUE_REGISTRY.values():
            assert league.classification in valid, (
                f"{league.league_id} has invalid classification: {league.classification}"
            )

    def test_get_league_case_insensitive(self) -> None:
        """get_league normalizes to uppercase."""
        assert get_league("epl") is not None
        assert get_league("nba") is not None
        result = get_league("epl")
        assert result is not None
        assert result.league_id == "EPL"

    def test_get_league_unknown(self) -> None:
        """get_league returns None for unknown IDs."""
        assert get_league("NONEXISTENT_LEAGUE") is None

    def test_get_league_by_api_football_id(self) -> None:
        """Lookup by API-Football numeric ID."""
        league = get_league_by_api_football_id(39)
        assert league is not None
        assert league.league_id == "EPL"

        league = get_league_by_api_football_id(78)
        assert league is not None
        assert league.league_id == "BUNDESLIGA"

    def test_get_league_by_api_football_id_unknown(self) -> None:
        """Unknown API-Football ID returns None."""
        assert get_league_by_api_football_id(99999) is None

    def test_get_leagues_for_sport_football(self) -> None:
        """Football leagues are correctly filtered — now 30+."""
        football_leagues = get_leagues_for_sport("FOOTBALL")
        assert len(football_leagues) >= 30
        for league in football_leagues:
            assert league.sport == "FOOTBALL"

    def test_get_leagues_for_sport_basketball(self) -> None:
        """Basketball leagues are correctly filtered."""
        basketball_leagues = get_leagues_for_sport("basketball")
        assert len(basketball_leagues) >= 1
        for league in basketball_leagues:
            assert league.sport == "BASKETBALL"

    def test_get_leagues_for_sport_unknown(self) -> None:
        """Unknown sport returns empty list."""
        assert get_leagues_for_sport("CURLING") == []

    def test_get_leagues_by_classification_prediction(self) -> None:
        """Prediction leagues filter correctly."""
        prediction = get_leagues_by_classification("Prediction")
        assert len(prediction) >= 25  # 25+ prediction leagues
        for league in prediction:
            assert league.classification == "Prediction"

    def test_get_prediction_leagues(self) -> None:
        """Convenience function returns prediction leagues."""
        prediction = get_prediction_leagues()
        assert len(prediction) >= 25
        for league in prediction:
            assert league.classification == "Prediction"

    def test_get_leagues_by_country(self) -> None:
        """Country filter returns correct leagues."""
        gb_leagues = get_leagues_by_country("GB")
        assert len(gb_leagues) >= 4  # EPL, Championship, L1, L2, Scottish, cups...
        for league in gb_leagues:
            assert league.country == "GB"

    def test_league_definition_is_frozen(self) -> None:
        """LeagueDefinition instances are immutable (frozen dataclass)."""
        epl = LEAGUE_REGISTRY["EPL"]
        with pytest.raises(AttributeError):
            epl.league_id = "CHANGED"  # type: ignore[misc]
