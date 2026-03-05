"""
Unit tests for LeagueClassification and LeagueClassificationRegistry.

Tests cover:
- Registry loads correctly from embedded data
- get_league returns correct data for known IDs
- get_prediction_leagues filters tier 1+2 Prediction leagues
- get_leagues_by_tier filters correctly
- get_data_sources returns provider flags
- LeagueClassificationType enum values
- from_raw_dict factory method
- Edge cases: unknown league IDs, empty registry
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from instruments_service.sports.league_classification import (
    DEFAULT_CLASSIFICATION_REGISTRY,
    LEAGUE_CLASSIFICATION_DATA,
    LeagueClassification,
    LeagueClassificationRegistry,
    LeagueClassificationType,
)

# ---------------------------------------------------------------------------
# LeagueClassificationType enum
# ---------------------------------------------------------------------------


class TestLeagueClassificationType:
    """Tests for the classification enum."""

    def test_prediction_value(self) -> None:
        assert LeagueClassificationType.PREDICTION == "Prediction"

    def test_reference_value(self) -> None:
        assert LeagueClassificationType.REFERENCE == "Reference"

    def test_features_value(self) -> None:
        assert LeagueClassificationType.FEATURES == "Features"

    def test_is_str(self) -> None:
        """StrEnum values are usable as plain strings."""
        val: str = LeagueClassificationType.PREDICTION
        assert isinstance(val, str)
        assert val == "Prediction"


# ---------------------------------------------------------------------------
# LeagueClassification model
# ---------------------------------------------------------------------------


class TestLeagueClassificationModel:
    """Tests for the Pydantic model."""

    def test_create_minimal(self) -> None:
        lc = LeagueClassification(
            league_id=39,
            name="Premier League",
            country="England",
            tier=1,
            classification=LeagueClassificationType.PREDICTION,
        )
        assert lc.league_id == 39
        assert lc.name == "Premier League"
        assert lc.odds_api_name is None
        assert lc.data_sources == {}

    def test_create_full(self) -> None:
        lc = LeagueClassification(
            league_id=39,
            name="Premier League",
            country="England",
            tier=1,
            classification=LeagueClassificationType.PREDICTION,
            odds_api_name="soccer_epl",
            data_sources={
                "api_football": True,
                "understat": True,
                "odds_api": True,
            },
        )
        assert lc.odds_api_name == "soccer_epl"
        assert lc.data_sources["api_football"] is True
        assert lc.data_sources["understat"] is True

    def test_tier_must_be_positive(self) -> None:
        """Tier must be >= 1."""
        with pytest.raises(ValueError):
            LeagueClassification(
                league_id=1,
                name="Test",
                country="Test",
                tier=0,
                classification=LeagueClassificationType.PREDICTION,
            )

    def test_frozen_model(self) -> None:
        """Model is frozen (immutable)."""
        lc = LeagueClassification(
            league_id=39,
            name="Premier League",
            country="England",
            tier=1,
            classification=LeagueClassificationType.PREDICTION,
        )
        with pytest.raises(ValidationError):
            lc.name = "Changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# LeagueClassificationRegistry — default instance
# ---------------------------------------------------------------------------


class TestDefaultClassificationRegistry:
    """Tests for the default registry built from LEAGUE_CLASSIFICATION_DATA."""

    def test_registry_not_empty(self) -> None:
        assert DEFAULT_CLASSIFICATION_REGISTRY.league_count > 0

    def test_registry_has_94_leagues(self) -> None:
        """Embedded data has 94 leagues (complete classification)."""
        assert DEFAULT_CLASSIFICATION_REGISTRY.league_count == 94

    def test_data_dict_matches_registry(self) -> None:
        """LEAGUE_CLASSIFICATION_DATA keys match registry entries."""
        assert len(LEAGUE_CLASSIFICATION_DATA) == DEFAULT_CLASSIFICATION_REGISTRY.league_count

    # -- get_league ----------------------------------------------------------

    def test_get_league_epl(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(39)
        assert league is not None
        assert league.league_id == 39
        assert league.name == "Premier League"
        assert league.country == "England"
        assert league.tier == 1
        assert league.classification == LeagueClassificationType.PREDICTION
        assert league.odds_api_name == "soccer_epl"

    def test_get_league_la_liga(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(140)
        assert league is not None
        assert league.name == "La Liga"
        assert league.country == "Spain"
        assert league.tier == 1

    def test_get_league_bundesliga(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(78)
        assert league is not None
        assert league.name == "Bundesliga"
        assert league.country == "Germany"

    def test_get_league_serie_a(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(135)
        assert league is not None
        assert league.name == "Serie A"
        assert league.country == "Italy"

    def test_get_league_ligue_1(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(61)
        assert league is not None
        assert league.name == "Ligue 1"
        assert league.country == "France"

    def test_get_league_championship(self) -> None:
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(40)
        assert league is not None
        assert league.name == "Championship"
        assert league.tier == 2

    def test_get_league_unknown_returns_none(self) -> None:
        assert DEFAULT_CLASSIFICATION_REGISTRY.get_league(99999) is None

    # -- get_prediction_leagues (tier 1+2 Prediction) ------------------------

    def test_get_prediction_leagues_count(self) -> None:
        """Prediction leagues include tier 1 and tier 2."""
        prediction = DEFAULT_CLASSIFICATION_REGISTRY.get_prediction_leagues()
        # 27 tier-1 Prediction + 5 tier-2 Prediction = 30 (excludes tier 3/4 and Features/Reference)
        assert len(prediction) == 30

    def test_get_prediction_leagues_all_prediction(self) -> None:
        prediction = DEFAULT_CLASSIFICATION_REGISTRY.get_prediction_leagues()
        for league in prediction:
            assert league.classification == LeagueClassificationType.PREDICTION

    def test_get_prediction_leagues_tier_constraint(self) -> None:
        """All returned leagues are tier 1 or 2."""
        prediction = DEFAULT_CLASSIFICATION_REGISTRY.get_prediction_leagues()
        for league in prediction:
            assert league.tier <= 2, f"{league.name} has tier {league.tier}"

    def test_get_prediction_leagues_excludes_tier_3(self) -> None:
        """3. Liga (tier 3) is NOT in prediction leagues."""
        prediction = DEFAULT_CLASSIFICATION_REGISTRY.get_prediction_leagues()
        league_ids = {league.league_id for league in prediction}
        assert 80 not in league_ids  # 3. Liga

    # -- get_leagues_by_tier -------------------------------------------------

    def test_get_leagues_by_tier_1(self) -> None:
        tier_1 = DEFAULT_CLASSIFICATION_REGISTRY.get_leagues_by_tier(1)
        assert len(tier_1) == 25
        for league in tier_1:
            assert league.tier == 1

    def test_get_leagues_by_tier_2(self) -> None:
        tier_2 = DEFAULT_CLASSIFICATION_REGISTRY.get_leagues_by_tier(2)
        assert len(tier_2) == 24
        for league in tier_2:
            assert league.tier == 2

    def test_get_leagues_by_tier_3(self) -> None:
        tier_3 = DEFAULT_CLASSIFICATION_REGISTRY.get_leagues_by_tier(3)
        assert len(tier_3) == 4
        tier_3_ids = {league.league_id for league in tier_3}
        assert 80 in tier_3_ids  # 3. Liga
        assert 41 in tier_3_ids  # League One

    def test_get_leagues_by_tier_4(self) -> None:
        tier_4 = DEFAULT_CLASSIFICATION_REGISTRY.get_leagues_by_tier(4)
        assert len(tier_4) == 40
        for league in tier_4:
            assert league.tier == 4

    def test_get_leagues_by_tier_nonexistent(self) -> None:
        assert DEFAULT_CLASSIFICATION_REGISTRY.get_leagues_by_tier(99) == []

    # -- get_data_sources ----------------------------------------------------

    def test_get_data_sources_epl(self) -> None:
        """EPL has all 7 sources enabled."""
        sources = DEFAULT_CLASSIFICATION_REGISTRY.get_data_sources(39)
        assert sources["api_football"] is True
        assert sources["soccerfootball_info"] is True
        assert sources["footystats"] is True
        assert sources["transfermarkt"] is True
        assert sources["understat"] is True
        assert sources["odds_api"] is True
        assert sources["open_meteo"] is True

    def test_get_data_sources_eredivisie(self) -> None:
        """Eredivisie has no Understat."""
        sources = DEFAULT_CLASSIFICATION_REGISTRY.get_data_sources(88)
        assert sources["understat"] is False
        assert sources["api_football"] is True

    def test_get_data_sources_argentina(self) -> None:
        """Argentina Primera has no FootyStats (subscription limit)."""
        sources = DEFAULT_CLASSIFICATION_REGISTRY.get_data_sources(128)
        assert sources["footystats"] is False
        assert sources["api_football"] is True

    def test_get_data_sources_unknown_returns_empty(self) -> None:
        sources = DEFAULT_CLASSIFICATION_REGISTRY.get_data_sources(99999)
        assert sources == {}

    # -- get_all_leagues -----------------------------------------------------

    def test_get_all_leagues(self) -> None:
        all_leagues = DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues()
        assert len(all_leagues) == 94

    # -- Understat coverage --------------------------------------------------

    def test_understat_only_top_5(self) -> None:
        """Only EPL, La Liga, Bundesliga, Serie A, Ligue 1 have Understat."""
        understat_ids = {39, 140, 78, 135, 61}
        for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues():
            sources = league.data_sources
            if league.league_id in understat_ids:
                assert sources.get("understat") is True, f"{league.name} should have Understat"
            else:
                assert sources.get("understat") is not True, f"{league.name} should NOT have Understat"

    # -- Classification counts (94-league expansion) -------------------------

    def test_prediction_league_count(self) -> None:
        """33 Prediction leagues across all tiers."""
        prediction = [
            league
            for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues()
            if league.classification == LeagueClassificationType.PREDICTION
        ]
        assert len(prediction) == 33

    def test_reference_league_count(self) -> None:
        """39 Reference leagues (cups and continental competitions)."""
        reference = [
            league
            for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues()
            if league.classification == LeagueClassificationType.REFERENCE
        ]
        assert len(reference) == 39

    def test_features_league_count(self) -> None:
        """22 Features leagues (limited data coverage)."""
        features = [
            league
            for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues()
            if league.classification == LeagueClassificationType.FEATURES
        ]
        assert len(features) == 22

    def test_all_entries_have_valid_classification(self) -> None:
        """Every entry has a valid LeagueClassificationType."""
        valid = {
            LeagueClassificationType.PREDICTION,
            LeagueClassificationType.REFERENCE,
            LeagueClassificationType.FEATURES,
        }
        for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues():
            assert league.classification in valid, (
                f"League {league.league_id} ({league.name}) has invalid classification: {league.classification}"
            )

    def test_reference_leagues_have_tier_4(self) -> None:
        """All Reference (cup) leagues use tier 4 as the non-league convention."""
        for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues():
            if league.classification == LeagueClassificationType.REFERENCE:
                assert league.tier == 4, (
                    f"Reference league {league.league_id} ({league.name}) has tier {league.tier}, expected 4"
                )

    def test_all_prediction_leagues_resolve(self) -> None:
        """Every Prediction league can be looked up by ID."""
        for league in DEFAULT_CLASSIFICATION_REGISTRY.get_all_leagues():
            if league.classification == LeagueClassificationType.PREDICTION:
                resolved = DEFAULT_CLASSIFICATION_REGISTRY.get_league(league.league_id)
                assert resolved is not None, f"Prediction league {league.league_id} not found"
                assert resolved.league_id == league.league_id

    def test_api_football_id_matches_key(self) -> None:
        """api_football_id in each entry matches its dict key."""
        for league_id, entry in LEAGUE_CLASSIFICATION_DATA.items():
            assert entry["api_football_id"] == league_id, (
                f"Key {league_id} has mismatched api_football_id: {entry['api_football_id']}"
            )

    # -- Spot-check newly added leagues ------------------------------------

    def test_champions_league_is_reference(self) -> None:
        """UEFA Champions League is a Reference league at tier 4."""
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(2)
        assert league is not None
        assert league.name == "UEFA Champions League"
        assert league.classification == LeagueClassificationType.REFERENCE
        assert league.tier == 4

    def test_greece_super_league(self) -> None:
        """Greece Super League is a Prediction league at tier 1."""
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(197)
        assert league is not None
        assert league.name == "Super League"
        assert league.country == "Greece"
        assert league.classification == LeagueClassificationType.PREDICTION
        assert league.tier == 1

    def test_k_league_1(self) -> None:
        """South Korea K League 1 is a Prediction league."""
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(292)
        assert league is not None
        assert league.name == "K League 1"
        assert league.country == "South Korea"
        assert league.classification == LeagueClassificationType.PREDICTION

    def test_copa_del_rey_is_reference(self) -> None:
        """Copa del Rey is a Reference league at tier 4."""
        league = DEFAULT_CLASSIFICATION_REGISTRY.get_league(143)
        assert league is not None
        assert league.name == "Copa del Rey"
        assert league.classification == LeagueClassificationType.REFERENCE
        assert league.tier == 4


# ---------------------------------------------------------------------------
# LeagueClassificationRegistry — from_raw_dict factory
# ---------------------------------------------------------------------------


class TestRegistryFromRawDict:
    """Tests for the from_raw_dict factory method."""

    def test_empty_dict(self) -> None:
        registry = LeagueClassificationRegistry.from_raw_dict({})
        assert registry.league_count == 0
        assert registry.get_all_leagues() == []

    def test_single_entry(self) -> None:
        raw: dict[int, dict[str, str | int | bool | dict[str, bool] | None]] = {
            100: {
                "country_region": "TestCountry",
                "api_football_id": 100,
                "api_football_league_name": "Test League",
                "odds_api_league_name": None,
                "classification": "Features",
                "tier": 2,
                "data_sources": {"api_football": True, "odds_api": False},
            }
        }
        registry = LeagueClassificationRegistry.from_raw_dict(raw)
        assert registry.league_count == 1
        league = registry.get_league(100)
        assert league is not None
        assert league.name == "Test League"
        assert league.country == "TestCountry"
        assert league.classification == LeagueClassificationType.FEATURES
        assert league.odds_api_name is None
        assert league.data_sources == {"api_football": True, "odds_api": False}

    def test_from_raw_dict_preserves_all_entries(self) -> None:
        """All entries from LEAGUE_CLASSIFICATION_DATA are loaded."""
        registry = LeagueClassificationRegistry.from_raw_dict(LEAGUE_CLASSIFICATION_DATA)
        assert registry.league_count == len(LEAGUE_CLASSIFICATION_DATA)


# ---------------------------------------------------------------------------
# LeagueClassificationRegistry — custom instance
# ---------------------------------------------------------------------------


class TestRegistryCustomInstance:
    """Tests for creating custom registry instances."""

    def test_init_direct(self) -> None:
        lc = LeagueClassification(
            league_id=999,
            name="Custom League",
            country="Testland",
            tier=1,
            classification=LeagueClassificationType.PREDICTION,
            odds_api_name="custom_odds",
            data_sources={"api_football": True},
        )
        registry = LeagueClassificationRegistry({999: lc})
        assert registry.league_count == 1
        assert registry.get_league(999) is not None

    def test_get_prediction_leagues_custom(self) -> None:
        """Only tier 1+2 Prediction leagues are returned."""
        lc1 = LeagueClassification(
            league_id=1,
            name="Tier 1 Pred",
            country="A",
            tier=1,
            classification=LeagueClassificationType.PREDICTION,
        )
        lc2 = LeagueClassification(
            league_id=2,
            name="Tier 2 Pred",
            country="A",
            tier=2,
            classification=LeagueClassificationType.PREDICTION,
        )
        lc3 = LeagueClassification(
            league_id=3,
            name="Tier 3 Pred",
            country="A",
            tier=3,
            classification=LeagueClassificationType.PREDICTION,
        )
        lc4 = LeagueClassification(
            league_id=4,
            name="Tier 1 Features",
            country="A",
            tier=1,
            classification=LeagueClassificationType.FEATURES,
        )
        registry = LeagueClassificationRegistry({1: lc1, 2: lc2, 3: lc3, 4: lc4})
        prediction = registry.get_prediction_leagues()
        assert len(prediction) == 2
        ids = {lc.league_id for lc in prediction}
        assert ids == {1, 2}


# ---------------------------------------------------------------------------
# Import smoke test via league_registry re-export shim
# ---------------------------------------------------------------------------


class TestLeagueRegistryReExports:
    """Verify new types are importable from the re-export shim."""

    def test_import_from_league_registry(self) -> None:
        from instruments_service.sports.league_registry import (
            DEFAULT_CLASSIFICATION_REGISTRY,
            LEAGUE_CLASSIFICATION_DATA,
            LeagueClassification,
            LeagueClassificationRegistry,
            LeagueClassificationType,
        )

        assert LeagueClassification is not None
        assert LeagueClassificationRegistry is not None
        assert LeagueClassificationType is not None
        assert DEFAULT_CLASSIFICATION_REGISTRY is not None
        assert LEAGUE_CLASSIFICATION_DATA is not None

    def test_import_from_sports_package(self) -> None:
        from instruments_service.sports import (
            DEFAULT_CLASSIFICATION_REGISTRY,
            LeagueClassification,
            LeagueClassificationRegistry,
            LeagueClassificationType,
        )

        assert LeagueClassification is not None
        assert LeagueClassificationRegistry is not None
        assert LeagueClassificationType is not None
        assert DEFAULT_CLASSIFICATION_REGISTRY is not None
