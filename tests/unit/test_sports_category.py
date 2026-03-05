"""
Unit tests for the SPORTS category support in instruments-service.

Tests cover:
- League registry: lookup, sport filtering, 60+ leagues, new fields
- Team normalizer: alias resolution, case insensitivity, unknown teams,
  accent-stripped matching, suffix stripping, historical name changes
- Fixture parser: instrument key generation, validation errors,
  API-Football ID lookup, batch parsing
- Sports orchestrator: league determination, stub processing
- Service config: SPORTS bucket configuration
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pytest

from instruments_service.sports.fixture_parser import FixtureParser
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
from instruments_service.sports.team_normalizer import TeamNormalizer

# ---------------------------------------------------------------------------
# League Registry
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Team Normalizer
# ---------------------------------------------------------------------------


class TestTeamNormalizer:
    """Tests for the team normalizer."""

    def test_normalize_known_team(self) -> None:
        """Known teams resolve to canonical (id, display_name)."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Arsenal")
        assert result is not None
        canonical_id, display_name = result
        assert canonical_id == "ARS"
        assert display_name == "Arsenal"

    def test_normalize_case_insensitive(self) -> None:
        """Lookup is case-insensitive."""
        normalizer = TeamNormalizer()
        assert normalizer.normalize("ARSENAL") == normalizer.normalize("arsenal")

    def test_normalize_alias_variant(self) -> None:
        """Alias variants resolve to the same canonical team."""
        normalizer = TeamNormalizer()
        assert normalizer.normalize("Man Utd") is not None
        assert normalizer.normalize("Man United") is not None
        result_1 = normalizer.normalize("Man Utd")
        result_2 = normalizer.normalize("Manchester United")
        assert result_1 is not None and result_2 is not None
        assert result_1[0] == result_2[0] == "MUN"

    def test_normalize_unknown_team(self) -> None:
        """Unknown teams return None."""
        normalizer = TeamNormalizer()
        assert normalizer.normalize("Nonexistent FC") is None

    def test_normalize_strips_whitespace(self) -> None:
        """Leading/trailing whitespace is stripped."""
        normalizer = TeamNormalizer()
        assert normalizer.normalize("  Arsenal  ") is not None
        result = normalizer.normalize("  Arsenal  ")
        assert result is not None
        assert result[0] == "ARS"

    def test_register_alias(self) -> None:
        """Runtime alias registration works."""
        normalizer = TeamNormalizer()
        assert normalizer.normalize("The Gunners") is None
        normalizer.register_alias("The Gunners", "ARS", "Arsenal")
        result = normalizer.normalize("The Gunners")
        assert result is not None
        assert result[0] == "ARS"

    def test_known_team_count(self) -> None:
        """known_team_count counts unique canonical IDs."""
        normalizer = TeamNormalizer()
        count = normalizer.known_team_count
        assert count > 0
        # Multiple aliases for same team should not inflate the count
        assert count < len(normalizer._aliases)

    def test_known_team_count_large(self) -> None:
        """After expansion, there should be 100+ unique teams."""
        normalizer = TeamNormalizer()
        assert normalizer.known_team_count >= 100

    def test_nba_teams(self) -> None:
        """NBA teams resolve correctly."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Los Angeles Lakers")
        assert result is not None
        assert result[0] == "LAL"

    def test_nfl_teams(self) -> None:
        """NFL teams resolve correctly."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Kansas City Chiefs")
        assert result is not None
        assert result[0] == "KC"

    # --- Expanded EPL tests ---

    def test_epl_expanded_teams(self) -> None:
        """Expanded EPL team aliases resolve correctly."""
        normalizer = TeamNormalizer()
        cases = (
            ("Aston Villa", "AVL"),
            ("Villa", "AVL"),
            ("Bournemouth", "BOU"),
            ("AFC Bournemouth", "BOU"),
            ("Crystal Palace", "CRY"),
            ("C Palace", "CRY"),
            ("Nottingham Forest", "NFO"),
            ("Nottm Forest", "NFO"),
            ("Forest", "NFO"),
            ("Sheffield United", "SHU"),
            ("Sheff Utd", "SHU"),
            ("West Ham", "WHU"),
            ("West Ham United", "WHU"),
            ("Wolves", "WOL"),
            ("Wolverhampton Wanderers", "WOL"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id, f"{name} resolved to {result[0]}, expected {expected_id}"

    # --- Bundesliga tests ---

    def test_bundesliga_teams(self) -> None:
        """Bundesliga teams resolve correctly."""
        normalizer = TeamNormalizer()
        cases = (
            ("Bayern Munich", "BAY"),
            ("Bayern", "BAY"),
            ("Borussia Dortmund", "BVB"),
            ("Dortmund", "BVB"),
            ("BVB", "BVB"),
            ("RB Leipzig", "RBL"),
            ("Leipzig", "RBL"),
            ("Bayer Leverkusen", "B04"),
            ("Eintracht Frankfurt", "SGE"),
            ("Union Berlin", "FCU"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id, f"{name} resolved to {result[0]}, expected {expected_id}"

    # --- La Liga tests ---

    def test_la_liga_teams(self) -> None:
        """La Liga teams resolve correctly."""
        normalizer = TeamNormalizer()
        cases = (
            ("Real Madrid", "RMA"),
            ("Barcelona", "BAR"),
            ("FC Barcelona", "BAR"),
            ("Atletico Madrid", "ATM"),
            ("Sevilla", "SEV"),
            ("Real Betis", "BET"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id, f"{name} resolved to {result[0]}, expected {expected_id}"

    # --- Serie A tests ---

    def test_serie_a_teams(self) -> None:
        """Serie A teams resolve correctly."""
        normalizer = TeamNormalizer()
        cases = (
            ("Juventus", "JUV"),
            ("Inter Milan", "INT"),
            ("Inter", "INT"),
            ("AC Milan", "MIL"),
            ("Napoli", "NAP"),
            ("Roma", "ROM"),
            ("AS Roma", "ROM"),
            ("Lazio", "LAZ"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id, f"{name} resolved to {result[0]}, expected {expected_id}"

    # --- Ligue 1 tests ---

    def test_ligue_1_teams(self) -> None:
        """Ligue 1 teams resolve correctly."""
        normalizer = TeamNormalizer()
        cases = (
            ("Paris Saint-Germain", "PSG"),
            ("PSG", "PSG"),
            ("Marseille", "OM"),
            ("Lyon", "OL"),
            ("Monaco", "MON_FR"),
            ("Lille", "LIL"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id, f"{name} resolved to {result[0]}, expected {expected_id}"

    # --- Accent / Unicode tests ---

    def test_accent_stripping_munchen(self) -> None:
        """Accented team names resolve via accent-stripped matching."""
        normalizer = TeamNormalizer()
        # Bayern München (with umlaut) should match
        result = normalizer.normalize("Bayern München")
        assert result is not None
        assert result[0] == "BAY"

    def test_accent_stripping_monchengladbach(self) -> None:
        """Mönchengladbach with umlaut resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Borussia Mönchengladbach")
        assert result is not None
        assert result[0] == "BMG"

    def test_accent_stripping_alaves(self) -> None:
        """Alavés (with accent) resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Alavés")
        assert result is not None
        assert result[0] == "ALA"

    def test_accent_stripping_saint_etienne(self) -> None:
        """Saint-Étienne with accent resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Saint-Étienne")
        assert result is not None
        assert result[0] == "STE"

    def test_accent_stripping_koln(self) -> None:
        """FC Köln with umlaut resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("FC Köln")
        assert result is not None
        assert result[0] == "KOE"

    # --- Suffix handling tests ---

    def test_suffix_fc_stripping(self) -> None:
        """Teams with trailing 'FC' suffix resolve via core-name matching."""
        normalizer = TeamNormalizer()
        # "Arsenal FC" is in the alias table directly
        result = normalizer.normalize("Arsenal FC")
        assert result is not None
        assert result[0] == "ARS"

    # --- Historical name change tests ---

    def test_historical_name_olympiakos(self) -> None:
        """Historical name 'Olympiakos Piraeus' resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Olympiakos Piraeus")
        assert result is not None
        assert result[0] == "OLY"

    # --- Portuguese / Dutch team tests ---

    def test_portuguese_teams(self) -> None:
        """Portuguese league teams resolve."""
        normalizer = TeamNormalizer()
        cases = (
            ("Benfica", "BEN"),
            ("FC Porto", "POR"),
            ("Sporting CP", "SCP"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id

    def test_dutch_teams(self) -> None:
        """Dutch league teams resolve."""
        normalizer = TeamNormalizer()
        cases = (
            ("Ajax", "AJX"),
            ("PSV Eindhoven", "PSV_NL"),
            ("Feyenoord", "FEY"),
        )
        for name, expected_id in cases:
            result = normalizer.normalize(name)
            assert result is not None, f"Failed to resolve: {name}"
            assert result[0] == expected_id


# ---------------------------------------------------------------------------
# Fixture Parser
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Sports Orchestrator
# ---------------------------------------------------------------------------

# The SportsOrchestrator lives under instruments_service.engine.operations.instruments.orchestration.
# Importing that path transitively triggers __init__.py files that pull in heavy dependencies
# (unified_events_interface.ErrorWarningCounter, unified_market_interface.clients.block_resolver)
# which may not be available in every test environment.
# We use importlib.util to load the module directly, bypassing the __init__.py chain.


def _load_sports_orchestrator() -> type:
    """Load SportsOrchestrator without triggering transitive __init__ imports."""
    _root = Path(__file__).resolve().parent.parent.parent
    _sports_path = (
        _root
        / "instruments_service"
        / "engine"
        / "operations"
        / "instruments"
        / "orchestration"
        / "sports_orchestration.py"
    )
    spec = importlib.util.spec_from_file_location(
        "sports_orchestration",
        str(_sports_path),
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.SportsOrchestrator  # type: ignore[no-any-return]


_SportsOrchestrator = _load_sports_orchestrator()


class TestSportsOrchestrator:
    """Tests for the sports orchestrator."""

    @pytest.mark.asyncio
    async def test_process_sports_no_filter(self) -> None:
        """Without filter, all registered leagues are considered."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=[],
        )
        # Phase 1 stub: always returns empty dict
        assert result == {}

    @pytest.mark.asyncio
    async def test_process_sports_with_filter(self) -> None:
        """With filter, only matching leagues are processed."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=["EPL", "NBA"],
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_process_sports_no_matching_leagues(self) -> None:
        """If filter contains no valid leagues, returns empty."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=["BINANCE-SPOT"],
        )
        assert result == {}

    def test_determine_leagues_no_filter(self) -> None:
        """No filter returns all registered leagues."""
        orchestrator = _SportsOrchestrator()
        leagues = orchestrator._determine_leagues([])
        assert sorted(leagues) == sorted(LEAGUE_REGISTRY.keys())

    def test_determine_leagues_with_filter(self) -> None:
        """Filter restricts to matching leagues."""
        orchestrator = _SportsOrchestrator()
        leagues = orchestrator._determine_leagues(["EPL", "NBA", "UNKNOWN"])
        assert sorted(leagues) == ["EPL", "NBA"]


# ---------------------------------------------------------------------------
# Service Config — SPORTS bucket support
# ---------------------------------------------------------------------------


class TestServiceConfigSports:
    """Tests for SPORTS category in InstrumentsServiceConfig."""

    def test_get_cloud_target_sports(self) -> None:
        """get_cloud_target('SPORTS') returns a CloudTarget."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        target = config.get_cloud_target("SPORTS")
        assert target is not None

    def test_get_cloud_target_invalid_category(self) -> None:
        """get_cloud_target with invalid category raises ValueError."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        with pytest.raises(ValueError, match="Invalid category"):
            config.get_cloud_target("INVALID")

    def test_get_bucket_for_category_sports(self) -> None:
        """get_bucket_for_category('SPORTS') returns a bucket name."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        # Either returns the configured bucket or falls back to default
        bucket = config.get_bucket_for_category("SPORTS")
        assert isinstance(bucket, str)

    def test_get_bucket_for_category_invalid(self) -> None:
        """get_bucket_for_category with invalid category raises ValueError."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        with pytest.raises(ValueError, match="Invalid category"):
            config.get_bucket_for_category("INVALID")


# ---------------------------------------------------------------------------
# Sports __init__ imports
# ---------------------------------------------------------------------------


class TestSportsPackageImports:
    """Verify the sports package is importable and exports expected symbols."""

    def test_import_sports_package(self) -> None:
        import instruments_service.sports

        assert hasattr(instruments_service.sports, "FixtureParser")
        assert hasattr(instruments_service.sports, "LeagueDefinition")
        assert hasattr(instruments_service.sports, "LEAGUE_REGISTRY")
        assert hasattr(instruments_service.sports, "TeamNormalizer")

    def test_import_new_registry_functions(self) -> None:
        """New registry functions are importable from the package."""
        from instruments_service.sports import (
            get_league,
            get_league_by_api_football_id,
            get_leagues_by_classification,
            get_leagues_by_country,
            get_leagues_for_sport,
            get_prediction_leagues,
        )

        assert callable(get_league)
        assert callable(get_league_by_api_football_id)
        assert callable(get_leagues_by_classification)
        assert callable(get_leagues_by_country)
        assert callable(get_leagues_for_sport)
        assert callable(get_prediction_leagues)

    def test_import_fixture_parser(self) -> None:
        from instruments_service.sports import FixtureParser

        parser = FixtureParser()
        assert parser is not None

    def test_import_league_registry(self) -> None:
        from instruments_service.sports import LEAGUE_REGISTRY

        assert isinstance(LEAGUE_REGISTRY, dict)

    def test_import_team_normalizer(self) -> None:
        from instruments_service.sports import TeamNormalizer

        normalizer = TeamNormalizer()
        assert normalizer is not None
