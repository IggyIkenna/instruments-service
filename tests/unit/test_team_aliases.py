"""Unit tests for the team alias resolution module.

Tests cover:
- TeamAliasResolver construction and indexing
- resolve_team: provider-specific ID lookup
- get_mapping: canonical team lookup
- get_aliases: all known names for a team
- find_by_name: fuzzy name lookup across aliases
- TeamNameHistory: frozen model construction
- load_team_mappings_from_dict: dict-to-TeamMapping conversion
"""

from __future__ import annotations

import pytest
from unified_api_contracts.unified_api_contracts_external.sports.canonical.mappings import TeamMapping

from instruments_service.sports.team_aliases import (
    TeamAliasResolver,
    TeamNameHistory,
    load_team_mappings_from_dict,
)

# ---------------------------------------------------------------------------
# Test fixtures — realistic cross-provider mock data
# ---------------------------------------------------------------------------

_MOCK_MAPPINGS_DATA: list[dict[str, str | int | None | list[str]]] = [
    {
        "canonical_team_id": "ARS",
        "display_name": "Arsenal",
        "api_football_id": 42,
        "footystats_id": "arsenal-fc",
        "understat_name": "Arsenal",
        "soccer_football_id": 1001,
        "betfair_id": "bf_ars",
        "pinnacle_id": 7001,
        "odds_api_key": "arsenal",
        "aliases": ["Arsenal FC", "AFC", "The Gunners"],
    },
    {
        "canonical_team_id": "MUN",
        "display_name": "Manchester United",
        "api_football_id": 33,
        "footystats_id": "manchester-united",
        "understat_name": "Manchester United",
        "soccer_football_id": 1002,
        "betfair_id": "bf_mun",
        "pinnacle_id": 7002,
        "odds_api_key": "manchester_united",
        "aliases": ["Man Utd", "Man United", "MUFC", "Manchester Utd"],
    },
    {
        "canonical_team_id": "BAY",
        "display_name": "Bayern Munich",
        "api_football_id": 157,
        "footystats_id": "bayern-munich",
        "understat_name": "Bayern Munich",
        "soccer_football_id": None,
        "betfair_id": None,
        "pinnacle_id": None,
        "odds_api_key": "bayern_munich",
        "aliases": ["Bayern", "FC Bayern Munich", "Bayern München"],
    },
    {
        "canonical_team_id": "BAR",
        "display_name": "Barcelona",
        "api_football_id": 529,
        "footystats_id": "barcelona-fc",
        "understat_name": None,
        "soccer_football_id": 1004,
        "betfair_id": "bf_bar",
        "pinnacle_id": 7004,
        "odds_api_key": "barcelona",
        "aliases": ["FC Barcelona", "Barca"],
    },
]


@pytest.fixture()
def mock_mappings() -> list[TeamMapping]:
    """Build TeamMapping objects from the mock data."""
    return load_team_mappings_from_dict(_MOCK_MAPPINGS_DATA)  # type: ignore[arg-type]


@pytest.fixture()
def resolver(mock_mappings: list[TeamMapping]) -> TeamAliasResolver:
    """Build a TeamAliasResolver from mock data."""
    return TeamAliasResolver(mock_mappings)


# ---------------------------------------------------------------------------
# TeamAliasResolver — construction
# ---------------------------------------------------------------------------


class TestTeamAliasResolverConstruction:
    """Tests for resolver initialisation and indexing."""

    def test_mapping_count(self, resolver: TeamAliasResolver) -> None:
        """Resolver indexes the correct number of teams."""
        assert resolver.mapping_count == 4

    def test_empty_resolver(self) -> None:
        """Resolver works with an empty mapping list."""
        r = TeamAliasResolver([])
        assert r.mapping_count == 0
        assert r.resolve_team("api_football", "42") is None


# ---------------------------------------------------------------------------
# resolve_team — provider-specific ID lookup
# ---------------------------------------------------------------------------


class TestResolveTeam:
    """Tests for provider-based team resolution."""

    def test_resolve_by_api_football_id(self, resolver: TeamAliasResolver) -> None:
        """Resolve Arsenal by API-Football ID."""
        assert resolver.resolve_team("api_football", "42") == "ARS"

    def test_resolve_by_api_football_id_int(self, resolver: TeamAliasResolver) -> None:
        """Resolve Arsenal by API-Football ID passed as int."""
        assert resolver.resolve_team("api_football", 42) == "ARS"

    def test_resolve_by_footystats_id(self, resolver: TeamAliasResolver) -> None:
        """Resolve Manchester United by FootyStats ID."""
        assert resolver.resolve_team("footystats", "manchester-united") == "MUN"

    def test_resolve_by_understat_name(self, resolver: TeamAliasResolver) -> None:
        """Resolve Bayern by Understat name."""
        assert resolver.resolve_team("understat", "Bayern Munich") == "BAY"

    def test_resolve_by_soccer_football_id(self, resolver: TeamAliasResolver) -> None:
        """Resolve Barcelona by Soccer Football ID."""
        assert resolver.resolve_team("soccer_football", "1004") == "BAR"

    def test_resolve_by_betfair_id(self, resolver: TeamAliasResolver) -> None:
        """Resolve Arsenal by Betfair ID."""
        assert resolver.resolve_team("betfair", "bf_ars") == "ARS"

    def test_resolve_by_pinnacle_id(self, resolver: TeamAliasResolver) -> None:
        """Resolve Man Utd by Pinnacle ID."""
        assert resolver.resolve_team("pinnacle", "7002") == "MUN"

    def test_resolve_by_odds_api_key(self, resolver: TeamAliasResolver) -> None:
        """Resolve Barcelona by Odds API key."""
        assert resolver.resolve_team("odds_api", "barcelona") == "BAR"

    def test_resolve_unknown_provider_id(self, resolver: TeamAliasResolver) -> None:
        """Unknown provider ID returns None."""
        assert resolver.resolve_team("api_football", "99999") is None

    def test_resolve_unknown_provider(self, resolver: TeamAliasResolver) -> None:
        """Unknown provider name returns None."""
        assert resolver.resolve_team("unknown_provider", "42") is None

    def test_resolve_none_field_not_indexed(self, resolver: TeamAliasResolver) -> None:
        """Provider fields set to None are not indexed."""
        # Bayern has soccer_football_id=None — should not match "None"
        assert resolver.resolve_team("soccer_football", "None") is None


# ---------------------------------------------------------------------------
# get_mapping — canonical team lookup
# ---------------------------------------------------------------------------


class TestGetMapping:
    """Tests for full-mapping retrieval by canonical ID."""

    def test_get_mapping_known(self, resolver: TeamAliasResolver) -> None:
        """Known canonical ID returns the TeamMapping."""
        mapping = resolver.get_mapping("ARS")
        assert mapping is not None
        assert mapping.canonical_team_id == "ARS"
        assert mapping.display_name == "Arsenal"
        assert mapping.api_football_id == 42

    def test_get_mapping_unknown(self, resolver: TeamAliasResolver) -> None:
        """Unknown canonical ID returns None."""
        assert resolver.get_mapping("UNKNOWN") is None

    def test_get_mapping_all_fields(self, resolver: TeamAliasResolver) -> None:
        """All provider fields are preserved."""
        mapping = resolver.get_mapping("MUN")
        assert mapping is not None
        assert mapping.footystats_id == "manchester-united"
        assert mapping.odds_api_key == "manchester_united"
        assert mapping.pinnacle_id == 7002

    def test_get_mapping_none_fields(self, resolver: TeamAliasResolver) -> None:
        """None-valued provider fields are preserved."""
        mapping = resolver.get_mapping("BAY")
        assert mapping is not None
        assert mapping.soccer_football_id is None
        assert mapping.betfair_id is None
        assert mapping.pinnacle_id is None


# ---------------------------------------------------------------------------
# get_aliases — all known names for a team
# ---------------------------------------------------------------------------


class TestGetAliases:
    """Tests for alias enumeration."""

    def test_get_aliases_known(self, resolver: TeamAliasResolver) -> None:
        """Known team returns display_name + aliases."""
        aliases = resolver.get_aliases("ARS")
        assert "Arsenal" in aliases
        assert "Arsenal FC" in aliases
        assert "AFC" in aliases
        assert "The Gunners" in aliases

    def test_get_aliases_count(self, resolver: TeamAliasResolver) -> None:
        """Alias count = display_name (1) + explicit aliases."""
        aliases = resolver.get_aliases("ARS")
        # Arsenal: display_name + 3 aliases = 4
        assert len(aliases) == 4

    def test_get_aliases_returns_frozenset(self, resolver: TeamAliasResolver) -> None:
        """Return type is frozenset."""
        aliases = resolver.get_aliases("ARS")
        assert isinstance(aliases, frozenset)

    def test_get_aliases_unknown(self, resolver: TeamAliasResolver) -> None:
        """Unknown team returns empty frozenset."""
        aliases = resolver.get_aliases("UNKNOWN")
        assert aliases == frozenset()

    def test_get_aliases_man_utd(self, resolver: TeamAliasResolver) -> None:
        """Man Utd has all expected variants."""
        aliases = resolver.get_aliases("MUN")
        assert "Manchester United" in aliases
        assert "Man Utd" in aliases
        assert "Man United" in aliases
        assert "MUFC" in aliases
        assert "Manchester Utd" in aliases


# ---------------------------------------------------------------------------
# find_by_name — fuzzy name lookup across aliases
# ---------------------------------------------------------------------------


class TestFindByName:
    """Tests for fuzzy name lookup."""

    def test_find_by_display_name(self, resolver: TeamAliasResolver) -> None:
        """Exact display name matches."""
        mapping = resolver.find_by_name("Arsenal")
        assert mapping is not None
        assert mapping.canonical_team_id == "ARS"

    def test_find_by_alias(self, resolver: TeamAliasResolver) -> None:
        """Alias name matches."""
        mapping = resolver.find_by_name("MUFC")
        assert mapping is not None
        assert mapping.canonical_team_id == "MUN"

    def test_find_by_name_case_insensitive(self, resolver: TeamAliasResolver) -> None:
        """Lookup is case-insensitive."""
        mapping = resolver.find_by_name("arsenal")
        assert mapping is not None
        assert mapping.canonical_team_id == "ARS"

    def test_find_by_name_accent_normalised(self, resolver: TeamAliasResolver) -> None:
        """Accented names are normalised (Bayern München -> Bayern Munchen)."""
        mapping = resolver.find_by_name("Bayern München")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAY"

    def test_find_by_name_unknown(self, resolver: TeamAliasResolver) -> None:
        """Unknown name returns None."""
        assert resolver.find_by_name("Nonexistent FC") is None

    def test_find_by_name_barca(self, resolver: TeamAliasResolver) -> None:
        """Short alias 'Barca' matches Barcelona."""
        mapping = resolver.find_by_name("Barca")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAR"

    def test_find_by_name_fc_barcelona(self, resolver: TeamAliasResolver) -> None:
        """Full name 'FC Barcelona' matches."""
        mapping = resolver.find_by_name("FC Barcelona")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAR"

    def test_find_by_name_whitespace(self, resolver: TeamAliasResolver) -> None:
        """Leading/trailing whitespace is stripped."""
        mapping = resolver.find_by_name("  Arsenal  ")
        assert mapping is not None
        assert mapping.canonical_team_id == "ARS"


# ---------------------------------------------------------------------------
# TeamNameHistory — frozen model
# ---------------------------------------------------------------------------


class TestTeamNameHistory:
    """Tests for the TeamNameHistory model."""

    def test_construction(self) -> None:
        """TeamNameHistory can be constructed with all fields."""
        history = TeamNameHistory(
            canonical_team_id="BAY",
            old_name="FC Bayern München",
            new_name="Bayern Munich",
            effective_date="2020-01-01",
            reason="English name standardisation",
        )
        assert history.canonical_team_id == "BAY"
        assert history.old_name == "FC Bayern München"
        assert history.new_name == "Bayern Munich"
        assert history.effective_date == "2020-01-01"
        assert history.reason == "English name standardisation"

    def test_optional_reason(self) -> None:
        """Reason field is optional."""
        history = TeamNameHistory(
            canonical_team_id="BAY",
            old_name="FC Bayern München",
            new_name="Bayern Munich",
            effective_date="2020-01-01",
        )
        assert history.reason is None

    def test_frozen(self) -> None:
        """TeamNameHistory is immutable (frozen)."""
        from pydantic import ValidationError

        history = TeamNameHistory(
            canonical_team_id="BAY",
            old_name="FC Bayern München",
            new_name="Bayern Munich",
            effective_date="2020-01-01",
        )
        with pytest.raises(ValidationError):
            history.old_name = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# load_team_mappings_from_dict
# ---------------------------------------------------------------------------


class TestLoadTeamMappingsFromDict:
    """Tests for the dict-to-TeamMapping loader."""

    def test_load_basic(self) -> None:
        """Basic loading produces valid TeamMapping objects."""
        data: list[dict[str, str | int | None | list[str]]] = [
            {
                "canonical_team_id": "TEST",
                "display_name": "Test Team",
                "aliases": ["TT", "Team T"],
            },
        ]
        mappings = load_team_mappings_from_dict(data)  # type: ignore[arg-type]
        assert len(mappings) == 1
        assert mappings[0].canonical_team_id == "TEST"
        assert mappings[0].display_name == "Test Team"
        assert "TT" in mappings[0].aliases
        assert "Team T" in mappings[0].aliases

    def test_load_empty(self) -> None:
        """Loading empty list returns empty list."""
        assert load_team_mappings_from_dict([]) == []

    def test_load_without_aliases(self) -> None:
        """Mappings without explicit aliases get empty frozenset."""
        data: list[dict[str, str | int | None]] = [
            {
                "canonical_team_id": "X",
                "display_name": "Team X",
            },
        ]
        mappings = load_team_mappings_from_dict(data)
        assert len(mappings) == 1
        assert mappings[0].aliases == frozenset()

    def test_load_multiple(self) -> None:
        """Loading multiple dicts produces multiple TeamMapping objects."""
        mappings = load_team_mappings_from_dict(_MOCK_MAPPINGS_DATA)  # type: ignore[arg-type]
        assert len(mappings) == 4
        ids = {m.canonical_team_id for m in mappings}
        assert ids == {"ARS", "MUN", "BAY", "BAR"}

    def test_load_preserves_provider_ids(self) -> None:
        """Provider IDs are correctly set on loaded mappings."""
        mappings = load_team_mappings_from_dict(_MOCK_MAPPINGS_DATA)  # type: ignore[arg-type]
        ars = next(m for m in mappings if m.canonical_team_id == "ARS")
        assert ars.api_football_id == 42
        assert ars.footystats_id == "arsenal-fc"
        assert ars.pinnacle_id == 7001


# ---------------------------------------------------------------------------
# Integration: resolve across all four mock teams
# ---------------------------------------------------------------------------


class TestResolverIntegration:
    """End-to-end tests across all mock teams."""

    def test_all_teams_resolvable_by_api_football(self, resolver: TeamAliasResolver) -> None:
        """All teams with api_football_id can be resolved."""
        assert resolver.resolve_team("api_football", 42) == "ARS"
        assert resolver.resolve_team("api_football", 33) == "MUN"
        assert resolver.resolve_team("api_football", 157) == "BAY"
        assert resolver.resolve_team("api_football", 529) == "BAR"

    def test_all_teams_findable_by_display_name(self, resolver: TeamAliasResolver) -> None:
        """All teams can be found by their display name."""
        for expected_id, name in [
            ("ARS", "Arsenal"),
            ("MUN", "Manchester United"),
            ("BAY", "Bayern Munich"),
            ("BAR", "Barcelona"),
        ]:
            mapping = resolver.find_by_name(name)
            assert mapping is not None, f"Could not find {name}"
            assert mapping.canonical_team_id == expected_id

    def test_resolve_then_get_mapping_round_trip(self, resolver: TeamAliasResolver) -> None:
        """Resolve by provider ID, then get full mapping."""
        cid = resolver.resolve_team("api_football", 42)
        assert cid == "ARS"
        mapping = resolver.get_mapping(cid)
        assert mapping is not None
        assert mapping.display_name == "Arsenal"
        assert mapping.odds_api_key == "arsenal"
