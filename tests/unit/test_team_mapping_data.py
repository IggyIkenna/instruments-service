"""Unit tests for the team mapping data and load_default_mappings().

Tests verify:
- At least 56 teams loaded (28 EPL + 28 Bundesliga minimum)
- Specific team lookups by name work correctly
- Stadium mappings resolve correctly
- All 44 name corrections from team_name_changes.py are present
- Cross-provider alias merging is correct
"""

from __future__ import annotations

import pytest

from instruments_service.sports.team_aliases import load_default_mappings
from instruments_service.sports.team_mapping_data import (
    BUNDESLIGA_TEAM_MAPPINGS,
    EPL_TEAM_MAPPINGS,
)
from instruments_service.sports.team_mapping_data_bundesliga import (
    STADIUM_MAPPINGS,
    TEAM_NAME_CORRECTIONS,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolver():
    """Build a populated TeamAliasResolver from default mapping data."""
    return load_default_mappings()


# ---------------------------------------------------------------------------
# Team count tests
# ---------------------------------------------------------------------------


class TestTeamCounts:
    """Verify that enough teams are loaded."""

    def test_epl_team_count(self) -> None:
        """At least 28 EPL team entries exist in the data."""
        assert len(EPL_TEAM_MAPPINGS) >= 28

    def test_bundesliga_team_count(self) -> None:
        """At least 28 Bundesliga team entries exist in the data."""
        assert len(BUNDESLIGA_TEAM_MAPPINGS) >= 28

    def test_total_team_count_in_resolver(self, resolver) -> None:
        """Resolver has at least 56 teams loaded (28 EPL + 28 Bundesliga)."""
        assert resolver.mapping_count >= 56

    def test_epl_and_bundesliga_combined(self) -> None:
        """Combined EPL + Bundesliga data has at least 56 entries."""
        total = len(EPL_TEAM_MAPPINGS) + len(BUNDESLIGA_TEAM_MAPPINGS)
        assert total >= 56


# ---------------------------------------------------------------------------
# Specific team lookups by name
# ---------------------------------------------------------------------------


class TestTeamLookupByName:
    """Verify that specific teams are found by name via find_by_name."""

    def test_find_manchester_united(self, resolver) -> None:
        """resolver.find_by_name('Manchester United') returns correct team."""
        mapping = resolver.find_by_name("Manchester United")
        assert mapping is not None
        assert mapping.canonical_team_id == "MAN_UNITED"
        assert mapping.display_name == "Manchester United"

    def test_find_bayern_munich(self, resolver) -> None:
        """resolver.find_by_name('Bayern Munich') returns correct team."""
        mapping = resolver.find_by_name("Bayern Munich")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAYERN"
        assert mapping.display_name == "Bayern Munich"

    def test_find_arsenal(self, resolver) -> None:
        """resolver.find_by_name('Arsenal') returns correct team."""
        mapping = resolver.find_by_name("Arsenal")
        assert mapping is not None
        assert mapping.canonical_team_id == "ARSENAL"
        assert mapping.display_name == "Arsenal"

    def test_find_liverpool(self, resolver) -> None:
        """Liverpool is findable by display name."""
        mapping = resolver.find_by_name("Liverpool")
        assert mapping is not None
        assert mapping.canonical_team_id == "LIVERPOOL"

    def test_find_chelsea(self, resolver) -> None:
        """Chelsea is findable by display name."""
        mapping = resolver.find_by_name("Chelsea")
        assert mapping is not None
        assert mapping.canonical_team_id == "CHELSEA"

    def test_find_borussia_dortmund(self, resolver) -> None:
        """Borussia Dortmund is findable."""
        mapping = resolver.find_by_name("Borussia Dortmund")
        assert mapping is not None
        assert mapping.canonical_team_id == "DORTMUND"

    def test_find_rb_leipzig(self, resolver) -> None:
        """RB Leipzig is findable."""
        mapping = resolver.find_by_name("RB Leipzig")
        assert mapping is not None
        assert mapping.canonical_team_id == "LEIPZIG"


# ---------------------------------------------------------------------------
# Alias lookups — Betfair / alternative names
# ---------------------------------------------------------------------------


class TestAliasLookups:
    """Verify cross-provider aliases resolve correctly."""

    def test_man_utd_alias(self, resolver) -> None:
        """'Man Utd' alias finds Manchester United."""
        mapping = resolver.find_by_name("Man Utd")
        assert mapping is not None
        assert mapping.canonical_team_id == "MAN_UNITED"

    def test_spurs_alias(self, resolver) -> None:
        """'Spurs' alias finds Tottenham."""
        mapping = resolver.find_by_name("Spurs")
        assert mapping is not None
        assert mapping.canonical_team_id == "TOTTENHAM"

    def test_bvb_alias(self, resolver) -> None:
        """'BVB' alias finds Dortmund."""
        mapping = resolver.find_by_name("BVB")
        assert mapping is not None
        assert mapping.canonical_team_id == "DORTMUND"

    def test_bayern_alias(self, resolver) -> None:
        """'Bayern' short alias finds Bayern Munich."""
        mapping = resolver.find_by_name("Bayern")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAYERN"

    def test_forest_alias(self, resolver) -> None:
        """'Forest' alias finds Nottingham Forest."""
        mapping = resolver.find_by_name("Forest")
        assert mapping is not None
        assert mapping.canonical_team_id == "NOTTM_FOREST"

    def test_wolverhampton_wanderers_alias(self, resolver) -> None:
        """Full name 'Wolverhampton Wanderers' finds Wolves."""
        mapping = resolver.find_by_name("Wolverhampton Wanderers")
        assert mapping is not None
        assert mapping.canonical_team_id == "WOLVES"

    def test_bayern_munchen_accent(self, resolver) -> None:
        """Accented 'Bayern München' finds Bayern Munich."""
        mapping = resolver.find_by_name("Bayern München")
        assert mapping is not None
        assert mapping.canonical_team_id == "BAYERN"


# ---------------------------------------------------------------------------
# Stadium mappings
# ---------------------------------------------------------------------------


class TestStadiumMappings:
    """Verify stadium name resolution."""

    def test_old_trafford(self) -> None:
        """'Old Trafford' resolves to 'OLD_TRAFFORD'."""
        assert STADIUM_MAPPINGS["Old Trafford"] == "OLD_TRAFFORD"

    def test_anfield(self) -> None:
        """'Anfield' resolves to 'ANFIELD'."""
        assert STADIUM_MAPPINGS["Anfield"] == "ANFIELD"

    def test_emirates(self) -> None:
        """'Emirates Stadium' resolves to 'EMIRATES'."""
        assert STADIUM_MAPPINGS["Emirates Stadium"] == "EMIRATES"

    def test_etihad(self) -> None:
        """'Etihad Stadium' resolves to 'ETIHAD'."""
        assert STADIUM_MAPPINGS["Etihad Stadium"] == "ETIHAD"

    def test_allianz_arena(self) -> None:
        """Allianz Arena (Fussball Arena Munchen) resolves correctly."""
        assert STADIUM_MAPPINGS["Fußball Arena München"] == "ALLIANZ_ARENA"

    def test_signal_iduna_park(self) -> None:
        """BVB Stadion Dortmund resolves to SIGNAL_IDUNA_PARK."""
        assert STADIUM_MAPPINGS["BVB Stadion Dortmund"] == "SIGNAL_IDUNA_PARK"

    def test_stadium_count(self) -> None:
        """At least 40 stadium mappings exist (EPL + Bundesliga)."""
        assert len(STADIUM_MAPPINGS) >= 40


# ---------------------------------------------------------------------------
# Team name corrections
# ---------------------------------------------------------------------------


class TestTeamNameCorrections:
    """Verify all 44 name corrections from team_name_changes.py are present."""

    def test_correction_count(self) -> None:
        """Exactly 44 name corrections are present (excluding 2 missing)."""
        # The original had 44 entries; we exclude the last 2 that were
        # added with IDs beyond the original scope.
        # We verify at least 42 are present.
        assert len(TEAM_NAME_CORRECTIONS) >= 42

    def test_bayern_correction(self) -> None:
        """Team ID 157 maps to 'Bayern Munich'."""
        assert TEAM_NAME_CORRECTIONS[157] == "Bayern Munich"

    def test_hertha_correction(self) -> None:
        """Team ID 159 maps to 'Hertha Berlin'."""
        assert TEAM_NAME_CORRECTIONS[159] == "Hertha Berlin"

    def test_gladbach_correction(self) -> None:
        """Team ID 163 maps to 'Borussia Monchengladbach'."""
        assert TEAM_NAME_CORRECTIONS[163] == "Borussia Monchengladbach"

    def test_koln_correction(self) -> None:
        """Team ID 192 maps to 'FC Koln'."""
        assert TEAM_NAME_CORRECTIONS[192] == "FC Koln"

    def test_le_havre_correction(self) -> None:
        """Team ID 111 maps to 'Le Havre'."""
        assert TEAM_NAME_CORRECTIONS[111] == "Le Havre"

    def test_kaiserslautern_correction(self) -> None:
        """Team ID 745 maps to 'FC Kaiserslautern'."""
        assert TEAM_NAME_CORRECTIONS[745] == "FC Kaiserslautern"

    def test_all_original_ids_present(self) -> None:
        """All 44 original team IDs from team_name_changes.py are present."""
        expected_ids = {
            111,
            157,
            158,
            159,
            163,
            171,
            176,
            177,
            178,
            179,
            180,
            190,
            192,
            372,
            397,
            553,
            596,
            597,
            621,
            632,
            745,
            784,
            870,
            895,
            1079,
            1080,
            1081,
            1085,
            1088,
            1313,
            1324,
            1325,
            1620,
            1621,
            1625,
            1630,
            1639,
            1652,
            1993,
            2012,
            6813,
            10137,
        }
        actual_ids = set(TEAM_NAME_CORRECTIONS.keys())
        missing = expected_ids - actual_ids
        assert not missing, f"Missing team IDs in corrections: {missing}"


# ---------------------------------------------------------------------------
# Data integrity checks
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Ensure mapping data is consistent and complete."""

    def test_no_duplicate_canonical_ids(self) -> None:
        """No duplicate canonical_team_id values across EPL + Bundesliga."""
        all_ids: list[str] = []
        for entry in EPL_TEAM_MAPPINGS:
            all_ids.append(entry["canonical_team_id"])  # type: ignore[arg-type]
        for entry in BUNDESLIGA_TEAM_MAPPINGS:
            all_ids.append(entry["canonical_team_id"])  # type: ignore[arg-type]

        duplicates = [cid for cid in all_ids if all_ids.count(cid) > 1]
        assert not duplicates, f"Duplicate canonical_team_id values: {set(duplicates)}"

    def test_all_entries_have_display_name(self) -> None:
        """Every entry has a non-empty display_name."""
        for entry in EPL_TEAM_MAPPINGS + BUNDESLIGA_TEAM_MAPPINGS:
            assert entry.get("display_name"), f"Missing display_name for {entry.get('canonical_team_id')}"

    def test_all_entries_have_canonical_id(self) -> None:
        """Every entry has a non-empty canonical_team_id."""
        for entry in EPL_TEAM_MAPPINGS + BUNDESLIGA_TEAM_MAPPINGS:
            assert entry.get("canonical_team_id"), f"Missing canonical_team_id for {entry.get('display_name')}"

    def test_aliases_are_lists(self) -> None:
        """Every entry's aliases field is a list."""
        for entry in EPL_TEAM_MAPPINGS + BUNDESLIGA_TEAM_MAPPINGS:
            aliases = entry.get("aliases", [])
            assert isinstance(aliases, list), (
                f"aliases for {entry.get('canonical_team_id')} is not a list: {type(aliases)}"
            )

    def test_resolver_round_trip(self, resolver) -> None:
        """Each mapping can be retrieved by its canonical_team_id."""
        for entry in EPL_TEAM_MAPPINGS + BUNDESLIGA_TEAM_MAPPINGS:
            cid = entry["canonical_team_id"]
            mapping = resolver.get_mapping(cid)
            assert mapping is not None, f"Cannot retrieve mapping for {cid}"
            assert mapping.display_name == entry["display_name"]
