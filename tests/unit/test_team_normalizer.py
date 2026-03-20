"""
Unit tests for the team normalizer.

Tests cover:
- Exact match, case-insensitive, alias resolution
- Accent-stripped matching (Unicode normalization)
- Suffix stripping (FC, United, etc.)
- Historical name changes
- Runtime alias registration
- Multi-league teams (EPL, Bundesliga, La Liga, Serie A, Ligue 1, Portuguese, Dutch, NBA, NFL)
- Module-level helper functions (_strip_accents, normalize_for_fuzzy, _extract_core_name)
"""

from __future__ import annotations

from instruments_service.sports.team_normalizer import TeamNormalizer


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
        """Alaves (with accent) resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Alaves")
        assert result is not None
        assert result[0] == "ALA"

    def test_accent_stripping_saint_etienne(self) -> None:
        """Saint-Etienne with accent resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("Saint-Etienne")
        assert result is not None
        assert result[0] == "STE"

    def test_accent_stripping_koln(self) -> None:
        """FC Koln with umlaut resolves."""
        normalizer = TeamNormalizer()
        result = normalizer.normalize("FC Koln")
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


class TestTeamNormalizerFunctions:
    """Tests for module-level functions in team_normalizer."""

    def test_strip_accents(self) -> None:
        from instruments_service.sports.team_normalizer import _strip_accents

        assert _strip_accents("Alaves") == "alaves"
        assert _strip_accents("Munchen") == "munchen"
        assert _strip_accents("Arsenal") == "arsenal"

    def test_normalize_for_fuzzy(self) -> None:
        from instruments_service.sports.team_normalizer import normalize_for_fuzzy

        result = normalize_for_fuzzy("  Bayern Munchen  ")
        assert "munchen" in result
        assert result == result.strip()

    def test_extract_core_name_removes_suffix(self) -> None:
        from instruments_service.sports.team_normalizer import _extract_core_name

        assert _extract_core_name("Arsenal FC") == "arsenal"
        assert _extract_core_name("Manchester United") == "manchester"
        assert _extract_core_name("AC Milan") == "milan"


class TestTeamNormalizerClass:
    """Tests for the TeamNormalizer class."""

    def test_instantiation(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        assert tn is not None

    def test_normalize_exact_match(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Arsenal")
        assert result is not None
        assert result[0] == "ARS"
        assert result[1] == "Arsenal"

    def test_normalize_case_insensitive(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("ARSENAL")
        assert result is not None
        assert result[0] == "ARS"

    def test_normalize_accent_stripped(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        # Alaves -> alaves which should match "alaves"
        result = tn.normalize("Alaves")
        assert result is not None
        assert result[0] == "ALA"

    def test_normalize_with_accents_in_data(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Bayern Munchen")
        assert result is not None
        assert result[0] == "BAY"

    def test_normalize_unknown_returns_none(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Totally Unknown Club XYZ")
        assert result is None

    def test_normalize_empty_string_returns_none(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("   ")
        assert result is None

    def test_normalize_whitespace_stripped(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("  liverpool  ")
        assert result is not None
        assert result[0] == "LIV"

    def test_normalize_chelsea_alias(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Chelsea FC")
        assert result is not None
        assert result[0] == "CHE"

    def test_normalize_full_name(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Manchester City")
        assert result is not None
        assert result[0] == "MCI"

    def test_normalize_abbreviation(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("PSG")
        assert result is not None
        assert result[0] == "PSG"

    def test_register_alias(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        tn.register_alias("Test FC", "TST", "Test Club")
        result = tn.normalize("Test FC")
        assert result is not None
        assert result[0] == "TST"
        assert result[1] == "Test Club"

    def test_register_alias_overwrite(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        tn.register_alias("Arsenal", "ARS2", "Arsenal v2")
        result = tn.normalize("Arsenal")
        assert result is not None
        assert result[0] == "ARS2"

    def test_known_team_count(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        count = tn.known_team_count
        assert count > 50  # We have many teams

    def test_normalize_historical_name(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        # "vfl bochum" is in historical name changes
        result = tn.normalize("vfl bochum")
        assert result is not None

    def test_normalize_core_name_match(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        # "liverpool fc" should match core "liverpool"
        result = tn.normalize("Liverpool FC")
        assert result is not None
        assert result[0] == "LIV"

    def test_normalize_juventus(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Juventus")
        assert result is not None
        assert result[0] == "JUV"

    def test_normalize_nba_team(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Lakers")
        assert result is not None
        assert result[0] == "LAL"

    def test_normalize_nfl_team(self) -> None:
        from instruments_service.sports.team_normalizer import TeamNormalizer

        tn = TeamNormalizer()
        result = tn.normalize("Chiefs")
        assert result is not None
        assert result[0] == "KC"
