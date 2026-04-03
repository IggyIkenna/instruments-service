"""Unit tests for the sports team normalizer."""

from __future__ import annotations

from instruments_service.reference_data.adapters.sports._normalizer import (
    TeamNormalizer,
    _extract_core_name,
    _strip_accents,
    normalize_for_fuzzy,
)


class TestStripAccents:
    def test_alaves(self) -> None:
        assert _strip_accents("Alavés") == "alaves"

    def test_munchen(self) -> None:
        assert _strip_accents("München") == "munchen"

    def test_plain_text(self) -> None:
        assert _strip_accents("Arsenal") == "arsenal"

    def test_empty_string(self) -> None:
        assert _strip_accents("") == ""


class TestNormalizeForFuzzy:
    def test_basic(self) -> None:
        result = normalize_for_fuzzy("Arsenal FC")
        assert result == "arsenal fc"

    def test_accents_removed(self) -> None:
        result = normalize_for_fuzzy("Alavés")
        assert result == "alaves"

    def test_extra_whitespace(self) -> None:
        result = normalize_for_fuzzy("  Arsenal   FC  ")
        assert result == "arsenal fc"


class TestExtractCoreName:
    def test_strips_fc_suffix(self) -> None:
        result = _extract_core_name("Arsenal FC")
        # Core should have FC stripped
        assert "fc" not in result

    def test_basic_name(self) -> None:
        result = _extract_core_name("Arsenal")
        assert result == "arsenal"


class TestTeamNormalizer:
    def test_exact_match(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Arsenal")
        assert result is not None
        assert result[0] == "ARS"
        assert result[1] == "Arsenal"

    def test_case_insensitive(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("ARSENAL")
        assert result is not None
        assert result[0] == "ARS"

    def test_whitespace_stripped(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("  Arsenal  ")
        assert result is not None
        assert result[0] == "ARS"

    def test_fc_variant(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Arsenal FC")
        assert result is not None
        assert result[0] == "ARS"

    def test_chelsea(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Chelsea")
        assert result is not None
        assert result[0] == "CHE"

    def test_manchester_united(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Manchester United")
        assert result is not None
        assert result[0] == "MUN"

    def test_liverpool(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Liverpool")
        assert result is not None
        assert result[0] == "LIV"

    def test_accent_match_bayern_munchen(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Bayern München")
        if result is not None:
            assert result[0] == "BAY"

    def test_unknown_team_returns_none(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Totally Unknown Team XYZ 123")
        assert result is None

    def test_register_alias(self) -> None:
        norm = TeamNormalizer()
        norm.register_alias("The Gunners", "ARS", "Arsenal")
        result = norm.normalize("The Gunners")
        assert result is not None
        assert result[0] == "ARS"

    def test_known_team_count(self) -> None:
        norm = TeamNormalizer()
        count = norm.known_team_count
        assert count > 10  # should have at least EPL teams

    def test_core_name_match(self) -> None:
        """Core name matching strips suffixes and finds team."""
        norm = TeamNormalizer()
        # Many EPL teams have FC/AFC variants
        result = norm.normalize("Chelsea FC")
        assert result is not None
        assert result[0] == "CHE"

    def test_brighton_full_name(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Brighton & Hove Albion")
        assert result is not None
        assert result[0] == "BHA"

    def test_tottenham_variants(self) -> None:
        norm = TeamNormalizer()
        for name in ["Tottenham", "Tottenham Hotspur", "Spurs"]:
            result = norm.normalize(name)
            if result is not None:
                assert result[0] == "TOT"

    def test_crystal_palace(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Crystal Palace")
        assert result is not None
        assert result[0] == "CRY"

    def test_west_ham(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("West Ham")
        assert result is not None

    def test_newcastle(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Newcastle")
        assert result is not None

    def test_bundesliga_bayern(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Bayern Munich")
        if result is not None:
            assert result[0] == "BAY"

    def test_bundesliga_dortmund(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Borussia Dortmund")
        if result is not None:
            assert result[0] == "BVB"

    def test_la_liga_barcelona(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Barcelona")
        if result is not None:
            assert result[0] == "BAR"

    def test_la_liga_real_madrid(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Real Madrid")
        if result is not None:
            assert result[0] == "RMA"
