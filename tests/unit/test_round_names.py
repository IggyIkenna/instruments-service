"""
Unit tests for round_names — tournament round data and lookup.

Tests cover:
- Data integrity: counts, type safety, no duplicates
- Exact match resolution for known round names
- Prefix match resolution for partial/variant names
- Unknown input returns None
- Edge cases: empty string, whitespace, case sensitivity
- is_known_round convenience helper
- RoundMatch dataclass fields
"""

from __future__ import annotations

import pytest

from instruments_service.sports.round_names import (
    ROUND_NAMES,
    ROUND_PREFIXES,
    RoundMatch,
    is_known_round,
    resolve_round_name,
)

# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------


class TestRoundNamesData:
    """Verify the static data sets are well-formed."""

    def test_round_names_is_frozenset(self) -> None:
        assert isinstance(ROUND_NAMES, frozenset)

    def test_round_prefixes_is_frozenset(self) -> None:
        assert isinstance(ROUND_PREFIXES, frozenset)

    def test_round_names_count(self) -> None:
        """Source file has 209 unique entries (210 list items, 1 duplicate case variant)."""
        assert len(ROUND_NAMES) >= 200

    def test_round_prefixes_count(self) -> None:
        """Source file has 65 unique prefix entries."""
        assert len(ROUND_PREFIXES) >= 60

    def test_all_entries_are_strings(self) -> None:
        for name in ROUND_NAMES:
            assert isinstance(name, str), f"Expected str, got {type(name)}: {name!r}"

    def test_all_prefixes_are_strings(self) -> None:
        for prefix in ROUND_PREFIXES:
            assert isinstance(prefix, str), f"Expected str, got {type(prefix)}: {prefix!r}"

    def test_no_empty_entries_in_names(self) -> None:
        for name in ROUND_NAMES:
            assert name.strip(), f"Empty or whitespace-only entry in ROUND_NAMES: {name!r}"

    def test_no_empty_entries_in_prefixes(self) -> None:
        for prefix in ROUND_PREFIXES:
            assert prefix, f"Empty entry in ROUND_PREFIXES: {prefix!r}"


# ---------------------------------------------------------------------------
# Exact match resolution
# ---------------------------------------------------------------------------


class TestResolveExactMatch:
    """Tests for exact-match lookups in ROUND_NAMES."""

    @pytest.mark.parametrize(
        "name",
        [
            "Final",
            "Semi-finals",
            "Quarter-finals",
            "1st Round",
            "Regular Season - 1",
            "Regular Season - 38",
            "Group A - 1",
            "Group L - 6",
            "League Stage - 8",
            "Knockout Round Play-offs",
            "Preliminary round",
            "Relegation Decider",
            "Round of 16",
            "Round of 128",
            "1/128-finals",
        ],
    )
    def test_known_round_exact(self, name: str) -> None:
        result = resolve_round_name(name)
        assert result is not None, f"Expected exact match for {name!r}"
        assert result.is_exact is True
        assert result.name == name
        assert result.matched_prefix is None

    def test_regular_season_midrange(self) -> None:
        result = resolve_round_name("Regular Season - 22")
        assert result is not None
        assert result.is_exact is True
        assert result.name == "Regular Season - 22"


# ---------------------------------------------------------------------------
# Prefix match resolution
# ---------------------------------------------------------------------------


class TestResolvePrefixMatch:
    """Tests for prefix-based fallback lookups."""

    def test_regular_season_beyond_46(self) -> None:
        """Regular Season - 50 is NOT in ROUND_NAMES but matches prefix."""
        result = resolve_round_name("Regular Season - 50")
        assert result is not None
        assert result.is_exact is False
        assert result.matched_prefix == "Regular Season "

    def test_group_a_beyond_6(self) -> None:
        result = resolve_round_name("Group A - 10")
        assert result is not None
        assert result.is_exact is False
        assert result.matched_prefix == "Group A "

    def test_regions_path_unknown_sub_round(self) -> None:
        result = resolve_round_name("Regions Path - 7th Round")
        assert result is not None
        assert result.is_exact is False
        assert result.matched_prefix == "Regions Path "

    def test_premier_league_path_unknown_sub(self) -> None:
        result = resolve_round_name("Premier League Path - 3rd Place")
        assert result is not None
        assert result.is_exact is False
        assert result.matched_prefix == "Premier League Path "

    def test_league_stage_beyond_8(self) -> None:
        result = resolve_round_name("League Stage - 9")
        assert result is not None
        assert result.is_exact is False
        assert result.matched_prefix == "League Stage "


# ---------------------------------------------------------------------------
# No match / edge cases
# ---------------------------------------------------------------------------


class TestResolveNoMatch:
    """Tests for inputs that should not resolve."""

    def test_empty_string(self) -> None:
        assert resolve_round_name("") is None

    def test_whitespace_only(self) -> None:
        assert resolve_round_name("   ") is None

    def test_unknown_round(self) -> None:
        assert resolve_round_name("Imaginary Round - 99") is None

    def test_gibberish(self) -> None:
        assert resolve_round_name("xyzzy") is None

    def test_case_sensitive_mismatch(self) -> None:
        """Round names are case-sensitive; 'final' != 'Final'."""
        result = resolve_round_name("final")
        assert result is None

    def test_trailing_whitespace_stripped(self) -> None:
        """Leading/trailing whitespace is stripped before lookup."""
        result = resolve_round_name("  Final  ")
        assert result is not None
        assert result.is_exact is True
        assert result.name == "Final"


# ---------------------------------------------------------------------------
# is_known_round convenience function
# ---------------------------------------------------------------------------


class TestIsKnownRound:
    """Tests for the boolean convenience helper."""

    def test_known_exact(self) -> None:
        assert is_known_round("Final") is True

    def test_known_prefix(self) -> None:
        assert is_known_round("Regular Season - 99") is True

    def test_unknown(self) -> None:
        assert is_known_round("Not A Real Round") is False

    def test_empty(self) -> None:
        assert is_known_round("") is False


# ---------------------------------------------------------------------------
# RoundMatch dataclass
# ---------------------------------------------------------------------------


class TestRoundMatchDataclass:
    """Tests for the RoundMatch result container."""

    def test_frozen(self) -> None:
        match = RoundMatch(name="Final", is_exact=True)
        with pytest.raises(AttributeError):
            match.name = "Semi-finals"  # type: ignore[misc]

    def test_exact_match_defaults(self) -> None:
        match = RoundMatch(name="Final", is_exact=True)
        assert match.matched_prefix is None

    def test_prefix_match_fields(self) -> None:
        match = RoundMatch(
            name="Regular Season - 50",
            is_exact=False,
            matched_prefix="Regular Season ",
        )
        assert match.name == "Regular Season - 50"
        assert match.is_exact is False
        assert match.matched_prefix == "Regular Season "

    def test_equality(self) -> None:
        a = RoundMatch(name="Final", is_exact=True)
        b = RoundMatch(name="Final", is_exact=True)
        assert a == b

    def test_inequality(self) -> None:
        a = RoundMatch(name="Final", is_exact=True)
        b = RoundMatch(name="Final", is_exact=False, matched_prefix="Final")
        assert a != b
