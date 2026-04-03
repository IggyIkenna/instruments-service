"""Unit tests for competition_phase.py — classify_competition_phase."""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.sports.competition_phase import (
    CompetitionPhase,
    classify_competition_phase,
)


class TestClassifyNone:
    def test_none_round_name(self) -> None:
        phase, sub, is_normal = classify_competition_phase(None)
        assert phase == CompetitionPhase.UNKNOWN
        assert sub is None
        assert is_normal is False

    def test_empty_round_name(self) -> None:
        phase, sub, is_normal = classify_competition_phase("")
        assert phase == CompetitionPhase.UNKNOWN
        assert sub is None
        assert is_normal is False


class TestSplitKnockout:
    """apertura/clausura/opening/closing + knockout keyword → TOURNAMENT or PLAYOFF."""

    @pytest.mark.parametrize("prefix", ["apertura", "clausura", "opening", "closing"])
    def test_split_prefix_final(self, prefix: str) -> None:
        phase, _sub, _is_normal = classify_competition_phase(f"{prefix} - final")
        assert phase == CompetitionPhase.TOURNAMENT

    @pytest.mark.parametrize("prefix", ["apertura", "clausura"])
    def test_split_prefix_semi_final(self, prefix: str) -> None:
        phase, _sub, _is_normal = classify_competition_phase(f"{prefix} - semi-final")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_split_prefix_quarter_final(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("opening - quarter-final")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_split_prefix_finals(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("apertura finals")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_split_prefix_8th_final(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("clausura 8th final")
        assert phase == CompetitionPhase.TOURNAMENT

    @pytest.mark.parametrize("prefix", ["apertura", "clausura", "opening", "closing"])
    def test_split_prefix_playoff(self, prefix: str) -> None:
        phase, sub, is_normal = classify_competition_phase(f"{prefix} play-off")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub is None
        assert is_normal is False


class TestNormalLeague:
    """Regular season, split-prefix matchdays, league stage → NORMAL_LEAGUE."""

    def test_regular_season(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("Regular Season - 1")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_regular_season_exact(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("regular season")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_apertura_matchday(self) -> None:
        """Apertura without knockout/playoff keywords → normal league."""
        phase, _sub, is_normal = classify_competition_phase("Apertura - 5")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_clausura_matchday(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("clausura 10")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_opening_matchday(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("opening - matchday 3")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_closing_matchday(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("closing - week 8")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_league_stage(self) -> None:
        phase, _sub, is_normal = classify_competition_phase("League Stage - Matchday 1")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True


class TestLeagueSplit:
    """Championship/relegation/promotion rounds → LEAGUE_SPLIT."""

    def test_championship_round(self) -> None:
        phase, sub, is_normal = classify_competition_phase("Championship Round - 1")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "CHAMPIONSHIP"
        assert is_normal is False

    def test_championship_group(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("championship group - 2")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "CHAMPIONSHIP"

    def test_relegation_round(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("relegation round - 3")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "RELEGATION"

    def test_relegation_group(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("relegation group")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "RELEGATION"

    def test_promotion_round_no_playoff(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("promotion round - 1")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "PROMOTION"

    def test_upper_table_round(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("upper table round - 1")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "UPPER"

    def test_lower_table_round(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("lower table round - 2")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "LOWER"

    def test_1st_phase(self) -> None:
        phase, sub, is_normal = classify_competition_phase("1st phase")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub is None
        assert is_normal is False

    def test_2nd_phase(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("2nd phase - round 5")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub is None

    def test_2nd_phase_with_final_skips_league_split(self) -> None:
        """1st/2nd phase with a knockout keyword must NOT match league_split."""
        phase, _sub, _is_normal = classify_competition_phase("1st phase - final")
        assert phase == CompetitionPhase.TOURNAMENT


class TestPlayoff:
    """Play-off variants → PLAYOFF_LEAGUE."""

    def test_promotion_play_off(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("promotion play-off - 1st leg")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub == "PROMOTION"

    def test_promotion_playoff(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("promotion playoff")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub == "PROMOTION"

    def test_relegation_play_off(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("relegation play-off")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub == "RELEGATION"

    def test_relegation_playoff(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("relegation playoff - 2nd leg")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub == "RELEGATION"

    def test_play_offs_literal(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("play-offs")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub is None

    def test_playoffs_literal(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("playoffs")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub is None

    def test_playoff_round_literal(self) -> None:
        phase, sub, _is_normal = classify_competition_phase("playoff round")
        assert phase == CompetitionPhase.PLAYOFF_LEAGUE
        assert sub is None


class TestDecider:
    """Decider matches → DECIDER_MATCH."""

    def test_decider_keyword(self) -> None:
        phase, sub, is_normal = classify_competition_phase("Title Decider")
        assert phase == CompetitionPhase.DECIDER_MATCH
        assert sub is None
        assert is_normal is False

    def test_promotion_final(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("promotion final")
        assert phase == CompetitionPhase.DECIDER_MATCH

    def test_relegation_final(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("relegation final")
        assert phase == CompetitionPhase.DECIDER_MATCH

    def test_championship_final_literal(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("championship - final")
        assert phase == CompetitionPhase.DECIDER_MATCH

    def test_relegation_promotion_slash(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("relegation/promotion - 1st leg")
        assert phase == CompetitionPhase.DECIDER_MATCH


class TestTournamentFallback:
    """Strings matching no checker → TOURNAMENT."""

    def test_group_stage(self) -> None:
        phase, sub, is_normal = classify_competition_phase("Group Stage - Matchday 3")
        assert phase == CompetitionPhase.TOURNAMENT
        assert sub is None
        assert is_normal is False

    def test_round_of_16(self) -> None:
        phase, _sub, _is_normal = classify_competition_phase("Round of 16")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_quarter_finals_standalone(self) -> None:
        """quarter-final without split prefix falls through all checkers → TOURNAMENT."""
        phase, _sub, _is_normal = classify_competition_phase("quarter-finals")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_final_standalone(self) -> None:
        """'final' alone without split prefix or decider context → TOURNAMENT (fallback)."""
        phase, _sub, _is_normal = classify_competition_phase("Final")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_whitespace_stripped(self) -> None:
        """Leading/trailing whitespace stripped before classification."""
        phase, _sub, is_normal = classify_competition_phase("  Regular Season - 5  ")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True
