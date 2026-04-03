"""Competition phase classification for sports fixtures.

Classifies API-Football league.round values into canonical competition phases.
Critical for ML training data filtering — different phases (playoffs, dead rubbers,
relegation battles) have fundamentally different dynamics.

Ported from CosmicTrader (new-sports-batting-services/footballbets/core/mapping.py).
"""

from __future__ import annotations

from enum import StrEnum


class CompetitionPhase(StrEnum):
    """Canonical competition phase classification."""

    NORMAL_LEAGUE = "NORMAL_LEAGUE"
    LEAGUE_SPLIT = "LEAGUE_SPLIT"
    PLAYOFF_LEAGUE = "PLAYOFF_LEAGUE"
    DECIDER_MATCH = "DECIDER_MATCH"
    TOURNAMENT = "TOURNAMENT"
    UNKNOWN = "UNKNOWN"


_KNOCKOUT_KW = frozenset(
    {
        "final",
        "finals",
        "semi-final",
        "quarter-final",
        "8th final",
    }
)
_SPLIT_PFX = ("apertura", "clausura", "opening", "closing")

_Result = tuple[CompetitionPhase, str | None, bool]


def _check_split_knockout(r: str) -> _Result | None:
    if any(r.startswith(p) and any(kw in r for kw in _KNOCKOUT_KW) for p in _SPLIT_PFX):
        return CompetitionPhase.TOURNAMENT, None, False
    if any(r.startswith(p) and "play-off" in r for p in _SPLIT_PFX):
        return CompetitionPhase.PLAYOFF_LEAGUE, None, False
    return None


def _check_normal_league(r: str) -> _Result | None:
    if r.startswith("regular season"):
        return CompetitionPhase.NORMAL_LEAGUE, None, True
    if r.startswith(_SPLIT_PFX):
        return CompetitionPhase.NORMAL_LEAGUE, None, True
    if r.startswith("league stage"):
        return CompetitionPhase.NORMAL_LEAGUE, None, True
    return None


def _check_league_split(r: str) -> _Result | None:
    if "championship round" in r or "championship group" in r:
        return CompetitionPhase.LEAGUE_SPLIT, "CHAMPIONSHIP", False
    if "relegation round" in r or "relegation group" in r:
        return CompetitionPhase.LEAGUE_SPLIT, "RELEGATION", False
    if "promotion round" in r and "play-off" not in r:
        return CompetitionPhase.LEAGUE_SPLIT, "PROMOTION", False
    if "upper table round" in r or "lower table round" in r:
        sub = "UPPER" if "upper" in r else "LOWER"
        return CompetitionPhase.LEAGUE_SPLIT, sub, False
    is_phase = ("1st phase" in r or "2nd phase" in r) and not any(x in r for x in _KNOCKOUT_KW)
    if is_phase:
        return CompetitionPhase.LEAGUE_SPLIT, None, False
    return None


def _check_playoff(r: str) -> _Result | None:
    if "promotion play-off" in r or "promotion playoff" in r:
        return CompetitionPhase.PLAYOFF_LEAGUE, "PROMOTION", False
    if "relegation play-off" in r or "relegation playoff" in r:
        return CompetitionPhase.PLAYOFF_LEAGUE, "RELEGATION", False
    if r in {"play-offs", "playoffs", "playoff round"}:
        return CompetitionPhase.PLAYOFF_LEAGUE, None, False
    return None


def _check_decider(r: str) -> _Result | None:
    if "decider" in r:
        return CompetitionPhase.DECIDER_MATCH, None, False
    if r.endswith("final") and ("promotion" in r or "relegation" in r):
        return CompetitionPhase.DECIDER_MATCH, None, False
    if r == "championship - final":
        return CompetitionPhase.DECIDER_MATCH, None, False
    if "relegation/promotion" in r:
        return CompetitionPhase.DECIDER_MATCH, None, False
    return None


def classify_competition_phase(
    round_name: str | None,
) -> tuple[CompetitionPhase, str | None, bool]:
    """Classify API-Football league.round into canonical phases.

    Args:
        round_name: The round name from API-Football.

    Returns:
        Tuple of (phase, subphase, is_normal_league).
    """
    if not round_name:
        return CompetitionPhase.UNKNOWN, None, False

    r = round_name.lower().strip()

    for checker in (
        _check_split_knockout,
        _check_normal_league,
        _check_league_split,
        _check_playoff,
        _check_decider,
    ):
        result = checker(r)
        if result is not None:
            return result

    return CompetitionPhase.TOURNAMENT, None, False
