"""
League Lookup — assembled registry and query functions.

Merges Prediction, Features, Reference, and Non-football league data into the
canonical ``LEAGUE_REGISTRY`` dict and exposes convenience lookup functions
used throughout instruments-service.
"""

from __future__ import annotations

from instruments_service.sports.league_data_other import (
    FEATURES_LEAGUES,
    NON_FOOTBALL_LEAGUES,
    REFERENCE_LEAGUES,
)
from instruments_service.sports.league_data_prediction import PREDICTION_LEAGUES
from instruments_service.sports.league_definition import LeagueDefinition

# ---------------------------------------------------------------------------
# Assembled registry — single source of truth
# ---------------------------------------------------------------------------

LEAGUE_REGISTRY: dict[str, LeagueDefinition] = {
    **PREDICTION_LEAGUES,
    **FEATURES_LEAGUES,
    **REFERENCE_LEAGUES,
    **NON_FOOTBALL_LEAGUES,
}


# ---------------------------------------------------------------------------
# Reverse lookup — API-Football ID -> league_id
# ---------------------------------------------------------------------------

_API_FOOTBALL_ID_TO_LEAGUE: dict[int, str] = {
    league.api_football_id: league.league_id
    for league in LEAGUE_REGISTRY.values()
    if league.api_football_id is not None
}


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_league(league_id: str) -> LeagueDefinition | None:
    """Look up a league by its canonical identifier (case-insensitive)."""
    return LEAGUE_REGISTRY.get(league_id.upper())


def get_league_by_api_football_id(api_football_id: int) -> LeagueDefinition | None:
    """Look up a league by its API-Football numeric ID."""
    lid = _API_FOOTBALL_ID_TO_LEAGUE.get(api_football_id)
    if lid is None:
        return None
    return LEAGUE_REGISTRY.get(lid)


def get_leagues_for_sport(sport: str) -> list[LeagueDefinition]:
    """Return all leagues for a given sport type (case-insensitive)."""
    sport_upper = sport.upper()
    return [league for league in LEAGUE_REGISTRY.values() if league.sport == sport_upper]


def get_leagues_by_classification(classification: str) -> list[LeagueDefinition]:
    """Return all leagues matching a classification (Prediction, Features, Reference, Other)."""
    cls_lower = classification.lower()
    return [league for league in LEAGUE_REGISTRY.values() if league.classification.lower() == cls_lower]


def get_leagues_by_country(country: str) -> list[LeagueDefinition]:
    """Return all leagues for a given ISO country code (case-insensitive)."""
    country_upper = country.upper()
    return [league for league in LEAGUE_REGISTRY.values() if league.country == country_upper]


def get_prediction_leagues() -> list[LeagueDefinition]:
    """Return all Prediction-tier leagues (suitable for model-based betting)."""
    return get_leagues_by_classification("Prediction")
