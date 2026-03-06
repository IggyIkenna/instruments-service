"""
League Registry — public API for sports league data.

The implementation is split into focused modules:

- ``league_definition``  — ``LeagueDefinition`` dataclass + helper constants
- ``league_data_prediction`` — Prediction-tier football leagues
- ``league_data_other``  — Features, Reference, and Non-football leagues
- ``league_lookup``  — assembled ``LEAGUE_REGISTRY`` + query functions
- ``league_classification``  — Pydantic-based classification config
"""

from instruments_service.sports.league_classification import (
    DEFAULT_CLASSIFICATION_REGISTRY,
    LEAGUE_CLASSIFICATION_DATA,
    LeagueClassification,
    LeagueClassificationRegistry,
    LeagueClassificationType,
)
from instruments_service.sports.league_definition import LeagueDefinition
from instruments_service.sports.league_lookup import (
    LEAGUE_REGISTRY,
    get_league,
    get_league_by_api_football_id,
    get_leagues_by_classification,
    get_leagues_by_country,
    get_leagues_for_sport,
    get_prediction_leagues,
)

__all__ = [
    "DEFAULT_CLASSIFICATION_REGISTRY",
    "LEAGUE_CLASSIFICATION_DATA",
    "LEAGUE_REGISTRY",
    "LeagueClassification",
    "LeagueClassificationRegistry",
    "LeagueClassificationType",
    "LeagueDefinition",
    "get_league",
    "get_league_by_api_football_id",
    "get_leagues_by_classification",
    "get_leagues_by_country",
    "get_leagues_for_sport",
    "get_prediction_leagues",
]
