"""
Sports Category Support for Instruments Service

Provides sports-specific parsers and registries for the SPORTS market category:
- League registry: canonical league definitions (EPL, NBA, NFL, MLB, NHL, etc.)
- Fixture parser: converts raw fixture/event data to instrument definitions
- Team normalizer: maps team name variants to canonical identifiers
- Team aliases: cross-provider team identity resolution (API-Football, FootyStats, etc.)
- Round names: canonical tournament round identifiers and prefix-based lookup

Sports augments instruments-service as a category alongside CEFI, TRADFI, DEFI.
Implementation follows the same pattern as other categories.
"""

from instruments_service.sports.fixture_parser import FixtureParser
from instruments_service.sports.league_registry import (
    DEFAULT_CLASSIFICATION_REGISTRY,
    LEAGUE_CLASSIFICATION_DATA,
    LEAGUE_REGISTRY,
    LeagueClassification,
    LeagueClassificationRegistry,
    LeagueClassificationType,
    LeagueDefinition,
    get_league,
    get_league_by_api_football_id,
    get_leagues_by_classification,
    get_leagues_by_country,
    get_leagues_for_sport,
    get_prediction_leagues,
)
from instruments_service.sports.round_names import (
    ROUND_NAMES,
    ROUND_PREFIXES,
    RoundMatch,
    is_known_round,
    resolve_round_name,
)
from instruments_service.sports.team_aliases import (
    TeamAliasResolver,
    TeamNameHistory,
    load_team_mappings_from_dict,
    load_team_mappings_from_gcs,
)
from instruments_service.sports.team_normalizer import TeamNormalizer

__all__ = [
    "DEFAULT_CLASSIFICATION_REGISTRY",
    "LEAGUE_CLASSIFICATION_DATA",
    "LEAGUE_REGISTRY",
    "ROUND_NAMES",
    "ROUND_PREFIXES",
    "FixtureParser",
    "LeagueClassification",
    "LeagueClassificationRegistry",
    "LeagueClassificationType",
    "LeagueDefinition",
    "RoundMatch",
    "TeamAliasResolver",
    "TeamNameHistory",
    "TeamNormalizer",
    "get_league",
    "get_league_by_api_football_id",
    "get_leagues_by_classification",
    "get_leagues_by_country",
    "get_leagues_for_sport",
    "get_prediction_leagues",
    "is_known_round",
    "load_team_mappings_from_dict",
    "load_team_mappings_from_gcs",
    "resolve_round_name",
]
