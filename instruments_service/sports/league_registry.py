"""League Registry - public API for sports league data.

Re-exports all league symbols from UAC (unified_api_contracts.sports)
as the single source of truth for league definitions, classification,
and lookup functions.

Previously had local implementations; these have been ported to UAC.
"""

from unified_api_contracts.sports import (
    DEFAULT_CLASSIFICATION_REGISTRY as DEFAULT_CLASSIFICATION_REGISTRY,  # noqa: deep-import — UAC sports is the correct domain facade per architecture rules
)
from unified_api_contracts.sports import (
    LEAGUE_CLASSIFICATION_DATA as LEAGUE_CLASSIFICATION_DATA,  # noqa: deep-import — UAC sports facade
)
from unified_api_contracts.sports import LEAGUE_REGISTRY as LEAGUE_REGISTRY  # noqa: deep-import — UAC sports facade
from unified_api_contracts.sports import LeagueClassification as LeagueClassification
from unified_api_contracts.sports import LeagueClassificationRegistry as LeagueClassificationRegistry
from unified_api_contracts.sports import LeagueClassificationType as LeagueClassificationType
from unified_api_contracts.sports import LeagueDefinition as LeagueDefinition
from unified_api_contracts.sports import get_league as get_league
from unified_api_contracts.sports import get_league_by_api_football_id as get_league_by_api_football_id
from unified_api_contracts.sports import get_leagues_by_classification as get_leagues_by_classification
from unified_api_contracts.sports import get_leagues_by_country as get_leagues_by_country
from unified_api_contracts.sports import get_leagues_for_sport as get_leagues_for_sport
from unified_api_contracts.sports import get_prediction_leagues as get_prediction_leagues

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
