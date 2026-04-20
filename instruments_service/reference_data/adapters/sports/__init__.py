"""Sports reference data sub-package.

Provides sports-specific reference data adapters with a separate contract
from URDI's BaseReferenceDataAdapter. Sports adapters return CanonicalFixture,
CanonicalLeague, CanonicalTeam, CanonicalOdds instead of InstrumentRecord.

Usage:
    from instruments_service.reference_data.adapters.sports import (
        create_sports_reference_adapter,
        ApiFootballAdapter,
        CompetitionPhase,
        classify_competition_phase,
    )

    adapter = create_sports_reference_adapter("api_football", api_key=key)
    fixtures = await adapter.get_fixtures(date="2026-03-26")

    # Enrichment adapter with api-football dep pre-flight (raises
    # DependencyError if api-football reference data missing for the date):
    adapter = create_sports_reference_adapter(
        "footystats", api_key=key, date="2026-04-14"
    )
"""

from instruments_service.reference_data.sports_dependency import (
    check_api_football_dependency as check_api_football_dependency,
)
from instruments_service.reference_data.sports_dependency import (
    venue_requires_api_football as venue_requires_api_football,
)

from .adapters.api_football import (
    ApiFootballAdapter as ApiFootballAdapter,
)
from .adapters.base import (
    BaseSportsReferenceAdapter as BaseSportsReferenceAdapter,
)
from .competition_phase import (
    CompetitionPhase as CompetitionPhase,
)
from .competition_phase import (
    classify_competition_phase as classify_competition_phase,
)
from .factory import (
    create_sports_reference_adapter as create_sports_reference_adapter,
)
