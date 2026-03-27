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
"""

from .adapters.api_football import (
    ApiFootballAdapter as ApiFootballAdapter,
)
from .adapters.base import (
    BaseSportsReferenceAdapter as BaseSportsReferenceAdapter,
)
from .adapters.odds_api import OddsApiAdapter as OddsApiAdapter
from .competition_phase import (
    CompetitionPhase as CompetitionPhase,
)
from .competition_phase import (
    classify_competition_phase as classify_competition_phase,
)
from .factory import (
    create_sports_reference_adapter as create_sports_reference_adapter,
)
