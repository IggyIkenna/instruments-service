"""Adapter factory for unified-sports-reference-interface.

Creates the appropriate sports reference data adapter for a given venue.
Convention: interfaces are API-keyless. Services fetch credentials from
Secret Manager and inject them at runtime via the ``api_key`` parameter.

Enrichment adapters (footystats / understat / transfermarkt / SFI /
open_meteo / betfair) depend on api-football having been fetched first for
the target date. When ``date`` is supplied, the factory pre-flight checks
that dependency and raises ``unified_trading_library.DependencyError`` with
an actionable CLI message if api-football data is missing. SSOT:
``unified-trading-pm/codex/02-data/sports-adapter-dependency-order.md``.
"""

from __future__ import annotations

import logging

from instruments_service.reference_data.sports_dependency import (
    check_api_football_dependency,
    venue_requires_api_football,
)

from .adapters.api_football import ApiFootballAdapter
from .adapters.base import BaseSportsReferenceAdapter
from .adapters.footystats import FootystatsAdapter
from .adapters.open_meteo import OpenMeteoAdapter
from .adapters.soccerfootball_info import SoccerFootballInfoAdapter
from .adapters.transfermarkt import TransfermarktAdapter
from .adapters.understat import UnderstatAdapter

_logger = logging.getLogger(__name__)

_ADAPTERS: dict[str, type[BaseSportsReferenceAdapter]] = {
    "api_football": ApiFootballAdapter,
    "footystats": FootystatsAdapter,
    "open_meteo": OpenMeteoAdapter,
    "soccer_football_info": SoccerFootballInfoAdapter,
    "soccerfootball_info": SoccerFootballInfoAdapter,
    "transfermarkt": TransfermarktAdapter,
    "understat": UnderstatAdapter,
}


def create_sports_reference_adapter(
    venue: str,
    api_key: str | None = None,
    date: str | None = None,
    bucket: str | None = None,
) -> BaseSportsReferenceAdapter:
    """Create and return a sports reference data adapter for the given venue.

    Args:
        venue: Venue identifier (e.g. 'api_football').
        api_key: API key for the venue. The calling service MUST fetch this
            from Secret Manager and pass it in. Adapters that require
            authentication will raise ``ValueError`` if not provided.
        date: Optional target shard date (YYYY-MM-DD). When supplied for an
            api-football-dependent venue (footystats / understat /
            transfermarkt / SFI / open_meteo / betfair), the factory
            pre-flight checks that api-football data has been fetched for
            this date and raises ``DependencyError`` otherwise.
        bucket: Optional sports-reference bucket name for the pre-flight
            check. When None, resolved from ``InstrumentsServiceConfig``
            honouring ``IS_TEST_RUN``.

    Returns:
        A sports reference data adapter instance.

    Raises:
        ValueError: If venue is not supported.
        unified_trading_library.DependencyError: If ``date`` is supplied for
            an api-football-dependent venue and api-football reference data
            is missing for that date.
    """
    venue_lower = venue.lower()
    adapter_class = _ADAPTERS.get(venue_lower)
    if adapter_class is None:
        supported = sorted(_ADAPTERS.keys())
        raise ValueError(f"Unsupported venue: {venue!r}. Supported: {supported}")

    if date is not None and venue_requires_api_football(venue_lower):
        check_api_football_dependency(date=date, bucket=bucket)

    return adapter_class(api_key=api_key)
