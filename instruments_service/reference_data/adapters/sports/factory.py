"""Adapter factory for unified-sports-reference-interface.

Creates the appropriate sports reference data adapter for a given venue.
Convention: interfaces are API-keyless. Services fetch credentials from
Secret Manager and inject them at runtime via the ``api_key`` parameter.
"""

from __future__ import annotations

import logging

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
) -> BaseSportsReferenceAdapter:
    """Create and return a sports reference data adapter for the given venue.

    Args:
        venue: Venue identifier (e.g. 'api_football').
        api_key: API key for the venue. The calling service MUST fetch this
                 from Secret Manager and pass it in. Adapters that require
                 authentication will raise ``ValueError`` if not provided.

    Returns:
        A sports reference data adapter instance.

    Raises:
        ValueError: If venue is not supported.
    """
    venue_lower = venue.lower()
    adapter_class = _ADAPTERS.get(venue_lower)
    if adapter_class is None:
        supported = sorted(_ADAPTERS.keys())
        raise ValueError(f"Unsupported venue: {venue!r}. Supported: {supported}")
    return adapter_class(api_key=api_key)
