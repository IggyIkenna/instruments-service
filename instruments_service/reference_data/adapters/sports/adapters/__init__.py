"""Sports reference data adapters."""

from .api_football import ApiFootballAdapter
from .base import BaseSportsReferenceAdapter
from .footystats import FootystatsAdapter
from .odds_api import OddsApiAdapter
from .open_meteo import OpenMeteoAdapter
from .soccerfootball_info import SoccerFootballInfoAdapter
from .transfermarkt import TransfermarktAdapter
from .understat import UnderstatAdapter

__all__ = [
    "ApiFootballAdapter",
    "BaseSportsReferenceAdapter",
    "FootystatsAdapter",
    "OddsApiAdapter",
    "OpenMeteoAdapter",
    "SoccerFootballInfoAdapter",
    "TransfermarktAdapter",
    "UnderstatAdapter",
]
