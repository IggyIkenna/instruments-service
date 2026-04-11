"""Sports reference data adapters."""

from .api_football import ApiFootballAdapter
from .base import BaseSportsReferenceAdapter

__all__ = [
    "ApiFootballAdapter",
    "BaseSportsReferenceAdapter",
]
