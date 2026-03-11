"""
Data Models for Instruments Service

InstrumentDefinition is the canonical schema for instrument records used by
instruments-service. InstrumentKey, Venue, InstrumentType from shared config.
"""

from __future__ import annotations

import logging

from unified_api_contracts import (
    HttpRateLimitHeaders,
    VenueRateLimitSpec,
)

# UAC contract type references — canonical types for cross-service contract alignment
# G6 rate limits STRONG
from unified_config_interface import InstrumentType, Venue
from unified_internal_contracts import InstrumentDefinition, InstrumentKey

logger = logging.getLogger(__name__)

__all__ = [
    "HttpRateLimitHeaders",
    "InstrumentDefinition",
    "InstrumentKey",
    "InstrumentType",
    "Venue",
    "VenueRateLimitSpec",
]
