"""Adapters package — URDI reference data provider only.

instruments-service has one adapter: the URDI reference provider.
All infrastructure concerns (storage, messaging, cloud) are handled
by UTL based on service identity — not declared here.
"""

from instruments_service.adapters.urdi_reference_provider import (
    URDI_SUPPORTED_VENUES,
    fetch_instruments_for_all_venues,
    fetch_instruments_via_urdi,
)

__all__ = [
    "URDI_SUPPORTED_VENUES",
    "fetch_instruments_for_all_venues",
    "fetch_instruments_via_urdi",
]
