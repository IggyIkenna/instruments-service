"""Adapter-routing UAC invariant (registry-consolidation Phase 2).

UAC owns venue→adapter-KEY (``VENUE_TO_ADAPTER_KEY``, pure data); this repo owns
key→CLASS (``factory._ADAPTERS``). These tests are the ship gate for that split:

- every UAC venue resolves to a real adapter class OR the explicit
  ``NO_ADAPTER_YET`` sentinel — a silent ``KeyError`` between the two tables is
  impossible while this suite is green;
- ``URDI_SUPPORTED_VENUES`` derives from UAC, not a frozen IS-side set;
- sentinel/unknown venues raise loudly with an actionable message.

Companion (UAC side): tests/unit/test_venue_adapter_keys.py asserts every
``VENUES_BY_ASSET_GROUP`` venue has an entry and the sentinel set is the
deliberate, documented one.
"""

from __future__ import annotations

import pytest
from unified_api_contracts.registry import (
    NO_ADAPTER_YET,
    VENUE_TO_ADAPTER_KEY,
    VENUES_BY_ASSET_GROUP,
    VENUES_WITH_REFERENCE_ADAPTER,
)

from instruments_service.engine.orchestrator.venue_core import expand_cefi_tardis_endpoints
from instruments_service.engine.urdi_reference_provider import URDI_SUPPORTED_VENUES
from instruments_service.reference_data.factory import _ADAPTERS, get_adapter_for_canonical_venue


class TestAdapterRoutingUACInvariant:
    def test_every_uac_adapter_key_resolves_to_a_class(self) -> None:
        """key→class closure: every non-sentinel UAC key exists in _ADAPTERS."""
        unresolvable = {
            venue: key for venue, key in VENUE_TO_ADAPTER_KEY.items() if key != NO_ADAPTER_YET and key not in _ADAPTERS
        }
        assert not unresolvable, (
            f"UAC VENUE_TO_ADAPTER_KEY entries with no adapter CLASS in factory._ADAPTERS: "
            f"{unresolvable}. Add the class to _ADAPTERS or fix the key in UAC."
        )

    def test_every_canonical_venue_has_a_uac_entry(self) -> None:
        """Cross-check of the UAC-side gate from the consuming side."""
        missing = {
            ag: [v for v in venues if v not in VENUE_TO_ADAPTER_KEY] for ag, venues in VENUES_BY_ASSET_GROUP.items()
        }
        missing = {ag: m for ag, m in missing.items() if m}
        assert not missing, f"canonical venues missing from UAC VENUE_TO_ADAPTER_KEY: {missing}"

    def test_urdi_supported_venues_derives_from_uac(self) -> None:
        """URDI_SUPPORTED_VENUES is the UAC-derived membership set, not a frozen IS list."""
        assert URDI_SUPPORTED_VENUES is VENUES_WITH_REFERENCE_ADAPTER

    def test_expanded_cefi_enumeration_fully_resolvable(self) -> None:
        """The venue set IS actually enumerates for cefi (post Tardis grain-expand)
        must resolve to real adapters — the end-to-end enumeration path guard."""
        expanded = expand_cefi_tardis_endpoints(list(VENUES_BY_ASSET_GROUP["cefi"]))
        unresolvable = [v for v in expanded if VENUE_TO_ADAPTER_KEY.get(v, NO_ADAPTER_YET) == NO_ADAPTER_YET]
        assert not unresolvable, f"expanded cefi venues without a real adapter key: {unresolvable}"

    def test_sentinel_venue_raises_loudly(self) -> None:
        with pytest.raises(ValueError, match=r"No URDI adapter.*NO_ADAPTER_YET"):
            get_adapter_for_canonical_venue("ODDS_API")

    def test_unknown_venue_raises_loudly(self) -> None:
        with pytest.raises(ValueError, match=r"No URDI adapter.*UNKNOWN to UAC"):
            get_adapter_for_canonical_venue("TOTALLY-UNKNOWN-VENUE-XYZ")
