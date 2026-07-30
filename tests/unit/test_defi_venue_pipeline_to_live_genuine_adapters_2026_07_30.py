"""Targeted resolution check — the 6 pipeline→live DeFi venues now have genuine adapters.

``defi_venue_pipeline_to_live_ao_build_2026_07_30.md`` todo 1: ANKR-ETHEREUM /
STADER-ETHEREUM / STAKEWISE-ETHEREUM / SWELL-ETHEREUM / MANTLE-ETHEREUM /
MAKER-ETHEREUM each resolve through ``get_adapter_for_canonical_venue()``
(UAC ``VENUE_TO_ADAPTER_KEY`` → ``factory._ADAPTERS``) to a REAL
instruments-service reference-data adapter that returns non-placeholder
instrument entries — not a bare MTDS-only on-chain handler with no IS
counterpart (the exact gap ``DEFI_VENUE_PHASE``'s invariant flags).

"Adapter first, declaration second" (mirrors the CHAINLINK precedent in
``unified_api_contracts.registry.venue_adapter_keys``): these venues are
deliberately NOT yet in ``instruments_service.engine.orchestrator.defi.
_STATIC_DEFI_VENUES`` / ``_build_defi_venues()`` — that "declaration" step is
the coordinated LAST todo of the same plan (todo 5, the ``DEFI_VENUE_PHASE``
flip), so this file also pins that the drift-guard test
(``test_orchestrator_helpers.py::test_defi_set_equals_uac_denominator_drift_guard``)
stays unaffected today.
"""

from __future__ import annotations

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus
from unified_api_contracts.registry import NO_ADAPTER_YET, VENUE_TO_ADAPTER_KEY

from instruments_service.engine.orchestrator.defi import _build_defi_venues
from instruments_service.reference_data.factory import get_adapter_for_canonical_venue

_PIPELINE_TO_LIVE_VENUES: tuple[str, ...] = (
    "ANKR-ETHEREUM",
    "STADER-ETHEREUM",
    "STAKEWISE-ETHEREUM",
    "SWELL-ETHEREUM",
    "MANTLE-ETHEREUM",
    "MAKER-ETHEREUM",
)


class TestGenuineAdaptersExist:
    @pytest.mark.parametrize("venue", _PIPELINE_TO_LIVE_VENUES)
    def test_venue_has_a_real_uac_adapter_key(self, venue: str) -> None:
        assert VENUE_TO_ADAPTER_KEY.get(venue) not in (None, NO_ADAPTER_YET), (
            f"{venue} must resolve to a real URDI adapter key, not missing/NO_ADAPTER_YET"
        )

    @pytest.mark.parametrize("venue", _PIPELINE_TO_LIVE_VENUES)
    @pytest.mark.asyncio
    async def test_venue_resolves_to_non_placeholder_instruments(self, venue: str) -> None:
        adapter = get_adapter_for_canonical_venue(venue)
        records = await adapter.get_instruments()
        assert records, f"{venue} adapter returned zero instruments"
        for rec in records:
            assert isinstance(rec, InstrumentRecord)
            assert rec.venue == venue
            assert rec.status == InstrumentStatus.ACTIVE
            assert rec.base_asset_contract_address, f"{venue} record missing a real contract address"
            assert rec.base_asset_decimals is not None and rec.base_asset_decimals > 0
            assert rec.available_from_datetime is not None


class TestDeclarationStepDeliberatelyDeferred:
    """The phase flip (todo 5) — NOT this todo — adds these to _build_defi_venues()."""

    def test_venues_not_yet_in_build_defi_venues(self) -> None:
        current = set(_build_defi_venues())
        overlap = current & set(_PIPELINE_TO_LIVE_VENUES)
        assert not overlap, (
            f"{overlap} unexpectedly already in _build_defi_venues() — if todo 5 (the "
            "DEFI_VENUE_PHASE flip + _STATIC_DEFI_VENUES wiring) has since shipped, this "
            "guard test is stale and should be deleted, not adjusted."
        )
