"""Unit tests for the Deribit combo reference data adapter (no live network).

CF-11 honest-absence regression (cefi_manifest_canonicalisation_2026_06_01): a genuine
fetch-failure across every currency MUST raise (→ attempted_failed via
urdi_reference_provider._fetch_one), never ``return []`` which would land DERIBIT in
_non_error_venues and silently vanish from the manifest. The DeribitCombo adapter was the
one cefi reference-data adapter the cross-AG CF-11 sweep (slot-6, e2e008f0) missed; this
completes it with the same RuntimeError convention as aster/hyperliquid/tardis.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
    DeribitComboReferenceDataAdapter,
)


def _combo_record(key: str = "DERIBIT:COMBO:BTC-FS-25APR26_PERP") -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=key,
        venue="DERIBIT",
        raw_symbol="BTC-FS-25APR26_PERP",
        instrument_type="COMBO",
        base_asset="BTC",
        quote_asset="USD",
    )


class TestDeribitComboCF11:
    def test_venue_name(self) -> None:
        assert DeribitComboReferenceDataAdapter().venue == "DERIBIT"

    @pytest.mark.asyncio
    async def test_get_instruments_raises_when_all_currencies_fail(self) -> None:
        """Every currency fetch-failing must raise RuntimeError (CF-11), not return []."""
        adapter = DeribitComboReferenceDataAdapter()
        with (
            patch.object(
                adapter,
                "_fetch_combos_for_currency",
                AsyncMock(side_effect=RuntimeError("Deribit combo fetch failed for currency=BTC")),
            ),
            pytest.raises(RuntimeError, match="no instruments fetched"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_partial_success_does_not_raise(self) -> None:
        """≥1 currency fetching OK is trustworthy → return it (shard isolation), even if
        another currency fetch-failed."""
        good = _combo_record()

        async def _side_effect(currency: str, _now: object) -> list[InstrumentRecord]:
            if currency == "BTC":
                return [good]
            raise RuntimeError(f"Deribit combo fetch failed for currency={currency}")

        adapter = DeribitComboReferenceDataAdapter()
        with patch.object(adapter, "_fetch_combos_for_currency", AsyncMock(side_effect=_side_effect)):
            results = await adapter.get_instruments()
        assert results == [good]

    @pytest.mark.asyncio
    async def test_get_instruments_all_empty_no_error_returns_empty(self) -> None:
        """All currencies fetched OK but legitimately had zero combos → genuine empty,
        return [] (NOT a raise — there was no fetch-failure)."""
        adapter = DeribitComboReferenceDataAdapter()
        with patch.object(adapter, "_fetch_combos_for_currency", AsyncMock(return_value=[])):
            results = await adapter.get_instruments()
        assert results == []
