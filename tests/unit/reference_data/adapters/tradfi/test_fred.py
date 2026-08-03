"""Unit tests — FRED reference data adapter (static KEY_SERIES catalogue).

FRED has no vendor call: the KEY_SERIES universe is a small, curated
same-shape duplicate of MTDS's FredAdapter.KEY_SERIES. Added 2026-08-03
(instruments-service NO_ADAPTER_YET -> "fred" flip).
"""

from __future__ import annotations

import pytest
from unified_api_contracts import AssetClass, InstrumentType

from instruments_service.reference_data.adapters.tradfi.fred import (
    _KEY_SERIES,
    FredReferenceDataAdapter,
)


class TestFredReferenceDataAdapter:
    @pytest.mark.asyncio
    async def test_emits_one_record_per_curated_series(self) -> None:
        adapter = FredReferenceDataAdapter()

        results = await adapter.get_instruments()

        assert len(results) == len(_KEY_SERIES) == 28
        assert {r.raw_symbol for r in results} == {entry["series_id"] for entry in _KEY_SERIES}

    @pytest.mark.asyncio
    async def test_canonical_identity_matches_mtds_convention(self) -> None:
        """FRED:{BOND|INDEX}:{SERIES_ID}-USD — byte-identical to MTDS's
        derive_tradfi_row_instrument_id(..., venue="FRED", instrument_type=classify_fred_series(series_id))
        output for the same series (both resolve through build_instrument_id's
        TradFi-cash branch, which appends -USD uniformly for INDEX/BOND)."""
        adapter = FredReferenceDataAdapter()

        results = await adapter.get_instruments()
        by_symbol = {r.raw_symbol: r for r in results}

        dgs10 = by_symbol["DGS10"]
        assert dgs10.instrument_key == "FRED:BOND:DGS10-USD"
        assert dgs10.canonical_instrument_id == dgs10.instrument_key
        assert dgs10.venue == "FRED"
        assert dgs10.instrument_type == InstrumentType.BOND
        assert dgs10.asset_class == AssetClass.FIXED_INCOME

        vixcls = by_symbol["VIXCLS"]
        assert vixcls.instrument_key == "FRED:INDEX:VIXCLS-USD"
        assert vixcls.instrument_type == InstrumentType.INDEX
        assert vixcls.asset_class == AssetClass.COMMODITY

    @pytest.mark.asyncio
    async def test_filters_on_instrument_type(self) -> None:
        adapter = FredReferenceDataAdapter()

        bonds = await adapter.get_instruments(instrument_type=InstrumentType.BOND)
        indices = await adapter.get_instruments(instrument_type=InstrumentType.INDEX)

        assert len(bonds) + len(indices) == len(_KEY_SERIES)
        assert all(r.instrument_type == InstrumentType.BOND for r in bonds)
        assert all(r.instrument_type == InstrumentType.INDEX for r in indices)
        assert await adapter.get_instruments(instrument_type=InstrumentType.SPOT_PAIR) == []

    @pytest.mark.asyncio
    async def test_get_instrument_by_series_id(self) -> None:
        adapter = FredReferenceDataAdapter()

        record = await adapter.get_instrument("DGS10")

        assert record is not None
        assert record.raw_symbol == "DGS10"
        assert record.instrument_key == "FRED:BOND:DGS10-USD"

    @pytest.mark.asyncio
    async def test_unsupported_operations_raise_not_implemented(self) -> None:
        """FRED series have no options/expiry/funding/reference-OHLCV surface."""
        adapter = FredReferenceDataAdapter()

        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("DGS10")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("DGS10")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("DGS10")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("DGS10")
