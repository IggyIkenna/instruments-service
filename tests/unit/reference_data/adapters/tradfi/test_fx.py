"""Unit tests — FX reference data adapter (static FX_SPOT_PAIRS universe).

FX has no vendor call: the spot-pair universe is a small, curated UAC static
list, byte-identical to what MTDS's Yahoo adapter already emits as tick rows.
Added 2026-07-23 (instruments-service NO_ADAPTER_YET -> "fx" flip).
"""

from __future__ import annotations

import pytest
from unified_api_contracts import AssetClass, InstrumentType
from unified_api_contracts.registry import FX_SPOT_PAIRS

from instruments_service.reference_data.adapters.tradfi.fx import FxReferenceDataAdapter


class TestFxReferenceDataAdapter:
    @pytest.mark.asyncio
    async def test_emits_one_record_per_curated_pair(self) -> None:
        adapter = FxReferenceDataAdapter()

        results = await adapter.get_instruments()

        assert len(results) == len(FX_SPOT_PAIRS) > 0
        assert {r.base_asset for r in results} == {p.base for p in FX_SPOT_PAIRS}

    @pytest.mark.asyncio
    async def test_canonical_identity_matches_mtds_convention(self) -> None:
        """FX:SPOT_PAIR:{BASE}-{QUOTE} — byte-identical to MTDS's
        derive_tradfi_row_instrument_id(..., venue="FX", instrument_type=SPOT_PAIR)
        output for the same pair (both resolve through build_instrument_id's
        plain VENUE:TYPE:SYMBOL branch)."""
        adapter = FxReferenceDataAdapter()

        results = await adapter.get_instruments()
        by_base = {r.base_asset: r for r in results}

        aud_usd = next(p for p in FX_SPOT_PAIRS if p.base == "AUD")
        record = by_base["AUD"]
        assert record.instrument_key == f"FX:SPOT_PAIR:{aud_usd.base}-{aud_usd.quote}"
        assert record.instrument_key == "FX:SPOT_PAIR:AUD-USD"
        assert record.canonical_instrument_id == record.instrument_key
        assert record.venue == "FX"
        assert record.instrument_type == InstrumentType.SPOT_PAIR
        assert record.asset_class == AssetClass.FX
        assert record.raw_symbol == aud_usd.yahoo_ticker
        assert record.quote_asset == aud_usd.quote

    @pytest.mark.asyncio
    async def test_filters_on_instrument_type(self) -> None:
        adapter = FxReferenceDataAdapter()

        assert len(await adapter.get_instruments(instrument_type=InstrumentType.SPOT_PAIR)) == len(FX_SPOT_PAIRS)
        assert await adapter.get_instruments(instrument_type=InstrumentType.FUTURE) == []

    @pytest.mark.asyncio
    async def test_get_instrument_by_yahoo_ticker(self) -> None:
        adapter = FxReferenceDataAdapter()
        aud_usd = next(p for p in FX_SPOT_PAIRS if p.base == "AUD")

        record = await adapter.get_instrument(aud_usd.yahoo_ticker)

        assert record is not None
        assert record.base_asset == "AUD"

    @pytest.mark.asyncio
    async def test_unsupported_operations_raise_not_implemented(self) -> None:
        """FX spot pairs have no options/expiry/funding/reference-OHLCV surface."""
        adapter = FxReferenceDataAdapter()

        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("AUD-USD")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("AUD-USD")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("AUD-USD")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("AUD-USD")
