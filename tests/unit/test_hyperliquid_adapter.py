"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from unittest.mock import patch

import pytest

from instruments_service.reference_data.adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter


class TestHyperliquidAdapter:
    def test_venue_name(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter.venue == "HYPERLIQUID"

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_instruments_non_perp_returns_empty(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with patch("aiohttp.ClientSession"):
            result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == []
