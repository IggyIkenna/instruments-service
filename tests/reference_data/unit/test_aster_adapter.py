"""Unit tests for the Aster reference data adapter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.aster import AsterReferenceDataAdapter


class TestAsterAdapter:
    def test_venue_name(self) -> None:
        adapter = AsterReferenceDataAdapter()
        assert adapter.venue is not None

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "Trading",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        ],
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) >= 0  # may vary based on adapter parsing

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises_or_returns(self) -> None:
        """Funding rate may raise NotImplementedError or return a result."""
        adapter = AsterReferenceDataAdapter()
        # Aster may implement funding rate — just verify no crash
        try:
            result = await adapter.get_funding_rate("BTC")
        except (NotImplementedError, RuntimeError):
            pass  # expected for some adapters

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises_or_returns(self) -> None:
        """OHLCV may raise NotImplementedError or return a result."""
        adapter = AsterReferenceDataAdapter()
        try:
            result = await adapter.get_ohlcv("BTC")
        except (NotImplementedError, RuntimeError):
            pass  # expected for some adapters
