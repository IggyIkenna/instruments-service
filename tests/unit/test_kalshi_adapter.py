"""Unit tests for the Kalshi prediction market adapter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.prediction.kalshi import KalshiReferenceDataAdapter


class TestKalshiAdapter:
    def test_venue_name(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        assert adapter.venue == "kalshi"

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTC-26MAR-90000",
                        "event_ticker": "KXBTC",
                        "title": "BTC above $90,000?",
                        "subtitle": "",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 65,
                        "yes_ask": 68,
                        "no_bid": 32,
                        "no_ask": 35,
                        "open_time": "2026-03-25T00:00:00Z",
                        "close_time": "2026-03-26T00:00:00Z",
                        "expiration_time": "2026-03-26T23:59:59Z",
                    }
                ],
                "cursor": "",
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
        assert len(results) >= 1
        assert results[0].venue == "kalshi"

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "market": {
                    "ticker": "KXBTC-26MAR-90000",
                    "event_ticker": "KXBTC",
                    "title": "BTC above $90,000?",
                    "category": "Crypto",
                    "status": "active",
                    "yes_bid": 65,
                    "yes_ask": 68,
                }
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
            result = await adapter.get_instrument("KXBTC-26MAR-90000")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("BTC")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("BTC")
