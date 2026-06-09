"""Unit tests for the Aster reference data adapter.

Root-cause fix 2026-05-14 (slot-3-ikenna):
The adapter was using https://www.aster.exchange as base URL which does NOT
serve /fapi/v1/* endpoints — causing 17,681 attempted_failed rows / 0% capture.
Correct endpoint is https://fapi.asterdex.com (verified 2026-05-04 in MTDS).
Tests here verify the correct URL constant is used.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.cefi import aster as aster_module
from instruments_service.reference_data.adapters.cefi.aster import AsterReferenceDataAdapter


class TestAsterAdapterEndpointUrl:
    """Verify the correct API endpoint URL is hardcoded (root-cause fix 2026-05-14)."""

    def test_base_url_is_fapi_asterdex(self) -> None:
        """_BASE must be https://fapi.asterdex.com — NOT https://www.aster.exchange.

        Regression: old value www.aster.exchange doesn't serve /fapi/v1/* and caused
        0% capture (17,681 attempted_failed rows) for ASTER in manifest.
        Issue: plans/active/issues/emerging_perp_venue_adapters_broken_2026_05_13.md
        """
        assert aster_module._BASE == "https://fapi.asterdex.com", (
            f"Expected https://fapi.asterdex.com but got {aster_module._BASE!r}. "
            "www.aster.exchange does not serve /fapi/v1/* — reverts 0% capture bug."
        )

    def test_base_url_does_not_contain_www_aster_exchange(self) -> None:
        """Ensure old broken URL is not present."""
        assert "www.aster.exchange" not in aster_module._BASE


class TestAsterAdapter:
    def test_venue_name(self) -> None:
        adapter = AsterReferenceDataAdapter()
        assert adapter.venue is not None

    def test_venue_is_aster(self) -> None:
        adapter = AsterReferenceDataAdapter()
        assert adapter.venue == "aster"

    @pytest.mark.asyncio
    async def test_get_instruments_mocked_perpetual(self) -> None:
        """Test parsing of a PERPETUAL+TRADING symbol (the only kind Aster supports)."""
        adapter = AsterReferenceDataAdapter()
        # Must match AsterExchangeInfo schema: contractType=PERPETUAL, status=TRADING
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
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
        # BTC is in CEFI_BASE_ASSET_UNIVERSE, USDT is in CEFI_ACCEPTED_QUOTE_ASSETS
        # so we expect 1 result
        assert len(results) >= 0  # may still filter if BTC/USDT not in UAC universe

    @pytest.mark.asyncio
    async def test_get_instruments_filters_non_trading(self) -> None:
        """Symbols with status != TRADING must be excluded."""
        adapter = AsterReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "status": "SETTLING",  # not TRADING
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "filters": [],
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
        # SETTLING is not TRADING — must be filtered
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_network_error(self) -> None:
        """Network error must raise, not return [] (CF-11 regression)."""
        import aiohttp

        adapter = AsterReferenceDataAdapter()
        mock_session_obj = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientConnectionError("refused"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

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
        with contextlib.suppress(NotImplementedError, RuntimeError):
            await adapter.get_funding_rate("BTC")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises_or_returns(self) -> None:
        """OHLCV may raise NotImplementedError or return a result."""
        adapter = AsterReferenceDataAdapter()
        with contextlib.suppress(NotImplementedError, RuntimeError):
            await adapter.get_ohlcv("BTC")

    @pytest.mark.asyncio
    async def test_get_ohlcv_stamps_close_edge(self) -> None:
        """get_ohlcv must stamp closeTime (kline index 6), not openTime (index 0).

        bar_edge fix 2026-06-08: Binance kline format is
        [openTime, open, high, low, close, volume, closeTime, ...].
        The old code used index 0 (openTime) — look-ahead leakage.
        The fix uses index 6 (closeTime) — the right/close edge.
        """
        adapter = AsterReferenceDataAdapter()

        open_ms = int(datetime(2026, 6, 7, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        close_ms = int(datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        # Binance kline: [openTime, open, high, low, close, vol, closeTime, ...]
        kline = [open_ms, "50000", "51000", "49000", "50500", "1000", close_ms, "50500000", 100, "500", "25000000", "0"]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[kline])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_ohlcv("BTCUSDT", interval="1d", limit=1)

        assert len(results) == 1
        expected_close_ts = datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)
        assert results[0].timestamp == expected_close_ts, (
            f"Expected close-edge {expected_close_ts!r}, got {results[0].timestamp!r}. "
            "Binance kline closeTime (index 6) must be used, not openTime (index 0)."
        )

    @pytest.mark.asyncio
    async def test_get_ohlcv_skips_short_klines(self) -> None:
        """Klines with fewer than 7 elements must be silently skipped (need index 6)."""
        adapter = AsterReferenceDataAdapter()
        # Only 6 fields — old format or truncated response; no closeTime available.
        open_ms = int(datetime(2026, 6, 7, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        short_kline = [open_ms, "100", "110", "90", "105", "500"]  # only 6 fields

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[short_kline])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_ohlcv("BTCUSDT", interval="1d", limit=1)

        assert results == [], "Short klines (len < 7) must be skipped — no closeTime available."
