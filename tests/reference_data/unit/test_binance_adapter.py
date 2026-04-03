"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.external.binance.market_schemas import (
    BinanceOptionInstrumentInfo,
)

from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter


class TestBinanceAdapter:
    def test_venue_name(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        assert adapter.venue == "BINANCE-SPOT"

    def test_filter_size_found(self) -> None:
        filters = [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
        ]
        result = BinanceReferenceDataAdapter._filter_size(filters, "PRICE_FILTER", "tickSize")
        assert result == "0.01"

    def test_filter_size_not_found(self) -> None:
        result = BinanceReferenceDataAdapter._filter_size([], "PRICE_FILTER", "tickSize")
        assert result == "0.001"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        with patch("aiohttp.ClientSession") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.json = AsyncMock(return_value={"symbols": []})
            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session.return_value)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_session.return_value.get = MagicMock(return_value=mock_cm)
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None


class TestBinanceAdapterExtended:
    @pytest.mark.asyncio
    async def test_fetch_spot_filters_non_trading(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        ],
                    },
                    {
                        "symbol": "OLDCOIN",
                        "status": "BREAK",
                        "baseAsset": "OLD",
                        "quoteAsset": "USDT",
                        "filters": [],
                    },
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = MagicMock(return_value=mock_cm)
        results = await adapter._fetch_spot(mock_session)
        assert len(results) == 1
        assert results[0].raw_symbol == "BTCUSDT"
        assert results[0].instrument_type == "SPOT_PAIR"
        assert results[0].tick_size == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_fetch_futures_perp_vs_future(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "filters": [],
                        "deliveryDate": 0,
                        "marginAsset": "USDT",
                    },
                    {
                        "symbol": "BTCUSDT_240329",
                        "status": "TRADING",
                        "contractType": "CURRENT_QUARTER",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "filters": [],
                        "deliveryDate": 1711670400000,
                        "marginAsset": "USDT",
                    },
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = MagicMock(return_value=mock_cm)
        results = await adapter._fetch_futures(mock_session)
        assert len(results) == 2
        perp = next(r for r in results if r.raw_symbol == "BTCUSDT")
        fut = next(r for r in results if r.raw_symbol == "BTCUSDT_240329")
        assert perp.instrument_type == "PERPETUAL"
        assert fut.instrument_type == "FUTURE"
        assert fut.expiry is not None

    @pytest.mark.asyncio
    async def test_fetch_options_filters_no_expiry(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "optionSymbols": [
                    {
                        "symbol": "BTC-240329-50000-C",
                        "underlying": "BTCUSDT",
                        "quoteAsset": "USDT",
                        "expiryDate": 1711670400000,
                        "strikePrice": "50000",
                        "side": "CALL",
                        "priceScale": 2,
                        "quantityScale": 3,
                        "minQty": "0.001",
                    },
                    {
                        "symbol": "NO-EXPIRY",
                        "underlying": "BTCUSDT",
                        "quoteAsset": "USDT",
                        # No expiryDate key
                    },
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = MagicMock(return_value=mock_cm)
        results = await adapter._fetch_options(mock_session)
        assert len(results) == 1
        assert results[0].raw_symbol == "BTC-240329-50000-C"

    def test_parse_option_symbol(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        sym = BinanceOptionInstrumentInfo.model_validate(
            {
                "symbol": "BTC-240329-50000-C",
                "underlying": "BTCUSDT",
                "expiryDate": 1711670400000,
                "strikePrice": "50000",
                "side": "CALL",
                "priceScale": 2,
                "quantityScale": 3,
                "minQty": "0.001",
            }
        )
        inst = adapter._parse_option_symbol(sym)
        assert inst.instrument_type == "OPTION"
        assert inst.strike == Decimal("50000")
        assert inst.option_type == "call"
        assert inst.expiry is not None

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT_240329",
                        "baseAsset": "BTC",
                        "status": "TRADING",
                        "contractType": "CURRENT_QUARTER",
                        "deliveryDate": 1711670400000,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "baseAsset": "ETH",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "deliveryDate": 0,
                    },
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
            calendar = await adapter.get_expiry_calendar("BTC")
        assert calendar.venue == "BINANCE-SPOT"
        assert calendar.underlying == "BTC"
        assert len(calendar.expiries) == 1

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = BinanceReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "optionSymbols": [
                    {
                        "symbol": "BTC-240329-50000-C",
                        "underlying": "BTCUSDT",
                        "quoteAsset": "USDT",
                        "expiryDate": 1711670400000,
                        "strikePrice": "50000",
                        "side": "CALL",
                        "priceScale": 2,
                        "quantityScale": 3,
                        "minQty": "0.001",
                    },
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
            chain = await adapter.get_options_chain("BTCUSDT")
        assert chain.venue == "BINANCE-SPOT"
        assert chain.underlying == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate is implemented — mock the HTTP call."""
        adapter = BinanceReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={"lastFundingRate": "0.0001", "nextFundingTime": 0, "markPrice": "30000"}
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTCUSDT")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented — mock the HTTP call."""
        adapter = BinanceReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTCUSDT")
        assert isinstance(result, list)
