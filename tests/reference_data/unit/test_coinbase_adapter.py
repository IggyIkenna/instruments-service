"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.coinbase import CoinbaseReferenceDataAdapter

# ---------------------------------------------------------------------------
# Helpers shared across TestCoinbaseAdapterExtended
# ---------------------------------------------------------------------------

_COINBASE_PRODUCTS_RESPONSE: dict[str, object] = {
    "products": [
        {
            "product_id": "BTC-USD",
            "base_currency_id": "BTC",
            "quote_currency_id": "USD",
            "status": "online",
            "product_type": "SPOT",
            "quote_increment": "0.01",
            "base_increment": "0.00000001",
            "base_min_size": "0.001",
        }
    ]
}


def _make_coinbase_session_mock(resp_data: dict[str, object], status: int = 200) -> MagicMock:
    """Return an aiohttp.ClientSession mock that yields resp_data as JSON."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=resp_data)
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    return mock_session


class TestCoinbaseAdapter:
    def test_venue_name(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        assert adapter.venue == "coinbase"

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")


class TestCoinbaseAdapterExtended:
    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        mock_session_cm = _make_coinbase_session_mock(_COINBASE_PRODUCTS_RESPONSE)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].raw_symbol == "BTC-USD"
        assert results[0].instrument_type == "SPOT_PAIR"

    @pytest.mark.asyncio
    async def test_get_instruments_non_spot_returns_empty(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_mocked_found(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        resp_data = {
            "product_id": "BTC-USD",
            "base_currency_id": "BTC",
            "quote_currency_id": "USD",
            "quote_increment": "0.01",
            "base_increment": "0.00000001",
            "base_min_size": "0.001",
        }
        mock_session_cm = _make_coinbase_session_mock(resp_data, status=200)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            inst = await adapter.get_instrument("BTC-USD")
        assert inst is not None
        assert inst.raw_symbol == "BTC-USD"
        assert inst.base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_get_instrument_mocked_404(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        mock_session_cm = _make_coinbase_session_mock({}, status=404)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            inst = await adapter.get_instrument("NOTEXIST-USD")
        assert inst is None

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = CoinbaseReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("BTC-USD")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented — mock the HTTP call."""
        adapter = CoinbaseReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"candles": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTC-USD")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_candles(self) -> None:
        """get_ohlcv parses candle data into OHLCVRef list (newest-first reversed)."""
        adapter = CoinbaseReferenceDataAdapter()
        # Coinbase returns newest-first; adapter reverses to oldest-first
        candles = [
            {
                "start": "1700086400",
                "open": "31000",
                "high": "32000",
                "low": "30500",
                "close": "31500",
                "volume": "200",
            },
            {
                "start": "1700000000",
                "open": "30000",
                "high": "31000",
                "low": "29500",
                "close": "30500",
                "volume": "100",
            },
        ]
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"candles": candles})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTC-USD")
        assert len(result) == 2
        assert result[0].venue == "coinbase"


# ---------------------------------------------------------------------------
# Extended OKX adapter tests
# ---------------------------------------------------------------------------
