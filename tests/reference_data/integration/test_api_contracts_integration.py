"""Integration tests validating CCXT API response shapes against api-contracts schemas.

Follows UTEI_URDI_UDEI_UPI_VERSION_ALIGNMENT_REPORT.md pattern.
Uses unittest.mock to patch CCXT exchange methods — no real network calls required.
Validates that CcxtMarket and CcxtTicker Pydantic models accept the canonical CCXT
response structure returned by fetch_markets / fetch_ticker.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("unified_api_contracts")
pytest.importorskip("ccxt")

import ccxt.async_support as ccxt
from unified_api_contracts.ccxt.schemas import CcxtMarket, CcxtTicker

# ---------------------------------------------------------------------------
# Representative fixtures matching real CCXT fetch_markets / fetch_ticker shapes
# ---------------------------------------------------------------------------

_MARKET_RAW: dict[str, Any] = {
    "id": "BTCUSDT",
    "symbol": "BTC/USDT",
    "base": "BTC",
    "quote": "USDT",
    "active": True,
    "type": "spot",
    "spot": True,
    "futures": False,
    "swap": False,
    "margin": True,
    "contract": False,
    "contractSize": None,
    "expiry": None,
    "expiryDatetime": None,
    "settle": None,
    "settleId": None,
    "linear": None,
    "inverse": None,
    "precision": {"amount": 6, "price": 2, "cost": 8},
    "limits": {
        "amount": {"min": 0.00001, "max": 9000.0},
        "price": {"min": 0.01, "max": 1000000.0},
        "cost": {"min": 10.0, "max": None},
    },
    "percentage": True,
    "fees": {"taker": 0.001, "maker": 0.001},
    "info": {"symbol": "BTCUSDT", "status": "TRADING"},
}

_TICKER_RAW: dict[str, Any] = {
    "symbol": "BTC/USDT",
    "last": 65000.0,
    "bid": 64990.0,
    "ask": 65010.0,
    "high": 66000.0,
    "low": 63000.0,
    "volume": 12345.67,
    "info": {"symbol": "BTCUSDT", "lastPrice": "65000.00"},
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_markets_validates_against_ccxt_market() -> None:
    """Verify CcxtMarket.model_validate accepts the canonical CCXT fetch_markets shape."""
    mock_exchange = MagicMock()
    mock_exchange.fetch_markets = AsyncMock(return_value=[_MARKET_RAW])
    mock_exchange.close = AsyncMock()

    with patch.object(ccxt, "binance", return_value=mock_exchange):
        exchange = ccxt.binance({"enableRateLimit": True})
        markets = await exchange.fetch_markets()
        for raw in markets:
            market_dict = dict(raw)
            market = CcxtMarket.model_validate(market_dict)
            assert market.id is not None or market.symbol is not None or market.info is not None
        await exchange.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_ticker_validates_against_ccxt_ticker() -> None:
    """Verify CcxtTicker.model_validate accepts the canonical CCXT fetch_ticker shape."""
    mock_exchange = MagicMock()
    mock_exchange.fetch_ticker = AsyncMock(return_value=_TICKER_RAW)
    mock_exchange.close = AsyncMock()

    with patch.object(ccxt, "binance", return_value=mock_exchange):
        exchange = ccxt.binance({"enableRateLimit": True})
        ticker = await exchange.fetch_ticker("BTC/USDT")
        ticker_dict = dict(ticker)
        validated = CcxtTicker.model_validate(ticker_dict)
        assert validated.symbol is not None or validated.last is not None or validated.info is not None
        await exchange.close()
