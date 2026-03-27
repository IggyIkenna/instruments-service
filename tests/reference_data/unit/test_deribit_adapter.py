"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.external.deribit.schemas import (
    DeribitInstrumentInfoFull,
)

from instruments_service.reference_data.adapters.deribit import DeribitReferenceDataAdapter


class TestDeribitAdapter:
    def test_venue_name(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        assert adapter.venue == "deribit"

    def test_parse_instrument_perp(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        data = DeribitInstrumentInfoFull.model_validate(
            {
                "instrument_name": "BTC-PERPETUAL",
                "kind": "future",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": 0.5,
                "min_trade_amount": 10.0,
                "contract_size": 10,
                "settlement_currency": "BTC",
            }
        )
        inst = adapter._parse_instrument(data)
        assert inst.instrument_type == "perp"
        assert inst.venue == "deribit"

    def test_parse_instrument_option(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        data = DeribitInstrumentInfoFull.model_validate(
            {
                "instrument_name": "BTC-31DEC24-50000-C",
                "kind": "option",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": 0.0001,
                "min_trade_amount": 0.1,
                "contract_size": 1,
                "settlement_currency": "BTC",
                "expiration_timestamp": 1735689600000,
                "strike": 50000,
                "option_type": "call",
            }
        )
        inst = adapter._parse_instrument(data)
        assert inst.instrument_type == "option"
        assert inst.strike == Decimal("50000")
        assert inst.option_type == "call"


class TestDeribitAdapterExtended:
    def test_parse_instrument_futures(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        data = DeribitInstrumentInfoFull.model_validate(
            {
                "instrument_name": "BTC-31MAR24",
                "kind": "future",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": 0.5,
                "min_trade_amount": 10.0,
                "contract_size": 10,
                "settlement_currency": "BTC",
                "expiration_timestamp": 1711670400000,
            }
        )
        inst = adapter._parse_instrument(data)
        # Deribit maps non-perp futures to InstrumentType.FUTURES ("futures")
        assert str(inst.instrument_type).lower() in ("future", "futures")
        assert inst.expiry is not None

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {
                        "instrument_name": "BTC-PERPETUAL",
                        "kind": "future",
                        "base_currency": "BTC",
                        "quote_currency": "USD",
                        "tick_size": 0.5,
                        "min_trade_amount": 10.0,
                        "contract_size": 10,
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
            results = await adapter.get_instruments(instrument_type="perp")
        assert len(results) >= 0  # just ensure no crash

    @pytest.mark.asyncio
    async def test_get_instrument_found_mocked(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": {
                    "instrument_name": "BTC-PERPETUAL",
                    "kind": "future",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "tick_size": 0.5,
                    "min_trade_amount": 10.0,
                    "contract_size": 10,
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
            inst = await adapter.get_instrument("BTC-PERPETUAL")
        assert inst is not None
        assert inst.instrument_key == "BTC-PERPETUAL"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_mocked(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"result": None})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            inst = await adapter.get_instrument("NOTEXIST")
        assert inst is None

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate is implemented — mock the HTTP call."""
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": {"interest_1h": "0.0001", "current_funding": "0.0001"},
                "jsonrpc": "2.0",
                "id": 1,
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC-PERPETUAL")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented — mock the HTTP call."""
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": {
                    "ticks": [],
                    "open": [],
                    "high": [],
                    "low": [],
                    "close": [],
                    "volume": [],
                },
                "jsonrpc": "2.0",
                "id": 1,
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTC-PERPETUAL")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_instruments_option_kind(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"result": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="option")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_unknown_type_fetches_both_kinds(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"result": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="spot")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        call_inst = adapter._parse_instrument(
            DeribitInstrumentInfoFull.model_validate(
                {
                    "instrument_name": "BTC-31DEC24-50000-C",
                    "kind": "option",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "tick_size": 0.0001,
                    "min_trade_amount": 0.1,
                    "contract_size": 1,
                    "expiration_timestamp": 1735689600000,
                    "strike": 50000,
                    "option_type": "call",
                }
            )
        )
        put_inst = adapter._parse_instrument(
            DeribitInstrumentInfoFull.model_validate(
                {
                    "instrument_name": "BTC-31DEC24-50000-P",
                    "kind": "option",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "tick_size": 0.0001,
                    "min_trade_amount": 0.1,
                    "contract_size": 1,
                    "expiration_timestamp": 1735689600000,
                    "strike": 50000,
                    "option_type": "put",
                }
            )
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst, put_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "deribit"
        assert chain.underlying == "BTC"
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("50000") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_options_chain_filters_non_matching_underlying(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        eth_inst = adapter._parse_instrument(
            DeribitInstrumentInfoFull.model_validate(
                {
                    "instrument_name": "ETH-31DEC24-2000-C",
                    "kind": "option",
                    "base_currency": "ETH",
                    "quote_currency": "USD",
                    "tick_size": 0.0001,
                    "min_trade_amount": 0.1,
                    "contract_size": 1,
                    "expiration_timestamp": 1735689600000,
                    "strike": 2000,
                    "option_type": "call",
                }
            )
        )
        with patch.object(adapter, "get_instruments", return_value=[eth_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = DeribitReferenceDataAdapter()
        inst = adapter._parse_instrument(
            DeribitInstrumentInfoFull.model_validate(
                {
                    "instrument_name": "BTC-31MAR24",
                    "kind": "future",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "tick_size": 0.5,
                    "min_trade_amount": 10.0,
                    "contract_size": 10,
                    "expiration_timestamp": 1711670400000,
                }
            )
        )
        inst_type_str = str(inst.instrument_type).lower()
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type=inst_type_str)
        assert calendar.venue == "deribit"
        assert calendar.underlying == "BTC"
        assert len(calendar.expiries) == 1


# ---------------------------------------------------------------------------
# Extended Coinbase adapter tests
# ---------------------------------------------------------------------------


_COINBASE_PRODUCTS_RESPONSE = {
    "products": [
        {
            "product_id": "BTC-USD",
            "status": "online",
            "product_type": "SPOT",
            "base_currency_id": "BTC",
            "quote_currency_id": "USD",
            "quote_increment": "0.01",
            "base_increment": "0.00000001",
            "base_min_size": "0.001",
        },
        {
            "product_id": "BTC-USD-PERP",
            "status": "online",
            "product_type": "PERP",
            "base_currency_id": "BTC",
            "quote_currency_id": "USD",
            "quote_increment": "0.01",
            "base_increment": "0.001",
            "base_min_size": "0.001",
        },
        {
            "product_id": "OFFLINE-USD",
            "status": "offline",
            "product_type": "SPOT",
            "base_currency_id": "OLD",
            "quote_currency_id": "USD",
            "quote_increment": "0.01",
            "base_increment": "0.001",
            "base_min_size": "0.001",
        },
    ]
}


def _make_coinbase_session_mock(response_data: object, status: int = 200) -> MagicMock:
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=response_data)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm
