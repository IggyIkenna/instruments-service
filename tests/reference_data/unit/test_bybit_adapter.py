"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.bybit import BybitReferenceDataAdapter


class TestBybitAdapter:
    def test_venue_name(self) -> None:
        adapter = BybitReferenceDataAdapter()
        assert adapter.venue == "BYBIT-SPOT"


class TestBybitAdapterExtended:
    def _make_bybit_session(self, symbol_list: list[dict[str, object]]) -> MagicMock:
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"result": {"list": symbol_list}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        return mock_session_cm

    @pytest.mark.asyncio
    async def test_fetch_category_spot(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_bybit_session(
            [
                {
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "contractType": "",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceFilter": {"tickSize": "0.01"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                    "deliveryTime": "0",
                }
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert len(results) == 1
        assert results[0].raw_symbol == "BTCUSDT"
        assert results[0].instrument_type == "SPOT_PAIR"

    @pytest.mark.asyncio
    async def test_fetch_category_linear_perp(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_bybit_session(
            [
                {
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "contractType": "LinearPerpetual",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceFilter": {"tickSize": "0.5"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                    "deliveryTime": "0",
                    "settleCoin": "USDT",
                }
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert len(results) == 1
        assert results[0].instrument_type == "PERPETUAL"

    @pytest.mark.asyncio
    async def test_fetch_category_filters_non_trading(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_bybit_session(
            [
                {
                    "symbol": "DEADCOIN",
                    "status": "Delisted",
                    "contractType": "",
                    "baseCoin": "DEAD",
                    "quoteCoin": "USDT",
                    "priceFilter": None,
                    "lotSizeFilter": None,
                    "deliveryTime": "0",
                }
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_category_with_expiry(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_bybit_session(
            [
                {
                    "symbol": "BTCUSDT-240329",
                    "status": "Trading",
                    "contractType": "LinearFutures",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceFilter": {"tickSize": "0.5"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                    "deliveryTime": "1711670400000",
                    "settleCoin": "USDT",
                }
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="FUTURE")
        assert len(results) == 1
        assert results[0].instrument_type == "FUTURE"
        assert results[0].expiry is not None

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate is implemented — mock the HTTP call."""
        adapter = BybitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": {"list": [{"fundingRate": "0.0001", "fundingRateTimestamp": "0"}]},
                "retCode": 0,
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
            result = await adapter.get_funding_rate("BTCUSDT")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented — mock the HTTP call."""
        adapter = BybitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": {"list": []}, "retCode": 0})
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

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_bars(self) -> None:
        """get_ohlcv parses bar arrays into OHLCVRef list (newest-first reversed)."""
        adapter = BybitReferenceDataAdapter()
        # Bybit returns bars newest-first; adapter reverses to oldest-first
        bars = [
            ["1700086400000", "31000", "32000", "30500", "31500", "200", "6000000"],
            ["1700000000000", "30000", "31000", "29500", "30500", "100", "3000000"],
        ]
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": {"list": bars}, "retCode": 0})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTCUSDT")
        assert len(result) == 2
        # oldest-first: first bar should be the one with ts 1700000000000
        assert result[0].open.compare(result[1].open) < 0  # 30000 < 31000

    @pytest.mark.asyncio
    async def test_get_funding_rate_with_timestamp(self) -> None:
        """get_funding_rate parses non-zero fundingRateTimestamp as next_funding_time."""
        adapter = BybitReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": {
                    "list": [
                        {
                            "fundingRate": "0.0003",
                            "fundingRateTimestamp": "1700000000000",
                        }
                    ]
                },
                "retCode": 0,
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
            result = await adapter.get_funding_rate("BTCUSDT")
        assert result.venue == "BYBIT-SPOT"
        from decimal import Decimal

        assert result.rate == Decimal("0.0003")


class TestBybitAdapterCoverage:
    """Additional coverage tests for uncovered branches in BybitReferenceDataAdapter."""

    @pytest.mark.asyncio
    async def test_get_exchange_fee_schedule_returns_none(self) -> None:
        """get_exchange_fee_schedule logs a warning and returns None (stub)."""
        adapter = BybitReferenceDataAdapter()
        result = await adapter.get_exchange_fee_schedule("client-123")
        assert result is None

    def test_parse_bybit_bar_invalid_returns_none(self) -> None:
        """_parse_bybit_bar returns None for non-list or short bar."""
        adapter = BybitReferenceDataAdapter()
        assert adapter._parse_bybit_bar("not-a-list", "BTCUSDT", "1d") is None
        assert adapter._parse_bybit_bar(["only-one"], "BTCUSDT", "1d") is None
        assert adapter._parse_bybit_bar(None, "BTCUSDT", "1d") is None

    @pytest.mark.asyncio
    async def test_get_options_chain_with_puts_and_expiry_filter(self) -> None:
        """get_options_chain filters by expiry date and separates calls/puts."""
        now = datetime.now(UTC)
        call_inst = InstrumentRecord(
            instrument_key="BTC-31DEC24-50000-C",
            venue="bybit",
            raw_symbol="BTC-31DEC24-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USDC",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            contract_size=Decimal("1"),
            expiry=now,
            strike=Decimal("50000"),
            option_type="call",
        )
        put_inst = InstrumentRecord(
            instrument_key="BTC-31DEC24-50000-P",
            venue="bybit",
            raw_symbol="BTC-31DEC24-50000-P",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USDC",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            contract_size=Decimal("1"),
            expiry=now,
            strike=Decimal("50000"),
            option_type="put",
        )
        wrong_base_inst = InstrumentRecord(
            instrument_key="ETH-31DEC24-3000-C",
            venue="bybit",
            raw_symbol="ETH-31DEC24-3000-C",
            instrument_type="OPTION",
            base_asset="ETH",
            quote_asset="USDC",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            contract_size=Decimal("1"),
            expiry=now,
            strike=Decimal("3000"),
            option_type="call",
        )
        adapter = BybitReferenceDataAdapter()
        with patch.object(
            adapter,
            "get_instruments",
            return_value=[call_inst, put_inst, wrong_base_inst],
        ):
            chain = await adapter.get_options_chain("BTC")
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert chain.venue == "BYBIT-SPOT"

    @pytest.mark.asyncio
    async def test_get_options_chain_expiry_mismatch_filtered(self) -> None:
        """get_options_chain filters out instruments with non-matching expiry date."""
        now = datetime.now(UTC)
        future_expiry = now + timedelta(days=30)
        inst = InstrumentRecord(
            instrument_key="BTC-31DEC24-50000-C",
            venue="bybit",
            raw_symbol="BTC-31DEC24-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USDC",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            contract_size=Decimal("1"),
            expiry=future_expiry,
            strike=Decimal("50000"),
            option_type="call",
        )
        adapter = BybitReferenceDataAdapter()
        # Request chain with a specific expiry that doesn't match inst.expiry
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            chain = await adapter.get_options_chain("BTC", expiry=now)
        # Instrument filtered out because expiry date doesn't match
        assert len(chain.calls) == 0

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_non_matching_base(self) -> None:
        """get_expiry_calendar skips instruments with non-matching base_asset."""
        now = datetime.now(UTC)
        expiry_dt = now + timedelta(days=90)
        eth_inst = InstrumentRecord(
            instrument_key="ETHUSD-240329",
            venue="bybit",
            raw_symbol="ETHUSD-240329",
            instrument_type="FUTURE",
            base_asset="ETH",
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            contract_size=Decimal("1"),
            expiry=expiry_dt,
        )
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[eth_inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="FUTURE")
        # ETH instrument should not appear in BTC calendar
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_matching_base(self) -> None:
        """get_expiry_calendar includes expiry for instruments with matching base_asset."""
        now = datetime.now(UTC)
        expiry_dt = now + timedelta(days=90)
        btc_inst = InstrumentRecord(
            instrument_key="BTCUSD-240329",
            venue="bybit",
            raw_symbol="BTCUSD-240329",
            instrument_type="FUTURE",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.5"),
            lot_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=expiry_dt,
        )
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[btc_inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="FUTURE")
        assert len(calendar.expiries) == 1
        assert calendar.expiries[0] == expiry_dt

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """get_instrument returns the matching InstrumentRecord when symbol exists."""
        btc_inst = InstrumentRecord(
            instrument_key="BTCUSDT",
            venue="bybit",
            raw_symbol="BTCUSDT",
            instrument_type="PERPETUAL",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.5"),
            lot_size=Decimal("0.001"),
            contract_size=Decimal("1"),
        )
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[btc_inst]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is not None
        assert result.raw_symbol == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        """get_instrument returns None when no instrument matches the symbol."""
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NONEXISTENT")
        assert result is None


# ---------------------------------------------------------------------------
# Router tests
# ---------------------------------------------------------------------------
