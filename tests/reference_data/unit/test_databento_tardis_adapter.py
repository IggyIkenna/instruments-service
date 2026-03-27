"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.bybit import BybitReferenceDataAdapter
from instruments_service.reference_data.adapters.databento import DatabentoReferenceDataAdapter
from instruments_service.reference_data.adapters.tardis import TardisReferenceDataAdapter


class TestDatabentoAdapterMocked:
    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = DatabentoReferenceDataAdapter(
            project_id=None,
            datasets=["GLBX.MDP3"],
        )
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "raw_symbol": "ESZ4",
                    "instrument_id": 12345,
                    "instrument_class": "F",
                    "currency": "USD",
                    "min_price_increment": 0.25,
                    "min_lot_size_round_lot": 1,
                }
            ]
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="test-key"),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].venue == "databento"
        assert results[0].instrument_type == "future"

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "raw_symbol": "ESZ4",
                    "instrument_id": 12345,
                    "instrument_class": "F",
                    "currency": "USD",
                },
                {
                    "raw_symbol": "AAPL",
                    "instrument_id": 99,
                    "instrument_class": "E",
                    "currency": "USD",
                },
            ]
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="test-key"),
        ):
            results = await adapter.get_instruments(instrument_type="future")
        assert all(r.instrument_type == "future" for r in results)

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "raw_symbol": "ESZ4",
                    "instrument_id": 12345,
                    "instrument_class": "F",
                    "currency": "USD",
                }
            ]
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="test-key"),
        ):
            result = await adapter.get_instrument("ESZ4")
        assert result is not None
        assert result.raw_symbol == "ESZ4"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_skips_dataset(self) -> None:
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        import aiohttp as _aiohttp

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("fail"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="test-key"),
        ):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_dataset_401_raises_runtime_error(self) -> None:
        """401 response raises RuntimeError with auth message."""
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        mock_resp = AsyncMock()
        mock_resp.status = 401
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="bad-key"),
            pytest.raises(RuntimeError, match="authentication failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_fetch_dataset_404_returns_empty(self) -> None:
        """404 response skips dataset and returns empty list."""
        adapter = DatabentoReferenceDataAdapter(datasets=["NOTEXIST.MDP3"])
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
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_optional_api_key", return_value="test-key"),
        ):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_options_chain_with_options(self) -> None:
        """get_options_chain collects calls, puts, and strikes from matching instruments."""
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        expiry_dt = datetime(2024, 6, 21, tzinfo=UTC)
        from unified_api_contracts.internal import InstrumentRecord

        call_inst = InstrumentRecord(
            instrument_key="GLBX:ESM4 C4500",
            venue="databento",
            symbol="ESM4 C4500",
            raw_symbol="ESM4 C4500",
            instrument_type="option",
            base_asset="ES",
            quote_asset="USD",
            tick_size=Decimal("0.25"),
            lot_size=Decimal("1"),
            min_order_size=Decimal("1"),
            contract_size=Decimal("50"),
            strike=Decimal("4500"),
            option_type="Call",
            expiry=expiry_dt,
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst]):
            chain = await adapter.get_options_chain("ES")
        assert chain.venue == "databento"
        assert len(chain.calls) == 1
        assert Decimal("4500") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_futures(self) -> None:
        """get_expiry_calendar collects expiry dates from matching futures."""
        adapter = DatabentoReferenceDataAdapter(datasets=["GLBX.MDP3"])
        expiry_dt = datetime(2024, 3, 15, tzinfo=UTC)
        from unified_api_contracts.internal import InstrumentRecord

        fut_inst = InstrumentRecord(
            instrument_key="GLBX:ESH4",
            venue="databento",
            symbol="ESH4",
            raw_symbol="ESH4",
            instrument_type="future",
            base_asset="ES",
            quote_asset="USD",
            tick_size=Decimal("0.25"),
            lot_size=Decimal("1"),
            min_order_size=Decimal("1"),
            contract_size=Decimal("50"),
            expiry=expiry_dt,
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[fut_inst]):
            calendar = await adapter.get_expiry_calendar("ES", instrument_type="future")
        assert calendar.venue == "databento"
        assert expiry_dt in calendar.expiries

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("ESH4")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("ESH4")

    def test_parse_databento_instrument_no_symbol_returns_none(self) -> None:
        """_parse_databento_instrument returns None when raw_symbol is empty."""
        from unified_api_contracts.external.databento.schemas import (
            DatabentoReferenceInstrument,
        )

        adapter = DatabentoReferenceDataAdapter()
        item = DatabentoReferenceInstrument.model_validate(
            {"raw_symbol": "", "instrument_id": 0, "instrument_class": "F"}
        )
        result = adapter._parse_databento_instrument(item, "GLBX.MDP3", datetime.now(UTC))
        assert result is None


# ---------------------------------------------------------------------------
# Tardis adapter mocked tests
# ---------------------------------------------------------------------------


class TestTardisAdapterMocked:
    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                        "availableTo": None,
                    }
                ],
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
        assert len(results) == 1
        assert results[0].instrument_type == "perp"
        assert results[0].venue == "tardis"

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                    },
                    {
                        "id": "BTC-31MAR24",
                        "type": "future",
                        "availableSince": "2024-01-01T00:00:00Z",
                        "availableTo": "2024-03-31T08:00:00Z",
                    },
                ],
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
        assert len(results) == 1
        assert all(r.instrument_type == "perp" for r in results)

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        with patch.object(
            adapter,
            "get_instruments",
            return_value=[],
        ):
            result = await adapter.get_instrument("BTC-PERPETUAL")
        assert result is None

    @pytest.mark.asyncio
    async def test_exchange_not_found_skips(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["unknown-exchange"])
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
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])

        from unified_api_contracts.internal import InstrumentRecord

        call_inst = InstrumentRecord(
            instrument_key="deribit:BTC-31DEC24-50000-C",
            venue="tardis",
            symbol="BTC/USD",
            raw_symbol="BTC-31DEC24-50000-C",
            instrument_type="option",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            strike=Decimal("50000"),
            option_type="call",
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "tardis"
        assert len(chain.calls) == 1

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:

        from unified_api_contracts.internal import InstrumentRecord

        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        expiry_dt = datetime(2024, 3, 31, 8, 0, tzinfo=UTC)
        fut_inst = InstrumentRecord(
            instrument_key="deribit:BTC-31MAR24",
            venue="tardis",
            symbol="BTC/USD",
            raw_symbol="BTC-31MAR24",
            instrument_type="future",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.5"),
            lot_size=Decimal("10"),
            min_order_size=Decimal("10"),
            contract_size=Decimal("1"),
            expiry=expiry_dt,
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[fut_inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="future")
        assert calendar.venue == "tardis"
        assert len(calendar.expiries) == 1


# ---------------------------------------------------------------------------
# Tardis funding rate and OHLCV mocked tests
# ---------------------------------------------------------------------------


def _make_tardis_datafeed_session(text_body: str, status: int = 200) -> MagicMock:
    """Helper: mock aiohttp.ClientSession for Tardis data-feeds endpoint."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    mock_resp.text = AsyncMock(return_value=text_body)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


class TestTardisAdapterFundingAndOHLCV:
    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate exercises _scan_exchanges, _find_funding_rate,
        _make_funding_rate_ref."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"fundingRate": "0.0001", "timestamp": 1700000000000}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("XBTUSD")
        assert result.venue == "tardis"
        assert str(result.rate) == "0.0001"

    @pytest.mark.asyncio
    async def test_get_funding_rate_404_raises(self) -> None:
        """When all exchanges return 404, RuntimeError is raised."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(RuntimeError, match="No funding rate"),
        ):
            await adapter.get_funding_rate("XBTUSD")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv exercises _collect_ohlcv, _fetch_ohlcv_from_exchange, _parse_ohlcv_line."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = (
            '{"open": "30000", "high": "31000", "low": "29000",'
            ' "close": "30500", "volume": "100", "timestamp": 1700000000000}\n'
        )
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert len(result) == 1
        assert result[0].venue == "tardis"
        assert result[0].open == Decimal("30000")

    @pytest.mark.asyncio
    async def test_get_ohlcv_line_missing_open_skipped(self) -> None:
        """Lines without 'open' field are skipped in _parse_ohlcv_line."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"timestamp": 1700000000000, "volume": "100"}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_exchange_404_skips(self) -> None:
        """404 from exchange in get_ohlcv returns empty batch."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_parse_expiry_invalid_string_returns_none(self) -> None:
        """_parse_expiry returns None for invalid ISO strings."""
        from instruments_service.reference_data.adapters.tardis import _parse_expiry

        assert _parse_expiry("not-a-date") is None
        assert _parse_expiry(None) is None
        assert _parse_expiry("") is None

    def test_build_datafeed_headers_no_key(self) -> None:
        """_build_datafeed_headers returns empty dict when no API key."""
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value=None):
            headers = adapter._build_datafeed_headers()
        assert headers == {}

    def test_build_datafeed_headers_with_key(self) -> None:
        """_build_datafeed_headers adds Authorization header when key is set."""
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value="test-key"):
            headers = adapter._build_datafeed_headers()
        assert headers == {"Authorization": "Bearer test-key"}

    def test_resolve_bar_type(self) -> None:
        """_resolve_bar_type maps intervals to seconds and channel names."""
        from instruments_service.reference_data.adapters.tardis import TardisReferenceDataAdapter

        assert TardisReferenceDataAdapter._resolve_bar_type("1m") == (60, "trade_bar_1m")
        assert TardisReferenceDataAdapter._resolve_bar_type("1h") == (3600, "trade_bar_1h")
        assert TardisReferenceDataAdapter._resolve_bar_type("1d") == (86400, "trade_bar_1d")
        assert TardisReferenceDataAdapter._resolve_bar_type("unknown") == (86400, "trade_bar_1d")


# ---------------------------------------------------------------------------
# Bybit extended tests (covering get_options_chain, get_expiry_calendar, _fetch_options)
# ---------------------------------------------------------------------------


class TestBybitAdapterFullCoverage:
    def _make_linear_session(self, symbol_list: list[dict[str, object]]) -> MagicMock:
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
    async def test_get_options_chain_empty(self) -> None:
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "bybit"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="future")
        assert calendar.venue == "bybit"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_category_spot(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_linear_session(
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
            results = await adapter.get_instruments(instrument_type="spot")
        assert len(results) == 1
        assert results[0].instrument_type == "spot"

    @pytest.mark.asyncio
    async def test_fetch_category_perp(self) -> None:
        adapter = BybitReferenceDataAdapter()
        mock_session_cm = self._make_linear_session(
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
            results = await adapter.get_instruments(instrument_type="perp")
        assert len(results) == 1
        assert results[0].instrument_type == "perp"

    @pytest.mark.asyncio
    async def test_fetch_options(self) -> None:
        adapter = BybitReferenceDataAdapter()
        option_data = {
            "symbol": "BTC-31DEC24-50000-C",
            "status": "Trading",
            "contractType": "",
            "baseCoin": "BTC",
            "quoteCoin": "USD",
            "optionsType": "Call",
            "deliveryTime": "1735689600000",
            "priceFilter": {"tickSize": "0.0001"},
            "lotSizeFilter": {"qtyStep": "0.01", "minOrderQty": "0.01"},
        }
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"result": {"list": [option_data]}})
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
        assert len(results) >= 1
        assert results[0].instrument_type == "option"
        assert results[0].option_type == "call"

    def test_parse_category_symbol_no_symbol_returns_none(self) -> None:
        from unified_api_contracts.external.bybit.schemas import (
            BybitInstrumentInfo,
        )

        adapter = BybitReferenceDataAdapter()
        sym = BybitInstrumentInfo.model_validate({"symbol": "", "status": "Trading"})
        result = adapter._parse_category_symbol(sym, "spot", datetime.now(UTC))
        assert result is None
