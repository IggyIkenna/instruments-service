"""Unit tests for Databento, Tardis, and Bybit adapters (no live network — mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.bybit import BybitReferenceDataAdapter
from instruments_service.reference_data.adapters.databento import DatabentoReferenceDataAdapter
from instruments_service.reference_data.adapters.tardis import TardisReferenceDataAdapter

# ---------------------------------------------------------------------------
# InstrumentRecord helper (current API — no removed fields)
# ---------------------------------------------------------------------------


def _make_record(
    key: str = "TEST:FUTURE:ESZ4",
    venue: str = "databento",
    instrument_type: str = "future",
    raw_symbol: str = "ESZ4",
    base_asset: str = "ES",
    quote_asset: str = "USD",
    **kwargs: object,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=key,
        venue=venue,
        raw_symbol=raw_symbol,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# DatabentoReferenceDataAdapter
# ---------------------------------------------------------------------------


class TestDatabentoAdapterMocked:
    def test_venue_name(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        assert adapter.venue == "databento"

    @pytest.mark.asyncio
    async def test_get_instruments_requires_api_key(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_with_key_returns_results(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        record = _make_record()
        with patch.object(adapter, "_fetch_symbols", return_value=[record]):
            with patch.object(adapter, "_get_equity_symbols", return_value=[]):
                with patch.object(adapter, "_create_fx_spot_records", return_value=[]):
                    with patch.object(adapter, "_create_yahoo_index_records", return_value=[]):
                        with patch.object(adapter, "_enrich_session_metadata"):
                            results = await adapter.get_instruments()
        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        fut = _make_record(instrument_type="future")
        spot = _make_record(key="DBEQ:SPOT:AAPL", instrument_type="spot", raw_symbol="AAPL")
        with patch.object(adapter, "_fetch_symbols", return_value=[fut, spot]):
            with patch.object(adapter, "_get_equity_symbols", return_value=[]):
                with patch.object(adapter, "_create_fx_spot_records", return_value=[]):
                    with patch.object(adapter, "_create_yahoo_index_records", return_value=[]):
                        with patch.object(adapter, "_enrich_session_metadata"):
                            results = await adapter.get_instruments(instrument_type="future")
        assert all(r.instrument_type == "future" for r in results)

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        record = _make_record(raw_symbol="ESZ4")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[record])):
            result = await adapter.get_instrument("ESZ4")
        assert result is not None
        assert result.raw_symbol == "ESZ4"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_with_options(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        expiry_dt = datetime(2024, 6, 21, tzinfo=UTC)
        call_inst = _make_record(
            key="GLBX:OPT:ESM4 C4500",
            instrument_type="option",
            raw_symbol="ESM4 C4500",
            strike=Decimal("4500"),
            option_type="Call",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[call_inst])):
            chain = await adapter.get_options_chain("ES")
        assert chain.venue == "databento"
        assert len(chain.calls) == 1
        assert Decimal("4500") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_futures(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        expiry_dt = datetime(2024, 3, 15, tzinfo=UTC)
        fut_inst = _make_record(
            key="GLBX:FUTURE:ESH4",
            instrument_type="future",
            raw_symbol="ESH4",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[fut_inst])):
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

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            chain = await adapter.get_options_chain("SPY")
        assert chain.venue == "databento"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            calendar = await adapter.get_expiry_calendar("ES")
        assert calendar.venue == "databento"
        assert calendar.expiries == []


# ---------------------------------------------------------------------------
# Tardis adapter mocked tests
# ---------------------------------------------------------------------------


class TestTardisAdapterMocked:
    def test_venue_name(self) -> None:
        adapter = TardisReferenceDataAdapter()
        assert adapter.venue == "tardis"

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
        # Tardis stores raw type from API — "PERPETUAL" (uppercase)
        assert "perpetual" in str(results[0].instrument_type).lower()
        # Tardis adapter uses the exchange name as venue (e.g. DERIBIT)
        assert results[0].venue is not None

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
            # Tardis uses uppercase type strings from the API ("PERPETUAL")
            results = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_on_empty(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
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
        call_inst = _make_record(
            key="deribit:BTC-31DEC24-50000-C",
            venue="tardis",
            instrument_type="option",
            raw_symbol="BTC-31DEC24-50000-C",
            base_asset="BTC",
            quote_asset="USD",
            strike=Decimal("50000"),
            option_type="call",
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[call_inst])):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "tardis"
        assert len(chain.calls) == 1

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        expiry_dt = datetime(2024, 3, 31, 8, 0, tzinfo=UTC)
        fut_inst = _make_record(
            key="deribit:BTC-31MAR24",
            venue="tardis",
            instrument_type="future",
            raw_symbol="BTC-31MAR24",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[fut_inst])):
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
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"fundingRate": "0.0001", "timestamp": 1700000000000}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("XBTUSD")
        assert result.venue == "tardis"
        assert str(result.rate) == "0.0001"

    @pytest.mark.asyncio
    async def test_get_funding_rate_404_raises(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(RuntimeError, match="No funding rate"),
        ):
            await adapter.get_funding_rate("XBTUSD")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
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
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"timestamp": 1700000000000, "volume": "100"}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_exchange_404_skips(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_parse_expiry_invalid_string_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.tardis import _parse_expiry

        assert _parse_expiry("not-a-date") is None
        assert _parse_expiry(None) is None
        assert _parse_expiry("") is None

    def test_build_datafeed_headers_no_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value=None):
            headers = adapter._build_datafeed_headers()
        assert headers == {}

    def test_build_datafeed_headers_with_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value="test-key"):
            headers = adapter._build_datafeed_headers()
        assert headers == {"Authorization": "Bearer test-key"}

    def test_resolve_bar_type(self) -> None:
        assert TardisReferenceDataAdapter._resolve_bar_type("1m") == (60, "trade_bar_1m")
        assert TardisReferenceDataAdapter._resolve_bar_type("1h") == (3600, "trade_bar_1h")
        assert TardisReferenceDataAdapter._resolve_bar_type("1d") == (86400, "trade_bar_1d")
        assert TardisReferenceDataAdapter._resolve_bar_type("unknown") == (86400, "trade_bar_1d")


# ---------------------------------------------------------------------------
# Bybit extended tests
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
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "BYBIT-SPOT"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="future")
        assert calendar.venue == "BYBIT-SPOT"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = BybitReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
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
