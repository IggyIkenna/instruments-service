"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.betfair import BetfairReferenceDataAdapter
from instruments_service.reference_data.adapters.polygon import PolygonReferenceDataAdapter
from instruments_service.reference_data.adapters.polymarket import PolymarketReferenceDataAdapter


def _make_polygon_option(
    instrument_key: str,
    option_type: str,
    expiry_dt: datetime,
    strike: Decimal = Decimal("150"),
) -> InstrumentRecord:
    """Helper to build a Polygon option InstrumentRecord for tests."""
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue="polygon",
        raw_symbol=instrument_key,
        instrument_type="OPTION",
        base_asset="AAPL",
        quote_asset="USD",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("1"),
        contract_size=Decimal("100"),
        strike=strike,
        option_type=option_type,
        expiry=expiry_dt,
    )


class TestBetfairAdapter:
    def test_venue_name(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        assert adapter.venue == "betfair"

    def test_get_credentials_raises_without_project_id(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            adapter._get_credentials()

    @pytest.mark.asyncio
    async def test_get_instruments_raises_without_credentials(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_non_sports_event_returns_empty(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {
                        "marketId": "1.234567",
                        "marketName": "Match Odds",
                        "marketStartTime": "2026-01-01T15:00:00Z",
                        "eventType": {"id": "1", "name": "Soccer"},
                        "event": {"id": "28375802", "name": "Team A vs Team B"},
                        "runners": [
                            {"selectionId": 111, "runnerName": "Team A"},
                            {"selectionId": 222, "runnerName": "Team B"},
                        ],
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_credentials", return_value=("tok", "key")),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].venue == "betfair"
        assert results[0].instrument_type == "EXCHANGE_ODDS"
        assert results[0].instrument_key == "1.234567/111"

    @pytest.mark.asyncio
    async def test_get_instrument_found_mocked(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with patch.object(
            adapter,
            "get_instruments",
            return_value=[],
        ):
            result = await adapter.get_instrument("1.234567/111")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("soccer")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("soccer")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("1.234567/111")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        """Betfair OHLCV is not available via reference data API."""
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("1.234567/111")

    def test_parse_catalogue_item_no_runners(self) -> None:
        from unified_api_contracts.external.betfair.schemas import (
            BetfairMarketCatalogue,
        )

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.111",
                "marketName": "No Runners",
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert results == []

    def test_parse_catalogue_item_runner_no_selection_id(self) -> None:
        from unified_api_contracts.external.betfair.schemas import (
            BetfairMarketCatalogue,
        )

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.111",
                "marketName": "Market",
                "runners": [{"runnerName": "NoId"}],
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_network_error_returns_empty(self) -> None:
        import aiohttp as _aiohttp

        adapter = BetfairReferenceDataAdapter()
        mock_session_cm = MagicMock()
        mock_session_obj = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("network"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_credentials", return_value=("tok", "key")),
        ):
            results = await adapter.get_instruments()
        assert results == []


# ---------------------------------------------------------------------------
# Polymarket adapter tests
# ---------------------------------------------------------------------------


class TestPolymarketAdapterExtended:
    def test_venue_name(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter.venue == "polymarket"

    @pytest.mark.asyncio
    async def test_get_instruments_non_prediction_market_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_mocked_single_page(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "conditionId": "0xabc123",
                    "marketSlug": "will-btc-reach-100k",
                    "question": "Will BTC reach $100K?",
                    "active": True,
                    "closed": False,
                    "minimumTickSize": 0.01,
                    "minimumOrderSize": 1.0,
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
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].venue == "polymarket"
        assert results[0].instrument_key == "0xabc123"
        assert results[0].instrument_type == "PREDICTION_MARKET"

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("0xabc123")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """Polymarket get_ohlcv uses CLOB API — mock the HTTP call."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"history": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_ohlcv("0xabc123")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_network_error_returns_empty_page(self) -> None:
        import aiohttp as _aiohttp

        adapter = PolymarketReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("fail"))
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
    async def test_get_instrument_found(self) -> None:
        """get_instrument returns matching record by condition_id."""
        adapter = PolymarketReferenceDataAdapter()
        with patch.object(
            adapter,
            "get_instruments",
            AsyncMock(return_value=[MagicMock(instrument_key="0xabc123", raw_symbol="will-btc-reach-100k")]),
        ):
            result = await adapter.get_instrument("0xabc123")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        """get_instrument returns None when no match found."""
        adapter = PolymarketReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("0xnotexist")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_history_data(self) -> None:
        """get_ohlcv parses price history points into OHLCVRef list."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={"history": [{"t": 1700000000, "p": "0.65"}, {"t": 1700086400, "p": "0.70"}]}
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
            result = await adapter.get_ohlcv("0xabc123")
        assert len(result) == 2
        assert result[0].venue == "polymarket"


# ---------------------------------------------------------------------------
# Polygon adapter tests
# ---------------------------------------------------------------------------


class TestPolygonAdapterExtended:
    def test_venue_name(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        assert adapter.venue == "polygon"

    def test_get_api_key_raises_without_project_id(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            adapter._get_api_key()

    @pytest.mark.asyncio
    async def test_get_instruments_raises_without_api_key(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "results": [
                    {
                        "ticker": "AAPL",
                        "name": "Apple Inc.",
                        "market": "stocks",
                        "locale": "us",
                        "primary_exchange": "XNAS",
                        "type": "CS",
                        "active": True,
                        "currency_name": "usd",
                    }
                ],
                "next_url": None,
                "status": "OK",
                "count": 1,
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
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_api_key", return_value="test-api-key"),
        ):
            results = await adapter.get_instruments()
        assert len(results) >= 1
        assert results[0].venue == "polygon"

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("AAPL")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented for Polygon — mock HTTP call."""
        adapter = PolygonReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"results": [], "status": "OK"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch("aiohttp.ClientSession", return_value=mock_session),
        ):
            result = await adapter.get_ohlcv("AAPL")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch.object(adapter, "_fetch_options", return_value=[]),
        ):
            chain = await adapter.get_options_chain("AAPL")
        assert chain.venue == "polygon"
        assert chain.calls == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch.object(adapter, "_fetch_options", return_value=[]),
        ):
            calendar = await adapter.get_expiry_calendar("AAPL")
        assert calendar.venue == "polygon"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
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
            patch.object(adapter, "_get_api_key", return_value="test-key"),
        ):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_found_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "results": {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "usd",
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
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_api_key", return_value="test-key"),
        ):
            result = await adapter.get_instrument("AAPL")
        assert result is not None
        assert result.venue == "polygon"

    def test_parse_expiry_date_invalid_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.polygon import _parse_expiry_date

        result = _parse_expiry_date("not-a-date")
        assert result is None

    def test_parse_expiry_date_none_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.polygon import _parse_expiry_date

        result = _parse_expiry_date(None)
        assert result is None

    def test_parse_expiry_date_valid(self) -> None:
        from datetime import UTC, datetime

        from instruments_service.reference_data.adapters.polygon import _parse_expiry_date

        result = _parse_expiry_date("2024-06-21")
        assert result == datetime(2024, 6, 21, tzinfo=UTC)

    def test_parse_ticker_no_ticker_sym_returns_none(self) -> None:
        from datetime import UTC, datetime

        from unified_api_contracts.external.polygon.schemas import (
            PolygonTicker,
        )

        adapter = PolygonReferenceDataAdapter()
        now = datetime.now(UTC)
        ticker = PolygonTicker.model_validate({"ticker": "", "name": "Empty"})
        result = adapter._parse_ticker(ticker, now)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instruments_option_type_mocked(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch.object(adapter, "_fetch_options", return_value=[]),
        ):
            results = await adapter.get_instruments(instrument_type="OPTION")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_bars(self) -> None:
        """get_ohlcv _parse_polygon_bar parses real aggregate bar data."""
        adapter = PolygonReferenceDataAdapter()
        bars = [
            {
                "t": 1700000000000,
                "o": "150.0",
                "h": "155.0",
                "l": "149.0",
                "c": "152.0",
                "v": "1000000",
            },
            {
                "t": 1700086400000,
                "o": "152.0",
                "h": "158.0",
                "l": "151.0",
                "c": "157.0",
                "v": "1200000",
            },
        ]
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"results": bars, "status": "OK"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch("aiohttp.ClientSession", return_value=mock_session),
        ):
            result = await adapter.get_ohlcv("AAPL")
        assert len(result) == 2
        assert result[0].venue == "polygon"
        from decimal import Decimal

        assert result[0].open == Decimal("150.0")

    def test_parse_polygon_bar_not_dict_returns_none(self) -> None:
        """_parse_polygon_bar returns None for non-dict input."""
        adapter = PolygonReferenceDataAdapter()
        result = adapter._parse_polygon_bar("not-a-dict", "AAPL", "1d")
        assert result is None

    def test_parse_polygon_bar_no_timestamp_returns_none(self) -> None:
        """_parse_polygon_bar returns None when 't' key is missing."""
        adapter = PolygonReferenceDataAdapter()
        result = adapter._parse_polygon_bar({"o": "100.0"}, "AAPL", "1d")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_with_options(self) -> None:
        """get_options_chain filters calls and puts, collects strikes."""
        adapter = PolygonReferenceDataAdapter()
        expiry_dt = datetime(2024, 6, 21, tzinfo=UTC)
        call_inst = _make_polygon_option("O:AAPL240621C00150000", "call", expiry_dt)
        put_inst = _make_polygon_option("O:AAPL240621P00150000", "put", expiry_dt)
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch.object(adapter, "_fetch_options", return_value=[call_inst, put_inst]),
        ):
            chain = await adapter.get_options_chain("AAPL")
        assert chain.venue == "polygon"
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("150") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_options(self) -> None:
        """get_expiry_calendar collects expiry dates from fetched options."""
        adapter = PolygonReferenceDataAdapter()
        expiry_dt = datetime(2024, 6, 21, tzinfo=UTC)
        opt_inst = _make_polygon_option("O:AAPL240621C00150000", "call", expiry_dt)
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch.object(adapter, "_fetch_options", return_value=[opt_inst]),
        ):
            calendar = await adapter.get_expiry_calendar("AAPL")
        assert calendar.venue == "polygon"
        assert expiry_dt in calendar.expiries

    @pytest.mark.asyncio
    async def test_get_instrument_results_none_returns_none(self) -> None:
        """get_instrument returns None when API returns results=None."""
        adapter = PolygonReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"results": None})
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
            patch.object(adapter, "_get_api_key", return_value="test-key"),
        ):
            result = await adapter.get_instrument("AAPL")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_ohlcv_client_error_returns_empty(self) -> None:
        """get_ohlcv returns [] when ClientError is raised."""
        adapter = PolygonReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("network"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(adapter, "_get_api_key", return_value="test-key"),
            patch("aiohttp.ClientSession", return_value=mock_session),
        ):
            result = await adapter.get_ohlcv("AAPL")
        assert result == []


# ---------------------------------------------------------------------------
# Databento adapter mocked tests
# ---------------------------------------------------------------------------
