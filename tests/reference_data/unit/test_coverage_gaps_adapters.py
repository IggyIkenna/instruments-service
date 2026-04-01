"""Targeted coverage tests for URDI adapters — covering specific uncovered branches.

All tests are credential-free and use unittest.mock. No live network calls.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.betfair import BetfairReferenceDataAdapter
from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter
from instruments_service.reference_data.adapters.ccxt_adapter import CCXTReferenceDataAdapter
from instruments_service.reference_data.adapters.okx import OKXReferenceDataAdapter
from instruments_service.reference_data.adapters.tardis import TardisReferenceDataAdapter

# ── shared helpers ─────────────────────────────────────────────────────────────


def _make_instrument(
    raw_symbol: str = "BTCUSDT",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    venue: str = "test",
    expiry: datetime | None = None,
    strike: Decimal | None = None,
    option_type: str | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=raw_symbol,
        venue=venue,
        raw_symbol=raw_symbol,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        contract_size=Decimal("1"),
        expiry=expiry,
        strike=strike,
        option_type=option_type,
    )


# ── BetfairReferenceDataAdapter ────────────────────────────────────────────────


class TestBetfairCoverageGaps:
    def test_parse_market_start_time_invalid_iso_returns_none(self) -> None:
        """Line 206-207: ValueError branch in _parse_market_start_time."""
        adapter = BetfairReferenceDataAdapter()
        result = adapter._parse_market_start_time("NOT-A-DATE")
        assert result is None

    def test_parse_market_start_time_none_returns_none(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        assert adapter._parse_market_start_time(None) is None

    def test_parse_market_start_time_valid(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        result = adapter._parse_market_start_time("2026-01-01T15:00:00Z")
        assert result is not None
        assert result.tzinfo is not None

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """Lines 145-146: get_instrument returns matching instrument."""
        adapter = BetfairReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="1.234567/111", venue="betfair", instrument_type="EXCHANGE_ODDS")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("1.234567/111")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_by_key(self) -> None:
        """Lines 145-146: get_instrument matches instrument_key."""
        adapter = BetfairReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="other", venue="betfair", instrument_type="EXCHANGE_ODDS")
        inst2 = _make_instrument(raw_symbol="market/sel", venue="betfair", instrument_type="EXCHANGE_ODDS")
        with patch.object(adapter, "get_instruments", return_value=[inst, inst2]):
            result = await adapter.get_instrument("market/sel")
        assert result is inst2

    @pytest.mark.asyncio
    async def test_get_credentials_raises_without_api_key(self) -> None:
        """ValueError when no credentials injected."""
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            adapter._get_credentials()

    @pytest.mark.asyncio
    async def test_get_credentials_with_injected_keys(self) -> None:
        """Credentials passed via constructor."""
        adapter = BetfairReferenceDataAdapter()
        adapter._session_token = "sess-token"
        adapter._app_key = "app-key"
        session_token, app_key = adapter._get_credentials()
        assert session_token == "sess-token"
        assert app_key == "app-key"

    @pytest.mark.asyncio
    async def test_fetch_markets_raw_401_raises_runtime_error(self) -> None:
        """Line 129: 401 status raises RuntimeError."""
        adapter = BetfairReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 401
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
            pytest.raises(RuntimeError, match="Betfair authentication failed"),
        ):
            await adapter._fetch_markets_raw("bad-token", "bad-key")

    @pytest.mark.asyncio
    async def test_get_instruments_returns_empty_for_wrong_type(self) -> None:
        """Line 84-85: non-sports instrument_type filter returns []."""
        adapter = BetfairReferenceDataAdapter()
        with patch.object(adapter, "_get_credentials", return_value=("tok", "key")):
            result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []


# ── BinanceReferenceDataAdapter ────────────────────────────────────────────────


class TestBinanceCoverageGaps:
    def _make_session_with_responses(self, *responses: object) -> MagicMock:
        """Build a session mock that returns sequential json responses."""
        resps = list(responses)
        call_count = 0

        async def json_side_effect() -> object:
            nonlocal call_count
            if call_count < len(resps):
                result = resps[call_count]
                call_count += 1
                return result
            return {}

        mock_resp = AsyncMock()
        mock_resp.json = json_side_effect
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
    async def test_get_instrument_found(self) -> None:
        """Lines 55-56: get_instrument returns matching instrument."""
        adapter = BinanceReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="BTCUSDT", venue="binance")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instruments_spot_only(self) -> None:
        """Lines 44->46: spot-only filter path in get_instruments."""
        adapter = BinanceReferenceDataAdapter()
        mock_session = self._make_session_with_responses(
            {
                "symbols": [
                    {
                        "symbol": "ETHUSDT",
                        "status": "TRADING",
                        "baseAsset": "ETH",
                        "quoteAsset": "USDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        ],
                    }
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert len(results) == 1
        assert results[0].instrument_type == "SPOT_PAIR"

    @pytest.mark.asyncio
    async def test_fetch_futures_with_delivery_date(self) -> None:
        """Line 169: futures with non-zero deliveryDate → expiry datetime set."""
        adapter = BinanceReferenceDataAdapter()
        delivery_ms = int(datetime(2026, 6, 27, tzinfo=UTC).timestamp() * 1000)
        # Call _fetch_futures directly to isolate it
        mock_session_obj = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "symbols": [
                    {
                        "symbol": "BTCUSDT_0627",
                        "status": "TRADING",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "contractType": "CURRENT_QUARTER",
                        "deliveryDate": delivery_ms,
                        "contractSize": 10,
                        "filters": [],
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj.get = MagicMock(return_value=mock_cm)

        results = await adapter._fetch_futures(mock_session_obj)
        assert len(results) == 1
        assert results[0].instrument_type == "FUTURE"
        assert results[0].expiry is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_parses_bars(self) -> None:
        """Lines 291-294: OHLCV bar parsing."""
        adapter = BinanceReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[[ts_ms, "50000.0", "51000.0", "49000.0", "50500.0", "100.5", ts_ms + 60000]]
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
            results = await adapter.get_ohlcv("BTCUSDT", interval="1d", limit=1)
        assert len(results) == 1
        assert results[0].open == Decimal("50000.0")
        assert results[0].close == Decimal("50500.0")
        assert results[0].venue == "BINANCE-SPOT"


# ── CCXTReferenceDataAdapter ───────────────────────────────────────────────────


class TestCCXTCoverageGaps:
    def test_get_exchange_unsupported_raises_runtime_error(self) -> None:
        """Line 44: ccxt doesn't support exchange → RuntimeError."""
        adapter = CCXTReferenceDataAdapter(venue="nonexistent_exchange_xyz")
        with pytest.raises(RuntimeError, match="ccxt does not support exchange"):
            adapter._get_exchange()

    def test_parse_ccxt_market_inactive_returns_none(self) -> None:
        """Line 104: active=False market → None."""
        adapter = CCXTReferenceDataAdapter(venue="kraken")
        result = adapter._parse_ccxt_market(
            "BTC/USDT",
            {"type": "spot", "active": False, "base": "BTC", "quote": "USDT"},
            None,
        )
        assert result is None

    def test_parse_ccxt_market_wrong_type_returns_none(self) -> None:
        """Line 110: mapped_type != instrument_type filter → None."""
        adapter = CCXTReferenceDataAdapter(venue="kraken")
        result = adapter._parse_ccxt_market(
            "BTC/USDT",
            {"type": "spot", "active": True, "base": "BTC", "quote": "USDT"},
            "PERPETUAL",  # asking for perpetual but market is spot
        )
        assert result is None

    def test_parse_ccxt_market_non_dict_returns_none(self) -> None:
        """Line 103-104: market_raw not a dict → None."""
        adapter = CCXTReferenceDataAdapter(venue="kraken")
        result = adapter._parse_ccxt_market("BTC/USDT", "not a dict", None)
        assert result is None

    def test_parse_ccxt_market_with_settle_and_expiry(self) -> None:
        """Lines 113, 114: market with settle and expiryDatetime → InstrumentRecord."""
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        result = adapter._parse_ccxt_market(
            "BTC-27JUN25",
            {
                "type": "future",
                "active": True,
                "base": "BTC",
                "quote": "USD",
                "settle": "BTC",
                "expiryDatetime": "2025-06-27T08:00:00Z",
                "precision": {"price": "0.5", "amount": "0.1"},
                "limits": {"amount": {"min": "0.001"}},
                "contractSize": "1",
            },
            None,
        )
        assert result is not None
        assert result.instrument_type == "FUTURE"
        assert result.expiry is not None

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """Lines 150-151: get_instrument returns matching instrument."""
        adapter = CCXTReferenceDataAdapter(venue="kraken")
        inst = _make_instrument(raw_symbol="BTCUSDT", venue="kraken")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_options_chain_with_matching_options(self) -> None:
        """Lines 164-180: get_options_chain with call and put options."""
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        call_inst = _make_instrument(
            raw_symbol="BTC-27JUN25-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="call",
            venue="deribit",
        )
        put_inst = _make_instrument(
            raw_symbol="BTC-27JUN25-50000-P",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="put",
            venue="deribit",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst, put_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("50000") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_options_chain_with_expiry_filter(self) -> None:
        """Line 167-168: expiry filter skips non-matching options."""
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        other_expiry = datetime(2026, 9, 26, tzinfo=UTC)
        call_match = _make_instrument(
            raw_symbol="BTC-27JUN26-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="call",
            venue="deribit",
        )
        call_other = _make_instrument(
            raw_symbol="BTC-26SEP26-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=other_expiry,
            strike=Decimal("50000"),
            option_type="call",
            venue="deribit",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_match, call_other]):
            chain = await adapter.get_options_chain("BTC", expiry=expiry_dt)
        assert len(chain.calls) == 1
        assert chain.calls[0].expiry == expiry_dt

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_collects_expiries(self) -> None:
        """Line 196: get_expiry_calendar with matching base_asset and expiry."""
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        inst = _make_instrument(
            raw_symbol="BTC-27JUN26",
            instrument_type="FUTURE",
            base_asset="BTC",
            expiry=expiry_dt,
            venue="deribit",
        )
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="FUTURE")
        assert expiry_dt in calendar.expiries

    @pytest.mark.asyncio
    async def test_get_funding_rate_with_next_dt_valid(self) -> None:
        """Lines 224-228: nextFundingDatetime parsed to UTC datetime."""
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_funding_rate = AsyncMock(
            return_value={
                "fundingRate": 0.0001,
                "markPrice": 50000.0,
                "nextFundingDatetime": "2026-03-12T08:00:00Z",
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            rate_ref = await adapter.get_funding_rate("BTC/USDT:USDT")
        assert rate_ref.rate == Decimal("0.0001")
        assert rate_ref.next_funding_time.hour == 8

    @pytest.mark.asyncio
    async def test_get_funding_rate_with_invalid_next_dt(self) -> None:
        """Lines 228-229: invalid nextFundingDatetime falls back to now."""
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_funding_rate = AsyncMock(
            return_value={
                "fundingRate": 0.0001,
                "markPrice": None,
                "nextFundingDatetime": "NOT-A-DATETIME",
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            rate_ref = await adapter.get_funding_rate("BTC/USDT:USDT")
        assert rate_ref.rate == Decimal("0.0001")
        assert rate_ref.mark_price is None

    @pytest.mark.asyncio
    async def test_get_ohlcv_skips_short_bars(self) -> None:
        """Line 263-264: bar with fewer than 6 elements is skipped."""
        adapter = CCXTReferenceDataAdapter(venue="kraken")
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_ohlcv = AsyncMock(
            return_value=[
                [ts_ms, "50000", "51000", "49000", "50500", "100"],  # valid 6-element bar
                [ts_ms, "50000", "51000"],  # too short — skipped
            ]
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_ohlcv("BTC/USDT", interval="1d", limit=2)
        assert len(results) == 1
        assert results[0].open == Decimal("50000")


# ── OKXReferenceDataAdapter ────────────────────────────────────────────────────


class TestOKXCoverageGaps:
    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """Line 62: get_instrument returns matching instrument."""
        adapter = OKXReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="BTC-USDT", venue="okx", instrument_type="SPOT_PAIR")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTC-USDT")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_options_chain_with_matching_options(self) -> None:
        """Lines 76-85: options chain with strike and call/put classification."""
        adapter = OKXReferenceDataAdapter()
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        call_inst = _make_instrument(
            raw_symbol="BTC-USD-260627-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="C",
            venue="okx",
        )
        put_inst = _make_instrument(
            raw_symbol="BTC-USD-260627-50000-P",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="P",
            venue="okx",
        )
        non_match_inst = _make_instrument(
            raw_symbol="ETH-USD-260627-2000-C",
            instrument_type="OPTION",
            base_asset="ETH",
            venue="okx",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst, put_inst, non_match_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("50000") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_options_chain_with_expiry_filter(self) -> None:
        """Line 78: expiry filter skips non-matching options."""
        adapter = OKXReferenceDataAdapter()
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        other_expiry = datetime(2026, 9, 26, tzinfo=UTC)
        call_match = _make_instrument(
            raw_symbol="BTC-USD-260627-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="C",
            venue="okx",
        )
        call_other = _make_instrument(
            raw_symbol="BTC-USD-260926-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=other_expiry,
            strike=Decimal("50000"),
            option_type="C",
            venue="okx",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_match, call_other]):
            chain = await adapter.get_options_chain("BTC", expiry=expiry_dt)
        assert len(chain.calls) == 1

    @pytest.mark.asyncio
    async def test_get_exchange_fee_schedule_returns_none(self) -> None:
        """Lines 307-314: fee schedule stub returns None."""
        adapter = OKXReferenceDataAdapter()
        result = await adapter.get_exchange_fee_schedule("client-123")
        assert result is None


# ── TardisReferenceDataAdapter ─────────────────────────────────────────────────


class TestTardisCoverageGaps:
    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """Lines 110-111: get_instrument returns matching instrument."""
        adapter = TardisReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="BTCUSDT", venue="tardis", instrument_type="PERPETUAL")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_options_chain_with_call_and_put(self) -> None:
        """Lines 126-134: options chain with strike, call/put, expiry filter."""
        adapter = TardisReferenceDataAdapter()
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        call_inst = _make_instrument(
            raw_symbol="BTC-27JUN26-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="call",
            venue="tardis",
        )
        put_inst = _make_instrument(
            raw_symbol="BTC-27JUN26-50000-P",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="put",
            venue="tardis",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst, put_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("50000") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_options_chain_with_expiry_filter(self) -> None:
        """Line 127-128: expiry filter excludes non-matching options."""
        adapter = TardisReferenceDataAdapter()
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        other_expiry = datetime(2026, 9, 26, tzinfo=UTC)
        call_match = _make_instrument(
            raw_symbol="BTC-27JUN26-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=expiry_dt,
            strike=Decimal("50000"),
            option_type="call",
            venue="tardis",
        )
        call_other = _make_instrument(
            raw_symbol="BTC-26SEP26-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            expiry=other_expiry,
            strike=Decimal("50000"),
            option_type="call",
            venue="tardis",
        )
        with patch.object(adapter, "get_instruments", return_value=[call_match, call_other]):
            chain = await adapter.get_options_chain("BTC", expiry=expiry_dt)
        assert len(chain.calls) == 1

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_collects_expiries(self) -> None:
        """Line 156: get_expiry_calendar with matching base and expiry."""
        adapter = TardisReferenceDataAdapter()
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        inst = _make_instrument(
            raw_symbol="BTC-27JUN26",
            instrument_type="FUTURE",
            base_asset="BTC",
            expiry=expiry_dt,
            venue="tardis",
        )
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="FUTURE")
        assert expiry_dt in calendar.expiries

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_from_exchange_skips_empty_lines(self) -> None:
        """Line 334-335: empty lines in OHLCV text are skipped."""
        import json as json_mod

        adapter = TardisReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, 12, tzinfo=UTC).timestamp() * 1000)
        bar1 = {
            "timestamp": ts_ms,
            "open": "50000",
            "high": "51000",
            "low": "49000",
            "close": "50500",
            "volume": "10",
        }
        text_with_empty = "\n" + json_mod.dumps(bar1) + "\n\n"
        mock_session = MagicMock()
        with patch.object(adapter, "_fetch_datafeed_text", return_value=text_with_empty):
            results = await adapter._fetch_ohlcv_from_exchange(
                mock_session, "deribit", 0, 1000, "[]", {}, "BTC-PERPETUAL", "1d"
            )
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_fetch_exchange_instruments_404_returns_empty(self) -> None:
        """Lines 419-421: 404 status returns empty list with warning."""
        adapter = TardisReferenceDataAdapter(exchanges=["nonexistent-exchange"])
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        results = await adapter._fetch_exchange_instruments(mock_session, None, "nonexistent-exchange")
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_exchange_instruments_with_api_key_adds_auth_header(self) -> None:
        """Line 416: API key is added to headers when present."""

        from unified_api_contracts import TardisExchangeDetail

        adapter = TardisReferenceDataAdapter(exchanges=["binance-futures"])
        empty_detail = TardisExchangeDetail(id="binance-futures", name="Binance Futures", instruments=[])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"id": "binance-futures", "name": "Binance Futures", "instruments": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch(
            "instruments_service.reference_data.adapters.tardis.TardisExchangeDetail.model_validate",
            return_value=empty_detail,
        ):
            results = await adapter._fetch_exchange_instruments(mock_session, "my-api-key", "binance-futures")

        # Verify get was called with auth header
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert (
            "Authorization" in call_args[1]["headers"]
            or (len(call_args[0]) > 1 and "Authorization" in call_args[0][1])
            or any("Authorization" in str(a) for a in call_args)
        )
        assert results == []
