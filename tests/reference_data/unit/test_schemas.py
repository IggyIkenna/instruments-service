"""Unit tests for canonical schemas."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data import (
    CanonicalCorporateAction,
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from instruments_service.reference_data.adapters.hyperliquid import HyperliquidReferenceDataAdapter


class TestCanonicalInstrument:
    def test_instrument_creation(self) -> None:
        inst = InstrumentRecord(
            instrument_key="BTCUSDT",
            venue="binance",
            raw_symbol="BTCUSDT",
            instrument_type="SPOT_PAIR",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
        )
        assert inst.venue == "binance"
        assert inst.instrument_type == "SPOT_PAIR"
        assert inst.tick_size == Decimal("0.01")
        assert inst.expiry is None

    def test_option_instrument(self) -> None:
        inst = InstrumentRecord(
            instrument_key="BTC-31DEC24-50000-C",
            venue="deribit",
            raw_symbol="BTC-31DEC24-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.1"),
            contract_size=Decimal("1"),
            expiry=datetime(2024, 12, 31, tzinfo=UTC),
            strike=Decimal("50000"),
            option_type="call",
        )
        assert inst.instrument_type == "OPTION"
        assert inst.strike == Decimal("50000")
        assert inst.option_type == "call"

    def test_perpetual_instrument(self) -> None:
        inst = InstrumentRecord(
            instrument_key="BTCUSDT",
            venue="bybit",
            raw_symbol="BTCUSDT",
            instrument_type="PERPETUAL",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
        )
        assert inst.instrument_type == "PERPETUAL"
        assert inst.expiry is None

    def test_instrument_fields_types(self) -> None:
        inst = InstrumentRecord(
            instrument_key="ETH-USDT",
            venue="okx",
            raw_symbol="ETH-USDT",
            instrument_type="SPOT_PAIR",
            base_asset="ETH",
            quote_asset="USDT",
            tick_size=Decimal("0.001"),
            lot_size=Decimal("0.00001"),
            contract_size=Decimal("1"),
        )
        assert isinstance(inst.tick_size, Decimal)
        assert isinstance(inst.lot_size, Decimal)


class TestCanonicalOptionsChain:
    def test_options_chain_creation(self) -> None:
        now = datetime.now(UTC)
        chain = CanonicalOptionsChain(
            venue="deribit",
            underlying="BTC",
            expiry=now,
            strikes=[Decimal("40000"), Decimal("45000"), Decimal("50000")],
            calls=[],
            puts=[],
            fetched_at=now,
        )
        assert chain.underlying == "BTC"
        assert len(chain.strikes) == 3
        assert chain.venue == "deribit"

    def test_options_chain_defaults(self) -> None:
        now = datetime.now(UTC)
        chain = CanonicalOptionsChain(
            venue="binance",
            underlying="ETH",
            expiry=now,
        )
        assert chain.strikes == []
        assert chain.calls == []
        assert chain.puts == []

    def test_options_chain_with_instruments(self) -> None:
        now = datetime.now(UTC)
        inst = InstrumentRecord(
            instrument_key="BTC-31DEC24-50000-C",
            venue="deribit",
            raw_symbol="BTC-31DEC24-50000-C",
            instrument_type="OPTION",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.1"),
            contract_size=Decimal("1"),
            expiry=now,
            strike=Decimal("50000"),
            option_type="call",
        )
        chain = CanonicalOptionsChain(
            venue="deribit",
            underlying="BTC",
            expiry=now,
            strikes=[Decimal("50000")],
            calls=[inst],
            puts=[],
            fetched_at=now,
        )
        assert len(chain.calls) == 1
        assert chain.calls[0].strike == Decimal("50000")


class TestCanonicalExpiryCalendar:
    def test_expiry_calendar_creation(self) -> None:
        expiry = datetime(2024, 12, 27, tzinfo=UTC)
        cal = CanonicalExpiryCalendar(
            venue="binance",
            instrument_type="FUTURE",
            underlying="BTC",
            expiries=[expiry],
        )
        assert cal.underlying == "BTC"
        assert len(cal.expiries) == 1
        assert cal.expiries[0] == expiry

    def test_expiry_calendar_defaults(self) -> None:
        cal = CanonicalExpiryCalendar(
            venue="deribit",
            instrument_type="OPTION",
            underlying="ETH",
        )
        assert cal.expiries == []
        assert cal.settlement_assets == {}

    def test_expiry_calendar_multiple_expiries(self) -> None:
        expiries = [
            datetime(2024, 12, 27, tzinfo=UTC),
            datetime(2025, 3, 28, tzinfo=UTC),
        ]
        cal = CanonicalExpiryCalendar(
            venue="bybit",
            instrument_type="FUTURE",
            underlying="BTC",
            expiries=expiries,
        )
        assert len(cal.expiries) == 2


class TestCanonicalCorporateAction:
    def test_dividend_action(self) -> None:
        action = CanonicalCorporateAction(
            venue="ibkr",
            symbol="AAPL",
            action_type="dividend",
            effective_date=datetime(2024, 11, 15, tzinfo=UTC),
            ratio=None,
            cash_amount=Decimal("0.25"),
            currency="USD",
        )
        assert action.action_type == "dividend"
        assert action.cash_amount == Decimal("0.25")
        assert action.venue == "ibkr"

    def test_split_action(self) -> None:
        action = CanonicalCorporateAction(
            venue="ibkr",
            symbol="NVDA",
            action_type="split",
            effective_date=datetime(2024, 6, 10, tzinfo=UTC),
            ratio=Decimal("10"),
            cash_amount=None,
            currency=None,
            description="10-for-1 stock split",
        )
        assert action.action_type == "split"
        assert action.ratio == Decimal("10")
        assert action.description == "10-for-1 stock split"


class TestFundingRateRef:
    def test_creation(self) -> None:
        ref = FundingRateRef(
            venue="binance",
            symbol="BTCUSDT",
            rate=Decimal("0.0001"),
            next_funding_time=datetime(2024, 12, 1, 8, 0, tzinfo=UTC),
            mark_price=Decimal("50000"),
        )
        assert ref.venue == "binance"
        assert ref.rate == Decimal("0.0001")
        assert ref.mark_price == Decimal("50000")

    def test_defaults(self) -> None:
        ref = FundingRateRef(
            venue="bybit",
            symbol="ETHUSDT",
            rate=Decimal("0.0003"),
            next_funding_time=datetime(2024, 12, 1, tzinfo=UTC),
        )
        assert ref.mark_price is None
        assert ref.updated_at is not None


class TestOHLCVRef:
    def test_creation(self) -> None:
        bar = OHLCVRef(
            venue="binance",
            symbol="BTCUSDT",
            timestamp=datetime(2024, 12, 1, tzinfo=UTC),
            open=Decimal("50000"),
            high=Decimal("51000"),
            low=Decimal("49000"),
            close=Decimal("50500"),
            volume=Decimal("1000"),
            interval="1h",
        )
        assert bar.venue == "binance"
        assert bar.interval == "1h"
        assert bar.close == Decimal("50500")

    def test_default_interval(self) -> None:
        bar = OHLCVRef(
            venue="bybit",
            symbol="ETHUSDT",
            timestamp=datetime(2024, 12, 1, tzinfo=UTC),
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2900"),
            close=Decimal("3050"),
            volume=Decimal("500"),
        )
        assert bar.interval == "1d"


# ---------------------------------------------------------------------------
# Extended Hyperliquid adapter tests
# ---------------------------------------------------------------------------


class TestHyperliquidAdapterExtended:
    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "universe": [
                    {"name": "BTC", "szDecimals": 3},
                    {"name": "ETH", "szDecimals": 4},
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
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 2
        btc = next(r for r in results if r.raw_symbol == "BTC")
        assert btc.instrument_type == "PERPETUAL"
        assert btc.lot_size == Decimal("0.001")  # 10^-3

    @pytest.mark.asyncio
    async def test_get_instruments_empty_universe(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"universe": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_found_mocked(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"universe": [{"name": "BTC", "szDecimals": 3}]})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            inst = await adapter.get_instrument("BTC")
        assert inst is not None
        assert inst.raw_symbol == "BTC"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_mocked(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"universe": [{"name": "BTC", "szDecimals": 3}]})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            inst = await adapter.get_instrument("NOTEXIST")
        assert inst is None

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate is implemented for Hyperliquid — mock HTTP call."""
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[{"coin": "BTC", "fundingRate": "0.0001", "time": 0}])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented for Hyperliquid — mock HTTP call."""
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTC")
        assert isinstance(result, list)
