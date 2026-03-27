"""Unit tests for BaseReferenceDataAdapter and stub adapters."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data import FundingRateRef, OHLCVRef
from instruments_service.reference_data.adapters.ccxt_adapter import CCXTReferenceDataAdapter
from instruments_service.reference_data.adapters.databento import DatabentoReferenceDataAdapter
from instruments_service.reference_data.adapters.tardis import TardisReferenceDataAdapter
from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter
from instruments_service.reference_data.schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
)


class _ConcreteAdapter(BaseReferenceDataAdapter):
    @property
    def venue(self) -> str:
        return "test_venue"

    async def get_instruments(self, instrument_type: str | None = None) -> list[InstrumentRecord]:
        return []

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=expiry or datetime.now(UTC),
            fetched_at=datetime.now(UTC),
        )

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "future") -> CanonicalExpiryCalendar:
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("test stub")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        return []


class TestBaseAdapter:
    def test_venue_property(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter.venue == "test_venue"

    @pytest.mark.asyncio
    async def test_get_corporate_actions_default_empty(self) -> None:
        adapter = _ConcreteAdapter()
        actions = await adapter.get_corporate_actions("BTC", datetime.now(UTC))
        assert actions == []

    @pytest.mark.asyncio
    async def test_get_instruments_empty(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instrument_none(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_instrument("BTCUSDT")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_chain(self) -> None:
        adapter = _ConcreteAdapter()
        chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "test_venue"
        assert chain.underlying == "BTC"

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_calendar(self) -> None:
        adapter = _ConcreteAdapter()
        calendar = await adapter.get_expiry_calendar("BTC")
        assert calendar.venue == "test_venue"
        assert calendar.underlying == "BTC"
        assert calendar.instrument_type == "future"

    @pytest.mark.asyncio
    async def test_get_instruments_with_filter(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_instruments(instrument_type="spot")
        assert result == []

    def test_cannot_instantiate_abstract_base(self) -> None:
        with pytest.raises(TypeError):
            BaseReferenceDataAdapter()  # type: ignore[abstract]

    def test_optional_api_key_returns_none_without_project_id(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._project_id is None
        result = adapter._optional_api_key()
        assert result is None

    def test_optional_api_key_fetches_from_secret_manager(self) -> None:
        adapter = _ConcreteAdapter(project_id="my-project")
        mock_secret_client = MagicMock()
        mock_secret_client.get_secret.return_value = "test-api-key"
        with patch(
            "instruments_service.reference_data.base_adapter.get_secret_client",
            return_value=mock_secret_client,
        ):
            result = adapter._optional_api_key()
        assert result == "test-api-key"
        mock_secret_client.get_secret.assert_called_once_with("TEST_VENUE_API_KEY")

    def test_parse_raw_success(self) -> None:
        from pydantic import BaseModel

        class SimpleSchema(BaseModel):
            name: str
            value: int

        adapter = _ConcreteAdapter()
        result = adapter._parse_raw({"name": "hello", "value": 42}, SimpleSchema)
        assert result.name == "hello"
        assert result.value == 42

    def test_parse_raw_raises_runtime_error_on_caught_exception(self) -> None:
        from pydantic import BaseModel

        class BrokenSchema(BaseModel):
            value: int

            def __init__(self, **data: object) -> None:
                raise ValueError("simulated validation failure")

        adapter = _ConcreteAdapter()
        with (
            patch("instruments_service.reference_data.base_adapter.log_event"),
            pytest.raises(RuntimeError, match="Schema validation failed"),
        ):
            adapter._parse_raw({"value": 1}, BrokenSchema)

    def test_init_with_project_id(self) -> None:
        adapter = _ConcreteAdapter(project_id="proj-123")
        assert adapter._project_id == "proj-123"


# ---------------------------------------------------------------------------
# CCXTReferenceDataAdapter
# ---------------------------------------------------------------------------


class TestCCXTAdapter:
    def test_venue_name(self) -> None:
        adapter = CCXTReferenceDataAdapter("kraken")
        assert adapter.venue == "kraken"

    def test_venue_name_custom(self) -> None:
        adapter = CCXTReferenceDataAdapter("ftx")
        assert adapter.venue == "ftx"

    def test_init_with_project_id(self) -> None:
        adapter = CCXTReferenceDataAdapter("kraken", project_id="my-project")
        assert adapter.venue == "kraken"
        assert adapter._project_id == "my-project"

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        """CCXT adapter delegates to ccxt — mock load_markets to verify mapping."""
        adapter = CCXTReferenceDataAdapter("kraken")
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(
            return_value={
                "BTC/USD": {
                    "active": True,
                    "type": "spot",
                    "base": "BTC",
                    "quote": "USD",
                    "id": "XBTUSD",
                    "settle": None,
                    "expiryDatetime": None,
                    "precision": {"price": "0.01", "amount": "0.001"},
                    "limits": {"amount": {"min": "0.001"}},
                    "contractSize": None,
                }
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].raw_symbol == "XBTUSD"

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filtered(self) -> None:
        """CCXT adapter filters by instrument_type correctly."""
        adapter = CCXTReferenceDataAdapter("kraken")
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_instruments(instrument_type="perp")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_on_empty(self) -> None:
        """CCXT get_instrument returns None when no match found."""
        adapter = CCXTReferenceDataAdapter("kraken")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("BTC/USD")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_empty(self) -> None:
        """CCXT get_options_chain returns empty chain when no options exist."""
        adapter = CCXTReferenceDataAdapter("kraken")
        with patch.object(adapter, "get_instruments", return_value=[]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "kraken"
        assert chain.calls == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_empty(self) -> None:
        """CCXT get_expiry_calendar returns empty calendar when no futures exist."""
        adapter = CCXTReferenceDataAdapter("kraken")
        with patch.object(adapter, "get_instruments", return_value=[]):
            calendar = await adapter.get_expiry_calendar("BTC")
        assert calendar.venue == "kraken"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """CCXT get_funding_rate delegates to ccxt fetch_funding_rate."""
        adapter = CCXTReferenceDataAdapter("kraken")
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_funding_rate = AsyncMock(
            return_value={
                "fundingRate": "0.0001",
                "markPrice": "30000",
                "nextFundingDatetime": None,
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            result = await adapter.get_funding_rate("BTC/USD")
        assert result is not None
        assert result.venue == "kraken"

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """CCXT get_ohlcv delegates to ccxt fetch_ohlcv."""
        adapter = CCXTReferenceDataAdapter("kraken")
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_ohlcv = AsyncMock(return_value=[])
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            result = await adapter.get_ohlcv("BTC/USD")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_bars(self) -> None:
        """CCXT get_ohlcv parses bar data into OHLCVRef list."""
        adapter = CCXTReferenceDataAdapter("kraken")
        ts_ms = 1700000000000
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_ohlcv = AsyncMock(return_value=[[ts_ms, "30000", "31000", "29000", "30500", "100"]])
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            result = await adapter.get_ohlcv("BTC/USD")
        assert len(result) == 1
        assert result[0].venue == "kraken"

    def test_get_exchange_unsupported_raises(self) -> None:
        """_get_exchange raises RuntimeError for unknown venue names."""
        adapter = CCXTReferenceDataAdapter("not_a_real_exchange_xyz")
        with pytest.raises(RuntimeError, match="ccxt does not support exchange"):
            adapter._get_exchange()

    def test_map_ccxt_type_all_cases(self) -> None:
        """_map_ccxt_type covers all known mapping cases."""
        assert CCXTReferenceDataAdapter._map_ccxt_type("spot") == "spot"
        assert CCXTReferenceDataAdapter._map_ccxt_type("swap") == "perp"
        assert CCXTReferenceDataAdapter._map_ccxt_type("perpetual") == "perp"
        assert CCXTReferenceDataAdapter._map_ccxt_type("future") == "future"
        assert CCXTReferenceDataAdapter._map_ccxt_type("option") == "option"
        assert CCXTReferenceDataAdapter._map_ccxt_type("unknown_type") == "unknown_type"

    def test_parse_ccxt_expiry_valid(self) -> None:
        """_parse_ccxt_expiry parses ISO datetime strings to UTC datetimes."""
        result = CCXTReferenceDataAdapter._parse_ccxt_expiry("2024-03-31T08:00:00Z")
        assert result is not None
        assert result.year == 2024

    def test_parse_ccxt_expiry_invalid(self) -> None:
        """_parse_ccxt_expiry returns None for invalid strings."""
        assert CCXTReferenceDataAdapter._parse_ccxt_expiry("not-a-date") is None
        assert CCXTReferenceDataAdapter._parse_ccxt_expiry(None) is None
        assert CCXTReferenceDataAdapter._parse_ccxt_expiry("") is None

    @pytest.mark.asyncio
    async def test_get_instruments_inactive_market_skipped(self) -> None:
        """Markets with active=False are skipped."""
        adapter = CCXTReferenceDataAdapter("kraken")
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(
            return_value={
                "BTC/USD": {
                    "active": False,
                    "type": "spot",
                    "base": "BTC",
                    "quote": "USD",
                    "id": "XBTUSD",
                    "settle": None,
                    "expiryDatetime": None,
                    "precision": {},
                    "limits": {},
                    "contractSize": None,
                },
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_options_chain_with_option_instruments(self) -> None:
        """CCXT get_options_chain filters by underlying and groups calls/puts."""
        from datetime import UTC, datetime
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord

        adapter = CCXTReferenceDataAdapter("deribit")
        opt_call = InstrumentRecord(
            instrument_key="BTC/USD-call",
            venue="deribit",
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
        with patch.object(adapter, "get_instruments", return_value=[opt_call]):
            chain = await adapter.get_options_chain("BTC")
        assert len(chain.calls) == 1
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_futures(self) -> None:
        """CCXT get_expiry_calendar extracts expiry dates from futures."""
        from datetime import UTC, datetime
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord

        adapter = CCXTReferenceDataAdapter("deribit")
        expiry = datetime(2024, 3, 31, tzinfo=UTC)
        fut = InstrumentRecord(
            instrument_key="BTC-31MAR24",
            venue="deribit",
            symbol="BTC/USD",
            raw_symbol="BTC-31MAR24",
            instrument_type="future",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.5"),
            lot_size=Decimal("10"),
            min_order_size=Decimal("10"),
            contract_size=Decimal("1"),
            expiry=expiry,
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[fut]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="future")
        assert len(calendar.expiries) == 1
        assert calendar.expiries[0] == expiry


# ---------------------------------------------------------------------------
# TardisReferenceDataAdapter
# ---------------------------------------------------------------------------


class TestTardisAdapter:
    def test_venue_name(self) -> None:
        adapter = TardisReferenceDataAdapter()
        assert adapter.venue == "tardis"

    def test_init_no_project_id(self) -> None:
        adapter = TardisReferenceDataAdapter()
        assert adapter._project_id is None

    def test_init_with_project_id(self) -> None:
        adapter = TardisReferenceDataAdapter(project_id="proj")
        assert adapter._project_id == "proj"

    @pytest.mark.asyncio
    async def test_get_instruments_returns_empty_on_network_error(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch.object(adapter, "get_instruments", return_value=[]):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_on_empty(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("BTC-PERP")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_empty_chain(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "tardis"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_empty(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            calendar = await adapter.get_expiry_calendar("BTC")
        assert calendar.venue == "tardis"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises_runtime_error_when_no_data(self) -> None:
        # With exchanges=[], _scan_exchanges_for_funding_rate returns (None, fallback)
        # and get_funding_rate raises RuntimeError — not NotImplementedError.
        adapter = TardisReferenceDataAdapter(exchanges=[])
        with pytest.raises(RuntimeError, match="No funding rate"):
            await adapter.get_funding_rate("BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_ohlcv_returns_empty_when_no_exchanges(self) -> None:
        # With exchanges=[], _collect_ohlcv_from_exchanges yields no results.
        adapter = TardisReferenceDataAdapter(exchanges=[])
        result = await adapter.get_ohlcv("BTC-PERP")
        assert result == []


# ---------------------------------------------------------------------------
# DatabentoReferenceDataAdapter
# ---------------------------------------------------------------------------


class TestDatabentoAdapter:
    def test_venue_name(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        assert adapter.venue == "databento"

    def test_init_no_project_id(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        assert adapter._project_id is None

    def test_init_with_project_id(self) -> None:
        adapter = DatabentoReferenceDataAdapter(project_id="proj")
        assert adapter._project_id == "proj"

    @pytest.mark.asyncio
    async def test_get_instruments_returns_empty_without_api_key(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(RuntimeError, match="Databento requires an API key"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_raises_without_api_key(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(RuntimeError, match="Databento requires an API key"):
            await adapter.get_instruments(instrument_type="future")

    @pytest.mark.asyncio
    async def test_get_instrument_raises_without_api_key(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(RuntimeError, match="Databento requires an API key"):
            await adapter.get_instrument("ES.FUT.CME")

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            chain = await adapter.get_options_chain("SPY")
        assert chain.venue == "databento"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            calendar = await adapter.get_expiry_calendar("ES")
        assert calendar.venue == "databento"
        assert calendar.expiries == []

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("ES.FUT.CME")
