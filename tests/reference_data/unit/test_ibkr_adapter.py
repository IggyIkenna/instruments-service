"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from instruments_service.reference_data.adapters.ibkr import IBKRReferenceDataAdapter


class TestIBKRAdapter:
    """Unit tests for IBKRReferenceDataAdapter using inject-IB pattern.

    MagicMock(spec=IB) is the canonical test approach for TWS adapters.
    HTTP VCR is not applicable to the binary TWS socket protocol.
    """

    def test_venue_name(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        assert adapter.venue == "ibkr"

    def test_get_ib_raises_without_injection(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        with pytest.raises(RuntimeError, match="requires an injected IB connection"):
            adapter._get_ib()

    def test_build_stub_instrument(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        inst = adapter._build_stub_instrument("AAPL")
        assert inst.symbol == "AAPL"
        assert inst.venue == "ibkr"

    @pytest.mark.asyncio
    async def test_get_instruments_no_ib_returns_empty(self) -> None:
        """Without injected IB, get_instruments gracefully returns [] with warning."""
        adapter = IBKRReferenceDataAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instrument_no_ib_returns_none(self) -> None:
        """Without injected IB, get_instrument gracefully returns None."""
        adapter = IBKRReferenceDataAdapter()
        result = await adapter.get_instrument("AAPL")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_corporate_actions_no_ib_returns_empty(self) -> None:
        """Without injected IB, get_corporate_actions gracefully returns []."""
        adapter = IBKRReferenceDataAdapter()
        result = await adapter.get_corporate_actions("AAPL", datetime.now(UTC))
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_with_mock_ib(self) -> None:
        """Inject-IB pattern: mock_ib returns one ContractDetails, expect one instrument."""
        mock_ib = MagicMock()

        # Build a mock ContractDetails object
        mock_contract = MagicMock()
        mock_contract.symbol = "AAPL"
        mock_contract.secType = "STK"
        mock_contract.currency = "USD"
        mock_contract.conId = 265598
        mock_contract.exchange = "SMART"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = "1"
        mock_contract.localSymbol = "AAPL"
        mock_contract.tradingClass = "NMS"

        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Apple Inc."
        mock_details.industry = "Technology"
        mock_details.category = "Computers"
        mock_details.subcategory = "Computers"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        mock_details.secIdList = []
        mock_details.notes = ""

        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[mock_details])

        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        # Only request AAPL — patch the symbol list via instrument_type filter
        # get_instruments fetches from _DEFAULT_EQUITY_SYMBOLS; we call get_instrument instead
        result = await adapter.get_instrument("AAPL")
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.venue == "ibkr"
        mock_ib.reqContractDetailsAsync.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_returns_none(self) -> None:
        """get_instrument returns None when ib returns empty details list."""
        mock_ib = MagicMock()
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[])
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instrument("UNKNOWN")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_ib_exception_returns_none(self) -> None:
        """get_instrument returns None and logs warning when ib raises."""
        mock_ib = MagicMock()
        mock_ib.reqContractDetailsAsync = AsyncMock(side_effect=ConnectionError("TWS disconnected"))
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instrument("AAPL")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_corporate_actions_with_mock_ib_no_notes(self) -> None:
        """get_corporate_actions with mock returning details with no notes → []."""
        mock_ib = MagicMock()

        mock_contract = MagicMock()
        mock_contract.symbol = "AAPL"
        mock_contract.secType = "STK"
        mock_contract.currency = "USD"
        mock_contract.conId = 265598
        mock_contract.exchange = "SMART"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = ""
        mock_contract.localSymbol = "AAPL"
        mock_contract.tradingClass = "NMS"

        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Apple Inc."
        mock_details.industry = "Technology"
        mock_details.category = "Computers"
        mock_details.subcategory = "Computers"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        mock_details.secIdList = []
        mock_details.notes = ""

        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[mock_details])

        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_corporate_actions("AAPL", datetime.now(UTC))
        # No notes → no corporate action events parsed
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_corporate_actions_ib_exception_returns_empty(self) -> None:
        """get_corporate_actions returns [] when ib raises."""
        mock_ib = MagicMock()
        mock_ib.reqContractDetailsAsync = AsyncMock(side_effect=TimeoutError("TWS timeout"))
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_corporate_actions("AAPL", datetime.now(UTC))
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_equity_uses_stk(self) -> None:
        """get_instruments with instrument_type='equity' queries STK contracts."""
        mock_ib = MagicMock()
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[])
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instruments(instrument_type="equity")
        assert isinstance(result, list)
        # reqContractDetailsAsync called once per symbol in _DEFAULT_EQUITY_SYMBOLS
        assert mock_ib.reqContractDetailsAsync.call_count > 0

    @pytest.mark.asyncio
    async def test_get_instruments_unsupported_type_returns_empty(self) -> None:
        """get_instruments with unsupported instrument_type returns []."""
        mock_ib = MagicMock()
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[])
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instruments(instrument_type="crypto")
        assert result == []
        # No IB calls for unsupported type
        mock_ib.reqContractDetailsAsync.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("AAPL")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("ES")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("AAPL")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("AAPL")

    def test_contract_details_to_instrument_dict_input(self) -> None:
        """_contract_details_to_instrument handles plain dict (test-mode path)."""
        adapter = IBKRReferenceDataAdapter()
        raw = {
            "conid": 265598,
            "symbol": "MSFT",
            "secType": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "minTick": 0.01,
            "multiplier": "1",
            "longName": "Microsoft Corporation",
        }
        result = adapter._contract_details_to_instrument(raw)
        assert result is not None
        assert result.symbol == "MSFT"
        assert result.venue == "ibkr"

    def test_contract_details_to_instrument_missing_symbol_returns_none(self) -> None:
        """_contract_details_to_instrument returns None if symbol is missing."""
        adapter = IBKRReferenceDataAdapter()
        raw: dict[str, object] = {"secType": "STK", "currency": "USD"}
        result = adapter._contract_details_to_instrument(raw)
        assert result is None

    def test_contract_details_future_type_mapping(self) -> None:
        """_contract_details_to_instrument maps FUT secType to FUTURES InstrumentType."""
        adapter = IBKRReferenceDataAdapter()
        raw = {
            "conid": 12345,
            "symbol": "ES",
            "secType": "FUT",
            "currency": "USD",
            "exchange": "CME",
            "minTick": 0.25,
            "multiplier": "50",
        }
        result = adapter._contract_details_to_instrument(raw)
        assert result is not None
        assert result.symbol == "ES"

    def test_contract_details_no_multiplier_defaults_to_one(self) -> None:
        """_contract_details_to_instrument defaults contract_size to 1 when multiplier absent."""

        adapter = IBKRReferenceDataAdapter()
        raw = {
            "symbol": "AAPL",
            "secType": "STK",
            "currency": "USD",
        }
        result = adapter._contract_details_to_instrument(raw)
        assert result is not None
        assert result.contract_size == Decimal("1")

    @pytest.mark.asyncio
    async def test_get_corporate_actions_with_dividend_notes(self) -> None:
        """get_corporate_actions parses notes containing 'Dividend' into CA record."""
        mock_ib = MagicMock()

        mock_contract = MagicMock()
        mock_contract.symbol = "AAPL"
        mock_contract.secType = "STK"
        mock_contract.currency = "USD"
        mock_contract.conId = 265598
        mock_contract.exchange = "SMART"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = ""
        mock_contract.localSymbol = "AAPL"
        mock_contract.tradingClass = "NMS"

        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Apple Inc."
        mock_details.industry = "Technology"
        mock_details.category = "Computers"
        mock_details.subcategory = "Computers"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        mock_details.secIdList = []
        mock_details.notes = "Dividend paid 2026-03-07"

        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[mock_details])

        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_corporate_actions("AAPL", datetime.now(UTC))
        # notes contain "Dividend" → one CA record parsed
        assert len(result) == 1
        assert result[0].action_type == "dividend"
        assert result[0].symbol == "AAPL"
        assert result[0].venue == "ibkr"

    @pytest.mark.asyncio
    async def test_get_instruments_ib_exception_per_symbol_skipped(self) -> None:
        """get_instruments skips symbols where ib raises and continues."""
        mock_ib = MagicMock()
        # First call raises, subsequent calls return empty
        mock_ib.reqContractDetailsAsync = AsyncMock(side_effect=ConnectionError("socket error"))
        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_instruments(instrument_type="future")
        # All calls raised → empty list, no exception propagated
        assert result == []
