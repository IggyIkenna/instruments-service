"""Unit tests for venue adapters (no live network — uses mocked responses)."""

import pytest

from instruments_service.reference_data import create_reference_data_adapter
from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter
from instruments_service.reference_data.adapters.bybit import BybitReferenceDataAdapter
from instruments_service.reference_data.adapters.coinbase import CoinbaseReferenceDataAdapter
from instruments_service.reference_data.adapters.deribit import DeribitReferenceDataAdapter
from instruments_service.reference_data.adapters.hyperliquid import HyperliquidReferenceDataAdapter
from instruments_service.reference_data.adapters.ibkr import IBKRReferenceDataAdapter
from instruments_service.reference_data.adapters.okx import OKXReferenceDataAdapter


class TestFactory:
    def test_create_binance(self) -> None:
        adapter = create_reference_data_adapter("binance")
        assert isinstance(adapter, BinanceReferenceDataAdapter)
        assert adapter.venue == "binance"

    def test_create_bybit(self) -> None:
        adapter = create_reference_data_adapter("bybit")
        assert isinstance(adapter, BybitReferenceDataAdapter)

    def test_create_okx(self) -> None:
        adapter = create_reference_data_adapter("okx")
        assert isinstance(adapter, OKXReferenceDataAdapter)

    def test_create_deribit(self) -> None:
        adapter = create_reference_data_adapter("deribit")
        assert isinstance(adapter, DeribitReferenceDataAdapter)

    def test_create_coinbase(self) -> None:
        adapter = create_reference_data_adapter("coinbase")
        assert isinstance(adapter, CoinbaseReferenceDataAdapter)

    def test_create_hyperliquid(self) -> None:
        adapter = create_reference_data_adapter("hyperliquid")
        assert isinstance(adapter, HyperliquidReferenceDataAdapter)

    def test_create_ibkr(self) -> None:
        adapter = create_reference_data_adapter("ibkr")
        assert isinstance(adapter, IBKRReferenceDataAdapter)

    def test_unsupported_venue_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported venue"):
            create_reference_data_adapter("notavenue")

    def test_case_insensitive(self) -> None:
        adapter = create_reference_data_adapter("BINANCE")
        assert isinstance(adapter, BinanceReferenceDataAdapter)
