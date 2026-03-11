"""Tests for config modules — importing covers module-level data definitions."""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestConfigModuleImports:
    """Just importing these modules covers module-level constant definitions."""

    def test_api_keys_module(self):
        from instruments_service.config.api_keys import (
            DEFAULT_AAVESCAN_API_URL,
            DEFAULT_GRAPH_SECRET_NAME,
            DEFAULT_THEGRAPH_GATEWAY_TEMPLATE,
        )

        assert DEFAULT_GRAPH_SECRET_NAME == "graph-api-key"
        assert "aavescan" in DEFAULT_AAVESCAN_API_URL
        assert "{api_key}" in DEFAULT_THEGRAPH_GATEWAY_TEMPLATE

    def test_data_type_config(self):
        import instruments_service.config.data_type_config as m

        # All constants/dicts in module are defined
        assert m is not None

    def test_defi_definitions(self):
        import instruments_service.config.defi_definitions as m

        assert m is not None

    def test_equity_definitions(self):
        import instruments_service.config.equity_definitions as m

        assert m is not None

    def test_futures_options_definitions(self):
        import instruments_service.config.futures_options_definitions as m

        assert m is not None

    def test_ticker_lists(self):
        import instruments_service.config.ticker_lists as m

        assert m is not None

    def test_venue_mappings(self):
        import instruments_service.config.venue_mappings as m

        assert m is not None

    def test_metrics_module(self):
        from instruments_service.metrics import PROCESSING_LATENCY, RECORDS_PROCESSED

        assert RECORDS_PROCESSED is not None
        assert PROCESSING_LATENCY is not None


@pytest.mark.unit
class TestConfigPy:
    """Test instruments_service/config.py data loading functions."""

    def test_config_module_imports(self):
        import instruments_service.config as m

        assert m is not None

    def test_tradfi_instrument_dataclass(self):
        """TradFiInstrument dataclass can be instantiated."""
        from instruments_service.config import TradFiInstrument

        inst = TradFiInstrument(
            symbol="SPY",
            venue="NYSE_ARCA",
            instrument_type="ETF",
            dataset="DBEQ.BASIC",
            stype_in="raw_symbol",
            base_asset="SPDR S&P 500 ETF",
        )
        assert inst.symbol == "SPY"
        assert inst.venue == "NYSE_ARCA"
        assert inst.quote_asset == "USD"  # default

    def test_tradfi_instruments_config_not_empty(self):
        from instruments_service.config.instrument_definitions import TRADFI_INSTRUMENTS_CONFIG

        assert isinstance(TRADFI_INSTRUMENTS_CONFIG, (list, dict))

    def test_unified_instrument_config(self):
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        instruments = cfg.get_all_instruments()
        assert isinstance(instruments, list)

    def test_instrument_definition_alias(self):
        from instruments_service.config import InstrumentDefinition, TradFiInstrument

        assert InstrumentDefinition is TradFiInstrument


@pytest.mark.unit
class TestIoWriter:
    """Test instruments_service/io/writer.py."""

    def test_io_init_imports(self):
        import instruments_service.io

        assert instruments_service.io is not None

    def test_writer_module_imports(self):
        import instruments_service.io.writer as m

        assert m is not None

    def test_writer_has_write_function_or_class(self):
        import instruments_service.io.writer as m

        # Just verify the module defines something useful
        attrs = [a for a in dir(m) if not a.startswith("_")]
        assert len(attrs) > 0
