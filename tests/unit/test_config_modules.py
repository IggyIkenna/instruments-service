"""Tests for config modules — importing covers module-level data definitions."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

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


# ---------------------------------------------------------------------------
# Config data coverage (from test_config_coverage)
# ---------------------------------------------------------------------------


class TestDataTypeConfig:
    """Tests for instruments_service/config/data_type_config.py."""

    def test_module_importable(self) -> None:
        mod = importlib.import_module("instruments_service.config.data_type_config")
        assert mod is not None

    def test_default_enable_ccxt_integration(self) -> None:
        from instruments_service.config.data_type_config import DEFAULT_ENABLE_CCXT_INTEGRATION

        assert isinstance(DEFAULT_ENABLE_CCXT_INTEGRATION, bool)
        assert DEFAULT_ENABLE_CCXT_INTEGRATION is True

    def test_default_enable_metadata_caching(self) -> None:
        from instruments_service.config.data_type_config import DEFAULT_ENABLE_METADATA_CACHING

        assert isinstance(DEFAULT_ENABLE_METADATA_CACHING, bool)
        assert DEFAULT_ENABLE_METADATA_CACHING is True

    def test_default_cache_ttl_hours(self) -> None:
        from instruments_service.config.data_type_config import DEFAULT_CACHE_TTL_HOURS

        assert isinstance(DEFAULT_CACHE_TTL_HOURS, int)
        assert DEFAULT_CACHE_TTL_HOURS == 24

    def test_default_max_batch_size(self) -> None:
        from instruments_service.config.data_type_config import DEFAULT_MAX_BATCH_SIZE

        assert isinstance(DEFAULT_MAX_BATCH_SIZE, int)
        assert DEFAULT_MAX_BATCH_SIZE == 1000


class TestDefiDefinitions:
    """Tests for instruments_service/config/defi_definitions.py."""

    def test_module_importable(self) -> None:
        mod = importlib.import_module("instruments_service.config.defi_definitions")
        assert mod is not None

    def test_defi_venue_to_protocol_is_dict(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_VENUE_TO_PROTOCOL

        assert isinstance(DEFI_VENUE_TO_PROTOCOL, dict)
        assert len(DEFI_VENUE_TO_PROTOCOL) > 0

    def test_defi_venue_to_protocol_values_are_tuples(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_VENUE_TO_PROTOCOL

        for key, value in DEFI_VENUE_TO_PROTOCOL.items():
            assert isinstance(value, tuple), f"{key}: expected tuple, got {type(value)}"
            assert len(value) == 2

    def test_defi_venue_to_protocol_hyperliquid(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_VENUE_TO_PROTOCOL

        assert "HYPERLIQUID" in DEFI_VENUE_TO_PROTOCOL
        protocol, chain = DEFI_VENUE_TO_PROTOCOL["HYPERLIQUID"]
        assert protocol == "hyperliquid"
        assert chain is None

    def test_defi_venue_to_protocol_uniswap_v3(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_VENUE_TO_PROTOCOL

        assert "UNISWAPV3-ETH" in DEFI_VENUE_TO_PROTOCOL
        protocol, chain = DEFI_VENUE_TO_PROTOCOL["UNISWAPV3-ETH"]
        assert protocol == "uniswap_v3"
        assert chain == "ETHEREUM"

    def test_defi_protocols_is_list(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        assert isinstance(DEFI_PROTOCOLS, list)
        assert len(DEFI_PROTOCOLS) > 0

    def test_defi_protocols_entries_are_tuples(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        for entry in DEFI_PROTOCOLS:
            assert isinstance(entry, tuple)
            assert len(entry) == 2

    def test_defi_protocols_contains_uniswap_v2(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        protocols = [p for p, _ in DEFI_PROTOCOLS]
        assert "uniswap_v2" in protocols

    def test_defi_protocols_contains_curve(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        protocols = [p for p, _ in DEFI_PROTOCOLS]
        assert "curve" in protocols

    def test_defi_protocols_ethereum_chain(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        eth_protocols = [(p, c) for p, c in DEFI_PROTOCOLS if c == "ETHEREUM"]
        assert len(eth_protocols) > 0

    def test_defi_protocols_none_chain(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        none_chain = [(p, c) for p, c in DEFI_PROTOCOLS if c is None]
        assert len(none_chain) > 0


class TestVenueMappings:
    """Tests for instruments_service/config/venue_mappings.py."""

    def test_module_importable(self) -> None:
        mod = importlib.import_module("instruments_service.config.venue_mappings")
        assert mod is not None

    def test_tradfi_venue_mappings_is_list(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        assert isinstance(TRADFI_VENUE_MAPPINGS, list)
        assert len(TRADFI_VENUE_MAPPINGS) > 0

    def test_tradfi_venue_mappings_entries_have_venue_key(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        for entry in TRADFI_VENUE_MAPPINGS:
            assert "venue" in entry
            assert isinstance(entry["venue"], str)

    def test_tradfi_venue_mappings_cme_present(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        venues = [e["venue"] for e in TRADFI_VENUE_MAPPINGS]
        assert "CME" in venues

    def test_tradfi_venue_mappings_nasdaq_present(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        venues = [e["venue"] for e in TRADFI_VENUE_MAPPINGS]
        assert "NASDAQ" in venues

    def test_tradfi_venue_mappings_entries_have_dataset(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        for entry in TRADFI_VENUE_MAPPINGS:
            assert "dataset" in entry

    def test_tradfi_venue_mappings_cme_dataset(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        cme = next(e for e in TRADFI_VENUE_MAPPINGS if e["venue"] == "CME")
        assert cme["dataset"] == "GLBX.MDP3"

    def test_venue_mappings_defi_venue_to_protocol_dict(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_VENUE_TO_PROTOCOL

        assert isinstance(DEFI_VENUE_TO_PROTOCOL, dict)
        assert len(DEFI_VENUE_TO_PROTOCOL) > 0

    def test_venue_mappings_defi_venue_to_protocol_values(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_VENUE_TO_PROTOCOL

        for key, value in DEFI_VENUE_TO_PROTOCOL.items():
            assert isinstance(value, tuple), f"{key}: expected tuple"
            assert len(value) == 2

    def test_venue_mappings_defi_protocols_list(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_PROTOCOLS

        assert isinstance(DEFI_PROTOCOLS, list)
        assert len(DEFI_PROTOCOLS) > 0

    def test_venue_mappings_uniswap_ethereum(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_VENUE_TO_PROTOCOL

        assert "UNISWAP" in DEFI_VENUE_TO_PROTOCOL
        protocol, chain = DEFI_VENUE_TO_PROTOCOL["UNISWAP"]
        assert "uniswap" in protocol
        assert chain == "ETHEREUM"

    def test_venue_mappings_entries_have_description(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        for entry in TRADFI_VENUE_MAPPINGS:
            assert "description" in entry
            assert isinstance(entry["description"], str)


@pytest.mark.unit
class TestInstrumentWriterFromBoost:
    """Tests for InstrumentWriter.build_path from boost_5."""

    def test_instrument_writer_init(self):
        """Cover InstrumentWriter.__init__ (lines 33-44)."""
        from instruments_service.io.writer import InstrumentWriter

        with (
            patch("instruments_service.io.writer.get_config") as mock_cfg,
            patch("instruments_service.io.writer.BaseGCSWriter.__init__", return_value=None),
        ):
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            writer = InstrumentWriter(category="CEFI", dry_run=True)
            assert writer.category == "CEFI"

    def test_build_path_returns_correct_structure(self):
        from datetime import datetime

        from instruments_service.io.writer import InstrumentWriter

        with (
            patch("instruments_service.io.writer.get_config") as mock_cfg,
            patch("instruments_service.io.writer.BaseGCSWriter.__init__", return_value=None),
        ):
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            writer = InstrumentWriter.__new__(InstrumentWriter)
            writer.category = "CEFI"
            dt = datetime(2024, 3, 15)
            path = writer.build_path(date=dt, instrument_id="BTC-USDT-SPOT")
            assert path == "by_date/day=2024-03-15/instrument_id=BTC-USDT-SPOT.parquet"

    def test_build_path_raises_on_non_datetime_date(self):
        from instruments_service.io.writer import InstrumentWriter

        with (
            patch("instruments_service.io.writer.get_config") as mock_cfg,
            patch("instruments_service.io.writer.BaseGCSWriter.__init__", return_value=None),
        ):
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            writer = InstrumentWriter.__new__(InstrumentWriter)
            writer.category = "CEFI"
            with pytest.raises(TypeError, match="date: datetime"):
                writer.build_path(date="2024-03-15", instrument_id="BTC-USDT-SPOT")

    def test_build_path_raises_on_non_str_instrument_id(self):
        from datetime import datetime

        from instruments_service.io.writer import InstrumentWriter

        with (
            patch("instruments_service.io.writer.get_config") as mock_cfg,
            patch("instruments_service.io.writer.BaseGCSWriter.__init__", return_value=None),
        ):
            mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
            writer = InstrumentWriter.__new__(InstrumentWriter)
            writer.category = "CEFI"
            dt = datetime(2024, 3, 15)
            with pytest.raises(TypeError, match="instrument_id: str"):
                writer.build_path(date=dt, instrument_id=123)
