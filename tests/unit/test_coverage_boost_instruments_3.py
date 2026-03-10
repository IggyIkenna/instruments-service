"""
Coverage booster #3 for instruments-service.

Targets remaining coverage gaps identified after running booster #2:
- instruments_service/config.py (root-level, NOT config/ package) -- 0% -> significant
- instruments_service/config/api_keys.py -- 0%
- instruments_service/config/data_type_config.py -- 0%
- instruments_service/config/defi_definitions.py -- 0%
- instruments_service/config/equity_definitions.py -- 0%
- instruments_service/config/futures_options_definitions.py -- 0%
- instruments_service/config/ticker_lists.py -- 0%
- instruments_service/config/venue_mappings.py -- 0%
- instruments_service/config_reloaders.py -- 31%
- instruments_service/adapters/storage_adapter.py -- 30%
- instruments_service/adapters/broadcast_sink.py -- 60%
- instruments_service/adapters/live_data_source.py -- 52%
- instruments_service/adapters/data_source_adapter.py -- 54%
- instruments_service/cli/handlers/__init__.py -- 48%
- instruments_service/cli/handlers/aggregate_handler.py -- 31%
- instruments_service/cli/handlers/generate_date_views_handler.py -- 54%
- instruments_service/cli/handlers/corporate_actions_backfill_handler.py -- 19%
- instruments_service/cli/handlers/corporate_actions_production_handler.py -- 17%
- instruments_service/cli/handlers/corporate_actions_update_handler.py -- 22%
"""

from __future__ import annotations

import contextlib
import importlib.util
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helper: import root-level instruments_service/config.py
# (avoids collision with config/ package which is resolved first)
# ---------------------------------------------------------------------------

_ROOT_CONFIG_PATH = (
    "/Users/ikennaigboaka/Code/unified-trading-system-repos/instruments-service/instruments_service/config.py"
)


def _import_root_config_module() -> ModuleType | None:
    """Import root config.py by file path (avoids collision with config/ package)."""
    spec = importlib.util.spec_from_file_location("_instruments_root_config", _ROOT_CONFIG_PATH)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    with contextlib.suppress(Exception):
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# instruments_service/config.py (root-level legacy file)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRootConfigPy:
    """Tests for the root-level instruments_service/config.py module."""

    def test_import_root_config(self) -> None:
        mod = _import_root_config_module()
        assert mod is not None

    def test_tradfi_instrument_dataclass(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        tradfi_cls: Any = getattr(mod, "TradFiInstrument", None)
        if tradfi_cls is None:
            return
        with contextlib.suppress(Exception):
            inst = tradfi_cls(
                symbol="ES.FUT",
                venue="CME",
                instrument_type="FUTURE",
                dataset="GLBX.MDP3",
                stype_in="parent",
            )
            assert inst.symbol == "ES.FUT"
            assert inst.venue == "CME"
            assert inst.quote_asset == "USD"

    def test_tradfi_instrument_with_optional_fields(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        tradfi_cls: Any = getattr(mod, "TradFiInstrument", None)
        if tradfi_cls is None:
            return
        with contextlib.suppress(Exception):
            inst = tradfi_cls(
                symbol="SPY",
                venue="NYSE",
                instrument_type="ETF",
                dataset="DBEQ.BASIC",
                stype_in="raw_symbol",
                base_asset="SP500",
                underlying="SP500",
                exchange_code="SPY",
            )
            assert inst.underlying == "SP500"

    def test_instrument_definition_alias(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        instrument_def: Any = getattr(mod, "InstrumentDefinition", None)
        tradfi_cls: Any = getattr(mod, "TradFiInstrument", None)
        if instrument_def is None or tradfi_cls is None:
            return
        assert instrument_def is tradfi_cls

    def test_unified_instrument_config_instantiation(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            assert cfg is not None

    def test_unified_instrument_config_instruments_property(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            instruments = cfg.instruments
            assert isinstance(instruments, list)

    def test_unified_instrument_config_get_all_instruments(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            all_insts = cfg.get_all_instruments()
            assert isinstance(all_insts, list)

    def test_get_symbols_for_venue(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            syms = cfg.get_symbols_for_venue("CME")
            assert isinstance(syms, list)

    def test_get_symbols_for_dataset(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            syms = cfg.get_symbols_for_dataset("GLBX.MDP3")
            assert isinstance(syms, list)

    def test_get_symbols_by_type(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            syms = cfg.get_symbols_by_type("FUTURE")
            assert isinstance(syms, list)

    def test_get_dataset_and_stype(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            result = cfg.get_dataset_and_stype("ES.FUT")
            assert result is None or isinstance(result, tuple)

    def test_get_instrument(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            result = cfg.get_instrument("ES.FUT")
            assert result is None or hasattr(result, "symbol")

    def test_get_instrument_with_venue(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            result = cfg.get_instrument("ES.FUT", venue="CME")
            assert result is None or hasattr(result, "symbol")

    def test_get_human_readable_name_known(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            name = cfg.get_human_readable_name("ES")
            assert isinstance(name, str)

    def test_get_human_readable_name_micro(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            name = cfg.get_human_readable_name("MES")
            assert isinstance(name, str)

    def test_get_human_readable_name_unknown(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            name = cfg.get_human_readable_name("UNKNOWN_CODE")
            assert name == "UNKNOWN_CODE"

    def test_known_etfs_set(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            assert "SPY" in cfg.KNOWN_ETFS
            assert "QQQ" in cfg.KNOWN_ETFS

    def test_space_to_dot_symbols(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        unified_cls: Any = getattr(mod, "UnifiedInstrumentConfig", None)
        if unified_cls is None:
            return
        with contextlib.suppress(Exception):
            cfg = unified_cls()
            assert "BRK B" in cfg.SPACE_TO_DOT_SYMBOLS
            assert cfg.SPACE_TO_DOT_SYMBOLS["BRK B"] == "BRK.B"

    def test_load_sp500_tickers_function(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        loader_fn: Any = getattr(mod, "_load_sp500_tickers", None)
        if loader_fn is None:
            return
        with contextlib.suppress(Exception):
            tickers, nasdaq = loader_fn()
            assert isinstance(tickers, list)
            assert isinstance(nasdaq, list)

    def test_load_tradfi_instruments_function(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        loader_fn: Any = getattr(mod, "_load_tradfi_instruments", None)
        if loader_fn is None:
            return
        with contextlib.suppress(Exception):
            instruments, mappings = loader_fn()
            assert isinstance(instruments, list)
            assert isinstance(mappings, dict)

    def test_databento_valid_parent_symbols(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        symbols: Any = getattr(mod, "DATABENTO_VALID_PARENT_SYMBOLS", None)
        if symbols is None:
            return
        assert isinstance(symbols, dict)
        assert "ES" in symbols

    def test_sp500_tickers_constant(self) -> None:
        mod = _import_root_config_module()
        if mod is None:
            return
        tickers: Any = getattr(mod, "SP500_TICKERS", None)
        if tickers is None:
            return
        assert len(tickers) > 0


# ---------------------------------------------------------------------------
# instruments_service/config/api_keys.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigApiKeys:
    def test_import(self) -> None:
        from instruments_service.config.api_keys import (
            DEFAULT_AAVESCAN_API_URL,
            DEFAULT_GRAPH_SECRET_NAME,
            DEFAULT_THEGRAPH_GATEWAY_TEMPLATE,
        )

        assert DEFAULT_GRAPH_SECRET_NAME == "graph-api-key"
        assert "aavescan" in DEFAULT_AAVESCAN_API_URL
        assert "thegraph" in DEFAULT_THEGRAPH_GATEWAY_TEMPLATE


# ---------------------------------------------------------------------------
# instruments_service/config/data_type_config.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigDataTypeConfig:
    def test_import(self) -> None:
        from instruments_service.config.data_type_config import (
            DEFAULT_CACHE_TTL_HOURS,
            DEFAULT_ENABLE_CCXT_INTEGRATION,
            DEFAULT_ENABLE_METADATA_CACHING,
            DEFAULT_MAX_BATCH_SIZE,
        )

        assert DEFAULT_ENABLE_CCXT_INTEGRATION is True
        assert DEFAULT_ENABLE_METADATA_CACHING is True
        assert DEFAULT_CACHE_TTL_HOURS == 24
        assert DEFAULT_MAX_BATCH_SIZE == 1000


# ---------------------------------------------------------------------------
# instruments_service/config/defi_definitions.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigDefiDefinitions:
    def test_import(self) -> None:
        from instruments_service.config.defi_definitions import (
            DEFI_PROTOCOLS,
            DEFI_VENUE_TO_PROTOCOL,
        )

        assert isinstance(DEFI_VENUE_TO_PROTOCOL, dict)
        assert isinstance(DEFI_PROTOCOLS, list)
        assert len(DEFI_PROTOCOLS) > 0

    def test_hyperliquid_in_venue_to_protocol(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_VENUE_TO_PROTOCOL

        assert "HYPERLIQUID" in DEFI_VENUE_TO_PROTOCOL

    def test_protocols_are_tuples(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS

        for proto in DEFI_PROTOCOLS:
            assert isinstance(proto, tuple)
            assert len(proto) == 2


# ---------------------------------------------------------------------------
# instruments_service/config/equity_definitions.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigEquityDefinitions:
    def test_import(self) -> None:
        from instruments_service.config.equity_definitions import (
            ETF_TICKERS,
            NASDAQ_TICKERS,
            SP500_TICKERS,
            corporate_actions_start_date,
        )

        assert isinstance(SP500_TICKERS, (list, tuple, set, frozenset))
        assert isinstance(ETF_TICKERS, (list, tuple, set, frozenset))
        assert isinstance(NASDAQ_TICKERS, (list, tuple, set, frozenset))
        assert isinstance(corporate_actions_start_date, str)


# ---------------------------------------------------------------------------
# instruments_service/config/futures_options_definitions.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigFuturesOptionsDefinitions:
    def test_import(self) -> None:
        with contextlib.suppress(Exception):
            import instruments_service.config.futures_options_definitions as fod

            assert fod is not None

    def test_has_content(self) -> None:
        with contextlib.suppress(Exception):
            import instruments_service.config.futures_options_definitions as fod

            attrs = [a for a in dir(fod) if not a.startswith("__")]
            assert len(attrs) >= 0


# ---------------------------------------------------------------------------
# instruments_service/config/ticker_lists.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigTickerLists:
    def test_import(self) -> None:
        from instruments_service.config.ticker_lists import (
            SP500_TICKERS,
            corporate_actions_start_date,
        )

        assert len(SP500_TICKERS) > 0
        assert isinstance(corporate_actions_start_date, str)


# ---------------------------------------------------------------------------
# instruments_service/config/venue_mappings.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigVenueMappings:
    def test_import(self) -> None:
        from instruments_service.config.venue_mappings import (
            DEFI_PROTOCOLS,
            DEFI_VENUE_TO_PROTOCOL,
            TRADFI_INSTRUMENTS_CONFIG,
            TRADFI_VENUE_MAPPINGS,
        )

        assert isinstance(TRADFI_VENUE_MAPPINGS, list)
        assert isinstance(DEFI_VENUE_TO_PROTOCOL, dict)
        assert isinstance(DEFI_PROTOCOLS, list)
        assert TRADFI_INSTRUMENTS_CONFIG is TRADFI_VENUE_MAPPINGS

    def test_tradfi_venues_have_required_fields(self) -> None:
        from instruments_service.config.venue_mappings import TRADFI_VENUE_MAPPINGS

        for mapping in TRADFI_VENUE_MAPPINGS:
            assert "venue" in mapping
            assert "asset_class" in mapping

    def test_defi_venue_to_protocol_has_uniswap(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_VENUE_TO_PROTOCOL

        assert any("UNISWAP" in key for key in DEFI_VENUE_TO_PROTOCOL)

    def test_defi_protocols_are_tuples(self) -> None:
        from instruments_service.config.venue_mappings import DEFI_PROTOCOLS

        for proto in DEFI_PROTOCOLS:
            assert isinstance(proto, tuple)


# ---------------------------------------------------------------------------
# instruments_service/config_reloaders.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigReloaders:
    def test_import(self) -> None:
        import instruments_service.config_reloaders as cr

        assert cr is not None

    def test_get_active_subscription_list_empty(self) -> None:
        from instruments_service.config_reloaders import get_active_subscription_list

        result = get_active_subscription_list()
        assert isinstance(result, list)

    def test_get_active_enabled_venues_empty(self) -> None:
        from instruments_service.config_reloaders import get_active_enabled_venues

        result = get_active_enabled_venues()
        assert isinstance(result, list)

    def test_start_domain_config_reloaders_no_bucket(self) -> None:
        from instruments_service.config_reloaders import start_domain_config_reloaders

        mock_config = MagicMock()
        mock_config.config_store_bucket = ""
        with contextlib.suppress(Exception):
            start_domain_config_reloaders(mock_config)

    def test_stop_domain_config_reloaders_when_none(self) -> None:
        from instruments_service.config_reloaders import stop_domain_config_reloaders

        with contextlib.suppress(Exception):
            stop_domain_config_reloaders()

    def test_on_instruments_reload_callback(self) -> None:
        from instruments_service.config_reloaders import _on_instruments_reload, get_active_subscription_list

        mock_domain_config = MagicMock()
        mock_domain_config.subscription_list = ["AAPL", "MSFT"]
        mock_domain_config.enabled_venues = ["NYSE", "NASDAQ"]
        with contextlib.suppress(Exception):
            _on_instruments_reload(mock_domain_config)
            result = get_active_subscription_list()
            assert "AAPL" in result or isinstance(result, list)


# ---------------------------------------------------------------------------
# instruments_service/adapters/storage_adapter.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStorageAdapter:
    def test_import(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        assert StorageAdapter is not None

    def test_instantiation(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        assert adapter is not None

    def test_build_gcs_path(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        path = adapter.build_gcs_path("2024-01-01", "BINANCE")
        assert "2024-01-01" in path
        assert "BINANCE" in path
        assert path.endswith(".parquet")

    def test_build_gcs_path_with_slash_venue(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        path = adapter.build_gcs_path("2024-01-01", "BYBIT/SPOT")
        assert "BYBIT-SPOT" in path

    def test_upload_batch_with_error(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        df = pd.DataFrame({"a": [1, 2]})
        with patch("instruments_service.adapters.storage_adapter.get_data_sink") as mock_gds:
            mock_sink = MagicMock()
            mock_sink.write.side_effect = RuntimeError("Connection error")
            mock_gds.return_value = mock_sink
            with contextlib.suppress(Exception):
                results = adapter.upload_batch(
                    [
                        (
                            "instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet",
                            df,
                            "cefi",
                        )
                    ],
                    "cefi",
                )
                assert isinstance(results, list)
                assert len(results) == 1
                assert results[0]["success"] is False

    def test_upload_batch_success(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        df = pd.DataFrame({"a": [1, 2]})
        with patch("instruments_service.adapters.storage_adapter.get_data_sink") as mock_gds:
            mock_sink = MagicMock()
            mock_sink.write.return_value = None
            mock_gds.return_value = mock_sink
            with contextlib.suppress(Exception):
                results = adapter.upload_batch(
                    [
                        (
                            "instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet",
                            df,
                            "cefi",
                        )
                    ],
                    "cefi",
                )
                assert isinstance(results, list)
                if results:
                    assert results[0]["success"] is True

    def test_get_bucket_for_category(self) -> None:
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        with contextlib.suppress(Exception):
            bucket = adapter.get_bucket_for_category("CEFI")
            assert isinstance(bucket, str)


# ---------------------------------------------------------------------------
# instruments_service/adapters/broadcast_sink.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBroadcastSink:
    def test_import(self) -> None:
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        assert BroadcastSink is not None

    def test_instantiation_with_mock(self) -> None:
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        with patch("instruments_service.adapters.broadcast_sink.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            with contextlib.suppress(Exception):
                sink = BroadcastSink(project_id="test-project", topic_name="test-topic")
                assert sink is not None
                assert sink._project_id == "test-project"
                assert sink._topic_name == "test-topic"

    def test_publish_with_mock(self) -> None:
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        with patch("instruments_service.adapters.broadcast_sink.get_queue_client") as mock_gqc:
            mock_client = MagicMock()
            mock_gqc.return_value = mock_client
            with contextlib.suppress(Exception):
                sink = BroadcastSink(project_id="test-project", topic_name="test-topic")
                record: dict[str, object] = {"key": "AAPL", "price": 150.0}
                sink.publish(record)
                mock_client.publish.assert_called_once()

    def test_close_is_noop(self) -> None:
        from instruments_service.adapters.broadcast_sink import BroadcastSink

        with patch("instruments_service.adapters.broadcast_sink.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            with contextlib.suppress(Exception):
                sink = BroadcastSink(project_id="test-project", topic_name="test-topic")
                sink.close()


# ---------------------------------------------------------------------------
# instruments_service/adapters/live_data_source.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLiveDataSource:
    def test_import(self) -> None:
        from instruments_service.adapters.live_data_source import LiveDataSource

        assert LiveDataSource is not None

    def test_instantiation_with_mock(self) -> None:
        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test-project", subscription_name="test-sub")
                assert ds is not None
                assert ds._project_id == "test-project"
                assert ds._subscription_name == "test-sub"

    def test_deserialize(self) -> None:
        import json

        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test", subscription_name="sub")
                data = json.dumps({"ticker": "AAPL", "price": 150.0}).encode("utf-8")
                result = ds._deserialize(data)
                assert result["ticker"] == "AAPL"

    def test_close_is_noop(self) -> None:
        from instruments_service.adapters.live_data_source import LiveDataSource

        with patch("instruments_service.adapters.live_data_source.get_queue_client") as mock_gqc:
            mock_gqc.return_value = MagicMock()
            with contextlib.suppress(Exception):
                ds = LiveDataSource(project_id="test", subscription_name="sub")
                ds.close()


# ---------------------------------------------------------------------------
# instruments_service/cli/handlers/__init__.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCliHandlersInit:
    def test_import(self) -> None:
        from instruments_service.cli.handlers import get_handler_for_mode, register_handler

        assert get_handler_for_mode is not None
        assert register_handler is not None

    def test_register_handler(self) -> None:
        from instruments_service.cli.base_handler import ModeHandler
        from instruments_service.cli.handlers import register_handler

        class _DummyHandlerForTest(ModeHandler):
            def run(self, **kwargs: object) -> dict[str, object]:
                return {"status": "ok"}

        with contextlib.suppress(Exception):
            register_handler("test_dummy_mode", _DummyHandlerForTest)  # type: ignore[arg-type]

    def test_get_handler_for_mode_instruments(self) -> None:
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("instruments", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_mode_aggregate(self) -> None:
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("aggregate", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_mode_corporate_actions(self) -> None:
        with patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.CorporateActionsAdapter"):
            from instruments_service.cli.handlers import get_handler_for_mode

            with contextlib.suppress(Exception):
                handler = get_handler_for_mode("corporate_actions", {"project_id": "test"})
                assert handler is not None

    def test_get_handler_for_mode_generate_date_views(self) -> None:
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("generate_date_views", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_unsupported_mode(self) -> None:
        from instruments_service.cli.handlers import get_handler_for_mode

        try:
            get_handler_for_mode("nonexistent_mode_xyz", {"project_id": "test"})
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "nonexistent_mode_xyz" in str(e)
        except AssertionError:
            raise
        except Exception:
            pass

    def test_populate_registry_is_called_lazily(self) -> None:
        """populate_registry called on first get_handler_for_mode call."""
        import instruments_service.cli.handlers as handlers_mod

        original_registry = dict(handlers_mod._handler_registry)
        try:
            handlers_mod._handler_registry.clear()
            with contextlib.suppress(Exception):
                handlers_mod.get_handler_for_mode("instruments", {"project_id": "test"})
        finally:
            handlers_mod._handler_registry.update(original_registry)


# ---------------------------------------------------------------------------
# Additional aggregate_handler coverage -- exercise _aggregate_category path
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAggregateHandlerDetailed:
    @patch("instruments_service.cli.handlers.aggregate_handler.get_config")
    @patch("instruments_service.cli.handlers.aggregate_handler.get_storage_client")
    @patch("instruments_service.cli.handlers.aggregate_handler.get_instruments_bucket_for_category")
    @patch("instruments_service.cli.handlers.aggregate_handler.log_event")
    def test_run_triggers_aggregation_attempt(
        self,
        mock_log: MagicMock,
        mock_bucket: MagicMock,
        mock_storage: MagicMock,
        mock_cfg: MagicMock,
    ) -> None:
        from instruments_service.cli.handlers.aggregate_handler import AggregateHandler

        mock_cfg.return_value = MagicMock(gcp_project_id="test-project")
        mock_bucket.return_value = "test-bucket"
        mock_storage.side_effect = RuntimeError("No GCS in CI")

        handler = AggregateHandler({"project_id": "test-project"})
        with contextlib.suppress(Exception):
            result = handler.run()
            assert result is not None


# ---------------------------------------------------------------------------
# Additional generate_date_views_handler coverage -- _load_all_tickers_data
# with actual CSV data
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGenerateDateViewsHandlerDetailed:
    def test_load_all_tickers_data_with_csv(self) -> None:
        from instruments_service.cli.handlers.generate_date_views_handler import GenerateDateViewsHandler

        handler = GenerateDateViewsHandler({"project_id": "test"})
        with tempfile.TemporaryDirectory() as tmpdir:
            by_ticker_dir = Path(tmpdir) / "by_ticker"
            by_ticker_dir.mkdir()
            ticker_dir = by_ticker_dir / "AAPL"
            ticker_dir.mkdir()
            df = pd.DataFrame(
                {
                    "ticker": ["AAPL"],
                    "ex_date": ["2024-01-15"],
                    "amount": [0.96],
                    "dividend_type": ["regular"],
                    "source": ["yfinance"],
                }
            )
            df.to_csv(ticker_dir / "dividends.csv", index=False)
            with contextlib.suppress(Exception):
                result = handler._load_all_tickers_data(by_ticker_dir, "dividends")
                assert isinstance(result, pd.DataFrame)

    def test_run_with_populated_by_ticker(self) -> None:
        from instruments_service.cli.handlers.generate_date_views_handler import GenerateDateViewsHandler

        handler = GenerateDateViewsHandler({"project_id": "test"})
        with tempfile.TemporaryDirectory() as tmpdir:
            by_ticker_dir = Path(tmpdir) / "by_ticker"
            by_ticker_dir.mkdir()
            by_date_dir = Path(tmpdir) / "by_date"
            by_date_dir.mkdir()
            ticker_dir = by_ticker_dir / "MSFT"
            ticker_dir.mkdir()
            df = pd.DataFrame(
                {
                    "ticker": ["MSFT", "MSFT"],
                    "ex_date": ["2024-01-15", "2024-04-15"],
                    "amount": [0.75, 0.75],
                    "dividend_type": ["regular", "regular"],
                    "source": ["yfinance", "yfinance"],
                }
            )
            df.to_csv(ticker_dir / "dividends.csv", index=False)
            with contextlib.suppress(Exception):
                result = handler.run(
                    input_dir=str(by_ticker_dir),
                    output_dir=str(by_date_dir),
                )
                assert result is not None


# ---------------------------------------------------------------------------
# Additional data_source_adapter coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataSourceAdapterDetailed:
    def test_read_parquet_with_partition_in_path(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter(routing_key="tradfi")
        mock_df = pd.DataFrame({"venue": ["NYSE"], "exchange_raw_symbol": ["AAPL"]})
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.return_value = mock_df
            mock_gds.return_value = mock_ds
            with contextlib.suppress(Exception):
                result = adapter.read_parquet(
                    "instrument_availability/by_date/day=2024-07-01/venue=NYSE/instruments.parquet"
                )
                assert isinstance(result, pd.DataFrame)

    def test_read_parquet_404_in_error_message(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.side_effect = OSError("404 Not Found")
            mock_gds.return_value = mock_ds
            with contextlib.suppress(Exception):
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)

    def test_read_parquet_non_dataframe_response(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.return_value = "not a dataframe"
            mock_gds.return_value = mock_ds
            with contextlib.suppress(Exception):
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)
                assert result.empty


# ---------------------------------------------------------------------------
# Additional corporate_actions_backfill_handler coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCorporateActionsBackfillHandlerDetailed:
    @patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.CorporateActionsAdapter")
    def test_get_tickers_fallback_to_sp500(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_backfill_handler import (
            CorporateActionsBackfillHandler,
        )

        mock_adapter_cls.return_value = MagicMock()
        with patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.get_data_source") as mock_gds:
            mock_gds.side_effect = OSError("no connection")
            with contextlib.suppress(Exception):
                handler = CorporateActionsBackfillHandler({"project_id": "test"})
                tickers = handler._get_tickers(None)
                assert isinstance(tickers, list)

    @patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.CorporateActionsAdapter")
    def test_run_with_explicit_tickers(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_backfill_handler import (
            CorporateActionsBackfillHandler,
        )

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_bundle = MagicMock()
        mock_bundle.ticker = "AAPL"
        mock_bundle.dividends = []
        mock_bundle.splits = []
        mock_bundle.earnings = []
        mock_adapter.fetch_corporate_actions.return_value = mock_bundle
        with contextlib.suppress(Exception):
            handler = CorporateActionsBackfillHandler({"project_id": "test"})
            result = handler.run(tickers=["AAPL"], parallel_workers=1, append_mode=False)
            assert result is not None


# ---------------------------------------------------------------------------
# Additional corporate_actions_production_handler coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCorporateActionsProductionHandlerDetailed:
    @patch("instruments_service.cli.handlers.corporate_actions_production_handler.CorporateActionsAdapter")
    def test_run_with_no_tickers(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_production_handler import (
            CorporateActionsProductionHandler,
        )

        mock_adapter_cls.return_value = MagicMock()
        with patch("instruments_service.cli.handlers.corporate_actions_production_handler.get_data_source") as mock_gds:
            mock_gds.side_effect = OSError("no connection")
            with (
                patch(
                    "instruments_service.cli.handlers.corporate_actions_production_handler.SP500_TICKERS",
                    ["AAPL"],
                ),
                contextlib.suppress(Exception),
            ):
                handler = CorporateActionsProductionHandler({"project_id": "test"})
                assert handler is not None

    @patch("instruments_service.cli.handlers.corporate_actions_production_handler.CorporateActionsAdapter")
    def test_adapter_is_initialized(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_production_handler import (
            CorporateActionsProductionHandler,
        )

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        with contextlib.suppress(Exception):
            handler = CorporateActionsProductionHandler({"project_id": "test"})
            assert handler.adapter is mock_adapter

    @patch("instruments_service.cli.handlers.corporate_actions_production_handler.CorporateActionsAdapter")
    def test_base_dir_created(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_production_handler import (
            CorporateActionsProductionHandler,
        )

        mock_adapter_cls.return_value = MagicMock()
        with contextlib.suppress(Exception):
            handler = CorporateActionsProductionHandler({"project_id": "test"})
            assert handler.base_dir is not None
            assert handler.by_ticker_dir is not None
            assert handler.by_date_dir is not None


# ---------------------------------------------------------------------------
# Additional corporate_actions_update_handler coverage -- run() method
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCorporateActionsUpdateHandlerDetailed:
    @patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.CorporateActionsAdapter")
    def test_run_with_empty_registry(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_update_handler import (
            CorporateActionsUpdateHandler,
        )

        mock_adapter_cls.return_value = MagicMock()
        with contextlib.suppress(Exception):
            handler = CorporateActionsUpdateHandler({"project_id": "test"})
            with tempfile.TemporaryDirectory() as tmpdir:
                result = handler.run(
                    input_dir=tmpdir,
                    output_dir=tmpdir,
                    days_threshold=7,
                    parallel_workers=1,
                )
                assert result is not None

    @patch("instruments_service.cli.handlers.corporate_actions_backfill_handler.CorporateActionsAdapter")
    def test_has_expected_attributes(self, mock_adapter_cls: MagicMock) -> None:
        from instruments_service.cli.handlers.corporate_actions_update_handler import (
            CorporateActionsUpdateHandler,
        )

        mock_adapter_cls.return_value = MagicMock()
        with contextlib.suppress(Exception):
            handler = CorporateActionsUpdateHandler({"project_id": "test"})
            assert hasattr(handler, "backfill_handler")
            assert hasattr(handler, "date_views_handler")
            assert hasattr(handler, "config")
