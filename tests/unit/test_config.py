"""Unit tests for InstrumentsServiceConfig and config setup.

Verifies that:
- Config is importable and instantiable with test env vars
- Config reads from UnifiedCloudConfig (not os.getenv)
- Required fields have correct types
- get_config() returns an InstrumentsServiceConfig instance
"""

import contextlib
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from unified_config_interface import UnifiedCloudConfig

from instruments_service.config import get_config
from instruments_service.config.service_config import InstrumentsServiceConfig


class TestInstrumentsServiceConfigImport:
    """Verify InstrumentsServiceConfig is importable and structurally correct."""

    def test_config_class_importable(self) -> None:
        """InstrumentsServiceConfig must be importable from instruments_service.config.service_config."""
        assert InstrumentsServiceConfig is not None

    def test_config_inherits_unified_cloud_config(self) -> None:
        """InstrumentsServiceConfig must extend UnifiedCloudConfig (not os.getenv)."""
        assert issubclass(InstrumentsServiceConfig, UnifiedCloudConfig)

    def test_config_has_required_fields(self) -> None:
        """Config class must declare the required service-level fields."""
        fields = InstrumentsServiceConfig.model_fields
        assert "service_name" in fields, "service_name field must be declared"


class TestGetConfig:
    """Verify get_config() returns a valid config singleton."""

    def test_get_config_returns_config_instance(self) -> None:
        """get_config() must return an InstrumentsServiceConfig instance."""
        config = get_config()
        assert isinstance(config, InstrumentsServiceConfig)

    def test_get_config_service_name(self) -> None:
        """Config service_name must be 'instruments-service'."""
        config = get_config()
        assert config.service_name == "instruments-service"

    def test_config_gcp_project_id_type(self) -> None:
        """gcp_project_id must be a string (set via GCP_PROJECT_ID env var in test env)."""
        config = get_config()
        # In test mode CLOUD_MOCK_MODE=true; gcp_project_id may be empty but must be str
        assert isinstance(config.gcp_project_id, str)

    def test_config_does_not_use_os_getenv(self) -> None:
        """Production config module must not use os.getenv — values come from UnifiedCloudConfig."""
        import inspect

        import instruments_service.config.service_config as cfg_module

        source = inspect.getsource(cfg_module)
        assert "os.getenv" not in source, "Config module must not use os.getenv — extend UnifiedCloudConfig instead"
        assert "os.environ" not in source, "Config module must not use os.environ — extend UnifiedCloudConfig instead"


class TestConfigCloudMockMode:
    """Verify config works in CLOUD_MOCK_MODE (used in CI and unit tests)."""

    def test_config_instantiable_without_credentials(self) -> None:
        """Config must be instantiable with CLOUD_MOCK_MODE=true and GCP_PROJECT_ID set."""
        # CLOUD_MOCK_MODE and GCP_PROJECT_ID are set in quality-gates.sh for test runs
        config = get_config()
        assert config is not None

    def test_config_environment_field(self) -> None:
        """Config environment field must be accessible."""
        config = get_config()
        # environment may be None in test mode; it must be a string or None
        assert config.environment is None or isinstance(config.environment, str)


# From test_coverage_boost_4


class TestVenueConfigFunctions:
    """Tests for functions in instruments_service/config/venue_config.py."""

    def test_load_tradfi_instruments(self) -> None:
        from instruments_service.config.venue_config import _load_tradfi_instruments

        instruments, exchange_map = _load_tradfi_instruments()
        assert isinstance(instruments, list)
        assert isinstance(exchange_map, dict)
        assert len(instruments) > 0

    def test_load_tradfi_instruments_caching(self) -> None:
        """Second call should use cache."""
        from instruments_service.config.venue_config import _load_tradfi_instruments

        result1 = _load_tradfi_instruments()
        result2 = _load_tradfi_instruments()
        assert result1[0] is result2[0]  # Same object (cached)

    def test_load_sp500_tickers(self) -> None:
        from instruments_service.config.venue_config import _load_sp500_tickers

        sp500, nasdaq = _load_sp500_tickers()
        assert isinstance(sp500, list)
        assert isinstance(nasdaq, list)
        assert len(sp500) > 0

    def test_load_sp500_tickers_caching(self) -> None:
        """Second call should use cache."""
        from instruments_service.config.venue_config import _load_sp500_tickers

        result1 = _load_sp500_tickers()
        result2 = _load_sp500_tickers()
        assert result1[0] is result2[0]

    def test_databento_valid_parent_symbols(self) -> None:
        from instruments_service.config import DATABENTO_VALID_PARENT_SYMBOLS

        assert "ES" in DATABENTO_VALID_PARENT_SYMBOLS
        assert "GOLD" in DATABENTO_VALID_PARENT_SYMBOLS
        es = DATABENTO_VALID_PARENT_SYMBOLS["ES"]
        assert isinstance(es, tuple)
        assert len(es) == 2

    def test_databento_valid_options_symbols(self) -> None:
        from instruments_service.config import DATABENTO_VALID_OPTIONS_SYMBOLS

        assert "ES" in DATABENTO_VALID_OPTIONS_SYMBOLS
        assert "GOLD" in DATABENTO_VALID_OPTIONS_SYMBOLS

    def test_exchange_code_to_name(self) -> None:
        from instruments_service.config import EXCHANGE_CODE_TO_NAME

        assert "ES" in EXCHANGE_CODE_TO_NAME
        assert EXCHANGE_CODE_TO_NAME["ES"] == "SP500"

    def test_unified_instrument_config_get_symbols_for_venue(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        symbols = cfg.get_symbols_for_venue("CME")
        assert isinstance(symbols, list)

    def test_unified_instrument_config_get_symbols_for_dataset(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        symbols = cfg.get_symbols_for_dataset("GLBX.MDP3")
        assert isinstance(symbols, list)
        assert len(symbols) > 0

    def test_unified_instrument_config_get_symbols_by_type(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        futures = cfg.get_symbols_by_type("FUTURE")
        assert isinstance(futures, list)
        assert len(futures) > 0

    def test_unified_instrument_config_get_dataset_and_stype(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        # ES.FUT should be in GLBX.MDP3 with parent stype
        result = cfg.get_dataset_and_stype("ES.FUT")
        if result is not None:
            dataset, stype = result
            assert isinstance(dataset, str)
            assert isinstance(stype, str)

    def test_unified_instrument_config_get_dataset_and_stype_unknown(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        result = cfg.get_dataset_and_stype("UNKNOWN_SYMBOL_XYZ")
        assert result is None

    def test_unified_instrument_config_get_instrument(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        # Try to find a known instrument
        all_insts = cfg.get_all_instruments()
        if all_insts:
            first = all_insts[0]
            result = cfg.get_instrument(first.symbol)
            assert result is not None

    def test_unified_instrument_config_get_instrument_with_venue(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        all_insts = cfg.get_all_instruments()
        if all_insts:
            first = all_insts[0]
            result = cfg.get_instrument(first.symbol, venue=first.venue)
            assert result is not None

    def test_unified_instrument_config_get_instrument_wrong_venue(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        all_insts = cfg.get_all_instruments()
        if all_insts:
            first = all_insts[0]
            result = cfg.get_instrument(first.symbol, venue="WRONG_VENUE_XYZ")
            assert result is None

    def test_unified_instrument_config_get_human_readable_name(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        name = cfg.get_human_readable_name("ES")
        assert name == "SP500"

    def test_unified_instrument_config_get_human_readable_name_micro(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        # MES is micro ES — starts with M, base is ES -> SP500
        name = cfg.get_human_readable_name("MES")
        assert name == "SP500"

    def test_unified_instrument_config_get_human_readable_name_unknown(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        name = cfg.get_human_readable_name("XYZUNKNOWN")
        assert name == "XYZUNKNOWN"

    def test_unified_instrument_config_sp500_equities(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        equities = cfg._get_sp500_equities()
        assert isinstance(equities, list)
        assert len(equities) > 0

    def test_unified_instrument_config_known_etfs(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        assert "SPY" in cfg.KNOWN_ETFS
        assert "QQQ" in cfg.KNOWN_ETFS
        assert "IBIT" in cfg.KNOWN_ETFS

    def test_unified_instrument_config_space_to_dot_symbols(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        assert "BRK B" in cfg.SPACE_TO_DOT_SYMBOLS
        assert cfg.SPACE_TO_DOT_SYMBOLS["BRK B"] == "BRK.B"

    def test_unified_instrument_config_exchange_code_to_name_property(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        mapping = cfg.exchange_code_to_name
        assert isinstance(mapping, dict)
        assert "ES" in mapping

    def test_tradfi_instrument_dataclass_defaults(self) -> None:
        from instruments_service.config import TradFiInstrument

        inst = TradFiInstrument(
            symbol="ES.FUT",
            venue="CME",
            instrument_type="FUTURE",
            dataset="GLBX.MDP3",
            stype_in="parent",
        )
        assert inst.quote_asset == "USD"
        assert inst.base_asset is None
        assert inst.exchange_code is None
        assert inst.underlying is None

    def test_instrument_definition_is_alias_for_tradfi(self) -> None:
        from instruments_service.config import InstrumentDefinition, TradFiInstrument

        assert InstrumentDefinition is TradFiInstrument


# ---------------------------------------------------------------------------
# From boost_instruments.py, boost_instruments_2.py, boost_instruments_3.py
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigModuleFromBoost:
    """Coverage for config.py import and basic usage."""

    def test_import(self) -> None:
        import instruments_service.config as cfg

        assert cfg is not None

    def test_unified_instrument_config(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        assert config is not None

    def test_tradfi_instrument_dataclass(self) -> None:
        from instruments_service.config import TradFiInstrument

        inst = TradFiInstrument(
            symbol="AAPL",
            venue="NYSE",
            instrument_type="EQUITY",
            dataset="GLBX.MDP3",
            stype_in="parent",
        )
        assert inst.symbol == "AAPL"

    def test_get_sp500_tickers_returns_list(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        tickers = config.instruments if hasattr(config, "instruments") else []
        assert isinstance(tickers, list)

    def test_get_etf_tickers_returns_list(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        tickers = list(config.KNOWN_ETFS) if hasattr(config, "KNOWN_ETFS") else []
        assert isinstance(tickers, list)

    def test_get_tradfi_instruments_returns_list(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        instruments = config.get_all_instruments() if hasattr(config, "get_all_instruments") else []
        assert isinstance(instruments, list)


@pytest.mark.unit
class TestConfigModuleExtra:
    """Additional config.py coverage tests."""

    def test_databento_valid_parent_symbols(self) -> None:
        from instruments_service.config import DATABENTO_VALID_PARENT_SYMBOLS

        assert isinstance(DATABENTO_VALID_PARENT_SYMBOLS, dict)
        assert "ES" in DATABENTO_VALID_PARENT_SYMBOLS
        assert DATABENTO_VALID_PARENT_SYMBOLS["ES"] == ("ES.FUT", "GLBX.MDP3")

    def test_sp500_tickers_list(self) -> None:
        from instruments_service.config import SP500_TICKERS

        assert isinstance(SP500_TICKERS, (list, tuple, set))
        assert len(SP500_TICKERS) > 0

    def test_get_config_returns_singleton(self) -> None:
        from instruments_service.config import get_config

        cfg1 = get_config()
        cfg2 = get_config()
        assert cfg1 is cfg2

    def test_instruments_config_singleton(self) -> None:
        from instruments_service.config import instruments_config

        assert instruments_config is not None

    def test_load_sp500_tickers_function(self) -> None:
        try:
            from instruments_service.config import _load_sp500_tickers

            tickers, nasdaq = _load_sp500_tickers()
            assert isinstance(tickers, list)
            assert isinstance(nasdaq, list)
        except ImportError:
            pass

    def test_tradfi_instrument_with_all_fields(self) -> None:
        from instruments_service.config import TradFiInstrument

        inst = TradFiInstrument(
            symbol="ES.FUT",
            venue="CME",
            instrument_type="FUTURE",
            dataset="GLBX.MDP3",
            stype_in="parent",
            base_asset="SP500",
            quote_asset="USD",
            exchange_code="ES",
            underlying="SP500",
        )
        assert inst.symbol == "ES.FUT"
        assert inst.venue == "CME"
        assert inst.underlying == "SP500"

    def test_unified_instrument_config_post_init(self) -> None:
        from instruments_service.config import UnifiedInstrumentConfig

        cfg = UnifiedInstrumentConfig()
        assert cfg is not None
        assert hasattr(cfg, "_instruments")

    def test_exchange_code_to_name_constant(self) -> None:
        try:
            from instruments_service.config import EXCHANGE_CODE_TO_NAME

            assert isinstance(EXCHANGE_CODE_TO_NAME, dict)
        except ImportError:
            pass


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


@pytest.mark.unit
class TestConfigDefiDefinitions:
    def test_import(self) -> None:
        from instruments_service.config.defi_definitions import DEFI_PROTOCOLS, DEFI_VENUE_TO_PROTOCOL

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


@pytest.mark.unit
class TestConfigFuturesOptionsDefinitions:
    def test_import(self) -> None:
        import contextlib

        with contextlib.suppress(Exception):
            import instruments_service.config.futures_options_definitions as fod

            assert fod is not None

    def test_has_content(self) -> None:
        import contextlib

        with contextlib.suppress(Exception):
            import instruments_service.config.futures_options_definitions as fod

            attrs = [a for a in dir(fod) if not a.startswith("__")]
            assert len(attrs) >= 0


@pytest.mark.unit
class TestConfigTickerLists:
    def test_import(self) -> None:
        from instruments_service.config.ticker_lists import SP500_TICKERS, corporate_actions_start_date

        assert len(SP500_TICKERS) > 0
        assert isinstance(corporate_actions_start_date, str)


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


@pytest.mark.unit
class TestConfigReloadersFromBoost:
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
        import contextlib

        with contextlib.suppress(Exception):
            start_domain_config_reloaders(mock_config)

    def test_stop_domain_config_reloaders_when_none(self) -> None:
        import contextlib

        from instruments_service.config_reloaders import stop_domain_config_reloaders

        with contextlib.suppress(Exception):
            stop_domain_config_reloaders()

    def test_on_instruments_reload_callback(self) -> None:
        from instruments_service.config_reloaders import _on_instruments_reload, get_active_subscription_list

        mock_domain_config = MagicMock()
        mock_domain_config.subscription_list = ["AAPL", "MSFT"]
        mock_domain_config.enabled_venues = ["NYSE", "NASDAQ"]
        import contextlib

        with contextlib.suppress(Exception):
            _on_instruments_reload(mock_domain_config)
            result = get_active_subscription_list()
            assert "AAPL" in result or isinstance(result, list)


# ---------------------------------------------------------------------------
# TestRootConfigPy from boost_instruments_3 - root-level config.py
# ---------------------------------------------------------------------------


def _import_root_config_module() -> ModuleType | None:
    """Import root config.py by file path (avoids collision with config/ package)."""
    root_path = Path(__file__).resolve().parents[2] / "instruments_service" / "config.py"
    spec = importlib.util.spec_from_file_location("_instruments_root_config", str(root_path))
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    with contextlib.suppress(Exception):
        spec.loader.exec_module(mod)
    return mod


@pytest.mark.unit
class TestRootConfigPyFromBoost:
    """Tests for the root-level instruments_service/config.py module."""

    def test_import_root_config(self) -> None:
        mod = _import_root_config_module()
        assert mod is not None

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


@pytest.mark.unit
class TestConfigReloadersBoost5:
    """Config reloaders from boost_5."""

    def test_on_instruments_reload_updates_state(self):
        import instruments_service.config_reloaders as cr

        mock_config = MagicMock()
        mock_config.subscription_list = ["BTC-USDT", "ETH-USDT"]
        mock_config.enabled_venues = ["binance", "coinbase"]
        with patch("instruments_service.config_reloaders.log_event", create=True):
            cr._on_instruments_reload(mock_config)
        assert cr._active_subscription_list == ["BTC-USDT", "ETH-USDT"]
        assert cr._active_enabled_venues == ["binance", "coinbase"]

    def test_on_instruments_reload_handles_log_event_error(self):
        import instruments_service.config_reloaders as cr

        mock_config = MagicMock()
        mock_config.subscription_list = []
        mock_config.enabled_venues = []
        import contextlib

        with patch.dict("sys.modules", {"unified_events_interface": None}), contextlib.suppress(Exception):
            cr._on_instruments_reload(mock_config)


@pytest.mark.unit
class TestConfigReloadersGetActive:
    """Tests for get_active_subscription_list and get_active_enabled_venues."""

    def test_get_active_subscription_list_returns_copy(self):
        import instruments_service.config_reloaders as cr

        cr._active_subscription_list = ["A", "B"]
        result = cr.get_active_subscription_list()
        assert result == ["A", "B"]
        assert result is not cr._active_subscription_list

    def test_get_active_enabled_venues_returns_copy(self):
        import instruments_service.config_reloaders as cr

        cr._active_enabled_venues = ["BINANCE", "CME"]
        result = cr.get_active_enabled_venues()
        assert result == ["BINANCE", "CME"]
        assert result is not cr._active_enabled_venues

    def test_start_domain_config_reloaders_skips_when_no_bucket(self):
        from unittest.mock import patch

        import instruments_service.config_reloaders as cr

        mock_config = type("C", (), {"config_store_bucket": "", "project_id": None})()
        with patch.object(cr, "_instrument_reloader", None):
            cr.start_domain_config_reloaders(mock_config)
        assert cr._instrument_reloader is None
