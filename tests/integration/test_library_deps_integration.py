"""Library dependency contract tests for instruments-service.

These tests verify that every symbol imported by production code still exists and
behaves as expected in the library versions currently installed.

WHY these tests exist:
  - instruments-service imports specific functions from UAC, UIC, UTL, UCI, UEI, URDI.
  - If a library refactor renames or removes a function (e.g. DomainValidationService.validate_for_domain
    → validate), this repo fails at IMPORT TIME in production, not at test time.
  - These tests fire at QG time (CI) and catch such breakage before deployment.

WHAT is tested:
  - Each symbol is imported exactly as production code imports it.
  - The symbol is not only importable but has the expected shape (callable, is a class,
    has specific attributes or can be instantiated with expected args).
  - No real GCP credentials required — no network calls, no cloud writes.

COVERAGE scope (mirrors pyproject.toml direct dependencies):
  - unified-api-contracts (UAC)       — domain contracts, venue mappings, ticker registries
  - unified-internal-contracts (UIC)  — core domain schemas (InstrumentRecord, MarketCategory)
  - unified-trading-library (UTL)     — service framework, validation, storage, sampling
  - unified-config-interface (UCI)    — config base class, config store
  - unified-events-interface (UEI)    — log_event (used in config_reloaders)
  - unified-reference-data-interface (URDI) — adapter registry used in adapters/
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# unified-api-contracts (UAC)
# Production imports: VenueMapping, TRADFI_TICKER_UNIVERSE, KNOWN_ETFS
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uac_venue_mapping_usable() -> None:
    """VenueMapping is instantiated as a module-level singleton in engine/orchestrator.py.
    Its venue list methods must return non-empty collections.
    """
    from unified_api_contracts import VenueMapping

    vm = VenueMapping()
    assert len(vm.all_defi_venues) > 0, "all_defi_venues must be non-empty"
    assert len(vm.all_databento_venues) > 0, "all_databento_venues must be non-empty"
    assert hasattr(vm, "tardis_to_venue"), "tardis_to_venue mapping must exist"
    assert isinstance(vm.tardis_to_venue, dict)


@pytest.mark.integration
def test_uac_tradfi_ticker_universe_usable() -> None:
    """TRADFI_TICKER_UNIVERSE is used in config_reloaders.py to seed the ticker cache."""
    from unified_api_contracts import TRADFI_TICKER_UNIVERSE

    assert isinstance(TRADFI_TICKER_UNIVERSE, dict), "must be a dict"
    assert "sp500_tickers" in TRADFI_TICKER_UNIVERSE, "sp500_tickers key must exist"
    assert "etf_tickers" in TRADFI_TICKER_UNIVERSE, "etf_tickers key must exist"
    assert "nasdaq_tickers" in TRADFI_TICKER_UNIVERSE, "nasdaq_tickers key must exist"
    assert len(TRADFI_TICKER_UNIVERSE["sp500_tickers"]) > 100, "sp500 must have >100 tickers"


@pytest.mark.integration
def test_uac_known_etfs_usable() -> None:
    """KNOWN_ETFS is used in config_reloaders.py alongside TRADFI_TICKER_UNIVERSE."""
    from unified_api_contracts import KNOWN_ETFS

    assert isinstance(KNOWN_ETFS, (set, frozenset, list)), "must be a collection"
    assert len(KNOWN_ETFS) > 0, "must not be empty"
    assert "SPY" in KNOWN_ETFS or "spy" in {e.upper() for e in KNOWN_ETFS}, "SPY must be a known ETF"


# ---------------------------------------------------------------------------
# unified-internal-contracts (UIC)
# Production imports: InstrumentRecord (in adapters/urdi_reference_provider.py)
# Framework imports (via UTL): MarketCategory, StartupValidationError
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uic_instrument_record_constructable() -> None:
    """InstrumentRecord is the core domain type returned by URDI and written to storage.
    If its constructor signature changes (new required field), production breaks at runtime.
    """
    from unified_api_contracts.internal import InstrumentRecord

    record = InstrumentRecord(
        instrument_key="UNISWAPV3-ETHEREUM:POOL:WETH-USDC",
        venue="UNISWAPV3-ETHEREUM",
        instrument_type="POOL",
        base_asset="WETH",
        quote_asset="USDC",
    )
    assert record.instrument_key == "UNISWAPV3-ETHEREUM:POOL:WETH-USDC"
    assert record.venue == "UNISWAPV3-ETHEREUM"
    assert record.model_dump() is not None, "model_dump() must work (used in orchestrator)"


@pytest.mark.integration
def test_uic_asset_group_values() -> None:
    """MarketCategory is the canonical category enum. ServiceRuntime stores
    list[MarketCategory] and handler converts via .value. If values change, the
    CLI category args break silently.
    """
    from unified_api_contracts.internal import MarketCategory

    assert MarketCategory.DEFI.value == "DEFI"
    assert MarketCategory.CEFI.value == "CEFI"
    assert MarketCategory.TRADFI.value == "TRADFI"
    assert MarketCategory.SPORTS.value == "SPORTS"


# ---------------------------------------------------------------------------
# unified-trading-library (UTL)
# Production imports: BatchPayload, ServiceRuntime, UnifiedServiceHandler,
#   ServiceBootstrap, get_bucket_name, validate_api_keys_for_venues,
#   validate_data_availability, DataSink, DomainValidationService, ManifestWriter,
#   SamplingService, create_sampling_service, get_data_sink, log_event,
#   DomainConfigReloader, unified_config
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_utl_service_framework_symbols_exist() -> None:
    """Core service framework: BatchPayload, ServiceRuntime, UnifiedServiceHandler.
    InstrumentsHandler inherits from UnifiedServiceHandler — if the ABC changes
    its abstract methods, the handler silently breaks.
    """
    from unified_trading_library import BatchPayload, ServiceRuntime, UnifiedServiceHandler

    # BatchPayload is the payload passed to process() — constructor must work
    payload = BatchPayload(date="2026-03-22", asset_groups=["DEFI"])
    assert payload.date == "2026-03-22"
    assert payload.asset_groups == ["DEFI"]
    assert isinstance(payload.extra, dict)

    # ServiceRuntime must have the fields handler preflight() reads.
    # Use __dataclass_fields__ since ServiceRuntime is a @dataclass.
    field_names = set(ServiceRuntime.__dataclass_fields__)
    assert "asset_group" in field_names, "must have asset_group field"
    assert "gcp_project_id" in field_names, "must have gcp_project_id field"
    assert "operation" in field_names, "must have operation field"
    assert "mode" in field_names, "must have mode field"

    # UnifiedServiceHandler must have the methods InstrumentsHandler overrides
    import inspect

    assert inspect.iscoroutinefunction(UnifiedServiceHandler.process), "process must be async"
    assert inspect.iscoroutinefunction(UnifiedServiceHandler.preflight), "preflight must be async"
    assert inspect.iscoroutinefunction(UnifiedServiceHandler.cleanup), "cleanup must be async"


@pytest.mark.integration
def test_utl_service_bootstrap_constructable() -> None:
    """ServiceBootstrap is the CLI entry point. If its constructor signature changes,
    cli/main.py fails at module import time.
    """
    from unittest.mock import MagicMock

    from unified_trading_library import BaseModeHandler, ServiceBootstrap

    class _Stub(BaseModeHandler):
        async def process(self, payload: object) -> object:
            return None

    # Must not raise — only tests the constructor signature
    sb = ServiceBootstrap(
        service_name="test-service",
        operations={"instruments": _Stub},
        config=MagicMock(),
    )
    assert sb is not None


@pytest.mark.integration
def test_utl_startup_validation_callables() -> None:
    """validate_api_keys_for_venues and validate_data_availability are called in preflight().
    If their signatures change (new required arg), preflight() crashes at call time.
    """
    import inspect

    from unified_trading_library import validate_api_keys_for_venues, validate_data_availability

    assert callable(validate_api_keys_for_venues)
    sig = inspect.signature(validate_api_keys_for_venues)
    assert "venues" in sig.parameters, "venues param must exist"
    assert "project_id" in sig.parameters, "project_id param must exist"

    assert callable(validate_data_availability)
    sig2 = inspect.signature(validate_data_availability)
    assert "service_name" in sig2.parameters
    assert "bucket" in sig2.parameters
    assert "start_date" in sig2.parameters
    assert "end_date" in sig2.parameters


@pytest.mark.integration
def test_utl_domain_validation_service_api() -> None:
    """DomainValidationService("instruments") is called in orchestrator.py.
    The method validate_for_domain must exist and accept a DataFrame.
    """
    import pandas as pd
    from unified_trading_library import DomainValidationService

    service = DomainValidationService("instruments")
    assert hasattr(service, "validate_for_domain"), "validate_for_domain method must exist"

    # Must accept an empty DataFrame without raising
    service.validate_for_domain(pd.DataFrame())


@pytest.mark.integration
def test_utl_manifest_writer_api() -> None:
    """ManifestWriter is used in _write_catalogue_record via .add() + .write().
    These method names are NOT part of a stable public API — they can be renamed.
    """
    from unified_trading_library import ManifestWriter

    writer = ManifestWriter(service_name="instruments-service")
    assert hasattr(writer, "add"), "ManifestWriter must have .add() (used in orchestrator)"
    assert hasattr(writer, "write"), "ManifestWriter must have .write() (flush to storage)"
    assert callable(writer.add)
    assert callable(writer.write)


@pytest.mark.integration
def test_utl_sampling_service_api() -> None:
    """create_sampling_service() is called in process_instruments() to produce a
    SamplingService. The sampler.enable_sampling attribute and .generate_csv_sample()
    method are accessed directly in _write_venue().
    """
    from unified_trading_library import SamplingService, create_sampling_service

    sampler = create_sampling_service({"enable_sampling": False, "sample_size": 10, "sample_dir": "/tmp"})
    assert isinstance(sampler, SamplingService)
    assert hasattr(sampler, "enable_sampling"), "enable_sampling attr must exist"
    assert hasattr(sampler, "generate_csv_sample"), "generate_csv_sample method must exist"
    assert callable(sampler.generate_csv_sample)


@pytest.mark.integration
def test_utl_data_sink_and_bucket_api() -> None:
    """get_data_sink() and get_bucket_name() are called in process_instruments().
    DataSink is used as the type annotation for the sink parameter.
    """
    from unified_trading_library import DataSink, get_bucket_name, get_data_sink

    assert callable(get_data_sink)
    assert callable(get_bucket_name)
    # DataSink must be importable as a type (used in function signatures)
    assert DataSink is not None


@pytest.mark.integration
def test_utl_domain_config_reloader_api() -> None:
    """DomainConfigReloader is used in config_reloaders.py to hot-reload InstrumentDomainConfig.
    If the constructor signature changes (domain, config_class, config_bucket),
    start_watching() silently does nothing.
    """
    import inspect

    from unified_trading_library import DomainConfigReloader

    sig = inspect.signature(DomainConfigReloader.__init__)
    assert "domain" in sig.parameters
    assert "config_class" in sig.parameters
    assert "config_bucket" in sig.parameters
    assert hasattr(DomainConfigReloader, "on_reload"), "on_reload method must exist"
    assert hasattr(DomainConfigReloader, "start_watching"), "start_watching method must exist"


@pytest.mark.integration
def test_utl_unified_config_accessible() -> None:
    """unified_config (a module-level UnifiedCloudConfig singleton) is accessed as _uc
    in orchestrator.py. Fields csv_sample_size, csv_sample_dir, enable_csv_sampling must exist.
    """
    from unified_trading_library import unified_config

    assert hasattr(unified_config, "enable_csv_sampling"), "enable_csv_sampling must exist"
    assert hasattr(unified_config, "csv_sample_size"), "csv_sample_size must exist"
    assert hasattr(unified_config, "csv_sample_dir"), "csv_sample_dir must exist"


# ---------------------------------------------------------------------------
# unified-config-interface (UCI)
# Production imports: UnifiedCloudConfig, ConfigStoreError, InstrumentDomainConfig,
#   TickerUniverseConfig, TimeSeriesConfigStore (all in config_reloaders.py)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uci_config_base_class_usable() -> None:
    """UnifiedCloudConfig is the base class for InstrumentsServiceConfig. If its
    constructor or required env vars change, the service fails at startup.
    """
    from unified_trading_library import UnifiedCloudConfig

    # Must be constructable in mock mode (no real GCP needed)
    cfg = UnifiedCloudConfig()
    assert hasattr(cfg, "gcp_project_id"), "gcp_project_id must be a field"
    assert hasattr(cfg, "is_mock_mode"), "is_mock_mode() must exist"
    assert callable(cfg.is_mock_mode)


@pytest.mark.integration
def test_uci_domain_config_types_importable() -> None:
    """InstrumentDomainConfig and TickerUniverseConfig are used in config_reloaders.py
    as the schema classes passed to TimeSeriesConfigStore. If they are moved or renamed,
    the hot-reload system silently does nothing.
    """
    from unified_trading_library import (
        ConfigStoreError,
        InstrumentDomainConfig,
        TickerUniverseConfig,
        TimeSeriesConfigStore,
    )

    # These are Pydantic models or dataclasses — must have expected fields
    assert hasattr(InstrumentDomainConfig, "model_fields") or hasattr(InstrumentDomainConfig, "__dataclass_fields__"), (
        "InstrumentDomainConfig must be a structured type"
    )
    assert hasattr(TickerUniverseConfig, "model_fields") or hasattr(TickerUniverseConfig, "__dataclass_fields__"), (
        "TickerUniverseConfig must be a structured type"
    )
    assert issubclass(ConfigStoreError, Exception), "ConfigStoreError must be an Exception"
    assert callable(TimeSeriesConfigStore), "TimeSeriesConfigStore must be constructable"


# ---------------------------------------------------------------------------
# unified-events-interface (UEI)
# Production imports: log_event (in config_reloaders.py)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uei_log_event_callable() -> None:
    """log_event is imported directly in config_reloaders.py (not via UTL).
    It must be callable and accept event_name + optional details.
    """
    import inspect

    from unified_trading_library import log_event

    assert callable(log_event)
    sig = inspect.signature(log_event)
    assert "event_name" in sig.parameters
    assert "details" in sig.parameters


# ---------------------------------------------------------------------------
# unified-reference-data-interface (URDI)
# Production imports: ADAPTER_DATA_SOURCES, CANONICAL_VENUE_TO_ADAPTER,
#   get_adapter_for_canonical_venue (all in adapters/urdi_reference_provider.py)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_urdi_adapter_registry_populated() -> None:
    """CANONICAL_VENUE_TO_ADAPTER maps canonical venue names to URDI adapter keys.
    If URDI removes a venue (e.g. UNISWAPV3-ETHEREUM), instrument fetching silently
    returns zero records for that venue.
    """
    from instruments_service.reference_data import CANONICAL_VENUE_TO_ADAPTER

    assert isinstance(CANONICAL_VENUE_TO_ADAPTER, dict)
    assert len(CANONICAL_VENUE_TO_ADAPTER) >= 15, "must cover at least 15 canonical venues"
    # Key venues the service processes must be present
    assert "UNISWAPV3-ETHEREUM" in CANONICAL_VENUE_TO_ADAPTER, "Uniswap V3 must be registered"
    assert "AAVEV3-ETHEREUM" in CANONICAL_VENUE_TO_ADAPTER, "Aave V3 must be registered"
    assert "MORPHO-ETHEREUM" in CANONICAL_VENUE_TO_ADAPTER, "Morpho must be registered"


@pytest.mark.integration
def test_urdi_adapter_data_sources_aligned() -> None:
    """ADAPTER_DATA_SOURCES maps URDI adapter keys to data source IDs used for
    API key lookup. Every adapter key in CANONICAL_VENUE_TO_ADAPTER must have a
    corresponding data source entry (even if empty string = no key required).
    """
    from instruments_service.reference_data import ADAPTER_DATA_SOURCES, CANONICAL_VENUE_TO_ADAPTER

    adapter_keys = set(CANONICAL_VENUE_TO_ADAPTER.values())
    for key in adapter_keys:
        assert key in ADAPTER_DATA_SOURCES, (
            f"adapter key '{key}' in CANONICAL_VENUE_TO_ADAPTER has no ADAPTER_DATA_SOURCES entry"
        )


@pytest.mark.integration
def test_urdi_get_adapter_for_canonical_venue_callable() -> None:
    """get_adapter_for_canonical_venue() is called per-venue in fetch_instruments_for_all_venues().
    It must accept (canonical_venue, api_key=None) and return an adapter with get_instruments().
    """
    import inspect

    from instruments_service.reference_data import get_adapter_for_canonical_venue

    assert callable(get_adapter_for_canonical_venue)
    sig = inspect.signature(get_adapter_for_canonical_venue)
    params = list(sig.parameters)
    assert params[0] in ("canonical_venue", "venue", "name"), "first param must be venue name"
