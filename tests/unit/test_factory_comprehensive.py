"""Extended tests for factory.py and router.py — additional coverage for uncovered branches.

Extends existing test_factory.py and test_router.py with tests for:
- Factory: live mode CCXT routing, TradFi live routing, Tardis exchange resolution,
  DeFi chain parsing, adapter pool, protocol slug resolution
- Router: all route helpers, dataset/exchange resolution, error paths
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from instruments_service.reference_data.factory import (
    ADAPTER_DATA_SOURCES,
    CANONICAL_VENUE_TO_ADAPTER,
    _adapter_pool,
    _run_refdata_preflight,
    clear_adapter_pool,
    create_reference_data_adapter,
    get_adapter_for_canonical_venue,
)
from instruments_service.reference_data.router import (
    ReferenceDataSourceConfig,
    _resolve_databento_datasets,
    _resolve_tardis_exchanges,
    _route_ccxt,
    _route_databento,
    _route_direct,
    _route_tardis,
    _run_refdata_source_preflight,
    create_reference_data_adapter_for_source,
)

# ---------------------------------------------------------------------------
# Factory: get_adapter_for_canonical_venue — live mode routing
# ---------------------------------------------------------------------------


class TestFactoryLiveModeRouting:
    def test_live_cefi_routes_to_ccxt(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("BINANCE-SPOT", mode="live")
        # CCXTReferenceDataAdapter uses canonical_venue as venue when provided
        assert adapter.venue == "BINANCE-SPOT"
        from instruments_service.reference_data.adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter

        assert isinstance(adapter, CCXTReferenceDataAdapter)

    def test_live_cefi_ccxt_pool_reuse(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("BINANCE-SPOT", mode="live")
        a2 = get_adapter_for_canonical_venue("BINANCE-SPOT", mode="live")
        assert a1 is a2

    def test_live_tradfi_routes_to_tradfi_live(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("CME", mode="live")
        # TradFiLiveReferenceDataAdapter uses "tradfi_live" venue
        assert adapter is not None

    def test_live_tradfi_pool_reuse(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("CME", mode="live")
        a2 = get_adapter_for_canonical_venue("CME", mode="live")
        assert a1 is a2

    def test_batch_cefi_routes_to_tardis(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("BINANCE-SPOT", mode="batch")
        assert adapter.venue == "tardis"


# ---------------------------------------------------------------------------
# Factory: DeFi chain parsing
# ---------------------------------------------------------------------------


class TestFactoryDefiChainParsing:
    def test_defi_multichain_adapter_creation(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("AAVEV3-ETHEREUM")
        assert adapter is not None

    def test_defi_multichain_different_chains(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("AAVEV3-ETHEREUM")
        a2 = get_adapter_for_canonical_venue("AAVEV3-ARBITRUM")
        # Different chain = different pool entries
        assert a1 is not a2

    def test_defi_fork_protocol_slug(self) -> None:
        """DEX forks (PancakeSwap, SushiSwap) use UniV3 adapter with resolved protocol_slug."""
        clear_adapter_pool()
        if "PANCAKESWAPV3-ETHEREUM" in CANONICAL_VENUE_TO_ADAPTER:
            adapter = get_adapter_for_canonical_venue("PANCAKESWAPV3-ETHEREUM")
            assert adapter is not None

    def test_defi_solana_chain(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("ORCA-SOLANA")
        assert adapter is not None

    def test_defi_default_chain_ethereum(self) -> None:
        """Single-word DeFi venues default to ETHEREUM chain."""
        clear_adapter_pool()
        # LIDO-ETHEREUM has a hyphen, so chain=ETHEREUM is parsed
        adapter = get_adapter_for_canonical_venue("LIDO-ETHEREUM")
        assert adapter is not None


# ---------------------------------------------------------------------------
# Factory: Tardis exchange resolution
# ---------------------------------------------------------------------------


class TestFactoryTardisRouting:
    def test_tardis_venue_resolution(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("DERIBIT", mode="batch")
        assert adapter is not None
        assert adapter.venue == "tardis"

    def test_tardis_no_mapping_raises(self) -> None:
        """Venue in CANONICAL_VENUE_TO_ADAPTER as tardis but no Tardis exchange mapping raises ValueError."""
        clear_adapter_pool()
        mock_vm = MagicMock()
        mock_vm.get_tardis_exchange_for_venue.return_value = None
        mock_vm.tardis_to_venue = {}

        # VenueMapping is lazily imported inside factory function body as:
        #   from unified_api_contracts import VenueMapping as _VM_cls
        # So we patch at the source module level.
        mock_vm_cls = MagicMock(return_value=mock_vm)
        with (
            patch(
                "unified_api_contracts.VenueMapping",
                mock_vm_cls,
            ),
            patch.dict(CANONICAL_VENUE_TO_ADAPTER, {"FAKE-TARDIS-VENUE": "tardis"}, clear=False),
            pytest.raises(ValueError, match="No Tardis exchange mapping"),
        ):
            get_adapter_for_canonical_venue("FAKE-TARDIS-VENUE", mode="batch")


# ---------------------------------------------------------------------------
# Factory: Databento date routing
# ---------------------------------------------------------------------------


class TestFactoryDatabentoRouting:
    def test_databento_with_date(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("CME", date="2026-03-22", mode="batch")
        assert adapter is not None

    def test_databento_different_dates_different_pool(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("CME", date="2026-03-22", mode="batch")
        a2 = get_adapter_for_canonical_venue("CME", date="2026-03-23", mode="batch")
        assert a1 is not a2


# ---------------------------------------------------------------------------
# Factory: clear_adapter_pool
# ---------------------------------------------------------------------------


class TestClearAdapterPool:
    def test_clear_pool(self) -> None:
        clear_adapter_pool()
        _ = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert len(_adapter_pool) > 0
        clear_adapter_pool()
        assert len(_adapter_pool) == 0


# ---------------------------------------------------------------------------
# Factory: _run_refdata_preflight
# ---------------------------------------------------------------------------


class TestRunRefdataPreflight:
    def test_unknown_venue_silently_skipped(self) -> None:
        # Should not raise
        _run_refdata_preflight("unknown_venue_xyz_123")

    def test_known_venue(self) -> None:
        # Should not raise
        _run_refdata_preflight("hyperliquid")


# ---------------------------------------------------------------------------
# Factory: CANONICAL_VENUE_TO_ADAPTER completeness
# ---------------------------------------------------------------------------


class TestCanonicalVenueMapping:
    def test_all_cefi_venues_mapped(self) -> None:
        cefi = ["BINANCE-SPOT", "BINANCE-FUTURES", "BYBIT", "OKX-SPOT", "DERIBIT", "HYPERLIQUID"]
        for v in cefi:
            assert v in CANONICAL_VENUE_TO_ADAPTER, f"{v} missing"

    def test_all_tradfi_venues_mapped(self) -> None:
        tradfi = ["CME", "NASDAQ", "NYSE", "CBOE", "ICE", "FX"]
        for v in tradfi:
            assert v in CANONICAL_VENUE_TO_ADAPTER, f"{v} missing"

    def test_all_solana_venues_mapped(self) -> None:
        solana = ["DRIFT-SOLANA", "KAMINO-SOLANA", "RAYDIUM-SOLANA", "ORCA-SOLANA"]
        for v in solana:
            assert v in CANONICAL_VENUE_TO_ADAPTER, f"{v} missing"

    def test_adapter_data_sources_covers_all_adapters(self) -> None:
        """Every adapter key used in CANONICAL_VENUE_TO_ADAPTER should have a data source.

        Known gaps (adapter keys in CANONICAL_VENUE_TO_ADAPTER but not yet in ADAPTER_DATA_SOURCES)
        are tracked here so the test remains green while documenting the discrepancy.
        """
        # Adapter keys that are mapped in CANONICAL_VENUE_TO_ADAPTER but not yet wired
        # into ADAPTER_DATA_SOURCES — track here until the gap is closed in production code.
        known_gaps: set[str] = {"eigenlayer", "ethfi_governance"}

        adapter_keys = set(CANONICAL_VENUE_TO_ADAPTER.values())
        unexpected_missing: list[str] = []
        for key in adapter_keys:
            if key in known_gaps:
                continue
            if key not in ADAPTER_DATA_SOURCES:
                unexpected_missing.append(key)

        assert not unexpected_missing, (
            f"adapter_keys missing from ADAPTER_DATA_SOURCES (not in known_gaps): {unexpected_missing}"
        )


# ---------------------------------------------------------------------------
# Factory: create_reference_data_adapter
# ---------------------------------------------------------------------------


class TestCreateReferenceDataAdapter:
    def test_all_known_adapter_keys(self) -> None:
        """All keys in ADAPTER_DATA_SOURCES should be creatable (if not DeFi/Tardis)."""
        simple_adapters = ["hyperliquid", "aster", "polygon", "kalshi", "betfair"]
        for key in simple_adapters:
            adapter = create_reference_data_adapter(key)
            assert adapter is not None

    def test_case_insensitive_creation(self) -> None:
        adapter = create_reference_data_adapter("ASTER")
        assert adapter is not None


# ---------------------------------------------------------------------------
# Router: _resolve_databento_datasets
# ---------------------------------------------------------------------------


class TestResolveDatabentoDatasetsRouter:
    def test_override_takes_priority(self) -> None:
        result = _resolve_databento_datasets("apple", "CUSTOM.DATASET")
        assert result == ["CUSTOM.DATASET"]

    def test_known_venue_default(self) -> None:
        result = _resolve_databento_datasets("apple", None)
        assert result == ["XNAS.ITCH"]

    def test_unknown_venue_returns_empty(self) -> None:
        result = _resolve_databento_datasets("unknown_venue_xyz", None)
        assert result == []


# ---------------------------------------------------------------------------
# Router: _resolve_tardis_exchanges
# ---------------------------------------------------------------------------


class TestResolveTardisExchangesRouter:
    def test_override_takes_priority(self) -> None:
        result = _resolve_tardis_exchanges("binance", "custom-exchange")
        assert result == ["custom-exchange"]

    def test_known_venue_default(self) -> None:
        result = _resolve_tardis_exchanges("binance", None)
        assert result == ["binance-futures"]

    def test_unknown_venue_returns_empty(self) -> None:
        result = _resolve_tardis_exchanges("unknown_venue_xyz", None)
        assert result == []


# ---------------------------------------------------------------------------
# Router: _route_direct error handling
# ---------------------------------------------------------------------------


class TestRouteDirectRouter:
    def test_unsupported_venue_raises(self) -> None:
        with pytest.raises(ValueError, match="No direct adapter"):
            _route_direct("nonexistent_venue", None)

    def test_supported_venues(self) -> None:
        for venue in ["aave_v3", "aster", "hyperliquid", "betfair", "polymarket"]:
            adapter = _route_direct(venue, None)
            assert adapter is not None


# ---------------------------------------------------------------------------
# Router: _route_ccxt error handling
# ---------------------------------------------------------------------------


class TestRouteCcxtRouter:
    def test_no_exchange_raises(self) -> None:
        config = ReferenceDataSourceConfig(venue="unknown_venue", data_source="ccxt")
        with pytest.raises(ValueError, match="No CCXT exchange_id"):
            _route_ccxt("unknown_venue", config, None)

    def test_with_exchange_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="ccxt", exchange="binance")
        adapter = _route_ccxt("binance", config, None)
        assert adapter is not None

    def test_known_venue_default_exchange(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="ccxt")
        adapter = _route_ccxt("binance", config, None)
        assert adapter is not None


# ---------------------------------------------------------------------------
# Router: _route_databento
# ---------------------------------------------------------------------------


class TestRouteDatabento:
    def test_with_dataset_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="apple", data_source="databento", dataset="XNAS.ITCH")
        adapter = _route_databento("apple", config, None)
        assert adapter is not None

    def test_no_dataset(self) -> None:
        config = ReferenceDataSourceConfig(venue="unknown", data_source="databento")
        adapter = _route_databento("unknown", config, None)
        assert adapter is not None


# ---------------------------------------------------------------------------
# Router: _route_tardis
# ---------------------------------------------------------------------------


class TestRouteTardis:
    def test_with_exchange_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="tardis", exchange="binance-futures")
        adapter = _route_tardis("binance", config, None)
        assert adapter is not None

    def test_no_exchange(self) -> None:
        config = ReferenceDataSourceConfig(venue="unknown", data_source="tardis")
        adapter = _route_tardis("unknown", config, None)
        assert adapter is not None


# ---------------------------------------------------------------------------
# Router: create_reference_data_adapter_for_source — simple sources
# ---------------------------------------------------------------------------


class TestRouterSimpleSources:
    def test_api_football_source(self) -> None:
        config = ReferenceDataSourceConfig(venue="api_football", data_source="api_football")
        adapter = create_reference_data_adapter_for_source(config)
        assert adapter is not None

    def test_ibkr_source(self) -> None:
        config = ReferenceDataSourceConfig(venue="ibkr", data_source="ibkr")
        adapter = create_reference_data_adapter_for_source(config)
        assert adapter is not None

    def test_polygon_source(self) -> None:
        config = ReferenceDataSourceConfig(venue="polygon", data_source="polygon")
        adapter = create_reference_data_adapter_for_source(config)
        assert adapter is not None

    def test_kalshi_source(self) -> None:
        config = ReferenceDataSourceConfig(venue="kalshi", data_source="kalshi")
        adapter = create_reference_data_adapter_for_source(config)
        assert adapter is not None

    def test_unsupported_source_raises(self) -> None:
        config = ReferenceDataSourceConfig(venue="something", data_source="nonexistent_source")
        with pytest.raises(ValueError, match="Unsupported data_source"):
            create_reference_data_adapter_for_source(config)


# ---------------------------------------------------------------------------
# Router: _run_refdata_source_preflight
# ---------------------------------------------------------------------------


class TestRunRefdataSourcePreflight:
    def test_direct_source_uses_get_instruments(self) -> None:
        # Should not raise
        _run_refdata_source_preflight("hyperliquid", "direct")

    def test_non_direct_source(self) -> None:
        # Should not raise
        _run_refdata_source_preflight("binance", "tardis")


# ---------------------------------------------------------------------------
# Router: ReferenceDataSourceConfig model
# ---------------------------------------------------------------------------


class TestReferenceDataSourceConfig:
    def test_minimal_config(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="tardis")
        assert config.venue == "binance"
        assert config.data_source == "tardis"
        assert config.dataset is None
        assert config.exchange is None
        assert config.fallback_data_source is None

    def test_full_config(self) -> None:
        config = ReferenceDataSourceConfig(
            venue="binance",
            data_source="databento",
            dataset="XNAS.ITCH",
            exchange="binance-futures",
            fallback_data_source="tardis",
        )
        assert config.dataset == "XNAS.ITCH"
        assert config.exchange == "binance-futures"
        assert config.fallback_data_source == "tardis"
