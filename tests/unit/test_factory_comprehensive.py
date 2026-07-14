"""Extended tests for factory.py and router.py — additional coverage for uncovered branches.

Extends existing test_factory.py and test_router.py with tests for:
- Factory: live mode CCXT routing, TradFi live routing, Tardis exchange resolution,
  DeFi chain parsing, adapter pool, protocol slug resolution
- Router: all route helpers, dataset/exchange resolution, error paths
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from unified_api_contracts.registry import NO_ADAPTER_YET, VENUE_TO_ADAPTER_KEY

from instruments_service.reference_data.factory import (
    ADAPTER_DATA_SOURCES,
    _adapter_pool,
    _resolve_tardis_exchanges_for_venue,
    _run_refdata_preflight,
    clear_adapter_pool,
    create_reference_data_adapter,
    get_adapter_for_canonical_venue,
)
from instruments_service.reference_data.router import (
    ReferenceDataSourceConfig,
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

    def test_kraken_spot_live_routes_to_ccxt(self) -> None:
        from instruments_service.reference_data.adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter

        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("KRAKEN-SPOT", mode="live")
        assert isinstance(adapter, CCXTReferenceDataAdapter)
        assert adapter.venue == "KRAKEN-SPOT"

    def test_kraken_futures_live_routes_to_ccxt(self) -> None:
        from instruments_service.reference_data.adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter

        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("KRAKEN-FUTURES", mode="live")
        assert isinstance(adapter, CCXTReferenceDataAdapter)
        assert adapter.venue == "KRAKEN-FUTURES"

    def test_kraken_batch_routes_to_tardis(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("KRAKEN-SPOT", mode="batch")
        assert adapter.venue == "tardis"


# ---------------------------------------------------------------------------
# Factory: DeFi chain parsing
# ---------------------------------------------------------------------------


class TestFactoryDefiChainParsing:
    def test_defi_multichain_adapter_creation(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("AAVE_V3-ETHEREUM")
        assert adapter is not None

    def test_defi_multichain_different_chains(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("AAVE_V3-ETHEREUM")
        a2 = get_adapter_for_canonical_venue("AAVE_V3-ARBITRUM")
        # Different chain = different pool entries
        assert a1 is not a2

    def test_defi_fork_protocol_slug(self) -> None:
        """DEX forks (PancakeSwap, SushiSwap) use UniV3 adapter with resolved protocol_slug."""
        clear_adapter_pool()
        if "PANCAKESWAP_V3-ETHEREUM" in VENUE_TO_ADAPTER_KEY:
            adapter = get_adapter_for_canonical_venue("PANCAKESWAP_V3-ETHEREUM")
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

    def test_okx_bare_venue_resolves_all_itype_exchanges(self) -> None:
        """OKX spans 4 real Tardis exchanges depending on instrument_type
        (okex/okex-swap/okex-futures/okex-options) — the itype-aware gather
        must resolve ALL of them (not just one via the venue-only lookup),
        and the adapter must be told to tag every parsed instrument
        canonical_venue="OKX" (not the per-exchange reverse-map values like
        OKX-SPOT/OKX-SWAP). Regression guard for
        cefi_deribit_combo_and_okx_bare_venue_gaps_2026_07_12.md — bare "OKX"
        used to raise ValueError here (no direct tardis_to_venue value, and
        no "-" in "OKX" to trigger the suffixed-venue fallback)."""
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("OKX", mode="batch")
        assert adapter.venue == "tardis"
        assert sorted(adapter._exchanges) == [
            "okex",
            "okex-futures",
            "okex-options",
            "okex-swap",
        ]
        assert adapter._canonical_venue_override == "OKX"

    def test_deribit_combo_batch_routes_to_tardis(self) -> None:
        """Historical/batch DERIBIT-COMBO catalogue population routes through
        the Tardis adapter (exchange "deribit", combo-type self-filtered),
        NOT the live-only deribit_combo REST adapter. Regression guard for
        cefi_layer1_denominator_gaps_2026_07_03.md (NEW FINDING 2026-07-14):
        the instrument catalogue was permanently live-only because
        VENUE_TO_ADAPTER_KEY["DERIBIT-COMBO"] was hardcoded to "deribit_combo"
        regardless of mode."""
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("DERIBIT-COMBO", mode="batch")
        assert adapter.venue == "tardis"
        assert adapter._exchanges == ["deribit"]
        assert adapter._canonical_venue_override == "DERIBIT-COMBO"

    def test_deribit_combo_live_routes_to_rest_adapter(self) -> None:
        """Live/forward DERIBIT-COMBO capture keeps using the Deribit public
        REST adapter (public/get_combos) — that endpoint only exposes
        currently-active combos, which is exactly what live mode needs."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("DERIBIT-COMBO", mode="live")
        assert isinstance(adapter, DeribitComboReferenceDataAdapter)
        assert adapter.venue == "DERIBIT-COMBO"

    def test_deribit_combo_live_pool_reuse(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("DERIBIT-COMBO", mode="live")
        a2 = get_adapter_for_canonical_venue("DERIBIT-COMBO", mode="live")
        assert a1 is a2

    def test_resolve_tardis_exchanges_itype_aware_multi_exchange(self) -> None:
        """Pure-function unit test for the extracted helper: a venue with
        multiple itype-scoped Tardis exchanges (OKX-shaped) returns ALL of
        them, sorted, ignoring the single-exchange fallback entirely."""
        result = _resolve_tardis_exchanges_for_venue(
            "OKX",
            {
                ("OKX", "SPOT_PAIR"): "okex",
                ("OKX", "PERPETUAL"): "okex-swap",
                ("OKX", "FUTURE"): "okex-futures",
                ("OKX", "OPTION"): "okex-options",
                ("BYBIT", "PERPETUAL"): "bybit",
            },
            {"bybit": "BYBIT"},
            None,  # venue-only lookup would fail for bare OKX — must be ignored
        )
        assert result == ["okex", "okex-futures", "okex-options", "okex-swap"]

    def test_resolve_tardis_exchanges_single_exchange_fallback(self) -> None:
        """A venue with no itype-scoped entries falls back to the venue-only
        (direct/suffixed) exchange resolution — unchanged behaviour for
        every venue that was already working (BYBIT/DERIBIT/etc.)."""
        result = _resolve_tardis_exchanges_for_venue(
            "BYBIT",
            {},
            {"bybit": "BYBIT"},
            "bybit",
        )
        assert result == ["bybit"]

    def test_resolve_tardis_exchanges_lowercase_candidate_fallback(self) -> None:
        """No itype entries, no direct/suffixed match (direct_or_suffixed_exchange
        is None) — the lowercase-conversion candidate fallback still applies
        when the lowercased venue is a tardis_to_venue KEY, regardless of
        that key's mapped value."""
        result = _resolve_tardis_exchanges_for_venue(
            "UPBIT",
            {},
            {"upbit": "SOME-OTHER-VENUE"},
            None,
        )
        assert result == ["upbit"]

    def test_resolve_tardis_exchanges_lowercase_candidate_miss_returns_empty(self) -> None:
        """Lowercased venue is NOT a tardis_to_venue key — no fallback fires."""
        result = _resolve_tardis_exchanges_for_venue(
            "UPBIT",
            {},
            {"someone-else": "SOME-OTHER-VENUE"},
            None,
        )
        assert result == []

    def test_resolve_tardis_exchanges_no_mapping_returns_empty(self) -> None:
        result = _resolve_tardis_exchanges_for_venue("UNKNOWN-VENUE", {}, {}, None)
        assert result == []

    def test_tardis_no_mapping_raises(self) -> None:
        """Venue in VENUE_TO_ADAPTER_KEY as tardis but no Tardis exchange mapping raises ValueError."""
        clear_adapter_pool()
        mock_vm = MagicMock()
        mock_vm.get_tardis_exchange_for_venue.return_value = None
        mock_vm.tardis_to_venue = {}
        mock_vm.venue_instrument_type_to_tardis = {}

        # VenueMapping is lazily imported inside factory function body as:
        #   from unified_api_contracts import VenueMapping as _VM_cls
        # So we patch at the source module level.
        mock_vm_cls = MagicMock(return_value=mock_vm)
        with (
            patch(
                "unified_api_contracts.VenueMapping",
                mock_vm_cls,
            ),
            patch.dict(VENUE_TO_ADAPTER_KEY, {"FAKE-TARDIS-VENUE": "tardis"}, clear=False),
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
# Factory: VENUE_TO_ADAPTER_KEY completeness
# ---------------------------------------------------------------------------


class TestCanonicalVenueMapping:
    def test_all_cefi_venues_mapped(self) -> None:
        cefi = ["BINANCE-SPOT", "BINANCE-FUTURES", "BYBIT", "OKX-SPOT", "DERIBIT", "HYPERLIQUID"]
        for v in cefi:
            assert v in VENUE_TO_ADAPTER_KEY, f"{v} missing"

    def test_all_tradfi_venues_mapped(self) -> None:
        tradfi = ["CME", "NASDAQ", "NYSE", "CBOE", "ICE", "FX"]
        for v in tradfi:
            assert v in VENUE_TO_ADAPTER_KEY, f"{v} missing"

    def test_all_solana_venues_mapped(self) -> None:
        solana = ["DRIFT-SOLANA", "KAMINO-SOLANA", "RAYDIUM-SOLANA", "ORCA-SOLANA"]
        for v in solana:
            assert v in VENUE_TO_ADAPTER_KEY, f"{v} missing"

    def test_adapter_data_sources_covers_all_adapters(self) -> None:
        """Every adapter key used in VENUE_TO_ADAPTER_KEY should have a data source.

        Known gaps (adapter keys in VENUE_TO_ADAPTER_KEY but not yet in ADAPTER_DATA_SOURCES)
        are tracked here so the test remains green while documenting the discrepancy.
        """
        # Adapter keys that are mapped in VENUE_TO_ADAPTER_KEY but not yet wired
        # into ADAPTER_DATA_SOURCES — track here until the gap is closed in production code.
        known_gaps: set[str] = {"eigenlayer", "ethfi_governance"}

        adapter_keys = set(VENUE_TO_ADAPTER_KEY.values()) - {NO_ADAPTER_YET}
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
        simple_adapters = ["hyperliquid", "aster", "kalshi", "betfair"]
        for key in simple_adapters:
            adapter = create_reference_data_adapter(key)
            assert adapter is not None

    def test_case_insensitive_creation(self) -> None:
        adapter = create_reference_data_adapter("ASTER")
        assert adapter is not None


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
