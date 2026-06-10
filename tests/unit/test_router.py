"""Unit tests for venue adapters (no live network — uses mocked responses)."""

import pytest

from instruments_service.reference_data.adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter
from instruments_service.reference_data.adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter
from instruments_service.reference_data.adapters.cefi.tardis import TardisReferenceDataAdapter
from instruments_service.reference_data.adapters.prediction.polymarket import PolymarketReferenceDataAdapter
from instruments_service.reference_data.adapters.sports.adapters.betfair import BetfairReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.databento import DatabentoReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.ibkr import IBKRReferenceDataAdapter
from instruments_service.reference_data.router import (
    ReferenceDataSourceConfig,
    _resolve_tardis_exchanges,
    create_reference_data_adapter_for_source,
)


class TestRouter:
    def test_route_direct_hyperliquid(self) -> None:
        config = ReferenceDataSourceConfig(venue="hyperliquid", data_source="direct")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, HyperliquidReferenceDataAdapter)

    def test_route_direct_ibkr(self) -> None:
        config = ReferenceDataSourceConfig(venue="ibkr", data_source="direct")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, IBKRReferenceDataAdapter)

    def test_route_direct_betfair(self) -> None:
        config = ReferenceDataSourceConfig(venue="betfair", data_source="direct")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, BetfairReferenceDataAdapter)

    def test_route_direct_polymarket(self) -> None:
        config = ReferenceDataSourceConfig(venue="polymarket", data_source="direct")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, PolymarketReferenceDataAdapter)

    def test_route_direct_unsupported_raises(self) -> None:
        config = ReferenceDataSourceConfig(venue="notavenue", data_source="direct")
        with pytest.raises(ValueError, match="No direct adapter"):
            create_reference_data_adapter_for_source(config)

    def test_route_databento_with_dataset_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="databento", dataset="XNAS.ITCH")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, DatabentoReferenceDataAdapter)

    def test_route_databento_known_venue(self) -> None:
        config = ReferenceDataSourceConfig(venue="apple", data_source="databento")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, DatabentoReferenceDataAdapter)

    def test_route_databento_unknown_venue(self) -> None:
        config = ReferenceDataSourceConfig(venue="unknownvenue", data_source="databento")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, DatabentoReferenceDataAdapter)

    def test_route_tardis_with_exchange_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="tardis", exchange="binance-futures")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, TardisReferenceDataAdapter)

    def test_route_tardis_known_venue(self) -> None:
        config = ReferenceDataSourceConfig(venue="deribit", data_source="tardis")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, TardisReferenceDataAdapter)

    def test_route_tardis_unknown_venue(self) -> None:
        config = ReferenceDataSourceConfig(venue="unknownvenue", data_source="tardis")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, TardisReferenceDataAdapter)

    def test_resolve_tardis_exchanges_okx_three_distinct(self) -> None:
        """OKX is 3 distinct Tardis exchanges — each suffixed venue resolves to its OWN
        exchange (regression for the OKX-FUTURES HTTP-400 symbol bug: the lookup used to
        fall through to the adapter default 'okex' spot → BTC-USDT instead of native
        okex-futures dated ids like BTC-USD-260626)."""
        assert _resolve_tardis_exchanges("okx-spot", None) == ["okex"]
        assert _resolve_tardis_exchanges("okx-swap", None) == ["okex-swap"]
        assert _resolve_tardis_exchanges("okx-futures", None) == ["okex-futures"]
        # OKX-FUTURES must NOT fall through to the spot 'okex' default.
        assert _resolve_tardis_exchanges("okx-futures", None) != []
        # Explicit override still wins.
        assert _resolve_tardis_exchanges("okx-futures", "okex-swap") == ["okex-swap"]

    def test_route_ccxt_with_exchange_override(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="ccxt", exchange="binanceus")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, CCXTReferenceDataAdapter)

    def test_route_ccxt_known_venue(self) -> None:
        config = ReferenceDataSourceConfig(venue="bybit", data_source="ccxt")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, CCXTReferenceDataAdapter)

    def test_route_ccxt_unknown_venue_raises(self) -> None:
        config = ReferenceDataSourceConfig(venue="notavenue", data_source="ccxt")
        with pytest.raises(ValueError, match="No CCXT exchange_id"):
            create_reference_data_adapter_for_source(config)

    def test_route_ibkr(self) -> None:
        config = ReferenceDataSourceConfig(venue="cme_futures", data_source="ibkr")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, IBKRReferenceDataAdapter)

    def test_route_betfair(self) -> None:
        config = ReferenceDataSourceConfig(venue="betfair", data_source="betfair")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, BetfairReferenceDataAdapter)

    def test_route_polymarket(self) -> None:
        config = ReferenceDataSourceConfig(venue="polymarket", data_source="polymarket")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, PolymarketReferenceDataAdapter)

    def test_route_unsupported_source_raises(self) -> None:
        config = ReferenceDataSourceConfig(venue="binance", data_source="notsupported")
        with pytest.raises(ValueError, match="Unsupported data_source"):
            create_reference_data_adapter_for_source(config)

    def test_route_case_insensitive(self) -> None:
        config = ReferenceDataSourceConfig(venue="HYPERLIQUID", data_source="DIRECT")
        adapter = create_reference_data_adapter_for_source(config)
        assert isinstance(adapter, HyperliquidReferenceDataAdapter)

    def test_route_with_project_id(self) -> None:
        config = ReferenceDataSourceConfig(venue="hyperliquid", data_source="direct")
        adapter = create_reference_data_adapter_for_source(config, project_id="my-project")
        assert isinstance(adapter, HyperliquidReferenceDataAdapter)
        assert adapter._project_id == "my-project"

    def test_reference_data_source_config_fallback_field(self) -> None:
        config = ReferenceDataSourceConfig(
            venue="hyperliquid",
            data_source="direct",
            fallback_data_source="databento",
        )
        assert config.fallback_data_source == "databento"


# ---------------------------------------------------------------------------
# Betfair adapter tests
# ---------------------------------------------------------------------------
