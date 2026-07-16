"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from __future__ import annotations

import pytest
from unified_api_contracts.registry import VENUE_TO_ADAPTER_KEY

from instruments_service.reference_data import create_reference_data_adapter
from instruments_service.reference_data.adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.ibkr import IBKRReferenceDataAdapter
from instruments_service.reference_data.factory import (
    ADAPTER_DATA_SOURCES,
    _run_refdata_preflight,
    clear_adapter_pool,
    get_adapter_for_canonical_venue,
)


class TestFactory:
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
        adapter = create_reference_data_adapter("HYPERLIQUID")
        assert isinstance(adapter, HyperliquidReferenceDataAdapter)


class TestGetAdapterForCanonicalVenue:
    def test_cefi_venue(self) -> None:
        adapter = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert adapter.venue == "HYPERLIQUID"

    def test_defi_venue_with_chain(self) -> None:
        adapter = get_adapter_for_canonical_venue("AAVE_V3-ETHEREUM")
        assert adapter is not None

    def test_unsupported_venue_raises(self) -> None:
        with pytest.raises(ValueError, match="No URDI adapter"):
            get_adapter_for_canonical_venue("UNKNOWN_VENUE_XYZ")

    def test_adapter_pool_reuse(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("HYPERLIQUID")
        a2 = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert a1 is a2

    def test_adapter_pool_different_venues(self) -> None:
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("HYPERLIQUID")
        a2 = get_adapter_for_canonical_venue("ASTER")
        assert a1 is not a2

    def test_with_api_key(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("HYPERLIQUID", api_key="test-key")
        assert adapter._api_key == "test-key"

    def test_defi_solana_venue(self) -> None:
        adapter = get_adapter_for_canonical_venue("KAMINO-SOLANA")
        assert adapter is not None

    def test_databento_with_date(self) -> None:
        adapter = get_adapter_for_canonical_venue("CME", date="2026-03-22")
        assert adapter is not None

    def test_polymarket_with_extra_keys(self) -> None:
        adapter = get_adapter_for_canonical_venue("POLYMARKET", extra_api_keys={"api_football": "test-key"})
        assert adapter is not None

    def test_api_football_with_date(self) -> None:
        adapter = get_adapter_for_canonical_venue("API_FOOTBALL", date="2026-03-22")
        assert adapter is not None

    def test_api_football_not_pooled_across_dates(self) -> None:
        """2026-07-14 regression: the api_football URDI adapter bakes its
        target date in at construction (``self._date``). Pooling it WITHOUT
        the date in the pool key made every later date of a multi-date batch
        run reuse the FIRST date's fixture universe → per-date filter saw 0
        active instruments → the zero-record path stamped false
        EXPECTED_NO_FIXTURE markers over real fixture days (GW enrichment
        content-verification RED, issue
        sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14).
        """
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("API_FOOTBALL", date="2026-03-22")
        a2 = get_adapter_for_canonical_venue("API_FOOTBALL", date="2026-03-23")
        assert a1 is not a2, "api_football adapter must not be pooled across dates"
        assert a1._date == "2026-03-22"
        assert a2._date == "2026-03-23"
        # Same date → pool hit is still fine.
        a3 = get_adapter_for_canonical_venue("API_FOOTBALL", date="2026-03-22")
        assert a3 is a1


class TestClearAdapterPool:
    def test_clear_empties_pool(self) -> None:
        clear_adapter_pool()
        get_adapter_for_canonical_venue("HYPERLIQUID")
        clear_adapter_pool()
        a1 = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert a1 is not None


class TestAdapterDataSources:
    def test_expected_keys_exist(self) -> None:
        assert "tardis" in ADAPTER_DATA_SOURCES
        assert "databento" in ADAPTER_DATA_SOURCES
        assert "hyperliquid" in ADAPTER_DATA_SOURCES

    def test_defi_data_sources(self) -> None:
        assert ADAPTER_DATA_SOURCES["aave_v3"] == "thegraph"
        assert ADAPTER_DATA_SOURCES["uniswap_v3"] == "thegraph"
        assert ADAPTER_DATA_SOURCES["balancer"] == "balancer_api_v3"

    def test_solana_no_api_key(self) -> None:
        assert ADAPTER_DATA_SOURCES["kamino"] == ""
        assert ADAPTER_DATA_SOURCES["raydium"] == ""


class TestCanonicalVenueToAdapter:
    def test_cefi_venues_present(self) -> None:
        assert "BINANCE-SPOT" in VENUE_TO_ADAPTER_KEY
        assert "BINANCE-FUTURES" in VENUE_TO_ADAPTER_KEY
        assert "DERIBIT" in VENUE_TO_ADAPTER_KEY
        assert "HYPERLIQUID" in VENUE_TO_ADAPTER_KEY
        # Binance COIN-M (inverse/delivery) — cefi_universe_capture_rule 2026-06-24
        assert "BINANCE-DELIVERY" in VENUE_TO_ADAPTER_KEY
        assert VENUE_TO_ADAPTER_KEY["BINANCE-DELIVERY"] == "tardis"

    def test_defi_venues_present(self) -> None:
        assert any("AAVE_V3" in k for k in VENUE_TO_ADAPTER_KEY)
        assert any("UNISWAP_V3" in k for k in VENUE_TO_ADAPTER_KEY)

    def test_prediction_venues_present(self) -> None:
        assert "POLYMARKET" in VENUE_TO_ADAPTER_KEY
        assert "KALSHI" in VENUE_TO_ADAPTER_KEY

    def test_sports_venues_present(self) -> None:
        assert "BETFAIR" in VENUE_TO_ADAPTER_KEY
        assert "API_FOOTBALL" in VENUE_TO_ADAPTER_KEY

    def test_solana_defi_venues(self) -> None:
        assert "KAMINO-SOLANA" in VENUE_TO_ADAPTER_KEY
        assert "RAYDIUM-SOLANA" in VENUE_TO_ADAPTER_KEY


class TestRunRefdataPreflight:
    def test_unknown_venue_no_crash(self) -> None:
        _run_refdata_preflight("unknown_venue_xyz")

    def test_known_venue_no_crash(self) -> None:
        _run_refdata_preflight("hyperliquid")


class TestCreateReferenceDataAdapterExtended:
    def test_all_standard_venues(self) -> None:
        standard_venues = [
            "hyperliquid",
            "ibkr",
            "polymarket",
            "betfair",
            "tardis",
            "databento",
            "kalshi",
            "aster",
        ]
        for venue in standard_venues:
            adapter = create_reference_data_adapter(venue)
            assert adapter is not None, f"Factory failed for venue {venue}"

    def test_defi_venues(self) -> None:
        defi_venues = [
            "aave_v3",
            "uniswap_v2",
            "uniswap_v3",
            "uniswap_v4",
            "morpho",
            "fluid",
            "balancer",
            "curve",
            "lido",
            "etherfi",
            "ethena",
        ]
        for venue in defi_venues:
            adapter = create_reference_data_adapter(venue)
            assert adapter is not None, f"Factory failed for venue {venue}"

    def test_solana_venues(self) -> None:
        solana_venues = ["kamino", "raydium", "orca", "marinade"]
        for venue in solana_venues:
            adapter = create_reference_data_adapter(venue)
            assert adapter is not None, f"Factory failed for venue {venue}"


class TestKRXRouting:
    """Bug-1 regression: KRX was missing from VENUE_TO_ADAPTER_KEY (2026-06-24).

    Symptoms: 'No URDI adapter for ['KRX']' + 'URDI fetch: (KRX, UNSUPPORTED)'
    causing shard catastrophic failure (3/7 venues written, CME/KRX/NASDAQ/NYSE missing).
    """

    def test_krx_in_canonical_venue_to_adapter(self) -> None:
        """VENUE_TO_ADAPTER_KEY must map KRX to the databento adapter."""
        assert "KRX" in VENUE_TO_ADAPTER_KEY, "KRX missing from VENUE_TO_ADAPTER_KEY — add 'KRX': 'databento'"
        assert VENUE_TO_ADAPTER_KEY["KRX"] == "databento"

    def test_krx_get_adapter_resolves(self) -> None:
        """get_adapter_for_canonical_venue('KRX') must not raise ValueError."""
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("KRX", date="2026-06-24")
        assert adapter is not None

    def test_tradfi_venues_all_in_adapter_map(self) -> None:
        """All TradFi canonical venues (CME/NASDAQ/NYSE/CBOE/ICE/FX/KRX) must be mapped."""
        expected_tradfi = {"CME", "NASDAQ", "NYSE", "CBOE", "ICE", "FX", "KRX"}
        missing = expected_tradfi - set(VENUE_TO_ADAPTER_KEY)
        assert not missing, f"TradFi venues missing from VENUE_TO_ADAPTER_KEY: {missing}"
