"""Extended factory tests — covering get_adapter_for_canonical_venue, adapter pool, preflight."""

from __future__ import annotations

import pytest

from instruments_service.reference_data.factory import (
    ADAPTER_DATA_SOURCES,
    CANONICAL_VENUE_TO_ADAPTER,
    _run_refdata_preflight,
    clear_adapter_pool,
    create_reference_data_adapter,
    get_adapter_for_canonical_venue,
)


class TestGetAdapterForCanonicalVenue:
    def test_cefi_venue(self) -> None:
        adapter = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert adapter.venue == "HYPERLIQUID"

    def test_defi_venue_with_chain(self) -> None:
        adapter = get_adapter_for_canonical_venue("AAVEV3-ETHEREUM")
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
        a2 = get_adapter_for_canonical_venue("DERIBIT")
        assert a1 is not a2

    def test_with_api_key(self) -> None:
        clear_adapter_pool()
        adapter = get_adapter_for_canonical_venue("HYPERLIQUID", api_key="test-key")
        assert adapter._api_key == "test-key"

    def test_defi_solana_venue(self) -> None:
        adapter = get_adapter_for_canonical_venue("DRIFT-SOLANA")
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


class TestClearAdapterPool:
    def test_clear_empties_pool(self) -> None:
        clear_adapter_pool()
        get_adapter_for_canonical_venue("HYPERLIQUID")
        clear_adapter_pool()
        # After clearing, next get should create new adapter
        a1 = get_adapter_for_canonical_venue("HYPERLIQUID")
        assert a1 is not None


class TestAdapterDataSources:
    def test_expected_keys_exist(self) -> None:
        assert "binance" in ADAPTER_DATA_SOURCES
        assert "tardis" in ADAPTER_DATA_SOURCES
        assert "databento" in ADAPTER_DATA_SOURCES
        assert "hyperliquid" in ADAPTER_DATA_SOURCES

    def test_defi_data_sources(self) -> None:
        assert ADAPTER_DATA_SOURCES["aave_v3"] == "thegraph"
        assert ADAPTER_DATA_SOURCES["uniswap_v3"] == "thegraph"
        assert ADAPTER_DATA_SOURCES["balancer"] == "balancer_api_v3"

    def test_solana_no_api_key(self) -> None:
        assert ADAPTER_DATA_SOURCES["drift"] == ""
        assert ADAPTER_DATA_SOURCES["kamino"] == ""


class TestCanonicalVenueToAdapter:
    def test_cefi_venues_present(self) -> None:
        assert "BINANCE-SPOT" in CANONICAL_VENUE_TO_ADAPTER
        assert "BINANCE-FUTURES" in CANONICAL_VENUE_TO_ADAPTER
        assert "DERIBIT" in CANONICAL_VENUE_TO_ADAPTER
        assert "HYPERLIQUID" in CANONICAL_VENUE_TO_ADAPTER

    def test_defi_venues_present(self) -> None:
        assert any("AAVEV3" in k for k in CANONICAL_VENUE_TO_ADAPTER)
        assert any("UNISWAPV3" in k for k in CANONICAL_VENUE_TO_ADAPTER)

    def test_prediction_venues_present(self) -> None:
        assert "POLYMARKET" in CANONICAL_VENUE_TO_ADAPTER
        assert "KALSHI" in CANONICAL_VENUE_TO_ADAPTER

    def test_sports_venues_present(self) -> None:
        assert "BETFAIR" in CANONICAL_VENUE_TO_ADAPTER
        assert "API_FOOTBALL" in CANONICAL_VENUE_TO_ADAPTER

    def test_solana_defi_venues(self) -> None:
        assert "DRIFT-SOLANA" in CANONICAL_VENUE_TO_ADAPTER
        assert "KAMINO-SOLANA" in CANONICAL_VENUE_TO_ADAPTER


class TestRunRefdataPreflight:
    def test_unknown_venue_no_crash(self) -> None:
        # Should not raise for unknown venues
        _run_refdata_preflight("unknown_venue_xyz")

    def test_known_venue_no_crash(self) -> None:
        _run_refdata_preflight("binance")


class TestCreateReferenceDataAdapter:
    def test_all_standard_venues(self) -> None:
        standard_venues = [
            "binance",
            "bybit",
            "okx",
            "deribit",
            "coinbase",
            "hyperliquid",
            "ibkr",
            "polygon",
            "polymarket",
            "betfair",
            "tardis",
            "databento",
            "kalshi",
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
            "euler",
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
        solana_venues = ["drift", "kamino", "raydium", "orca", "marinade"]
        for venue in solana_venues:
            adapter = create_reference_data_adapter(venue)
            assert adapter is not None, f"Factory failed for venue {venue}"
