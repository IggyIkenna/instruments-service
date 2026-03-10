"""
Coverage tests for instruments_service config submodules.

Targets 0%-covered config modules:
- config/data_type_config.py — module constants
- config/defi_definitions.py — DEFI_VENUE_TO_PROTOCOL, DEFI_PROTOCOLS dicts
- config/venue_mappings.py — TRADFI_VENUE_MAPPINGS, DEFI_VENUE_TO_PROTOCOL lists
"""

from __future__ import annotations

import importlib


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
