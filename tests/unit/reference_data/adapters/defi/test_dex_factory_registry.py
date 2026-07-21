"""Tests for the static factory-address -> DEX protocol-version registry.

Covers the operator ruling (2026-07-21, defi_consolidated_closeout_2026_07_18.md
"bare SUSHISWAP/UNISWAP version"): a known (chain, factory_address) pair resolves
its exact protocol version; an unknown/absent factory address or chain resolves
to None (never a guess, never a default).
"""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.defi._dex_factory_registry import (
    SUSHISWAP_V2_FACTORY_BY_CHAIN,
    SUSHISWAP_V3_FACTORY_BY_CHAIN,
    UNISWAP_V2_FACTORY_BY_CHAIN,
    UNISWAP_V3_FACTORY_BY_CHAIN,
    UNISWAP_V4_POOL_MANAGER_BY_CHAIN,
    resolve_dex_version_from_factory,
)


class TestResolveKnownAddresses:
    """Every hardcoded (chain, address) pair round-trips to its own version tag."""

    @pytest.mark.parametrize("chain, address", list(UNISWAP_V2_FACTORY_BY_CHAIN.items()))
    def test_uniswap_v2(self, chain: str, address: str) -> None:
        assert resolve_dex_version_from_factory(address, chain) == "UNISWAP_V2"

    @pytest.mark.parametrize("chain, address", list(UNISWAP_V3_FACTORY_BY_CHAIN.items()))
    def test_uniswap_v3(self, chain: str, address: str) -> None:
        assert resolve_dex_version_from_factory(address, chain) == "UNISWAP_V3"

    @pytest.mark.parametrize("chain, address", list(UNISWAP_V4_POOL_MANAGER_BY_CHAIN.items()))
    def test_uniswap_v4(self, chain: str, address: str) -> None:
        assert resolve_dex_version_from_factory(address, chain) == "UNISWAP_V4"

    @pytest.mark.parametrize("chain, address", list(SUSHISWAP_V2_FACTORY_BY_CHAIN.items()))
    def test_sushiswap_v2(self, chain: str, address: str) -> None:
        assert resolve_dex_version_from_factory(address, chain) == "SUSHISWAP_V2"

    @pytest.mark.parametrize("chain, address", list(SUSHISWAP_V3_FACTORY_BY_CHAIN.items()))
    def test_sushiswap_v3(self, chain: str, address: str) -> None:
        assert resolve_dex_version_from_factory(address, chain) == "SUSHISWAP_V3"


class TestCaseInsensitivity:
    def test_uppercase_address_resolves(self) -> None:
        addr = UNISWAP_V2_FACTORY_BY_CHAIN["ETHEREUM"].upper()
        assert resolve_dex_version_from_factory(addr, "ETHEREUM") == "UNISWAP_V2"

    def test_lowercase_chain_resolves(self) -> None:
        addr = UNISWAP_V2_FACTORY_BY_CHAIN["ETHEREUM"]
        assert resolve_dex_version_from_factory(addr, "ethereum") == "UNISWAP_V2"


class TestGenuineResidualNeverGuessed:
    """The exact 'surface it, don't guess' contract the operator ruling requires."""

    def test_none_factory_address_returns_none(self) -> None:
        assert resolve_dex_version_from_factory(None, "ETHEREUM") is None

    def test_empty_factory_address_returns_none(self) -> None:
        assert resolve_dex_version_from_factory("", "ETHEREUM") is None

    def test_unknown_address_returns_none(self) -> None:
        assert resolve_dex_version_from_factory("0x" + "1" * 40, "ETHEREUM") is None

    def test_known_address_on_wrong_chain_returns_none(self) -> None:
        """A real SushiSwap V3 factory address is chain-specific — the SAME address
        can denote a DIFFERENT protocol/version on a different chain (verified live,
        see module docstring). Resolution must never leak across chains."""
        eth_sushi_v3 = SUSHISWAP_V3_FACTORY_BY_CHAIN["ETHEREUM"]
        assert resolve_dex_version_from_factory(eth_sushi_v3, "SOLANA") is None

    def test_unknown_chain_returns_none(self) -> None:
        assert resolve_dex_version_from_factory(UNISWAP_V2_FACTORY_BY_CHAIN["ETHEREUM"], "BSC") is None

    def test_sushiswap_v2_and_v3_arbitrum_addresses_are_distinct(self) -> None:
        """Regression guard for the real cross-chain address collision this module's
        docstring documents: 0xc35dadb6...bc74c4 is SushiSwap V2 Classic Factory on
        ARBITRUM but SushiSwap V3 Factory on BASE. Must resolve to the version that
        matches the CHAIN the address is captured on, not a fixed global mapping."""
        shared_address = "0xc35DADB65012eC5796536bD9864eD8773aBc74C4"
        assert SUSHISWAP_V2_FACTORY_BY_CHAIN["ARBITRUM"].lower() == shared_address.lower()
        assert SUSHISWAP_V3_FACTORY_BY_CHAIN["BASE"].lower() == shared_address.lower()
        assert resolve_dex_version_from_factory(shared_address, "ARBITRUM") == "SUSHISWAP_V2"
        assert resolve_dex_version_from_factory(shared_address, "BASE") == "SUSHISWAP_V3"


class TestRegistryScope:
    """The map only covers chains this workspace actually captures Uniswap/SushiSwap
    DeFi data on (verified against UAC PROTOCOL_LAUNCH_DATES + ALL_DEFI_VENUES)."""

    def test_uniswap_v2_ethereum_only(self) -> None:
        assert set(UNISWAP_V2_FACTORY_BY_CHAIN) == {"ETHEREUM"}

    def test_uniswap_v4_ethereum_only(self) -> None:
        assert set(UNISWAP_V4_POOL_MANAGER_BY_CHAIN) == {"ETHEREUM"}

    def test_uniswap_v3_matches_uac_audited_chains(self) -> None:
        assert set(UNISWAP_V3_FACTORY_BY_CHAIN) == {"ETHEREUM", "ARBITRUM", "OPTIMISM", "POLYGON", "BASE"}
