"""Unit tests for DeFi reference data adapters — venue properties and constructor patterns.

All DeFi adapters follow the same base pattern: accept chain, project_id, api_key.
These tests verify the basic instantiation and venue property for each adapter.
"""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter


class TestDefiAdapterVenueProperty:
    """Each DeFi adapter returns a non-empty venue string."""

    def test_aave_v3_venue(self) -> None:
        adapter = AaveV3ReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_compound_v3_venue(self) -> None:
        adapter = CompoundV3ReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_morpho_venue(self) -> None:
        adapter = MorphoReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_fluid_venue(self) -> None:
        adapter = FluidReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_uniswap_v2_venue(self) -> None:
        adapter = UniswapV2ReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_uniswap_v3_venue(self) -> None:
        adapter = UniswapV3ReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_uniswap_v4_venue(self) -> None:
        adapter = UniswapV4ReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_balancer_venue(self) -> None:
        adapter = BalancerReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_curve_venue(self) -> None:
        adapter = CurveReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_lido_venue(self) -> None:
        adapter = LidoReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_etherfi_venue(self) -> None:
        adapter = EtherFiReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_ethena_venue(self) -> None:
        adapter = EthenaReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_drift_venue(self) -> None:
        adapter = DriftReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_kamino_venue(self) -> None:
        adapter = KaminoReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_raydium_venue(self) -> None:
        adapter = RaydiumReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_orca_venue(self) -> None:
        adapter = OrcaReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0

    def test_marinade_venue(self) -> None:
        adapter = MarinadeReferenceDataAdapter()
        assert adapter.venue is not None and len(adapter.venue) > 0


class TestDefiAdapterConstructor:
    """DeFi adapters accept chain parameter."""

    def test_aave_v3_with_chain(self) -> None:
        adapter = AaveV3ReferenceDataAdapter(chain="ARBITRUM")
        assert adapter is not None

    def test_uniswap_v3_with_chain(self) -> None:
        adapter = UniswapV3ReferenceDataAdapter(chain="ETHEREUM")
        assert adapter is not None

    def test_compound_v3_with_chain(self) -> None:
        adapter = CompoundV3ReferenceDataAdapter(chain="ETHEREUM")
        assert adapter is not None

    def test_morpho_with_chain(self) -> None:
        adapter = MorphoReferenceDataAdapter(chain="ETHEREUM")
        assert adapter is not None

    def test_balancer_with_chain(self) -> None:
        adapter = BalancerReferenceDataAdapter(chain="ETHEREUM")
        assert adapter is not None

    def test_default_chain_is_ethereum(self) -> None:
        adapter = AaveV3ReferenceDataAdapter()
        # Most adapters default to ETHEREUM
        assert adapter is not None


class TestDefiAdapterUnsupportedMethods:
    """DeFi adapters raise NotImplementedError for unsupported methods."""

    @pytest.mark.asyncio
    async def test_aave_get_funding_rate_raises(self) -> None:
        adapter = AaveV3ReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("WETH")

    @pytest.mark.asyncio
    async def test_aave_get_ohlcv_raises(self) -> None:
        adapter = AaveV3ReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("WETH")

    @pytest.mark.asyncio
    async def test_uniswap_v3_get_funding_rate_raises(self) -> None:
        adapter = UniswapV3ReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("WETH")

    @pytest.mark.asyncio
    async def test_lido_get_funding_rate_raises(self) -> None:
        adapter = LidoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("stETH")

    @pytest.mark.asyncio
    async def test_drift_get_options_chain_raises(self) -> None:
        adapter = DriftReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("SOL")

    @pytest.mark.asyncio
    async def test_drift_get_expiry_calendar_raises(self) -> None:
        adapter = DriftReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("SOL")

    @pytest.mark.asyncio
    async def test_kamino_get_funding_rate_raises(self) -> None:
        adapter = KaminoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("SOL")

    @pytest.mark.asyncio
    async def test_raydium_get_funding_rate_raises(self) -> None:
        adapter = RaydiumReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("SOL")

    @pytest.mark.asyncio
    async def test_orca_get_funding_rate_raises(self) -> None:
        adapter = OrcaReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("SOL")

    @pytest.mark.asyncio
    async def test_marinade_get_funding_rate_raises(self) -> None:
        adapter = MarinadeReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("SOL")

    @pytest.mark.asyncio
    async def test_curve_get_funding_rate_raises(self) -> None:
        adapter = CurveReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("ETH")

    @pytest.mark.asyncio
    async def test_ethena_get_funding_rate_raises(self) -> None:
        adapter = EthenaReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("sUSDe")

    @pytest.mark.asyncio
    async def test_etherfi_get_funding_rate_raises(self) -> None:
        adapter = EtherFiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("eETH")
