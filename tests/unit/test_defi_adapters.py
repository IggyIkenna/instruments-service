"""Unit tests for DeFi reference data adapters — venue properties and constructor patterns.

All DeFi adapters follow the same base pattern: accept chain, project_id, api_key.
These tests verify the basic instantiation and venue property for each adapter.
"""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.benqi import BenqiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.euler_v2 import EulerV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.radiant import RadiantReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.venus import VenusReferenceDataAdapter


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

    # Phase-4 lending protocols.
    def test_euler_v2_venue(self) -> None:
        adapter = EulerV2ReferenceDataAdapter()
        assert adapter.venue == "euler_v2"

    def test_radiant_venue(self) -> None:
        adapter = RadiantReferenceDataAdapter()
        assert adapter.venue == "radiant"

    def test_venus_venue(self) -> None:
        adapter = VenusReferenceDataAdapter()
        assert adapter.venue == "venus"

    def test_benqi_venue(self) -> None:
        adapter = BenqiReferenceDataAdapter()
        assert adapter.venue == "benqi"


class TestPhase4LendingAdaptersBehaviour:
    """Phase-4 lending adapters: get_instruments returns expected shape.

    Network is blocked in QG, so these tests stub
    ``batch_resolve_evm_creation_timestamps`` to return an empty map; the
    adapter falls through to the protocol's deploy-date floor for every
    market and returns one InstrumentRecord per registry row.
    """

    @pytest.mark.asyncio
    async def test_euler_v2_returns_curated_lending_markets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolver(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {}

        monkeypatch.setattr(
            "instruments_service.reference_data.adapters.defi.euler_v2.batch_resolve_evm_creation_timestamps",
            _fake_resolver,
        )
        adapter = EulerV2ReferenceDataAdapter(chain="ETHEREUM")
        out = await adapter.get_instruments()
        assert len(out) >= 1
        assert all(rec.venue == "EULER_V2-ETHEREUM" for rec in out)
        assert all(rec.instrument_type.value == "LENDING" for rec in out)

    @pytest.mark.asyncio
    async def test_radiant_returns_per_chain_markets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolver(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {}

        monkeypatch.setattr(
            "instruments_service.reference_data.adapters.defi.radiant.batch_resolve_evm_creation_timestamps",
            _fake_resolver,
        )
        # Arbitrum (primary deployment) returns markets.
        adapter = RadiantReferenceDataAdapter(chain="ARBITRUM")
        out = await adapter.get_instruments()
        assert len(out) >= 1
        assert all(rec.venue == "RADIANT-ARBITRUM" for rec in out)
        # Unsupported chain returns empty.
        adapter_polygon = RadiantReferenceDataAdapter(chain="POLYGON")
        assert await adapter_polygon.get_instruments() == []

    @pytest.mark.asyncio
    async def test_venus_returns_bsc_markets_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolver(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {}

        monkeypatch.setattr(
            "instruments_service.reference_data.adapters.defi.venus.batch_resolve_evm_creation_timestamps",
            _fake_resolver,
        )
        adapter = VenusReferenceDataAdapter()
        out = await adapter.get_instruments()
        assert len(out) >= 1
        assert all(rec.venue == "VENUS-BSC" for rec in out)

    @pytest.mark.asyncio
    async def test_benqi_returns_avalanche_markets_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _fake_resolver(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {}

        monkeypatch.setattr(
            "instruments_service.reference_data.adapters.defi.benqi.batch_resolve_evm_creation_timestamps",
            _fake_resolver,
        )
        # Avalanche is the only supported chain.
        adapter = BenqiReferenceDataAdapter()
        out = await adapter.get_instruments()
        assert len(out) >= 1
        assert all(rec.venue == "BENQI-AVALANCHE" for rec in out)
        # Other chains return empty.
        adapter_eth = BenqiReferenceDataAdapter(chain="ETHEREUM")
        assert await adapter_eth.get_instruments() == []

    @pytest.mark.asyncio
    async def test_phase4_adapters_skip_non_lending_instrument_types(
        self,
    ) -> None:
        # When called with a non-lending instrument_type, every Phase-4
        # adapter returns an empty list (no falsey edge case).
        for cls in (
            EulerV2ReferenceDataAdapter,
            RadiantReferenceDataAdapter,
            VenusReferenceDataAdapter,
            BenqiReferenceDataAdapter,
        ):
            assert await cls().get_instruments(instrument_type="spot_pair") == []

    @pytest.mark.asyncio
    async def test_phase4_adapters_unsupported_methods_raise(self) -> None:
        for cls in (
            EulerV2ReferenceDataAdapter,
            RadiantReferenceDataAdapter,
            VenusReferenceDataAdapter,
            BenqiReferenceDataAdapter,
        ):
            adapter = cls()
            with pytest.raises(NotImplementedError):
                await adapter.get_options_chain("WETH")
            with pytest.raises(NotImplementedError):
                await adapter.get_expiry_calendar("WETH")
            with pytest.raises(NotImplementedError):
                await adapter.get_funding_rate("WETH")
            with pytest.raises(NotImplementedError):
                await adapter.get_ohlcv("WETH")


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
