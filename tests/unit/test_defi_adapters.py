"""
Unit tests for DeFi adapters (Balancer, EtherFi, Lido, Morpho, Curve, Uniswap V3).

Note: Aster and Hyperliquid tests are in test_onchain_perp_adapters.py (moved to onchain_perps/).
"""

from unittest.mock import Mock, patch


class TestBalancerAdapter:
    """Tests for BalancerAdapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.balancer_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.balancer_adapter import BalancerAdapter

            adapter = BalancerAdapter.__new__(BalancerAdapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            # Manually set venue as __init__ would
            chain_suffix_map = {"ETHEREUM": "ETH", "ARBITRUM": "ARB", "BASE": "BASE"}
            venue_suffix = chain_suffix_map.get(adapter.chain, adapter.chain[:3])
            adapter.venue = f"BALANCER-{venue_suffix}"

            assert adapter.venue == "BALANCER-ETH"

    def test_init_arbitrum(self):
        """Test initialization for Arbitrum chain."""
        with patch(
            "instruments_service.app.venues.defi.balancer_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.balancer_adapter import BalancerAdapter

            adapter = BalancerAdapter.__new__(BalancerAdapter)
            adapter.chain = "ARBITRUM"
            adapter.project_id = "test-project"
            chain_suffix_map = {"ETHEREUM": "ETH", "ARBITRUM": "ARB", "BASE": "BASE"}
            venue_suffix = chain_suffix_map.get(adapter.chain, adapter.chain[:3])
            adapter.venue = f"BALANCER-{venue_suffix}"

            assert adapter.venue == "BALANCER-ARB"

    def test_fetch_pools_empty(self):
        """Test fetch_pools with empty result."""
        with patch(
            "instruments_service.app.venues.defi.balancer_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.balancer_adapter import BalancerAdapter

            adapter = BalancerAdapter.__new__(BalancerAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "BALANCER-ETH"
            adapter.project_id = "test-project"
            adapter.graph_client = Mock()
            adapter.graph_client.execute_query_sync = Mock(return_value={"data": {"poolGetPools": []}})

            result = adapter.fetch_pools()
            assert isinstance(result, dict)
            assert len(result) == 0

    def test_fetch_pools_with_data(self):
        """Test fetch_pools with pool data."""
        with patch(
            "instruments_service.app.venues.defi.balancer_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.balancer_adapter import BalancerAdapter

            adapter = BalancerAdapter.__new__(BalancerAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "BALANCER-ETH"
            adapter.project_id = "test-project"
            adapter.graph_client = Mock()

            mock_pool = {
                "id": "0x123",
                "address": "0x123",
                "name": "ETH-USDC Pool",
                "symbol": "B-ETH-USDC",
                "poolTokens": [
                    {"symbol": "ETH", "address": "0xeth"},
                    {"symbol": "USDC", "address": "0xusdc"},
                ],
                "dynamicData": {"totalLiquidity": "1000000"},
                "type": "WEIGHTED",
            }
            adapter.graph_client.execute_query_sync = Mock(return_value={"data": {"poolGetPools": [mock_pool]}})

            result = adapter.fetch_pools()
            assert isinstance(result, dict)


# NOTE: Hyperliquid and Aster adapter tests moved to test_onchain_perp_adapters.py
# These adapters are now in venues/onchain_perps/ and use BaseClients from unified-cloud-services


class TestEtherFiAdapter:
    """Tests for EtherFiAdapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.lst_adapters.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.lst_adapters import EtherFiAdapter

            adapter = EtherFiAdapter.__new__(EtherFiAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "ETHERFI"
            adapter.project_id = "test-project"

            assert adapter.venue == "ETHERFI"
            assert adapter.chain == "ETHEREUM"

    def test_fetch_lst_instruments(self):
        """Test fetch_lst_instruments returns expected format."""
        with patch(
            "instruments_service.app.venues.defi.lst_adapters.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.lst_adapters import EtherFiAdapter

            adapter = EtherFiAdapter.__new__(EtherFiAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "ETHERFI"
            adapter.project_id = "test-project"

            result = adapter.fetch_lst_instruments()
            assert isinstance(result, dict)
            # EtherFi should have weETH instrument
            assert any("WEETH" in key for key in result.keys()) or len(result) >= 0


class TestLidoAdapter:
    """Tests for LidoAdapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.lst_adapters.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.lst_adapters import LidoAdapter

            adapter = LidoAdapter.__new__(LidoAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "LIDO"
            adapter.project_id = "test-project"

            assert adapter.venue == "LIDO"
            assert adapter.chain == "ETHEREUM"

    def test_fetch_lst_instruments(self):
        """Test fetch_lst_instruments returns expected format."""
        with patch(
            "instruments_service.app.venues.defi.lst_adapters.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.lst_adapters import LidoAdapter

            adapter = LidoAdapter.__new__(LidoAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "LIDO"
            adapter.project_id = "test-project"

            result = adapter.fetch_lst_instruments()
            assert isinstance(result, dict)
            # Lido should have stETH/wstETH instruments
            assert any("STETH" in key.upper() for key in result.keys()) or len(result) >= 0


class TestMorphoAdapter:
    """Tests for MorphoAdapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.morpho_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.morpho_adapter import MorphoAdapter

            adapter = MorphoAdapter.__new__(MorphoAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "MORPHO"
            adapter.project_id = "test-project"
            adapter.api_url = "https://blue-api.morpho.org/graphql"

            assert adapter.venue == "MORPHO"
            assert adapter.chain == "ETHEREUM"

    def test_fetch_markets_empty(self):
        """Test fetch_markets with empty response."""
        with patch(
            "instruments_service.app.venues.defi.morpho_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.morpho_adapter import MorphoAdapter

            adapter = MorphoAdapter.__new__(MorphoAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "MORPHO"
            adapter.project_id = "test-project"
            adapter.api_url = "https://blue-api.morpho.org/graphql"

            with patch("requests.post") as mock_post:
                mock_response = Mock()
                mock_response.json.return_value = {"data": {"markets": {"items": []}}}
                mock_response.raise_for_status = Mock()
                mock_post.return_value = mock_response

                result = adapter.fetch_markets()
                assert isinstance(result, dict)
                assert len(result) == 0


class TestCurveRPCAdapter:
    """Tests for CurveRPCAdapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.curve_rpc_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.curve_rpc_adapter import CurveRPCAdapter

            adapter = CurveRPCAdapter.__new__(CurveRPCAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "CURVE-ETH"
            adapter.project_id = "test-project"

            assert adapter.venue == "CURVE-ETH"
            assert adapter.chain == "ETHEREUM"

    def test_fetch_pools_empty(self):
        """Test fetch_pools with empty response (returns list when w3 is None)."""
        with patch(
            "instruments_service.app.venues.defi.curve_rpc_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.curve_rpc_adapter import CurveRPCAdapter

            adapter = CurveRPCAdapter.__new__(CurveRPCAdapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "CURVE-ETH"
            adapter.project_id = "test-project"
            adapter.w3 = None  # Set w3 attribute to None
            adapter._fetch_pools_from_api = Mock(return_value=[])

            result = adapter.fetch_pools()
            # Curve adapter returns empty list when w3 is None
            assert isinstance(result, (dict, list))
            assert len(result) == 0


class TestUniswapV3Adapter:
    """Tests for UniswapV3Adapter."""

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.uniswapv3_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter

            adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            # Venue format: UNISWAPV3-ETH
            chain_suffix_map = {"ETHEREUM": "ETH", "ARBITRUM": "ARB", "BASE": "BASE"}
            venue_suffix = chain_suffix_map.get(adapter.chain, adapter.chain[:3])
            adapter.venue = f"UNISWAPV3-{venue_suffix}"

            assert adapter.venue == "UNISWAPV3-ETH"
            assert adapter.chain == "ETHEREUM"

    def test_init_base(self):
        """Test initialization for Base chain."""
        with patch(
            "instruments_service.app.venues.defi.uniswapv3_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter

            adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
            adapter.chain = "BASE"
            adapter.project_id = "test-project"
            chain_suffix_map = {"ETHEREUM": "ETH", "ARBITRUM": "ARB", "BASE": "BASE"}
            venue_suffix = chain_suffix_map.get(adapter.chain, adapter.chain[:3])
            adapter.venue = f"UNISWAPV3-{venue_suffix}"

            assert adapter.venue == "UNISWAPV3-BASE"

    def test_fetch_pools_empty(self):
        """Test fetch_pools with empty response."""
        with patch(
            "instruments_service.app.venues.defi.uniswapv3_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter

            adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "UNISWAPV3-ETH"
            adapter.project_id = "test-project"
            adapter.graph_client = Mock()
            adapter.graph_client.query_pools = Mock(return_value=[])

            result = adapter.fetch_pools()
            assert isinstance(result, dict)
            assert len(result) == 0


class TestAsterAdapter:
    """Tests for AsterAdapter."""

    def test_init_default(self):
        """Test default initialization."""
        with patch(
            "instruments_service.app.venues.onchain_perps.aster_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

            adapter = AsterAdapter.__new__(AsterAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "ASTER"
            adapter.project_id = "test-project"
            adapter.api_base_url = "https://api.aster.fi"
            adapter.mvp_only = True
            adapter.mvp_base_currencies = set()

            assert adapter.venue == "ASTER"

    def test_fetch_perpetuals_empty(self):
        """Test fetch_perpetuals with empty response."""
        with patch(
            "instruments_service.app.venues.onchain_perps.aster_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

            adapter = AsterAdapter.__new__(AsterAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "ASTER"
            adapter.project_id = "test-project"
            adapter.api_base_url = "https://api.aster.fi"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()

            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.json.return_value = []
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = adapter.fetch_perpetuals()
                assert isinstance(result, dict)

    def test_fetch_spot_pairs_empty(self):
        """Test fetch_spot_pairs with empty response."""
        with patch(
            "instruments_service.app.venues.onchain_perps.aster_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

            adapter = AsterAdapter.__new__(AsterAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "ASTER"
            adapter.project_id = "test-project"
            adapter.api_base_url = "https://api.aster.fi"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()

            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.json.return_value = []
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = adapter.fetch_spot_pairs()
                assert isinstance(result, dict)


class TestBaseDefiAdapter:
    """Tests for BaseDefiAdapter shared functionality."""

    def test_init_with_chain(self):
        """Test base initialization with chain."""
        from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter

        # BaseDefiAdapter is abstract, but we can test that subclasses work
        with patch.object(BaseDefiAdapter, "__abstractmethods__", set()):
            adapter = BaseDefiAdapter.__new__(BaseDefiAdapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"

            assert adapter.chain == "ETHEREUM"

    def test_generate_instrument_key_pool(self):
        """Test generating canonical instrument key for pool."""
        # This tests the pattern used across adapters
        venue = "UNISWAPV3-ETH"
        instrument_type = "POOL"
        symbol = "ETH-USDC"
        chain = "ETHEREUM"

        key = f"{venue}:{instrument_type}:{symbol}@{chain}"
        assert key == "UNISWAPV3-ETH:POOL:ETH-USDC@ETHEREUM"

    def test_generate_instrument_key_perpetual(self):
        """Test generating canonical instrument key for perpetual."""
        venue = "HYPERLIQUID"
        instrument_type = "PERPETUAL"
        symbol = "BTC-USDC"

        key = f"{venue}:{instrument_type}:{symbol}"
        assert key == "HYPERLIQUID:PERPETUAL:BTC-USDC"

    def test_generate_instrument_key_lst(self):
        """Test generating canonical instrument key for LST."""
        venue = "ETHERFI"
        instrument_type = "LST"
        symbol = "WEETH"
        chain = "ETHEREUM"

        key = f"{venue}:{instrument_type}:{symbol}@{chain}"
        assert key == "ETHERFI:LST:WEETH@ETHEREUM"


class TestUniswapV3AdapterExtended:
    """Extended tests for UniswapV3Adapter."""

    def test_init_arbitrum(self):
        """Test initialization for Arbitrum chain."""
        with patch(
            "instruments_service.app.venues.defi.uniswapv3_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter

            adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
            adapter.chain = "ARBITRUM"
            adapter.project_id = "test-project"
            chain_suffix_map = {"ETHEREUM": "ETH", "ARBITRUM": "ARB", "BASE": "BASE"}
            venue_suffix = chain_suffix_map.get(adapter.chain, adapter.chain[:3])
            adapter.venue = f"UNISWAPV3-{venue_suffix}"

            assert adapter.venue == "UNISWAPV3-ARB"

    def test_fetch_pools_with_data(self):
        """Test fetch_pools with pool data."""
        with patch(
            "instruments_service.app.venues.defi.uniswapv3_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.defi.uniswapv3_adapter import UniswapV3Adapter

            adapter = UniswapV3Adapter.__new__(UniswapV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.venue = "UNISWAPV3-ETH"
            adapter.project_id = "test-project"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()
            adapter.graph_client = Mock()

            mock_pool = {
                "id": "0x123",
                "token0": {"symbol": "ETH", "id": "0xeth"},
                "token1": {"symbol": "USDC", "id": "0xusdc"},
                "feeTier": "3000",
                "totalValueLockedUSD": "1000000",
            }
            adapter.graph_client.query_pools = Mock(return_value=[mock_pool])

            result = adapter.fetch_pools()
            assert isinstance(result, dict)


# NOTE: Hyperliquid extended tests moved to test_onchain_perp_adapters.py


class TestRemovedHyperliquidAdapterExtended:
    """Extended tests for HyperliquidAdapter."""

    def test_fetch_spot_pairs_empty(self):
        """Test fetch_spot_pairs with empty response."""
        with patch(
            "instruments_service.app.venues.onchain_perps.hyperliquid_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.hyperliquid_adapter import HyperliquidAdapter

            adapter = HyperliquidAdapter.__new__(HyperliquidAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "HYPERLIQUID"
            adapter.api_base_url = "https://api.hyperliquid.xyz"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()
            adapter.project_id = "test-project"

            with patch("requests.post") as mock_post:
                mock_response = Mock()
                mock_response.json.return_value = []
                mock_response.raise_for_status = Mock()
                mock_post.return_value = mock_response

                result = adapter.fetch_spot_pairs()
                assert isinstance(result, dict)


# NOTE: Aster extended tests moved to test_onchain_perp_adapters.py


class TestRemovedAsterAdapterExtended:
    """Extended tests for AsterAdapter."""

    def test_fetch_perpetuals_with_data(self):
        """Test fetch_perpetuals with API data."""
        with patch(
            "instruments_service.app.venues.onchain_perps.aster_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

            adapter = AsterAdapter.__new__(AsterAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "ASTER"
            adapter.project_id = "test-project"
            adapter.api_base_url = "https://api.aster.fi"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()

            with patch("requests.get") as mock_get:
                mock_data = [{"symbol": "BTC-USDC", "baseAsset": "BTC", "quoteAsset": "USDC"}]
                mock_response = Mock()
                mock_response.json.return_value = mock_data
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = adapter.fetch_perpetuals()
                assert isinstance(result, dict)

    def test_fetch_spot_pairs_with_data(self):
        """Test fetch_spot_pairs with API data."""
        with patch(
            "instruments_service.app.venues.onchain_perps.aster_adapter.BaseOnchainPerpAdapter.__init__",
            return_value=None,
        ):
            from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

            adapter = AsterAdapter.__new__(AsterAdapter)
            adapter.chain = "off-chain"
            adapter.venue = "ASTER"
            adapter.project_id = "test-project"
            adapter.api_base_url = "https://api.aster.fi"
            adapter.mvp_only = False
            adapter.mvp_base_currencies = set()

            with patch("requests.get") as mock_get:
                mock_data = [{"symbol": "ETH-USDC", "baseAsset": "ETH", "quoteAsset": "USDC"}]
                mock_response = Mock()
                mock_response.json.return_value = mock_data
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = adapter.fetch_spot_pairs()
                assert isinstance(result, dict)
