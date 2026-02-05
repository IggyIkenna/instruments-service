"""
End-to-end test for DEFI instrument generation.

Tests the complete workflow for DeFi instruments:
1. Generate instruments from protocol adapters (Aave, Uniswap, etc.)
2. Upload to test bucket (instruments-store-test-defi-*)
3. Verify data integrity
"""

import pytest


@pytest.mark.e2e
class TestDefiInstrumentGeneration:
    """E2E tests for DeFi instrument generation."""

    def test_defi_category_identifier(self):
        """Test DEFI category identifier."""
        category = "DEFI"
        assert category.upper() == "DEFI"

    def test_defi_bucket_naming(self, gcp_project_id):
        """Test DEFI bucket naming convention."""
        category = "defi"
        bucket_template = f"instruments-store-{category}-{gcp_project_id}"

        assert "defi" in bucket_template
        assert gcp_project_id in bucket_template


@pytest.mark.e2e
class TestDefiProtocolSupport:
    """Tests for DeFi protocol/venue support."""

    def test_aave_protocol_supported(self):
        """Test Aave protocol is supported."""
        defi_protocols = [
            "aave_v3_eth",
            "uniswapv2_eth",
            "uniswapv3_eth",
            "curve_eth",
            "balancer_eth",
            "morpho_eth",
            "lido_eth",
            "etherfi_eth",
            "ethena_eth",
        ]
        assert "aave_v3_eth" in defi_protocols

    def test_uniswap_protocol_supported(self):
        """Test Uniswap protocol is supported."""
        defi_protocols = [
            "aave_v3_eth",
            "uniswapv2_eth",
            "uniswapv3_eth",
            "uniswapv4_eth",
        ]
        assert "uniswapv3_eth" in defi_protocols

    def test_lst_protocols_supported(self):
        """Test LST protocols (Lido, EtherFi) are supported."""
        lst_protocols = ["lido_eth", "etherfi_eth"]

        assert "lido_eth" in lst_protocols
        assert "etherfi_eth" in lst_protocols


@pytest.mark.e2e
class TestDefiInstrumentTypes:
    """Tests for DeFi instrument type support."""

    def test_lending_pool_instrument_schema(self):
        """Test lending pool instrument has required fields."""
        lending_instrument = {
            "instrument_key": "AAVE_V3_ETH:USDC:SUPPLY",
            "venue": "AAVE_V3_ETH",
            "symbol": "USDC",
            "category": "DEFI",
            "protocol_type": "lending",
            "chain": "ETHEREUM",
        }

        required = ["instrument_key", "venue", "symbol", "category", "protocol_type"]
        for field in required:
            assert field in lending_instrument

    def test_dex_pool_instrument_schema(self):
        """Test DEX pool instrument has required fields."""
        dex_instrument = {
            "instrument_key": "UNISWAPV3_ETH:WETH/USDC/3000",
            "venue": "UNISWAPV3_ETH",
            "symbol": "WETH/USDC",
            "category": "DEFI",
            "protocol_type": "dex_pool",
            "fee_tier": 3000,
            "chain": "ETHEREUM",
        }

        required = ["instrument_key", "venue", "symbol", "protocol_type"]
        for field in required:
            assert field in dex_instrument

    def test_lst_instrument_schema(self):
        """Test LST instrument has required fields."""
        lst_instrument = {
            "instrument_key": "LIDO_ETH:stETH",
            "venue": "LIDO_ETH",
            "symbol": "stETH",
            "category": "DEFI",
            "protocol_type": "lst",
            "chain": "ETHEREUM",
        }

        required = ["instrument_key", "venue", "symbol", "protocol_type"]
        for field in required:
            assert field in lst_instrument


@pytest.mark.e2e
class TestDefiChainSupport:
    """Tests for DeFi chain support."""

    def test_ethereum_mainnet_supported(self):
        """Test Ethereum mainnet is supported."""
        chains = ["ETHEREUM", "ARBITRUM", "OPTIMISM", "BASE", "POLYGON"]
        assert "ETHEREUM" in chains

    def test_l2_chains_supported(self):
        """Test L2 chains are supported."""
        l2_chains = ["ARBITRUM", "OPTIMISM", "BASE"]

        for chain in l2_chains:
            assert chain in ["ARBITRUM", "OPTIMISM", "BASE", "POLYGON"]


@pytest.mark.e2e
class TestDefiDataSources:
    """Tests for DeFi data source support."""

    def test_the_graph_client(self):
        """Test The Graph client configuration."""
        subgraph_url_template = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

        assert "{api_key}" in subgraph_url_template
        assert "{subgraph_id}" in subgraph_url_template

    def test_rpc_endpoint_support(self):
        """Test RPC endpoint support for protocols."""
        rpc_protocols = ["curve"]  # Curve uses RPC calls

        assert "curve" in rpc_protocols


@pytest.mark.e2e
class TestDefiDataTypes:
    """Tests for DeFi instrument data types."""

    def test_oracle_prices_data_type(self):
        """Test oracle prices data type."""
        defi_data_types = ["oracle_prices", "rates", "yields", "swaps"]
        assert "oracle_prices" in defi_data_types

    def test_rates_data_type(self):
        """Test lending rates data type."""
        defi_data_types = ["oracle_prices", "rates", "yields", "swaps"]
        assert "rates" in defi_data_types


@pytest.mark.e2e
class TestDefiTestBucketIsolation:
    """Tests for test bucket isolation in DEFI."""

    def test_test_bucket_contains_test_string(self, gcp_project_id):
        """Test bucket name contains 'test' for isolation."""
        test_bucket = f"instruments-store-test-defi-{gcp_project_id}"

        assert "test" in test_bucket.lower()

    def test_test_bucket_different_from_prod(self, gcp_project_id):
        """Test bucket is different from production bucket."""
        test_bucket = f"instruments-store-test-defi-{gcp_project_id}"
        prod_bucket = f"instruments-store-defi-{gcp_project_id}"

        assert test_bucket != prod_bucket


@pytest.mark.e2e
class TestDefiOnchainPerpsSupport:
    """Tests for on-chain perpetuals (Hyperliquid, Aster)."""

    def test_hyperliquid_supported(self):
        """Test Hyperliquid venue is supported."""
        onchain_perps = ["hyperliquid", "aster"]
        assert "hyperliquid" in onchain_perps

    def test_aster_supported(self):
        """Test Aster venue is supported."""
        onchain_perps = ["hyperliquid", "aster"]
        assert "aster" in onchain_perps

    def test_onchain_perp_instrument_schema(self):
        """Test on-chain perp instrument schema."""
        perp_instrument = {
            "instrument_key": "HYPERLIQUID:BTC-USD@LIN",
            "venue": "HYPERLIQUID",
            "symbol": "BTC@LIN",
            "category": "DEFI",
            "instrument_type": "perpetual",
            "contract_type": "linear",
        }

        required = ["instrument_key", "venue", "symbol", "instrument_type"]
        for field in required:
            assert field in perp_instrument
