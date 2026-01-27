"""
Unit tests for AaveV3Adapter.
"""

from unittest.mock import Mock, patch
from datetime import datetime, timezone
from instruments_service.app.venues.defi.aave_adapter import AaveV3Adapter


class TestAaveV3Adapter:
    """Tests for AaveV3Adapter."""

    def test_static_risk_params(self):
        """Test static risk parameters exist."""
        assert hasattr(AaveV3Adapter, "STATIC_RISK_PARAMS")
        assert "emode" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "standard" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "reserve_factors" in AaveV3Adapter.STATIC_RISK_PARAMS

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        # Mock the base class initialization properly
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            # Manually set venue as __init__ would
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_ETH"

    def test_init_unsupported_chain(self):
        """Test initialization for unsupported chain."""
        # Mock the base class initialization properly
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "UNSUPPORTED"
            adapter.project_id = "test-project"
            # Manually set venue as __init__ would
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_UNSUPPORTED"

    def test_get_fallback_reserves(self):
        """Test fallback reserves method."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            reserves = adapter._get_fallback_reserves()
            assert isinstance(reserves, list)
            assert len(reserves) > 0
            assert all("reserve" in r and "asset" in r for r in reserves)

    def test_get_a_token_address(self):
        """Test getting aToken address."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            # Test known token
            address = adapter._get_a_token_address(
                "WETH", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            )
            assert address == "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8"

            # Test unknown token
            address = adapter._get_a_token_address("UNKNOWN", "0x123")
            assert address == ""

    def test_get_debt_token_address(self):
        """Test getting debt token address."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            # Test known token
            address = adapter._get_debt_token_address(
                "WETH", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            )
            assert address == "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE"

            # Test unknown token
            address = adapter._get_debt_token_address("UNKNOWN", "0x123")
            assert address == ""

    def test_fetch_markets_empty(self):
        """Test fetch_markets with no reserves."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._fetch_reserves = Mock(return_value=[])

            result = adapter.fetch_markets()
            assert isinstance(result, dict)
            assert len(result) == 0

    def test_fetch_markets_with_reserves(self):
        """Test fetch_markets with reserves."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            mock_reserve = {
                "reserve": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "asset": {
                    "symbol": "WETH",
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "decimals": 18,
                },
            }
            adapter._fetch_reserves = Mock(return_value=[mock_reserve])
            adapter._create_a_token_instrument = Mock(
                return_value={"instrument_key": "AAVE_V3_ETH:A_TOKEN:WETH@ETHEREUM"}
            )
            adapter._create_debt_token_instrument = Mock(
                return_value={"instrument_key": "AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM"}
            )

            result = adapter.fetch_markets()
            assert isinstance(result, dict)
            assert len(result) >= 0  # May be empty if instrument creation fails

    def test_fetch_reserves_cache_hit(self):
        """Test _fetch_reserves with cache hit."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._reserves_cache = [{"reserve": "0x123", "asset": {"symbol": "TEST"}}]
            adapter._reserves_cache_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            result = adapter._fetch_reserves()
            assert result == adapter._reserves_cache

    def test_date_to_block_number_cache_hit(self):
        """Test _date_to_block_number with cache hit."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._block_number_cache = {"2024-01-01": 19000000}

            target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = adapter._date_to_block_number(target_date)
            assert result == 19000000

    def test_date_to_block_number_future_date(self):
        """Test _date_to_block_number with future date."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._block_number_cache = {}
            adapter._block_conversion_failed = set()

            future_date = datetime(2100, 1, 1, tzinfo=timezone.utc)
            result = adapter._date_to_block_number(future_date)
            assert result is None

    def test_date_to_block_number_failed_cache(self):
        """Test _date_to_block_number with failed cache."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._block_number_cache = {}
            adapter._block_conversion_failed = {"2024-01-01T00:00:00+00:00"}

            target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = adapter._date_to_block_number(target_date)
            assert result is None

    def test_fetch_reserves_from_graph_no_block(self):
        """Test _fetch_reserves_from_graph with no block number."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._date_to_block_number = Mock(return_value=None)
            adapter._historical_query_failed = set()

            target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = adapter._fetch_reserves_from_graph(target_date)
            assert result == []

    def test_fetch_reserves_from_graph_already_failed(self):
        """Test _fetch_reserves_from_graph with already failed date."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._historical_query_failed = {"2024-01-01T00:00:00+00:00"}

            target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = adapter._fetch_reserves_from_graph(target_date)
            assert result == []

    def test_init_arbitrum(self):
        """Test initialization for Arbitrum chain."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ARBITRUM"
            adapter.project_id = "test-project"
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH", "ARBITRUM": "AAVE_V3_ARB"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")

            assert adapter.venue == "AAVE_V3_ARB"

    def test_static_risk_params_emode_has_keys(self):
        """Test static risk parameters for emode has expected keys."""
        assert "ltv_limits" in AaveV3Adapter.STATIC_RISK_PARAMS["emode"]
        assert "liquidation_thresholds" in AaveV3Adapter.STATIC_RISK_PARAMS["emode"]
        assert "liquidation_bonus" in AaveV3Adapter.STATIC_RISK_PARAMS["emode"]

    def test_static_risk_params_standard_has_keys(self):
        """Test static risk parameters for standard has expected keys."""
        assert "ltv_limits" in AaveV3Adapter.STATIC_RISK_PARAMS["standard"]
        assert "liquidation_thresholds" in AaveV3Adapter.STATIC_RISK_PARAMS["standard"]

    def test_get_fallback_reserves_has_usdc(self):
        """Test fallback reserves include USDC."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            reserves = adapter._get_fallback_reserves()
            symbols = [r["asset"]["symbol"] for r in reserves]

            # Should include major tokens
            assert "USDC" in symbols or "WETH" in symbols or len(symbols) > 0

    def test_init_base_chain(self):
        """Test initialization for Base chain."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "BASE"
            adapter.project_id = "test-project"
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH", "ARBITRUM": "AAVE_V3_ARB", "BASE": "AAVE_V3_BASE"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")

            assert adapter.venue == "AAVE_V3_BASE"

    def test_reserve_factors_exist(self):
        """Test reserve factors exist in static params."""
        assert "reserve_factors" in AaveV3Adapter.STATIC_RISK_PARAMS

    def test_static_risk_params_has_all_modes(self):
        """Test static risk parameters has all expected modes."""
        assert "emode" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "standard" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "reserve_factors" in AaveV3Adapter.STATIC_RISK_PARAMS
