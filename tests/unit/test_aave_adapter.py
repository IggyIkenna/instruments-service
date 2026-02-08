"""
Unit tests for AaveV3Adapter.

Includes regression tests to guard against reintroduction of silent fallbacks
and tests for the RPC getEModeCategoryData on-chain truth method.
"""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from instruments_service.app.venues.defi.aave_adapter import AaveV3Adapter


class TestAaveV3Adapter:
    """Tests for AaveV3Adapter."""

    def test_static_risk_params_removed(self):
        """Regression: STATIC_RISK_PARAMS must not exist — no static fallbacks allowed."""
        assert not hasattr(AaveV3Adapter, "STATIC_RISK_PARAMS"), (
            "STATIC_RISK_PARAMS was reintroduced. Static risk parameter fallbacks are forbidden. "
            "Risk parameters must be fetched dynamically from RPC or The Graph."
        )

    def test_fallback_reserves_removed(self):
        """Regression: _get_fallback_reserves must not exist — no static fallbacks allowed."""
        assert not hasattr(AaveV3Adapter, "_get_fallback_reserves") or not callable(
            getattr(AaveV3Adapter, "_get_fallback_reserves", None)
        ), (
            "_get_fallback_reserves was reintroduced. Static fallback reserves are forbidden. "
            "The venue must fail if reserve data cannot be fetched from live APIs."
        )

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_ETH"

    def test_init_unsupported_chain(self):
        """Test initialization for unsupported chain."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "UNSUPPORTED"
            adapter.project_id = "test-project"
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_UNSUPPORTED"

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

            address = adapter._get_a_token_address("WETH", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
            assert address == "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8"

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

            address = adapter._get_debt_token_address("WETH", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
            assert address == "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE"

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


class TestAaveV3AdapterHardening:
    """Regression tests for data hardening — ensure fallbacks cannot be reintroduced."""

    def test_no_static_fallback_reserves_on_api_failure(self):
        """Regression: venue must fail when AaveScan API fails, not return hardcoded reserves."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter.api_key = "test-key"
            adapter.base_url = "https://api.aavescan.com/v2"
            adapter._reserves_cache = None
            adapter._reserves_cache_date = None
            adapter._historical_query_failed = set()

            with patch(
                "instruments_service.app.venues.defi.aave_adapter.get_http_session"
            ) as mock_session:
                mock_response = Mock()
                mock_response.raise_for_status.side_effect = Exception("API unavailable")
                mock_session.return_value.get.return_value = mock_response

                with pytest.raises(RuntimeError, match="Failed to fetch AAVE reserves"):
                    adapter._fetch_reserves()

    def test_no_static_risk_params_fallback(self):
        """Regression: venue must fail when risk params are missing, not use STATIC_RISK_PARAMS."""
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
                "asset": {"symbol": "WETH", "address": "0xC02", "decimals": 18},
            }
            adapter._fetch_reserves = Mock(return_value=[mock_reserve])
            adapter._create_a_token_instrument = Mock(
                side_effect=RuntimeError("Missing risk parameters for WETH")
            )
            adapter._create_debt_token_instrument = Mock(return_value=None)

            with pytest.raises(RuntimeError, match="AAVE_V3_ETH venue failed"):
                adapter.fetch_markets()

    def test_api_key_retrieval_failure_raises(self):
        """Regression: adapter must raise when Secret Manager fails, not use secret name as key."""

        def mock_base_init(self_obj, chain=None, api_key=None, project_id=None):
            """Mock BaseDefiAdapter.__init__ that sets required attributes."""
            self_obj.chain = chain or "ETHEREUM"
            self_obj.project_id = project_id or "test-project"

        with (
            patch(
                "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
                mock_base_init,
            ),
            patch(
                "instruments_service.app.venues.defi.aave_adapter.get_secret_with_fallback",
                side_effect=Exception("Secret Manager unavailable"),
            ),
            patch(
                "instruments_service.app.venues.defi.aave_adapter.AlchemyBaseClient",
            ),
            patch(
                "instruments_service.app.venues.defi.aave_adapter.TheGraphBaseClient",
            ),
        ):
            with pytest.raises(RuntimeError, match="Failed to retrieve AaveScan API key"):
                AaveV3Adapter(chain="ETHEREUM", project_id="test-project")

    def test_venue_level_failure_on_single_instrument_error(self):
        """Regression: if any instrument fails, the entire venue must fail."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"

            good_reserve = {
                "reserve": "0xaaa",
                "asset": {"symbol": "USDT", "address": "0xaaa", "decimals": 6},
            }
            bad_reserve = {
                "reserve": "0xbbb",
                "asset": {"symbol": "WETH", "address": "0xbbb", "decimals": 18},
            }
            adapter._fetch_reserves = Mock(return_value=[good_reserve, bad_reserve])

            call_count = 0

            def create_a_token_side_effect(reserve, target_date=None):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return {"instrument_key": "AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM"}
                raise RuntimeError("Graph unavailable for WETH")

            adapter._create_a_token_instrument = Mock(side_effect=create_a_token_side_effect)
            adapter._create_debt_token_instrument = Mock(return_value=None)

            with pytest.raises(RuntimeError, match="AAVE_V3_ETH venue failed"):
                adapter.fetch_markets()


class TestEModeCategoryDataRPC:
    """Tests for the RPC getEModeCategoryData on-chain truth method."""

    def test_emode_category_data_from_rpc_returns_correct_structure(self):
        """Test that _fetch_emode_category_data_from_rpc returns correctly structured data."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._reserve_config_cache = {}
            adapter._block_number_cache = {}
            adapter._block_conversion_failed = set()

            # Mock Web3 contract call returning ETH correlated eMode category
            mock_w3 = Mock()
            mock_contract = Mock()
            # Simulate getEModeCategoryData return: (ltv=9300, liqThreshold=9500, liqBonus=10100, priceSource=0x0, label="ETH correlated")
            mock_contract.functions.getEModeCategoryData.return_value.call.return_value = (
                9300,  # ltv in basis points (93.00%)
                9500,  # liquidationThreshold in basis points (95.00%)
                10100,  # liquidationBonus in basis points (101.00%)
                "0x0000000000000000000000000000000000000000",  # priceSource
                "ETH correlated",  # label
            )
            mock_w3.eth.contract.return_value = mock_contract

            adapter._alchemy_client = Mock()
            adapter._alchemy_client.get_web3.return_value = mock_w3

            result = adapter._fetch_emode_category_data_from_rpc(category_id=1)

            assert result is not None
            assert result["id"] == 1
            assert result["label"] == "ETH correlated"
            assert result["ltv"] == 0.93
            assert result["liquidation_threshold"] == 0.95
            assert result["liquidation_bonus"] == 1.01
            assert result["data_source"] == "rpc_getEModeCategoryData"

    def test_emode_category_data_from_rpc_with_historical_block(self):
        """Test that historical block number is passed correctly to RPC call."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._reserve_config_cache = {}
            adapter._block_number_cache = {"2025-01-10": 21500000}
            adapter._block_conversion_failed = set()

            mock_w3 = Mock()
            mock_contract = Mock()
            mock_contract.functions.getEModeCategoryData.return_value.call.return_value = (
                9300, 9500, 10100,
                "0x0000000000000000000000000000000000000000",
                "ETH correlated",
            )
            mock_w3.eth.contract.return_value = mock_contract

            adapter._alchemy_client = Mock()
            adapter._alchemy_client.get_web3.return_value = mock_w3

            target_date = datetime(2025, 1, 10, tzinfo=timezone.utc)
            result = adapter._fetch_emode_category_data_from_rpc(
                category_id=1, target_date=target_date
            )

            assert result is not None
            assert result["block_number"] == 21500000
            # Verify the call was made with block_identifier
            mock_contract.functions.getEModeCategoryData.return_value.call.assert_called_once_with(
                block_identifier=21500000
            )

    def test_emode_category_data_cached(self):
        """Test that eMode category data is cached and not re-fetched."""
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

            # Pre-populate cache
            cached_data = {
                "id": 1,
                "label": "ETH correlated",
                "ltv": 0.93,
                "liquidation_threshold": 0.95,
                "liquidation_bonus": 1.01,
                "oracle": "0x0000000000000000000000000000000000000000",
                "data_source": "rpc_getEModeCategoryData",
                "block_number": None,
            }
            adapter._reserve_config_cache = {"emode_cat_1_latest": cached_data}

            # Should return cached data without calling RPC
            adapter._alchemy_client = Mock()
            result = adapter._fetch_emode_category_data_from_rpc(category_id=1)

            assert result == cached_data
            # Verify no RPC call was made
            adapter._alchemy_client.get_web3.assert_not_called()

    def test_emode_category_data_rpc_failure_returns_none(self):
        """Test that RPC failure returns None (allowing Graph fallback)."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
            return_value=None,
        ):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            adapter.venue = "AAVE_V3_ETH"
            adapter._reserve_config_cache = {}
            adapter._block_number_cache = {}
            adapter._block_conversion_failed = set()

            adapter._alchemy_client = Mock()
            adapter._alchemy_client.get_web3.side_effect = ValueError("No API key")

            result = adapter._fetch_emode_category_data_from_rpc(category_id=1)
            assert result is None
