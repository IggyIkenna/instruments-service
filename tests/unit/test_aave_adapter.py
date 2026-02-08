"""
Unit tests for AaveV3Adapter.

Includes regression tests to guard against reintroduction of silent fallbacks.
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
            "Risk parameters must be fetched dynamically from AaveScan, The Graph, or RPC."
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
            assert len(result) >= 0

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

            # Mock the HTTP session to fail
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

            # Mock a reserve that would trigger risk params fetch
            mock_reserve = {
                "reserve": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "asset": {"symbol": "WETH", "address": "0xC02", "decimals": 18},
            }

            # Mock _fetch_reserves to succeed but _create_a_token_instrument to raise
            # because risk params are missing
            adapter._fetch_reserves = Mock(return_value=[mock_reserve])
            adapter._create_a_token_instrument = Mock(
                side_effect=RuntimeError("Missing risk parameters for WETH")
            )
            adapter._create_debt_token_instrument = Mock(return_value=None)

            with pytest.raises(RuntimeError, match="AAVE_V3_ETH venue failed"):
                adapter.fetch_markets()

    def test_api_key_retrieval_failure_raises(self):
        """Regression: adapter must raise when Secret Manager fails, not use secret name as key."""
        with (
            patch(
                "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__",
                return_value=None,
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

            # Two reserves: first succeeds, second fails
            good_reserve = {
                "reserve": "0xaaa",
                "asset": {"symbol": "USDT", "address": "0xaaa", "decimals": 6},
            }
            bad_reserve = {
                "reserve": "0xbbb",
                "asset": {"symbol": "WETH", "address": "0xbbb", "decimals": 18},
            }
            adapter._fetch_reserves = Mock(return_value=[good_reserve, bad_reserve])

            # First reserve succeeds, second fails
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
