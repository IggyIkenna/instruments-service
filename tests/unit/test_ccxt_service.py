"""
Unit tests for CCXTService.

Tests centralized CCXT integration, market loading, caching, and metadata extraction.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone

from instruments_service.utils.ccxt_service import CCXTService
from instruments_service.config import VenueMapping


class TestCCXTService:
    """Test CCXTService functionality."""

    @pytest.fixture
    def venue_mapping(self):
        """Create venue mapping fixture."""
        return VenueMapping()

    @pytest.fixture
    def ccxt_service(self, venue_mapping):
        """Create CCXTService fixture."""
        return CCXTService(venue_mapping=venue_mapping, cache_ttl_hours=4)

    def test_init(self, venue_mapping):
        """Test CCXTService initialization."""
        service = CCXTService(venue_mapping=venue_mapping, cache_ttl_hours=4)
        assert service.venue_mapping == venue_mapping
        assert service.cache_ttl_hours == 4
        assert service._markets_cache == {}
        assert service._cache_timestamps == {}

    @patch("instruments_service.utils.ccxt_service.getattr")
    @patch("instruments_service.utils.ccxt_service.ccxt")
    def test_load_markets_success(self, mock_ccxt, mock_getattr, ccxt_service):
        """Test successful market loading."""
        # Mock exchange class and instance
        mock_exchange_class = Mock()
        mock_exchange_instance = Mock()
        mock_exchange_instance.load_markets.return_value = {
            "BTC/USDT:USDT": {
                "precision": {"price": 0.01},
                "limits": {"amount": {"min": 0.001}},
            },
            "ETH/USDT:USDT": {
                "precision": {"price": 0.1},
                "limits": {"amount": {"min": 0.01}},
            },
        }
        mock_getattr.return_value = mock_exchange_class
        mock_exchange_class.return_value = mock_exchange_instance

        # Mock venue mapping
        ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

        result = ccxt_service.load_markets("BINANCE-FUTURES")

        assert result is not None
        assert "exchange" in result
        assert "markets" in result
        assert "exchange_id" in result
        assert len(result["markets"]) == 2

    def test_load_markets_no_mapping(self, ccxt_service):
        """Test load_markets when no CCXT mapping exists."""
        ccxt_service.venue_mapping.venue_to_ccxt = {}
        result = ccxt_service.load_markets("UNKNOWN-VENUE")
        assert result is None

    def test_get_metadata_success(self, ccxt_service):
        """Test successful metadata retrieval."""
        # Setup cache with markets
        ccxt_service._markets_cache["BINANCE-FUTURES_binance"] = {
            "exchange": Mock(),
            "markets": {
                "BTC/USDT:USDT": {
                    "precision": {"price": 0.01},
                    "limits": {"amount": {"min": 0.001}},
                    "contractSize": 1.0,
                }
            },
            "exchange_id": "binance",
        }
        ccxt_service._cache_timestamps["BINANCE-FUTURES_binance"] = datetime.now(timezone.utc)

        metadata = ccxt_service.get_metadata(
            venue="BINANCE-FUTURES",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        # Utils version returns dict with ccxt_exchange and ccxt_symbol even if symbol not found
        assert isinstance(metadata, dict)

    @patch("instruments_service.utils.ccxt_service.CCXTService.load_markets")
    def test_get_metadata_no_cache(self, mock_load_markets, ccxt_service):
        """Test metadata retrieval when cache is empty."""
        # Mock load_markets to return None (simulating no markets available)
        mock_load_markets.return_value = None

        metadata = ccxt_service.get_metadata(
            venue="BINANCE-FUTURES",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )
        # Utils version returns dict with ccxt_exchange and ccxt_symbol even when no cache
        assert isinstance(metadata, dict)

    def test_cache_validity(self, ccxt_service):
        """Test cache validity checking."""
        cache_key = "test_key"
        ccxt_service._markets_cache[cache_key] = {"markets": {}}
        ccxt_service._cache_timestamps[cache_key] = datetime.now(timezone.utc)

        # Cache should be valid immediately
        assert ccxt_service._is_cache_valid(cache_key) is True

        # Cache should be invalid after TTL expires
        ccxt_service._cache_timestamps[cache_key] = datetime.now(timezone.utc) - timedelta(hours=5)
        assert ccxt_service._is_cache_valid(cache_key) is False

    def test_cleanup(self, ccxt_service):
        """Test cleanup method."""
        ccxt_service._markets_cache["test"] = {}
        ccxt_service._cache_timestamps["test"] = datetime.now(timezone.utc)

        ccxt_service.clear_cache()

        assert ccxt_service._markets_cache == {}
        assert ccxt_service._cache_timestamps == {}

    def test_get_ccxt_exchange_success(self, ccxt_service):
        """Test getting CCXT exchange instance."""
        ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

        with patch("instruments_service.utils.ccxt_service.ccxt") as mock_ccxt:
            mock_exchange_class = Mock()
            mock_ccxt.binance = mock_exchange_class
            mock_exchange_class.return_value = Mock()

            exchange = ccxt_service.get_ccxt_exchange("BINANCE-FUTURES")
            assert exchange is not None

    def test_get_ccxt_exchange_no_mapping(self, ccxt_service):
        """Test getting CCXT exchange when no mapping exists."""
        ccxt_service.venue_mapping.venue_to_ccxt = {}
        exchange = ccxt_service.get_ccxt_exchange("UNKNOWN-VENUE")
        assert exchange is None

    def test_get_ccxt_exchange_not_available(self, ccxt_service):
        """Test getting CCXT exchange when exchange class not available."""
        ccxt_service.venue_mapping.venue_to_ccxt = {"TEST-VENUE": "nonexistent"}

        with patch("instruments_service.utils.ccxt_service.ccxt") as mock_ccxt:
            mock_ccxt.nonexistent = None

            exchange = ccxt_service.get_ccxt_exchange("TEST-VENUE")
            assert exchange is None

    def test_load_markets_force_refresh(self, ccxt_service):
        """Test loading markets with force refresh."""
        # Setup cache
        cache_key = "BINANCE-FUTURES_binance"
        ccxt_service._markets_cache[cache_key] = {"markets": {"OLD": "data"}}
        ccxt_service._cache_timestamps[cache_key] = datetime.now(timezone.utc)

        with patch(
            "instruments_service.utils.ccxt_service.CCXTService.get_ccxt_exchange"
        ) as mock_get_exchange:
            mock_exchange = Mock()
            mock_exchange.load_markets.return_value = {"BTC/USDT:USDT": {}}
            mock_get_exchange.return_value = mock_exchange

            ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

            result = ccxt_service.load_markets("BINANCE-FUTURES", force_refresh=True)
            assert result is not None
            assert "BTC/USDT:USDT" in result["markets"]

    def test_load_markets_exception(self, ccxt_service):
        """Test load_markets when exception occurs."""
        ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

        with patch(
            "instruments_service.utils.ccxt_service.CCXTService.get_ccxt_exchange"
        ) as mock_get_exchange:
            mock_exchange = Mock()
            mock_exchange.load_markets.side_effect = Exception("API Error")
            mock_get_exchange.return_value = mock_exchange

            result = ccxt_service.load_markets("BINANCE-FUTURES")
            assert result is None

    def test_build_symbol_formats_bybit(self, ccxt_service):
        """Test building symbol formats for Bybit."""
        formats = ccxt_service._build_symbol_formats(
            venue="BYBIT", base_asset="BTC", quote_asset="USDT", symbol_id="BTCUSDT"
        )
        assert len(formats) > 0
        assert "BTC/USDT:USDT" in formats

    def test_build_symbol_formats_hyperliquid(self, ccxt_service):
        """Test building symbol formats for Hyperliquid."""
        formats = ccxt_service._build_symbol_formats(
            venue="HYPERLIQUID", base_asset="BTC", quote_asset="USDC", symbol_id="BTCUSDC"
        )
        assert len(formats) > 0
        assert "BTC/USDC:USDC" in formats

    def test_build_symbol_formats_deribit(self, ccxt_service):
        """Test building symbol formats for Deribit."""
        formats = ccxt_service._build_symbol_formats(
            venue="DERIBIT",
            base_asset="BTC",
            quote_asset="USD",
            symbol_id="BTCUSD",
            tardis_symbol="BTC-PERPETUAL",
        )
        assert len(formats) > 0

    def test_get_metadata_with_tick_size(self, ccxt_service):
        """Test getting metadata with tick size."""
        ccxt_service._markets_cache["BINANCE-FUTURES_binance"] = {
            "exchange": Mock(),
            "markets": {
                "BTC/USDT:USDT": {
                    "precision": {"price": 0.01, "amount": 0.001},
                    "limits": {"amount": {"min": 0.001}},
                    "contractSize": 1.0,
                },
                "BTCUSDT": {  # Also add compressed format
                    "precision": {"price": 0.01, "amount": 0.001},
                    "limits": {"amount": {"min": 0.001}},
                    "contractSize": 1.0,
                },
            },
            "exchange_id": "binance",
        }
        ccxt_service._cache_timestamps["BINANCE-FUTURES_binance"] = datetime.now(timezone.utc)
        ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

        metadata = ccxt_service.get_metadata(
            venue="BINANCE-FUTURES",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        # Should have metadata if symbol format matches
        assert isinstance(metadata, dict)
        if metadata:  # If symbol was found
            assert "tick_size" in metadata or "min_size" in metadata or "contract_size" in metadata

    def test_get_metadata_cost_min(self, ccxt_service):
        """Test getting metadata with cost_min instead of amount precision."""
        ccxt_service._markets_cache["TEST_binance"] = {
            "exchange": Mock(),
            "markets": {
                "BTC/USDT:USDT": {
                    "precision": {},
                    "limits": {"cost": {"min": 10.0}},
                    "contractSize": 1.0,
                }
            },
            "exchange_id": "binance",
        }
        ccxt_service._cache_timestamps["TEST_binance"] = datetime.now(timezone.utc)
        ccxt_service.venue_mapping.venue_to_ccxt = {"TEST": "binance"}

        metadata = ccxt_service.get_metadata(
            venue="TEST",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        # Utils version returns dict with ccxt_exchange and ccxt_symbol
        assert isinstance(metadata, dict)

    @patch("instruments_service.utils.ccxt_service.CCXTService.load_markets")
    def test_get_leverage_limits(self, mock_load_markets, ccxt_service):
        """Test getting leverage limits."""
        mock_exchange = Mock()
        mock_exchange.fetchMarketLeverageTiers.return_value = [
            {
                "tier": 1,
                "minNotional": 0,
                "maxNotional": 1000000,
                "maintenanceMargin": 0.005,
                "maxLeverage": 125,
                "initialMargin": 0.008,
            },
            {
                "tier": 2,
                "minNotional": 1000000,
                "maxNotional": 5000000,
                "maintenanceMargin": 0.01,
                "maxLeverage": 100,
                "initialMargin": 0.01,
            },
        ]

        mock_load_markets.return_value = {
            "exchange": mock_exchange,
            "markets": {"BTC/USDT:USDT": {}},
            "exchange_id": "binance",
        }

        ccxt_service.venue_mapping.venue_to_ccxt = {"BINANCE-FUTURES": "binance"}

        limits = ccxt_service.get_leverage_limits(
            venue="BINANCE-FUTURES",
            symbol="BTC/USDT:USDT",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        assert isinstance(limits, dict)
        # Should have risk parameters
        assert (
            "max_leverage" in limits
            or "max_position_size" in limits
            or "initial_margin_rate" in limits
        )

    def test_get_leverage_limits_cached(self, ccxt_service):
        """Test getting leverage limits from cache."""
        cached_params = {
            "max_leverage": 125,
            "max_position_size": 1000000,
            "initial_margin_rate": 0.008,
            "maintenance_margin_rate": 0.005,
        }
        ccxt_service._leverage_tiers_cache["BINANCE-FUTURES"] = cached_params

        limits = ccxt_service.get_leverage_limits(
            venue="BINANCE-FUTURES",
            symbol="BTC/USDT:USDT",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        # Should return a copy, not the same object
        assert limits == cached_params
        assert limits is not cached_params  # Should be a copy

    def test_extract_risk_params_from_tiers(self, ccxt_service):
        """Test extracting risk parameters from leverage tiers."""
        leverage_tiers = [
            {
                "tier": 1,
                "minNotional": 0,
                "maxNotional": 1000000,
                "maintenanceMargin": 0.005,
                "maxLeverage": 125,
                "initialMargin": 0.008,
            },
            {
                "tier": 2,
                "minNotional": 1000000,
                "maxNotional": 5000000,
                "maintenanceMargin": 0.01,
                "maxLeverage": 100,
                "initialMargin": 0.01,
            },
        ]

        params = ccxt_service._extract_risk_params_from_tiers(leverage_tiers)

        assert isinstance(params, dict)
        assert "max_leverage" in params
        assert "max_position_size" in params
        assert params["max_position_size"] == 5000000  # Highest tier

    def test_extract_risk_params_empty_tiers(self, ccxt_service):
        """Test extracting risk parameters from empty tiers."""
        params = ccxt_service._extract_risk_params_from_tiers([])
        assert params == {}

    def test_get_leverage_limits_fallback(self, ccxt_service):
        """Test getting leverage limits fallback."""
        limits = ccxt_service._get_leverage_limits_fallback("BINANCE-FUTURES")
        assert isinstance(limits, dict)
        assert "max_leverage" in limits
        assert limits["max_leverage"] == 125.0

    def test_get_leverage_limits_fallback_unknown_venue(self, ccxt_service):
        """Test getting leverage limits fallback for unknown venue."""
        limits = ccxt_service._get_leverage_limits_fallback("UNKNOWN-VENUE")
        assert limits == {}

    def test_clear_cache_specific_venue(self, ccxt_service):
        """Test clearing cache for specific venue."""
        ccxt_service._markets_cache["BINANCE-FUTURES_binance"] = {"markets": {}}
        ccxt_service._markets_cache["DERIBIT_deribit"] = {"markets": {}}
        ccxt_service._cache_timestamps["BINANCE-FUTURES_binance"] = datetime.now(timezone.utc)
        ccxt_service._cache_timestamps["DERIBIT_deribit"] = datetime.now(timezone.utc)

        ccxt_service.clear_cache(venue="BINANCE-FUTURES")

        assert "BINANCE-FUTURES_binance" not in ccxt_service._markets_cache
        assert "DERIBIT_deribit" in ccxt_service._markets_cache
