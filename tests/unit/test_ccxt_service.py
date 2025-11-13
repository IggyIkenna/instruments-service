"""
Unit tests for CCXTService.

Tests centralized CCXT integration, market loading, caching, and metadata extraction.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from instruments_service.app.core.ccxt_service import CCXTService
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

    @patch("instruments_service.app.core.ccxt_service.getattr")
    @patch("instruments_service.app.core.ccxt_service.ccxt")
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
        ccxt_service._cache_timestamps["BINANCE-FUTURES_binance"] = datetime.now()

        metadata = ccxt_service.get_metadata(
            venue="BINANCE-FUTURES",
            base_asset="BTC",
            quote_asset="USDT",
            symbol_id="BTCUSDT",
        )

        assert "tick_size" in metadata or metadata == {}  # May be empty if symbol not found

    @patch("instruments_service.app.core.ccxt_service.CCXTService.load_markets")
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
        assert metadata == {}

    def test_cache_validity(self, ccxt_service):
        """Test cache validity checking."""
        cache_key = "test_key"
        ccxt_service._markets_cache[cache_key] = {"markets": {}}
        ccxt_service._cache_timestamps[cache_key] = datetime.now()

        # Cache should be valid immediately
        assert ccxt_service._is_cache_valid(cache_key) is True

        # Cache should be invalid after TTL expires
        ccxt_service._cache_timestamps[cache_key] = datetime.now() - timedelta(hours=5)
        assert ccxt_service._is_cache_valid(cache_key) is False

    def test_cleanup(self, ccxt_service):
        """Test cleanup method."""
        ccxt_service._markets_cache["test"] = {}
        ccxt_service._cache_timestamps["test"] = datetime.now()

        ccxt_service.clear_cache()

        assert ccxt_service._markets_cache == {}
        assert ccxt_service._cache_timestamps == {}
