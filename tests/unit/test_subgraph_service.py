"""
Unit tests for SubgraphService.

Tests subgraph URL resolution and caching.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from instruments_service.app.core.subgraph_service import SubgraphService


class TestSubgraphService:
    """Test SubgraphService functionality."""

    @pytest.fixture
    def subgraph_service(self):
        """Create SubgraphService fixture."""
        return SubgraphService(cache_ttl_hours=24)

    def test_init(self, subgraph_service):
        """Test SubgraphService initialization."""
        assert subgraph_service.cache_ttl_hours == 24
        assert subgraph_service._subgraph_cache == {}
        assert subgraph_service._cache_timestamps == {}
        assert "uniswap_v2" in subgraph_service._fallback_urls

    def test_get_subgraph_url_uniswap_v2(self, subgraph_service):
        """Test getting Uniswap V2 subgraph URL."""
        url = subgraph_service.get_subgraph_url("uniswap_v2", "ETHEREUM")
        # URL may be a Network gateway URL (with subgraph ID hash) or fallback URL
        # Just verify it's a valid URL string
        assert url is not None
        assert isinstance(url, str)
        assert url.startswith("http")

    def test_get_subgraph_url_uniswap_v3(self, subgraph_service):
        """Test getting Uniswap V3 subgraph URL."""
        url = subgraph_service.get_subgraph_url("uniswap_v3", "ETHEREUM")
        # URL may be a Network gateway URL (with subgraph ID hash) or fallback URL
        # Just verify it's a valid URL string
        assert url is not None
        assert isinstance(url, str)
        assert url.startswith("http")

    def test_get_subgraph_url_curve(self, subgraph_service):
        """Test getting Curve subgraph URL."""
        url = subgraph_service.get_subgraph_url("curve", "ETHEREUM")
        # Curve may return None if subgraph is not available
        # If URL is returned, verify it's a valid URL string
        if url is not None:
            assert isinstance(url, str)
            assert url.startswith("http")

    def test_get_subgraph_url_caching(self, subgraph_service):
        """Test subgraph URL caching."""
        # First call should populate cache
        url1 = subgraph_service.get_subgraph_url("uniswap_v2", "ETHEREUM")
        assert len(subgraph_service._subgraph_cache) > 0

        # Second call should use cache
        url2 = subgraph_service.get_subgraph_url("uniswap_v2", "ETHEREUM")
        assert url1 == url2

    def test_get_subgraph_url_unknown_protocol(self, subgraph_service):
        """Test getting URL for unknown protocol."""
        url = subgraph_service.get_subgraph_url("unknown_protocol", "ETHEREUM")
        assert url is None

    def test_cache_validity(self, subgraph_service):
        """Test cache validity checking."""
        cache_key = "test_key"
        subgraph_service._subgraph_cache[cache_key] = "https://test.url"
        subgraph_service._cache_timestamps[cache_key] = datetime.now()

        # Cache should be valid immediately
        assert subgraph_service._is_cache_valid(cache_key) is True

        # Cache should be invalid after TTL expires
        subgraph_service._cache_timestamps[cache_key] = datetime.now() - timedelta(
            hours=25
        )
        assert subgraph_service._is_cache_valid(cache_key) is False

    def test_clear_cache_all(self, subgraph_service):
        """Test clearing all cache."""
        subgraph_service._subgraph_cache["key1"] = "url1"
        subgraph_service._subgraph_cache["key2"] = "url2"
        subgraph_service._cache_timestamps["key1"] = datetime.now()
        subgraph_service._cache_timestamps["key2"] = datetime.now()

        subgraph_service.clear_cache()

        assert subgraph_service._subgraph_cache == {}
        assert subgraph_service._cache_timestamps == {}

    def test_clear_cache_protocol(self, subgraph_service):
        """Test clearing cache for specific protocol."""
        subgraph_service._subgraph_cache["uniswap_v2_ETHEREUM"] = "url1"
        subgraph_service._subgraph_cache["curve_ETHEREUM"] = "url2"
        subgraph_service._cache_timestamps["uniswap_v2_ETHEREUM"] = datetime.now()
        subgraph_service._cache_timestamps["curve_ETHEREUM"] = datetime.now()

        subgraph_service.clear_cache(protocol="uniswap_v2")

        assert "uniswap_v2_ETHEREUM" not in subgraph_service._subgraph_cache
        assert "curve_ETHEREUM" in subgraph_service._subgraph_cache
