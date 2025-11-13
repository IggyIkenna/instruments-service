"""
Unit tests for TheGraphClient.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from instruments_service.app.venues.defi.the_graph_client import TheGraphClient


class TestTheGraphClient:
    """Tests for TheGraphClient."""

    def test_init(self):
        """Test TheGraphClient initialization."""
        with patch("instruments_service.app.venues.defi.the_graph_client.get_secret_with_fallback", return_value=None):
            client = TheGraphClient(api_key="test-key", subgraph_url="https://test.com")
            assert client.api_key == "test-key"
            assert client.subgraph_url == "https://test.com"

    def test_init_without_url(self):
        """Test initialization without subgraph URL."""
        with patch("instruments_service.app.venues.defi.the_graph_client.get_secret_with_fallback", return_value=None), \
             patch("instruments_service.app.venues.defi.the_graph_client.os.getenv", return_value=None):
            client = TheGraphClient(api_key="test-key")
            assert client.api_key == "test-key"

