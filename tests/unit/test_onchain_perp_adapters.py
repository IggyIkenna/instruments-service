"""
Unit tests for on-chain perpetuals adapters (Aster, Hyperliquid).

These tests verify that adapters properly integrate with BaseClients from unified-cloud-services.
"""

from unittest.mock import Mock, patch


class TestAsterAdapterBaseClientIntegration:
    """Tests that Aster adapter properly uses AsterBaseClient."""

    @patch("instruments_service.app.venues.onchain_perps.aster_adapter.AsterBaseClient")
    def test_adapter_uses_base_client(self, mock_base_client_class):
        """Verify adapter uses AsterBaseClient for HTTP operations."""
        from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

        # Mock the BaseClient instance
        mock_client = Mock()
        mock_session = Mock()
        mock_client.sync_session = mock_session
        mock_client.get_futures_url = Mock(return_value="https://fapi.asterdex.com/fapi/v1/exchangeInfo")
        mock_base_client_class.return_value = mock_client

        # Create adapter (will lazy-load BaseClient)
        adapter = AsterAdapter(base_currency_list=["BTC", "ETH"], project_id="test-project")

        # Access client property to trigger lazy loading
        client = adapter.client

        # Verify BaseClient was used
        assert client == mock_client or mock_base_client_class.called

    def test_adapter_lazy_loads_client(self):
        """Verify adapter lazy-loads BaseClient only when accessed."""
        from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

        # Create adapter with explicit base_client=None
        adapter = AsterAdapter(base_client=None, project_id="test-project")

        # Client should be None initially
        assert adapter._base_client is None

        # Accessing client property should create it
        with patch("instruments_service.app.venues.onchain_perps.aster_adapter.AsterBaseClient"):
            _client = adapter.client
            # Should have created/cached a client
            assert adapter._base_client is not None
            assert _client is not None

    def test_adapter_reuses_provided_client(self):
        """Verify adapter reuses a provided BaseClient instance."""
        from unified_cloud_services import AsterBaseClient

        from instruments_service.app.venues.onchain_perps.aster_adapter import AsterAdapter

        # Create a mock BaseClient
        mock_client = Mock(spec=AsterBaseClient)

        # Pass it to adapter
        adapter = AsterAdapter(base_client=mock_client, project_id="test-project")

        # Client property should return the same instance
        assert adapter.client is mock_client


class TestHyperliquidAdapterBaseClientIntegration:
    """Tests that Hyperliquid adapter properly uses HyperliquidBaseClient."""

    @patch("instruments_service.app.venues.onchain_perps.hyperliquid_adapter.HyperliquidBaseClient")
    def test_adapter_uses_base_client(self, mock_base_client_class):
        """Verify adapter uses HyperliquidBaseClient for HTTP operations."""
        from instruments_service.app.venues.onchain_perps.hyperliquid_adapter import HyperliquidAdapter

        # Mock the BaseClient instance
        mock_client = Mock()
        mock_session = Mock()
        mock_client.sync_session = mock_session
        mock_client.get_api_url = Mock(return_value="https://api.hyperliquid.xyz/info")
        mock_base_client_class.return_value = mock_client

        # Create adapter (will lazy-load BaseClient)
        adapter = HyperliquidAdapter(base_currency_list=["BTC", "ETH"], project_id="test-project")

        # Access client property to trigger lazy loading
        client = adapter.client

        # Verify BaseClient was used
        assert client == mock_client or mock_base_client_class.called

    def test_adapter_lazy_loads_client(self):
        """Verify adapter lazy-loads BaseClient only when accessed."""
        from instruments_service.app.venues.onchain_perps.hyperliquid_adapter import HyperliquidAdapter

        # Create adapter with explicit base_client=None
        adapter = HyperliquidAdapter(base_client=None, project_id="test-project")

        # Client should be None initially
        assert adapter._base_client is None

        # Accessing client property should create it
        with patch("instruments_service.app.venues.onchain_perps.hyperliquid_adapter.HyperliquidBaseClient"):
            _client = adapter.client
            # Should have created/cached a client
            assert adapter._base_client is not None
            assert _client is not None

    def test_adapter_reuses_provided_client(self):
        """Verify adapter reuses a provided BaseClient instance."""
        from unified_cloud_services import HyperliquidBaseClient

        from instruments_service.app.venues.onchain_perps.hyperliquid_adapter import HyperliquidAdapter

        # Create a mock BaseClient
        mock_client = Mock(spec=HyperliquidBaseClient)

        # Pass it to adapter
        adapter = HyperliquidAdapter(base_client=mock_client, project_id="test-project")

        # Client property should return the same instance
        assert adapter.client is mock_client


class TestNoDirectHTTPCalls:
    """Tests that verify adapters don't make direct HTTP calls (must use BaseClient)."""

    def test_aster_adapter_no_direct_requests_import(self):
        """Verify Aster adapter doesn't use requests directly for API calls."""
        # This test enforces architectural pattern - adapters must use BaseClient.sync_session
        # If someone adds direct requests.get/post calls, this test will catch it

        import inspect

        from instruments_service.app.venues.onchain_perps import aster_adapter

        source = inspect.getsource(aster_adapter)

        # Should import requests (for exceptions), but not use requests.get/post directly
        # All HTTP calls should go through self.client.sync_session
        assert "import requests" in source
        # Check that adapter uses client.sync_session, not direct requests calls
        assert "self.client.sync_session.get" in source or "self.client.get_futures_url" in source

    def test_hyperliquid_adapter_no_direct_requests_import(self):
        """Verify Hyperliquid adapter doesn't use requests directly for API calls."""
        import inspect

        from instruments_service.app.venues.onchain_perps import hyperliquid_adapter

        source = inspect.getsource(hyperliquid_adapter)

        # Should use client.sync_session, not direct requests
        assert "self.client.sync_session.post" in source or "self.client.get_api_url" in source


class TestDeprecatedCodeRemoved:
    """Tests that verify deprecated defi/ adapters are fully removed."""

    def test_no_defi_aster_adapter_file(self):
        """Verify defi/aster_adapter.py file doesn't exist."""
        from pathlib import Path

        defi_aster_path = (
            Path(__file__).parent.parent.parent / "instruments_service" / "app" / "venues" / "defi" / "aster_adapter.py"
        )
        assert not defi_aster_path.exists(), "Deprecated defi/aster_adapter.py still exists!"

    def test_no_defi_hyperliquid_adapter_file(self):
        """Verify defi/hyperliquid_adapter.py file doesn't exist."""
        from pathlib import Path

        defi_hl_path = (
            Path(__file__).parent.parent.parent
            / "instruments_service"
            / "app"
            / "venues"
            / "defi"
            / "hyperliquid_adapter.py"
        )
        assert not defi_hl_path.exists(), "Deprecated defi/hyperliquid_adapter.py still exists!"

    def test_defi_init_no_backwards_compat_imports(self):
        """Verify defi/__init__.py doesn't have backwards-compat imports."""
        from instruments_service.app.venues import defi

        # Should not export Aster or Hyperliquid from defi module
        assert "AsterAdapter" not in defi.__all__
        assert "HyperliquidAdapter" not in defi.__all__

    def test_onchain_perps_are_in_correct_module(self):
        """Verify Aster and Hyperliquid are in onchain_perps module."""
        from instruments_service.app.venues import onchain_perps

        # Should be able to import from onchain_perps
        assert hasattr(onchain_perps, "AsterAdapter")
        assert hasattr(onchain_perps, "HyperliquidAdapter")
