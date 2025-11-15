"""
Unit tests for DatabentoAdapter.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone


class TestDatabentoAdapter:
    """Tests for DatabentoAdapter."""

    def test_init_with_api_key(self):
        """Test initialization with API key."""
        from instruments_service.app.venues.databento import databento_adapter

        # Mock db module
        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        original_db = getattr(databento_adapter, "db", None)
        original_available = databento_adapter.DATABENTO_AVAILABLE

        try:
            databento_adapter.db = mock_db_module
            databento_adapter.DATABENTO_AVAILABLE = True

            adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
            assert adapter.api_key == "test-key"
            assert adapter.client is not None
        finally:
            if original_db is not None:
                databento_adapter.db = original_db
            databento_adapter.DATABENTO_AVAILABLE = original_available

    def test_init_without_api_key(self):
        """Test initialization without API key (uses Secret Manager)."""
        from instruments_service.app.venues.databento import databento_adapter

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        original_db = getattr(databento_adapter, "db", None)
        original_available = databento_adapter.DATABENTO_AVAILABLE
        original_client = getattr(databento_adapter, "_DATABENTO_CLIENT", None)
        original_api_key = getattr(databento_adapter, "_DATABENTO_API_KEY", None)

        try:
            # Clear any cached state
            databento_adapter._DATABENTO_CLIENT = None
            databento_adapter._DATABENTO_API_KEY = None

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch("unified_cloud_services.get_secret_with_fallback", return_value="secret-key"),
            ):
                databento_adapter.db = mock_db_module
                databento_adapter.DATABENTO_AVAILABLE = True

                adapter = databento_adapter.DatabentoAdapter()
                assert adapter.api_key == "secret-key"
        finally:
            # Restore original state
            if original_db is not None:
                databento_adapter.db = original_db
            databento_adapter.DATABENTO_AVAILABLE = original_available
            if original_client is not None:
                databento_adapter._DATABENTO_CLIENT = original_client
            if original_api_key is not None:
                databento_adapter._DATABENTO_API_KEY = original_api_key
            else:
                databento_adapter._DATABENTO_API_KEY = None

    def test_init_databento_not_available(self):
        """Test initialization when databento package not available."""
        from instruments_service.app.venues.databento import databento_adapter

        original_available = databento_adapter.DATABENTO_AVAILABLE
        try:
            databento_adapter.DATABENTO_AVAILABLE = False
            with pytest.raises(ImportError, match="databento package not available"):
                databento_adapter.DatabentoAdapter()
        finally:
            databento_adapter.DATABENTO_AVAILABLE = original_available

    def test_clear_cache(self):
        """Test clearing module-level cache."""
        from instruments_service.app.venues.databento import databento_adapter

        # Set some cache values
        databento_adapter._DATABENTO_CLIENT = Mock()
        databento_adapter._DATABENTO_API_KEY = "test-key"
        databento_adapter._UNIFIED_CONFIG_CACHE = Mock()

        databento_adapter.clear_databento_cache()

        assert databento_adapter._DATABENTO_CLIENT is None
        assert databento_adapter._DATABENTO_API_KEY is None
        assert databento_adapter._UNIFIED_CONFIG_CACHE is None
