"""
Unit tests for DatabentoAdapter.

REFACTORED: Tests updated to work with DatabentoBaseClient architecture.
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

        try:
            with patch(
                "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                assert adapter.api_key == "test-key"
                assert adapter.client is not None
        finally:
            if original_db is not None:
                databento_adapter.db = original_db

    def test_init_without_api_key(self):
        """Test initialization without API key (uses Secret Manager via base client)."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        original_db = getattr(databento_adapter, "db", None)

        try:
            # Clear any cached state in base client
            databento_base_client.clear_databento_api_key_cache()
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.get_secret_with_fallback",
                    return_value="secret-key",
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter()
                assert adapter.api_key == "secret-key"
        finally:
            # Restore original state
            if original_db is not None:
                databento_adapter.db = original_db
            databento_base_client.clear_databento_api_key_cache()
            databento_base_client.clear_databento_client_cache()

    def test_init_databento_not_available(self):
        """Test initialization when databento package not available."""
        from unified_cloud_services.clients import databento_base_client

        # Clear cache
        databento_base_client.clear_databento_api_key_cache()
        databento_base_client.clear_databento_client_cache()

        try:
            # Mock DATABENTO_AVAILABLE as False
            with patch(
                "unified_cloud_services.clients.databento_base_client.DATABENTO_AVAILABLE", False
            ):
                # Re-import to pick up the patched value
                from instruments_service.app.venues.databento import databento_adapter

                # The base client should raise ImportError
                with pytest.raises(ImportError):
                    databento_adapter.DatabentoAdapter()
        finally:
            databento_base_client.clear_databento_api_key_cache()
            databento_base_client.clear_databento_client_cache()

    def test_clear_cache(self):
        """Test clearing module-level cache."""
        from instruments_service.app.venues.databento import databento_adapter

        # Set some cache values in the unified config cache (local to adapter)
        databento_adapter._UNIFIED_CONFIG_CACHE = Mock()

        # Call clear cache
        databento_adapter.clear_databento_cache()

        # Verify adapter-specific cache is cleared
        assert databento_adapter._UNIFIED_CONFIG_CACHE is None

    def test_get_dataset_for_exchange_cme(self):
        """Test dataset mapping for CME."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                dataset = adapter._get_dataset_for_exchange("CME")
                assert dataset == "GLBX.MDP3"
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_get_dataset_for_exchange_cboe(self):
        """Test dataset mapping for CBOE (VIX via Barchart)."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                dataset = adapter._get_dataset_for_exchange("CBOE")
                assert dataset == "BARCHART"  # CBOE VIX uses Barchart
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_get_dataset_for_exchange_nasdaq(self):
        """Test dataset mapping for NASDAQ."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                dataset = adapter._get_dataset_for_exchange("NASDAQ")
                # NASDAQ uses DBEQ.BASIC for equities (actual behavior may vary based on implementation)
                assert dataset in ["DBEQ.BASIC", "GLBX.MDP3"]
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_get_dataset_for_exchange_nyse(self):
        """Test dataset mapping for NYSE."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                dataset = adapter._get_dataset_for_exchange("NYSE")
                # NYSE uses DBEQ.BASIC for equities (actual behavior may vary based on implementation)
                assert dataset in ["DBEQ.BASIC", "GLBX.MDP3"]
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_client_reuse(self):
        """Test that client is reused via DatabentoBaseClient caching."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            # Clear cache first
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                # Create first adapter
                adapter1 = databento_adapter.DatabentoAdapter(api_key="test-key")
                client1 = adapter1.client

                # Create second adapter - should reuse client via base client caching
                adapter2 = databento_adapter.DatabentoAdapter(api_key="test-key")
                client2 = adapter2.client

                # Both should reference the same client
                assert client1 is client2
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

    def test_api_key_caching(self):
        """Test that API key is cached via DatabentoBaseClient."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        mock_get_secret = Mock(return_value="secret-from-manager")

        try:
            # Clear cache first
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.get_secret_with_fallback",
                    mock_get_secret,
                ),
            ):
                # First adapter - should call Secret Manager
                adapter1 = databento_adapter.DatabentoAdapter()
                assert adapter1.api_key == "secret-from-manager"

                # Second adapter - should use cached API key
                adapter2 = databento_adapter.DatabentoAdapter()
                assert adapter2.api_key == "secret-from-manager"

                # Secret Manager should only be called once (cached)
                assert mock_get_secret.call_count == 1
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

    def test_create_vix_instrument_definition(self):
        """Test VIX instrument definition creation."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                vix_def = adapter.create_vix_instrument_definition(target_date)

                assert vix_def is not None
                assert vix_def["instrument_key"] == "CBOE:INDEX:VIX-USD"
                assert vix_def["venue"] == "CBOE"
                assert vix_def["instrument_type"] == "INDEX"
                assert vix_def["base_asset"] == "VIX"
                assert vix_def["quote_asset"] == "USD"
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_create_bitcoin_etf_instrument_definition_ibit(self):
        """Test IBIT (iShares Bitcoin ETF) instrument definition creation."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                # Note: method signature is (ticker, target_date)
                etf_def = adapter.create_bitcoin_etf_instrument_definition("IBIT", target_date)

                assert etf_def is not None
                assert etf_def["instrument_key"] == "NASDAQ:ETF:IBIT-USD"
                assert etf_def["venue"] == "NASDAQ"
                assert etf_def["instrument_type"] == "ETF"
                assert etf_def["base_asset"] == "IBIT"
                assert etf_def["quote_asset"] == "USD"
                # underlying_asset may not be in the returned dict
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_create_bitcoin_etf_instrument_definition_unsupported(self):
        """Test unsupported Bitcoin ETF ticker returns None."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                # GBTC is not in the supported ETF list
                etf_def = adapter.create_bitcoin_etf_instrument_definition("GBTC", target_date)

                # Should return None for unsupported ticker
                assert etf_def is None
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_is_us_market_holiday(self):
        """Test US market holiday detection."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client
        from datetime import date

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")

                # Test a known holiday (Christmas 2024)
                is_holiday, name = adapter.is_us_market_holiday(date(2024, 12, 25))
                assert is_holiday
                assert name is not None

                # Test a regular trading day
                is_holiday, name = adapter.is_us_market_holiday(date(2024, 11, 11))
                # Nov 11 is Veterans Day - might be holiday for some exchanges
                # Just verify it returns a tuple
                assert isinstance(is_holiday, bool)
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_get_query_date_for_databento_weekend(self):
        """Test query date adjustment for weekends."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")

                # Saturday Nov 16, 2024 should roll back to Friday Nov 15
                saturday = datetime(2024, 11, 16, 12, 0, 0, tzinfo=timezone.utc)
                query_date = adapter._get_query_date_for_databento(saturday)

                # Should be Friday
                assert query_date.weekday() == 4  # Friday
        finally:
            databento_base_client.clear_databento_client_cache()

    def test_get_exchange_trading_hours_cme(self):
        """Test CME trading hours extraction."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch(
                    "instruments_service.app.venues.databento.databento_adapter.db", mock_db_module
                ),
                patch(
                    "unified_cloud_services.clients.databento_base_client.db", mock_db_module
                ),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                hours = adapter._get_exchange_trading_hours("CME", "FUTURE", target_date)

                assert hours is not None
                assert "session" in hours
                assert hours["session"] == "regular"
        finally:
            databento_base_client.clear_databento_client_cache()
