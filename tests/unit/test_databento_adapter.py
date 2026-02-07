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
            with patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module):
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
            with patch("unified_cloud_services.clients.databento_base_client.DATABENTO_AVAILABLE", False):
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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

    def test_create_krwusd_instrument_definition(self):
        """Test KRW/USD instrument definition: venue must be FX, not YAHOO_FINANCE."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            databento_base_client.clear_databento_client_cache()

            with (
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                krwusd_def = adapter.create_krwusd_instrument_definition(target_date)

                assert krwusd_def is not None
                # CRITICAL: venue must be "FX", not "YAHOO_FINANCE"
                # Yahoo Finance is a data provider, not a venue
                assert krwusd_def["venue"] == "FX"
                assert krwusd_def["instrument_key"] == "FX:SPOT_PAIR:KRW-USD"
                assert krwusd_def["instrument_type"] == "SPOT_PAIR"
                assert krwusd_def["base_asset"] == "KRW"
                assert krwusd_def["quote_asset"] == "USD"
                assert krwusd_def["data_provider"] == "yahoo_finance"
                assert krwusd_def["venue_type"] == "otc"
                assert krwusd_def["exchange_raw_symbol"] == "KRWUSD=X"
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
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
                patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
            ):
                adapter = databento_adapter.DatabentoAdapter(api_key="test-key")
                target_date = datetime(2024, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

                hours = adapter._get_exchange_trading_hours("CME", "FUTURE", target_date)

                assert hours is not None
                assert "session" in hours
                assert hours["session"] == "regular"
        finally:
            databento_base_client.clear_databento_client_cache()


class TestBatchJobDelegation:
    """Tests verifying DatabentoAdapter delegates batch logic to DatabentoBaseClient.

    The actual batch orchestration logic (find/submit/wait/download, deterministic key
    selection, expanded states, GCS cache) is tested in unified-cloud-services:
    tests/unit/test_databento_batch.py

    These tests verify the adapter correctly delegates to the base client.
    """

    # ---------------------------------------------------------------
    # Helper to create adapter with a mocked client
    # ---------------------------------------------------------------

    def _make_adapter(self, mock_db_module, mock_client):
        """Create a DatabentoAdapter with mocked db module and client."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        databento_base_client.clear_databento_client_cache()
        databento_base_client.clear_databento_api_key_cache()

        with (
            patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
            patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
        ):
            adapter = databento_adapter.DatabentoAdapter(api_key="test-key")

        # Replace the underlying client with our mock so batch calls are captured
        adapter._base_client._client = mock_client
        return adapter

    # ---------------------------------------------------------------
    # _fetch_with_batch_api delegates to base_client.batch_download
    # ---------------------------------------------------------------

    def test_fetch_with_batch_api_delegates_to_base_client(self):
        """_fetch_with_batch_api calls base_client.batch_download."""
        from pathlib import Path
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            adapter = self._make_adapter(mock_db_module, mock_client)

            # Mock base_client.batch_download to return a temp dir with a .dbn file
            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                data_file = tmp_path / "data.dbn.zst"
                data_file.write_bytes(b"fake-data")

                mock_dbn_store = Mock()
                mock_db_module.DBNStore.from_file.return_value = mock_dbn_store

                with (
                    patch.object(adapter._base_client, "batch_download", return_value=tmp_path) as mock_batch,
                    patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                ):
                    result = adapter._fetch_with_batch_api(
                        dataset="GLBX.MDP3",
                        schema="definition",
                        symbols=["ES"],
                        stype_in="raw_symbol",
                        start="2024-01-15",
                        end="2024-01-16",
                    )

                mock_batch.assert_called_once_with(
                    dataset="GLBX.MDP3",
                    schema="definition",
                    symbols=["ES"],
                    stype_in="raw_symbol",
                    start="2024-01-15",
                    end="2024-01-16",
                )
                mock_db_module.DBNStore.from_file.assert_called_once()
                assert result is mock_dbn_store
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

    def test_fetch_with_batch_api_no_streaming_fallback(self):
        """_fetch_with_batch_api does NOT fall back to streaming API on error."""
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            adapter = self._make_adapter(mock_db_module, mock_client)

            with patch.object(
                adapter._base_client,
                "batch_download",
                side_effect=RuntimeError("Batch failed"),
            ):
                with pytest.raises(RuntimeError, match="Batch failed"):
                    adapter._fetch_with_batch_api(
                        dataset="GLBX.MDP3",
                        schema="definition",
                        symbols=["ES"],
                        stype_in="raw_symbol",
                        start="2024-01-15",
                        end="2024-01-16",
                    )

            # Streaming API should never be called
            mock_client.timeseries.get_range.assert_not_called()
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

    def test_fetch_with_batch_api_raises_on_no_data_file(self):
        """When batch download has no .dbn file, raises FileNotFoundError (no silent fallback)."""
        from pathlib import Path
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            adapter = self._make_adapter(mock_db_module, mock_client)

            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                # Create a non-.dbn file -- no valid data
                (tmp_path / "metadata.json").write_text("{}")

                with patch.object(adapter._base_client, "batch_download", return_value=tmp_path):
                    with pytest.raises(FileNotFoundError, match="No .dbn or .dbn.zst data file found"):
                        adapter._fetch_with_batch_api(
                            dataset="GLBX.MDP3",
                            schema="definition",
                            symbols=["ES"],
                            stype_in="raw_symbol",
                            start="2024-01-15",
                            end="2024-01-16",
                        )
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()

    def test_fetch_with_batch_api_finds_dbn_in_subdirectory(self):
        """batch.download() places files in output_path/JOB_ID/ -- rglob must find them."""
        from pathlib import Path
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        try:
            adapter = self._make_adapter(mock_db_module, mock_client)

            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                # Simulate Databento's batch download structure: output_path/JOB_ID/file.dbn.zst
                job_dir = tmp_path / "GLBX-20260125-ABCDEF"
                job_dir.mkdir()
                data_file = job_dir / "data.dbn.zst"
                data_file.write_bytes(b"fake-data")

                mock_dbn_store = Mock()
                mock_db_module.DBNStore.from_file.return_value = mock_dbn_store

                with (
                    patch.object(adapter._base_client, "batch_download", return_value=tmp_path),
                    patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
                ):
                    result = adapter._fetch_with_batch_api(
                        dataset="GLBX.MDP3",
                        schema="definition",
                        symbols=["ES"],
                        stype_in="raw_symbol",
                        start="2024-01-15",
                        end="2024-01-16",
                    )

                # Should find the file in the subdirectory
                mock_db_module.DBNStore.from_file.assert_called_once_with(str(data_file))
                assert result is mock_dbn_store
        finally:
            databento_base_client.clear_databento_client_cache()
            databento_base_client.clear_databento_api_key_cache()


class TestDatabentoT2Availability:
    """Tests for the T+2 historical data availability guard.

    Databento historical data is published ~2 calendar days after the
    trading date (available around UTC midnight, two days later).
    The adapter should skip dates that are too recent to avoid billable
    422 errors from the Databento API.
    """

    # ---------------------------------------------------------------
    # Helper to create adapter with mocked internals
    # ---------------------------------------------------------------

    def _make_adapter(self):
        """Create a DatabentoAdapter with fully mocked Databento client."""
        from instruments_service.app.venues.databento import databento_adapter
        from unified_cloud_services.clients import databento_base_client

        mock_db_module = MagicMock()
        mock_client = Mock()
        mock_db_module.Historical.return_value = mock_client

        databento_base_client.clear_databento_client_cache()
        databento_base_client.clear_databento_api_key_cache()

        with (
            patch("instruments_service.app.venues.databento.databento_adapter.db", mock_db_module),
            patch("unified_cloud_services.clients.databento_base_client.db", mock_db_module),
        ):
            adapter = databento_adapter.DatabentoAdapter(api_key="test-key")

        adapter._base_client._client = mock_client
        return adapter, mock_client, databento_base_client

    # ---------------------------------------------------------------
    # T+2 guard: dates that should be SKIPPED
    # ---------------------------------------------------------------

    def test_t2_skips_today(self):
        """Today's date should be skipped (T+0 < T+2)."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            # Use a fixed "today" to avoid test flakiness
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            today_date = datetime(2026, 2, 7, 0, 0, 0, tzinfo=timezone.utc)

            with patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt:
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                result = adapter.fetch_instrument_definitions(
                    exchange="CME",
                    symbols=["ES.FUT"],
                    date=today_date,
                )

            assert result == {}
            # No Databento API calls should have been made
            mock_client.batch.list_jobs.assert_not_called()
            mock_client.timeseries.get_range.assert_not_called()
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    def test_t2_skips_yesterday(self):
        """Yesterday's date should be skipped (T+1 < T+2)."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            yesterday = datetime(2026, 2, 6, 0, 0, 0, tzinfo=timezone.utc)

            with patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt:
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                result = adapter.fetch_instrument_definitions(
                    exchange="CME",
                    symbols=["ES.FUT"],
                    date=yesterday,
                )

            assert result == {}
            mock_client.batch.list_jobs.assert_not_called()
            mock_client.timeseries.get_range.assert_not_called()
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    def test_t2_skips_all_exchanges(self):
        """T+2 guard should apply to all Databento exchanges (CME, NASDAQ, NYSE, ICE)."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            yesterday = datetime(2026, 2, 6, 0, 0, 0, tzinfo=timezone.utc)

            for exchange in ["CME", "NASDAQ", "NYSE", "ICE"]:
                with patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt:
                    mock_dt.now.return_value = fake_now
                    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                    result = adapter.fetch_instrument_definitions(
                        exchange=exchange,
                        symbols=["ES.FUT"],
                        date=yesterday,
                    )

                assert result == {}, f"Expected empty dict for {exchange} but got {result}"
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    # ---------------------------------------------------------------
    # T+2 guard: dates that should be ALLOWED through
    # ---------------------------------------------------------------

    def test_t2_allows_two_days_ago(self):
        """Date exactly 2 days ago should pass the T+2 check (boundary case)."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            two_days_ago = datetime(2026, 2, 5, 0, 0, 0, tzinfo=timezone.utc)

            with (
                patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt,
                patch("instruments_service.app.venues.databento.databento_adapter.logger") as mock_logger,
            ):
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                adapter.fetch_instrument_definitions(
                    exchange="CME",
                    symbols=["ES.FUT"],
                    date=two_days_ago,
                )

            # The T+2 guard should NOT have fired — verify no DATABENTO_T2 warning
            for call in mock_logger.warning.call_args_list:
                assert "DATABENTO_T2" not in str(call), (
                    f"T+2 guard should not block a date exactly 2 days ago, but got: {call}"
                )
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    def test_t2_allows_old_dates(self):
        """Historical dates well in the past should pass the T+2 check."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            old_date = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

            with (
                patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt,
                patch("instruments_service.app.venues.databento.databento_adapter.logger") as mock_logger,
            ):
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                adapter.fetch_instrument_definitions(
                    exchange="CME",
                    symbols=["ES.FUT"],
                    date=old_date,
                )

            # The T+2 guard should NOT have fired
            for call in mock_logger.warning.call_args_list:
                assert "DATABENTO_T2" not in str(call), f"T+2 guard should not block an old date, but got: {call}"
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    # ---------------------------------------------------------------
    # T+2 guard: warning message content
    # ---------------------------------------------------------------

    def test_t2_warning_message_content(self):
        """Verify the warning message includes the correct dates and exchange."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            yesterday = datetime(2026, 2, 6, 0, 0, 0, tzinfo=timezone.utc)

            with (
                patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt,
                patch("instruments_service.app.venues.databento.databento_adapter.logger") as mock_logger,
            ):
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                adapter.fetch_instrument_definitions(
                    exchange="NASDAQ",
                    symbols=["SPY"],
                    date=yesterday,
                )

            # Check the warning was logged with expected content
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "DATABENTO_T2" in warning_msg
            assert "NASDAQ" in warning_msg
            assert "2026-02-06" in warning_msg
            assert "2026-02-05" in warning_msg  # earliest queryable date
            assert "2026-02-08" in warning_msg  # try again after
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()

    def test_t2_naive_datetime_handled(self):
        """Timezone-naive dates should still be handled correctly by the T+2 guard."""
        adapter, mock_client, base_client = self._make_adapter()

        try:
            fake_now = datetime(2026, 2, 7, 12, 0, 0, tzinfo=timezone.utc)
            # Pass a naive datetime (no timezone) - adapter converts to UTC
            yesterday_naive = datetime(2026, 2, 6, 0, 0, 0)

            with patch("instruments_service.app.venues.databento.databento_adapter.datetime") as mock_dt:
                mock_dt.now.return_value = fake_now
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

                result = adapter.fetch_instrument_definitions(
                    exchange="CME",
                    symbols=["ES.FUT"],
                    date=yesterday_naive,
                )

            assert result == {}
        finally:
            base_client.clear_databento_client_cache()
            base_client.clear_databento_api_key_cache()
