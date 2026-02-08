"""
Integration tests for CloudInstrumentStorage.

Tests GCS/BigQuery operations with test bucket and real credentials.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage


@pytest.mark.integration
class TestCloudInstrumentStorage:
    """Test CloudInstrumentStorage with real cloud services."""

    def test_storage_initialization_test_bucket(self, test_cloud_target, test_bucket_name):
        """Test storage initialization with test bucket."""
        storage = CloudInstrumentStorage(cloud_target=test_cloud_target)
        assert storage.cloud_target.gcs_bucket == test_bucket_name
        assert "test" in storage.cloud_target.gcs_bucket.lower()

    def test_store_instruments_to_test_bucket(self, test_cloud_target, test_bucket_name):
        """Test storing instruments to test bucket."""
        storage = CloudInstrumentStorage(cloud_target=test_cloud_target)

        # Create test instruments DataFrame
        test_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        instruments_df = pd.DataFrame(
            [
                {
                    "instrument_key": "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",
                    "symbol": "BTC-USDT",
                    "available_from_datetime": test_date,
                    "available_to_datetime": None,
                    "data_types": "trades,book_snapshot_5",
                    "base_asset": "BTC",
                    "quote_asset": "USDT",
                    "tardis_exchange": "binance-futures",
                    "tardis_symbol": "btcusdt",
                }
            ]
        )

        # Store to test bucket
        result = storage.store_instruments(
            instruments_df=instruments_df,
            table_name="instruments_integration_test",
            date=test_date,
        )

        assert result, "Should successfully store instruments to test bucket"
        assert storage.cloud_target.gcs_bucket == test_bucket_name

    def test_query_instruments_from_test_bucket(self, test_cloud_target, test_bucket_name):
        """Test querying instruments from test bucket."""
        storage = CloudInstrumentStorage(cloud_target=test_cloud_target)

        # Query instruments
        results = storage.query_instruments(table_name="instruments_integration_test")

        # Should return DataFrame (may be empty if no data)
        assert isinstance(results, pd.DataFrame)
        assert storage.cloud_target.gcs_bucket == test_bucket_name

    def test_test_bucket_isolation(self, test_cloud_target, test_bucket_name, prod_bucket_name):
        """Test that test bucket is isolated from prod bucket."""
        storage = CloudInstrumentStorage(cloud_target=test_cloud_target)
        assert storage.cloud_target.gcs_bucket == test_bucket_name
        assert storage.cloud_target.gcs_bucket != prod_bucket_name
        assert "test" in storage.cloud_target.gcs_bucket.lower()
