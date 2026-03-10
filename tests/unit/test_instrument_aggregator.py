"""
Unit tests for InstrumentAggregator.

Tests aggregate_category, _deduplicate, _load_all_from_storage,
_load_delta_and_merge, and _load_latest_aggregated with fully mocked
storage clients.
"""

import io
from datetime import date, timedelta
from unittest.mock import MagicMock

import pandas as pd

from instruments_service.engine.operations.aggregate.aggregator import (
    AGGREGATED_PREFIX,
    BASE_PREFIX,
    InstrumentAggregator,
)

# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _make_blob(name: str) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    return blob


# ─── InstrumentAggregator.__init__ ───────────────────────────────────────────


class TestInstrumentAggregatorInit:
    def test_default_max_workers(self):
        agg = InstrumentAggregator()
        assert agg.max_workers == 12

    def test_custom_max_workers(self):
        agg = InstrumentAggregator(max_workers=4)
        assert agg.max_workers == 4


# ─── _deduplicate ─────────────────────────────────────────────────────────────


class TestDeduplicate:
    def setup_method(self):
        self.agg = InstrumentAggregator()

    def test_deduplicates_by_instrument_key(self):
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "timestamp": "2025-01-02", "val": 2},
                {"instrument_key": "A", "timestamp": "2025-01-01", "val": 1},
                {"instrument_key": "B", "timestamp": "2025-01-01", "val": 3},
            ]
        )
        result = self.agg._deduplicate(df)
        assert len(result) == 2
        # Latest timestamp kept for A
        a_row = result[result["instrument_key"] == "A"]
        assert a_row.iloc[0]["val"] == 2

    def test_no_instrument_key_column_uses_fallback(self):
        df = pd.DataFrame(
            [
                {"some_instrument_col": "A", "val": 1},
                {"some_instrument_col": "A", "val": 2},
            ]
        )
        result = self.agg._deduplicate(df)
        assert len(result) == 1

    def test_no_timestamp_col_still_deduplicates(self):
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "val": 1},
                {"instrument_key": "A", "val": 2},
                {"instrument_key": "B", "val": 3},
            ]
        )
        result = self.agg._deduplicate(df)
        assert len(result) == 2


# ─── aggregate_category ───────────────────────────────────────────────────────


class TestAggregateCategory:
    def setup_method(self):
        self.agg = InstrumentAggregator()

    def test_redo_all_empty_returns_zero(self):
        storage = MagicMock()
        storage.list_blobs.return_value = []
        result = self.agg.aggregate_category(storage, "test-bucket", redo_all=True)
        assert result == 0

    def test_delta_mode_empty_returns_zero(self):
        storage = MagicMock()
        # Bucket mock for delta mode
        mock_bucket = MagicMock()
        mock_iterator = MagicMock()
        mock_iterator.prefixes = []
        mock_bucket.list_blobs.return_value = mock_iterator
        storage.bucket.return_value = mock_bucket
        # For _load_latest_aggregated
        storage.list_blobs.return_value = []

        result = self.agg.aggregate_category(storage, "test-bucket", redo_all=False)
        assert result == 0

    def test_redo_all_writes_aggregated_file(self):
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "timestamp": "2025-01-01", "val": 1},
                {"instrument_key": "B", "timestamp": "2025-01-01", "val": 2},
            ]
        )
        data = _make_parquet_bytes(df)

        blob1 = _make_blob(f"{BASE_PREFIX}day=2025-01-01/venue=BINANCE/instruments.parquet")

        storage = MagicMock()
        storage.list_blobs.return_value = [blob1]
        storage.download_bytes.return_value = data

        result = self.agg.aggregate_category(storage, "test-bucket", redo_all=True)
        assert result == 2
        storage.upload_bytes.assert_called_once()

    def test_delta_mode_with_data_writes_file(self):
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "timestamp": "2025-03-27", "val": 10},
            ]
        )
        data = _make_parquet_bytes(df)

        yesterday = (date.today() - timedelta(days=1)).isoformat()
        base_path = f"{BASE_PREFIX}day={yesterday}/"
        venue_path = f"{base_path}venue=BINANCE/"
        parquet_path = f"{venue_path}instruments.parquet"

        mock_blob = _make_blob(parquet_path)
        mock_iterator = MagicMock()
        mock_iterator.prefixes = [venue_path]
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = mock_iterator

        storage = MagicMock()
        storage.bucket.return_value = mock_bucket
        storage.download_bytes.return_value = data
        # No existing aggregated files
        storage.list_blobs.return_value = []

        result = self.agg.aggregate_category(storage, "test-bucket", redo_all=False)
        assert result == 1
        storage.upload_bytes.assert_called_once()


# ─── _load_latest_aggregated ─────────────────────────────────────────────────


class TestLoadLatestAggregated:
    def setup_method(self):
        self.agg = InstrumentAggregator()

    def test_no_blobs_returns_empty(self):
        storage = MagicMock()
        storage.list_blobs.return_value = []
        result = self.agg._load_latest_aggregated(storage, "test-bucket")
        assert result.empty

    def test_latest_blob_is_loaded(self):
        df = pd.DataFrame([{"instrument_key": "A", "val": 1}])
        data = _make_parquet_bytes(df)

        blob1 = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-01.parquet")
        blob2 = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-28.parquet")

        storage = MagicMock()
        storage.list_blobs.return_value = [blob1, blob2]
        storage.download_bytes.return_value = data

        result = self.agg._load_latest_aggregated(storage, "test-bucket")
        assert not result.empty
        # Should have downloaded the latest (2025-03-28)
        call_args = storage.download_bytes.call_args
        assert "2025-03-28" in call_args[1]["blob_path"]

    def test_os_error_on_download_returns_empty(self):
        blob = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-28.parquet")
        storage = MagicMock()
        storage.list_blobs.return_value = [blob]
        storage.download_bytes.side_effect = OSError("download failed")

        result = self.agg._load_latest_aggregated(storage, "test-bucket")
        assert result.empty
