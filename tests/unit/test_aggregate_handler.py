"""
Unit tests for AggregateHandler CLI handler.

Tests _deduplicate, _load_all_from_gcs, _download_parquet,
_load_delta_and_merge, _load_latest_aggregated, and run() with
mocked GCS storage clients.
"""

import io
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.cli.handlers.aggregate_handler import (
    AGGREGATED_PREFIX,
    BASE_PREFIX,
    AggregateHandler,
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


def _make_handler(project_id: str = "test-project") -> AggregateHandler:
    return AggregateHandler(config={"project_id": project_id})


# ─── _deduplicate ─────────────────────────────────────────────────────────────


class TestAggregateHandlerDeduplicate:
    def test_deduplicates_by_instrument_key(self):
        handler = _make_handler()
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "timestamp": "2025-03-28", "val": 2},
                {"instrument_key": "A", "timestamp": "2025-03-27", "val": 1},
                {"instrument_key": "B", "timestamp": "2025-03-28", "val": 3},
            ]
        )
        result = handler._deduplicate(df)
        assert len(result) == 2
        a_row = result[result["instrument_key"] == "A"]
        assert a_row.iloc[0]["val"] == 2  # latest kept

    def test_no_instrument_key_col_uses_fallback(self):
        handler = _make_handler()
        df = pd.DataFrame(
            [
                {"instrument_id": "A"},
                {"instrument_id": "A"},
            ]
        )
        result = handler._deduplicate(df)
        assert len(result) == 1

    def test_no_timestamp_col_still_deduplicates(self):
        handler = _make_handler()
        df = pd.DataFrame(
            [
                {"instrument_key": "A", "val": 1},
                {"instrument_key": "A", "val": 2},
            ]
        )
        result = handler._deduplicate(df)
        assert len(result) == 1


# ─── _download_parquet ────────────────────────────────────────────────────────


class TestAggregateHandlerDownloadParquet:
    def test_returns_dataframe(self):
        handler = _make_handler()
        df = pd.DataFrame([{"instrument_key": "A"}])
        data = _make_parquet_bytes(df)
        mock_client = MagicMock()
        mock_client.download_bytes.return_value = data
        result = handler._download_parquet(mock_client, "test-bucket", "path/to/file.parquet")
        assert not result.empty
        assert "instrument_key" in result.columns


# ─── _load_all_from_gcs ───────────────────────────────────────────────────────


class TestAggregateHandlerLoadAllFromGcs:
    def test_empty_blobs_returns_empty_df(self):
        handler = _make_handler()
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = []
        result = handler._load_all_from_gcs(mock_client, "test-bucket")
        assert result.empty

    def test_loads_parquet_blobs(self):
        handler = _make_handler()
        df = pd.DataFrame([{"instrument_key": "A", "timestamp": "2025-03-28"}])
        data = _make_parquet_bytes(df)
        blob = _make_blob(f"{BASE_PREFIX}day=2025-03-28/venue=BINANCE/instruments.parquet")
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [blob]
        mock_client.download_bytes.return_value = data
        result = handler._load_all_from_gcs(mock_client, "test-bucket")
        assert not result.empty
        assert "instrument_key" in result.columns

    def test_non_parquet_blobs_filtered(self):
        handler = _make_handler()
        blob = _make_blob(f"{BASE_PREFIX}day=2025-03-28/some_metadata.json")
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [blob]
        result = handler._load_all_from_gcs(mock_client, "test-bucket")
        assert result.empty


# ─── _load_latest_aggregated ─────────────────────────────────────────────────


class TestAggregateHandlerLoadLatestAggregated:
    def test_no_blobs_returns_empty(self):
        handler = _make_handler()
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = []
        result = handler._load_latest_aggregated(mock_client, "test-bucket")
        assert result.empty

    def test_loads_latest_by_date(self):
        handler = _make_handler()
        df = pd.DataFrame([{"instrument_key": "A"}])
        data = _make_parquet_bytes(df)
        blob1 = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-01.parquet")
        blob2 = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-28.parquet")
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [blob1, blob2]
        mock_client.download_bytes.return_value = data
        result = handler._load_latest_aggregated(mock_client, "test-bucket")
        assert not result.empty
        # Should load the latest (2025-03-28)
        call_args = mock_client.download_bytes.call_args
        assert "2025-03-28" in call_args[1]["blob_path"]

    def test_os_error_returns_empty(self):
        handler = _make_handler()
        blob = _make_blob(f"{AGGREGATED_PREFIX}aggregated_instruments_2025-03-28.parquet")
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = [blob]
        mock_client.download_bytes.side_effect = OSError("download failed")
        result = handler._load_latest_aggregated(mock_client, "test-bucket")
        assert result.empty


# ─── run() ───────────────────────────────────────────────────────────────────


class TestAggregateHandlerRun:
    def test_run_returns_error_without_project_id(self):
        # Handler with non-empty config but empty project_id; get_config also returns empty
        handler = AggregateHandler(config={"mode": "aggregate"})
        with patch("instruments_service.cli.handlers.aggregate_handler.get_config") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = ""
            result = handler.run()
        assert result["status"] == "error"

    def test_run_with_redo_all_and_empty_buckets(self):
        handler = _make_handler()
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []
        with (
            patch("instruments_service.cli.handlers.aggregate_handler.get_storage_client", return_value=mock_storage),
            patch(
                "instruments_service.cli.handlers.aggregate_handler.get_instruments_bucket_for_category",
                return_value="test-bucket",
            ),
        ):
            result = handler.run(redo_all=True)
        assert result["status"] == "success"
        assert result["total_instruments"] == 0

    def test_run_handles_category_error(self):
        handler = _make_handler()
        with (
            patch("instruments_service.cli.handlers.aggregate_handler.get_storage_client") as mock_storage,
            patch(
                "instruments_service.cli.handlers.aggregate_handler.get_instruments_bucket_for_category",
                return_value="test-bucket",
            ),
        ):
            mock_storage.side_effect = ValueError("storage error")
            result = handler.run(redo_all=False)
        # Errors are caught per-category, so result is still success
        assert result["status"] == "success"
        assert result["categories_processed"] == 0
