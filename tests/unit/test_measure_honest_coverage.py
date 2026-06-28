"""Unit tests for measure_honest_coverage.py — Bug 1 (freshest bucket) + Bug 2 (prd/non-prd merge).

Covers:
1. Freshest-bucket selection: even when the non-prd bucket has MORE rows, the bucket with
   the newer blob.updated timestamp is selected as primary (Bug 1 fix).
2. Merge with day column: primary prd rows override secondary stale rows on (day, venue,
   data_type) key; expected_unattempted rows from secondary that have no prd counterpart
   are retained (Bug 2 fix).
3. Merge without day column: fallback to concat (no dedup, warning logged).
4. --no-merge flag: skips the secondary-bucket read, uses freshest-wins only.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _load_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "measure_honest_coverage.py"
    spec = importlib.util.spec_from_file_location("_measure_honest_coverage_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_measure_honest_coverage_test"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod() -> ModuleType:
    return _load_module()


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=UTC)


def _make_blob_mock(updated: datetime | None) -> MagicMock:
    blob = MagicMock()
    blob.updated = updated
    return blob


def _make_df_with_day(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_df_no_day(rows: list[dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    return df.drop(columns=["day"], errors="ignore")


class TestFreshestBucketSelection:
    """Bug 1: prefer the bucket with the most recent blob.updated, not the most rows."""

    def test_freshest_wins_over_most_rows(self, mod: ModuleType) -> None:
        """prd bucket (5M rows, updated 2026-06-28) beats legacy (35M rows, updated 2026-06-08)."""
        prd_bucket = f"market-data-tick-cefi-prd-{mod.PROJECT_ID}"
        legacy_bucket = f"market-data-tick-cefi-{mod.PROJECT_ID}"

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
        ] * 5_000_000)
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-01"},
        ] * 35_000_000)

        prd_blob = _make_blob_mock(prd_updated)
        legacy_blob = _make_blob_mock(legacy_updated)

        def fake_get_blob(path: str) -> MagicMock:
            # Called on gcs_client.bucket(name).get_blob(path)
            # We intercept via side_effect on the bucket mock
            raise AssertionError("Should not be called directly — mocked via _get_blob_updated")

        def fake_get_blob_updated(
            client: object, bucket_name: str
        ) -> datetime | None:
            if "prd" in bucket_name:
                return prd_updated
            return legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            if "prd" in bucket_name:
                return prd_df.copy()
            return legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        assert result is not None
        # Primary should be the prd bucket (5M rows fresh), not legacy (35M stale)
        assert len(result) == 5_000_000
        assert (result["capture_status"] == "captured").all()

    def test_row_count_tiebreaker_when_timestamps_equal(self, mod: ModuleType) -> None:
        """When timestamps are identical, fall back to row count (more rows wins)."""
        same_ts = _utc(2026, 6, 28)

        small_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "A", "data_type": "t", "day": "2026-06-28"},
        ])
        large_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t", "day": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "C", "data_type": "t", "day": "2026-06-28"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return same_ts

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            if "prd" in bucket_name:
                return small_df.copy()
            return large_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # With same timestamp, more rows wins as tiebreaker
        assert result is not None
        assert len(result) == 2

    def test_inaccessible_primary_falls_back_to_secondary(self, mod: ModuleType) -> None:
        """If prd bucket is not accessible, use legacy bucket."""
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "X", "data_type": "t", "day": "2026-01-01"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            if "prd" in bucket_name:
                return None  # not accessible
            return _utc(2026, 6, 8)

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            if "prd" in bucket_name:
                return None  # not accessible
            return legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        assert result is not None
        assert len(result) == 1


class TestManifestMerge:
    """Bug 2: prd/non-prd merge correctly combines rows, preferring prd's capture_status."""

    def test_merge_with_day_column_deduplicates_on_shard_key(self, mod: ModuleType) -> None:
        """Primary (prd) captured rows override secondary expected_unattempted for same shard."""
        primary_df = _make_df_with_day([
            # prd has live captured data for day 2026-06-28
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
            {"capture_status": "captured", "venue": "KRAKEN", "data_type": "tick", "day": "2026-06-28"},
        ])
        secondary_df = _make_df_with_day([
            # non-prd has stale expected_unattempted for same shards PLUS unique skeleton rows
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "KRAKEN", "data_type": "tick", "day": "2026-06-28"},
            # unique skeleton rows from non-prd (older days without prd coverage)
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "day": "2026-01-01"},
            {"capture_status": "expected_unattempted", "venue": "KRAKEN", "data_type": "tick", "day": "2026-01-01"},
        ])

        result = mod._merge_manifests(primary_df, secondary_df)

        # Total: 2 prd rows (captured) + 2 unique skeleton rows from non-prd = 4
        assert len(result) == 4

        # Check that the 2026-06-28 rows kept prd's "captured" status
        june28_rows = result[result["day"] == "2026-06-28"]
        assert len(june28_rows) == 2
        assert (june28_rows["capture_status"] == "captured").all()

        # Check that the 2026-01-01 skeleton rows from non-prd are retained
        jan01_rows = result[result["day"] == "2026-01-01"]
        assert len(jan01_rows) == 2
        assert (jan01_rows["capture_status"] == "expected_unattempted").all()

    def test_merge_status_priority_captured_beats_expected_unattempted(self, mod: ModuleType) -> None:
        """captured > attempted_failed > empty_confirmed > expected_unattempted."""
        primary_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "A", "data_type": "t", "day": "2026-06-28"},
            {"capture_status": "attempted_failed", "venue": "B", "data_type": "t", "day": "2026-06-28"},
        ])
        secondary_df = _make_df_with_day([
            # These overlap with primary — secondary's status should be dropped
            {"capture_status": "expected_unattempted", "venue": "A", "data_type": "t", "day": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t", "day": "2026-06-28"},
        ])

        result = mod._merge_manifests(primary_df, secondary_df)

        assert len(result) == 2
        row_a = result[result["venue"] == "A"].iloc[0]
        assert row_a["capture_status"] == "captured"
        row_b = result[result["venue"] == "B"].iloc[0]
        assert row_b["capture_status"] == "attempted_failed"

    def test_merge_without_day_column_concats_with_warning(
        self, mod: ModuleType, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Without day column, fallback to concat (no dedup); warning is logged."""
        primary_df = _make_df_no_day([
            {"capture_status": "captured", "venue": "X", "data_type": "t"},
            {"capture_status": "captured", "venue": "Y", "data_type": "t"},
        ])
        secondary_df = _make_df_no_day([
            {"capture_status": "expected_unattempted", "venue": "X", "data_type": "t"},
            {"capture_status": "expected_unattempted", "venue": "Z", "data_type": "t"},
        ])

        import logging

        with caplog.at_level(logging.WARNING, logger="__main__"):
            result = mod._merge_manifests(primary_df, secondary_df)

        # Concat without dedup: 2 + 2 = 4 rows (potential double-count is documented)
        assert len(result) == 4
        assert any("double-count" in r.message for r in caplog.records)

    def test_no_merge_flag_skips_secondary(self, mod: ModuleType) -> None:
        """With merge=False, only the freshest bucket is used — secondary is not merged."""
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "day": "2025-01-01"},
        ])

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        merge_calls: list[str] = []
        original_merge = mod._merge_manifests

        def tracking_merge(
            df_primary: pd.DataFrame, df_secondary: pd.DataFrame
        ) -> pd.DataFrame:
            merge_calls.append("called")
            return original_merge(df_primary, df_secondary)

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch.object(mod, "_merge_manifests", side_effect=tracking_merge),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # merge=False → _merge_manifests must NOT have been called
        assert len(merge_calls) == 0
        # Only prd rows returned (no skeleton from legacy)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["capture_status"] == "captured"

    def test_merge_enabled_by_default_calls_merge(self, mod: ModuleType) -> None:
        """With merge=True (default), _merge_manifests is called when 2 buckets accessible."""
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "day": "2026-06-28"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "day": "2025-01-01"},
        ])

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        merge_calls: list[str] = []
        original_merge = mod._merge_manifests

        def tracking_merge(
            df_primary: pd.DataFrame, df_secondary: pd.DataFrame
        ) -> pd.DataFrame:
            merge_calls.append("called")
            return original_merge(df_primary, df_secondary)

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch.object(mod, "_merge_manifests", side_effect=tracking_merge),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=True)

        # merge=True → _merge_manifests must have been called
        assert len(merge_calls) == 1
        # Result contains rows from both buckets
        assert result is not None
        assert len(result) == 2
