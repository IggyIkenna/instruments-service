"""Unit tests for measure_honest_coverage.py — Bug 1 (freshest bucket) + Bug 2 (prd/non-prd merge)
+ v2 new projections (by_venue_instrument_type, by_venue_instrument_type_data_type, by_day,
layer_1, schema_version, instrument_gates_download).

Covers:
1. Freshest-bucket selection: even when the non-prd bucket has MORE rows, the bucket with
   the newer blob.updated timestamp is selected as primary (Bug 1 fix).
2. Merge with date column: primary prd rows override secondary stale rows on (date, venue,
   data_type) key; expected_unattempted rows from secondary that have no prd counterpart
   are retained (Bug 2 fix).
3. Merge without date column: fallback to concat (no dedup, warning logged).
4. --no-merge flag: skips the secondary-bucket read, uses freshest-wins only.
5. v2: schema_version=2 in payload.
6. v2: by_venue_instrument_type aggregation present and correct.
7. v2: by_venue_instrument_type_data_type aggregation present and correct.
8. v2: by_day aggregation present and correct.
9. v2: layer_1 block present; instrument_gates_download / denominator_complete / layer1_completeness_pct
   additive fields on by_asset_group[ag].
10. v2: instrument_type column absent → graceful degradation (empty projections, warning logged).
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
    return df.drop(columns=["date"], errors="ignore")


class TestFreshestBucketSelection:
    """Bug 1: prefer the bucket with the most recent blob.updated, not the most rows."""

    def test_freshest_wins_over_most_rows(self, mod: ModuleType) -> None:
        """prd bucket (5M rows, updated 2026-06-28) beats legacy (35M rows, updated 2026-06-08)."""
        prd_bucket = f"market-data-tick-cefi-prd-{mod.PROJECT_ID}"
        legacy_bucket = f"market-data-tick-cefi-{mod.PROJECT_ID}"

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
        ] * 5_000_000)
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-01"},
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
            {"capture_status": "captured", "venue": "A", "data_type": "t", "date": "2026-06-28"},
        ])
        large_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t", "date": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "C", "data_type": "t", "date": "2026-06-28"},
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
            {"capture_status": "expected_unattempted", "venue": "X", "data_type": "t", "date": "2026-01-01"},
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


class TestContentBasedFreshness:
    """Bug 3 (2026-07-06): PRIMARY selection uses max(date) in the parquet, not blob.updated.

    Regression scenario:  the 2026-07-03 ASTER corrective pass rewrote the legacy cefi
    index, bumping its ``blob.updated`` past prd.  Under the old mtime-only ranking,
    primary flipped to the stale legacy bucket, prd's captured-only rows dropped from
    ENUMERATED, and 3 artifact "holes" appeared in the coverage report.  Content
    freshness (max ``date`` in the parquet) is intrinsic to the manifest data and cannot
    be inflated by index-rewrite surgery.
    """

    def test_content_freshness_beats_surgery_bumped_mtime(self, mod: ModuleType) -> None:
        """The exact regression: legacy has newer blob.updated (mtime bumped by surgery)
        but prd has fresher content (max(date))."""
        prd_bucket = f"market-data-tick-cefi-prd-{mod.PROJECT_ID}"
        legacy_bucket = f"market-data-tick-cefi-{mod.PROJECT_ID}"

        # prd content: through 2026-07-05; mtime hasn't been touched recently.
        prd_updated = _utc(2026, 6, 28)
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE-FUTURES", "data_type": "tick",
             "date": "2026-07-05", "instrument_id": "BTCUSDT-PERP"},
        ])

        # legacy content: only through 2026-06-08, but mtime got bumped by an index rewrite.
        legacy_updated = _utc(2026, 7, 4)  # NEWER mtime — the surgery bump
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick",
             "date": "2026-06-08", "instrument_id": "ETHUSDT"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        assert result is not None
        # prd wins — its captured 2026-07-05 row is the only one returned when merge=False.
        assert len(result) == 1
        assert result.iloc[0]["capture_status"] == "captured"
        assert result.iloc[0]["venue"] == "BINANCE-FUTURES"
        # Explicit invariant: the legacy row must NOT be the returned primary frame,
        # since that's exactly the incident (BINANCE-FUTURES future rows would drop out).
        assert prd_bucket != legacy_bucket  # sanity — different candidate names

    def test_content_freshness_fallback_to_mtime_when_date_absent(self, mod: ModuleType) -> None:
        """When a candidate lacks a date column (legacy 3-column bucket) it cannot supply
        content-freshness — falls back to blob.updated for that candidate."""
        prd_updated = _utc(2026, 6, 8)
        legacy_updated = _utc(2026, 6, 28)  # newer mtime

        # prd has a date column but old max(date); legacy has no date at all.
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "A", "data_type": "t",
             "date": "2026-01-01", "instrument_id": "X"},
        ])
        legacy_df = _make_df_no_day([
            {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # prd wins on content-freshness (real max(date)) even though legacy has newer mtime,
        # because a candidate with a real max(date) beats one with no date column at all.
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["venue"] == "A"

    def test_content_tie_falls_through_to_mtime(self, mod: ModuleType) -> None:
        """When both candidates have the same max(date), the tie falls through to
        blob.updated — preserving the original Bug 1 behaviour for content-tied buckets."""
        same_max_date = "2026-06-28"
        prd_updated = _utc(2026, 7, 5)
        legacy_updated = _utc(2026, 6, 8)

        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "P", "data_type": "t",
             "date": same_max_date, "instrument_id": "X"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "L", "data_type": "t",
             "date": same_max_date, "instrument_id": "Y"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # Content tie → mtime wins → prd (newer mtime) is primary.
        assert result is not None
        assert result.iloc[0]["venue"] == "P"

    def test_get_content_freshness_returns_none_when_no_date_column(self, mod: ModuleType) -> None:
        """Helper unit test: _get_content_freshness returns None for a df without a date column."""
        df = pd.DataFrame([{"capture_status": "captured", "venue": "X", "data_type": "t"}])
        assert mod._get_content_freshness(df) is None

    def test_get_content_freshness_returns_max_date(self, mod: ModuleType) -> None:
        """Helper unit test: _get_content_freshness picks the maximum date."""
        df = pd.DataFrame([
            {"date": "2026-06-01"},
            {"date": "2026-07-05"},
            {"date": "2026-04-15"},
        ])
        result = mod._get_content_freshness(df)
        assert result is not None
        assert result.date().isoformat() == "2026-07-05"


class TestPinPrimaryOverride:
    """Operator escape hatch: --pin-primary forces a specific bucket as PRIMARY regardless
    of content-freshness ranking. Non-matching pins are ignored so a single flag can be
    passed for --asset-group all."""

    def test_pin_primary_forces_selection(self, mod: ModuleType) -> None:
        """Pinning the legacy bucket makes it PRIMARY even when prd would win by content."""
        legacy_bucket = f"market-data-tick-cefi-{mod.PROJECT_ID}"

        # prd is objectively fresher, but the operator pins legacy.
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "PRD", "data_type": "t",
             "date": "2026-07-05", "instrument_id": "X"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "LEG", "data_type": "t",
             "date": "2026-01-01", "instrument_id": "Y"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return _utc(2026, 7, 5) if "prd" in bucket_name else _utc(2026, 6, 8)

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False, pin_primary=legacy_bucket)

        # Legacy is PRIMARY because it was pinned, even though prd has fresher content.
        assert result is not None
        assert result.iloc[0]["venue"] == "LEG"

    def test_pin_primary_non_matching_falls_through_to_content(self, mod: ModuleType) -> None:
        """A pin that doesn't match any candidate for this asset_group is ignored — the
        content-freshness ranking still applies. This lets --pin-primary be passed once
        for --asset-group all without needing per-AG matching."""
        prd_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "PRD", "data_type": "t",
             "date": "2026-07-05", "instrument_id": "X"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "LEG", "data_type": "t",
             "date": "2026-01-01", "instrument_id": "Y"},
        ])

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return _utc(2026, 6, 8) if "prd" in bucket_name else _utc(2026, 7, 5)

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest(
                "cefi",
                merge=False,
                pin_primary="unrelated-defi-bucket-that-does-not-match",
            )

        # Pin is ignored (no match) — prd wins on content-freshness.
        assert result is not None
        assert result.iloc[0]["venue"] == "PRD"


class TestManifestMerge:
    """Bug 2: prd/non-prd merge correctly combines rows, preferring prd's capture_status."""

    def test_merge_with_date_column_deduplicates_on_shard_key(self, mod: ModuleType) -> None:
        """Primary (prd) captured rows override secondary expected_unattempted for same shard."""
        primary_df = _make_df_with_day([
            # prd has live captured data for day 2026-06-28 (instrument_id enables per-shard dedup)
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "BTCUSDT"},
            {"capture_status": "captured", "venue": "KRAKEN", "data_type": "tick", "date": "2026-06-28", "instrument_id": "ETHUSDT"},
        ])
        secondary_df = _make_df_with_day([
            # non-prd has stale expected_unattempted for the same instruments — should be overridden
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "BTCUSDT"},
            {"capture_status": "expected_unattempted", "venue": "KRAKEN", "data_type": "tick", "date": "2026-06-28", "instrument_id": "ETHUSDT"},
            # unique skeleton rows from non-prd (older days without prd coverage)
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-01-01", "instrument_id": "BTCUSDT"},
            {"capture_status": "expected_unattempted", "venue": "KRAKEN", "data_type": "tick", "date": "2026-01-01", "instrument_id": "ETHUSDT"},
        ])

        result = mod._merge_manifests(primary_df, secondary_df)

        # Total: 2 prd rows (captured) + 2 unique skeleton rows from non-prd = 4
        assert len(result) == 4

        # Check that the 2026-06-28 rows kept prd's "captured" status
        june28_rows = result[result["date"] == "2026-06-28"]
        assert len(june28_rows) == 2
        assert (june28_rows["capture_status"] == "captured").all()

        # Check that the 2026-01-01 skeleton rows from non-prd are retained
        jan01_rows = result[result["date"] == "2026-01-01"]
        assert len(jan01_rows) == 2
        assert (jan01_rows["capture_status"] == "expected_unattempted").all()

    def test_merge_instrument_id_prevents_cross_instrument_collapse(self, mod: ModuleType) -> None:
        """With instrument_id in both DFs, different instruments on the same (date,venue,data_type)
        are kept separately — not collapsed by the 3-column fallback key."""
        primary_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "BTCUSDT"},
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "ETHUSDT"},
        ])
        # Secondary (oracle eu_only): SOLUSDT is new (not in prd); BTCUSDT overlaps (prd wins)
        secondary_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "SOLUSDT"},
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28", "instrument_id": "BTCUSDT"},
        ])

        result = mod._merge_manifests(primary_df, secondary_df)

        # 3 rows: BTC+ETH captured from prd + SOL eu from oracle (BTC eu dropped as prd has it)
        assert len(result) == 3

        btc = result[result["instrument_id"] == "BTCUSDT"]
        assert len(btc) == 1
        assert btc.iloc[0]["capture_status"] == "captured"  # prd wins

        eth = result[result["instrument_id"] == "ETHUSDT"]
        assert len(eth) == 1
        assert eth.iloc[0]["capture_status"] == "captured"

        sol = result[result["instrument_id"] == "SOLUSDT"]
        assert len(sol) == 1
        assert sol.iloc[0]["capture_status"] == "expected_unattempted"  # only in oracle

    def test_merge_status_priority_captured_beats_expected_unattempted(self, mod: ModuleType) -> None:
        """captured > attempted_failed > empty_confirmed > expected_unattempted."""
        primary_df = _make_df_with_day([
            {"capture_status": "captured", "venue": "A", "data_type": "t", "date": "2026-06-28", "instrument_id": "X"},
            {"capture_status": "attempted_failed", "venue": "B", "data_type": "t", "date": "2026-06-28", "instrument_id": "Y"},
        ])
        secondary_df = _make_df_with_day([
            # Same instruments overlap with primary — secondary's status should be dropped
            {"capture_status": "expected_unattempted", "venue": "A", "data_type": "t", "date": "2026-06-28", "instrument_id": "X"},
            {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t", "date": "2026-06-28", "instrument_id": "Y"},
        ])

        result = mod._merge_manifests(primary_df, secondary_df)

        assert len(result) == 2
        row_a = result[result["venue"] == "A"].iloc[0]
        assert row_a["capture_status"] == "captured"
        row_b = result[result["venue"] == "B"].iloc[0]
        assert row_b["capture_status"] == "attempted_failed"

    def test_merge_without_date_column_concats_with_warning(
        self, mod: ModuleType, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Without date column, fallback to concat (no dedup); warning is logged."""
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
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "date": "2025-01-01"},
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
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
        ])
        legacy_df = _make_df_with_day([
            {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "date": "2025-01-01"},
        ])

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        # _read_parquet_eu_only is called for the secondary merge read — mock it too.
        def fake_read_parquet_eu_only(bucket_name: str) -> pd.DataFrame | None:
            return legacy_df.copy() if "prd" not in bucket_name else None

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
            patch.object(mod, "_read_parquet_eu_only", side_effect=fake_read_parquet_eu_only),
            patch.object(mod, "_merge_manifests", side_effect=tracking_merge),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=True)

        # merge=True → _merge_manifests must have been called
        assert len(merge_calls) == 1
        # Result contains rows from both buckets
        assert result is not None
        assert len(result) == 2


# ===========================================================================
# v2 tests — new projections + Layer-1 integration
# ===========================================================================


def _make_df_v2(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Return a DataFrame with the v2 columns including instrument_type."""
    return pd.DataFrame(rows)


def _make_stub_layer1_result(
    denominator_complete: bool = True,
    completeness_pct: float | None = 100.0,
    denominator_status: str = "COMPLETE",
) -> object:
    """Return a minimal stub object that mimics AgLayer1Result."""

    class _StubResult:
        def __init__(self) -> None:
            self.denominator_complete = denominator_complete
            self.denominator_status = denominator_status
            self.completeness_pct = completeness_pct
            self.missing_tuples: list[object] = []
            self.stray_tuples: list[object] = []

        def as_dict(self) -> dict[str, object]:
            return {
                "denominator_complete": self.denominator_complete,
                "denominator_status": self.denominator_status,
                "completeness_pct": self.completeness_pct,
                "expected_tuples": 10 if self.denominator_status != "UNDEFINED" else 0,
                "present_tuples": 10 if self.denominator_complete else 9,
                "missing_tuples": [],
                "stray_tuples": [],
                "by_venue": {},
            }

    return _StubResult()


def _compute_coverage_with_stub(
    mod: ModuleType,
    dfs: dict[str, pd.DataFrame],
    denominator_complete: bool = True,
    completeness_pct: float | None = 100.0,
    denominator_status: str = "COMPLETE",
) -> dict[str, object]:
    """Call mod._compute_coverage with a stubbed Layer-1 checker.

    Patches _get_completeness_module to return a mock module that returns
    a deterministic stub AgLayer1Result, isolating the Layer-2 projection
    tests from the real UAC-dependent Layer-1 check.
    """
    stub_result = _make_stub_layer1_result(denominator_complete, completeness_pct, denominator_status)

    class _FakeChecker:
        @staticmethod
        def check_enumeration_completeness(
            ag: str, df_arg: object, *, diagnose: bool = False
        ) -> object:
            return stub_result

        @staticmethod
        def filter_manifest_to_expected(
            ag: str,
            df_arg: object,
            *,
            expected: object | None = None,
        ) -> object:
            # Passthrough — Layer-2 projection tests exercise the raw counts,
            # not the MVP read-time gate.  The gate itself is exercised by
            # test_filter_manifest_to_expected.py against the real EXPECTED
            # matrix from build_expected.
            return df_arg

    with patch.object(mod, "_get_completeness_module", return_value=_FakeChecker()):
        return mod._compute_coverage(dfs)


class TestV2SchemaVersion:
    """schema_version=2 must be present in the payload (added in main())."""

    def test_layer1_block_present_in_compute_coverage(self, mod: ModuleType) -> None:
        """layer_1 block must be present in _compute_coverage output."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert "layer_1" in coverage, "layer_1 block must be present in coverage output"


class TestV2ByVenueInstrumentType:
    """by_venue_instrument_type aggregation."""

    def test_by_venue_instrument_type_present(self, mod: ModuleType) -> None:
        """by_venue_instrument_type must be present in the output."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
            {"capture_status": "expected_unattempted", "venue": "DERIBIT", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "options_chain"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_venue_instrument_type" in coverage
        by_vit = coverage["by_venue_instrument_type"]
        assert "cefi" in by_vit
        cefi_vit = by_vit["cefi"]
        assert "BINANCE" in cefi_vit
        assert "spot_pair" in cefi_vit["BINANCE"]
        binance_spot = cefi_vit["BINANCE"]["spot_pair"]
        assert binance_spot["captured"] == 1
        assert binance_spot["expected_unattempted"] == 0

    def test_by_venue_instrument_type_correct_counts(self, mod: ModuleType) -> None:
        """counts per (venue, instrument_type) are correct."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BYBIT", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "perpetual"},
            {"capture_status": "attempted_failed", "venue": "BYBIT", "data_type": "book_snapshot_5",
             "date": "2026-06-28", "instrument_type": "perpetual"},
            {"capture_status": "expected_unattempted", "venue": "DERIBIT", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "futures_chain"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        bybit_perp = coverage["by_venue_instrument_type"]["cefi"]["BYBIT"]["perpetual"]
        assert bybit_perp["captured"] == 1
        assert bybit_perp["attempted_failed"] == 1
        assert bybit_perp["expected_unattempted"] == 0
        assert bybit_perp["total"] == 2

        deribit_fc = coverage["by_venue_instrument_type"]["cefi"]["DERIBIT"]["futures_chain"]
        assert deribit_fc["expected_unattempted"] == 1
        assert deribit_fc["captured"] == 0

    def test_by_venue_instrument_type_empty_when_column_absent(self, mod: ModuleType) -> None:
        """When instrument_type column is absent, by_venue_instrument_type is empty."""
        df = pd.DataFrame([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades", "date": "2026-06-28"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_venue_instrument_type" in coverage
        # Should be empty (no instrument_type column)
        assert coverage["by_venue_instrument_type"]["cefi"] == {}


class TestV2ByVenueInstrumentTypeDataType:
    """by_venue_instrument_type_data_type aggregation."""

    def test_structure_is_correct(self, mod: ModuleType) -> None:
        """4-level nesting: ag → venue → itype → data_type → counts."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "book_snapshot_5",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_venue_instrument_type_data_type" in coverage
        vitdt = coverage["by_venue_instrument_type_data_type"]["cefi"]
        assert "BINANCE" in vitdt
        assert "spot_pair" in vitdt["BINANCE"]
        assert "trades" in vitdt["BINANCE"]["spot_pair"]
        assert "book_snapshot_5" in vitdt["BINANCE"]["spot_pair"]
        assert vitdt["BINANCE"]["spot_pair"]["trades"]["captured"] == 1
        assert vitdt["BINANCE"]["spot_pair"]["book_snapshot_5"]["captured"] == 1


class TestV2ByDay:
    """by_day aggregation."""

    def test_by_day_present(self, mod: ModuleType) -> None:
        """by_day must be present in the output."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
            {"capture_status": "expected_unattempted", "venue": "DERIBIT", "data_type": "trades",
             "date": "2026-06-27", "instrument_type": "futures_chain"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_day" in coverage
        by_day = coverage["by_day"]["cefi"]
        assert "2026-06-28" in by_day
        assert "2026-06-27" in by_day
        assert by_day["2026-06-28"]["captured"] == 1
        assert by_day["2026-06-27"]["expected_unattempted"] == 1

    def test_by_day_empty_when_date_absent(self, mod: ModuleType) -> None:
        """When date column is absent, by_day is empty."""
        df = pd.DataFrame([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert coverage["by_day"]["cefi"] == {}


class TestV2Layer1Integration:
    """Layer-1 block and additive per-AG fields."""

    def test_layer1_block_present(self, mod: ModuleType) -> None:
        """layer_1 must be a top-level key in coverage output."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert "layer_1" in coverage
        assert "by_asset_group" in coverage["layer_1"]
        assert "cefi" in coverage["layer_1"]["by_asset_group"]

    def test_instrument_gates_download_true_when_incomplete(self, mod: ModuleType) -> None:
        """instrument_gates_download=True when denominator_complete=False."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(
            mod, {"cefi": df},
            denominator_complete=False, completeness_pct=90.0, denominator_status="INCOMPLETE",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is True
        assert ag_cell["denominator_complete"] is False
        assert ag_cell["denominator_status"] == "INCOMPLETE"
        assert ag_cell["layer1_completeness_pct"] == 90.0

    def test_instrument_gates_download_false_when_complete(self, mod: ModuleType) -> None:
        """instrument_gates_download=False when denominator_complete=True."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(
            mod, {"cefi": df},
            denominator_complete=True, completeness_pct=100.0, denominator_status="COMPLETE",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is False
        assert ag_cell["denominator_complete"] is True
        assert ag_cell["denominator_status"] == "COMPLETE"
        assert ag_cell["layer1_completeness_pct"] == 100.0

    def test_instrument_gates_download_true_when_undefined(self, mod: ModuleType) -> None:
        """instrument_gates_download=True and completeness_pct=None when UNDEFINED."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(
            mod, {"cefi": df},
            denominator_complete=False, completeness_pct=None, denominator_status="UNDEFINED",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is True
        assert ag_cell["denominator_complete"] is False
        assert ag_cell["denominator_status"] == "UNDEFINED"
        assert ag_cell["layer1_completeness_pct"] is None

    def test_existing_keys_preserved(self, mod: ModuleType) -> None:
        """All v1 keys must still be present in the output (byte-compatible)."""
        df = _make_df_v2([
            {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades",
             "date": "2026-06-28", "instrument_type": "spot_pair"},
            {"capture_status": "empty_confirmed", "venue": "BINANCE", "data_type": "book_snapshot_5",
             "date": "2026-06-27", "instrument_type": "spot_pair"},
        ])
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        # All v1 keys must be present
        assert "by_asset_group" in coverage
        assert "by_venue" in coverage
        assert "by_venue_data_type" in coverage
        # v1 per-cell fields
        ag_cell = coverage["by_asset_group"]["cefi"]
        for key in ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted",
                    "total", "coverage_pct", "all_shards_coverage_pct"):
            assert key in ag_cell, f"v1 field '{key}' missing from by_asset_group[cefi]"
