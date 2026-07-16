"""Unit tests for measure_honest_coverage.py — pinned-primary bucket selection + Bug 2
(prd/non-prd merge) + v2 new projections (by_venue_instrument_type,
by_venue_instrument_type_data_type, by_day, layer_1, schema_version,
instrument_gates_download).

Covers:
1. Pinned-primary selection (hardened 2026-07-06): primary is pinned by tuple order
   (``-prd`` first by construction), NOT by ``blob.updated`` mtime. Surgery on the
   legacy bucket that bumps its mtime past prd no longer flips roles.
2. ``--primary-bucket`` CLI override wins over the tuple-order pin when accessible;
   silently falls back to the pin when the named bucket is inaccessible.
3. Surgery-signal warning: a secondary bucket with newer ``blob.updated`` than primary
   logs a WARNING (informational; does not affect selection).
4. Merge with date column: primary prd rows override secondary stale rows on (date, venue,
   data_type) key; expected_unattempted rows from secondary that have no prd counterpart
   are retained (Bug 2 fix).
5. Merge without date column: fallback to concat (no dedup, warning logged).
6. --no-merge flag: skips the secondary-bucket read, uses primary-only.
7. v2: schema_version=2 in payload.
6. v2: by_venue_instrument_type aggregation present and correct.
7. v2: by_venue_instrument_type_data_type aggregation present and correct.
8. v2: by_day aggregation present and correct.
9. v2: layer_1 block present; instrument_gates_download / denominator_complete / layer1_completeness_pct
   additive fields on by_asset_group[ag].
10. v2: instrument_type column absent → graceful degradation (empty projections, warning logged).

Column-prune hardening (defence-in-depth, 2026-07-16 — see data_status_page_ux_and_canonicalisation
plan § P1 "remaining hardening"): _read_parquet_safe/_read_parquet_eu_only now pass
``read_dictionary=<columns>`` so the parquet's on-disk PLAIN_DICTIONARY encoding survives as pandas
``category`` dtype instead of being expanded into a python-object string per row (measured ~38.6x
memory reduction on a real 1.96M-row bucket; see the plan's Progress Log for the cited numbers).
That dtype change has two correctness footguns which are also covered here:
11. ``_read_parquet_safe`` (all 4 fallback tiers) returns ``category`` dtype for the columns it reads,
    with values identical to a plain object-dtype read (real local-parquet round-trip, not mocked).
12. ``_merge_manifests``'s priority column stays correct (numeric sort, not category-discovery-order
    sort) even when ``capture_status`` is ``category`` dtype — ``Series.map()`` on a Categorical
    returns a Categorical result whose default sort order is NOT the numeric value order, which would
    silently pick the wrong "best status" per shard without the ``.astype("int64")`` guard.
13. ``_compute_coverage``'s groupby calls pass ``observed=True`` so a category dtype grouper column
    never synthesises a phantom empty group for an unobserved category combination (which would
    otherwise inject bogus zero-count venue/data_type/instrument_type cells into the coverage output).
14. Categorical-input and object-dtype-input runs of ``_compute_coverage`` produce byte-identical
    output for the same underlying data (dtype is an implementation detail, not a behavior change).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

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


def _make_df_with_day(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_df_no_day(rows: list[dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    return df.drop(columns=["date"], errors="ignore")


class TestPinnedPrimarySelection:
    """Pinned-primary bucket selection (hardened 2026-07-06 vs. surgery-bumped mtimes).

    PRIMARY is the first accessible candidate in ``_MANIFEST_BUCKET_CANDIDATES`` tuple
    order (which places ``-prd`` first for every asset_group). ``blob.updated`` mtime
    is logged for visibility but no longer drives selection.
    """

    def test_prd_wins_over_legacy_by_tuple_order(self, mod: ModuleType) -> None:
        """prd (fewer rows, first in tuple) beats legacy (more rows) — row count irrelevant.

        Row counts reduced 1000x from the original 5M/35M (2026-07-16 — unrelated pre-existing
        flake found while shipping the column-prune hardening below: the 5M/35M
        list-of-identical-dicts -> pd.DataFrame construction reproducibly hung under
        pytest-xdist on this host, CPU-idle for 4+ minutes past a 280s pytest-timeout —
        an execnet/xdist IPC stall, not a real slowdown in the code under test. The
        assertion only needs "legacy has strictly more rows than prd" to prove row count
        isn't a tiebreaker; 5_000/35_000 preserves that at negligible construction cost.
        """
        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            ]
            * 5_000
        )
        legacy_df = _make_df_with_day(
            [
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-01",
                },
            ]
            * 35_000
        )

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
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
        assert len(result) == 5_000
        assert (result["capture_status"] == "captured").all()

    def test_pinned_primary_wins_when_secondary_mtime_is_newer(
        self, mod: ModuleType, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Legacy surgery bumps its mtime past prd — pinned prd still wins (regression guard)."""
        # This is the exact 2026-07-03 ASTER-corrective-pass scenario: rewriting the
        # legacy cefi index bumped its blob.updated past prd's. Under the old mtime-based
        # selection, roles flipped and prd's captured-only tuples dropped from ENUMERATED.
        prd_updated = _utc(2026, 6, 28)
        legacy_updated_after_surgery = _utc(2026, 7, 3)  # newer than prd

        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE-FUTURES", "data_type": "future", "date": "2026-06-29"},
            ]
        )
        legacy_df = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "t", "date": "2026-06-01"},
            ]
        )

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated_after_surgery

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
            caplog.at_level("WARNING"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # PINNED primary wins even though legacy has newer mtime.
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["capture_status"] == "captured"
        # Surgery-signal warning fired so operators can see the anomaly in logs.
        assert any("SURGERY-SIGNAL" in r.message for r in caplog.records), (
            "expected SURGERY-SIGNAL warning when secondary bucket has newer mtime"
        )

    def test_row_count_no_longer_a_tiebreaker(self, mod: ModuleType) -> None:
        """Under pinned selection, prd wins regardless of row count when both accessible."""
        same_ts = _utc(2026, 6, 28)

        small_prd = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "A", "data_type": "t", "date": "2026-06-28"},
            ]
        )
        large_legacy = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "B", "data_type": "t", "date": "2026-06-28"},
                {"capture_status": "expected_unattempted", "venue": "C", "data_type": "t", "date": "2026-06-28"},
            ]
        )

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return same_ts

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return small_prd.copy() if "prd" in bucket_name else large_legacy.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest("cefi", merge=False)

        # prd (1 row) wins over legacy (2 rows) — pinned by tuple order, not row count.
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["capture_status"] == "captured"

    def test_inaccessible_primary_falls_back_to_secondary(self, mod: ModuleType) -> None:
        """If prd bucket is not accessible, use legacy bucket."""
        legacy_df = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "X", "data_type": "t", "date": "2026-01-01"},
            ]
        )

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


class TestPrimaryBucketOverride:
    """``--primary-bucket=<name>`` operator override for surgery/debugging."""

    def test_override_wins_over_tuple_pin_when_accessible(self, mod: ModuleType) -> None:
        """Override selects legacy as primary even though prd is first in tuple + accessible."""
        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            ]
        )
        legacy_df = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "t", "date": "2026-01-01"},
                {"capture_status": "expected_unattempted", "venue": "OLD2", "data_type": "t", "date": "2026-01-02"},
            ]
        )

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return _utc(2026, 6, 28) if "prd" in bucket_name else _utc(2026, 6, 8)

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        legacy_bucket = f"market-data-tick-cefi-{mod.PROJECT_ID}"

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
        ):
            result = mod._read_manifest(
                "cefi",
                merge=False,
                primary_bucket_override=legacy_bucket,
            )

        # Override wins → legacy's 2 rows are primary.
        assert result is not None
        assert len(result) == 2
        assert (result["capture_status"] == "expected_unattempted").all()

    def test_override_falls_back_to_pin_when_not_accessible(
        self, mod: ModuleType, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Override to a name not in accessible candidates → tuple-order pin wins + warning."""
        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            ]
        )
        legacy_df = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "t", "date": "2026-01-01"},
            ]
        )

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return _utc(2026, 6, 28) if "prd" in bucket_name else _utc(2026, 6, 8)

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        with (
            patch.object(mod, "_get_blob_updated", side_effect=fake_get_blob_updated),
            patch.object(mod, "_read_parquet_safe", side_effect=fake_read_parquet_safe),
            patch("google.cloud.storage.Client"),
            caplog.at_level("WARNING"),
        ):
            result = mod._read_manifest(
                "cefi",
                merge=False,
                primary_bucket_override="does-not-exist-in-candidates",
            )

        # Fell back to tuple-order pin → prd is primary.
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["capture_status"] == "captured"
        # Fallback warning logged so operator notices the typo/miss.
        assert any(
            "not accessible" in r.message and "does-not-exist-in-candidates" in r.message for r in caplog.records
        )


class TestManifestMerge:
    """Bug 2: prd/non-prd merge correctly combines rows, preferring prd's capture_status."""

    def test_merge_with_date_column_deduplicates_on_shard_key(self, mod: ModuleType) -> None:
        """Primary (prd) captured rows override secondary expected_unattempted for same shard."""
        primary_df = _make_df_with_day(
            [
                # prd has live captured data for day 2026-06-28 (instrument_id enables per-shard dedup)
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "BTCUSDT",
                },
                {
                    "capture_status": "captured",
                    "venue": "KRAKEN",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "ETHUSDT",
                },
            ]
        )
        secondary_df = _make_df_with_day(
            [
                # non-prd has stale expected_unattempted for the same instruments — should be overridden
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "BTCUSDT",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "KRAKEN",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "ETHUSDT",
                },
                # unique skeleton rows from non-prd (older days without prd coverage)
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-01-01",
                    "instrument_id": "BTCUSDT",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "KRAKEN",
                    "data_type": "tick",
                    "date": "2026-01-01",
                    "instrument_id": "ETHUSDT",
                },
            ]
        )

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
        primary_df = _make_df_with_day(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "BTCUSDT",
                },
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "ETHUSDT",
                },
            ]
        )
        # Secondary (oracle eu_only): SOLUSDT is new (not in prd); BTCUSDT overlaps (prd wins)
        secondary_df = _make_df_with_day(
            [
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "SOLUSDT",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                    "instrument_id": "BTCUSDT",
                },
            ]
        )

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
        primary_df = _make_df_with_day(
            [
                {
                    "capture_status": "captured",
                    "venue": "A",
                    "data_type": "t",
                    "date": "2026-06-28",
                    "instrument_id": "X",
                },
                {
                    "capture_status": "attempted_failed",
                    "venue": "B",
                    "data_type": "t",
                    "date": "2026-06-28",
                    "instrument_id": "Y",
                },
            ]
        )
        secondary_df = _make_df_with_day(
            [
                # Same instruments overlap with primary — secondary's status should be dropped
                {
                    "capture_status": "expected_unattempted",
                    "venue": "A",
                    "data_type": "t",
                    "date": "2026-06-28",
                    "instrument_id": "X",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "B",
                    "data_type": "t",
                    "date": "2026-06-28",
                    "instrument_id": "Y",
                },
            ]
        )

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
        primary_df = _make_df_no_day(
            [
                {"capture_status": "captured", "venue": "X", "data_type": "t"},
                {"capture_status": "captured", "venue": "Y", "data_type": "t"},
            ]
        )
        secondary_df = _make_df_no_day(
            [
                {"capture_status": "expected_unattempted", "venue": "X", "data_type": "t"},
                {"capture_status": "expected_unattempted", "venue": "Z", "data_type": "t"},
            ]
        )

        import logging

        with caplog.at_level(logging.WARNING, logger="__main__"):
            result = mod._merge_manifests(primary_df, secondary_df)

        # Concat without dedup: 2 + 2 = 4 rows (potential double-count is documented)
        assert len(result) == 4
        assert any("double-count" in r.message for r in caplog.records)

    def test_no_merge_flag_skips_secondary(self, mod: ModuleType) -> None:
        """With merge=False, only the primary (pinned) bucket is used — secondary is not merged."""
        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            ]
        )
        legacy_df = _make_df_with_day(
            [
                {
                    "capture_status": "expected_unattempted",
                    "venue": "BINANCE",
                    "data_type": "tick",
                    "date": "2026-06-28",
                },
                {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "date": "2025-01-01"},
            ]
        )

        prd_updated = _utc(2026, 6, 28)
        legacy_updated = _utc(2026, 6, 8)

        def fake_get_blob_updated(client: object, bucket_name: str) -> datetime | None:
            return prd_updated if "prd" in bucket_name else legacy_updated

        def fake_read_parquet_safe(bucket_name: str) -> pd.DataFrame | None:
            return prd_df.copy() if "prd" in bucket_name else legacy_df.copy()

        merge_calls: list[str] = []
        original_merge = mod._merge_manifests

        def tracking_merge(df_primary: pd.DataFrame, df_secondary: pd.DataFrame) -> pd.DataFrame:
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
        prd_df = _make_df_with_day(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "tick", "date": "2026-06-28"},
            ]
        )
        legacy_df = _make_df_with_day(
            [
                {"capture_status": "expected_unattempted", "venue": "OLD", "data_type": "tick", "date": "2025-01-01"},
            ]
        )

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

        def tracking_merge(df_primary: pd.DataFrame, df_secondary: pd.DataFrame) -> pd.DataFrame:
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
        def check_enumeration_completeness(ag: str, df_arg: object, *, diagnose: bool = False) -> object:
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
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert "layer_1" in coverage, "layer_1 block must be present in coverage output"


class TestV2ByVenueInstrumentType:
    """by_venue_instrument_type aggregation."""

    def test_by_venue_instrument_type_present(self, mod: ModuleType) -> None:
        """by_venue_instrument_type must be present in the output."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "DERIBIT",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "options_chain",
                },
            ]
        )
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
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BYBIT",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "perpetual",
                },
                {
                    "capture_status": "attempted_failed",
                    "venue": "BYBIT",
                    "data_type": "book_snapshot_5",
                    "date": "2026-06-28",
                    "instrument_type": "perpetual",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "DERIBIT",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "futures_chain",
                },
            ]
        )
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
        df = pd.DataFrame(
            [
                {"capture_status": "captured", "venue": "BINANCE", "data_type": "trades", "date": "2026-06-28"},
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_venue_instrument_type" in coverage
        # Should be empty (no instrument_type column)
        assert coverage["by_venue_instrument_type"]["cefi"] == {}


class TestV2ByVenueInstrumentTypeDataType:
    """by_venue_instrument_type_data_type aggregation."""

    def test_structure_is_correct(self, mod: ModuleType) -> None:
        """4-level nesting: ag → venue → itype → data_type → counts."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "book_snapshot_5",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
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
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
                {
                    "capture_status": "expected_unattempted",
                    "venue": "DERIBIT",
                    "data_type": "trades",
                    "date": "2026-06-27",
                    "instrument_type": "futures_chain",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        assert "by_day" in coverage
        by_day = coverage["by_day"]["cefi"]
        assert "2026-06-28" in by_day
        assert "2026-06-27" in by_day
        assert by_day["2026-06-28"]["captured"] == 1
        assert by_day["2026-06-27"]["expected_unattempted"] == 1

    def test_by_day_empty_when_date_absent(self, mod: ModuleType) -> None:
        """When date column is absent, by_day is empty."""
        df = pd.DataFrame(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert coverage["by_day"]["cefi"] == {}


class TestV2Layer1Integration:
    """Layer-1 block and additive per-AG fields."""

    def test_layer1_block_present(self, mod: ModuleType) -> None:
        """layer_1 must be a top-level key in coverage output."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})
        assert "layer_1" in coverage
        assert "by_asset_group" in coverage["layer_1"]
        assert "cefi" in coverage["layer_1"]["by_asset_group"]

    def test_instrument_gates_download_true_when_incomplete(self, mod: ModuleType) -> None:
        """instrument_gates_download=True when denominator_complete=False."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(
            mod,
            {"cefi": df},
            denominator_complete=False,
            completeness_pct=90.0,
            denominator_status="INCOMPLETE",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is True
        assert ag_cell["denominator_complete"] is False
        assert ag_cell["denominator_status"] == "INCOMPLETE"
        assert ag_cell["layer1_completeness_pct"] == 90.0

    def test_instrument_gates_download_false_when_complete(self, mod: ModuleType) -> None:
        """instrument_gates_download=False when denominator_complete=True."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(
            mod,
            {"cefi": df},
            denominator_complete=True,
            completeness_pct=100.0,
            denominator_status="COMPLETE",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is False
        assert ag_cell["denominator_complete"] is True
        assert ag_cell["denominator_status"] == "COMPLETE"
        assert ag_cell["layer1_completeness_pct"] == 100.0

    def test_instrument_gates_download_true_when_undefined(self, mod: ModuleType) -> None:
        """instrument_gates_download=True and completeness_pct=None when UNDEFINED."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(
            mod,
            {"cefi": df},
            denominator_complete=False,
            completeness_pct=None,
            denominator_status="UNDEFINED",
        )

        ag_cell = coverage["by_asset_group"]["cefi"]
        assert ag_cell["instrument_gates_download"] is True
        assert ag_cell["denominator_complete"] is False
        assert ag_cell["denominator_status"] == "UNDEFINED"
        assert ag_cell["layer1_completeness_pct"] is None

    def test_existing_keys_preserved(self, mod: ModuleType) -> None:
        """All v1 keys must still be present in the output (byte-compatible)."""
        df = _make_df_v2(
            [
                {
                    "capture_status": "captured",
                    "venue": "BINANCE",
                    "data_type": "trades",
                    "date": "2026-06-28",
                    "instrument_type": "spot_pair",
                },
                {
                    "capture_status": "empty_confirmed",
                    "venue": "BINANCE",
                    "data_type": "book_snapshot_5",
                    "date": "2026-06-27",
                    "instrument_type": "spot_pair",
                },
            ]
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        # All v1 keys must be present
        assert "by_asset_group" in coverage
        assert "by_venue" in coverage
        assert "by_venue_data_type" in coverage
        # v1 per-cell fields
        ag_cell = coverage["by_asset_group"]["cefi"]
        for key in (
            "captured",
            "empty_confirmed",
            "attempted_failed",
            "expected_unattempted",
            "total",
            "coverage_pct",
            "all_shards_coverage_pct",
        ):
            assert key in ag_cell, f"v1 field '{key}' missing from by_asset_group[cefi]"


# ===========================================================================
# Column-prune hardening (defence-in-depth, 2026-07-16) — read_dictionary
# category-dtype preservation + its two correctness footguns (categorical
# .map() sort order, groupby phantom-group generation with observed=False).
# ===========================================================================


class TestReadParquetSafeColumnSelection:
    """_read_parquet_safe reads with read_dictionary — real local-parquet round-trip.

    A local file is used (not a GCS mock) so the test exercises the ACTUAL pandas/
    pyarrow dtype behavior, not just that the right kwarg was passed. pandas'
    read_parquet is monkeypatched to redirect the gs:// URI to a local path while
    forwarding every kwarg (columns, read_dictionary, filters) unchanged.
    """

    def _write_local_index(self, path: Path, *, with_iid: bool = True, with_itype: bool = True) -> pd.DataFrame:
        rows: dict[str, list[object]] = {
            "capture_status": ["captured", "expected_unattempted"] * 500,
            "venue": ["BINANCE", "KRAKEN"] * 500,
            "data_type": ["trades"] * 1000,
            "date": ["2026-06-28"] * 1000,
        }
        if with_iid:
            rows["instrument_id"] = [f"SYM{i % 50}" for i in range(1000)]
        if with_itype:
            rows["instrument_type"] = ["spot_pair", "perpetual"] * 500
        df = pd.DataFrame(rows)
        df.to_parquet(path, engine="pyarrow")
        return df

    def _patch_read_parquet_to_local(self, mod: ModuleType, local_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        real_read_parquet = pd.read_parquet

        def fake_read_parquet(uri: str, **kwargs: object) -> pd.DataFrame:
            assert uri.startswith("gs://"), f"expected a gs:// URI, got {uri!r}"
            return real_read_parquet(local_path, **kwargs)

        monkeypatch.setattr(mod.pd, "read_parquet", fake_read_parquet)

    def test_full_read_preserves_dictionary_encoding_as_category(
        self, mod: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Tier 1 (all 6 columns): instrument_id comes back as category dtype with identical values."""
        local_path = tmp_path / "availability_index.parquet"
        source_df = self._write_local_index(local_path)
        self._patch_read_parquet_to_local(mod, local_path, monkeypatch)

        result = mod._read_parquet_safe("fake-bucket")

        assert result is not None
        assert len(result) == 1000
        assert str(result["instrument_id"].dtype) == "category"
        assert set(result["instrument_id"].unique()) == set(source_df["instrument_id"].unique())
        assert result["instrument_id"].nunique() == 50
        # Values are unchanged, not just the dtype label — spot check a few rows.
        assert result["instrument_id"].tolist() == source_df["instrument_id"].tolist()
        assert result["capture_status"].tolist() == source_df["capture_status"].tolist()

    def test_fallback_tier_without_instrument_id_still_returns_category_dtype(
        self, mod: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Legacy bucket lacking instrument_id: falls back to tier 2, other columns still categorical."""
        local_path = tmp_path / "availability_index.parquet"
        source_df = self._write_local_index(local_path, with_iid=False)
        self._patch_read_parquet_to_local(mod, local_path, monkeypatch)

        result = mod._read_parquet_safe("fake-bucket")

        assert result is not None
        assert "instrument_id" not in result.columns
        assert len(result) == 1000
        assert str(result["venue"].dtype) == "category"
        assert str(result["instrument_type"].dtype) == "category"
        assert result["venue"].tolist() == source_df["venue"].tolist()

    def test_eu_only_read_preserves_category_dtype(
        self, mod: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_read_parquet_eu_only also gets the read_dictionary treatment (dtype-consistent with primary)."""
        local_path = tmp_path / "availability_index.parquet"
        self._write_local_index(local_path)
        self._patch_read_parquet_to_local(mod, local_path, monkeypatch)

        result = mod._read_parquet_eu_only("fake-bucket")

        assert result is not None
        assert (result["capture_status"] == "expected_unattempted").all()
        assert str(result["instrument_id"].dtype) == "category"


class TestCategoricalDtypeCorrectness:
    """Regression coverage for the two footguns introduced by category-dtype columns.

    Both were found empirically (real pandas 2.3.3 behavior) while implementing the
    read_dictionary hardening, NOT assumed — see the inline comments in
    measure_honest_coverage.py at _merge_manifests and _compute_coverage's groupby calls.
    """

    def test_merge_priority_correct_with_categorical_capture_status(self, mod: ModuleType) -> None:
        """Regression: Series.map() on a Categorical returns a Categorical whose default sort
        order is category-discovery order, NOT numeric value order — verified directly:
        mapping {captured:0, attempted_failed:1, expected_unattempted:3} over a Categorical
        ['captured','expected_unattempted','attempted_failed'] sorts as [1,0,3] uncast vs the
        correct [0,1,3] after .astype('int64'). Without the guard in _merge_manifests, the
        "best status wins" merge would keep the WRONG status per shard.
        """
        primary_df = pd.DataFrame(
            {
                "date": pd.Categorical(["2026-06-28", "2026-06-28"]),
                "venue": pd.Categorical(["BINANCE", "BINANCE"]),
                "instrument_id": pd.Categorical(["BTCUSDT", "ETHUSDT"]),
                "data_type": pd.Categorical(["tick", "tick"]),
                "capture_status": pd.Categorical(["captured", "attempted_failed"]),
            }
        )
        secondary_df = pd.DataFrame(
            {
                "date": pd.Categorical(["2026-06-28", "2026-06-28"]),
                "venue": pd.Categorical(["BINANCE", "BINANCE"]),
                "instrument_id": pd.Categorical(["BTCUSDT", "ETHUSDT"]),
                "data_type": pd.Categorical(["tick", "tick"]),
                "capture_status": pd.Categorical(["expected_unattempted", "expected_unattempted"]),
            }
        )

        result = mod._merge_manifests(primary_df, secondary_df)

        assert len(result) == 2
        btc = result[result["instrument_id"] == "BTCUSDT"].iloc[0]
        eth = result[result["instrument_id"] == "ETHUSDT"].iloc[0]
        # Primary's higher-priority status must win for BOTH shards — if the categorical
        # sort-order bug were present, the wrong row could survive drop_duplicates instead.
        assert btc["capture_status"] == "captured"
        assert eth["capture_status"] == "attempted_failed"

    def test_compute_coverage_no_phantom_groups_with_categorical_columns(self, mod: ModuleType) -> None:
        """Regression: groupby(observed=False) (pandas default) would synthesise an empty
        group for every category combination that was declared but never co-occurs in the
        data — injecting bogus zero-count cells. observed=True must suppress this.
        """
        df = pd.DataFrame(
            {
                "capture_status": pd.Categorical(["captured", "expected_unattempted"]),
                "venue": pd.Categorical(["BINANCE", "DERIBIT"]),
                "data_type": pd.Categorical(["trades", "trades"]),
                "date": pd.Categorical(["2026-06-28", "2026-06-28"]),
                "instrument_type": pd.Categorical(["spot_pair", "options_chain"]),
            }
        )
        coverage = _compute_coverage_with_stub(mod, {"cefi": df})

        vdt = coverage["by_venue_data_type"]["cefi"]
        # Only the two ACTUALLY-observed (venue, data_type) combos may appear — no phantom
        # (DERIBIT would be a phantom source only if venue/data_type categories diverged from
        # what's observed; the real risk is at the (venue, instrument_type) / 4-way levels
        # where BINANCE never appears with options_chain and DERIBIT never appears with
        # spot_pair in this data).
        assert set(vdt.keys()) == {"BINANCE", "DERIBIT"}
        assert set(vdt["BINANCE"].keys()) == {"trades"}
        assert set(vdt["DERIBIT"].keys()) == {"trades"}

        vit = coverage["by_venue_instrument_type"]["cefi"]
        # BINANCE must NOT have an options_chain cell, and DERIBIT must NOT have a
        # spot_pair cell — those are the phantom combinations observed=False would add.
        assert "options_chain" not in vit["BINANCE"], "phantom empty group leaked into by_venue_instrument_type"
        assert "spot_pair" not in vit["DERIBIT"], "phantom empty group leaked into by_venue_instrument_type"
        assert set(vit["BINANCE"].keys()) == {"spot_pair"}
        assert set(vit["DERIBIT"].keys()) == {"options_chain"}

    def test_compute_coverage_identical_for_categorical_vs_object_dtype(self, mod: ModuleType) -> None:
        """dtype must be an implementation detail: categorical-column input and plain
        object-dtype input over the SAME rows must produce byte-identical coverage output.
        """
        rows = [
            {
                "capture_status": "captured",
                "venue": "BINANCE",
                "data_type": "trades",
                "date": "2026-06-28",
                "instrument_type": "spot_pair",
            },
            {
                "capture_status": "expected_unattempted",
                "venue": "DERIBIT",
                "data_type": "trades",
                "date": "2026-06-27",
                "instrument_type": "options_chain",
            },
            {
                "capture_status": "attempted_failed",
                "venue": "BINANCE",
                "data_type": "book_snapshot_5",
                "date": "2026-06-28",
                "instrument_type": "spot_pair",
            },
        ]
        df_object = pd.DataFrame(rows)
        df_categorical = df_object.astype(
            {
                "capture_status": "category",
                "venue": "category",
                "data_type": "category",
                "date": "category",
                "instrument_type": "category",
            }
        )

        coverage_object = _compute_coverage_with_stub(mod, {"cefi": df_object})
        coverage_categorical = _compute_coverage_with_stub(mod, {"cefi": df_categorical})

        assert coverage_object == coverage_categorical
