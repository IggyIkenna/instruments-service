"""Unit tests for canonicalize_defi_manifest_data_types_2026_05_16.py.

All tests are credential-free and GCS-free.  The GCS bucket + blob are mocked
via ``unittest.mock.MagicMock`` so no network access occurs.  Tests exercise:

  1. test_dry_run_reports_drift_no_writes — dry-run mode: drift detected, no upload
  2. test_apply_flips_kebab_to_snake_in_canonical_manifest — apply mode: kebab→snake
  3. test_idempotent_rerun_skips_already_canonical_bucket — zero kebab rows → skipped
  4. test_apply_requires_confirm_safety_belt — --apply without --confirm returns 1
  5. test_single_bucket_filter_narrows_scope — --bucket filters to one family
  6. test_unknown_bucket_arg_rejected — invalid --bucket value returns 1
  7. test_preserves_other_columns_v8_round_trip — v8 unknown columns pass through
  8. test_per_bucket_summary_counts_correct — summary counts match actual data

Plan ref: ``plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md``
§ Option A — workspace-wide canonicalisation
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Module loader — mirrors pattern from test_reconcile_lending_indices_phantom.py
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    """Load canonicalize_defi_manifest_data_types_2026_05_16 as a module from disk."""
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "canonicalize_defi_manifest_data_types_2026_05_16.py"
    module_name = "_canonicalize_defi_manifest_data_types_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manifest(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Return a minimal manifest DataFrame."""
    defaults: dict[str, object] = {
        "capture_status": "captured",
        "data_type": "lending_indices",
        "venue": "aave_v3",
        "chain": "ETHEREUM",
        "date": "2026-05-07",
        "instrument_type": "lending",
        "error_reason": "",
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows)


def _df_to_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _mock_blob_with_df(df: pd.DataFrame) -> MagicMock:
    """Return a mock GCS Blob whose download_to_filename writes *df* to disk
    and whose upload_from_file stores the uploaded bytes on blob.last_upload."""
    blob = MagicMock()
    captured_uploads: list[bytes] = []
    blob.last_upload = captured_uploads

    def _download(path: str) -> None:
        df.to_parquet(path, index=False)

    def _upload(fileobj: io.BytesIO, content_type: str = "") -> None:
        captured_uploads.append(fileobj.read())

    blob.download_to_filename.side_effect = _download
    blob.upload_from_file.side_effect = _upload
    return blob


def _mock_bucket_with_blob(blob: MagicMock) -> MagicMock:
    bucket = MagicMock()
    bucket.blob.return_value = blob
    return bucket


def _mock_client_with_buckets(family_to_df: dict[str, pd.DataFrame], project_id: str) -> MagicMock:
    """Build a mock storage.Client where each bucket family gets its own blob."""
    blobs: dict[str, MagicMock] = {}
    buckets: dict[str, MagicMock] = {}
    for family, df in family_to_df.items():
        b = _mock_blob_with_df(df)
        blobs[family] = b
        bucket = _mock_bucket_with_blob(b)
        bname = f"{family}-{project_id}"
        buckets[bname] = bucket

    def _get_bucket(name: str) -> MagicMock:
        if name in buckets:
            return buckets[name]
        # Unknown bucket — return a passthrough mock that will raise on download.
        m = MagicMock()
        m.blob.return_value = MagicMock(side_effect=Exception(f"Unknown bucket {name}"))
        return m

    client = MagicMock()
    client.bucket.side_effect = _get_bucket
    return client, blobs


# ---------------------------------------------------------------------------
# Test 1 — dry-run reports drift, no GCS upload
# ---------------------------------------------------------------------------


def test_dry_run_reports_drift_no_writes() -> None:
    """dry-run mode: kebab rows are detected + counted but no upload occurs."""
    df = _make_manifest([
        {"data_type": "lending-indices"},
        {"data_type": "lending-indices"},
        {"data_type": "lending_indices"},
    ])
    blob = _mock_blob_with_df(df)
    bucket = _mock_bucket_with_blob(blob)

    client = MagicMock()
    client.bucket.return_value = bucket

    with patch("google.cloud.storage.Client", return_value=client):
        kebab_count, total = _mod._process_bucket(
            family="lending-indices",
            project_id=_mod.PROJECT_ID,
            client=client,
            apply=False,
        )

    assert kebab_count == 2, f"Expected 2 kebab rows, got {kebab_count}"
    assert total == 3
    blob.upload_from_file.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2 — apply flips kebab → snake in canonical manifest
# ---------------------------------------------------------------------------


def test_apply_flips_kebab_to_snake_in_canonical_manifest() -> None:
    """apply mode: kebab data_type values are renamed to snake in the uploaded manifest."""
    df = _make_manifest([
        {"data_type": "dex-swaps"},
        {"data_type": "dex_swaps"},
        {"data_type": "dex-swaps"},
    ])
    blob = _mock_blob_with_df(df)
    bucket = _mock_bucket_with_blob(blob)
    client = MagicMock()
    client.bucket.return_value = bucket

    kebab_count, total = _mod._process_bucket(
        family="dex-swaps",
        project_id=_mod.PROJECT_ID,
        client=client,
        apply=True,
    )

    assert kebab_count == 2
    assert total == 3
    blob.upload_from_file.assert_called_once()

    # Verify the uploaded bytes contain only snake-form data_type.
    uploaded_bytes = blob.last_upload[0]
    uploaded_df = pd.read_parquet(io.BytesIO(uploaded_bytes))
    assert (uploaded_df["data_type"] == "dex_swaps").all(), (
        f"Expected all data_type == 'dex_swaps'; got: {uploaded_df['data_type'].unique()}"
    )
    assert "dex-swaps" not in uploaded_df["data_type"].values


# ---------------------------------------------------------------------------
# Test 3 — idempotent: already-canonical bucket skipped
# ---------------------------------------------------------------------------


def test_idempotent_rerun_skips_already_canonical_bucket() -> None:
    """Bucket with zero kebab rows → returns (0, total) and no upload."""
    df = _make_manifest([
        {"data_type": "oracle_prices"},
        {"data_type": "oracle_prices"},
    ])
    blob = _mock_blob_with_df(df)
    bucket = _mock_bucket_with_blob(blob)
    client = MagicMock()
    client.bucket.return_value = bucket

    kebab_count, total = _mod._process_bucket(
        family="oracle-prices",
        project_id=_mod.PROJECT_ID,
        client=client,
        apply=True,
    )

    assert kebab_count == 0, "Already-canonical bucket should return 0 kebab rows"
    assert total == 2
    blob.upload_from_file.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4 — --apply without --confirm returns exit code 1
# ---------------------------------------------------------------------------


def test_apply_requires_confirm_safety_belt() -> None:
    """main() with --apply but no --confirm must return 1 (safety belt)."""
    with patch("sys.argv", ["canonicalize_defi_manifest_data_types_2026_05_16.py", "--apply"]):
        result = _mod.main()
    assert result == 1, "Expected exit code 1 when --apply used without --confirm"


# ---------------------------------------------------------------------------
# Test 5 — --bucket filter narrows scope to single family
# ---------------------------------------------------------------------------


def test_single_bucket_filter_narrows_scope() -> None:
    """--bucket <family> processes only that family; other families untouched."""
    df_lending = _make_manifest([{"data_type": "lending-indices"}, {"data_type": "lending_indices"}])
    df_perp = _make_manifest([{"data_type": "perp-funding"}, {"data_type": "perp_funding"}])

    client, blobs = _mock_client_with_buckets(
        {"lending-indices": df_lending, "perp-funding": df_perp},
        _mod.PROJECT_ID,
    )

    with (
        patch("sys.argv", [
            "script.py",
            "--dry-run",
            "--bucket", "lending-indices",
        ]),
        patch("google.cloud.storage.Client", return_value=client),
    ):
        result = _mod.main()

    assert result == 0
    # lending-indices blob was accessed; perp-funding blob was NOT.
    blobs["lending-indices"].download_to_filename.assert_called_once()
    blobs["perp-funding"].download_to_filename.assert_not_called()


# ---------------------------------------------------------------------------
# Test 6 — unknown --bucket value rejected
# ---------------------------------------------------------------------------


def test_unknown_bucket_arg_rejected() -> None:
    """main() returns 1 when --bucket names a family not in the closed set."""
    with patch("sys.argv", ["script.py", "--dry-run", "--bucket", "not-a-valid-family"]):
        result = _mod.main()
    assert result == 1, "Expected exit code 1 for unknown --bucket value"


# ---------------------------------------------------------------------------
# Test 7 — v8 unknown columns pass through unchanged (round-trip)
# ---------------------------------------------------------------------------


def test_preserves_other_columns_v8_round_trip() -> None:
    """v8 emission-tracking columns present in the manifest survive the upload unchanged."""
    df = _make_manifest([
        {
            "data_type": "lst-rates",
            # Simulate v8 columns that the script must NOT alter.
            "pipeline_mode": "batch",
            "service_emission_state": "emitted",
            "last_emission_decision_at": "2026-05-16T10:00:00Z",
            "expected_window_completeness_fraction": 1.0,
        },
        {"data_type": "lst_rates"},
    ])
    blob = _mock_blob_with_df(df)
    bucket = _mock_bucket_with_blob(blob)
    client = MagicMock()
    client.bucket.return_value = bucket

    kebab_count, total = _mod._process_bucket(
        family="lst-rates",
        project_id=_mod.PROJECT_ID,
        client=client,
        apply=True,
    )

    assert kebab_count == 1
    uploaded_bytes = blob.last_upload[0]
    uploaded_df = pd.read_parquet(io.BytesIO(uploaded_bytes))

    # data_type renamed.
    assert (uploaded_df["data_type"] == "lst_rates").all()
    # v8 columns preserved.
    assert "pipeline_mode" in uploaded_df.columns
    assert uploaded_df.loc[0, "pipeline_mode"] == "batch"
    assert "service_emission_state" in uploaded_df.columns
    assert uploaded_df.loc[0, "service_emission_state"] == "emitted"
    assert "last_emission_decision_at" in uploaded_df.columns
    assert "expected_window_completeness_fraction" in uploaded_df.columns


# ---------------------------------------------------------------------------
# Test 8 — per-bucket summary counts match actual data
# ---------------------------------------------------------------------------


def test_per_bucket_summary_counts_correct() -> None:
    """_process_bucket returns accurate (kebab_count, total) for known data."""
    # 55 kebab + 20 snake = 75 total (mirrors dex-pools issue-doc numbers roughly).
    kebab_rows = [{"data_type": "dex-pools"} for _ in range(55)]
    snake_rows = [{"data_type": "dex_pools"} for _ in range(20)]
    df = _make_manifest(kebab_rows + snake_rows)

    blob = _mock_blob_with_df(df)
    bucket = _mock_bucket_with_blob(blob)
    client = MagicMock()
    client.bucket.return_value = bucket

    kebab_count, total = _mod._process_bucket(
        family="dex-pools",
        project_id=_mod.PROJECT_ID,
        client=client,
        apply=False,
    )

    assert kebab_count == 55, f"Expected 55 kebab rows, got {kebab_count}"
    assert total == 75, f"Expected 75 total rows, got {total}"
    # No upload in dry-run.
    blob.upload_from_file.assert_not_called()
