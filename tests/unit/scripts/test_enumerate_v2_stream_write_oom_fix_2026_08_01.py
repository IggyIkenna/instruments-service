"""Unit tests — v2 apply-write STREAMING fix (DeFi 19-day OOM, 2026-08-01).

``enumerate_expected_universe.py``'s v2 bounded-window path used to drain the
entire ``enumerate_v2`` generator into one in-memory ``list[ExpectedRow]``
before any write happened (``main()`` -> ``_write_absent_rows``). Fine for
cefi/tradfi/sports/prediction, but DeFi's proportionally enormous
per-instrument catalog blew the 8Gi Cloud Run Job ceiling every day for 19
consecutive days (2026-07-14..2026-08-01).

The fix (``_stream_write_v2_absent_rows`` + ``_write_v2_per_vm_shard_chunk``)
streams the generator through bounded chunks, flushing CSV rows + (when
``--apply-write``) a per-VM shard PART file every ``chunk_size`` rows, and
checks the ``max_writes_per_run`` halt-safety incrementally instead of only
after a full drain.

Tests cover:
  1. Scan-only (apply_write=False) streaming across multiple chunk flushes —
     CSV report content is complete + correctly ordered, no GCS writes occur.
  2. Halt-safety triggers mid-stream and chunks already flushed stay written
     (the documented trade-off vs the old atomic-abort).
  3. apply_write=True writes one per-VM shard PART file per chunk with the
     expected blob-name pattern and row counts, mocked storage.Client (no
     network).
  4. Missing MANIFEST_PER_VM_SHARDS / VM_NAME env guards still fire before
     any row is processed.
  5. Zero candidates -> exit 0, empty report file removed, no writes.

Issue: plans/active/issues/defi_v2_expected_universe_enumerator_oom_2026_08_01.md
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module loader (mirrors the existing v2 / memory-frugal test pattern)
# ---------------------------------------------------------------------------


def _load_enumerator_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_stream_write_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
ExpectedRow = enumerator_module.ExpectedRow


def _make_rows(n: int) -> list:
    return [
        ExpectedRow(
            asset_group="defi",
            venue="curve",
            chain="ethereum",
            data_type="dex_pool_state",
            instrument_type="curve_pool",
            instrument_id=f"pool-{i:04d}",
            league_id="",
            date="2026-08-01",
            reason="" if i % 2 == 0 else "EXPECTED_INSTRUMENT_NOT_LISTED",
            capture_status="expected_unattempted" if i % 2 == 0 else "empty_confirmed",
        )
        for i in range(n)
    ]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# 1. Scan-only streaming across multiple chunk flushes
# ---------------------------------------------------------------------------


def test_scan_only_streams_all_rows_across_multiple_flushes(tmp_path: Path) -> None:
    rows = _make_rows(7)
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter(rows),
        max_writes_per_run=1_000,
        chunk_size=3,  # forces 3 flushes: 3 + 3 + 1
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=False,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",  # skip GCS report upload — stays network-free
    )

    assert code == 0
    csv_rows = _read_csv_rows(report_path)
    assert len(csv_rows) == 7
    assert [r["instrument_id"] for r in csv_rows] == [f"pool-{i:04d}" for i in range(7)]
    # Distribution: 4 expected_unattempted (even i), 3 empty_confirmed (odd i).
    assert sum(1 for r in csv_rows if r["capture_status"] == "expected_unattempted") == 4
    assert sum(1 for r in csv_rows if r["capture_status"] == "empty_confirmed") == 3


def test_scan_only_chunk_size_larger_than_input_still_flushes_final_partial(tmp_path: Path) -> None:
    rows = _make_rows(2)
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter(rows),
        max_writes_per_run=1_000,
        chunk_size=250_000,  # production chunk size — never hits the boundary
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=False,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",
    )

    assert code == 0
    assert len(_read_csv_rows(report_path)) == 2


# ---------------------------------------------------------------------------
# 2. Halt-safety — incremental check, partial flush stays written
# ---------------------------------------------------------------------------


def test_halt_safety_triggers_mid_stream_and_keeps_already_flushed_chunks(tmp_path: Path) -> None:
    rows = _make_rows(6)
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter(rows),
        max_writes_per_run=5,  # trips while counting row 6
        chunk_size=3,  # chunk 1 (rows 0-2) flushes before the trip
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=False,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",
    )

    assert code == 5
    # Chunk 1 (3 rows) was flushed to disk BEFORE the cap tripped on row 6 —
    # the documented trade-off vs the old atomic all-or-nothing abort.
    csv_rows = _read_csv_rows(report_path)
    assert len(csv_rows) == 3
    assert [r["instrument_id"] for r in csv_rows] == ["pool-0000", "pool-0001", "pool-0002"]


# ---------------------------------------------------------------------------
# 3. apply_write=True — per-VM shard PART files, mocked GCS
# ---------------------------------------------------------------------------


def _mock_storage_client() -> tuple[MagicMock, dict[str, pd.DataFrame]]:
    """Mock ``storage.Client`` capturing every uploaded per-VM shard as a df."""
    uploaded: dict[str, pd.DataFrame] = {}

    def _make_blob(name: str) -> MagicMock:
        blob = MagicMock()

        def _upload(local_path: str, timeout: int = 600) -> None:
            uploaded[name] = pd.read_parquet(local_path)

        blob.upload_from_filename.side_effect = _upload
        return blob

    bucket = MagicMock()
    bucket.blob.side_effect = _make_blob

    client = MagicMock()
    client.bucket.return_value = bucket
    return client, uploaded


def test_apply_write_writes_one_part_file_per_chunk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
    monkeypatch.setenv("VM_NAME", "enum-universe-v2-defi")
    rows = _make_rows(5)
    report_path = tmp_path / "report.csv"
    client, uploaded = _mock_storage_client()

    with patch("google.cloud.storage.Client", return_value=client):
        code = enumerator_module._stream_write_v2_absent_rows(
            rows=iter(rows),
            max_writes_per_run=1_000,
            chunk_size=2,  # 5 rows -> parts of 2, 2, 1
            asset_group="defi",
            bucket_name="market-data-tick-defi-prd",
            apply_write=True,
            report_path=report_path,
            run_id="test-run",
            run_ts="20260801-000000",
            gcs_report_bucket_arg="",
        )

    assert code == 0
    assert set(uploaded.keys()) == {
        "_index/per_vm/enum-universe-v2-defi-part00001.parquet",
        "_index/per_vm/enum-universe-v2-defi-part00002.parquet",
        "_index/per_vm/enum-universe-v2-defi-part00003.parquet",
    }
    assert len(uploaded["_index/per_vm/enum-universe-v2-defi-part00001.parquet"]) == 2
    assert len(uploaded["_index/per_vm/enum-universe-v2-defi-part00002.parquet"]) == 2
    assert len(uploaded["_index/per_vm/enum-universe-v2-defi-part00003.parquet"]) == 1
    total_written = sum(len(df) for df in uploaded.values())
    assert total_written == 5
    # CSV report still carries every candidate row (report + shard writes are
    # independent flush targets within the same chunk loop).
    assert len(_read_csv_rows(report_path)) == 5


# ---------------------------------------------------------------------------
# 4. Env guards fire before any row is processed
# ---------------------------------------------------------------------------


def test_apply_write_missing_manifest_per_vm_shards_env_fails_fast(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MANIFEST_PER_VM_SHARDS", raising=False)
    monkeypatch.setenv("VM_NAME", "enum-universe-v2-defi")
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter(_make_rows(3)),
        max_writes_per_run=1_000,
        chunk_size=2,
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=True,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",
    )

    assert code == 4
    assert not report_path.exists()  # never opened — guard fires before the write loop


def test_apply_write_missing_vm_name_env_fails_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
    monkeypatch.delenv("VM_NAME", raising=False)
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter(_make_rows(3)),
        max_writes_per_run=1_000,
        chunk_size=2,
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=True,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",
    )

    assert code == 4
    assert not report_path.exists()


# ---------------------------------------------------------------------------
# 5. Zero candidates
# ---------------------------------------------------------------------------


def test_zero_candidates_exits_clean_and_removes_empty_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.csv"

    code = enumerator_module._stream_write_v2_absent_rows(
        rows=iter([]),
        max_writes_per_run=1_000,
        chunk_size=250_000,
        asset_group="defi",
        bucket_name="unused-bucket",
        apply_write=False,
        report_path=report_path,
        run_id="test-run",
        run_ts="20260801-000000",
        gcs_report_bucket_arg="",
    )

    assert code == 0
    assert not report_path.exists()
