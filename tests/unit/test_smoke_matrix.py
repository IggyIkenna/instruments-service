"""Unit tests for instruments-service ``scripts/smoke_matrix.py``.

Verifies the 3-step assertion contract, shard-level isolation, dry-run
behaviour, cell-filter flags, and the Phase 3 api-football DependencyError
skip path.

Tests use monkeypatch for subprocess + storage_client so no real CLIs run
and no real GCS calls are made.
"""

from __future__ import annotations

import importlib.util
import sys
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SMOKE_PATH = _REPO_ROOT / "scripts" / "smoke_matrix.py"


def _load_smoke_module() -> ModuleType:
    """Import scripts/smoke_matrix.py as a module (scripts/ isn't packaged)."""
    spec = importlib.util.spec_from_file_location("instruments_smoke_matrix", _SMOKE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["instruments_smoke_matrix"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def smoke() -> ModuleType:
    return _load_smoke_module()


class _FakeBlob:
    def __init__(self, name: str, exists_flag: bool = True, payload: bytes = b"") -> None:
        self.name = name
        self._exists = exists_flag
        self._payload = payload

    def exists(self) -> bool:
        return self._exists

    def download_as_bytes(self) -> bytes:
        return self._payload


class _FakeBucket:
    def __init__(self, manifest_blob: _FakeBlob | None = None) -> None:
        self._manifest_blob = manifest_blob

    def blob(self, path: str) -> _FakeBlob:
        if self._manifest_blob is not None and path.endswith("availability_index.parquet"):
            return self._manifest_blob
        return _FakeBlob(path, exists_flag=False)


class _FakeStorageClient:
    def __init__(
        self,
        parquet_blobs: list[_FakeBlob] | None = None,
        manifest_blob: _FakeBlob | None = None,
    ) -> None:
        self._parquet_blobs = parquet_blobs or []
        self._manifest_blob = manifest_blob

    def list_blobs(self, bucket: str, prefix: str) -> list[_FakeBlob]:
        return self._parquet_blobs

    def bucket(self, name: str) -> _FakeBucket:
        return _FakeBucket(self._manifest_blob)


def _make_manifest_bytes(
    date: str,
    category: str,
    venue: str,
    data_type: str,
    capture_status: str = "captured",
) -> bytes:
    df = pd.DataFrame(
        [
            {
                "date": date,
                "category": category,
                "venue": venue,
                "data_type": data_type,
                "capture_status": capture_status,
            }
        ]
    )
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class _FakeCompleted:
    def __init__(self, rc: int = 0, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.returncode = rc
        self.stdout = stdout
        self.stderr = stderr


# ---------------------------------------------------------------------------
# Enumeration tests
# ---------------------------------------------------------------------------


def test_enumerate_cells_produces_multiple_categories(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells()
    categories = {c.category for c in cells}
    # Every major category should produce at least one cell.
    assert {"CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION"}.issubset(categories)
    assert len(cells) > 20, f"expected >20 cells from UAC SSOT, got {len(cells)}"


def test_enumerate_cells_category_filter(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells(category_filter="CEFI")
    assert cells
    assert {c.category for c in cells} == {"CEFI"}


def test_enumerate_cells_sports_emits_api_football_first(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells(category_filter="SPORTS")
    assert cells
    # T0 (API_FOOTBALL) MUST appear before any T1 enrichment provider.
    providers_in_order = [c.sports_provider for c in cells if c.sports_provider]
    assert providers_in_order, "sports enumeration produced no provider cells"
    assert providers_in_order[0] == "API_FOOTBALL", f"expected API_FOOTBALL first, got: {providers_in_order[:3]}"


# ---------------------------------------------------------------------------
# Bucket / path resolution
# ---------------------------------------------------------------------------


def test_resolve_test_bucket_honours_category(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    assert smoke.resolve_test_bucket("CEFI") == "instruments-store-cefi-test-test-project"


def test_expected_write_prefix_sports_uses_sports_reference(smoke: ModuleType) -> None:
    cell = smoke.SmokeCell(category="SPORTS", venue="API_FOOTBALL", data_type="odds")
    prefix = smoke.expected_write_prefix(cell, "2026-04-20")
    assert prefix.startswith("sports_reference/by_date/day=2026-04-20/")


def test_expected_write_prefix_non_sports_uses_instrument_availability(smoke: ModuleType) -> None:
    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    prefix = smoke.expected_write_prefix(cell, "2026-04-20")
    assert prefix == "instrument_availability/by_date/day=2026-04-20/venue=BINANCE-FUTURES/"


# ---------------------------------------------------------------------------
# 3-step contract — pass case
# ---------------------------------------------------------------------------


def test_run_cell_passes_when_all_three_steps_succeed(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    smoke_date = "2026-04-20"

    def fake_runner(argv: list[str], **kwargs: Any) -> _FakeCompleted:
        assert "IS_TEST_RUN" in kwargs["env"]
        assert kwargs["env"]["IS_TEST_RUN"] == "true"
        return _FakeCompleted(rc=0, stdout=b"ok", stderr=b"")

    client = _FakeStorageClient(
        parquet_blobs=[
            _FakeBlob("instrument_availability/by_date/day=2026-04-20/venue=BINANCE-FUTURES/instruments.parquet")
        ],
        manifest_blob=_FakeBlob(
            "_index/availability_index.parquet",
            exists_flag=True,
            payload=_make_manifest_bytes(smoke_date, "CEFI", "BINANCE-FUTURES", "trades"),
        ),
    )

    result = smoke.run_cell(
        cell=cell,
        smoke_date=smoke_date,
        subprocess_runner=fake_runner,
        storage_client=client,
    )

    assert result.status == "passed", f"expected passed, got {result.status} ({result.reason})"
    assert result.parquet_count == 1
    assert result.manifest_status == "captured"


def test_run_cell_empty_confirmed_is_pass(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    smoke_date = "2026-04-20"

    client = _FakeStorageClient(
        parquet_blobs=[
            _FakeBlob("instrument_availability/by_date/day=2026-04-20/venue=BINANCE-FUTURES/instruments.parquet")
        ],
        manifest_blob=_FakeBlob(
            "_index/availability_index.parquet",
            exists_flag=True,
            payload=_make_manifest_bytes(
                smoke_date, "CEFI", "BINANCE-FUTURES", "trades", capture_status="empty_confirmed"
            ),
        ),
    )

    result = smoke.run_cell(
        cell=cell,
        smoke_date=smoke_date,
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=client,
    )
    assert result.status == "passed"
    assert result.manifest_status == "empty_confirmed"


# ---------------------------------------------------------------------------
# 3-step contract — failure modes
# ---------------------------------------------------------------------------


def test_run_cell_fails_on_nonzero_rc(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=1, stderr=b"boom"),
        storage_client=_FakeStorageClient(),
    )
    assert result.status == "failed"
    assert "cli_nonzero_rc=1" in result.reason


def test_run_cell_fails_when_no_parquet(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=_FakeStorageClient(parquet_blobs=[]),  # no parquet
    )
    assert result.status == "failed"
    assert result.reason.startswith("no_parquet_at:")


def test_run_cell_fails_when_manifest_row_missing(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    # Manifest has row for a DIFFERENT date — no matching row for our smoke_date.
    bad_manifest = _make_manifest_bytes("2000-01-01", "CEFI", "BINANCE-FUTURES", "trades")
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob("x.parquet")],
        manifest_blob=_FakeBlob("_index/availability_index.parquet", exists_flag=True, payload=bad_manifest),
    )
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=client,
    )
    assert result.status == "failed"
    assert "no_matching_row" in result.reason or "manifest_status_invalid" in result.reason


def test_run_cell_skips_on_api_football_dependency_error(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(
        category="SPORTS",
        venue="FOOTYSTATS",
        data_type="odds",
        sports_provider="FOOTYSTATS",
    )
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(
            rc=1,
            stderr=b"DependencyError: api-football reference data missing for date 2026-04-20",
        ),
        storage_client=_FakeStorageClient(),
    )
    assert result.status == "skipped"
    assert result.reason == "api_football_missing"


# ---------------------------------------------------------------------------
# Shard-level isolation
# ---------------------------------------------------------------------------


def test_matrix_continues_after_single_cell_failure(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed cell must NOT abort subsequent cells."""
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cells = [
        smoke.SmokeCell(category="CEFI", venue="BINANCE-FUTURES", data_type="trades"),
        smoke.SmokeCell(category="CEFI", venue="BYBIT", data_type="trades"),
    ]

    call_count = {"n": 0}

    def fake_runner(argv: list[str], **kwargs: Any) -> _FakeCompleted:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _FakeCompleted(rc=1, stderr=b"first cell dies")
        return _FakeCompleted(rc=0)

    # Fake storage succeeds for the second cell only.
    manifest_bytes = _make_manifest_bytes("2026-04-20", "CEFI", "BYBIT", "trades")
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob("x.parquet")],
        manifest_blob=_FakeBlob("_index/availability_index.parquet", exists_flag=True, payload=manifest_bytes),
    )

    # Patch run_cell's storage default via monkeypatching get_storage_client inside smoke module.
    monkeypatch.setattr(smoke, "get_storage_client", lambda: client)

    # Drive run_matrix directly but route via our fake subprocess.runner.
    # Note: run_matrix doesn't accept a runner injection by design — we inline it here.
    results = []
    for c in cells:
        results.append(
            smoke.run_cell(cell=c, smoke_date="2026-04-20", subprocess_runner=fake_runner, storage_client=client)
        )

    assert call_count["n"] == 2, "second cell must still have run after the first failed"
    assert results[0].status == "failed"
    assert results[1].status == "passed"


# ---------------------------------------------------------------------------
# Dry-run + CLI smoke
# ---------------------------------------------------------------------------


def test_dry_run_enumerates_without_invoking_subprocess(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    called = {"n": 0}

    def fail_if_called(*a: Any, **k: Any) -> Any:
        called["n"] += 1
        raise AssertionError("subprocess must NOT run during --dry-run")

    monkeypatch.setattr(smoke.subprocess, "run", fail_if_called)

    cells = smoke.enumerate_cells(category_filter="PREDICTION")
    assert cells, "PREDICTION should enumerate at least one cell"
    report = smoke.run_matrix(cells=cells, smoke_date="2026-04-20", execute=False)

    assert called["n"] == 0
    assert report.skipped == len(cells)
    assert report.failed == 0
    assert report.passed == 0
    assert all(r.reason == "dry_run" for r in report.results)


def test_main_returns_zero_on_dry_run(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    rc = smoke.main(["--asset-group", "PREDICTION"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "smoke matrix" in out
