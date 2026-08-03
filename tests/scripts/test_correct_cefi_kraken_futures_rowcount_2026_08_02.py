"""Unit tests for correct_cefi_kraken_futures_rowcount_2026_08_02.py.

Credential-free + GCS-free: fakes the storage client's ``list_blobs`` and the
reused footer-read helper so the correction's own summing/prefix logic is exercised
without touching real GCS.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "correct_cefi_kraken_futures_rowcount_2026_08_02.py"
    module_name = "_correct_cefi_kraken_futures_rowcount_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


@dataclass(frozen=True)
class _FakeBlob:
    name: str
    size: int


class _FakeClient:
    def __init__(self, blobs: list[_FakeBlob]) -> None:
        self._blobs = blobs
        self.seen_prefixes: list[str] = []

    def list_blobs(self, bucket: str, prefix: str = "") -> list[_FakeBlob]:
        self.seen_prefixes.append(prefix)
        return [b for b in self._blobs if b.name.startswith(prefix)]


class TestCanonicalPrefix:
    def test_prefix_shape_excludes_remediation_backups_by_construction(self) -> None:
        prefix = _mod._canonical_prefix("2024-02-01", "trades")
        assert prefix == (
            "raw_tick_data/by_date/day=2024-02-01/pipeline_mode=batch_tardis/asset_group=cefi/"
            "venue=KRAKEN-FUTURES/instrument_type=future/data_type=trades/"
        )
        assert "_remediation_backups" not in prefix


class TestComputeTrueRowCount:
    def test_sums_footer_rows_across_canonical_objects_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        prefix = _mod._canonical_prefix("2024-02-01", "trades")
        blobs = [
            _FakeBlob(name=f"{prefix}A.parquet", size=100),
            _FakeBlob(name=f"{prefix}B.parquet", size=200),
            # A stale backup living at a DIFFERENT top-level prefix — must never be
            # matched by the canonical-prefix listing (proves exclusion is structural,
            # not a runtime filter).
            _FakeBlob(name=f"_remediation_backups/kraken_futures_collision_2026_07_08/{prefix}A.parquet", size=100),
        ]
        client = _FakeClient(blobs)

        footer_calls: list[str] = []

        def _fake_footer(bucket: str, blob_path: str, size: int = 0) -> tuple[int, list[str], object]:
            footer_calls.append(blob_path)
            rows = 18 if blob_path.endswith("A.parquet") else 7
            return rows, ["timestamp", "price"], object()

        monkeypatch.setattr(_mod._backfill, "_read_parquet_footer", _fake_footer)
        monkeypatch.setattr(_mod._backfill, "_empty_frame_from_schema", lambda schema: "FAKE_SCHEMA_DF")

        result = _mod.compute_true_row_count(client, "some-bucket", "2024-02-01", "trades")

        assert result.true_row_count == 25  # 18 + 7, backup NEVER footer-read
        assert result.object_count == 2
        assert result.schema_df == "FAKE_SCHEMA_DF"
        assert footer_calls == [f"{prefix}A.parquet", f"{prefix}B.parquet"]

    def test_raises_when_no_canonical_objects_found(self) -> None:
        client = _FakeClient([])
        with pytest.raises(RuntimeError, match="0 canonical objects found"):
            _mod.compute_true_row_count(client, "some-bucket", "2024-02-01", "trades")


class TestAffectedCells:
    def test_exactly_the_4_cells_from_the_issue_doc(self) -> None:
        assert _mod.AFFECTED_CELLS == (
            ("2024-02-01", "book_snapshot_5"),
            ("2024-02-01", "trades"),
            ("2025-01-10", "book_snapshot_5"),
            ("2025-01-10", "trades"),
        )
