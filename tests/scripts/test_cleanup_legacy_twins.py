"""Unit tests for cleanup_legacy_twins.py (CF-21 / G4.5 verified-delete scaffold).

Credential-free + GCS-free: exercises the PURE 'genetic' delete gate (``is_deletable``)
+ the canonical-twin path reconstruction. The per-object crc32c fetch + delete are thin.

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V6/CF-21.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "scripts"))  # cleanup imports migration_orphan_sweep
    script_path = repo_root / "scripts" / "cleanup_legacy_twins.py"
    module_name = "_cleanup_legacy_twins_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


class TestIsDeletable:
    def test_crc_identical_in_manifest_legacy_shape_deletable(self) -> None:
        ok, reason = _mod.is_deletable(
            legacy_is_canonical_shape=False,
            cell_captured_in_manifest=True,
            legacy_crc32c="abc123",
            canonical_crc32c="abc123",
        )
        assert ok
        assert "safe to delete" in reason

    def test_crc_mismatch_not_deletable(self) -> None:
        ok, reason = _mod.is_deletable(
            legacy_is_canonical_shape=False,
            cell_captured_in_manifest=True,
            legacy_crc32c="abc123",
            canonical_crc32c="def456",
        )
        assert not ok
        assert "MISMATCH" in reason

    def test_canonical_object_never_deleted(self) -> None:
        ok, reason = _mod.is_deletable(
            legacy_is_canonical_shape=True,
            cell_captured_in_manifest=True,
            legacy_crc32c="abc",
            canonical_crc32c="abc",
        )
        assert not ok
        assert "CANONICAL" in reason

    def test_not_captured_never_deleted(self) -> None:
        ok, reason = _mod.is_deletable(
            legacy_is_canonical_shape=False,
            cell_captured_in_manifest=False,
            legacy_crc32c="abc",
            canonical_crc32c="abc",
        )
        assert not ok
        assert "only copy" in reason

    def test_missing_crc_never_deleted(self) -> None:
        ok, _r = _mod.is_deletable(
            legacy_is_canonical_shape=False,
            cell_captured_in_manifest=True,
            legacy_crc32c="",
            canonical_crc32c="abc",
        )
        assert not ok


class TestCanonicalTwinPath:
    def test_inserts_pipeline_mode_after_day(self) -> None:
        legacy = "raw_tick_data/by_date/day=2024-06-01/asset_group=cefi/venue=DERIBIT/instrument_type=option/data_type=trades/x.parquet"
        out = _mod.canonical_twin_path(legacy, "tardis")
        assert "day=2024-06-01/pipeline_mode=batch_tardis/asset_group=cefi/" in out
        assert out.endswith("x.parquet")

    def test_category_normalised_to_asset_group(self) -> None:
        legacy = "raw_tick_data/by_date/day=2024-06-01/category=cefi/venue=X/data_type=trades/y.parquet"
        out = _mod.canonical_twin_path(legacy, "tardis")
        assert "asset_group=cefi" in out
        assert "category=" not in out

    def test_already_canonical_not_double_inserted(self) -> None:
        canon = "raw_tick_data/by_date/day=2024-06-01/pipeline_mode=batch_tardis/asset_group=cefi/venue=X/data_type=trades/z.parquet"
        out = _mod.canonical_twin_path(canon, "tardis")
        # exactly one pipeline_mode= segment (no double insert)
        assert out.count("pipeline_mode=") == 1


class TestShapeDetection:
    def test_canonical_uri_detected(self) -> None:
        assert _mod._is_canonical_shape_uri(
            "gs://b/raw_tick_data/by_date/day=x/pipeline_mode=batch_tardis/asset_group=cefi/f.parquet"
        )

    def test_legacy_uri_detected(self) -> None:
        assert not _mod._is_canonical_shape_uri("gs://b/raw_tick_data/by_date/day=x/asset_group=cefi/f.parquet")
