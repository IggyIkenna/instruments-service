"""Unit tests for beta_manifest_writer.py (CF-20 / V5 scaffold).

Credential-free + GCS-free: exercises the dev-target guard + the v9 column projection
(the pure surface). The single GCS upload in ``write_projected_index`` is thin.

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V5/CF-20.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "beta_manifest_writer.py"
    module_name = "_beta_manifest_writer_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


class TestAssertDevTarget:
    @pytest.mark.parametrize(
        "uri",
        [
            "gs://market-data-tick-cefi-dev-pid/_index/availability_index.parquet",
            "gs://bucket/dev/_index/availability_index.parquet",
            "gs://bucket/_index/audit/projected_cefi.parquet",
            "gs://bucket/beta/_index.parquet",
        ],
    )
    def test_dev_targets_accepted(self, uri: str) -> None:
        _mod.assert_dev_target(uri)  # must not raise

    @pytest.mark.parametrize(
        "uri",
        [
            "gs://market-data-tick-cefi-prd-pid/_index/availability_index.parquet",
            "gs://market-data-tick-cefi-stg-pid/_index/availability_index.parquet",
            "s3://bucket/dev/_index.parquet",
            "/local/path/_index.parquet",
        ],
    )
    def test_non_dev_or_non_gcs_refused(self, uri: str) -> None:
        with pytest.raises(_mod.NonDevManifestTargetError):
            _mod.assert_dev_target(uri)


class TestProjectV9Columns:
    def test_stamps_schema_version_9(self) -> None:
        df = pd.DataFrame({"capture_status": ["captured"], "venue": ["BINANCE"]})
        out = _mod.project_v9_columns(df)
        assert (out["schema_version"] == 9).all()

    def test_fills_missing_required_columns(self) -> None:
        df = pd.DataFrame({"capture_status": ["captured"]})
        out = _mod.project_v9_columns(df)
        for col in _mod.V9_REQUIRED_COLUMNS:
            assert col in out.columns

    def test_does_not_mutate_input(self) -> None:
        df = pd.DataFrame({"capture_status": ["captured"]})
        _mod.project_v9_columns(df)
        assert "schema_version" not in df.columns

    def test_preserves_existing_capture_states(self) -> None:
        df = pd.DataFrame({"capture_status": ["captured", "expected_unattempted", "attempted_failed"]})
        out = _mod.project_v9_columns(df)
        assert list(out["capture_status"]) == ["captured", "expected_unattempted", "attempted_failed"]
