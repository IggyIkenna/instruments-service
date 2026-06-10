"""Unit tests for migration_schema_completeness.py (CF-18 / V3 scaffold).

Credential-free + GCS-free: exercises the PURE column-diff (``diff_schema`` /
``canonical_columns_for`` / ``sample_targets_from_objects`` / report aggregation). The
footer-sampling GCS read is thin orchestration over these.

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V3/CF-18.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "migration_schema_completeness.py"
    module_name = "_migration_schema_completeness_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


class TestDiffSchema:
    def test_dropped_source_column_is_red(self) -> None:
        canon = _mod.canonical_columns_for("prediction", "trades")
        assert canon is not None
        src = set(canon) | {"mystery_attr"}
        diff = _mod.diff_schema("prediction", "trades", "POLYMARKET", src)
        assert diff.is_red
        assert "mystery_attr" in diff.dropped

    def test_exact_match_is_green(self) -> None:
        canon = _mod.canonical_columns_for("prediction", "trades")
        assert canon is not None
        diff = _mod.diff_schema("prediction", "trades", "POLYMARKET", set(canon))
        assert not diff.is_red
        assert diff.dropped == frozenset()

    def test_partition_and_meta_columns_excluded_from_red(self) -> None:
        canon = _mod.canonical_columns_for("prediction", "trades")
        assert canon is not None
        # hive/partition + writer-meta columns must NOT count as a drop
        src = set(canon) | {"asset_group", "venue", "day", "pipeline_mode", "source", "schema_version"}
        diff = _mod.diff_schema("prediction", "trades", "POLYMARKET", src)
        assert not diff.is_red

    def test_missing_contract_not_red(self) -> None:
        diff = _mod.diff_schema("cefi", "nonexistent_data_type", "X", {"a", "b"})
        assert not diff.canonical_known
        assert not diff.is_red  # missing-contract is a separate finding, not a truncation

    def test_contract_column_absent_from_sample_is_informational(self) -> None:
        canon = _mod.canonical_columns_for("prediction", "trades")
        assert canon is not None
        partial = set(sorted(canon)[:2])  # sample carries only 2 of the contract cols
        diff = _mod.diff_schema("prediction", "trades", "POLYMARKET", partial)
        assert not diff.is_red  # missing-from-sample is extra_canonical, not a drop
        assert diff.extra_canonical


class TestReport:
    def test_red_and_green_aggregation(self) -> None:
        report = _mod.SchemaCompletenessReport()
        canon = _mod.canonical_columns_for("prediction", "trades")
        assert canon is not None
        report.add(_mod.diff_schema("prediction", "trades", "A", set(canon)))  # green
        report.add(_mod.diff_schema("prediction", "trades", "B", set(canon) | {"dropme"}))  # red
        report.add(_mod.diff_schema("cefi", "unknown_dt", "C", {"x"}))  # missing contract
        assert len(report.red) == 1
        assert len(report.missing_contract) == 1
        assert not report.is_green()


class TestSampling:
    def test_picks_most_recent_per_cell(self) -> None:
        objs = [
            ("prediction", "trades", "POLY", "gs://b/day=2024-06-01/x.parquet"),
            ("prediction", "trades", "POLY", "gs://b/day=2024-07-01/y.parquet"),
            ("prediction", "trades", "POLY", "gs://b/day=2024-05-01/z.parquet"),
        ]
        targets = _mod.sample_targets_from_objects(objs, per_cell=1)
        assert len(targets) == 1
        # day=2024-07-01 sorts lexicographically last → most recent
        assert "2024-07-01" in targets[0].uri

    def test_blank_data_type_skipped(self) -> None:
        objs = [("prediction", "", "POLY", "gs://b/x.parquet")]
        assert _mod.sample_targets_from_objects(objs) == []
