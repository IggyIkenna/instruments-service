"""Regression coverage for
``issues/features_pipeline_e2e_check_vm_name_collision_same_second_2026_08_01.md``:

``_shard_vm_name()`` previously built its VM name from ``(asset_group, venue, leg,
run_ts)`` only — no day/window component — so two concurrent runs of the SAME
``(asset_group, venue, leg)`` cell targeting DIFFERENT days, whose VM-launch calls land
in the same UTC second (identical ``run_ts``), computed the identical VM name. Asserts
two ``_shard_vm_name()`` calls for the same cell but different days produce different
names even with a frozen ``run_ts``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DRIVER_PATH = _REPO_ROOT / "scripts" / "pipeline_e2e_check.py"


def _load_driver_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("instruments_pipeline_e2e_check_vm_name_day", _DRIVER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["instruments_pipeline_e2e_check_vm_name_day"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def driver() -> ModuleType:
    return _load_driver_module()


def test_vm_name_differs_for_same_cell_different_day_same_run_ts(driver: ModuleType) -> None:
    frozen_run_ts = "20260801-135620"

    baseline_name = driver._shard_vm_name("SPORTS", "SPORTSBOOK", "force", frozen_run_ts, "2025-12-20")
    final_name = driver._shard_vm_name("SPORTS", "SPORTSBOOK", "force", frozen_run_ts, "2025-12-18")

    assert baseline_name != final_name, (
        f"same-cell different-day VM names collided under a frozen run_ts: {baseline_name!r} == {final_name!r}"
    )


def test_vm_name_deterministic_for_same_cell_same_day(driver: ModuleType) -> None:
    frozen_run_ts = "20260801-135620"

    first = driver._shard_vm_name("SPORTS", "SPORTSBOOK", "force", frozen_run_ts, "2025-12-20")
    second = driver._shard_vm_name("SPORTS", "SPORTSBOOK", "force", frozen_run_ts, "2025-12-20")

    assert first == second


def test_vm_name_stays_under_gce_63_char_instance_name_limit(driver: ModuleType) -> None:
    name = driver._shard_vm_name("PREDICTION", "POLYMARKET", "force", "0801135620", "2025-12-20")
    assert len(name) <= 63, f"VM name {name!r} ({len(name)} chars) exceeds GCE's 63-char instance-name limit"
