"""F8 (2026-07-18): the CATALOGUE_SHRINK_BLOCKED drop-list diagnostic.

`build_instrument_catalogue._shrink_drop_diagnostics` makes a blocked shrink REVIEWABLE
— it reports which instruments a rejected `--mode full` catalogue would drop vs the
current one (by venue/type + active-vs-delisted split), instead of an opaque count.
Kept in its own file (not appended to the large, frequently-contended
test_build_instrument_catalogue.py) to avoid concurrent-edit collisions.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rollup() -> ModuleType:
    return _load_script_module("build_instrument_catalogue.py", "_bic_shrink_diag_test_module")


def test_shrink_drop_diagnostics_reports_dropped_instruments(rollup: ModuleType) -> None:
    """A blocked shrink must be REVIEWABLE — the diagnostic reports which instruments the
    new catalogue drops vs the current one, by venue/type + active-vs-delisted split."""
    prev = pd.DataFrame(
        [
            {"instrument_id": "KEEP-1", "venue": "MORPHO", "instrument_type": "SPOT_ASSET", "available_to": None},
            {"instrument_id": "DROP-1", "venue": "MORPHO", "instrument_type": "A_TOKEN", "available_to": "2023-06-01"},
            {"instrument_id": "DROP-2", "venue": "MORPHO", "instrument_type": "A_TOKEN", "available_to": "2023-07-01"},
            {"instrument_id": "DROP-3", "venue": "UNISWAP_V3", "instrument_type": "POOL", "available_to": None},
        ]
    )
    new = prev[prev["instrument_id"] == "KEEP-1"].copy()  # keeps only KEEP-1 → drops 3

    diag = rollup._shrink_drop_diagnostics(new, prev)

    assert diag["dropped"] == 3
    assert diag["added"] == 0
    # Two delisted (available_to set) + one active (null) — the F8 signal.
    assert diag["dropped_delisted"] == 2
    assert diag["dropped_active"] == 1
    assert diag["dropped_by_venue"] == {"MORPHO": 2, "UNISWAP_V3": 1}
    assert diag["dropped_by_instrument_type"] == {"A_TOKEN": 2, "POOL": 1}
    assert set(diag["dropped_sample_ids"]) == {"DROP-1", "DROP-2", "DROP-3"}


def test_shrink_drop_diagnostics_handles_missing_column(rollup: ModuleType) -> None:
    diag = rollup._shrink_drop_diagnostics(pd.DataFrame({"x": [1]}), pd.DataFrame({"instrument_id": ["A"]}))
    assert "error" in diag
