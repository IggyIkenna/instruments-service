"""Unit tests for reconcile_manifest_after_entity_change.py's `_default_csv_path()`.

Regression for the Path-B per-slot topology bug: `_default_csv_path()` used to resolve
`Path(__file__).parents[4]`, assuming a non-slotted checkout — under Path-B this landed on
the READ-ONLY root PM clone instead of the invoking slot's own `unified-trading-pm` sibling
(found during batch3's tombstone run, worked around via `--output-csv`; fixed properly here
per `tradfi_satellite_ao_dispatch_batch4_2026_07_26.md`'s follow-up todo).

The fix resolves the destination from the invoking repo's own git identity (walk up to the
clone's `.git` root, then its sibling `unified-trading-pm`) instead of a fixed parent-count
hop, and raises loudly rather than silently writing to an unintended location when no
sibling clone can be found.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_manifest_after_entity_change.py"
    module_name = "_reconcile_manifest_after_entity_change_default_csv_path_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


def _make_fake_clone(tmp_path: Path, repo_name: str = "instruments-service", nested_depth: int = 2) -> Path:
    """Build a fake `<workspace>/<repo_name>/<nested...>/script.py` clone with a real `.git` dir.

    `nested_depth` controls how many directories the "script" lives under the repo root —
    proving resolution no longer depends on a fixed hop count.
    """
    repo_root = tmp_path / repo_name
    (repo_root / ".git").mkdir(parents=True)
    script_dir = repo_root
    for i in range(nested_depth):
        script_dir = script_dir / f"level{i}"
    script_dir.mkdir(parents=True)
    return script_dir


def test_find_repo_toplevel_walks_up_to_git_root(tmp_path: Path) -> None:
    script_dir = _make_fake_clone(tmp_path, nested_depth=2)
    toplevel = _mod._find_repo_toplevel(script_dir)
    assert toplevel == tmp_path / "instruments-service"


def test_find_repo_toplevel_raises_when_no_git_root_found(tmp_path: Path) -> None:
    orphan_dir = tmp_path / "no-git-anywhere" / "scripts"
    orphan_dir.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="Could not locate the invoking repo's clone root"):
        _mod._find_repo_toplevel(orphan_dir)


def test_default_csv_path_resolves_inside_sibling_pm_clone_regardless_of_script_depth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same-slot resolution must work whether the script lives 1 or 3 directories deep —
    proving the fix no longer depends on a hard-coded parent-count hop."""
    for nested_depth in (1, 2, 3):
        workspace = tmp_path / f"depth_{nested_depth}"
        script_dir = _make_fake_clone(workspace, nested_depth=nested_depth)
        pm_dir = workspace / "unified-trading-pm"
        pm_dir.mkdir(parents=True)

        fake_file = script_dir / "reconcile_manifest_after_entity_change.py"
        monkeypatch.setattr(_mod, "__file__", str(fake_file))

        csv_path = _mod._default_csv_path("tradfi", "venue", "OLD_VENUE")

        assert pm_dir in csv_path.parents
        assert csv_path.name.startswith("tradfi_venue_OLD_VENUE_")
        assert csv_path.suffix == ".csv"


def test_default_csv_path_raises_when_no_sibling_pm_clone_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No unintended fallback location — an unresolvable destination must raise loudly,
    never silently write outside the intended sibling clone."""
    workspace = tmp_path / "no_pm_sibling"
    script_dir = _make_fake_clone(workspace, nested_depth=1)
    fake_file = script_dir / "reconcile_manifest_after_entity_change.py"
    monkeypatch.setattr(_mod, "__file__", str(fake_file))

    with pytest.raises(RuntimeError, match="Cannot resolve the audit-CSV destination"):
        _mod._default_csv_path("tradfi", "venue", "OLD_VENUE")
