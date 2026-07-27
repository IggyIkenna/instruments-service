"""Unit tests for reconcile_manifest_after_entity_change.py's `_default_csv_path()`.

Regression coverage for the Path-B per-slot bug: `_default_csv_path()` used to resolve
`Path(__file__).parents[4]` assuming a non-slotted checkout, which under the Path-B
per-slot topology lands on the read-only root PM clone instead of the invoking slot's
own `unified-trading-pm` sibling. The fix resolves the destination from THIS repo's own
git-toplevel identity rather than a fixed parent-count hop, and raises loudly instead of
silently writing outside the invoking clone when no sibling can be found.

Plan ref: `plans/active/tradfi_satellite_ao_dispatch_batch4_2026_07_26.md` (the
`_default_csv_path()` Path-B fix todo).
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
    module_name = "_reconcile_manifest_after_entity_change_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


def _make_fake_repo_clone(base: Path, repo_name: str) -> Path:
    """Build a synthetic `.git`-rooted repo clone dir with a `scripts/` subdir."""
    repo_dir = base / repo_name
    (repo_dir / ".git").mkdir(parents=True)
    (repo_dir / "scripts").mkdir()
    return repo_dir


def test_find_repo_toplevel_walks_up_to_the_git_dir(tmp_path: Path) -> None:
    repo_dir = _make_fake_repo_clone(tmp_path / ".tabs" / "4", "instruments-service")
    nested = repo_dir / "scripts"

    assert _mod._find_repo_toplevel(nested) == repo_dir


def test_find_repo_toplevel_raises_when_no_git_ancestor(tmp_path: Path) -> None:
    orphan = tmp_path / "no_repo_here" / "scripts"
    orphan.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="Could not resolve the enclosing git repo root"):
        _mod._find_repo_toplevel(orphan)


def test_default_csv_path_resolves_relative_to_the_invoking_clone_not_a_fixed_hop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Path-B per-slot layout: .tabs/4/instruments-service (invoking clone) and
    # .tabs/4/unified-trading-pm (the SLOT'S OWN sibling) live under the SAME parent —
    # a fixed parents[N] hop can't distinguish this from a non-slotted layout, but
    # resolving via the invoking repo's own git toplevel does.
    slot_dir = tmp_path / ".tabs" / "4"
    repo_dir = _make_fake_repo_clone(slot_dir, "instruments-service")
    fake_script = repo_dir / "scripts" / "reconcile_manifest_after_entity_change.py"
    slot_pm_dir = slot_dir / "unified-trading-pm"
    slot_pm_dir.mkdir()

    # A decoy "root clone" PM dir elsewhere must NEVER be picked — proves the resolution
    # is anchored to the invoking clone, not some other fixed ancestor.
    decoy_pm_dir = tmp_path / "unified-trading-pm"
    decoy_pm_dir.mkdir()

    monkeypatch.setattr(_mod, "__file__", str(fake_script))

    result = _mod._default_csv_path("tradfi", "venue", "OLD_VENUE")

    assert slot_pm_dir in result.parents
    assert decoy_pm_dir not in result.parents
    assert result.name.startswith("tradfi_venue_OLD_VENUE_")
    assert result.suffix == ".csv"


def test_default_csv_path_raises_rather_than_writing_outside_the_invoking_clone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No unified-trading-pm sibling exists next to the invoking clone — the old code
    # silently fell back to writing beside the script; the fix must raise instead.
    repo_dir = _make_fake_repo_clone(tmp_path / ".tabs" / "9", "instruments-service")
    fake_script = repo_dir / "scripts" / "reconcile_manifest_after_entity_change.py"

    monkeypatch.setattr(_mod, "__file__", str(fake_script))

    with pytest.raises(RuntimeError, match="Cannot resolve a writable unified-trading-pm sibling"):
        _mod._default_csv_path("tradfi", "venue", "OLD_VENUE")
