#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Regenerate `tests/unit/scripts/goldens/expected_universe/<ag>.json` from
`expected_universe.build_expected(ag)`.

`build_expected()` reads UAC (`unified-api-contracts`) and, transitively, UTL
(`unified-trading-library`) as **editable path dependencies**
(`tool.uv.sources` → `../unified-api-contracts`, `../unified-trading-library`).
An editable install imports whatever is on disk in the sibling clone RIGHT NOW
— including uncommitted, unpushed local edits. Regenerating the golden while
either sibling clone is dirty silently bakes that local-only state into a
checked-in fixture (root cause of `instruments-service@94512ec3`'s golden
carrying 5 phantom cefi tuples that never existed on `origin/live-defi-rollout`
— see `plans/active/issues/instruments_service_qg_red_golden_drift_2026_07_10.md`).

So this script asserts `git status --porcelain` is empty for every UAC/UTL
path-dependency before writing anything. Use this instead of the raw
`python -c` one-liner for any intentional golden update.

Usage:
    .venv/bin/python scripts/regenerate_expected_universe_golden.py [--allow-dirty]

`--allow-dirty` bypasses the check (e.g. a deliberate local dry-run) — never
pass it on a commit that ships the regenerated golden.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLDEN_DIR = REPO_ROOT / "tests" / "unit" / "scripts" / "goldens" / "expected_universe"
PATH_DEPENDENCIES: tuple[str, ...] = ("unified-api-contracts", "unified-trading-library")


def _dirty_files(path_dep_dir: Path) -> list[str]:
    result = subprocess.run(  # nosec B603 B607 — fixed args, no user input
        ["git", "-C", str(path_dep_dir), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def assert_path_dependencies_clean(path_deps: Iterable[str]) -> None:
    """Fail loudly if any editable UAC/UTL sibling clone has uncommitted changes."""
    problems: dict[str, list[str]] = {}
    for dep_name in path_deps:
        dep_dir = REPO_ROOT.parent / dep_name
        if not dep_dir.is_dir():
            raise SystemExit(f"path-dependency clone not found at {dep_dir} — cannot verify cleanliness")
        dirty = _dirty_files(dep_dir)
        if dirty:
            problems[dep_name] = dirty

    if problems:
        lines = [
            "Refusing to regenerate the golden — the following editable path-dependencies "
            "have uncommitted changes that would be baked into the fixture:",
        ]
        for dep_name, dirty in problems.items():
            lines.append(f"  {dep_name}:")
            lines.extend(f"    {entry}" for entry in dirty)
        lines.append(
            "Commit/stash/push those changes (or pass --allow-dirty for a throwaway local check) before regenerating."
        )
        raise SystemExit("\n".join(lines))


def _load_expected_universe() -> ModuleType:
    script_path = REPO_ROOT / "scripts" / "expected_universe.py"
    module_name = "_expected_universe_regen"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def regenerate() -> None:
    eu = _load_expected_universe()
    captured_at = datetime.now(UTC).strftime("%Y-%m-%d")
    for asset_group in eu.KNOWN_ASSET_GROUPS:
        tuples = sorted(eu.build_expected(asset_group))
        fixture = {
            "asset_group": asset_group,
            "captured_at": captured_at,
            "tuple_count": len(tuples),
            "tuples": [list(t) for t in tuples],
        }
        out_path = GOLDEN_DIR / f"{asset_group}.json"
        out_path.write_text(json.dumps(fixture, indent=2, sort_keys=False) + "\n")
        print(f"wrote {out_path} ({len(tuples)} tuples)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Skip the UAC/UTL cleanliness check (local dry-run only — never on a shipped commit).",
    )
    args = parser.parse_args()

    if not args.allow_dirty:
        assert_path_dependencies_clean(PATH_DEPENDENCIES)

    regenerate()


if __name__ == "__main__":
    main()
