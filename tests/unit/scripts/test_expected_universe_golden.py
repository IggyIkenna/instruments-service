# Epic: instruments_master
"""Byte-identical golden regression for the single expected-universe producer.

The Layer-1 denominator drift is the single most dangerous failure mode of
Honest Coverage v2 — if the EXPECTED matrix silently changes shape, coverage %
moves without anyone noticing.  This test locks the per-AG output of
`expected_universe.build_expected(ag)` to a checked-in golden snapshot.

Any producer edit that mutates the EXPECTED set for a shipped AG has to
explicitly update the golden fixture in the SAME commit — the diff surfaces
loudly in review (the reviewer sees which tuples appeared/disappeared and can
judge whether it is an intentional universe change or accidental drift).

Regeneration recipe when the change IS intentional:

    .venv/bin/python scripts/regenerate_expected_universe_golden.py

This refuses to write the fixture while UAC (`unified-api-contracts`) or UTL
(`unified-trading-library`) — both editable path-dependencies that
`build_expected()` reads live off disk — have uncommitted local changes,
which would otherwise get silently baked into the checked-in golden (see
`instruments-service@94512ec3` incident,
`plans/active/issues/instruments_service_qg_red_golden_drift_2026_07_10.md`).

Plan: plans/active/issues/cefi_layer1_denominator_gaps_2026_07_03.md § 2a
SSOT: codex/02-data/honest-coverage-model.md § Layer-1 enumeration-completeness matrix
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_expected_universe() -> ModuleType:
    """Load scripts/expected_universe.py (sibling to check_enumeration_completeness)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "expected_universe.py"
    module_name = "_expected_universe_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_check_module() -> ModuleType:
    """Load scripts/check_enumeration_completeness.py (routes through build_expected)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "check_enumeration_completeness.py"
    module_name = "_check_enumeration_completeness_golden_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _golden_path(asset_group: str) -> Path:
    return Path(__file__).resolve().parent / "goldens" / "expected_universe" / f"{asset_group}.json"


def _load_golden(asset_group: str) -> list[tuple[str, str, str]]:
    fixture = json.loads(_golden_path(asset_group).read_text())
    assert fixture["asset_group"] == asset_group, (
        f"golden asset_group mismatch: {fixture['asset_group']!r} vs {asset_group!r}"
    )
    return [tuple(t) for t in fixture["tuples"]]  # type: ignore[misc]


@pytest.fixture(scope="module")
def eu() -> ModuleType:
    return _load_expected_universe()


@pytest.fixture(scope="module")
def check_mod() -> ModuleType:
    return _load_check_module()


class TestSingleProducer:
    """Consolidation gate — every EXPECTED consumer routes through build_expected."""

    def test_build_expected_is_public_on_expected_universe(self, eu: ModuleType) -> None:
        assert callable(eu.build_expected)

    def test_check_enumeration_reexports_build_expected(self, check_mod: ModuleType) -> None:
        """check_enumeration_completeness re-exports build_expected from the sibling."""
        assert callable(check_mod.build_expected)

    def test_check_enumeration_delegator_matches_build_expected(self, eu: ModuleType, check_mod: ModuleType) -> None:
        """The _build_expected_tuples delegator MUST return the same set as build_expected."""
        for ag in eu.KNOWN_ASSET_GROUPS:
            direct = eu.build_expected(ag)
            via_delegator = check_mod._build_expected_tuples(ag)
            assert direct == via_delegator, (
                f"delegator drift for {ag!r}: build_expected returned {len(direct)} tuples "
                f"but check._build_expected_tuples returned {len(via_delegator)}"
            )

    def test_unknown_asset_group_returns_empty(self, eu: ModuleType) -> None:
        """Unknown AG → empty set + ERROR log; downstream guard fails-CLOSED."""
        assert eu.build_expected("nonexistent_ag") == set()


class TestGoldenByteIdentical:
    """One golden fixture per asset_group — any EXPECTED matrix change fails loudly."""

    @pytest.mark.parametrize("asset_group", ["cefi", "defi", "tradfi", "sports", "prediction"])
    def test_expected_matches_golden(self, eu: ModuleType, asset_group: str) -> None:
        actual = sorted(eu.build_expected(asset_group))
        expected = sorted(_load_golden(asset_group))
        # Rich diff on failure — reviewer sees exactly which tuples changed.
        missing = set(expected) - set(actual)
        extra = set(actual) - set(expected)
        assert not missing and not extra, (
            f"EXPECTED matrix drift for {asset_group!r}:\n"
            f"  golden={len(expected)}, actual={len(actual)}\n"
            f"  missing (in golden but not actual, first 10): {sorted(missing)[:10]}\n"
            f"  extra   (in actual but not golden, first 10): {sorted(extra)[:10]}\n"
            f"If the change is INTENTIONAL, regenerate the fixture per the docstring recipe."
        )

    @pytest.mark.parametrize("asset_group", ["cefi", "defi", "tradfi", "sports", "prediction"])
    def test_golden_tuple_count_matches_metadata(self, asset_group: str) -> None:
        """The `tuple_count` metadata in the fixture must equal len(tuples)."""
        fixture = json.loads(_golden_path(asset_group).read_text())
        assert fixture["tuple_count"] == len(fixture["tuples"]), (
            f"{asset_group} fixture: tuple_count={fixture['tuple_count']} != len(tuples)={len(fixture['tuples'])}"
        )
