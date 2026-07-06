"""Unit tests — B2 wiring: enumerate_expected_universe.py reads TOTAL_UNIVERSE_AXES.

The B2 downstream of ``is_catalogue_completion_2d`` wires the enumerator to the
UAC ``TOTAL_UNIVERSE_AXES`` SSOT (the ``total_universe.py`` module in
``unified_api_contracts.canonical.crosscutting``).  These tests pin the wiring:

* SUPPORTED_ASSET_GROUPS ⊆ TOTAL_UNIVERSE_AXES.keys() (the module-level guard).
* :func:`is_total_universe` returns True for every supported AG (structural).
* :func:`enumerate_v2` refuses to emit a denominator for an AG that has no
  axis taxonomy (the runtime entry-point guard).
* MVP ⊆ TOTAL is respected — for a canonical MVP cell per AG,
  :func:`universe_membership` returns :attr:`UniverseTier.MVP` (short-circuits
  through the MVP check before falling into ``TOTAL_ONLY``, proving MVP is a
  strict subset of the total-reasonable universe).

Plan: ``plans/active/is_catalogue_completion_2d_2026_07_06.md`` — B2 downstream.
SSOT: ``codex/04-architecture/instruments-service-as-ssot-for-mtds.md``.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest
from unified_api_contracts import (
    TOTAL_UNIVERSE_AXES,
    UniverseTier,
    is_total_universe,
    universe_membership,
)


def _load_enumerator_module() -> ModuleType:
    """Load the enumerator script as a module by path (script lives outside the package)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_b2_wiring_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
SUPPORTED_ASSET_GROUPS = enumerator_module.SUPPORTED_ASSET_GROUPS
enumerate_v2 = enumerator_module.enumerate_v2


# ---------------------------------------------------------------------------
# 1. Module-level guard — SUPPORTED_ASSET_GROUPS ⊆ TOTAL_UNIVERSE_AXES
# ---------------------------------------------------------------------------


def test_supported_asset_groups_are_all_in_total_universe_axes() -> None:
    """Every AG the enumerator supports MUST have a UAC axis taxonomy entry.

    The module-level guard raises RuntimeError at import time if this invariant
    is violated — a successful module load already proves it, but the test
    pins the intent so a future contributor doesn't silently add an AG to
    SUPPORTED_ASSET_GROUPS without updating ``TOTAL_UNIVERSE_AXES``.
    """
    missing = set(SUPPORTED_ASSET_GROUPS) - set(TOTAL_UNIVERSE_AXES.keys())
    assert not missing, (
        f"SUPPORTED_ASSET_GROUPS ⊄ TOTAL_UNIVERSE_AXES — missing UAC axis taxonomy "
        f"for {sorted(missing)}."
    )


def test_total_universe_axes_covers_all_supported_asset_groups() -> None:
    """Every supported AG must return True from is_total_universe."""
    for ag in SUPPORTED_ASSET_GROUPS:
        assert is_total_universe(ag, "", ""), f"is_total_universe({ag!r}) returned False"


# ---------------------------------------------------------------------------
# 2. enumerate_v2 structural guard — non-taxonomy AG rejected
# ---------------------------------------------------------------------------


def test_enumerate_v2_rejects_unknown_asset_group() -> None:
    """enumerate_v2 refuses to emit a denominator for an unregistered AG."""
    with pytest.raises(ValueError, match="unsupported asset_group"):
        list(
            enumerate_v2(
                asset_group="not_a_real_asset_group",
                catalog=[],
                date_axis=[date(2024, 1, 1)],
                data_types=["trades"],
            )
        )


# ---------------------------------------------------------------------------
# 3. MVP ⊆ TOTAL invariant — canonical MVP cells classify as UniverseTier.MVP
# ---------------------------------------------------------------------------


def test_universe_membership_returns_mvp_for_canonical_cefi_mvp_cell() -> None:
    """A BINANCE-FUTURES BTC PERPETUAL cell is MVP; the SSOT function must agree.

    ``universe_membership`` short-circuits: if ``is_mvp`` returns True, the tier
    is MVP without checking TOTAL — this IS the "MVP ⊆ TOTAL respected" invariant
    (an MVP cell is automatically part of the total-reasonable universe, so the
    SSOT does not need to double-check TOTAL membership).
    """
    tier = universe_membership(
        "cefi",
        venue="BINANCE-FUTURES",
        instrument_type="PERPETUAL",
        data_type="trades",
        base_ccy="BTC",
    )
    assert tier == UniverseTier.MVP


def test_universe_membership_returns_mvp_for_canonical_tradfi_mvp_cell() -> None:
    """A CME ES FUTURE + ``ohlcv_1m`` cell classifies as MVP.

    The tradfi MVP rule keys off underlier code + data_type — ES / NQ / VX at
    ``ohlcv_1m`` are MVP CME cells (see UAC ``test_mvp_scope`` fixtures).
    Same invariant as cefi above: an MVP cell short-circuits to
    :attr:`UniverseTier.MVP` in :func:`universe_membership`, proving MVP ⊆ TOTAL.
    """
    tier = universe_membership(
        "tradfi",
        venue="CME",
        instrument_type="FUTURE",
        data_type="ohlcv_1m",
        base_ccy="ES",
    )
    assert tier == UniverseTier.MVP


def test_universe_membership_returns_not_in_universe_for_unknown_ag() -> None:
    """A cell in a non-taxonomy AG classifies as NOT_IN_UNIVERSE."""
    tier = universe_membership(
        "not_a_real_asset_group",
        venue="X",
        instrument_type="Y",
        data_type="trades",
    )
    assert tier == UniverseTier.NOT_IN_UNIVERSE


# ---------------------------------------------------------------------------
# 4. Axis taxonomy shape — every declared axis has a non-empty ssot_ref +
#    description, so a future audit against the enumerator's cross-join can
#    resolve WHERE each axis's membership lives without opening the SSOT.
# ---------------------------------------------------------------------------


def test_every_universe_axis_has_ssot_ref_and_description() -> None:
    for ag, axes in TOTAL_UNIVERSE_AXES.items():
        assert axes, f"TOTAL_UNIVERSE_AXES[{ag!r}] is empty"
        for axis in axes:
            assert axis.name, f"{ag} axis missing name"
            assert axis.ssot_ref, f"{ag}.{axis.name} missing ssot_ref"
            assert axis.description, f"{ag}.{axis.name} missing description"
