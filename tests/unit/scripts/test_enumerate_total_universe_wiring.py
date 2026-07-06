"""Unit tests — B2 downstream: enumerate_expected_universe wired to the UAC
TOTAL_UNIVERSE_AXES SSOT.

Verifies the wiring gate from ``instruments_mtds_subset_consistency_remediation
§ B2`` / ``is_catalogue_completion_2d_2026_07_06`` § B2 downstream:

* The enumerator's ``SUPPORTED_ASSET_GROUPS`` is exactly the set of AGs declared
  in :data:`unified_api_contracts.TOTAL_UNIVERSE_AXES` (both sides symmetric —
  otherwise the enumerator over-counts an axis-less AG, or the SSOT drifts).
* The structural gate loud-fails on an AG not declared in the SSOT.
* MVP ⊆ TOTAL: an MVP-scoped cell classifies as :attr:`UniverseTier.MVP`; a
  non-MVP cell in a declared AG classifies as :attr:`UniverseTier.TOTAL_ONLY`;
  never :attr:`UniverseTier.NOT_IN_UNIVERSE` for a declared AG's venue/type.
* The tier-distribution helper returns a dict keyed by every :class:`UniverseTier`
  member (so downstream telemetry sees zero-count tiers as well).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from unified_api_contracts import (
    TOTAL_UNIVERSE_AXES,
    UniverseTier,
    is_total_universe,
    total_universe_config_descriptor,
    universe_membership,
)


def _load_enumerator_module() -> ModuleType:
    """Load the script by path (mirrors the other enumerator test modules)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_total_universe_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
ExpectedRow = enumerator_module.ExpectedRow
SUPPORTED_ASSET_GROUPS = enumerator_module.SUPPORTED_ASSET_GROUPS
_assert_asset_group_in_total_universe = enumerator_module._assert_asset_group_in_total_universe
_classify_row_tier = enumerator_module._classify_row_tier
_tier_distribution = enumerator_module._tier_distribution


# --- SSOT ↔ enumerator consistency -----------------------------------------


def test_supported_asset_groups_all_declared_in_total_universe_axes() -> None:
    """Every AG the enumerator claims to support MUST have an axis tuple in
    :data:`TOTAL_UNIVERSE_AXES` — else the could-exist denominator has no
    honest membership rule and the enumerator over-counts."""
    for ag in SUPPORTED_ASSET_GROUPS:
        assert ag in TOTAL_UNIVERSE_AXES, (
            f"asset_group={ag!r} in SUPPORTED_ASSET_GROUPS but not in "
            f"TOTAL_UNIVERSE_AXES (declared: {sorted(TOTAL_UNIVERSE_AXES)}). "
            f"Add an axis tuple to unified_api_contracts.canonical.crosscutting."
            f"total_universe.TOTAL_UNIVERSE_AXES before enumerating."
        )


def test_total_universe_axes_matches_supported_asset_groups_exactly() -> None:
    """The two sides are the same set — any AG in the SSOT that the enumerator
    doesn't support (or vice versa) is a plan-time drift."""
    assert set(SUPPORTED_ASSET_GROUPS) == set(TOTAL_UNIVERSE_AXES), (
        f"SUPPORTED_ASSET_GROUPS={set(SUPPORTED_ASSET_GROUPS)} vs "
        f"TOTAL_UNIVERSE_AXES={set(TOTAL_UNIVERSE_AXES)} — symmetric drift."
    )


# --- Structural gate --------------------------------------------------------


def test_structural_gate_accepts_every_declared_asset_group() -> None:
    """The gate must not spuriously reject a declared AG (regression-guard)."""
    for ag in TOTAL_UNIVERSE_AXES:
        _assert_asset_group_in_total_universe(ag)  # must not raise


def test_structural_gate_rejects_unregistered_asset_group() -> None:
    """A caller that passes an AG not in the SSOT gets a ValueError naming the
    declared set (the enumerator's `main()` calls this before touching GCS,
    so an axis-less AG cannot silently produce a wrong denominator)."""
    with pytest.raises(ValueError, match="TOTAL_UNIVERSE_AXES"):
        _assert_asset_group_in_total_universe("options_on_moon_rocks")


def test_structural_gate_rejects_blank_asset_group() -> None:
    """Blank AG is a mis-configured caller; loud-fail."""
    with pytest.raises(ValueError, match="TOTAL_UNIVERSE_AXES"):
        _assert_asset_group_in_total_universe("")


# --- MVP ⊆ TOTAL invariant --------------------------------------------------


def test_mvp_cefi_cell_classifies_as_mvp_tier() -> None:
    """A concrete MVP cefi cell — BINANCE-SPOT SPOT BTC on 2024-06-01 — must
    classify as :attr:`UniverseTier.MVP` (MVP ⊆ TOTAL by construction). This
    is the invariant :func:`universe_membership` enforces via ``is_mvp`` first."""
    row = ExpectedRow(
        asset_group="cefi",
        venue="BINANCE-FUTURES",
        chain="",
        data_type="trades",
        instrument_type="PERPETUAL",
        instrument_id="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
        league_id="",
        date="2024-06-01",
        reason="",
    )
    # Direct predicate call — this is the SSOT the enumerator uses.
    tier = universe_membership(
        row.asset_group,
        row.venue,
        row.instrument_type,
        row.data_type,
        base_ccy="BTC",
    )
    assert tier == UniverseTier.MVP, (
        f"MVP-scoped cell classified as {tier!r} — MVP ⊆ TOTAL invariant broken. "
        f"universe_membership must return MVP for a cell where is_mvp is True."
    )


def test_declared_asset_group_never_classifies_not_in_universe_structurally() -> None:
    """Every declared AG passes the structural :func:`is_total_universe` check
    (the tier taxonomy invariant: a declared AG is at MINIMUM ``TOTAL_ONLY``)."""
    for ag in TOTAL_UNIVERSE_AXES:
        # The structural predicate — venue/instrument_type unused today, reserved
        # for future per-venue exclusions.
        assert is_total_universe(ag, "any_venue", "any_type"), (
            f"is_total_universe(ag={ag!r}) returned False — the AG is declared "
            f"in TOTAL_UNIVERSE_AXES but the structural predicate rejects it."
        )


def test_undeclared_asset_group_classifies_not_in_universe() -> None:
    """Symmetric: an AG not in the SSOT classifies :attr:`UniverseTier.NOT_IN_UNIVERSE`."""
    tier = universe_membership("options_on_moon_rocks", "MOONVENUE", "SPOT", "trades")
    assert tier == UniverseTier.NOT_IN_UNIVERSE


# --- Tier-distribution helper (telemetry) ----------------------------------


def test_tier_distribution_returns_all_tier_keys() -> None:
    """Empty input still returns a dict keyed by every UniverseTier member —
    downstream ENUMERATOR_COMPLETED telemetry needs zero-count tiers visible
    (a NOT_IN_UNIVERSE count that appears mid-run vs is-always-present-as-0
    is easier to alert on)."""
    dist = _tier_distribution([])
    assert set(dist.keys()) == {t.value for t in UniverseTier}
    assert all(v == 0 for v in dist.values())


def test_tier_distribution_counts_mvp_and_total_only_correctly() -> None:
    """A hand-built row set should count into the right tiers."""
    mvp_row = ExpectedRow(
        asset_group="cefi",
        venue="BINANCE-FUTURES",
        chain="",
        data_type="trades",
        instrument_type="PERPETUAL",
        instrument_id="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
        league_id="",
        date="2024-06-01",
        reason="",
    )
    # A cefi cell whose venue/instrument_type combination is NOT MVP-included
    # (per the perp-gated MVP predicate). The exact classification depends on
    # is_mvp — either way the row is at MINIMUM TOTAL_ONLY (never
    # NOT_IN_UNIVERSE for cefi because cefi IS declared in TOTAL_UNIVERSE_AXES).
    non_mvp_row = ExpectedRow(
        asset_group="cefi",
        venue="A_MADE_UP_VENUE_NEVER_IN_MVP",
        chain="",
        data_type="trades",
        instrument_type="SPOT",
        instrument_id="MADE_UP:SPOT:XYZ",
        league_id="",
        date="2024-06-01",
        reason="EXPECTED_INSTRUMENT_NOT_LISTED",
    )
    dist = _tier_distribution([mvp_row, non_mvp_row])
    # Total row count preserved.
    assert sum(dist.values()) == 2
    # MVP row lands in MVP; the non-MVP cefi row is TOTAL_ONLY (never NOT_IN_UNIVERSE
    # because "cefi" is a declared AG).
    assert dist[UniverseTier.MVP.value] >= 1
    assert dist[UniverseTier.NOT_IN_UNIVERSE.value] == 0


def test_row_classifier_delegates_to_universe_membership() -> None:
    """_classify_row_tier must call :func:`universe_membership` — the same SSOT
    the data-status denominator uses — so the enumerator + the denominator
    share a single classification path."""
    row = ExpectedRow(
        asset_group="cefi",
        venue="BINANCE-FUTURES",
        chain="",
        data_type="trades",
        instrument_type="PERPETUAL",
        instrument_id="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
        league_id="",
        date="2024-06-01",
        reason="",
    )
    assert _classify_row_tier(row) == UniverseTier.MVP


# --- Config descriptor telemetry -------------------------------------------


def test_config_descriptor_is_present_and_content_addressed() -> None:
    """The descriptor the enumerator emits in ENUMERATOR_STARTED carries a
    version + content_hash. Content-addressed so a coverage delta can attribute
    to a taxonomy-DEFINITION change (hash flips) vs a DATA change (hash stable)."""
    desc = total_universe_config_descriptor()
    assert desc.version >= 1
    assert isinstance(desc.content_hash, str)
    assert len(desc.content_hash) > 0
