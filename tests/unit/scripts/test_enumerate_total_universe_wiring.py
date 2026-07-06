"""Unit tests — B2 wiring: ``enumerate_expected_universe`` reads the UAC
total-reasonable-universe SSOT (``TOTAL_UNIVERSE_AXES``).

Gate for ``is_catalogue_completion_2d_2026_07_06`` B2 downstream item: the
enumerator's supported-AG list is derived from the UAC SSOT (dynamic, not
hardcoded); the per-AG dispatch tables cover exactly the SSOT-declared AGs;
an unknown AG raises with an SSOT-pointing message; the MVP⊆TOTAL invariant
holds on emitted rows via the ``universe_membership`` classifier; and the
``ENUMERATOR_STARTED`` event stamps the total-universe config descriptor
(version + hash).

SSOT: ``unified_api_contracts.canonical.crosscutting.total_universe``.
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
    TOTAL_UNIVERSE_CONFIG_HASH,
    TOTAL_UNIVERSE_CONFIG_VERSION,
    UniverseTier,
)


def _load_enumerator_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_total_universe_wiring_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()


# ---------------------------------------------------------------------------
# SSOT-derived supported-AG list
# ---------------------------------------------------------------------------


def test_supported_asset_groups_derived_from_uac_ssot() -> None:
    """``SUPPORTED_ASSET_GROUPS`` MUST equal the UAC SSOT keys (dynamic).

    The whole point of B2 is that the enumerator reads
    ``TOTAL_UNIVERSE_AXES`` — a new AG in UAC flows through without an
    enumerator code change. If the enumerator hardcodes a different list,
    this test fires.
    """
    assert set(enumerator_module.SUPPORTED_ASSET_GROUPS) == set(TOTAL_UNIVERSE_AXES.keys())


def test_supported_asset_groups_is_sorted() -> None:
    """Deterministic order for the ``--asset-group`` choices display."""
    _sag = enumerator_module.SUPPORTED_ASSET_GROUPS
    assert list(_sag) == sorted(_sag)


def test_v1_and_v2_dispatch_tables_cover_uac_ssot_axes() -> None:
    """Both dispatch tables MUST cover exactly the SSOT-declared AGs.

    The module-load sanity check
    (``_check_enumerator_dispatch_covers_total_universe_axes``) raises if drift
    exists, so if the module loaded at all, this assertion is a belt-and-braces
    equality check that survives future refactors.
    """
    _declared = set(TOTAL_UNIVERSE_AXES.keys())
    assert set(enumerator_module._ENUMERATORS.keys()) == _declared
    assert set(enumerator_module._V2_ENUMERATORS.keys()) == _declared


# ---------------------------------------------------------------------------
# Structural guard — is_total_universe wired to enumerate_v2
# ---------------------------------------------------------------------------


def test_enumerate_v2_rejects_asset_group_not_in_total_universe() -> None:
    """An AG absent from the SSOT is rejected with a message that points at
    ``TOTAL_UNIVERSE_AXES`` — not a silent empty enumeration."""
    with pytest.raises(ValueError, match="TOTAL_UNIVERSE_AXES"):
        # Materialise the generator so the guard actually runs.
        list(
            enumerator_module.enumerate_v2(
                asset_group="equities_options",  # NOT in TOTAL_UNIVERSE_AXES
                catalog=[],
                date_axis=[date(2026, 1, 1)],
            )
        )


def test_ensure_asset_group_in_total_universe_accepts_every_declared_ag() -> None:
    """Each declared AG passes the structural guard cleanly."""
    for ag in TOTAL_UNIVERSE_AXES:
        # No raise = pass.
        enumerator_module._ensure_asset_group_in_total_universe(ag, caller="test")


# ---------------------------------------------------------------------------
# MVP⊆TOTAL invariant via _classify_membership_row
# ---------------------------------------------------------------------------


def test_classify_membership_row_returns_mvp_for_known_mvp_cell() -> None:
    """A known MVP cell (BTC perp on BINANCE-FUTURES trades) classifies as MVP.

    MVP ⊆ TOTAL means an MVP-classified row is also in the total universe.
    """
    ExpectedRow = enumerator_module.ExpectedRow
    row = ExpectedRow(
        asset_group="cefi",
        venue="BINANCE-FUTURES",
        chain="",
        data_type="trades",
        instrument_type="PERPETUAL",
        instrument_id="BTC-USDT",
        league_id="",
        date="2026-01-01",
        reason="",
        capture_status="expected_unattempted",
    )
    assert enumerator_module._classify_membership_row(row) == UniverseTier.MVP


def test_classify_membership_row_returns_total_only_for_non_mvp_real_ag_cell() -> None:
    """A non-MVP cell in a real universe-bearing AG classifies as TOTAL_ONLY."""
    ExpectedRow = enumerator_module.ExpectedRow
    row = ExpectedRow(
        asset_group="cefi",
        venue="UPBIT",  # real cefi venue but not MVP scope
        chain="",
        data_type="trades",
        instrument_type="SPOT_PAIR",
        instrument_id="BTC-KRW",
        league_id="",
        date="2026-01-01",
        reason="",
        capture_status="expected_unattempted",
    )
    assert enumerator_module._classify_membership_row(row) == UniverseTier.TOTAL_ONLY


def test_classify_membership_row_returns_not_in_universe_for_unknown_ag() -> None:
    """A row from an AG that isn't declared in the SSOT is NOT_IN_UNIVERSE."""
    ExpectedRow = enumerator_module.ExpectedRow
    row = ExpectedRow(
        asset_group="equities_options",  # NOT in TOTAL_UNIVERSE_AXES
        venue="NYSE",
        chain="",
        data_type="trades",
        instrument_type="STOCK",
        instrument_id="AAPL",
        league_id="",
        date="2026-01-01",
        reason="",
        capture_status="expected_unattempted",
    )
    assert enumerator_module._classify_membership_row(row) == UniverseTier.NOT_IN_UNIVERSE


# ---------------------------------------------------------------------------
# Config descriptor is the same object the enumerator will stamp
# ---------------------------------------------------------------------------


def test_enumerator_module_reads_current_universe_config_descriptor() -> None:
    """The enumerator's stamped descriptor MUST match the UAC-exported constants.

    Guards against a stale local copy of the version / hash — the enumerator
    calls ``total_universe_config_descriptor()`` at run-time, so the value
    tracks any UAC bump automatically. This test just documents the wiring.
    """
    _desc = enumerator_module.total_universe_config_descriptor()
    assert _desc.config_version == TOTAL_UNIVERSE_CONFIG_VERSION
    assert _desc.config_content_hash == TOTAL_UNIVERSE_CONFIG_HASH
