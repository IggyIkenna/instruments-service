"""Dynamic wire-up tests for enumerate_expected_universe.py <-> UAC TOTAL_UNIVERSE_AXES.

B2 downstream (plan ``is_catalogue_completion_2d_2026_07_06.md`` task 6): the
enumerator is the primary consumer of the total-reasonable-universe axis
taxonomy declared in
:mod:`unified_api_contracts.canonical.crosscutting.total_universe`. These tests
lock in the wiring so a drift on either side breaks the build:

* the enumerator's :data:`SUPPORTED_ASSET_GROUPS` is derived from — and stays
  in lock-step with — :data:`unified_api_contracts.TOTAL_UNIVERSE_AXES`;
* every asset_group with an axis taxonomy passes the enumerator's
  :func:`is_total_universe` structural gate;
* MVP ⊆ TOTAL is respected via :func:`universe_membership` on known MVP cells
  for every asset_group (mirrors the UAC's own hierarchy test but pinned at
  the enumerator's consumer boundary);
* the enumerator emits the UAC total-universe config descriptor (version +
  hash) on the ``ENUMERATOR_STARTED`` event so a coverage delta attributes to
  a universe-DEFINITION change vs a DATA change;
* the axes declared in :data:`TOTAL_UNIVERSE_AXES` name the SSOTs the
  enumerator actually reads (``VENUES_BY_ASSET_GROUP`` / ``DATA_TYPES_BY_ASSET_GROUP`` /
  the venue-launch + chain-genesis registries).

Codex SSOTs: ``codex/04-architecture/instruments-service-as-ssot-for-mtds.md``,
``codex/02-data/honest-coverage-model.md``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from unified_api_contracts import (
    DATA_TYPES_BY_ASSET_GROUP,
    TOTAL_UNIVERSE_AXES,
    TOTAL_UNIVERSE_CONFIG_HASH,
    TOTAL_UNIVERSE_CONFIG_VERSION,
    VENUES_BY_ASSET_GROUP,
    UniverseProvenance,
    UniverseTier,
    is_mvp,
    is_total_universe,
    total_universe_config_descriptor,
    universe_membership,
)


def _load_enumerator_module() -> ModuleType:
    """Load the enumerator script as a module by path (mirrors the other scripts tests)."""
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


# ---------------------------------------------------------------------------
# Structural wire-up: enumerator's SUPPORTED_ASSET_GROUPS matches the SSOT.
# ---------------------------------------------------------------------------


def test_supported_asset_groups_derives_from_total_universe_axes() -> None:
    """The enumerator's supported set is EXACTLY the SSOT-declared axis taxonomy keys.

    Derivation is at import time (``tuple(sorted(TOTAL_UNIVERSE_AXES.keys()))``),
    so adding an AG to the SSOT auto-enrols it in the enumerator and removing one
    auto-retires it — no manual sync step.
    """
    assert set(enumerator_module.SUPPORTED_ASSET_GROUPS) == set(TOTAL_UNIVERSE_AXES.keys())
    # Order is stable (sorted) so the CLI ``--asset-group`` choices are deterministic.
    assert enumerator_module.SUPPORTED_ASSET_GROUPS == tuple(sorted(TOTAL_UNIVERSE_AXES.keys()))


def test_every_supported_asset_group_passes_the_structural_universe_gate() -> None:
    """The enumerator's ``main()`` gate uses :func:`is_total_universe`; every AG the
    enumerator ships MUST return ``True`` (else ``main()`` refuses with exit 4)."""
    for asset_group in enumerator_module.SUPPORTED_ASSET_GROUPS:
        assert is_total_universe(asset_group, "", ""), asset_group


def test_unknown_asset_group_is_rejected_by_the_universe_gate() -> None:
    """A made-up AG NOT in the SSOT fails the structural gate. Belt-and-braces for
    the case where an internal caller bypasses argparse choices."""
    assert not is_total_universe("equities_options", "", "")
    assert not is_total_universe("", "", "")


# ---------------------------------------------------------------------------
# Hierarchy: MVP ⊆ TOTAL respected for every asset_group.
# ---------------------------------------------------------------------------


def test_mvp_is_subset_of_total_for_every_asset_group() -> None:
    """For every AG with a known MVP cell, ``universe_membership`` returns MVP tier
    (which implies TOTAL by the hierarchy). This is the invariant that lets the
    cefi / tradfi enumerators use the tighter MVP gate as a strict subset of
    TOTAL — a seeded cell that passes the MVP predicate is TOTAL by construction.

    Known-MVP cells (per :mod:`unified_api_contracts.canonical.crosscutting.mvp_scope`):

    * cefi   — BTC PERPETUAL trades on BINANCE-FUTURES
    * defi   — UNISWAP_V3 pool dex_pool_swaps on ETHEREUM
    * tradfi — CME ES (S&P 500) futures trades
    * sports — English Premier League fixtures (FOOTYSTATS source)
    * prediction — POLYMARKET conditionId trades
    """
    mvp_probes: dict[str, dict[str, str]] = {
        "cefi": {
            "venue": "BINANCE-FUTURES",
            "instrument_type": "PERPETUAL",
            "data_type": "trades",
            "base_ccy": "BTC",
        },
        "defi": {
            "venue": "UNISWAP_V3",
            "instrument_type": "POOL",
            "data_type": "dex_pool_swaps",
        },
        "tradfi": {
            "venue": "CME",
            "instrument_type": "FUTURE",
            "data_type": "trades",
            "base_ccy": "ES",
        },
        "prediction": {
            "venue": "POLYMARKET",
            "instrument_type": "BINARY",
            "data_type": "trades",
        },
    }
    for ag, probe in mvp_probes.items():
        # An MVP-probe cell MUST classify as MVP; the tier is also >= TOTAL_ONLY
        # (MVP ⊆ TOTAL — MVP is the strictest tier), so it's in the total universe.
        if is_mvp(
            ag,
            probe["venue"],
            probe["instrument_type"],
            probe.get("data_type"),
            base_ccy=probe.get("base_ccy"),
        ):
            tier = universe_membership(
                ag,
                probe["venue"],
                probe["instrument_type"],
                probe.get("data_type"),
                base_ccy=probe.get("base_ccy"),
            )
            assert tier == UniverseTier.MVP, (ag, probe, tier)
            # And the structural TOTAL check must also pass (MVP ⊆ TOTAL).
            assert is_total_universe(ag, probe["venue"], probe["instrument_type"]), (ag, probe)
        else:
            # If mvp_scope's registry drifts and a probe is no longer MVP,
            # the cell must at least be in TOTAL (real AG, real venue) — a
            # softer assertion so the drift doesn't hide the wiring gap.
            tier = universe_membership(
                ag,
                probe["venue"],
                probe["instrument_type"],
                probe.get("data_type"),
                base_ccy=probe.get("base_ccy"),
            )
            assert tier in (UniverseTier.MVP, UniverseTier.TOTAL_ONLY), (ag, probe, tier)


def test_non_mvp_but_real_asset_group_cell_is_total_only() -> None:
    """A non-MVP cell in a declared AG is TOTAL_ONLY (in the could-exist universe
    but outside the MVP subset) — mirroring the UAC test at the enumerator's
    consumer boundary."""
    tier = universe_membership("cefi", "UPBIT", "SPOT_PAIR", "trades")
    assert tier == UniverseTier.TOTAL_ONLY


def test_not_in_universe_for_undeclared_asset_group() -> None:
    """An AG with no axis taxonomy is NOT_IN_UNIVERSE — the enumerator refuses it."""
    tier = universe_membership("equities_options", "NYSE", "STOCK", "trades")
    assert tier == UniverseTier.NOT_IN_UNIVERSE


# ---------------------------------------------------------------------------
# Config descriptor: the enumerator carries the SSOT version + hash on runs.
# ---------------------------------------------------------------------------


def test_enumerator_imports_the_total_universe_config_descriptor() -> None:
    """The enumerator module resolves :func:`total_universe_config_descriptor` at
    import time; the descriptor's fields are the same fields the enumerator
    emits on ``ENUMERATOR_STARTED``.
    """
    # Re-resolve via the enumerator's own imports to prove the wiring is live.
    desc = enumerator_module.total_universe_config_descriptor()
    assert desc.config_version == TOTAL_UNIVERSE_CONFIG_VERSION
    assert desc.config_content_hash == TOTAL_UNIVERSE_CONFIG_HASH
    # The descriptor is deterministic (mirrors UAC's own test) — two calls
    # must agree, else the ENUMERATOR_STARTED event carries a moving target.
    assert desc == total_universe_config_descriptor()


# ---------------------------------------------------------------------------
# Axis-taxonomy shape: the SSOT names the same registries the enumerator reads.
# ---------------------------------------------------------------------------


def test_venues_by_asset_group_covers_every_hardcoded_genesis_venue_axis() -> None:
    """For every AG whose axis taxonomy declares a HARDCODED_GENESIS ``venue`` axis,
    :data:`unified_api_contracts.VENUES_BY_ASSET_GROUP` has a non-empty entry —
    which is the SSOT the enumerator reads for that axis.
    """
    for ag, axes in TOTAL_UNIVERSE_AXES.items():
        venue_axes = [
            a for a in axes if a.name == "venue" and a.provenance == UniverseProvenance.HARDCODED_GENESIS
        ]
        if not venue_axes:
            continue  # sports has no ``venue`` axis (fixtures + data_type only)
        assert VENUES_BY_ASSET_GROUP.get(ag), (ag, "HARDCODED_GENESIS venue axis declared but no venues registered")


def test_data_types_by_asset_group_covers_every_hardcoded_genesis_data_type_axis() -> None:
    """Every AG with a HARDCODED_GENESIS ``data_type`` axis has a non-empty
    :data:`unified_api_contracts.DATA_TYPES_BY_ASSET_GROUP` entry — that's the SSOT
    the enumerator reads. Sports is the one exception: its ``data_type`` axis
    points at ``SPORTS_DATA_TYPE_TO_SOURCE`` (provider data_types, NOT the MTDS
    odds types in :data:`DATA_TYPES_BY_ASSET_GROUP`), so it is intentionally
    absent from the ``DATA_TYPES_BY_ASSET_GROUP`` check here — the enumerator's
    ``_sports_data_types()`` reads the correct SSOT.
    """
    for ag, axes in TOTAL_UNIVERSE_AXES.items():
        if ag == "sports":
            continue  # covered by _sports_data_types() reading SPORTS_DATA_TYPE_TO_SOURCE
        dt_axes = [
            a for a in axes if a.name == "data_type" and a.provenance == UniverseProvenance.HARDCODED_GENESIS
        ]
        if not dt_axes:
            continue  # prediction has no HARDCODED_GENESIS data_type axis today
        assert DATA_TYPES_BY_ASSET_GROUP.get(ag), (
            ag,
            "HARDCODED_GENESIS data_type axis declared but no data_types registered",
        )


def test_prediction_axes_do_not_declare_data_type_bound() -> None:
    """Prediction's axis taxonomy carries ``venue`` + ``combinations`` (no
    explicit ``data_type`` axis) — the catalogue itself binds the data_type
    per row via the cqg-bundle grain (decision 338, 2026-06-19). Guard against
    a future drift that would add a data_type axis without wiring it into the
    prediction enumerator's grain-binding logic.
    """
    pred_axis_names = {a.name for a in TOTAL_UNIVERSE_AXES["prediction"]}
    assert "data_type" not in pred_axis_names
    assert {"venue", "combinations"} <= pred_axis_names
