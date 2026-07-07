"""Unit tests — B2 downstream wiring: enumerate_expected_universe.py bound to
the UAC ``TOTAL_UNIVERSE_AXES`` SSOT.

Verifies:
  - The enumerator's dispatch tables (v1 / v2 / CLI) match ``TOTAL_UNIVERSE_AXES``.
  - ``enumerate_v2`` rejects an asset_group NOT in the SSOT (structural gate).
  - The rejection message names the UAC SSOT so operators trace back to the axes.
  - MVP ⊆ TOTAL is respected — every row emitted for a valid AG classifies as
    ``UniverseTier.MVP`` or ``UniverseTier.TOTAL_ONLY`` (never NOT_IN_UNIVERSE).
  - The stamped SSOT descriptor constants (version/hash) are the ones exported.

Ships per ``plans/active/is_catalogue_completion_2d_2026_07_06.md`` — B2 downstream.
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
    universe_membership,
)


def _load_enumerator_module() -> ModuleType:
    """Load the enumerator script as a module by path (script lives outside the package)."""
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
InstrumentCatalogEntry = enumerator_module.InstrumentCatalogEntry


# ---------------------------------------------------------------------------
# Dispatch parity — v1 / v2 / CLI choices ≡ TOTAL_UNIVERSE_AXES keys
# ---------------------------------------------------------------------------


def test_v2_dispatch_equals_uac_total_universe_axes() -> None:
    """_V2_ENUMERATORS keys must equal TOTAL_UNIVERSE_AXES keys.

    Drift here would mean the enumerator either serves an AG the SSOT doesn't
    declare (silent could-exist under-count) OR the SSOT declares an AG the
    enumerator can't serve (silent zero denominator).
    """
    assert set(enumerator_module._V2_ENUMERATORS) == set(TOTAL_UNIVERSE_AXES)


def test_v1_dispatch_equals_uac_total_universe_axes() -> None:
    """_ENUMERATORS (v1 calendar/genesis-pre-skip) must also cover exactly the SSOT AGs."""
    assert set(enumerator_module._ENUMERATORS) == set(TOTAL_UNIVERSE_AXES)


def test_supported_asset_groups_equals_uac_total_universe_axes() -> None:
    """The CLI ``--asset-group`` choices must equal the SSOT AG set — no way to
    invoke the script on an AG the SSOT doesn't recognise."""
    assert set(enumerator_module.SUPPORTED_ASSET_GROUPS) == set(TOTAL_UNIVERSE_AXES)


# ---------------------------------------------------------------------------
# enumerate_v2 SSOT gate
# ---------------------------------------------------------------------------


def test_enumerate_v2_rejects_asset_group_not_in_uac_axes() -> None:
    """Passing an AG absent from TOTAL_UNIVERSE_AXES raises ValueError."""
    with pytest.raises(ValueError, match="unsupported asset_group"):
        list(
            enumerator_module.enumerate_v2(
                asset_group="equities_options",  # not a real UAC AG
                catalog=[],
                date_axis=[date(2024, 1, 1)],
                data_types=["ohlcv_1d"],
            )
        )


def test_enumerate_v2_error_message_names_uac_ssot() -> None:
    """The rejection message names ``TOTAL_UNIVERSE_AXES`` so operators know the SSOT."""
    with pytest.raises(ValueError, match="TOTAL_UNIVERSE_AXES"):
        list(
            enumerator_module.enumerate_v2(
                asset_group="not_an_ag",
                catalog=[],
                date_axis=[date(2024, 1, 1)],
                data_types=["ohlcv_1d"],
            )
        )


@pytest.mark.parametrize("asset_group", sorted(TOTAL_UNIVERSE_AXES.keys()))
def test_enumerate_v2_accepts_every_declared_ag(asset_group: str) -> None:
    """Every AG in TOTAL_UNIVERSE_AXES is dispatchable — the ``is_total_universe``
    SSOT gate accepts the AG and the enumerator yields a well-formed iterator.

    We don't assert the row count: tradfi's v2 layers v1 calendar-holiday
    enumeration on top of the catalogue (:func:`_yield_v2_tradfi_non_trading_day_rows`
    fires unconditionally), so an empty catalog still yields rows for holiday/
    weekend dates — that's real calendar-driven semantics, not a dispatch defect.
    What we're proving here is that the SSOT gate resolves every declared AG
    without raising.
    """
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group=asset_group,
            catalog=[],
            date_axis=[date(2024, 1, 1)],
            data_types=["ohlcv_1d"],
        )
    )
    assert isinstance(rows, list)  # dispatch resolved cleanly; iterator exhausted


# ---------------------------------------------------------------------------
# MVP ⊆ TOTAL invariance — every yielded row is in the universe hierarchy
# ---------------------------------------------------------------------------


def test_mvp_cefi_rows_are_in_total_universe() -> None:
    """An MVP cefi catalogue entry → every yielded row classifies as MVP or TOTAL_ONLY.

    NOT_IN_UNIVERSE for a valid AG would mean the enumerator seeded a cell outside
    the total-reasonable-universe — a denominator inflation bug. The universe check
    is structural (``asset_group in TOTAL_UNIVERSE_AXES``); every cefi row passes it.
    """
    catalog = [
        InstrumentCatalogEntry(
            instrument_id="BTC-USDT-PERP",
            instrument_type="PERPETUAL",
            venue="BINANCE-FUTURES",
            chain="",
            league_id="",
            available_from="2025-01-01",
            available_to=None,
            market_created_at=None,
            settlement_time=None,
            base_asset="BTC",
            mvp=True,
        )
    ]
    date_axis = [date(2020, 1, 1), date(2025, 6, 1)]  # 2020-01-01 → NOT_LISTED; 2025-06-01 alive
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=date_axis,
            data_types=["trades"],
        )
    )
    assert rows, "expected at least one pre-listing row"
    for row in rows:
        tier = universe_membership(
            row.asset_group,
            row.venue,
            row.instrument_type,
            row.data_type,
            base_ccy="BTC",
        )
        assert tier != UniverseTier.NOT_IN_UNIVERSE, (
            f"row {row!r} classified NOT_IN_UNIVERSE — total-universe SSOT drift"
        )


# ---------------------------------------------------------------------------
# SSOT descriptor constants — the enumerator stamps these into ENUMERATOR_STARTED
# ---------------------------------------------------------------------------


def test_enumerator_module_imports_uac_ssot_descriptor() -> None:
    """The enumerator module holds the UAC ``TOTAL_UNIVERSE_CONFIG_{VERSION,HASH}``
    constants so the ``ENUMERATOR_STARTED`` event can stamp them (universe-
    definition change vs data change attribution)."""
    assert enumerator_module.TOTAL_UNIVERSE_CONFIG_VERSION == TOTAL_UNIVERSE_CONFIG_VERSION
    assert enumerator_module.TOTAL_UNIVERSE_CONFIG_HASH == TOTAL_UNIVERSE_CONFIG_HASH
    assert isinstance(TOTAL_UNIVERSE_CONFIG_VERSION, int)
    assert isinstance(TOTAL_UNIVERSE_CONFIG_HASH, str)
    assert len(TOTAL_UNIVERSE_CONFIG_HASH) == 16
