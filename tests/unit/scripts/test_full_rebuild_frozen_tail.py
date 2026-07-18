"""F8 (2026-07-18): a ``--mode full`` rebuild must be CUMULATIVE-preserving.

``build_instrument_catalogue._merge_incremental(..., close_absent=False)`` is the
full-rebuild frozen-tail merge: a full walk stays authoritative for the
``available_to`` of every instrument that still has by_date data, but a
previously-catalogued instrument whose by_date has since been PRUNED must be
preserved verbatim rather than silently dropped (the 2026-07-15 sports incident
class — a full defi rebuild dropped 2,346 delisted pools/tokens, under-producing
the all-instruments-ever contract and jamming the monotonic guard).

Kept in its own file (not appended to the large, frequently-contended
test_build_instrument_catalogue.py) to avoid concurrent-edit collisions.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
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
    return _load_script_module("build_instrument_catalogue.py", "_bic_full_rebuild_test_module")


def _cat_row(**overrides: object) -> dict[str, object]:
    """A prev-catalogue row with every CATALOG column defaulted (mvp included)."""
    row: dict[str, object] = {
        "instrument_id": "X",
        "instrument_type": "SPOT_PAIR",
        "venue": "V",
        "chain": "",
        "league_id": "",
        "available_from": "2024-01-01",
        "available_to": None,
        "market_created_at": None,
        "settlement_time": None,
        "data_type": None,
        "underlying": "",
        "raw_symbol": "",
        "base_asset": "",
        "mvp": False,
        "margin_type": "",
        "glued_pair_id": "",
        "pool_address": "",
    }
    row.update(overrides)
    return row


def _window(rows: list[dict[str, object]]) -> pd.DataFrame:
    """The fresh full-walk frame (mvp is re-stamped on the merged frame, so drop it)."""
    return pd.DataFrame(rows).drop(columns=["mvp"])


def test_full_rebuild_preserves_delisted_tail_pruned_from_by_date(rollup: ModuleType) -> None:
    """F8 core: a DELISTED instrument (available_to set) whose by_date aged off the
    corpus is ABSENT from the full walk — close_absent=False preserves it verbatim
    instead of dropping it (the 2,346-pool defi shrink)."""
    prev = pd.DataFrame(
        [
            _cat_row(instrument_id="LIVE", venue="MORPHO", available_to=None),
            _cat_row(
                instrument_id="DELISTED-PRUNED", venue="MORPHO", available_from="2023-01-01", available_to="2023-06-01"
            ),
        ]
    )
    # The full walk only re-observes the still-live instrument (the delisted one's
    # by_date was pruned, so the walk cannot see it).
    window = _window([_cat_row(instrument_id="LIVE", venue="MORPHO", available_to=None)])

    merged = rollup._merge_incremental(prev, window, window_start=None, asset_group="defi", close_absent=False)

    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert set(by_id) == {"LIVE", "DELISTED-PRUNED"}, "the pruned delisted pool must NOT be dropped"
    assert by_id["DELISTED-PRUNED"]["available_to"] == "2023-06-01"  # frozen tail untouched
    assert by_id["DELISTED-PRUNED"]["available_from"] == "2023-01-01"
    assert len(merged) >= len(prev)  # monotonic guard passes by construction


def test_full_rebuild_does_not_close_active_absent_row(rollup: ModuleType) -> None:
    """With close_absent=False there is NO meaningful window boundary, so an ACTIVE
    prev row absent from the walk is preserved active (NOT closed at a garbage
    window_start-1). Contrast test below proves the default flag DOES close it."""
    prev = pd.DataFrame(
        [
            _cat_row(instrument_id="STAYS", venue="V", available_to=None),
            _cat_row(instrument_id="ACTIVE-ABSENT", venue="V", available_to=None),
        ]
    )
    window = _window([_cat_row(instrument_id="STAYS", venue="V", available_to=None)])

    merged = rollup._merge_incremental(prev, window, window_start=None, asset_group="cefi", close_absent=False)

    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert by_id["ACTIVE-ABSENT"]["available_to"] is None  # preserved active, not closed


def test_default_close_absent_still_closes_active_absent_row(rollup: ModuleType) -> None:
    """Regression guard: close_absent defaults True, so the trailing-window
    incremental path is UNCHANGED — an active-absent row whose venue still captures
    is closed at window_start-1 (branch 3)."""
    prev = pd.DataFrame(
        [
            _cat_row(instrument_id="STAYS", venue="V", available_to=None),
            _cat_row(instrument_id="ACTIVE-ABSENT", venue="V", available_to=None),
        ]
    )
    window = _window([_cat_row(instrument_id="STAYS", venue="V", available_to=None)])

    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")

    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert by_id["ACTIVE-ABSENT"]["available_to"] == "2026-06-11"  # window_start - 1


def test_full_rebuild_known_row_takes_min_available_from(rollup: ModuleType) -> None:
    """Full-rebuild parity: a known row takes the fresh walk metadata but keeps the
    EARLIEST available_from (immutable-once-set) — a walk whose early by_date was
    pruned would otherwise drift the listing date later."""
    prev = pd.DataFrame([_cat_row(instrument_id="A", available_from="2022-01-01", raw_symbol="old")])
    window = _window([_cat_row(instrument_id="A", available_from="2024-05-01", raw_symbol="new")])

    merged = rollup._merge_incremental(prev, window, window_start=None, asset_group="cefi", close_absent=False)

    row = merged.to_dict("records")[0]
    assert row["available_from"] == "2022-01-01"  # earliest kept (immutable-once-set)
    assert row["raw_symbol"] == "new"  # metadata follows the fresh walk


def test_close_absent_true_requires_window_start(rollup: ModuleType) -> None:
    """A guard: the trailing-window incremental MUST pass a window_start (its
    delist-close boundary); only the full-rebuild merge may omit it."""
    prev = pd.DataFrame([_cat_row(instrument_id="GONE", venue="V", available_to=None)])
    window = _window([_cat_row(instrument_id="OTHER", venue="V")])
    with pytest.raises(ValueError, match="window_start"):
        rollup._merge_incremental(prev, window, window_start=None, asset_group="cefi", close_absent=True)
