"""Integration test — v2 enumerator is a strict superset of v1 at the per-cell level.

Closes ``expected_universe_v2_design_2026_05_08.md`` Phase 1 P1 deferred
integration-test claim: "Assert no v1 row missing from v2 output (v2 is strict
superset)".

The deferred claim required a live instruments-service catalog read; we cover
the SUPERSET PROPERTY here against synthetic catalogs (the property is
data-shape-invariant; live-catalog x 100-day-axis performance is the unrelated
sub-claim that genuinely needs a same-region VM and stays deferred).

Grain mapping:

    v1 emits rows at (asset_group, venue, data_type, day) [+ chain for defi]
    v2 emits rows at (asset_group, venue, data_type, instrument_id, day)

The superset property: for every (asset_group, venue, data_type, day) cell
where v1 yields ≥1 row, v2 yields ≥1 row at the same cell when the catalog
contains ≥1 instrument for that venue on a day in the venue's pre-existence
window. This test verifies the property holds for:

  - cefi: pre-venue-launch dates (CEFI_VENUE_LAUNCH_DATES)
  - defi: pre-genesis-chain dates (CHAIN_GENESIS_DATES) + protocol pre-launch
  - prediction: pre-venue-launch dates (PREDICTION_VENUE_LAUNCH_DATES)
  - tradfi: venue-grain non-trading days (weekends + holidays) — closed
    2026-07-06 by ``_yield_v2_tradfi_non_trading_day_rows`` (venue-grain pass
    within ``_enumerate_v2_tradfi``); no catalog required for the venue-grain
    cells.

Sports v1 (per-source pre-coverage dates) remains a documented asymmetry — v1
enumerates per-source not per-fixture; v2 enumerates per-fixture lifecycle. We
assert v2 ≥ v1 only on the grains where the property applies.

Plan: expected_universe_v2_design_2026_05_08.md Phase 1 P1
Status flip evidence: instruments-service@<commit-after-this-test>
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_enumerator_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_superset_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
InstrumentCatalogEntry = enumerator_module.InstrumentCatalogEntry


def _venue_day_dt_cells(rows: list) -> set[tuple[str, str, str, str]]:
    """Collapse v1/v2 rows to the common cell key (asset_group, venue, data_type, day)."""
    return {(r.asset_group, r.venue, r.data_type, r.date) for r in rows}


def _date_axis(start: str, end: str) -> list[date]:
    return [d.date() for d in pd.date_range(start, end, freq="D")]


# ---------------------------------------------------------------------------
# CeFi: pre-venue-launch superset
# ---------------------------------------------------------------------------


def test_cefi_v2_covers_all_v1_pre_venue_launch_cells() -> None:
    """For every (venue, data_type, day) cell v1 emits as EXPECTED_PRE_VENUE_LAUNCH,
    v2 emits ≥1 row at the same cell when the catalog has ≥1 instrument for that venue.

    CEFI_VENUE_LAUNCH_DATES seeds the v1 enumeration; we synthesise a catalog
    with one instrument per CeFi venue whose available_from sits well after
    the venue launch so the per-instrument enumerator also yields rows for
    the pre-venue-launch dates.
    """
    cefi_venue_launches = enumerator_module.CEFI_VENUE_LAUNCH_DATES
    # Sample 3 venues with the earliest launches (gives us a long pre-launch window).
    venues = sorted(cefi_venue_launches.keys(), key=lambda v: cefi_venue_launches[v])[:3]
    # Window: 2010-01-01 to the latest sampled launch date - 1.
    latest_launch = max(cefi_venue_launches[v] for v in venues)
    start = "2010-01-01"
    end_ts = pd.Timestamp(latest_launch) - pd.Timedelta(days=1)
    end = end_ts.strftime("%Y-%m-%d")

    # v1 walks all CeFi venues + data_types from the registry; restrict to one
    # data_type by intersecting the result with our sampled venues.
    v1_rows_all = list(enumerator_module._enumerate_cefi(start, end))
    v1_rows = [r for r in v1_rows_all if r.venue in venues]
    v1_cells = _venue_day_dt_cells(v1_rows)
    assert len(v1_cells) > 0, "v1 should yield ≥1 pre-launch cell for sampled venues"

    # Synthetic catalog: one instrument per sampled venue, available_from AFTER
    # the venue launch (so the v2 per-instrument enumerator emits
    # EXPECTED_PRE_VENUE_LAUNCH rows for every pre-launch day in the window).
    catalog = [
        InstrumentCatalogEntry(
            instrument_id=f"{venue}-INSTR",
            instrument_type="SPOT",
            venue=venue,
            chain="",
            league_id="",
            available_from=cefi_venue_launches[venue],  # alive on launch date
            available_to=None,
            market_created_at=None,
            settlement_time=None,
        )
        for venue in venues
    ]

    # Restrict v2 to the v1 data_types so we can compare cells apples-to-apples.
    data_types_in_v1 = sorted({r.data_type for r in v1_rows})
    v2_rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=_date_axis(start, end),
            data_types=data_types_in_v1,
        )
    )
    v2_cells = _venue_day_dt_cells(v2_rows)

    missing = v1_cells - v2_cells
    assert not missing, f"v2 missing {len(missing)} cells covered by v1 (sample: {sorted(missing)[:5]})"


# ---------------------------------------------------------------------------
# DeFi: pre-genesis-chain superset
# ---------------------------------------------------------------------------


def test_defi_v2_covers_v1_pre_genesis_chain_cells() -> None:
    """v2 defi enumerator must cover every v1 pre-genesis-chain cell when the
    catalog has ≥1 instrument on the matching (venue, chain) tuple.

    v1 emits rows for (PROTOCOL-CHAIN, day) where day < max(chain_genesis,
    protocol_launch). v2's per-instrument enumerator emits the same shape when
    the catalog instrument's chain matches and the instrument's
    available_from = protocol launch date.
    """
    protocol_launches = enumerator_module.PROTOCOL_LAUNCH_DATES
    # Sample 3 (chain, protocol) tuples with earliest launches.
    sampled = sorted(protocol_launches.items(), key=lambda kv: kv[1])[:3]

    earliest_launch = sampled[0][1]
    latest_launch = sampled[-1][1]
    start = "2018-01-01"
    end_ts = pd.Timestamp(latest_launch) - pd.Timedelta(days=1)
    end = end_ts.strftime("%Y-%m-%d")

    v1_rows_all = list(enumerator_module._enumerate_defi(start, end))
    sampled_venues = {f"{proto.upper()}-{chain.upper()}" for (chain, proto), _ in sampled}
    v1_rows = [r for r in v1_rows_all if r.venue in sampled_venues]
    v1_cells = _venue_day_dt_cells(v1_rows)
    assert len(v1_cells) > 0, "v1 should yield ≥1 pre-launch cell for sampled defi venues"

    catalog = [
        InstrumentCatalogEntry(
            instrument_id=f"{proto.upper()}-{chain.upper()}-INSTR",
            instrument_type="SPOT",
            venue=f"{proto.upper()}-{chain.upper()}",
            chain=chain.upper(),
            league_id="",
            available_from=launch_date,  # alive on protocol launch
            available_to=None,
            market_created_at=None,
            settlement_time=None,
        )
        for (chain, proto), launch_date in sampled
    ]

    data_types_in_v1 = sorted({r.data_type for r in v1_rows})
    v2_rows = list(
        enumerator_module.enumerate_v2(
            asset_group="defi",
            catalog=catalog,
            date_axis=_date_axis(start, end),
            data_types=data_types_in_v1,
        )
    )
    v2_cells = _venue_day_dt_cells(v2_rows)

    missing = v1_cells - v2_cells
    assert not missing, (
        f"v2 missing {len(missing)} pre-genesis/launch cells covered by v1 (sample: {sorted(missing)[:5]})"
    )


# ---------------------------------------------------------------------------
# Prediction: pre-venue-launch superset
# ---------------------------------------------------------------------------


def test_prediction_v2_covers_v1_pre_venue_launch_cells() -> None:
    """v2 prediction enumerator covers every v1 pre-venue-launch cell when the
    catalog has ≥1 market instrument with market_created_at = venue launch."""
    pred_launches = enumerator_module.PREDICTION_VENUE_LAUNCH_DATES
    if not pred_launches:
        pytest.skip("PREDICTION_VENUE_LAUNCH_DATES is empty — nothing to assert")
    venues = sorted(pred_launches.keys(), key=lambda v: pred_launches[v])[:2]
    latest = max(pred_launches[v] for v in venues)
    start = "2018-01-01"
    end_ts = pd.Timestamp(latest) - pd.Timedelta(days=1)
    end = end_ts.strftime("%Y-%m-%d")

    v1_rows_all = list(enumerator_module._enumerate_prediction(start, end))
    v1_rows = [r for r in v1_rows_all if r.venue in venues]
    if not v1_rows:
        pytest.skip("v1 prediction enumerator yielded 0 rows for sampled venues")
    v1_cells = _venue_day_dt_cells(v1_rows)

    catalog = [
        InstrumentCatalogEntry(
            instrument_id=f"{venue}-MKT-001",
            instrument_type="BINARY",
            venue=venue,
            chain="",
            league_id="",
            available_from=None,
            available_to=None,
            market_created_at=pred_launches[venue],
            settlement_time=None,
        )
        for venue in venues
    ]
    data_types_in_v1 = sorted({r.data_type for r in v1_rows})
    v2_rows = list(
        enumerator_module.enumerate_v2(
            asset_group="prediction",
            catalog=catalog,
            date_axis=_date_axis(start, end),
            data_types=data_types_in_v1,
        )
    )
    v2_cells = _venue_day_dt_cells(v2_rows)

    missing = v1_cells - v2_cells
    assert not missing, (
        f"v2 prediction missing {len(missing)} pre-launch cells covered by v1 (sample: {sorted(missing)[:5]})"
    )


# ---------------------------------------------------------------------------
# TradFi: venue-grain non-trading-day (weekend + holiday) superset
# ---------------------------------------------------------------------------


def test_tradfi_v2_covers_v1_non_trading_day_cells() -> None:
    """v2 tradfi enumerator covers every v1 venue-grain non-trading-day cell.

    v1 ``_enumerate_tradfi`` walks ``VENUES_BY_ASSET_GROUP["tradfi"]`` x
    non-trading-days x ``DATA_TYPES_BY_ASSET_GROUP["tradfi"]`` and emits
    venue-grain rows (``instrument_type=""`` ``instrument_id=""``) with
    ``EXPECTED_WEEKEND`` / ``EXPECTED_HOLIDAY`` reasons. v2 must cover the same
    ``(asset_group, venue, data_type, day)`` cells via the venue-grain pass
    in ``_enumerate_v2_tradfi``. The catalog is IRRELEVANT for the venue-grain
    cells (whole venue closed) — an empty catalog is a valid input.

    Window includes a full week so both weekend (Sat/Sun) and one weekday US
    market holiday (2024-07-04, Independence Day) are exercised.
    """
    # 2024-07-01 (Mon) through 2024-07-07 (Sun) — spans Independence Day (Thu)
    # AND a Sat/Sun weekend, so the non-trading-day set is non-empty for every
    # weekly-closed venue AND for US-holiday-observing venues.
    start = "2024-07-01"
    end = "2024-07-07"

    v1_rows_all = list(enumerator_module._enumerate_tradfi(start, end))
    # _enumerate_tradfi also delegates to _enumerate_tradfi_indices at the end
    # (per-instrument Yahoo index pre-genesis rows). Those are per-instrument
    # cells, not venue-grain non-trading-day cells, and v2 covers them via its
    # own per-instrument pass (available_from lifecycle) — filter them out
    # here so we only assert on the venue-grain non-trading-day slice.
    v1_venue_grain = [r for r in v1_rows_all if r.instrument_id == "" and r.instrument_type == ""]
    v1_cells = _venue_day_dt_cells(v1_venue_grain)
    assert len(v1_cells) > 0, "v1 should yield ≥1 venue-grain non-trading-day cell for this window"

    data_types_in_v1 = sorted({r.data_type for r in v1_venue_grain})
    v2_rows = list(
        enumerator_module.enumerate_v2(
            asset_group="tradfi",
            catalog=[],
            date_axis=_date_axis(start, end),
            data_types=data_types_in_v1,
        )
    )
    v2_cells = _venue_day_dt_cells(v2_rows)

    missing = v1_cells - v2_cells
    assert not missing, (
        f"v2 tradfi missing {len(missing)} venue-grain non-trading-day cells covered by v1 "
        f"(sample: {sorted(missing)[:5]})"
    )


# ---------------------------------------------------------------------------
# Cross-check: v2 reasons are richer (more granular) than v1 for matched cells
# ---------------------------------------------------------------------------


def test_v2_cefi_emits_at_least_one_row_per_catalog_instrument_in_pre_launch() -> None:
    """Sanity: v2 per-instrument expansion materially exceeds v1's venue-grain output."""
    venue = "BINANCE"
    launch_date = enumerator_module.CEFI_VENUE_LAUNCH_DATES.get(venue)
    if launch_date is None:
        pytest.skip("BINANCE missing from CEFI_VENUE_LAUNCH_DATES")
    end_ts = pd.Timestamp(launch_date) - pd.Timedelta(days=1)
    catalog = [
        InstrumentCatalogEntry(
            instrument_id=f"INSTR-{i}",
            instrument_type="SPOT",
            venue=venue,
            chain="",
            league_id="",
            available_from=launch_date,
            available_to=None,
            market_created_at=None,
            settlement_time=None,
        )
        for i in range(5)
    ]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=[end_ts.date()],
            data_types=["ohlcv_1d"],
        )
    )
    # 5 instruments x 1 day x 1 data_type = 5 rows (one per instrument).
    assert len(rows) == 5
    assert {r.instrument_id for r in rows} == {f"INSTR-{i}" for i in range(5)}
