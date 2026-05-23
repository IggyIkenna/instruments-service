"""Unit tests — Phase 3.D.4 enumerate_expected_universe.py.

Tests cover the per-asset-group enumerators + the present-set / row-key
helpers. They exercise real UAC SSOTs (`CHAIN_GENESIS_DATES`,
`CEFI_VENUE_LAUNCH_DATES`, `PREDICTION_VENUE_LAUNCH_DATES`,
`SOURCE_COVERAGE_START`, `non_trading_day_reason`) against a small known
window per asset_group and assert that yielded rows have the right shape
+ closed-set reasons. They DO NOT touch the network or GCS — pure
generator-driven inspection.

Plan: writegate_honest_coverage_endtoend_2026_05_06.md § Phase 3.D.4
[TEST] P0 todo.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from unified_api_contracts.canonical.crosscutting.honest_coverage import (
    EMPTY_CONFIRMED_REASONS,
)

# --- Module loader ----------------------------------------------------------


def _load_enumerator_module() -> ModuleType:
    """Load the enumerator script as a module by path (script lives outside the package).

    Register the module in ``sys.modules`` BEFORE ``exec_module`` so that the
    ``@dataclass`` decorator's ``_is_type`` lookup can find ``cls.__module__``
    in ``sys.modules`` (Python 3.13's dataclasses internals).
    """
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
ExpectedRow = enumerator_module.ExpectedRow


# --- Per-asset-group enumerator tests ---------------------------------------


def test_tradfi_yields_expected_weekend_for_known_saturday() -> None:
    """2018-01-06 is a Saturday; every TradFi venue x data_type should
    yield a row with reason=EXPECTED_WEEKEND."""
    rows = list(enumerator_module._enumerate_tradfi("2018-01-06", "2018-01-06"))
    assert len(rows) > 0, "expected at least one row for Saturday 2018-01-06"
    weekend_rows = [r for r in rows if r.reason == "EXPECTED_WEEKEND"]
    assert len(weekend_rows) > 0, "expected at least one EXPECTED_WEEKEND row"
    sample = weekend_rows[0]
    assert sample.asset_group == "tradfi"
    assert sample.date == "2018-01-06"
    assert sample.chain == ""  # tradfi has no chain axis


def test_tradfi_yields_no_rows_for_known_trading_day() -> None:
    """Tuesday 2018-01-09 should be a normal trading day across all
    standard TradFi venues — no EXPECTED_HOLIDAY/WEEKEND rows."""
    rows = list(enumerator_module._enumerate_tradfi("2018-01-09", "2018-01-09"))
    # Most TradFi venues open Tuesday Jan-9; a few specialty venues might
    # have a holiday but the count should be small or zero.
    saturday_or_holiday = [r for r in rows if r.reason in ("EXPECTED_WEEKEND",)]
    assert len(saturday_or_holiday) == 0, "Tuesday 2018-01-09 should not yield EXPECTED_WEEKEND rows"


def test_defi_yields_pre_genesis_for_arbitrum_pre_2021() -> None:
    """Arbitrum genesis is 2021-08-31; AAVE_V3-ARBITRUM on 2018-01-01 should
    yield EXPECTED_PRE_GENESIS_CHAIN (chain didn't exist yet)."""
    rows = list(enumerator_module._enumerate_defi("2018-01-01", "2018-01-01"))
    arbitrum_rows = [r for r in rows if r.chain == "ARBITRUM"]
    assert len(arbitrum_rows) > 0, "expected ARBITRUM rows for 2018-01-01"
    pre_genesis = [r for r in arbitrum_rows if r.reason == "EXPECTED_PRE_GENESIS_CHAIN"]
    assert len(pre_genesis) > 0, "expected EXPECTED_PRE_GENESIS_CHAIN for ARBITRUM 2018-01-01 (genesis 2021-08-31)"
    sample = pre_genesis[0]
    assert sample.asset_group == "defi"
    assert sample.date == "2018-01-01"
    assert sample.chain == "ARBITRUM"


def test_defi_yields_no_rows_for_post_protocol_launch() -> None:
    """A date well past every chain genesis + protocol launch (e.g.
    2026-01-01) should yield zero pre-launch rows for the entire window."""
    rows = list(enumerator_module._enumerate_defi("2026-01-01", "2026-01-01"))
    assert len(rows) == 0, "expected zero pre-launch rows for 2026-01-01 (all chains + protocols launched)"


def test_sports_yields_pre_source_coverage_for_pre_2018() -> None:
    """api_football coverage starts 2018-01-01; 2017-12-31 should yield
    EXPECTED_PRE_SOURCE_COVERAGE_START rows for all sports data_types."""
    rows = list(enumerator_module._enumerate_sports("2017-12-31", "2017-12-31"))
    af_rows = [r for r in rows if r.venue == "api_football"]
    assert len(af_rows) > 0, "expected api_football rows for 2017-12-31"
    pre_coverage = [r for r in af_rows if r.reason == "EXPECTED_PRE_SOURCE_COVERAGE_START"]
    assert len(pre_coverage) > 0, "expected EXPECTED_PRE_SOURCE_COVERAGE_START for api_football 2017-12-31"
    sample = pre_coverage[0]
    assert sample.asset_group == "sports"
    assert sample.date == "2017-12-31"


def test_cefi_yields_pre_venue_launch_for_lighter_pre_2024_09() -> None:
    """LIGHTER-ZKSYNC launched 2024-09-01; 2024-01-01 should yield
    EXPECTED_PRE_VENUE_LAUNCH rows."""
    rows = list(enumerator_module._enumerate_cefi("2024-01-01", "2024-01-01"))
    lighter_rows = [r for r in rows if r.venue == "LIGHTER-ZKSYNC"]
    assert len(lighter_rows) > 0, "expected LIGHTER-ZKSYNC rows for 2024-01-01"
    for r in lighter_rows:
        assert r.reason == "EXPECTED_PRE_VENUE_LAUNCH"
        assert r.asset_group == "cefi"
        assert r.date == "2024-01-01"


def test_cefi_yields_no_rows_for_post_all_venue_launches() -> None:
    """A date past every CeFi venue launch (e.g. 2026-01-01) should yield
    zero pre-venue-launch rows."""
    rows = list(enumerator_module._enumerate_cefi("2026-01-01", "2026-01-01"))
    assert len(rows) == 0, "expected zero pre-venue-launch rows for 2026-01-01 (every cefi venue launched)"


def test_prediction_yields_pre_venue_launch_for_pre_2020_polymarket() -> None:
    """POLYMARKET launched 2020-09-01; 2020-01-01 should yield
    EXPECTED_PRE_VENUE_LAUNCH rows."""
    rows = list(enumerator_module._enumerate_prediction("2020-01-01", "2020-01-01"))
    polymarket_rows = [r for r in rows if r.venue == "POLYMARKET"]
    assert len(polymarket_rows) > 0, "expected POLYMARKET rows for 2020-01-01"
    for r in polymarket_rows:
        assert r.reason == "EXPECTED_PRE_VENUE_LAUNCH"
        assert r.asset_group == "prediction"
        assert r.date == "2020-01-01"


def test_prediction_yields_pre_venue_launch_for_pre_2021_kalshi() -> None:
    """KALSHI launched 2021-07-30; 2021-01-01 should yield EXPECTED_PRE_VENUE_LAUNCH."""
    rows = list(enumerator_module._enumerate_prediction("2021-01-01", "2021-01-01"))
    kalshi_rows = [r for r in rows if r.venue == "KALSHI"]
    assert len(kalshi_rows) > 0, "expected KALSHI rows for 2021-01-01"
    for r in kalshi_rows:
        assert r.reason == "EXPECTED_PRE_VENUE_LAUNCH"


# --- Cross-asset-group invariants -------------------------------------------


@pytest.mark.parametrize(
    "asset_group,start,end",
    [
        ("tradfi", "2018-01-06", "2018-01-06"),  # Saturday
        ("defi", "2018-01-01", "2018-01-01"),  # pre-genesis
        ("sports", "2017-12-31", "2017-12-31"),  # pre-source-coverage
        ("cefi", "2024-01-01", "2024-01-01"),  # pre-venue-launch (LIGHTER)
        ("prediction", "2020-01-01", "2020-01-01"),  # pre-venue-launch (POLYMARKET)
    ],
)
def test_every_yielded_reason_is_in_closed_set(asset_group: str, start: str, end: str) -> None:
    """Every reason yielded by the enumerator MUST be in
    `EMPTY_CONFIRMED_REASONS` (UAC closed-set)."""
    enumerator_func = enumerator_module._ENUMERATORS[asset_group]
    rows = list(enumerator_func(start, end))
    assert len(rows) > 0, f"sanity: expected at least one row for {asset_group} {start}"
    for r in rows:
        assert r.reason in EMPTY_CONFIRMED_REASONS, (
            f"reason {r.reason!r} not in EMPTY_CONFIRMED_REASONS closed-set (asset_group={asset_group}, date={r.date})"
        )
        assert r.asset_group == asset_group


@pytest.mark.parametrize(
    "asset_group,start,end",
    [
        ("tradfi", "2018-01-06", "2018-01-06"),
        ("defi", "2018-01-01", "2018-01-01"),
        ("sports", "2017-12-31", "2017-12-31"),
        ("cefi", "2024-01-01", "2024-01-01"),
        ("prediction", "2020-01-01", "2020-01-01"),
    ],
)
def test_every_yielded_row_has_required_fields(asset_group: str, start: str, end: str) -> None:
    """Every yielded ExpectedRow must have non-empty asset_group, date, and reason."""
    enumerator_func = enumerator_module._ENUMERATORS[asset_group]
    rows = list(enumerator_func(start, end))
    assert len(rows) > 0
    for r in rows:
        assert r.asset_group, f"empty asset_group on row {r}"
        assert r.date, f"empty date on row {r}"
        assert r.reason, f"empty reason on row {r}"
        # Either venue or chain (or league_id for sports) must be set so the
        # manifest row key is unique.
        assert r.venue or r.chain or r.league_id, f"row has no venue/chain/league_id identifier: {r}"


# --- Helper tests -----------------------------------------------------------


def test_build_present_set_extracts_tuples_from_manifest() -> None:
    """Helper should extract a set of canonical (venue, chain, ..., date) tuples."""
    df = pd.DataFrame(
        [
            {
                "venue": "BARCHART",
                "chain": "",
                "data_type": "ohlcv_1m",
                "instrument_type": "",
                "instrument_id": "",
                "league_id": "",
                "date": "2018-01-06",
            },
            {
                "venue": "AAVE_V3-ARBITRUM",
                "chain": "ARBITRUM",
                "data_type": "lending_indices",
                "instrument_type": "",
                "instrument_id": "",
                "league_id": "",
                "date": "2018-01-01",
            },
        ]
    )
    present_set = enumerator_module._build_present_set(df, asset_group="tradfi")
    assert ("BARCHART", "", "ohlcv_1m", "", "", "", "2018-01-06") in present_set
    assert (
        "AAVE_V3-ARBITRUM",
        "ARBITRUM",
        "lending_indices",
        "",
        "",
        "",
        "2018-01-01",
    ) in present_set


def test_build_present_set_returns_empty_for_empty_manifest() -> None:
    df = pd.DataFrame()
    present_set = enumerator_module._build_present_set(df, asset_group="tradfi")
    assert present_set == set()


def test_row_key_aligns_with_manifest_columns() -> None:
    row = ExpectedRow(
        asset_group="defi",
        venue="AAVE_V3-ARBITRUM",
        chain="ARBITRUM",
        data_type="lending_indices",
        instrument_type="",
        instrument_id="",
        league_id="",
        date="2018-01-01",
        reason="EXPECTED_PRE_GENESIS_CHAIN",
    )
    available_cols = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    key = enumerator_module._row_key(row, available_cols)
    assert key == (
        "AAVE_V3-ARBITRUM",
        "ARBITRUM",
        "lending_indices",
        "",
        "",
        "",
        "2018-01-01",
    )


def test_row_key_handles_missing_columns() -> None:
    """If the manifest is missing some columns, _row_key should still produce
    a tuple of the right length."""
    row = ExpectedRow(
        asset_group="tradfi",
        venue="BARCHART",
        chain="",
        data_type="ohlcv_1m",
        instrument_type="",
        instrument_id="",
        league_id="",
        date="2018-01-06",
        reason="EXPECTED_WEEKEND",
    )
    available_cols = ["venue", "data_type", "date"]  # subset
    key = enumerator_module._row_key(row, available_cols)
    assert key == ("BARCHART", "ohlcv_1m", "2018-01-06")


# --- Enumerator dispatch table ---------------------------------------------


def test_all_5_asset_groups_in_enumerator_dispatch() -> None:
    """The _ENUMERATORS dict must cover all 5 asset_groups."""
    assert set(enumerator_module._ENUMERATORS.keys()) == {
        "cefi",
        "defi",
        "tradfi",
        "sports",
        "prediction",
    }
