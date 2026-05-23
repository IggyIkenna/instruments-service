"""Unit tests — Phase 1.C enumerate_expected_universe.py v2 per-instrument enumerators.

Tests cover:
  - InstrumentCatalogEntry construction + _catalog_from_dataframe helper
  - Per-asset-group v2 enumerators: cefi / defi / tradfi / sports / prediction
  - enumerate_v2() public API (dispatch + data_types fallback)
  - EMPTY_CONFIRMED_REASONS closed-set compliance
  - Edge cases: empty catalog, no lifecycle bounds, delisted instrument,
    venue-launch beats instrument lifecycle (cefi), chain-genesis beats
    instrument lifecycle (defi), prediction market_created_at preference

Plan: expected_universe_v2_design_2026_05_08.md Phase 1.C [TEST] P0
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from unified_api_contracts.canonical.crosscutting.honest_coverage import (
    EMPTY_CONFIRMED_REASONS,
)

# ---------------------------------------------------------------------------
# Module loader (mirrors the v1 test pattern)
# ---------------------------------------------------------------------------


def _load_enumerator_module() -> ModuleType:
    """Load the enumerator script as a module by path (script lives outside the package)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_v2_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
InstrumentCatalogEntry = enumerator_module.InstrumentCatalogEntry
ExpectedRow = enumerator_module.ExpectedRow


# ---------------------------------------------------------------------------
# Fixtures: catalog helpers
# ---------------------------------------------------------------------------


def _make_cefi_entry(
    instrument_id: str = "BTC-USDT",
    instrument_type: str = "SPOT",
    venue: str = "BINANCE",
    available_from: str | None = "2019-01-01",
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=None,
        settlement_time=None,
    )


def _make_defi_entry(
    instrument_id: str = "ETH-USDC",
    instrument_type: str = "SPOT",
    venue: str = "AAVE_V3",
    chain: str = "ARBITRUM",
    available_from: str | None = "2022-01-01",
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain=chain,
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=None,
        settlement_time=None,
    )


def _make_tradfi_entry(
    instrument_id: str = "SPY",
    instrument_type: str = "ETF",
    venue: str = "NASDAQ",
    available_from: str | None = "2020-01-01",
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=None,
        settlement_time=None,
    )


def _make_sports_entry(
    instrument_id: str = "FIX-1234",
    instrument_type: str = "FIXTURE",
    venue: str = "api_football",
    league_id: str = "PL",
    available_from: str | None = "2024-01-10",
    available_to: str | None = "2024-01-15",
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id=league_id,
        available_from=available_from,
        available_to=available_to,
        market_created_at=None,
        settlement_time=None,
    )


def _make_prediction_entry(
    instrument_id: str = "MKT-999",
    instrument_type: str = "BINARY",
    venue: str = "POLYMARKET",
    market_created_at: str | None = "2024-03-01",
    settlement_time: str | None = "2024-03-31",
    available_from: str | None = None,
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=market_created_at,
        settlement_time=settlement_time,
    )


def _date_axis(*dates: str) -> list[date]:
    return [date.fromisoformat(d) for d in dates]


# ---------------------------------------------------------------------------
# InstrumentCatalogEntry + _catalog_from_dataframe tests
# ---------------------------------------------------------------------------


def test_catalog_entry_is_namedtuple() -> None:
    """InstrumentCatalogEntry must be a NamedTuple (immutable, iterable)."""
    entry = _make_cefi_entry()
    assert entry.instrument_id == "BTC-USDT"
    assert entry.chain == ""
    assert entry.available_from == "2019-01-01"
    assert entry.available_to is None


def test_catalog_from_dataframe_basic() -> None:
    """_catalog_from_dataframe must convert a DataFrame to InstrumentCatalogEntry list."""
    df = pd.DataFrame(
        [
            {
                "instrument_id": "BTC-USDT",
                "instrument_type": "SPOT",
                "venue": "BINANCE",
                "chain": "",
                "league_id": "",
                "available_from": "2019-01-01",
                "available_to": None,
                "market_created_at": None,
                "settlement_time": None,
            }
        ]
    )
    catalog = enumerator_module._catalog_from_dataframe(df)
    assert len(catalog) == 1
    assert catalog[0].instrument_id == "BTC-USDT"
    assert catalog[0].available_from == "2019-01-01"
    assert catalog[0].available_to is None


def test_catalog_from_dataframe_handles_nan() -> None:
    """NaN values in optional fields must become None / empty string."""
    import numpy as np

    df = pd.DataFrame(
        [
            {
                "instrument_id": "ETH-USD",
                "instrument_type": "SPOT",
                "venue": "KRAKEN",
                "chain": np.nan,
                "league_id": np.nan,
                "available_from": np.nan,
                "available_to": np.nan,
                "market_created_at": np.nan,
                "settlement_time": np.nan,
            }
        ]
    )
    catalog = enumerator_module._catalog_from_dataframe(df)
    assert len(catalog) == 1
    entry = catalog[0]
    assert entry.chain == ""  # _safe_str converts NaN → ""
    assert entry.available_from is None  # _opt_date converts NaN → None
    assert entry.available_to is None
    assert entry.market_created_at is None
    assert entry.settlement_time is None


def test_catalog_from_dataframe_empty() -> None:
    """Empty DataFrame → empty catalog list."""
    df = pd.DataFrame(
        columns=[
            "instrument_id",
            "instrument_type",
            "venue",
            "chain",
            "league_id",
            "available_from",
            "available_to",
            "market_created_at",
            "settlement_time",
        ]
    )
    catalog = enumerator_module._catalog_from_dataframe(df)
    assert catalog == []


# ---------------------------------------------------------------------------
# CeFi v2 enumerator tests
# ---------------------------------------------------------------------------


def test_cefi_v2_pre_listing_yields_not_listed() -> None:
    """Date before available_from → EXPECTED_INSTRUMENT_NOT_LISTED.

    Window includes an alive date so the overlap filter does not skip the instrument;
    the pre-listing date still yields exactly one NOT_LISTED row.
    """
    catalog = [_make_cefi_entry(available_from="2021-01-01", available_to=None, venue="BINANCE")]
    dates = _date_axis("2020-06-01", "2021-06-01")  # 2021-06-01 is alive → no row; 2020-06-01 → NOT_LISTED
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert rows[0].asset_group == "cefi"
    assert rows[0].date == "2020-06-01"
    assert rows[0].instrument_id == "BTC-USDT"


def test_cefi_v2_post_delisting_yields_delisted() -> None:
    """Date after available_to → EXPECTED_INSTRUMENT_DELISTED.

    Window includes an alive date so the overlap filter does not skip the instrument;
    only the post-delisting date yields a row.
    """
    catalog = [_make_cefi_entry(available_from="2019-01-01", available_to="2022-12-31", venue="BINANCE")]
    dates = _date_axis("2022-06-01", "2023-06-01")  # 2022-06-01 alive → no row; 2023-06-01 → DELISTED
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"
    assert rows[0].instrument_id == "BTC-USDT"


def test_cefi_v2_live_instrument_skipped() -> None:
    """Date within [available_from, available_to] → no row emitted."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", available_to=None, venue="BINANCE")]
    dates = _date_axis("2023-06-01")
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"]))
    assert rows == []


def test_cefi_v2_pre_venue_launch_beats_instrument_lifecycle() -> None:
    """EXPECTED_PRE_VENUE_LAUNCH must fire BEFORE per-instrument lifecycle rules.

    LIGHTER-ZKSYNC launched 2024-09-01. Even if an instrument has
    available_from=2024-01-01, dates before venue launch must yield
    EXPECTED_PRE_VENUE_LAUNCH — not EXPECTED_INSTRUMENT_NOT_LISTED.
    """
    catalog = [
        _make_cefi_entry(
            instrument_id="ETH-USDT",
            venue="LIGHTER-ZKSYNC",
            available_from="2024-01-01",
            available_to=None,
        )
    ]
    # 2024-08-01 is before venue launch (2024-09-01) AND after available_from (2024-01-01)
    dates = _date_axis("2024-08-01")
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_PRE_VENUE_LAUNCH", "venue-launch date must override instrument lifecycle"


def test_cefi_v2_empty_catalog() -> None:
    """Empty catalog → no rows."""
    rows = list(enumerator_module._enumerate_v2_cefi([], _date_axis("2024-01-01"), ["ohlcv_1d"]))
    assert rows == []


def test_cefi_v2_multiple_data_types() -> None:
    """One absent instrument x N data_types should produce N rows.

    Window includes an alive date so the overlap filter does not skip the instrument.
    """
    catalog = [_make_cefi_entry(available_from="2025-01-01", venue="BINANCE")]
    dates = _date_axis("2020-01-01", "2025-06-01")  # 2025-06-01 alive → no row; 2020-01-01 → N NOT_LISTED rows
    data_types = ["ohlcv_1d", "ohlcv_1h", "book_snapshot_5"]
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, dates, data_types))
    assert len(rows) == len(data_types)
    assert {r.data_type for r in rows} == set(data_types)


# ---------------------------------------------------------------------------
# DeFi v2 enumerator tests
# ---------------------------------------------------------------------------


def test_defi_v2_pre_chain_genesis_yields_pre_genesis() -> None:
    """Date before chain genesis → EXPECTED_PRE_GENESIS_CHAIN.

    ARBITRUM genesis is 2021-08-31; 2020-01-01 should fire pre-genesis.
    Window includes an alive date so the overlap filter does not skip the instrument.
    """
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    dates = _date_axis("2020-01-01", "2022-06-01")  # 2022-06-01 alive → no row; 2020-01-01 → PRE_GENESIS
    rows = list(enumerator_module._enumerate_v2_defi(catalog, dates, ["lending_indices"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_PRE_GENESIS_CHAIN"
    assert rows[0].chain == "ARBITRUM"


def test_defi_v2_chain_genesis_beats_available_from() -> None:
    """Chain genesis takes priority over available_from.

    available_from=2022-01-01 but chain genesis=2021-08-31;
    a date between genesis and available_from (e.g. 2021-09-01) must emit
    EXPECTED_INSTRUMENT_NOT_LISTED — not EXPECTED_PRE_GENESIS_CHAIN.
    A date before genesis must emit EXPECTED_PRE_GENESIS_CHAIN.

    Each window includes an alive date so the overlap filter passes.
    """
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    # Date before chain genesis → pre-genesis; 2022-06-01 is alive → no row
    pre_genesis_rows = list(
        enumerator_module._enumerate_v2_defi(catalog, _date_axis("2020-01-01", "2022-06-01"), ["lending_indices"])
    )
    assert pre_genesis_rows[0].reason == "EXPECTED_PRE_GENESIS_CHAIN"

    # Date after chain genesis but before available_from → not_listed; 2022-06-01 is alive → no row
    not_listed_rows = list(
        enumerator_module._enumerate_v2_defi(catalog, _date_axis("2021-09-01", "2022-06-01"), ["lending_indices"])
    )
    assert not_listed_rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"


def test_defi_v2_delisted_instrument() -> None:
    """Date after available_to → EXPECTED_INSTRUMENT_DELISTED.

    Window includes an alive date so the overlap filter does not skip the instrument.
    """
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01", available_to="2023-06-30")]
    # 2023-01-01 is alive → no row; 2024-01-01 → DELISTED
    rows = list(
        enumerator_module._enumerate_v2_defi(catalog, _date_axis("2023-01-01", "2024-01-01"), ["lending_indices"])
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_defi_v2_empty_catalog() -> None:
    rows = list(enumerator_module._enumerate_v2_defi([], _date_axis("2024-01-01"), ["lending_indices"]))
    assert rows == []


# ---------------------------------------------------------------------------
# TradFi v2 enumerator tests
# ---------------------------------------------------------------------------


def test_tradfi_v2_pre_listing_yields_not_listed() -> None:
    # Window includes an alive date so the overlap filter does not skip the instrument.
    catalog = [_make_tradfi_entry(available_from="2022-01-01")]
    rows = list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2021-01-01", "2022-06-01"), ["ohlcv_1d"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert rows[0].asset_group == "tradfi"
    assert rows[0].chain == ""


def test_tradfi_v2_delisted_instrument() -> None:
    # Window includes an alive date so the overlap filter does not skip the instrument.
    catalog = [_make_tradfi_entry(available_from="2020-01-01", available_to="2021-06-30")]
    # 2021-01-01 is alive → no row; 2022-01-01 → DELISTED
    rows = list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2021-01-01", "2022-01-01"), ["ohlcv_1d"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_tradfi_v2_no_bounds_skips_all_dates() -> None:
    """Instrument with no available_from/to → no rows (always alive)."""
    catalog = [_make_tradfi_entry(available_from=None, available_to=None)]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2020-01-01", "2020-06-01", "2025-01-01"),
            ["ohlcv_1d"],
        )
    )
    assert rows == []


def test_tradfi_v2_empty_catalog() -> None:
    rows = list(enumerator_module._enumerate_v2_tradfi([], _date_axis("2024-01-01"), ["ohlcv_1d"]))
    assert rows == []


# ---------------------------------------------------------------------------
# Sports v2 enumerator tests
# ---------------------------------------------------------------------------


def test_sports_v2_pre_fixture_start_yields_not_listed() -> None:
    """Date before fixture available_from → EXPECTED_INSTRUMENT_NOT_LISTED.

    Window includes an alive date (2024-01-12) so the overlap filter passes.
    """
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to="2024-01-15")]
    rows = list(enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-01-05", "2024-01-12"), ["lineups"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert rows[0].league_id == "PL"
    assert rows[0].asset_group == "sports"


def test_sports_v2_post_fixture_end_yields_delisted() -> None:
    """Date after fixture available_to → EXPECTED_INSTRUMENT_DELISTED.

    Window includes an alive date (2024-01-12) so the overlap filter passes.
    """
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to="2024-01-15")]
    # 2024-01-12 is alive → no row; 2024-01-20 → DELISTED
    rows = list(enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-01-12", "2024-01-20"), ["lineups"]))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_sports_v2_league_id_propagated_to_row() -> None:
    """league_id from catalog must appear in every yielded row.

    Window includes an alive date so the overlap filter passes.
    available_to=None so the instrument has no end date and 2024-07-01 is alive.
    """
    catalog = [_make_sports_entry(league_id="LA_LIGA", available_from="2024-06-01", available_to=None)]
    # 2024-07-01 alive → no row; 2024-01-01 → NOT_LISTED with league_id
    rows = list(enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-01-01", "2024-07-01"), ["lineups"]))
    assert len(rows) == 1
    assert rows[0].league_id == "LA_LIGA"


def test_sports_v2_empty_catalog() -> None:
    rows = list(enumerator_module._enumerate_v2_sports([], _date_axis("2024-01-01"), ["lineups"]))
    assert rows == []


# ---------------------------------------------------------------------------
# Prediction v2 enumerator tests
# ---------------------------------------------------------------------------


def test_prediction_v2_market_created_at_prefers_over_available_from() -> None:
    """market_created_at takes precedence over available_from.

    market_created_at=2024-03-01 means date 2024-02-15 is before creation
    → EXPECTED_INSTRUMENT_NOT_LISTED.

    Window includes an alive date (2024-03-15) so the overlap filter passes.
    """
    catalog = [
        _make_prediction_entry(
            market_created_at="2024-03-01",
            settlement_time="2024-03-31",
            available_from="2024-01-01",  # earlier than market_created_at
        )
    ]
    # 2024-03-15 alive → no row; 2024-02-15 → NOT_LISTED
    rows = list(
        enumerator_module._enumerate_v2_prediction(catalog, _date_axis("2024-02-15", "2024-03-15"), ["prediction_clob"])
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"


def test_prediction_v2_settlement_time_prefers_over_available_to() -> None:
    """settlement_time takes precedence over available_to.

    settlement_time=2024-03-31 means date 2024-04-01 is after settlement
    → EXPECTED_INSTRUMENT_DELISTED.

    Window includes an alive date (2024-03-15) so the overlap filter passes.
    """
    catalog = [
        _make_prediction_entry(
            market_created_at="2024-03-01",
            settlement_time="2024-03-31",
            available_to="2025-12-31",  # later than settlement_time
        )
    ]
    # 2024-03-15 alive → no row; 2024-04-01 → DELISTED
    rows = list(
        enumerator_module._enumerate_v2_prediction(catalog, _date_axis("2024-03-15", "2024-04-01"), ["prediction_clob"])
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_prediction_v2_falls_back_to_available_from_when_no_market_dates() -> None:
    """When market_created_at is None, falls back to available_from.

    Each window includes an alive date so the overlap filter passes.
    """
    catalog = [
        _make_prediction_entry(
            market_created_at=None,
            settlement_time=None,
            available_from="2024-06-01",
            available_to="2024-09-30",
        )
    ]
    # Before available_from → not listed; 2024-07-01 alive → no row
    pre_rows = list(
        enumerator_module._enumerate_v2_prediction(catalog, _date_axis("2024-01-01", "2024-07-01"), ["prediction_clob"])
    )
    assert pre_rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"

    # After available_to → delisted; 2024-07-01 alive → no row
    post_rows = list(
        enumerator_module._enumerate_v2_prediction(catalog, _date_axis("2024-07-01", "2024-10-01"), ["prediction_clob"])
    )
    assert post_rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_prediction_v2_empty_catalog() -> None:
    rows = list(enumerator_module._enumerate_v2_prediction([], _date_axis("2024-01-01"), ["prediction_clob"]))
    assert rows == []


# ---------------------------------------------------------------------------
# enumerate_v2 public API tests
# ---------------------------------------------------------------------------


def test_enumerate_v2_dispatch_cefi() -> None:
    """enumerate_v2 routes cefi to _enumerate_v2_cefi.

    Window includes an alive date so the overlap filter passes.
    """
    catalog = [_make_cefi_entry(available_from="2025-01-01", venue="BINANCE")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=_date_axis("2020-01-01", "2025-06-01"),  # 2025-06-01 alive → no row; 2020-01-01 → NOT_LISTED
            data_types=["ohlcv_1d"],
        )
    )
    assert len(rows) == 1
    assert rows[0].asset_group == "cefi"


def test_enumerate_v2_invalid_asset_group_raises() -> None:
    """enumerate_v2 with unsupported asset_group must raise ValueError."""
    with pytest.raises(ValueError, match="unsupported asset_group"):
        list(
            enumerator_module.enumerate_v2(
                asset_group="crypto",  # invalid
                catalog=[],
                date_axis=_date_axis("2024-01-01"),
                data_types=["ohlcv_1d"],
            )
        )


def test_enumerate_v2_empty_catalog_returns_no_rows() -> None:
    """Empty catalog → no rows regardless of asset_group."""
    for ag in ("cefi", "defi", "tradfi", "sports", "prediction"):
        rows = list(
            enumerator_module.enumerate_v2(
                asset_group=ag,
                catalog=[],
                date_axis=_date_axis("2024-01-01"),
                data_types=["ohlcv_1d"],
            )
        )
        assert rows == [], f"expected no rows for empty catalog, got {rows} for {ag}"


# ---------------------------------------------------------------------------
# Closed-set compliance: all v2 reasons must be in EMPTY_CONFIRMED_REASONS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "asset_group,catalog,dates",
    [
        (
            "cefi",
            [_make_cefi_entry(available_from="2025-01-01", venue="BINANCE")],
            _date_axis("2020-01-01", "2025-06-01"),  # 2025-06-01 alive → no row; 2020-01-01 → NOT_LISTED
        ),
        (
            "cefi",
            [_make_cefi_entry(available_from="2019-01-01", available_to="2020-01-01", venue="BINANCE")],
            _date_axis("2019-06-01", "2023-01-01"),  # 2019-06-01 alive → no row; 2023-01-01 → DELISTED
        ),
        (
            "cefi",
            # LIGHTER-ZKSYNC: pre-venue-launch; available_from=2024-01-01 ≤ 2024-08-01 so no overlap filter needed
            [_make_cefi_entry(venue="LIGHTER-ZKSYNC", available_from="2024-01-01")],
            _date_axis("2024-08-01"),
        ),
        (
            "defi",
            [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")],
            _date_axis("2020-01-01", "2022-06-01"),  # 2022-06-01 alive → no row; 2020-01-01 → PRE_GENESIS
        ),
        (
            "defi",
            [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01", available_to="2023-06-30")],
            _date_axis("2023-01-01", "2024-01-01"),  # 2023-01-01 alive → no row; 2024-01-01 → DELISTED
        ),
        (
            "tradfi",
            [_make_tradfi_entry(available_from="2025-01-01")],
            _date_axis("2020-01-01", "2025-06-01"),  # 2025-06-01 alive → no row; 2020-01-01 → NOT_LISTED
        ),
        (
            "tradfi",
            [_make_tradfi_entry(available_from="2020-01-01", available_to="2021-01-01")],
            _date_axis("2020-06-01", "2024-01-01"),  # 2020-06-01 alive → no row; 2024-01-01 → DELISTED
        ),
        (
            "sports",
            [_make_sports_entry(available_from="2024-06-01", available_to=None)],  # no end → 2024-07-01 alive
            _date_axis("2024-01-01", "2024-07-01"),  # 2024-07-01 alive → no row; 2024-01-01 → NOT_LISTED
        ),
        (
            "sports",
            [_make_sports_entry(available_from="2024-01-01", available_to="2024-03-01")],
            _date_axis("2024-01-15", "2024-06-01"),  # 2024-01-15 alive → no row; 2024-06-01 → DELISTED
        ),
        (
            "prediction",
            [_make_prediction_entry(market_created_at="2024-06-01", settlement_time="2024-09-01")],
            _date_axis("2024-01-01", "2024-07-01"),  # 2024-07-01 alive → no row; 2024-01-01 → NOT_LISTED
        ),
        (
            "prediction",
            [_make_prediction_entry(market_created_at="2024-01-01", settlement_time="2024-03-01")],
            _date_axis("2024-02-01", "2024-06-01"),  # 2024-02-01 alive → no row; 2024-06-01 → DELISTED
        ),
    ],
)
def test_v2_all_reasons_in_closed_set(
    asset_group: str,
    catalog: list,
    dates: list[date],
) -> None:
    """Every reason yielded by v2 enumerators MUST be in EMPTY_CONFIRMED_REASONS."""
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group=asset_group,
            catalog=catalog,
            date_axis=dates,
            data_types=["ohlcv_1d"],
        )
    )
    assert len(rows) > 0, f"expected ≥1 row for asset_group={asset_group}"
    for r in rows:
        assert r.reason in EMPTY_CONFIRMED_REASONS, (
            f"v2 reason {r.reason!r} not in EMPTY_CONFIRMED_REASONS (asset_group={asset_group}, date={r.date})"
        )


# ---------------------------------------------------------------------------
# _V2_ENUMERATORS dispatch table completeness
# ---------------------------------------------------------------------------


def test_v2_enumerators_dict_covers_all_5_asset_groups() -> None:
    """_V2_ENUMERATORS must cover all 5 asset_groups."""
    assert set(enumerator_module._V2_ENUMERATORS.keys()) == {
        "cefi",
        "defi",
        "tradfi",
        "sports",
        "prediction",
    }


# ---------------------------------------------------------------------------
# Wave 3 — expected_unattempted rows (present_set-aware mode)
# ---------------------------------------------------------------------------

_DEFAULT_COLS = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]


def _row_key_from_dict(d: dict[str, str]) -> tuple[str, ...]:
    return tuple(d.get(c, "") for c in _DEFAULT_COLS)


def test_cefi_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """Alive instrument date absent from manifest → expected_unattempted row."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", available_to=None, venue="BINANCE")]
    date_axis = _date_axis("2023-06-01")
    present_set: set[tuple[str, ...]] = set()  # empty — no manifest rows
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set=present_set))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.date == "2023-06-01"
    assert r.data_type == "ohlcv_1d"
    assert r.asset_group == "cefi"


def test_cefi_v2_alive_date_in_present_set_skipped() -> None:
    """Alive instrument date already in manifest → no row emitted."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE")]
    date_axis = _date_axis("2023-06-01")
    key = _row_key_from_dict(
        {
            "venue": "BINANCE",
            "chain": "",
            "data_type": "ohlcv_1d",
            "instrument_type": "SPOT",
            "instrument_id": "BTC-USDT",
            "league_id": "",
            "date": "2023-06-01",
        }
    )
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set={key}))
    assert rows == []


def test_cefi_v2_legacy_mode_alive_date_skipped() -> None:
    """present_set=None (legacy mode) → alive dates produce no rows."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE")]
    date_axis = _date_axis("2023-06-01")
    rows = list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set=None))
    assert rows == []


def test_defi_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """DeFi alive instrument date absent from manifest → expected_unattempted."""
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    date_axis = _date_axis("2024-06-01")
    rows = list(enumerator_module._enumerate_v2_defi(catalog, date_axis, ["lending_indices"], present_set=set()))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.chain == "ARBITRUM"
    assert r.asset_group == "defi"


def test_defi_v2_alive_date_in_present_set_skipped() -> None:
    """DeFi alive date already in manifest → no row emitted."""
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    date_axis = _date_axis("2024-06-01")
    key = _row_key_from_dict(
        {
            "venue": "AAVE_V3",
            "chain": "ARBITRUM",
            "data_type": "lending_indices",
            "instrument_type": "SPOT",
            "instrument_id": "ETH-USDC",
            "league_id": "",
            "date": "2024-06-01",
        }
    )
    rows = list(enumerator_module._enumerate_v2_defi(catalog, date_axis, ["lending_indices"], present_set={key}))
    assert rows == []


def test_defi_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    rows = list(
        enumerator_module._enumerate_v2_defi(catalog, _date_axis("2024-06-01"), ["lending_indices"], present_set=None)
    )
    assert rows == []


def test_tradfi_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """TradFi alive instrument date absent from manifest → expected_unattempted."""
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    date_axis = _date_axis("2024-06-01")
    rows = list(enumerator_module._enumerate_v2_tradfi(catalog, date_axis, ["ohlcv_1m"], present_set=set()))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.chain == ""
    assert r.asset_group == "tradfi"


def test_tradfi_v2_alive_date_in_present_set_skipped() -> None:
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    date_axis = _date_axis("2024-06-01")
    key = _row_key_from_dict(
        {
            "venue": "NASDAQ",
            "chain": "",
            "data_type": "ohlcv_1m",
            "instrument_type": "ETF",
            "instrument_id": "SPY",
            "league_id": "",
            "date": "2024-06-01",
        }
    )
    rows = list(enumerator_module._enumerate_v2_tradfi(catalog, date_axis, ["ohlcv_1m"], present_set={key}))
    assert rows == []


def test_tradfi_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2024-06-01"), ["ohlcv_1m"], present_set=None)
    )
    assert rows == []


def test_sports_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """Sports alive fixture absent from manifest → expected_unattempted (league_id propagated)."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None, league_id="PL")]
    date_axis = _date_axis("2024-01-12")
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["lineups"], present_set=set()))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.league_id == "PL"
    assert r.asset_group == "sports"


def test_sports_v2_alive_date_in_present_set_skipped() -> None:
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None, league_id="PL")]
    date_axis = _date_axis("2024-01-12")
    key = _row_key_from_dict(
        {
            "venue": "api_football",
            "chain": "",
            "data_type": "lineups",
            "instrument_type": "FIXTURE",
            "instrument_id": "FIX-1234",
            "league_id": "PL",
            "date": "2024-01-12",
        }
    )
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["lineups"], present_set={key}))
    assert rows == []


def test_sports_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None)]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-01-12"), ["lineups"], present_set=None)
    )
    assert rows == []


def test_prediction_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """Prediction active market absent from manifest → expected_unattempted."""
    catalog = [_make_prediction_entry(market_created_at="2024-03-01", settlement_time="2024-03-31")]
    date_axis = _date_axis("2024-03-15")
    rows = list(enumerator_module._enumerate_v2_prediction(catalog, date_axis, ["prediction_clob"], present_set=set()))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.asset_group == "prediction"


def test_prediction_v2_alive_date_in_present_set_skipped() -> None:
    catalog = [_make_prediction_entry(market_created_at="2024-03-01", settlement_time="2024-03-31")]
    date_axis = _date_axis("2024-03-15")
    key = _row_key_from_dict(
        {
            "venue": "POLYMARKET",
            "chain": "",
            "data_type": "prediction_clob",
            "instrument_type": "BINARY",
            "instrument_id": "MKT-999",
            "league_id": "",
            "date": "2024-03-15",
        }
    )
    rows = list(enumerator_module._enumerate_v2_prediction(catalog, date_axis, ["prediction_clob"], present_set={key}))
    assert rows == []


def test_prediction_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_prediction_entry(market_created_at="2024-03-01", settlement_time="2024-03-31")]
    rows = list(
        enumerator_module._enumerate_v2_prediction(
            catalog, _date_axis("2024-03-15"), ["prediction_clob"], present_set=None
        )
    )
    assert rows == []


def test_enumerate_v2_forwards_present_set_to_enumerator() -> None:
    """enumerate_v2() must forward present_set to the per-asset-group enumerator."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE")]
    present_set: set[tuple[str, ...]] = set()  # empty → expected_unattempted
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=_date_axis("2023-06-01"),
            data_types=["ohlcv_1d"],
            present_set=present_set,
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"


def test_enumerate_v2_with_present_set_none_skips_alive_dates() -> None:
    """enumerate_v2() with present_set=None must skip alive dates (legacy mode)."""
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="tradfi",
            catalog=catalog,
            date_axis=_date_axis("2024-06-01"),
            data_types=["ohlcv_1m"],
            present_set=None,
        )
    )
    assert rows == []


def test_expected_unattempted_rows_have_empty_reason() -> None:
    """expected_unattempted rows must always have reason='' (not a typed reason)."""
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="defi",
            catalog=catalog,
            date_axis=_date_axis("2024-06-01"),
            data_types=["lending_indices"],
            present_set=set(),
        )
    )
    assert all(r.reason == "" for r in rows if r.capture_status == "expected_unattempted")


def test_empty_confirmed_rows_still_have_typed_reason_when_present_set_given() -> None:
    """Even when present_set is provided, lifecycle-boundary rows still emit typed reason.

    Window includes an alive date so the overlap filter passes.
    """
    catalog = [_make_cefi_entry(available_from="2025-01-01", venue="BINANCE")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=_date_axis(
                "2020-01-01", "2025-06-01"
            ),  # 2025-06-01 alive → expected_unattempted; 2020-01-01 → NOT_LISTED
            data_types=["ohlcv_1d"],
            present_set=set(),  # providing present_set shouldn't affect lifecycle boundary rows
        )
    )
    not_listed = [r for r in rows if r.capture_status == "empty_confirmed"]
    assert len(not_listed) == 1
    assert not_listed[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
