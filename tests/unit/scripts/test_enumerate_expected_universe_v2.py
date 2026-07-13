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
from typing import ClassVar

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


def _drop_v2_venue_grain(rows: list) -> list:
    """Filter out the venue-grain pass from v2 enumerator output.

    The v2 enumerators for tradfi/cefi/defi/prediction each yield a venue-grain
    pass (via ``_yield_v2_*`` helpers) that mirrors the v1 legacy enumerators'
    (venue, data_type, day) sentinel rows: tradfi emits non-trading-day rows
    (``EXPECTED_WEEKEND`` / ``EXPECTED_HOLIDAY``); cefi/prediction emit
    ``EXPECTED_PRE_VENUE_LAUNCH`` rows; defi emits ``EXPECTED_PRE_GENESIS_CHAIN``
    + ``EXPECTED_INSTRUMENT_NOT_LISTED`` per-protocol rows and chain-level
    gas_fees pre-genesis rows. All venue-grain rows carry
    ``instrument_type=""`` / ``instrument_id=""`` so a fresh / empty catalogue
    still emits the v1-equivalent sentinel matrix.

    Per-instrument tests in this file assert per-instrument behavior against
    catalogs and don't want the venue-grain pass to leak in (the v1↔v2 parity
    was verified by ``tests/integration/test_enumerate_v2_superset_property.py``
    before v1 was retired 2026-07-09 per
    ``plans/active/issues/v1_enumerator_dispatch_not_deletable_2026_07_06.md``;
    that file's job was done once v1 was deleted, so it was removed too).
    """
    return [r for r in rows if r.instrument_id != "" or r.instrument_type != ""]


# ---------------------------------------------------------------------------
# Fixtures: catalog helpers
# ---------------------------------------------------------------------------


def _make_cefi_entry(
    instrument_id: str = "BTC-USDT",
    instrument_type: str = "PERPETUAL",
    venue: str = "BINANCE-FUTURES",
    available_from: str | None = "2019-01-01",
    available_to: str | None = None,
    base_asset: str = "BTC",
    mvp: bool | None = True,
) -> InstrumentCatalogEntry:
    # Default to a canonical, MVP-qualifying cefi instrument (BINANCE-FUTURES BTC
    # perp) so the lifecycle tests pass the MVP capture gate
    # (cefi_universe_capture_rule_2026_06_23). ``mvp=True`` short-circuits the gate
    # for these lifecycle-focused fixtures; the gate itself is exercised by the
    # dedicated gate tests below (with mvp=None / non-MVP fixtures).
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
        base_asset=base_asset,
        mvp=mvp,
    )


def _make_defi_entry(
    instrument_id: str = "ETH-USDC",
    instrument_type: str = "LENDING",  # lending_indices is the defi test data_type
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
    underlying: str = "",
    base_asset: str = "",
    mvp: bool | None = True,
) -> InstrumentCatalogEntry:
    # Default mvp=True so existing lifecycle tests pass the new MVP gate without
    # needing fixture changes.  Dedicated gate tests use mvp=False / mvp=None.
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
        underlying=underlying,
        base_asset=base_asset,
        mvp=mvp,
    )


def _make_sports_entry(
    instrument_id: str = "FIX-1234",
    # LEAGUE-grain, matching the real production catalogue (build_instrument_catalogue's
    # SPORTS_LEAGUE_INSTRUMENT_TYPE = "league") and the enumerator's own
    # _SPORTS_LEAGUE_GRAIN_INSTRUMENT_TYPE filter added 2026-07-09 alongside the
    # sports catalogue's new FIXTURE/TEAM/PLAYER-grain rows — _enumerate_v2_sports
    # now skips any non-"league" instrument_type, so every existing per-league
    # lifecycle test below relies on this default matching that filter.
    instrument_type: str = enumerator_module._SPORTS_LEAGUE_GRAIN_INSTRUMENT_TYPE,
    venue: str = "api_football",
    league_id: str = "EPL",
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
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"])))
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
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"])))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"
    assert rows[0].instrument_id == "BTC-USDT"


def test_cefi_v2_live_instrument_skipped() -> None:
    """Date within [available_from, available_to] → no per-instrument row emitted."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", available_to=None, venue="BINANCE")]
    dates = _date_axis("2023-06-01")
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"])))
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
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, ["ohlcv_1d"])))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_PRE_VENUE_LAUNCH", "venue-launch date must override instrument lifecycle"


def test_cefi_v2_empty_catalog() -> None:
    """Empty catalog → no per-instrument rows (venue-grain pre-venue-launch pass filtered out).

    Venue-grain PRE_VENUE_LAUNCH sentinel coverage (empty-catalog case) was
    verified against v1's output by
    ``tests/integration/test_enumerate_v2_superset_property.py`` before that
    file was retired alongside v1 (2026-07-09).
    """
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi([], _date_axis("2024-01-01"), ["ohlcv_1d"])))
    assert rows == []


def test_cefi_v2_multiple_data_types() -> None:
    """One absent instrument x N data_types should produce N per-instrument rows.

    Window includes an alive date so the overlap filter does not skip the instrument.
    """
    catalog = [_make_cefi_entry(available_from="2025-01-01", venue="BINANCE")]
    dates = _date_axis("2020-01-01", "2025-06-01")  # 2025-06-01 alive → no row; 2020-01-01 → N NOT_LISTED rows
    data_types = ["ohlcv_1d", "ohlcv_1h", "book_snapshot_5"]
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, data_types)))
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
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_defi(catalog, dates, ["lending_indices"])))
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
    pre_genesis_rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, _date_axis("2020-01-01", "2022-06-01"), ["lending_indices"]))
    )
    assert pre_genesis_rows[0].reason == "EXPECTED_PRE_GENESIS_CHAIN"

    # Date after chain genesis but before available_from → not_listed; 2022-06-01 is alive → no row
    not_listed_rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, _date_axis("2021-09-01", "2022-06-01"), ["lending_indices"]))
    )
    assert not_listed_rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"


def test_defi_v2_delisted_instrument() -> None:
    """Date after available_to → EXPECTED_INSTRUMENT_DELISTED.

    Window includes an alive date so the overlap filter does not skip the instrument.
    """
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01", available_to="2023-06-30")]
    # 2023-01-01 is alive → no row; 2024-01-01 → DELISTED
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, _date_axis("2023-01-01", "2024-01-01"), ["lending_indices"]))
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_defi_v2_pool_seeds_canonical_pool_address_id_and_lowercase_type() -> None:
    """POOL seed atoms MUST match the MTDS writer grain (canonical pool_address + lowercase type).

    Root cause (defi_instrument_catalogue_and_capture_pipeline_2026_06_23): the catalogue carries a
    POOL row's ``instrument_id`` as the glued ``VENUE-CHAIN:POOL:PAIR:fee`` ``instrument_key``
    composite + UPPERCASE ``POOL`` ``instrument_type``, while MTDS captures key on
    ``pool_address.lower()`` + lowercase ``pool``. The seeder re-keys from ``raw_symbol`` (the pool
    address) so the seeded cell reconciles against the captured cell instead of sitting DELISTED.
    """
    pool_addr = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    entry = _make_defi_entry(
        instrument_id="UNISWAP_V3-ARBITRUM:POOL:WETH-USDC:500",  # glued composite (catalogue form)
        instrument_type="POOL",  # uppercase leaf (catalogue form)
        venue="UNISWAP_V3",
        chain="ARBITRUM",
        available_from="2022-01-01",
        available_to="2023-06-30",
    )._replace(raw_symbol=pool_addr)
    # 2024-01-01 > available_to → a DELISTED seed row; assert its canonical atoms.
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi([entry], _date_axis("2023-01-01", "2024-01-01"), ["dex_pool_state"]))
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.reason == "EXPECTED_INSTRUMENT_DELISTED"
    assert row.instrument_id == pool_addr.lower()  # canonical pool_address.lower(), NOT the glued composite
    assert row.instrument_type == "pool"  # lowercase (matches the writer), NOT uppercase POOL
    assert row.venue == "UNISWAP_V3"  # bare protocol
    assert row.chain == "ARBITRUM"  # populated chain


def test_defi_v2_empty_catalog() -> None:
    """Empty catalog → no per-instrument rows (venue-grain pre-launch pass filtered out).

    Venue-grain PRE_GENESIS_CHAIN / INSTRUMENT_NOT_LISTED sentinel coverage
    (empty-catalog case) was verified against v1's output by
    ``tests/integration/test_enumerate_v2_superset_property.py`` before that
    file was retired alongside v1 (2026-07-09).
    """
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi([], _date_axis("2024-01-01"), ["lending_indices"]))
    )
    assert rows == []


# ---------------------------------------------------------------------------
# TradFi v2 enumerator tests
# ---------------------------------------------------------------------------


def test_tradfi_v2_pre_listing_yields_not_listed() -> None:
    # Window includes an alive date so the overlap filter does not skip the instrument.
    catalog = [_make_tradfi_entry(available_from="2022-01-01")]
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2021-01-01", "2022-06-01"), ["ohlcv_1d"]))
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert rows[0].asset_group == "tradfi"
    assert rows[0].chain == ""


def test_tradfi_v2_delisted_instrument() -> None:
    # Window includes an alive date so the overlap filter does not skip the instrument.
    catalog = [_make_tradfi_entry(available_from="2020-01-01", available_to="2021-06-30")]
    # 2021-01-01 is alive → no row; 2022-01-01 → DELISTED
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2021-01-01", "2022-01-01"), ["ohlcv_1d"]))
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_DELISTED"


def test_tradfi_v2_no_bounds_skips_all_dates() -> None:
    """Instrument with no available_from/to → no rows (always alive)."""
    catalog = [_make_tradfi_entry(available_from=None, available_to=None)]
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_tradfi(
                catalog,
                _date_axis("2020-01-01", "2020-06-01", "2025-01-01"),
                ["ohlcv_1d"],
            )
        )
    )
    assert rows == []


def test_tradfi_v2_empty_catalog() -> None:
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi([], _date_axis("2024-01-01"), ["ohlcv_1d"]))
    )
    assert rows == []


# ---------------------------------------------------------------------------
# Shard-grain SSOT: seeded instrument_type == the CANONICAL WRITER grain
# (lowercase ``future``/``equity``/``etf``), NOT the raw UPPERCASE catalogue leaf.
# Without this, the seeded ``expected_unattempted`` / ``empty_confirmed`` cell can
# never be converted by the real capture (which writes lowercase) → the cell sits
# permanently expected_unattempted and deflates honest-coverage. Same shard-grain
# mismatch class as the defi PROTOCOL-CHAIN bug (instruments-service@38cec01).
# ---------------------------------------------------------------------------


def test_tradfi_v2_future_seeds_canonical_lowercase_instrument_type() -> None:
    """A raw UPPERCASE ``FUTURE`` leaf at CME must seed ``instrument_type == 'futures_chain'``.

    Pre-listing (empty_confirmed) path: the seed grain MUST be the writer grain
    that MTDS captures at. CME/ICE outright futures are written at the per-underlying
    ``futures_chain`` bundle grain (``symbol_rules._VENUE_INSTRUMENT_TYPE["CME"]``),
    NOT the raw ``FUTURE`` nor the passthrough ``future`` (writer-grain alignment
    2026-06-22 — FUTURE_BUNDLE_VENUES["tradfi"]).
    """
    catalog = [
        _make_tradfi_entry(instrument_id="ES-2025H", instrument_type="FUTURE", venue="CME", available_from="2025-01-01")
    ]
    # Window spans the listing date so the lifecycle-overlap filter keeps the
    # instrument: 2024-06-01 → NOT_LISTED, 2025-06-01 → alive (no row in legacy mode).
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2024-06-01", "2025-06-01"), ["ohlcv_1m"]))
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert rows[0].instrument_type == "futures_chain", "CME future seed must use the writer's futures_chain grain"
    assert rows[0].instrument_type != "FUTURE"


def test_tradfi_v2_future_expected_unattempted_uses_writer_grain() -> None:
    """Alive CME ``FUTURE`` with no manifest row → expected_unattempted at ``futures_chain``.

    The expected_unattempted seed (present_set provided) must carry the writer
    grain (futures_chain for CME/ICE) so a later capture converts it.
    """
    catalog = [
        _make_tradfi_entry(instrument_id="ES-2025H", instrument_type="FUTURE", venue="CME", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2024-06-03"),  # alive
            ["ohlcv_1m"],
            present_set=set(),  # nothing captured yet → seed expected_unattempted
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].instrument_type == "futures_chain"


def test_tradfi_v2_capture_at_writer_grain_suppresses_seed() -> None:
    """A captured BUNDLE cell at the canonical writer grain must SUPPRESS the seed.

    Proves the row_key match is at the writer grain: the MTDS writer records a CME
    futures_chain capture with ``instrument_id=""`` + ``underlying=<U>``
    (venue_fetch.py:318-320 → manifest_finalize.py base_row_key). The present-set
    tuple therefore carries a BLANK instrument_id and the underlying in the
    ``underlying`` column; the seed mirrors that shape (axis-3, 2026-06-22), so the
    alive-and-captured cell skips seeding. Were the enumerator still keyed on the
    leaf instrument_id (``ES-2025H``) the present-set tuple would NOT match and the
    cell would be (wrongly) re-seeded.
    """
    cols = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "underlying", "league_id", "date"]
    # Writer shape for a futures_chain bundle: instrument_id="", underlying=<U>.
    present = {("CME", "", "ohlcv_1m", "futures_chain", "", "ES-2025H", "", "2024-06-03")}
    catalog = [
        _make_tradfi_entry(instrument_id="ES-2025H", instrument_type="FUTURE", venue="CME", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2024-06-03"),
            ["ohlcv_1m"],
            present_set=present,
            present_cols=cols,
        )
    )
    assert rows == [], "captured writer-grain bundle cell must suppress the seed (grain match)"


def test_tradfi_v2_bundle_seed_has_blank_instrument_id_and_underlying() -> None:
    """A bundle (futures_chain) seed mirrors the writer: instrument_id="" + underlying set.

    The un-captured CME future is seeded as a per-underlying futures_chain bundle
    cell; the seed MUST carry a BLANK instrument_id and the underlying populated so
    its shard atom equals the writer's captured-cell atom (axis-3, 2026-06-22).
    """
    catalog = [
        _make_tradfi_entry(instrument_id="ES-2025H", instrument_type="FUTURE", venue="CME", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2024-06-03"),
            ["ohlcv_1m"],
            present_set=set(),  # nothing captured → seed
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].instrument_type == "futures_chain"
    assert rows[0].instrument_id == "", "bundle seed must carry a BLANK instrument_id (writer grain)"
    assert rows[0].underlying == "ES-2025H", "bundle seed must carry the underlying"


def test_tradfi_v2_leaf_seed_keeps_instrument_id_blank_underlying() -> None:
    """A LEAF (equity) seed keeps its real instrument_id and a blank underlying.

    Confirms the bundle collapse is scoped to per-underlying bundle types and never
    touches leaf/per-instrument types — they still carry instrument_id=<id>,
    underlying="" (no regression).
    """
    catalog = [
        _make_tradfi_entry(instrument_id="AAPL", instrument_type="EQUITY", venue="NASDAQ", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2024-06-03"),
            ["ohlcv_1m"],
            present_set=set(),
        )
    )
    assert len(rows) == 1
    assert rows[0].instrument_type == "equity"
    assert rows[0].instrument_id == "AAPL", "leaf seed must keep its real instrument_id"
    assert rows[0].underlying == "", "leaf seed must carry a blank underlying"


def test_tradfi_v2_bundle_capture_suppresses_via_full_enumerate_v2() -> None:
    """End-to-end (enumerate_v2 → rollup → tradfi): captured futures_chain/combo/options_chain
    cells with ``instrument_id=""`` + ``underlying=<U>`` SUPPRESS their seeds; the
    un-captured ones seed with a blank instrument_id. Covers all three bundle types.
    """
    catalog = [
        # futures_chain: CME outright future leaf → rolls up to futures_chain on underlying ES.
        _make_tradfi_entry(
            instrument_id="ESH5", instrument_type="FUTURE", venue="CME", underlying="ES", available_from="2020-01-01"
        ),
        # combo: CME calendar/inter-commodity combo leaf → rolls up to combo on underlying ES.
        _make_tradfi_entry(
            instrument_id="ES-CAL-1", instrument_type="COMBO", venue="CME", underlying="ES", available_from="2020-01-01"
        ),
        # options_chain: CME option leaf → rolls up to options_chain on underlying CL.
        _make_tradfi_entry(
            instrument_id="CL-C-70",
            instrument_type="OPTION",
            venue="CME-OPTIONS",
            underlying="CL",
            available_from="2020-01-01",
        ),
    ]
    axis = _date_axis("2024-06-03")
    # Writer-shape captures (instrument_id="", underlying=<U>) for ES futures_chain
    # and ES combo only — CL options_chain stays UN-captured so it must seed.
    cols = ["venue", "chain", "data_type", "instrument_type", "instrument_id", "underlying", "league_id", "date"]
    present = {
        ("CME", "", "trades", "futures_chain", "", "ES", "", "2024-06-03"),
        ("CME", "", "trades", "combo", "", "ES", "", "2024-06-03"),
    }
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="tradfi",
            catalog=catalog,
            date_axis=axis,
            data_types=["trades"],
            present_set=present,
            present_cols=cols,
        )
    )
    seeded = {(r.instrument_type, r.instrument_id, r.underlying) for r in rows}
    # ES futures_chain + ES combo captured → suppressed.
    assert ("futures_chain", "", "ES") not in seeded, "captured futures_chain must be suppressed"
    assert ("combo", "", "ES") not in seeded, "captured combo must be suppressed"
    # CL options_chain un-captured → seeded with blank instrument_id + underlying=CL.
    assert ("options_chain", "", "CL") in seeded, "un-captured options_chain must seed at writer grain"
    # No bundle seed ever carries a non-blank instrument_id.
    bundle_types = {"futures_chain", "combo", "options_chain"}
    assert all(r.instrument_id == "" for r in rows if r.instrument_type in bundle_types), (
        "every bundle seed must carry a blank instrument_id"
    )
    # De-dup: each (venue, bundle_type, underlying, data_type, date) appears at most once.
    bundle_keys = [
        (r.venue, r.instrument_type, r.underlying, r.data_type, r.date)
        for r in rows
        if r.instrument_type in bundle_types
    ]
    assert len(bundle_keys) == len(set(bundle_keys)), "bundle seeds must be emitted once per underlying (no dupes)"


def test_tradfi_v2_equity_and_etf_seed_canonical_lowercase() -> None:
    """``EQUITY`` / ``ETF`` leaves also seed the lowercase writer grain."""
    catalog = [
        _make_tradfi_entry(instrument_id="AAPL", instrument_type="EQUITY", venue="NASDAQ", available_from="2025-01-01"),
        _make_tradfi_entry(instrument_id="SPY", instrument_type="ETF", venue="NASDAQ", available_from="2025-01-01"),
    ]
    # 2024-06-01 → NOT_LISTED; 2025-06-01 alive keeps the lifecycle-overlap filter happy.
    rows = list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2024-06-01", "2025-06-01"), ["ohlcv_1m"]))
    seeded = {(r.instrument_id, r.instrument_type) for r in rows}
    assert ("AAPL", "equity") in seeded
    assert ("SPY", "etf") in seeded
    # never the raw uppercase leaf
    assert not any(r.instrument_type in {"EQUITY", "ETF"} for r in rows)


# ---------------------------------------------------------------------------
# MVP capture-universe denominator gate (tradfi, operator-directed 2026-06-24)
# The expected_unattempted denominator = the MVP universe, NOT the full IS
# catalogue.  Out-of-MVP tradfi cells are NOT seeded.
# ---------------------------------------------------------------------------


def test_tradfi_v2_mvp_gate_excludes_non_mvp_via_column() -> None:
    """A catalogue row tagged mvp=False is NOT seeded (excluded from denominator)."""
    catalog = [_make_tradfi_entry(available_from="2019-01-01", venue="NASDAQ", mvp=False)]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set())
    )
    assert rows == []


def test_tradfi_v2_mvp_gate_includes_mvp_via_column() -> None:
    """A catalogue row tagged mvp=True IS seeded as expected_unattempted."""
    catalog = [_make_tradfi_entry(available_from="2019-01-01", venue="NASDAQ", mvp=True)]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set())
    )
    assert rows
    assert all(r.capture_status == "expected_unattempted" for r in rows)


def test_tradfi_v2_mvp_gate_computes_predicate_when_column_absent() -> None:
    """mvp=None → gate computes the UAC is_mvp predicate.

    A CME FUTURE with underlying="ES" (TradfiMvpRule CME/CBOE futures scope) IS in
    the MVP set.  A random NASDAQ equity NOT in TRADFI_EQUITY_PERP_BASIS_UNIVERSE
    is NOT in the MVP set and must NOT be seeded.
    """
    # MVP instrument: CME futures (ES underlier) → must be seeded.
    mvp_catalog = [
        _make_tradfi_entry(
            instrument_id="ESM26",
            instrument_type="FUTURE",
            venue="CME",
            underlying="ES",
            available_from="2019-01-01",
            mvp=None,
        )
    ]
    mvp_rows = list(
        enumerator_module._enumerate_v2_tradfi(mvp_catalog, _date_axis("2023-06-01"), ["ohlcv_1m"], present_set=set())
    )
    assert mvp_rows, "MVP CME future must be seeded when mvp column is absent"
    assert all(r.capture_status == "expected_unattempted" for r in mvp_rows)

    # Non-MVP instrument: a NASDAQ equity NOT in the basis universe → must be dropped.
    non_mvp_catalog = [
        _make_tradfi_entry(
            instrument_id="ZZZNOTMVP",
            instrument_type="EQUITY",
            venue="NASDAQ",
            base_asset="ZZZNOTMVP",
            available_from="2019-01-01",
            mvp=None,
        )
    ]
    non_mvp_rows = list(
        enumerator_module._enumerate_v2_tradfi(
            non_mvp_catalog, _date_axis("2023-06-01"), ["ohlcv_1m"], present_set=set()
        )
    )
    assert non_mvp_rows == [], "Non-MVP equity must NOT be seeded — it inflates the denominator"


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
    assert rows[0].league_id == "EPL"
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


def test_sports_v2_sentinel_league_id_never_emits_rows() -> None:
    """Defense-in-depth guard for A1 (2026-07-08/09): a catalog entry whose
    league_id resolves to a sentinel (e.g. "UNKNOWN") must yield ZERO rows,
    even though the primary fix (build_instrument_catalogue's roll-up filter)
    should mean such an entry never reaches this enumerator's catalog in the
    first place. A real sibling league in the same call must be unaffected.
    """
    phantom = _make_sports_entry(
        instrument_id="UNKNOWN", league_id="UNKNOWN", available_from="2025-12-15", available_to=None
    )
    real = _make_sports_entry(league_id="EPL", available_from="2024-01-10", available_to="2024-01-15")
    rows = list(
        enumerator_module._enumerate_v2_sports([phantom, real], _date_axis("2024-01-05", "2024-01-12"), ["lineups"])
    )
    assert all(r.league_id != "UNKNOWN" for r in rows)
    assert any(r.league_id == "EPL" for r in rows)


def test_sports_v2_deregistered_league_ids_never_emit_rows() -> None:
    """2026-07-13 24-league de-registration ruling: a (stale) catalog entry whose
    league_id is outside UAC ``LEAGUE_REGISTRY`` — a raw numeric api-football
    long-tail id ("110") or an alias string ("RFPL"/"LA_LIGA_2") — must yield
    ZERO per-league rows, even though the primary fix
    (build_instrument_catalogue's registry-membership roll-up gate) should mean
    such an entry never reaches this enumerator's catalog in the first place.
    A registered sibling league in the same call must be unaffected.
    """
    dereg_entries = [
        _make_sports_entry(instrument_id=lid, league_id=lid, available_from="2024-01-01", available_to=None)
        for lid in ("110", "RFPL", "LA_LIGA_2")
    ]
    real = _make_sports_entry(league_id="EPL", available_from="2024-01-10", available_to="2024-01-15")
    rows = list(
        enumerator_module._enumerate_v2_sports(
            [*dereg_entries, real], _date_axis("2024-01-05", "2024-01-12"), ["lineups"]
        )
    )
    assert all(r.league_id not in {"110", "RFPL", "LA_LIGA_2"} for r in rows)
    assert any(r.league_id == "EPL" for r in rows)


def test_sports_v2_fixture_team_player_grain_rows_never_treated_as_leagues() -> None:
    """Regression (2026-07-09): FIXTURE/TEAM/PLAYER-grain catalogue rows must be
    invisible to the LEAGUE-grain enumeration loop.

    The sports catalogue gained real fixture/team/player-grain rows
    (build_instrument_catalogue.build_sports_fixture_team_player_catalogue)
    alongside the pre-existing league-grain rows in the SAME catalog.parquet.
    _enumerate_v2_sports treats every catalog entry's league_id as a per-league
    lifecycle window and cross-products it against data_types x date_axis — if a
    fixture row (a single day's availability window) or a team/player row (whose
    league_id is a real Prediction league, same as a genuine league row) leaked
    through, it would fabricate NOT_LISTED/DELISTED/expected_unattempted rows
    from a non-league lifecycle, multiplying the denominator once per fixture/
    team/player instead of once per league — exactly the could-exist-projection
    inflation `sports_catalog_league_grain_only_scope_2026_07_08.md` warned
    about. A real sibling LEAGUE row in the same call must still emit normally.
    """
    # All four entries share league_id="EPL" and an available_from on/after
    # 2024-01-10 — each, if leaked through as if it were a league, would
    # independently emit an EXPECTED_INSTRUMENT_NOT_LISTED row for the
    # 2024-01-05 pre-listing date (available_from > 2024-01-05). Only
    # real_league is a genuine "league"-grain row, so with the filter working
    # exactly ONE NOT_LISTED row is emitted total; without it, FOUR (one per
    # entry) — the sharpest possible discriminator for this regression.
    fixture = _make_sports_entry(
        instrument_id="ENG_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20240111",
        instrument_type="fixture",
        league_id="EPL",
        available_from="2024-01-11",
        available_to="2024-01-11",
    )
    team = _make_sports_entry(
        instrument_id="ARSENAL",
        instrument_type="team",
        league_id="EPL",
        available_from="2024-01-11",
        available_to=None,
    )
    player = _make_sports_entry(
        instrument_id="SAKA_B",
        instrument_type="player",
        league_id="EPL",
        available_from="2024-01-11",
        available_to=None,
    )
    real_league = _make_sports_entry(league_id="EPL", available_from="2024-01-10", available_to="2024-01-15")
    rows = list(
        enumerator_module._enumerate_v2_sports(
            [fixture, team, player, real_league], _date_axis("2024-01-05", "2024-01-12"), ["lineups"]
        )
    )
    not_listed = [r for r in rows if r.reason == "EXPECTED_INSTRUMENT_NOT_LISTED"]
    assert len(not_listed) == 1, f"fixture/team/player rows leaked into league-grain enumeration: {rows!r}"
    assert not_listed[0].league_id == "EPL"
    assert not_listed[0].date == "2024-01-05"


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
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module.enumerate_v2(
                asset_group="cefi",
                catalog=catalog,
                date_axis=_date_axis("2020-01-01", "2025-06-01"),  # 2025-06-01 alive → no row; 2020-01-01 → NOT_LISTED
                data_types=["ohlcv_1d"],
            )
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
    """Empty catalog → no per-instrument rows regardless of asset_group.

    tradfi/cefi/defi/prediction additionally emit venue-grain sentinel rows
    (tradfi: non-trading-day mirroring v1 ``_enumerate_tradfi``;
    cefi/prediction: EXPECTED_PRE_VENUE_LAUNCH mirroring v1
    ``_enumerate_cefi``/``_enumerate_prediction``; defi: per-protocol
    EXPECTED_PRE_GENESIS_CHAIN + chain-level gas_fees pre-genesis mirroring
    v1 ``_enumerate_defi`` + ``_enumerate_defi_gas_fees``). Those are filtered
    out here so the assertion stays focused on the per-instrument denominator
    (the venue-grain <-> v1 parity was verified separately by
    ``tests/integration/test_enumerate_v2_superset_property.py`` before that
    file was retired alongside v1 (2026-07-09)).
    """
    for ag in ("cefi", "defi", "tradfi", "sports", "prediction"):
        rows = list(
            enumerator_module.enumerate_v2(
                asset_group=ag,
                catalog=[],
                date_axis=_date_axis("2024-01-01"),
                data_types=["ohlcv_1d"],
            )
        )
        rows = _drop_v2_venue_grain(rows)
        assert rows == [], f"expected no per-instrument rows for empty catalog, got {rows} for {ag}"


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
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set=present_set))
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.date == "2023-06-01"
    assert r.data_type == "ohlcv_1d"
    assert r.asset_group == "cefi"


def test_cefi_v2_alive_date_in_present_set_skipped() -> None:
    """Alive instrument date already in manifest → no per-instrument row emitted."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE")]
    date_axis = _date_axis("2023-06-01")
    key = _row_key_from_dict(
        {
            "venue": "BINANCE",
            "chain": "",
            "data_type": "ohlcv_1d",
            # Match the canonical instrument_type the MVP-qualifying fixture emits.
            "instrument_type": "PERPETUAL",
            "instrument_id": "BTC-USDT",
            "league_id": "",
            "date": "2023-06-01",
        }
    )
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set={key}))
    )
    assert rows == []


def test_cefi_v2_legacy_mode_alive_date_skipped() -> None:
    """present_set=None (legacy mode) → alive dates produce no per-instrument rows."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE")]
    date_axis = _date_axis("2023-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set=None))
    )
    assert rows == []


# ---------------------------------------------------------------------------
# Per-(venue, data_type) start_date gate — cefi_layer1_denominator_gaps 2026_07_03 item -007
#
# The alive branch must consult get_venue_data_type_start_date(venue, dt) before
# seeding expected_unattempted: dates before a data_type's UAC-declared start
# emit EXPECTED_PRE_SOURCE_COVERAGE_START (empty_confirmed) instead. Prevents
# the 17,282-row over-seed class purged 2026-07-03 (ASTER book_snapshot_5:
# venue live from 2023-07-22 but the source archive did not cover book_snapshot_5
# until the live-wire date). Reference scenario: HYPERLIQUID PERPETUAL trades —
# venue launched 2023-06-14 but the S3 archive only ships trades from
# 2025-03-22.
# ---------------------------------------------------------------------------


def test_cefi_v2_alive_date_before_dt_start_yields_pre_source_coverage_start() -> None:
    """Alive date < get_venue_data_type_start_date(venue, dt) → EXPECTED_PRE_SOURCE_COVERAGE_START."""
    # HYPERLIQUID trades start_date = 2025-03-22 (post-launch source floor).
    catalog = [
        _make_cefi_entry(
            venue="HYPERLIQUID",
            instrument_type="PERPETUAL",
            instrument_id="BTC-USD",
            available_from="2023-06-14",
            base_asset="BTC",
            mvp=True,
        )
    ]
    date_axis = _date_axis("2024-06-01")  # post-venue-launch, pre-trades-start
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["trades"], present_set=set()))
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.reason == "EXPECTED_PRE_SOURCE_COVERAGE_START"
    assert r.capture_status == "empty_confirmed"
    assert r.data_type == "trades"
    assert r.venue == "HYPERLIQUID"
    assert r.date == "2024-06-01"
    # closed-set compliance
    assert r.reason in EMPTY_CONFIRMED_REASONS


def test_cefi_v2_alive_date_on_dt_start_yields_expected_unattempted() -> None:
    """Alive date >= get_venue_data_type_start_date(venue, dt) → expected_unattempted (unchanged)."""
    catalog = [
        _make_cefi_entry(
            venue="HYPERLIQUID",
            instrument_type="PERPETUAL",
            instrument_id="BTC-USD",
            available_from="2023-06-14",
            base_asset="BTC",
            mvp=True,
        )
    ]
    # 2025-03-22 = the declared HYPERLIQUID trades start_date (inclusive).
    date_axis = _date_axis("2025-03-22")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["trades"], present_set=set()))
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].reason == ""


def test_cefi_v2_per_dt_start_gates_data_types_independently() -> None:
    """Per-(venue, dt) start_date gate is applied PER data_type — the 17,282-row over-seed regression guard.

    HYPERLIQUID: trades start_date=2025-03-22, book_snapshot_5 start_date=2023-04-15.
    On 2024-06-01 (post-venue-launch 2023-06-14):
      - trades: pre-source-coverage → EXPECTED_PRE_SOURCE_COVERAGE_START
      - book_snapshot_5: post-source-coverage → expected_unattempted
    """
    catalog = [
        _make_cefi_entry(
            venue="HYPERLIQUID",
            instrument_type="PERPETUAL",
            instrument_id="BTC-USD",
            available_from="2023-06-14",
            base_asset="BTC",
            mvp=True,
        )
    ]
    date_axis = _date_axis("2024-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["trades", "book_snapshot_5"], present_set=set()))
    )
    by_dt = {r.data_type: r for r in rows}
    assert by_dt["trades"].reason == "EXPECTED_PRE_SOURCE_COVERAGE_START"
    assert by_dt["trades"].capture_status == "empty_confirmed"
    assert by_dt["book_snapshot_5"].reason == ""
    assert by_dt["book_snapshot_5"].capture_status == "expected_unattempted"


def test_cefi_v2_dt_start_gate_no_start_date_permissive() -> None:
    """Unknown venue/dt (no start_date registered) → gate is permissive (no emit change)."""
    # ohlcv_1d on BINANCE (bare) — BINANCE-FUTURES has entries but bare BINANCE
    # + non-cefi-standard data_type falls off the VENUE_DATA_TYPE_CAPABILITIES
    # gate AND the VenueMapping venue-level fallback (bare BINANCE not indexed).
    catalog = [_make_cefi_entry(venue="BINANCE", available_from="2019-01-01")]
    date_axis = _date_axis("2019-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, date_axis, ["ohlcv_1d"], present_set=set()))
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].reason == ""


def test_defi_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """DeFi alive instrument date absent from manifest → expected_unattempted."""
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    date_axis = _date_axis("2024-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, date_axis, ["lending_indices"], present_set=set()))
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.chain == "ARBITRUM"
    assert r.asset_group == "defi"


def test_defi_v2_alive_date_in_present_set_skipped() -> None:
    """DeFi alive date already in manifest → no per-instrument row emitted."""
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    date_axis = _date_axis("2024-06-01")
    key = _row_key_from_dict(
        {
            "venue": "AAVE_V3",
            "chain": "ARBITRUM",
            "data_type": "lending_indices",
            "instrument_type": "lending",
            "instrument_id": "ETH-USDC",
            "league_id": "",
            "date": "2024-06-01",
        }
    )
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, date_axis, ["lending_indices"], present_set={key}))
    )
    assert rows == []


def test_defi_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_defi_entry(chain="ARBITRUM", available_from="2022-01-01")]
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_defi(
                catalog, _date_axis("2024-06-01"), ["lending_indices"], present_set=None
            )
        )
    )
    assert rows == []


def test_defi_v2_denominator_is_could_exist_universe_not_just_manifest() -> None:
    """Plan defi_manifest item 7 — the coverage DENOMINATOR is the COULD-EXIST universe, not just rows
    already in the manifest. Two alive DeFi instruments, ONE captured (in present_set): the enumerator
    seeds ``expected_unattempted`` for the un-captured one and SKIPS the captured one — so the seeded
    universe UNION manifest = {captured-cell, expected_unattempted-cell} is a SUPERSET of (or equal to)
    the manifest. Adding a catalog instrument can only GROW the denominator (seed a new owed cell),
    never shrink it / drop a captured cell. This is the regression that locks deployment-api's honest
    4-state denominator to the IS could-exist universe (active-but-uncaptured instruments are counted,
    diluting completion %)."""
    captured = _make_defi_entry(
        instrument_id="ETH-USDC", venue="AAVE_V3", chain="ARBITRUM", available_from="2022-01-01"
    )
    uncaptured = _make_defi_entry(
        instrument_id="WBTC-USDC", venue="AAVE_V3", chain="ARBITRUM", available_from="2022-01-01"
    )
    date_axis = _date_axis("2024-06-01")
    present_set = {
        _row_key_from_dict(
            {
                "venue": "AAVE_V3",
                "chain": "ARBITRUM",
                "data_type": "lending_indices",
                "instrument_type": "lending",
                "instrument_id": "ETH-USDC",
                "league_id": "",
                "date": "2024-06-01",
            }
        )
    }
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_defi(
                [captured, uncaptured], date_axis, ["lending_indices"], present_set=present_set
            )
        )
    )
    # exactly ONE owed cell — the un-captured instrument; the captured one is skipped (not dropped)
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].instrument_id == "WBTC-USDC"
    # denominator (captured 1 + expected_unattempted 1) ≥ manifest captured (1) — never shrinks
    assert 1 + len(rows) >= len(present_set)


def test_tradfi_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """TradFi alive instrument date absent from manifest → expected_unattempted."""
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    date_axis = _date_axis("2024-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, date_axis, ["ohlcv_1m"], present_set=set()))
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.chain == ""
    assert r.asset_group == "tradfi"


def test_tradfi_v2_alive_date_in_present_set_skipped() -> None:
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    date_axis = _date_axis("2024-06-01")
    # present_set carries the CANONICAL WRITER grain (lowercase ``etf``) — the
    # instrument_type the MTDS writer actually stamps for a captured cell. The seed
    # now also normalises to ``etf``, so the captured cell suppresses the seed.
    key = _row_key_from_dict(
        {
            "venue": "NASDAQ",
            "chain": "",
            "data_type": "ohlcv_1m",
            "instrument_type": "etf",
            "instrument_id": "SPY",
            "league_id": "",
            "date": "2024-06-01",
        }
    )
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, date_axis, ["ohlcv_1m"], present_set={key}))
    )
    assert rows == []


def test_tradfi_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_tradfi(catalog, _date_axis("2024-06-01"), ["ohlcv_1m"], present_set=None))
    )
    assert rows == []


def test_tradfi_v2_denominator_is_could_exist_universe_not_just_manifest() -> None:
    """Plan tradfi_manifest item ⑦ — the coverage DENOMINATOR is the COULD-EXIST universe, not just
    rows already in the manifest. Two alive TradFi instruments, ONE captured (in present_set): the
    enumerator seeds ``expected_unattempted`` for the un-captured one and SKIPS the captured one — so the
    seeded universe UNION manifest = {captured-cell, expected_unattempted-cell} is a SUPERSET of (or equal
    to) the manifest. Adding an IS-catalog instrument can only GROW the denominator (seed a new owed cell),
    never shrink it / drop a captured cell. This is the ⑦ regression that locks deployment-api's honest
    4-state denominator to the IS could-exist universe (active-but-uncaptured tradfi instruments are
    counted, diluting completion %) — the tradfi mirror of the defi denominator regression."""
    captured = _make_tradfi_entry(instrument_id="SPY", instrument_type="ETF", venue="NASDAQ")
    uncaptured = _make_tradfi_entry(instrument_id="QQQ", instrument_type="ETF", venue="NASDAQ")
    date_axis = _date_axis("2024-06-01")
    # present_set keyed at the canonical lowercase writer grain (``etf``) — what the
    # writer records; the seed now normalises to the same grain so the captured cell
    # is suppressed (the raw uppercase ``ETF`` would never match a real capture).
    present_set = {
        _row_key_from_dict(
            {
                "venue": "NASDAQ",
                "chain": "",
                "data_type": "ohlcv_1m",
                "instrument_type": "etf",
                "instrument_id": "SPY",
                "league_id": "",
                "date": "2024-06-01",
            }
        )
    }
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_tradfi(
                [captured, uncaptured], date_axis, ["ohlcv_1m"], present_set=present_set
            )
        )
    )
    # exactly ONE owed cell — the un-captured instrument; the captured one is skipped (not dropped)
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].instrument_id == "QQQ"
    # denominator (captured 1 + expected_unattempted 1) ≥ manifest captured (1) — never shrinks
    assert 1 + len(rows) >= len(present_set)


def test_sports_v2_alive_date_not_in_present_set_yields_expected_unattempted() -> None:
    """Sports alive fixture absent from manifest → expected_unattempted (league_id propagated)."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None, league_id="EPL")]
    date_axis = _date_axis("2024-01-12")
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["lineups"], present_set=set()))
    assert len(rows) == 1
    r = rows[0]
    assert r.capture_status == "expected_unattempted"
    assert r.reason == ""
    assert r.league_id == "EPL"
    assert r.asset_group == "sports"


def test_sports_v2_alive_date_in_present_set_skipped() -> None:
    """League-grain present match: the captured atom is (data_type, league_id, date) —
    venue / instrument_id / instrument_type are blank-tolerant (excluded from the key)."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None, league_id="EPL")]
    date_axis = _date_axis("2024-01-12")
    present_cols = ["data_type", "league_id", "date"]
    # Captured row carries blank venue/instrument_id (the real sports manifest atom),
    # yet the league-grain key still matches the catalogue league.
    key = tuple({"data_type": "lineups", "league_id": "EPL", "date": "2024-01-12"}[c] for c in present_cols)
    rows = list(
        enumerator_module._enumerate_v2_sports(
            catalog, date_axis, ["lineups"], present_set={key}, present_cols=present_cols
        )
    )
    assert rows == []


def test_sports_v2_legacy_mode_alive_date_skipped() -> None:
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to=None)]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-01-12"), ["lineups"], present_set=None)
    )
    assert rows == []


# ---------------------------------------------------------------------------
# Sports v2 understat matchday-awareness (Root-cause writer fix, part (b)) —
# plans/active/issues/sports_is_manifest_eu_regression_overwrite_2026_06_29.md
# ---------------------------------------------------------------------------


def test_sports_v2_understat_no_fixture_day_yields_expected_no_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """A covered understat league with NO scheduled fixture that day → EXPECTED_NO_FIXTURE,
    not a blank-reason expected_unattempted seed (the daily-forward-poll regression)."""
    monkeypatch.setattr(enumerator_module, "_build_understat_fixture_index", lambda days: set())
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    date_axis = _date_axis("2024-06-05")
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["XG"], present_set=set()))
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_NO_FIXTURE"
    assert rows[0].league_id == "EPL"
    assert rows[0].capture_status != "expected_unattempted"


def test_sports_v2_understat_fixture_day_falls_through_to_expected_unattempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A covered understat league WITH a scheduled fixture that day is a real
    pending_fetch → falls through to the normal expected_unattempted seed, never
    silently typed."""
    monkeypatch.setattr(enumerator_module, "_build_understat_fixture_index", lambda days: {("EPL", "2024-06-05")})
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    date_axis = _date_axis("2024-06-05")
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["XG"], present_set=set()))
    assert len(rows) == 1
    assert rows[0].reason == ""
    assert rows[0].capture_status == "expected_unattempted"


def test_sports_v2_understat_matchday_index_skipped_for_large_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Single-walk discipline: a date_axis larger than _MATCHDAY_INDEX_MAX_DAYS must
    NEVER trigger the per-day fixture-index build (one GCS read per day) — a
    full-history/backfill run falls back to the pre-existing non-matchday-aware
    behaviour instead."""

    def _boom(days: list[str]) -> set[tuple[str, str]]:
        raise AssertionError("_build_understat_fixture_index must not be called for a large date_axis")

    monkeypatch.setattr(enumerator_module, "_build_understat_fixture_index", _boom)
    assert enumerator_module._MATCHDAY_INDEX_MAX_DAYS < 40
    big_dates = [f"2024-01-{d:02d}" for d in range(1, 32)] + [f"2024-02-{d:02d}" for d in range(1, 10)]
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    date_axis = _date_axis(*big_dates)
    # Must not raise — confirms the bound gates the expensive index build off.
    rows = list(enumerator_module._enumerate_v2_sports(catalog, date_axis, ["XG"], present_set=set()))
    assert all(r.capture_status == "expected_unattempted" for r in rows)


# ---------------------------------------------------------------------------
# Sports v2 api_football FIXTURES season-complete calendar gate (STEP-4
# structural fix, phantom-pending forensics 2026-07-13): a truthset-evidenced
# no-fixture day must seed EXPECTED_NO_FIXTURE (empty_confirmed), never a
# phantom expected_unattempted; NO calendar evidence → seeding unchanged.
# ---------------------------------------------------------------------------


def _af_calendar(
    fixture_days: set[tuple[str, str]] | None = None,
    coverage: dict[str, tuple[tuple[str, str], ...]] | None = None,
) -> object:
    return enumerator_module._AfFixtureCalendar(
        fixture_days=fixture_days or set(),
        coverage=coverage if coverage is not None else {"EPL": (("2024-01-01", "2024-12-31"),)},
    )


def test_sports_v2_af_fixtures_no_fixture_day_yields_expected_no_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calendar covers the (league, day) and shows NO fixture → EXPECTED_NO_FIXTURE
    (empty_confirmed), not a blank-reason expected_unattempted phantom seed."""
    monkeypatch.setattr(enumerator_module, "_build_af_fixture_calendar", lambda: _af_calendar())
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-06-05"), ["FIXTURES"], present_set=set())
    )
    assert len(rows) == 1
    assert rows[0].reason == "EXPECTED_NO_FIXTURE"
    assert rows[0].capture_status == "empty_confirmed"
    assert rows[0].league_id == "EPL"


def test_sports_v2_af_fixtures_match_day_falls_through_to_expected_unattempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calendar covers the day AND shows a fixture → real pending fetch: the
    expected_unattempted seed is kept (never silently typed away)."""
    monkeypatch.setattr(
        enumerator_module,
        "_build_af_fixture_calendar",
        lambda: _af_calendar(fixture_days={("EPL", "2024-06-05")}),
    )
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-06-05"), ["FIXTURES"], present_set=set())
    )
    assert len(rows) == 1
    assert rows[0].reason == ""
    assert rows[0].capture_status == "expected_unattempted"


def test_sports_v2_af_fixtures_no_calendar_evidence_keeps_seeding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No calendar available at all (build returns None) → pre-existing alive-day
    expected_unattempted seeding UNCHANGED (honest-coverage rule: never silently
    shrink the denominator for unaudited leagues)."""
    monkeypatch.setattr(enumerator_module, "_build_af_fixture_calendar", lambda: None)
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-06-05"), ["FIXTURES"], present_set=set())
    )
    assert len(rows) == 1
    assert rows[0].reason == ""
    assert rows[0].capture_status == "expected_unattempted"


def test_sports_v2_af_fixtures_day_outside_coverage_keeps_seeding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calendar exists but the day is OUTSIDE every season-complete span for the
    league → no evidence for that cell → seeding unchanged."""
    monkeypatch.setattr(
        enumerator_module,
        "_build_af_fixture_calendar",
        lambda: _af_calendar(coverage={"EPL": (("2023-08-01", "2024-05-31"),)}),
    )
    catalog = [_make_sports_entry(available_from="2024-01-01", available_to=None, league_id="EPL")]
    rows = list(
        enumerator_module._enumerate_v2_sports(catalog, _date_axis("2024-06-05"), ["FIXTURES"], present_set=set())
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"


def test_af_calendar_from_dataframe_bridges_consecutive_seasons_only() -> None:
    """Pure builder: consecutive seasons bridge the inter-season gap (evidenced
    no-fixture territory); a season JUMP (2019 → 2021) does NOT cover the gap."""
    df = pd.DataFrame(
        [
            {"canonical_league_id": "EPL", "season": 2023, "date": "2023-08-12"},
            {"canonical_league_id": "EPL", "season": 2023, "date": "2024-05-19"},
            {"canonical_league_id": "EPL", "season": 2024, "date": "2024-08-16"},
            {"canonical_league_id": "EPL", "season": 2024, "date": "2025-05-25"},
            {"canonical_league_id": "LIGA_X", "season": 2019, "date": "2019-08-01"},
            {"canonical_league_id": "LIGA_X", "season": 2019, "date": "2020-05-01"},
            {"canonical_league_id": "LIGA_X", "season": 2021, "date": "2021-08-01"},
            {"canonical_league_id": "LIGA_X", "season": 2021, "date": "2022-05-01"},
        ]
    )
    cal = enumerator_module._af_calendar_from_dataframe(df)
    assert cal is not None
    # Consecutive EPL seasons merge into ONE bridged interval.
    assert cal.coverage["EPL"] == (("2023-08-12", "2025-05-25"),)
    # Inter-season gap day (no fixture, bridged) → evidenced no-fixture day.
    assert cal.is_no_fixture_day("EPL", "2024-06-05") is True
    # A fixture day is never a no-fixture day.
    assert cal.is_no_fixture_day("EPL", "2023-08-12") is False
    # Outside every span → no evidence.
    assert cal.is_no_fixture_day("EPL", "2025-06-01") is False
    # Season jump: 2020 gap NOT covered.
    assert cal.coverage["LIGA_X"] == (("2019-08-01", "2020-05-01"), ("2021-08-01", "2022-05-01"))
    assert cal.is_no_fixture_day("LIGA_X", "2020-09-15") is False


def test_af_calendar_from_dataframe_empty_or_missing_columns_returns_none() -> None:
    """No usable truthset rows → None → callers keep the pre-existing seeding."""
    assert enumerator_module._af_calendar_from_dataframe(pd.DataFrame()) is None
    assert enumerator_module._af_calendar_from_dataframe(pd.DataFrame([{"league_id": "EPL"}])) is None


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
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module.enumerate_v2(
                asset_group="cefi",
                catalog=catalog,
                date_axis=_date_axis("2023-06-01"),
                data_types=["ohlcv_1d"],
                present_set=present_set,
            )
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"


def test_enumerate_v2_with_present_set_none_skips_alive_dates() -> None:
    """enumerate_v2() with present_set=None must skip alive dates (legacy mode)."""
    catalog = [_make_tradfi_entry(available_from="2020-01-01")]
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module.enumerate_v2(
                asset_group="tradfi",
                catalog=catalog,
                date_axis=_date_axis("2024-06-01"),
                data_types=["ohlcv_1m"],
                present_set=None,
            )
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
    rows = _drop_v2_venue_grain(
        list(
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
    )
    not_listed = [r for r in rows if r.capture_status == "empty_confirmed"]
    assert len(not_listed) == 1
    assert not_listed[0].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"


# ---------------------------------------------------------------------------
# G1-ENUM regression tests — instrument-type x data_type validity filter
# ---------------------------------------------------------------------------


class TestG1EnumCefiFilter:
    """cefi enumerator must respect the VALID_DATA_TYPES_BY_AG_AND_INSTRUMENT_TYPE matrix."""

    _ALL_CEFI_DTS: ClassVar[list[str]] = [
        "trades",
        "book_snapshot_5",
        "derivative_ticker",
        "liquidations",
        "ohlcv_1m",
        "options_chain",
        "futures_chain",
    ]

    def _run(self, instrument_type: str, data_types: list[str] | None = None) -> list:
        catalog = [_make_cefi_entry(instrument_type=instrument_type)]
        # G1 filter tests assert per-instrument validity-matrix behavior; drop the
        # venue-grain pre-venue-launch pass so its universal data_type fan (for
        # cefi venues whose launch_date > 2024-01-15 — e.g. LIGHTER-ZKSYNC,
        # EXTENDED-STARKNET, KALSHI-PERP, POLYMARKET-PERP) doesn't leak into
        # "data_types_emitted".
        return _drop_v2_venue_grain(
            list(
                enumerator_module._enumerate_v2_cefi(
                    catalog,
                    _date_axis("2024-01-15"),
                    data_types or self._ALL_CEFI_DTS,
                    present_set=set(),
                )
            )
        )

    def test_perpetual_has_trades_not_options_chain(self) -> None:
        rows = self._run("PERPETUAL")
        data_types_emitted = {r.data_type for r in rows}
        assert "trades" in data_types_emitted
        assert "derivative_ticker" in data_types_emitted
        assert "options_chain" not in data_types_emitted
        assert "futures_chain" not in data_types_emitted

    def test_perpetual_alias_perp_works(self) -> None:
        rows = self._run("PERP")
        data_types_emitted = {r.data_type for r in rows}
        assert "trades" in data_types_emitted
        assert "options_chain" not in data_types_emitted

    def test_spot_has_trades_not_derivative_ticker(self) -> None:
        rows = self._run("SPOT")
        data_types_emitted = {r.data_type for r in rows}
        assert "trades" in data_types_emitted
        assert "derivative_ticker" not in data_types_emitted
        assert "options_chain" not in data_types_emitted

    def test_option_leaf_yields_zero_rows(self) -> None:
        """cefi OPTION leaf → frozenset() → no rows emitted at all."""
        rows = self._run("OPTION")
        assert rows == [], f"Expected zero rows for cefi OPTION, got {len(rows)}"

    def test_combo_leaf_yields_zero_rows(self) -> None:
        """cefi COMBO multi-leg → frozenset() → no rows emitted."""
        rows = self._run("COMBO")
        assert rows == [], f"Expected zero rows for cefi COMBO, got {len(rows)}"

    def test_options_chain_bundle_emits_trades_only(self) -> None:
        """ERA-B: the cefi options_chain instrument_type's market data_type is
        trades (not the chain name) → only 'trades' emitted."""
        rows = self._run("options_chain")
        data_types_emitted = {r.data_type for r in rows}
        assert data_types_emitted == {"trades"}

    def test_futures_chain_bundle_emits_trades_only(self) -> None:
        """ERA-B: the cefi futures_chain instrument_type's market data_type is
        trades → only 'trades' emitted."""
        rows = self._run("futures_chain")
        data_types_emitted = {r.data_type for r in rows}
        assert data_types_emitted == {"trades"}


class TestG1EnumDefiFilter:
    """defi enumerator must derive valid data_types from PROTOCOL_CAPABILITIES."""

    _ALL_DEFI_DTS: ClassVar[list[str]] = [
        "lending_indices",
        "liquidations",
        "risk_params",
        "dex_pool_state",
        "dex_pool_swaps",
        "perp_funding",
    ]

    def _run(self, instrument_type: str, data_types: list[str] | None = None) -> list:
        catalog = [_make_defi_entry(instrument_type=instrument_type, chain="ETHEREUM")]
        # G1 filter tests assert per-instrument validity-matrix behavior; drop the
        # venue-grain pre-launch pass so its per-protocol data_type fan doesn't
        # leak into "data_types_emitted" for protocols still pre-launch on 2024-01-15.
        return _drop_v2_venue_grain(
            list(
                enumerator_module._enumerate_v2_defi(
                    catalog,
                    _date_axis("2024-01-15"),
                    data_types or self._ALL_DEFI_DTS,
                    present_set=set(),
                )
            )
        )

    def test_pool_emits_only_valid_data_types(self) -> None:
        """defi POOL instrument → must emit only data_types from the UAC matrix.

        The G1-ENUM benefit: data_types that NO pool protocol ever uses are filtered.
        ``risk_params`` is LENDING-exclusive (only in _LENDING_DATA, not any _POOL
        protocol) → key invariant.
        ``lending_indices`` is in one hybrid POOL protocol (line ~592 in _defi.py) so
        it legitimately appears in the POOL union — NOT a cross-product there.
        ``perp_funding`` is in GMX which uses _POOL → also legitimately in the union.
        """
        rows = self._run("POOL")
        data_types_emitted = {r.data_type for r in rows}
        # key G1-ENUM invariant: risk_params is LENDING-only (no POOL protocol uses it)
        assert "risk_params" not in data_types_emitted, (
            f"POOL emitted risk_params — this is a true cross-product: {data_types_emitted}"
        )
        # And there must be at least one DEX data_type (the POOL's primary data)
        assert "dex_pool_state" in data_types_emitted or "dex_pool_swaps" in data_types_emitted

    def test_lending_has_no_dex_pool_state(self) -> None:
        """defi LENDING instrument → should NOT emit dex_pool_state."""
        rows = self._run("LENDING")
        data_types_emitted = {r.data_type for r in rows}
        assert "dex_pool_state" not in data_types_emitted, (
            f"LENDING emitted dex_pool_state — false cross-product: {data_types_emitted}"
        )
        assert "dex_pool_swaps" not in data_types_emitted


class TestG1EnumTradfiFilter:
    """tradfi enumerator must respect the validity matrix."""

    _ALL_TRADFI_DTS: ClassVar[list[str]] = [
        "trades",
        "ohlcv_1m",
        "ohlcv_15m",
        "ohlcv_24h",
        "tbbo",
        "mbp_10",
        "corporate_action_confirmed",
        "earnings_result",
        "macro_result",
    ]

    def _run(self, instrument_type: str, data_types: list[str] | None = None) -> list:
        catalog = [_make_tradfi_entry(instrument_type=instrument_type)]
        # G1 filter tests assert per-instrument validity-matrix behavior; drop the
        # venue-grain non-trading-day pass so its universal data_type fan doesn't
        # leak into "data_types_emitted" (2024-01-15 is MLK Day → holiday).
        return _drop_v2_venue_grain(
            list(
                enumerator_module._enumerate_v2_tradfi(
                    catalog,
                    _date_axis("2024-01-15"),
                    data_types or self._ALL_TRADFI_DTS,
                    present_set=set(),
                )
            )
        )

    def test_etf_has_no_earnings_result(self) -> None:
        rows = self._run("ETF")
        data_types_emitted = {r.data_type for r in rows}
        assert "trades" in data_types_emitted
        assert "earnings_result" not in data_types_emitted

    def test_equity_has_earnings_result(self) -> None:
        rows = self._run("EQUITY")
        data_types_emitted = {r.data_type for r in rows}
        assert "earnings_result" in data_types_emitted
        assert "corporate_action_confirmed" in data_types_emitted

    def test_index_only_ohlcv(self) -> None:
        rows = self._run("INDEX")
        data_types_emitted = {r.data_type for r in rows}
        assert "trades" not in data_types_emitted
        # at least one ohlcv must be present
        assert any("ohlcv" in dt for dt in data_types_emitted)


# ---------------------------------------------------------------------------
# G1-ENUM bundle-grain rollup (slot-7 2026-06-07)
#
# Leaf OPTION/COMBO contracts roll UP into a per-underlying options_chain /
# futures_chain bundle: ZERO per-contract candidates (the pre-G1-ENUM over-fan
# was one candidate per leaf contract x data_type), and the per-underlying
# bundle catalogue entry carries exactly ONE candidate. Pins the contract so a
# regression cannot reintroduce the per-leaf fan.
# ---------------------------------------------------------------------------

_CEFI_DATA_TYPES = ["trades", "book_snapshot_5", "ohlcv_1m", "options_chain"]


def test_cefi_v2_option_leaf_yields_no_per_contract_candidate() -> None:
    """A leaf OPTION contract must produce NO per-contract expected_unattempted
    rows — it is captured at the options_chain bundle grain (frozenset() in the
    validity matrix). This kills the 72K-OPTION x N-data_type over-fan."""
    catalog = [
        _make_cefi_entry(
            instrument_id="BTC-29MAR24-50000-C",
            instrument_type="OPTION",
            venue="DERIBIT",
            available_from="2025-01-01",
        )
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")  # 2024-06-01 pre-listing, 2025-06-01 alive
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, _CEFI_DATA_TYPES)))
    assert rows == [], "leaf OPTION must not fan per-contract candidates"


def test_cefi_v2_combo_leaf_yields_no_per_contract_candidate() -> None:
    catalog = [
        _make_cefi_entry(
            instrument_id="BTC-COMBO-XYZ",
            instrument_type="COMBO",
            venue="DERIBIT",
            available_from="2025-01-01",
        )
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, _CEFI_DATA_TYPES)))
    assert rows == [], "leaf COMBO must not fan per-contract candidates"


def test_cefi_v2_options_chain_bundle_yields_exactly_one_per_underlying() -> None:
    """The per-underlying options_chain bundle catalogue entry produces exactly
    ONE candidate per date (the bundle grain) with data_type=trades (Era-B) —
    not the full cefi data_type cross-product, not data_type=options_chain."""
    catalog = [
        _make_cefi_entry(
            instrument_id="BTC",
            instrument_type="options_chain",
            venue="DERIBIT",
            available_from="2025-01-01",
        )
    ]
    # Two-date window (alive date keeps the lifecycle-overlap filter from
    # skipping the instrument); only the pre-listing date yields a candidate.
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_cefi(catalog, dates, _CEFI_DATA_TYPES)))
    assert len(rows) == 1, "bundle entry must yield exactly one candidate per underlying/date"
    assert rows[0].data_type == "trades"  # ERA-B: chain bundle's market data_type is trades
    assert rows[0].instrument_id == "BTC"


def test_cefi_v2_bundle_grain_matches_uac_grain_axis() -> None:
    """The enumerator's per-leaf-skip must agree with the UAC GRAIN SSOT."""
    from unified_api_contracts import GRAIN_BUNDLE_BY_UNDERLYING, grain_for_instrument_type

    assert grain_for_instrument_type("cefi", "OPTION") == GRAIN_BUNDLE_BY_UNDERLYING
    # SPOT is leaf → it DOES fan (one row per kept data_type) so the two agree
    # only for the bundle-grain types.
    spot_catalog = [_make_cefi_entry(instrument_type="SPOT", venue="BINANCE", available_from="2025-01-01")]
    spot_rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_cefi(spot_catalog, _date_axis("2024-06-01", "2025-06-01"), _CEFI_DATA_TYPES)
        )
    )
    assert len(spot_rows) > 0, "leaf SPOT must still fan per-data_type"


# ---------------------------------------------------------------------------
# G1-ENUM bundle-grain ROLLUP via enumerate_v2 (slot-7 2026-06-07, ERA-B)
#
# The end-to-end acceptance bar: OPTION/COMBO leaves collapse to ONE candidate
# per underlying with instrument_type=options_chain AND data_type=trades (Era-B;
# NOT one per leaf contract, NOT data_type=options_chain); the futures_chain
# bundle entry yields one candidate per underlying (data_type=trades); impossible
# pairs (PERPETUAL x options_chain) stay excluded. Generalises slot-4's
# league-grain rollup (driven by the UAC GRAIN + bundle_instrument_type registry +
# the validity matrix options_chain/futures_chain → trades).
# ---------------------------------------------------------------------------


def _opt_entry(instrument_id: str, underlying: str = "", *, instrument_type: str = "OPTION", venue: str = "DERIBIT"):
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id="",
        available_from="2025-01-01",
        available_to=None,
        market_created_at=None,
        settlement_time=None,
        underlying=underlying,
    )


def test_enumerate_v2_option_leaves_collapse_to_one_per_underlying() -> None:
    catalog = [
        _opt_entry("BTC-29MAR24-50000-C", "BTC"),
        _opt_entry("BTC-29MAR24-60000-P", "BTC"),
        _opt_entry("BTC-26APR24-50000-C", "BTC"),
        _opt_entry("ETH-29MAR24-3000-C", "ETH"),
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")  # pre-listing date + alive date
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    # Era-B: exactly one candidate per underlying, data_type=trades — NOT one per
    # contract, NOT data_type=options_chain.
    assert {(r.instrument_id, r.data_type) for r in rows} == {("BTC", "trades"), ("ETH", "trades")}
    assert all(r.instrument_type == "options_chain" for r in rows)
    # No per-contract OPTION candidates and no data_type=options_chain leaked through.
    assert not any(r.instrument_type == "OPTION" for r in rows)
    assert not any(r.data_type == "options_chain" for r in rows)


def test_enumerate_v2_combo_leaves_roll_up_to_options_chain() -> None:
    catalog = [
        _opt_entry("BTC-COMBO-1", "BTC", instrument_type="COMBO"),
        _opt_entry("BTC-COMBO-2", "BTC", instrument_type="COMBO"),
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    # Era-B: instrument_type=options_chain bundle, data_type=trades.
    assert {(r.instrument_id, r.instrument_type, r.data_type) for r in rows} == {("BTC", "options_chain", "trades")}


def test_enumerate_v2_underlying_derived_when_field_blank() -> None:
    catalog = [_opt_entry("BTC-29MAR24-50000-C")]  # underlying="" → derive "BTC"
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert {(r.instrument_id, r.data_type) for r in rows} == {("BTC", "trades")}


def test_enumerate_v2_futures_chain_bundle_entry_yields_one_per_underlying() -> None:
    """A futures_chain bundle entry (per-underlying) passes through the rollup and
    yields exactly one candidate — instrument_type=futures_chain, data_type=trades
    (Era-B; the chain name is the instrument_type, the market data_type is trades)."""
    catalog = [_opt_entry("BTC", "BTC", instrument_type="futures_chain")]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert {(r.instrument_id, r.instrument_type, r.data_type) for r in rows} == {("BTC", "futures_chain", "trades")}


def test_enumerate_v2_perpetual_does_not_produce_options_chain() -> None:
    """Impossible pair PERPETUAL x options_chain must stay excluded (a PERP leaf
    is per-contract and never rolls up to a chain bundle)."""
    catalog = [_opt_entry("BTC-USDT-PERP", "BTC", instrument_type="PERP", venue="BINANCE")]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert not any(r.data_type == "options_chain" for r in rows)
    # PERP stays per-contract (its own valid data_types), instrument_id unchanged.
    assert all(r.instrument_id == "BTC-USDT-PERP" for r in rows)


def test_enumerate_v2_tradfi_option_leaves_roll_up() -> None:
    """tradfi option leaves roll up to the per-underlying options_chain bundle, which
    admits the chain's OWN captured market-data data_types — NOT cefi-parity
    trades-only.

    The grain mechanism is identical to cefi (option/combo leaves → one synthetic
    per-underlying options_chain entry), but the bundle's emitted data_types come
    from the operator-ratified tradfi validity matrix (T-OLD-2b, slot-6 verified vs
    the market-data-tick-tradfi present-set): ``("tradfi","options_chain")`` admits
    ``{trades, ohlcv_1m}`` of the canonical tradfi data_types (the databento chain
    captures). The matrix also carries the non-canonical ``options_chain`` snapshot
    data_type (mark_iv/greeks), but the enumerator cross-joins only the canonical
    ``DATA_TYPES_BY_ASSET_GROUP["tradfi"]`` (where ``options_chain`` is an
    instrument_type, not a data_type) so it is correctly NOT emitted here — that
    snapshot cell is materialised by the per-AG v8→v9 migrator relabel, not the
    could-exist enumerator. cefi's clean ``{trades}`` is the cefi slice; tradfi's
    broader admit-set is deliberate (a trades-only tradfi slice marked ~12K real
    captured chain cells "impossible").
    """
    catalog = [
        _opt_entry("ES-OPT-1", "ES", instrument_type="OPTION", venue="CME"),
        _opt_entry("ES-OPT-2", "ES", instrument_type="OPTION", venue="CME"),
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")
    # Filter the venue-grain non-trading-day pass so the assertion stays focused
    # on the per-underlying bundle rows (the roll-up under test); venue-grain
    # rows carry blank instrument_type and would otherwise leak into the set.
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="tradfi", catalog=catalog, date_axis=dates))
    )
    # Era-B: instrument_type=options_chain bundle; tradfi admits trades + ohlcv_1m
    # (the captured chain market-data data_types — UAC validity matrix T-OLD-2b).
    # axis-3 (2026-06-22): a tradfi BUNDLE cell carries instrument_id="" + underlying=<U>
    # (the MTDS writer grain), NOT instrument_id=<underlying>.
    assert {(r.instrument_id, r.underlying, r.instrument_type, r.data_type) for r in rows} == {
        ("", "ES", "options_chain", "trades"),
        ("", "ES", "options_chain", "ohlcv_1m"),
    }


def test_enumerate_v2_non_bundle_instruments_unchanged() -> None:
    """SPOT (leaf) is untouched by the rollup — still per-instrument."""
    catalog = [_opt_entry("BTC-USDT", instrument_type="SPOT", venue="BINANCE", underlying="BTC")]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert all(r.instrument_id == "BTC-USDT" for r in rows)
    assert not any(r.data_type == "options_chain" for r in rows)


# ---------------------------------------------------------------------------
# F2 — VENUE-aware FUTURE bundle-grain rollup (slot-7 2026-06-07)
#
# Bare FUTURE leaves bundle to a per-underlying futures_chain ONLY at DERIBIT/OKX
# (the bulk-chain venues); at BYBIT (+ every other per-contract venue) a FUTURE
# leaf stays per-contract. Venue-blind bundling over-seeds BYBIT; venue-blind leaf
# over-seeds DERIBIT/OKX with ~700 false per-contract FUTURE candidates (the cefi
# F2 residual). Driven by the UAC FUTURE_BUNDLE_VENUES overlay.
# ---------------------------------------------------------------------------


def test_enumerate_v2_future_leaf_bundles_at_deribit() -> None:
    """A bare FUTURE leaf at DERIBIT rolls up to ONE per-underlying futures_chain
    candidate (data_type=trades) — the same shape as its options_chain."""
    catalog = [
        _opt_entry("BTC-27JUN25", "BTC", instrument_type="FUTURE", venue="DERIBIT"),
        _opt_entry("BTC-26SEP25", "BTC", instrument_type="FUTURE", venue="DERIBIT"),
    ]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert {(r.instrument_id, r.instrument_type, r.data_type) for r in rows} == {("BTC", "futures_chain", "trades")}
    assert not any(r.instrument_type == "FUTURE" for r in rows)


def test_enumerate_v2_future_leaf_bundles_at_okx() -> None:
    """OKX (and OKX-FUTURES via the base-venue token) also bundles FUTURE."""
    catalog = [_opt_entry("BTC-27JUN25", "BTC", instrument_type="FUTURE", venue="OKX-FUTURES")]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    assert {(r.instrument_id, r.instrument_type, r.data_type) for r in rows} == {("BTC", "futures_chain", "trades")}


def test_enumerate_v2_future_leaf_stays_per_contract_at_bybit() -> None:
    """A FUTURE leaf at BYBIT is captured per-contract — it must NOT collapse into a
    futures_chain bundle (BYBIT is a per-contract futures venue)."""
    catalog = [_opt_entry("BTC-27JUN25", "BTC", instrument_type="FUTURE", venue="BYBIT")]
    dates = _date_axis("2024-06-01", "2025-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module.enumerate_v2(asset_group="cefi", catalog=catalog, date_axis=dates))
    )
    # Stays per-contract: instrument_id unchanged, no futures_chain rollup.
    assert all(r.instrument_id == "BTC-27JUN25" for r in rows)
    assert not any(r.instrument_type == "futures_chain" for r in rows)
    assert rows, "BYBIT FUTURE leaf must still fan per-contract candidates"


# ---------------------------------------------------------------------------
# Prediction cqg-bundle-grain filter (decision 338, 2026-06-19)
# ---------------------------------------------------------------------------


def _make_prediction_cqg_entry(
    cqg: str = "BTC_UP_DOWN_DAILY",
    available_from: str | None = "2025-01-01",
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=cqg,
        instrument_type="prediction_market",
        venue="POLYMARKET",
        chain="",
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=available_from,
        settlement_time=available_to,
        data_type=enumerator_module._PREDICTION_CQG_DATA_TYPE,
    )


def _make_prediction_cid_entry(
    condition_id: str = "0xabc",
    data_type: str = "trades",
) -> InstrumentCatalogEntry:
    return InstrumentCatalogEntry(
        instrument_id=condition_id,
        instrument_type="prediction_market",
        venue="POLYMARKET",
        chain="",
        league_id="",
        available_from="2025-01-01",
        available_to=None,
        market_created_at="2025-01-01",
        settlement_time=None,
        data_type=data_type,
    )


def test_prediction_v2_cqg_filter_excludes_per_condition_id() -> None:
    """Decision 338: when the catalogue carries BOTH grains, the prediction enumerator
    seeds ONLY the cqg-bundle grain (per-conditionId trades/market_lifecycle EXCLUDED —
    else the >50M-row false-EU blow-up)."""
    catalog = [
        _make_prediction_cqg_entry("BTC_UP_DOWN_DAILY"),
        _make_prediction_cid_entry("0xabc", "trades"),
        _make_prediction_cid_entry("0xabc", "market_lifecycle"),
        _make_prediction_cid_entry("0xdef", "trades"),
    ]
    dates = _date_axis("2025-01-01", "2025-01-02", "2025-01-03")
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="prediction",
            catalog=catalog,
            date_axis=dates,
            data_types=["prediction_canonical_question_group", "trades", "market_lifecycle"],
            present_set=set(),
            present_cols=["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"],
        )
    )
    assert rows, "cqg-bundle rows must seed expected_unattempted"
    assert {r.data_type for r in rows} == {"prediction_canonical_question_group"}
    assert {r.instrument_id for r in rows} == {"BTC_UP_DOWN_DAILY"}, "no per-conditionId leak"


def test_prediction_v2_no_cqg_falls_through() -> None:
    """A catalogue with NO cqg-bundle rows (legacy/test) must NOT silently drop the AG —
    fall through to all rows unchanged."""
    catalog = [_make_prediction_cid_entry("0xabc", "trades")]
    dates = _date_axis("2025-01-01", "2025-01-02")
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="prediction",
            catalog=catalog,
            date_axis=dates,
            data_types=["trades"],
            present_set=set(),
            present_cols=["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"],
        )
    )
    assert rows, "legacy no-cqg catalogue must still enumerate"
    assert {r.data_type for r in rows} == {"trades"}


# ---------------------------------------------------------------------------
# Full-history range-encoding (Part 2 — scalable representation)
# ---------------------------------------------------------------------------


def _eu_row(date_str: str, instrument_id: str = "BTCUSDT", reason: str = "") -> object:
    return ExpectedRow(
        asset_group="cefi",
        venue="BINANCE",
        chain="",
        data_type="trades",
        instrument_type="spot",
        instrument_id=instrument_id,
        league_id="",
        date=date_str,
        reason=reason,
        capture_status="expected_unattempted",
    )


def test_range_encode_collapses_contiguous_run() -> None:
    """A contiguous run of days collapses into ONE RangeRow with exact n_days."""
    rows = [_eu_row(d) for d in ("2025-01-01", "2025-01-02", "2025-01-03")]
    ranges = enumerator_module.range_encode(rows)
    assert len(ranges) == 1
    assert ranges[0].date_start == "2025-01-01"
    assert ranges[0].date_end == "2025-01-03"
    assert ranges[0].n_days == 3


def test_range_encode_splits_on_gap() -> None:
    """A gap > 1 day starts a new span; Σ n_days equals the per-day count (exact denominator)."""
    rows = [_eu_row(d) for d in ("2025-01-01", "2025-01-02", "2025-01-05", "2025-01-06")]
    ranges = enumerator_module.range_encode(rows)
    assert len(ranges) == 2
    assert [(r.date_start, r.date_end, r.n_days) for r in ranges] == [
        ("2025-01-01", "2025-01-02", 2),
        ("2025-01-05", "2025-01-06", 2),
    ]
    assert sum(r.n_days for r in ranges) == 4


def test_range_encode_separate_keys_do_not_merge() -> None:
    """Different shard-keys never merge even on the same dates."""
    rows = [_eu_row("2025-01-01", "BTCUSDT"), _eu_row("2025-01-01", "ETHUSDT")]
    ranges = enumerator_module.range_encode(rows)
    assert len(ranges) == 2
    assert {r.instrument_id for r in ranges} == {"BTCUSDT", "ETHUSDT"}


def test_range_encode_deterministic() -> None:
    """Re-encoding the same rows (any input order) yields byte-identical output (sorted keys+dates)."""
    rows = [_eu_row(d) for d in ("2025-01-03", "2025-01-01", "2025-01-02")]
    a = enumerator_module.range_encode(rows)
    b = enumerator_module.range_encode(list(reversed(rows)))
    assert a == b
    assert a[0].n_days == 3


# ---------------------------------------------------------------------------
# TradFi writer-grain alignment (2026-06-22): the expected-universe SEED must
# roll FUTURE/COMBO leaves up to the SAME instrument_type the MTDS writer
# captures (futures_chain / combo), NOT the passthrough leaf (future) or the
# wrong bundle (options_chain). See UAC FUTURE_BUNDLE_VENUES["tradfi"] +
# BUNDLE_INSTRUMENT_TYPE_BY_AG_AND_LEAF[("tradfi","combo")].
# ---------------------------------------------------------------------------


def test_canonical_writer_instrument_type_tradfi_future_cme_is_futures_chain() -> None:
    """A CME outright FUTURE leaf seeds instrument_type=futures_chain (writer grain)."""
    entry = _make_tradfi_entry(instrument_id="ESM6", instrument_type="FUTURE", venue="CME")
    assert enumerator_module._canonical_writer_instrument_type("tradfi", entry) == "futures_chain"


def test_canonical_writer_instrument_type_tradfi_combo_is_combo() -> None:
    """A CME COMBO (spread) leaf seeds instrument_type=combo (writer keeps its own partition)."""
    entry = _make_tradfi_entry(instrument_id="ESM6-ESU6", instrument_type="COMBO", venue="CME")
    assert enumerator_module._canonical_writer_instrument_type("tradfi", entry) == "combo"


def test_canonical_writer_instrument_type_tradfi_equity_passthrough() -> None:
    """Regression: a NASDAQ EQUITY leaf still seeds the lowercase passthrough type."""
    entry = _make_tradfi_entry(instrument_id="AAPL", instrument_type="EQUITY", venue="NASDAQ")
    assert enumerator_module._canonical_writer_instrument_type("tradfi", entry) == "equity"


def test_rollup_bundle_grain_tradfi_future_collapses_to_futures_chain() -> None:
    """Two CME ES futures leaves collapse to ONE per-underlying futures_chain entry."""
    catalog = [
        _make_tradfi_entry(instrument_id="ESM6", instrument_type="FUTURE", venue="CME", underlying="ES"),
        _make_tradfi_entry(instrument_id="ESU6", instrument_type="FUTURE", venue="CME", underlying="ES"),
    ]
    rolled = enumerator_module._rollup_bundle_grain(catalog, "tradfi")
    synth = [e for e in rolled if e.instrument_type == "futures_chain"]
    assert len(synth) == 1
    assert synth[0].instrument_id == "ES"
    assert synth[0].data_type is None  # data_type resolved later from the validity matrix


def test_rollup_bundle_grain_tradfi_combo_collapses_to_combo() -> None:
    """CME combo leaves collapse to ONE per-underlying combo entry (NOT options_chain)."""
    catalog = [
        _make_tradfi_entry(instrument_id="ESM6-ESU6", instrument_type="COMBO", venue="CME", underlying="ES"),
        _make_tradfi_entry(instrument_id="ESU6-ESZ6", instrument_type="COMBO", venue="CME", underlying="ES"),
    ]
    rolled = enumerator_module._rollup_bundle_grain(catalog, "tradfi")
    types = {e.instrument_type for e in rolled}
    assert "combo" in types
    assert "options_chain" not in types  # the pre-2026-06-22 mis-grain is gone
    synth = [e for e in rolled if e.instrument_type == "combo"]
    assert len(synth) == 1
    assert synth[0].instrument_id == "ES"


def test_make_tradfi_entry_underlying_kw_supported() -> None:
    """Guard: the helper accepts an underlying kwarg used by the roll-up tests above."""
    entry = _make_tradfi_entry(instrument_id="ESM6", instrument_type="FUTURE", venue="CME", underlying="ES")
    assert entry.underlying == "ES"


# ---------------------------------------------------------------------------
# ICE COMBO underlying-extraction gap (tradfi_manifest_cf4_source_and_cf7_
# phantom_gaps_2026_07_07.md) — ICE COMBO symbols carry extra whitespace +
# numeric spread ids instead of the standard letter+month-code shape, so the
# generic "-"-split fallback in _derive_underlying can't key them and they were
# dropped from the roll-up with a WARNING.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("instrument_id", "expected_root"),
    [
        ("BRN   3  30615524", "BRN"),
        ("G   FSF0032.M0032", "G"),
    ],
)
def test_derive_underlying_ice_combo_whitespace_symbol(instrument_id: str, expected_root: str) -> None:
    """A whitespace-delimited ICE COMBO leading token resolves via TRADFI_ROOTS."""
    assert enumerator_module._derive_underlying(instrument_id, "tradfi") == expected_root


def test_derive_underlying_ice_combo_unknown_root_returns_blank() -> None:
    """A leading token that isn't a registered TradFi root still returns "" (no mis-key)."""
    assert enumerator_module._derive_underlying("ZZZNOTAROOT 123 456", "tradfi") == ""


def test_derive_underlying_whitespace_fallback_is_tradfi_only() -> None:
    """The whitespace-token fallback is scoped to tradfi — a non-tradfi asset_group with no
    ``-`` separator still returns "" rather than risk mis-keying a cefi/defi bundle."""
    assert enumerator_module._derive_underlying("BRN   3  30615524", "cefi") == ""
    assert enumerator_module._derive_underlying("BRN   3  30615524") == ""


def test_rollup_bundle_grain_tradfi_ice_combo_no_underlying_column_recovers_via_symbol() -> None:
    """CF-minor finding: ICE COMBO leaves with a blank ``underlying`` catalogue column
    (the real-world shape — the store never populated it for these rows) now collapse
    into ONE synthetic combo entry instead of being dropped from the roll-up."""
    catalog = [
        _make_tradfi_entry(instrument_id="BRN   3  30615524", instrument_type="COMBO", venue="ICE"),
        _make_tradfi_entry(instrument_id="BRN   4  30615525", instrument_type="COMBO", venue="ICE"),
    ]
    rolled = enumerator_module._rollup_bundle_grain(catalog, "tradfi")
    synth = [e for e in rolled if e.instrument_type == "combo"]
    assert len(synth) == 1
    assert synth[0].instrument_id == "BRN"


# ---------------------------------------------------------------------------
# DeFi canonical venue/chain split — gotcha #3 (defi-canonical-naming-ssot.md)
#
# The instruments-service catalog stores legacy combined venue='AAVEV3-ARBITRUM'
# with blank chain=''. MTDS captures use canonical venue='AAVE_V3' + chain='ARBITRUM'.
# Fix: _enumerate_v2_defi must split the combined form and canonicalise the protocol
# token before emitting ExpectedRows or building the present_set row_key.
# ---------------------------------------------------------------------------


def _make_defi_legacy_entry(
    instrument_id: str = "ETH-USDC",
    instrument_type: str = "LENDING",
    legacy_venue: str = "AAVEV3-ARBITRUM",
    available_from: str | None = "2022-01-01",
    available_to: str | None = None,
) -> InstrumentCatalogEntry:
    """Catalog entry with legacy combined venue and blank chain — mirrors the real catalog shape."""
    return InstrumentCatalogEntry(
        instrument_id=instrument_id,
        instrument_type=instrument_type,
        venue=legacy_venue,
        chain="",  # blank: the catalog legacy form
        league_id="",
        available_from=available_from,
        available_to=available_to,
        market_created_at=None,
        settlement_time=None,
    )


def test_defi_v2_legacy_combined_venue_yields_canonical_venue_and_chain() -> None:
    """Core regression test: legacy venue='AAVEV3-ARBITRUM' + chain='' in the catalog
    must produce ExpectedRow.venue='AAVE_V3' + ExpectedRow.chain='ARBITRUM'.

    This is durable gotcha #3 from defi-canonical-naming-ssot.md: seeded
    expected_unattempted cells carried the combined PROTOCOL-CHAIN venue and blank
    chain, so they could never be converted to 'captured' by the MTDS writer.
    """
    catalog = [_make_defi_legacy_entry(legacy_venue="AAVEV3-ARBITRUM")]
    dates = _date_axis("2024-06-01")
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, dates, ["lending_indices"], present_set=set()))
    )
    assert len(rows) >= 1, "expected ≥1 expected_unattempted row for alive AAVE_V3/ARBITRUM"
    r = rows[0]
    assert r.venue == "AAVE_V3", (
        f"expected canonical venue='AAVE_V3', got '{r.venue}' — PROTOCOL-CHAIN combined form leaked through"
    )
    assert r.chain == "ARBITRUM", (
        f"expected chain='ARBITRUM', got '{r.chain}' — blank chain from catalog leaked through"
    )
    assert r.capture_status == "expected_unattempted"


def test_defi_v2_legacy_uniswapv3_ethereum_splits_canonical() -> None:
    """UNISWAPV3-ETHEREUM → venue='UNISWAP_V3' + chain='ETHEREUM'."""
    catalog = [
        InstrumentCatalogEntry(
            instrument_id="WETH-USDC-500",
            instrument_type="POOL",
            venue="UNISWAPV3-ETHEREUM",
            chain="",
            league_id="",
            available_from="2021-01-01",
            available_to=None,
            market_created_at=None,
            settlement_time=None,
        )
    ]
    dates = _date_axis("2024-06-01")
    rows = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_defi(
                catalog, dates, ["dex_pool_state", "dex_pool_swaps"], present_set=set()
            )
        )
    )
    assert rows, "expected ≥1 row for alive UNISWAP_V3/ETHEREUM"
    assert all(r.venue == "UNISWAP_V3" for r in rows), f"expected 'UNISWAP_V3', got {[r.venue for r in rows]}"
    assert all(r.chain == "ETHEREUM" for r in rows), f"expected 'ETHEREUM', got {[r.chain for r in rows]}"


def test_defi_v2_canonical_venue_present_set_suppresses_seed() -> None:
    """When the manifest present_set carries a canonical-form key (venue='AAVE_V3', chain='ARBITRUM'),
    the enumerator with legacy combined-venue catalog entry must match and suppress the seed.

    Before the fix: row_key used legacy 'AAVEV3-ARBITRUM' / '' so the key never matched the
    canonical captured row and the seed was always emitted (false EU).
    """
    catalog = [_make_defi_legacy_entry(legacy_venue="AAVEV3-ARBITRUM")]
    date_axis = _date_axis("2024-06-01")
    # The manifest has a captured row with canonical venue/chain (as MTDS writer produces).
    key = _row_key_from_dict(
        {
            "venue": "AAVE_V3",
            "chain": "ARBITRUM",
            "data_type": "lending_indices",
            "instrument_type": "lending",
            "instrument_id": "ETH-USDC",
            "league_id": "",
            "date": "2024-06-01",
        }
    )
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_defi(catalog, date_axis, ["lending_indices"], present_set={key}))
    )
    assert rows == [], (
        "canonical present_set key must suppress the legacy-catalog entry — row_key mismatch after the fix"
    )


def test_defi_v2_legacy_venue_pre_genesis_uses_split_chain() -> None:
    """Empty_confirmed (pre-genesis) branch: AAVEV3-ARBITRUM + date before ARBITRUM genesis
    must use the split chain='ARBITRUM' for the CHAIN_GENESIS_DATES lookup.

    Before the fix: chain_upper was '' (blank), so chain_genesis_ts was None and the
    pre-genesis branch never fired — pre-genesis dates were misclassified as alive.
    """
    # ARBITRUM genesis = 2021-08-31; use a clearly pre-genesis date.
    catalog = [_make_defi_legacy_entry(legacy_venue="AAVEV3-ARBITRUM", available_from="2022-01-01")]
    # Window includes an alive date (2022-06-01) so the overlap filter passes.
    dates = _date_axis("2020-01-01", "2022-06-01")
    rows = _drop_v2_venue_grain(list(enumerator_module._enumerate_v2_defi(catalog, dates, ["lending_indices"])))
    assert len(rows) == 1, "expected exactly 1 pre-genesis row"
    assert rows[0].reason == "EXPECTED_PRE_GENESIS_CHAIN", (
        f"pre-genesis branch did not fire; got reason='{rows[0].reason}' — "
        "chain was likely still blank (split chain not forwarded to CHAIN_GENESIS_DATES)"
    )
    assert rows[0].chain == "ARBITRUM"


# ---------------------------------------------------------------------------
# MVP capture-universe denominator gate (cefi_universe_capture_rule_2026_06_23)
# The expected_unattempted denominator = the perp-gated MVP universe, NOT the
# full IS catalogue. Out-of-MVP cells are NOT seeded.
# ---------------------------------------------------------------------------


def test_cefi_v2_mvp_gate_excludes_non_mvp_via_column() -> None:
    """A catalogue row tagged mvp=False is NOT seeded (excluded from denominator)."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE-FUTURES", mvp=False)]
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set()))
    )
    assert rows == []


def test_cefi_v2_mvp_gate_includes_mvp_via_column() -> None:
    """An mvp=True row with no manifest entry IS seeded expected_unattempted."""
    catalog = [_make_cefi_entry(available_from="2019-01-01", venue="BINANCE-FUTURES", mvp=True)]
    rows = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(catalog, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set()))
    )
    assert rows
    assert all(r.capture_status == "expected_unattempted" for r in rows)


def test_cefi_v2_mvp_gate_computes_predicate_when_column_absent() -> None:
    """mvp=None → the gate computes the shared predicate. Spot-with-no-perp is dropped;
    the same spot with a sibling perp in the catalog is seeded."""
    # SPOT BTC on BINANCE-SPOT with NO perp anywhere → dropped by the perp-gate.
    spot_only = [
        _make_cefi_entry(
            instrument_id="BTC-USDT",
            instrument_type="SPOT_PAIR",
            venue="BINANCE-SPOT",
            base_asset="BTC",
            mvp=None,
            available_from="2019-01-01",
        )
    ]
    rows_drop = _drop_v2_venue_grain(
        list(enumerator_module._enumerate_v2_cefi(spot_only, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set()))
    )
    assert rows_drop == []

    # Same spot + a sibling perp for BTC on the venue-family → seeded.
    spot_plus_perp = [
        _make_cefi_entry(
            instrument_id="BTC-USDT",
            instrument_type="SPOT_PAIR",
            venue="BINANCE-SPOT",
            base_asset="BTC",
            mvp=None,
            available_from="2019-01-01",
        ),
        _make_cefi_entry(
            instrument_id="BTC-PERP",
            instrument_type="PERPETUAL",
            venue="BINANCE-SPOT",
            base_asset="BTC",
            mvp=None,
            available_from="2019-01-01",
        ),
    ]
    rows_keep = _drop_v2_venue_grain(
        list(
            enumerator_module._enumerate_v2_cefi(
                spot_plus_perp, _date_axis("2023-06-01"), ["ohlcv_1d"], present_set=set()
            )
        )
    )
    spot_rows = [r for r in rows_keep if r.instrument_id == "BTC-USDT"]
    assert spot_rows  # spot now rides the perp-gate


# ---------------------------------------------------------------------------
# ARCX-primary ETF enumerator fix (nasdaq_nyse_eu_silent_skip plan P2)
# ---------------------------------------------------------------------------


def test_tradfi_v2_nyse_etf_alive_yields_empty_confirmed_delivery_lag() -> None:
    """NYSE ETFs in-window seed empty_confirmed(EXPECTED_SOURCE_DELIVERY_LAG).

    Databento XNYS.PILLAR (NYSE Primary) carries no ETF data — ETFs list on
    NYSE Arca (ARCX). The enumerator must pre-seed empty_confirmed so the
    denominator is not inflated by cells that can never be captured from
    XNYS.PILLAR. Mirrors the writer-side fix in MTDS (307ffa05).
    """
    catalog = [
        _make_tradfi_entry(instrument_id="SPY", instrument_type="ETF", venue="NYSE", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2026-01-02", "2026-01-05", "2026-01-06"),
            ["ohlcv_1m"],
            present_set=set(),
        )
    )
    assert len(rows) == 3, f"expected 3 alive-date rows (one per date), got {len(rows)}"
    for row in rows:
        assert row.capture_status == "empty_confirmed", f"expected empty_confirmed but got {row.capture_status}"
        assert row.reason == "EXPECTED_SOURCE_DELIVERY_LAG", f"wrong reason: {row.reason}"


def test_tradfi_v2_nasdaq_etf_alive_yields_expected_unattempted() -> None:
    """NASDAQ ETFs in-window seed expected_unattempted (no ARCX filter applies)."""
    catalog = [
        _make_tradfi_entry(instrument_id="QQQ", instrument_type="ETF", venue="NASDAQ", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2026-01-02"),
            ["ohlcv_1m"],
            present_set=set(),
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].reason == ""


def test_tradfi_v2_nyse_equity_alive_yields_expected_unattempted() -> None:
    """NYSE equities (non-ETF) in-window still seed expected_unattempted."""
    catalog = [
        _make_tradfi_entry(instrument_id="JPM", instrument_type="EQUITY", venue="NYSE", available_from="2020-01-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2026-01-02"),
            ["ohlcv_1m"],
            present_set=set(),
        )
    )
    assert len(rows) == 1
    assert rows[0].capture_status == "expected_unattempted"
    assert rows[0].reason == ""


def test_tradfi_v2_nyse_etf_pre_listing_still_yields_not_listed() -> None:
    """NYSE ETF pre-listing dates keep EXPECTED_INSTRUMENT_NOT_LISTED (beats ARCX check)."""
    catalog = [
        _make_tradfi_entry(instrument_id="SPY", instrument_type="ETF", venue="NYSE", available_from="2026-06-01")
    ]
    rows = list(
        enumerator_module._enumerate_v2_tradfi(
            catalog,
            _date_axis("2026-01-02", "2026-06-30"),
            ["ohlcv_1m"],
        )
    )
    pre_listing = [r for r in rows if r.date < "2026-06-01"]
    in_window = [r for r in rows if r.date >= "2026-06-01"]
    assert all(r.reason == "EXPECTED_INSTRUMENT_NOT_LISTED" for r in pre_listing)
    assert all(r.reason == "EXPECTED_SOURCE_DELIVERY_LAG" for r in in_window)


# ---------------------------------------------------------------------------
# Oscillation guard (2026-07-13): a seeder never emits empty_confirmed over a
# captured atom. Regression for the captured->empty_confirmed oscillation where
# the nightly expected-universe-v2-sports run re-stamped EXPECTED_PRE_SEASON /
# EXPECTED_POST_SEASON empty_confirmed rows over atoms whose data was captured
# and verified on disk (SEGUNDA_DIVISION/BRASILEIRAO footystats
# MATCHES/ODDS/PREDICTIONS, 21 atoms flipped on 2026-07-13).
# ---------------------------------------------------------------------------

_SPORTS_KEY_COLS: list[str] = ["data_type", "league_id", "date"]


def test_build_captured_set_restricts_to_captured_rows() -> None:
    """_build_captured_set keys ONLY capture_status=='captured' rows at present-cols grain."""
    df = pd.DataFrame(
        {
            "data_type": ["MATCHES", "MATCHES", "ODDS"],
            "league_id": ["SEGUNDA_DIVISION", "SEGUNDA_DIVISION", "BRASILEIRAO"],
            "date": ["2026-06-06", "2026-06-07", "2026-03-18"],
            "capture_status": ["captured", "empty_confirmed", "captured"],
        }
    )
    got = enumerator_module._build_captured_set(df, "sports")
    assert got == {
        ("MATCHES", "SEGUNDA_DIVISION", "2026-06-06"),
        ("ODDS", "BRASILEIRAO", "2026-03-18"),
    }


def test_build_captured_set_missing_capture_status_column_is_empty() -> None:
    df = pd.DataFrame({"data_type": ["MATCHES"], "league_id": ["EPL"], "date": ["2026-06-06"]})
    assert enumerator_module._build_captured_set(df, "sports") == set()


def test_oscillation_guard_drops_lifecycle_empty_over_captured_atom() -> None:
    """A pre-listing EXPECTED_INSTRUMENT_NOT_LISTED cell is suppressed when the
    SAME (data_type, league_id, date) atom already has a captured manifest row."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to="2024-01-15")]
    kwargs = {
        "asset_group": "sports",
        "catalog": catalog,
        "date_axis": _date_axis("2024-01-05", "2024-01-12"),
        "data_types": ["lineups"],
        "present_set": {("lineups", "EPL", "2024-01-12")},
        "present_cols": list(_SPORTS_KEY_COLS),
    }
    without_guard = list(enumerator_module.enumerate_v2(**kwargs))
    assert [r.reason for r in without_guard] == ["EXPECTED_INSTRUMENT_NOT_LISTED"]

    with_guard = list(enumerator_module.enumerate_v2(**kwargs, captured_set={("lineups", "EPL", "2024-01-05")}))
    assert with_guard == []


def test_oscillation_guard_drops_season_gate_empty_over_captured_atom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The per-day source-rule gate (EXPECTED_POST_SEASON et al) must not emit
    empty_confirmed for an atom with capture evidence — the exact 2026-07-13
    prod flip (SEGUNDA_DIVISION footystats MATCHES 2026-06-06)."""
    import unified_api_contracts.registry.sports_per_source_rules as rules_mod

    def _fake_is_expected_for_source(
        source: str, league_id: str, day: object, *, data_type: str = ""
    ) -> tuple[bool, str | None]:
        return (False, "EXPECTED_POST_SEASON")

    monkeypatch.setattr(rules_mod, "is_expected_for_source", _fake_is_expected_for_source)
    catalog = [_make_sports_entry(league_id="SEGUNDA_DIVISION", available_from="2018-01-01", available_to=None)]
    kwargs = {
        "asset_group": "sports",
        "catalog": catalog,
        "date_axis": _date_axis("2026-06-06"),
        "data_types": ["MATCHES"],
        "present_set": {("MATCHES", "SEGUNDA_DIVISION", "2026-06-06")},
        "present_cols": list(_SPORTS_KEY_COLS),
    }
    without_guard = [r for r in enumerator_module.enumerate_v2(**kwargs) if r.league_id == "SEGUNDA_DIVISION"]
    assert len(without_guard) == 1
    assert without_guard[0].capture_status == "empty_confirmed"
    assert without_guard[0].reason == "EXPECTED_POST_SEASON"

    with_guard = [
        r
        for r in enumerator_module.enumerate_v2(**kwargs, captured_set={("MATCHES", "SEGUNDA_DIVISION", "2026-06-06")})
        if r.league_id == "SEGUNDA_DIVISION"
    ]
    assert with_guard == []


def test_oscillation_guard_leaves_non_captured_atoms_untouched() -> None:
    """The guard only drops empty_confirmed rows whose OWN atom is captured —
    sibling dates/atoms keep their rows, and expected_unattempted seeding is
    never affected (that stays present_set-gated as before)."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to="2024-01-15")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=_date_axis("2024-01-05", "2024-01-06", "2024-01-12"),
            data_types=["lineups"],
            present_set=set(),
            present_cols=list(_SPORTS_KEY_COLS),
            captured_set={("lineups", "EPL", "2024-01-05")},
        )
    )
    by_date = {r.date: r for r in rows}
    assert "2024-01-05" not in by_date  # captured atom: empty_confirmed suppressed
    assert by_date["2024-01-06"].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"  # sibling kept
    assert by_date["2024-01-12"].capture_status == "expected_unattempted"  # seeding kept


def test_oscillation_guard_inert_without_present_cols() -> None:
    """captured_set without present_cols cannot key rows — guard must be inert."""
    catalog = [_make_sports_entry(available_from="2024-01-10", available_to="2024-01-15")]
    rows = list(
        enumerator_module.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=_date_axis("2024-01-05", "2024-01-12"),
            data_types=["lineups"],
            captured_set={("lineups", "EPL", "2024-01-05")},
        )
    )
    assert [r.reason for r in rows] == ["EXPECTED_INSTRUMENT_NOT_LISTED"]
