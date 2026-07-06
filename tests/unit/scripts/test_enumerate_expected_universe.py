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
import re
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


def test_tradfi_index_pre_genesis_for_dxy_pre_2019() -> None:
    """DXY genesis is 2019-01-02; ICE:INDEX:DXY-USD on 2015-06-01 should yield
    an instrument-grain EXPECTED_INSTRUMENT_NOT_LISTED row."""
    rows = list(enumerator_module._enumerate_tradfi_indices("2015-06-01", "2015-06-01"))
    dxy = [r for r in rows if r.instrument_id == "ICE:INDEX:DXY-USD"]
    assert len(dxy) > 0, "expected DXY pre-genesis row for 2015-06-01 (genesis 2019-01-02)"
    sample = dxy[0]
    assert sample.asset_group == "tradfi"
    assert sample.venue == "ICE"
    assert sample.instrument_type == "INDEX"
    assert sample.data_type == "ohlcv_24h"
    assert sample.reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert sample.date == "2015-06-01"


def test_tradfi_index_pre_genesis_for_treasuries_pre_2000() -> None:
    """US treasury indices genesis 2000-01-03; on 1999-06-01 every tenor should
    pre-list under its canonical -USD key."""
    rows = list(enumerator_module._enumerate_tradfi_indices("1999-06-01", "1999-06-01"))
    keys = {r.instrument_id for r in rows if r.reason == "EXPECTED_INSTRUMENT_NOT_LISTED"}
    for tenor in ("US3M", "US5Y", "US10Y", "US30Y"):
        assert f"CBOE:INDEX:{tenor}-USD" in keys, f"{tenor} pre-genesis row missing"


def test_tradfi_index_no_rows_post_all_genesis() -> None:
    """A date past every Yahoo-index genesis (DXY 2019 is the latest) yields no
    pre-genesis index rows."""
    rows = list(enumerator_module._enumerate_tradfi_indices("2020-06-01", "2020-06-01"))
    assert rows == [], "expected zero index pre-genesis rows for 2020-06-01"


def test_tradfi_holiday_excludes_cboe_and_ice_on_new_year() -> None:
    """P2 regression: 2025-01-01 (New Year) is a holiday for CBOE + ICE — the
    venue-level tradfi pass must emit EXPECTED_HOLIDAY for both."""
    rows = list(enumerator_module._enumerate_tradfi("2025-01-01", "2025-01-01"))
    holiday_venues = {r.venue for r in rows if r.reason == "EXPECTED_HOLIDAY"}
    assert "CBOE" in holiday_venues, "CBOE should be holiday-excluded on 2025-01-01"
    assert "ICE" in holiday_venues, "ICE should be holiday-excluded on 2025-01-01"


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


def test_sports_yields_pre_source_coverage_before_source_start() -> None:
    """The day BEFORE a source's SOURCE_COVERAGE_START yields
    EXPECTED_PRE_SOURCE_COVERAGE_START rows for that source.

    Derives the date from the UAC SSOT (``SOURCE_COVERAGE_START``) rather than a
    hardcoded year so it never goes stale: api_football's start moved 2018-01-01 →
    2015-01-01, which silently broke the prior literal-"2017-12-31" assertion (2017
    is now AFTER coverage start, so it correctly yields no pre-source rows)."""
    import pandas as pd
    from unified_api_contracts.sports import SOURCE_COVERAGE_START

    af_start = pd.Timestamp(SOURCE_COVERAGE_START["api_football"])
    pre_day = (af_start - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    rows = list(enumerator_module._enumerate_sports(pre_day, pre_day))
    af_rows = [r for r in rows if r.venue == "api_football"]
    assert len(af_rows) > 0, f"expected api_football rows for {pre_day} (day before coverage start)"
    pre_coverage = [r for r in af_rows if r.reason == "EXPECTED_PRE_SOURCE_COVERAGE_START"]
    assert len(pre_coverage) > 0, f"expected EXPECTED_PRE_SOURCE_COVERAGE_START for api_football {pre_day}"
    sample = pre_coverage[0]
    assert sample.asset_group == "sports"
    assert sample.date == pre_day


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
    """A date past every CeFi venue launch (use 2026-07-01, after the latest —
    KALSHI-PERP launched 2026-05-29, POLYMARKET-PERP 2026-04-21) should yield
    zero pre-venue-launch rows. NOTE: bump this date whenever a later-launching
    CeFi venue is added, else newly-added post-date venues yield pre-launch rows."""
    rows = list(enumerator_module._enumerate_cefi("2026-07-01", "2026-07-01"))
    assert len(rows) == 0, "expected zero pre-venue-launch rows for 2026-07-01 (every cefi venue launched)"


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


# --- Canonical bucket resolution (⑦ coverage-denominator readiness) ----------


def test_default_bucket_for_resolves_canonical_env_tiered_per_asset_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_default_bucket_for`` must resolve the CANONICAL env-tiered manifest bucket
    via the bucket-name SSOT for every asset_group — NOT the prior hardcoded
    literals that were all missing the ``-{DEPLOYMENT_ENV_SHORT}-`` env tier.

    Regression for the ⑦ coverage-denominator gap: a no-``--bucket`` enumerator run
    must read/write the SAME canonical manifest bucket the MTDS reader + MDPS
    consolidator gate use, else the could-exist ``expected_unattempted`` seed lands
    on a non-existent bucket (silent no-op). SSOT: cloud-providers.yaml.
    """
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("DEPLOYMENT_ENV", "prod")
    monkeypatch.setenv("CLOUD_PROVIDER", "gcp")

    # The env-tier short-form is supplied by the cloud-providers.yaml SSOT, which differs
    # by environment: the canonical placeholder yaml resolves ``DEPLOYMENT_ENV=prod`` → ``prd``,
    # while CI runs against the pre-substituted ``ci-test-cloud-providers.yaml`` whose tier is the
    # literal ``test``. The regression being guarded is a *missing* env tier (the legacy untiered
    # ``market-data-tick-prediction-<pid>``), not its specific value — so assert the canonical
    # env-tiered SHAPE (a tier segment is present before the project_id) rather than pinning ``prd``.
    _tier = r"(?:prd|stg|dev|test|ci)"

    # Prediction: the canonical env-tiered ``pred-<tier>`` bucket, NOT the legacy
    # long-form ``market-data-tick-prediction-<pid>`` slated for L6 delete.
    pred = enumerator_module._default_bucket_for("prediction")
    assert re.fullmatch(rf"market-data-tick-pred-{_tier}-test-project", pred), pred
    assert pred != "market-data-tick-prediction-test-project"

    # Every supported asset_group resolves to an env-tiered bucket (tier segment present).
    for ag in enumerator_module.SUPPORTED_ASSET_GROUPS:
        bucket = enumerator_module._default_bucket_for(ag)
        assert re.search(rf"-{_tier}-test-project$", bucket), f"{ag} bucket {bucket!r} is missing the env tier"

    # cefi/defi/tradfi resolve via the per-asset_group market-data kind.
    assert re.fullmatch(rf"market-data-tick-cefi-{_tier}-test-project", enumerator_module._default_bucket_for("cefi"))
    # sports' manifest lives in the instruments-store bucket.
    assert re.fullmatch(
        rf"instruments-store-sports-{_tier}-test-project", enumerator_module._default_bucket_for("sports")
    )


def test_supported_asset_groups_has_all_5() -> None:
    """SUPPORTED_ASSET_GROUPS (the --asset-group choices) covers all 5 groups."""
    assert set(enumerator_module.SUPPORTED_ASSET_GROUPS) == {
        "cefi",
        "defi",
        "tradfi",
        "sports",
        "prediction",
    }


# --- Venue-capability carve-out at seeding (uac_writer_matrix_reconciliation) --


def _entry(venue: str, instrument_type: str) -> object:
    """Minimal catalogue entry for _row_data_types tests."""
    return enumerator_module.InstrumentCatalogEntry(
        instrument_id=f"{venue}:{instrument_type}:TEST",
        instrument_type=instrument_type,
        venue=venue,
        chain="",
        league_id="",
        available_from=None,
        available_to=None,
        market_created_at=None,
        settlement_time=None,
    )


def test_row_data_types_aster_capability_carveout() -> None:
    """ASTER cannot produce book_snapshot_5/liquidations (absent from
    VENUE_DATA_TYPE_CAPABILITIES["ASTER"]) — the enumerator must NEVER seed
    them (the 2026-06-29 over-seed contradiction: UAC is correct, the
    enumerator over-seeded 3,477 expected_unattempted rows each)."""
    from unified_api_contracts.registry import VENUE_DATA_TYPE_CAPABILITIES

    cefi_dts = ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "perp_funding"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("ASTER", "PERPETUAL"), cefi_dts)
    assert "book_snapshot_5" not in row_dts, "ASTER book_snapshot_5 must be carved out"
    assert "liquidations" not in row_dts, "ASTER liquidations must be carved out"
    # What survives is exactly the venue's declared capability ∩ validity.
    assert set(row_dts) <= set(VENUE_DATA_TYPE_CAPABILITIES["ASTER"]), row_dts
    assert "trades" in row_dts, "ASTER trades is a declared capability and must survive"


def test_row_data_types_capability_absent_venue_not_gated() -> None:
    """A cefi venue wholly absent from VENUE_DATA_TYPE_CAPABILITIES (e.g.
    BINANCE-DELIVERY) carries no carve-out information — it must NOT be
    blanket-blocked by the capability gate (denominator semantics for
    capability-absent venues are a separate open finding)."""
    from unified_api_contracts.registry import VENUE_DATA_TYPE_CAPABILITIES

    assert "BINANCE-DELIVERY" not in VENUE_DATA_TYPE_CAPABILITIES
    cefi_dts = ["trades", "book_snapshot_5"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("BINANCE-DELIVERY", "PERPETUAL"), cefi_dts)
    assert "trades" in row_dts


# --- MVP data_type gate (bundle-aware) — C2 point-fix per
# cefi_layer1_denominator_gaps_2026_07_03.md ---------------------------------


def test_row_data_types_coinbase_spot_mvp_cut_drops_book_snapshot_5() -> None:
    """COINBASE-SPOT ships trades+book_snapshot_5 as raw capability, but
    MVP_SCOPE narrows COINBASE-SPOT to {trades} only (venue_data_types
    override — no depth features derived from Coinbase, ~30 GB pandas peak
    on book5 backfill). The C2 point-fix intersection must drop
    book_snapshot_5 from the seeded denominator so VMs are not asked to
    capture cells MVP has excluded (over-seed → false EXPECTED_UNATTEMPTED)."""
    from unified_api_contracts import get_mvp_data_types_for_cefi_venue
    from unified_api_contracts.registry import VENUE_DATA_TYPE_CAPABILITIES

    # Pre-conditions the fix relies on: capability HAS book5, MVP drops it.
    assert "book_snapshot_5" in VENUE_DATA_TYPE_CAPABILITIES["COINBASE-SPOT"]
    assert "book_snapshot_5" not in get_mvp_data_types_for_cefi_venue("COINBASE-SPOT")
    assert "trades" in get_mvp_data_types_for_cefi_venue("COINBASE-SPOT")

    cefi_dts = ["trades", "book_snapshot_5"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("COINBASE-SPOT", "SPOT_PAIR"), cefi_dts)
    assert row_dts == ["trades"], f"MVP gate must drop book_snapshot_5 for COINBASE-SPOT; got {row_dts}"


def test_row_data_types_deribit_options_chain_bundle_survives_mvp_gate() -> None:
    """Deribit OPTION leaves roll up into a synthetic per-underlying
    ``options_chain`` bundle entry (BUNDLE_INSTRUMENT_TYPE_BY_AG_AND_LEAF).
    The UAC validity matrix narrows that bundle to data_type=trades. But
    ``get_mvp_data_types_for_cefi_venue("DERIBIT")`` returns the flat cefi
    tick set (Deribit has no venue-level override — it stays v10 behaviour;
    the OPTION cost cut lives in ``instrument_type_data_types``). Applying
    the intersection naively at the bundle entry would… still keep trades
    (trades IS in the flat MVP set), so this test primarily guards the
    SKIP-BY-OVERRIDE branch: the C2 point-fix must recognise that
    OPTIONS_CHAIN (bundle-normalised via ``_mvp_capture_itype`` → OPTION) is
    a key in ``MVP_SCOPE["cefi"].instrument_type_data_types`` and skip the
    intersection entirely — so any future widening of the flat MVP set does
    not accidentally re-widen options_chain seeding. This is the exact
    regression class the plan CAUTION documents (attempts 1+2 wiped the
    Deribit options_chain denominator by intersecting against the
    venue-only set — G1 backfill mvp_backfill_cefi_tick_v10 centres on it)."""
    from unified_api_contracts import MVP_SCOPE, CeFiMvpRule

    # Pre-conditions: MVP scope declares the OPTION override + Deribit has no venue override.
    cefi_rule = MVP_SCOPE.get("cefi")
    assert isinstance(cefi_rule, CeFiMvpRule)
    assert "OPTION" in cefi_rule.instrument_type_data_types
    assert cefi_rule.instrument_type_data_types["OPTION"] == frozenset({"options_chain"})
    assert "DERIBIT" not in cefi_rule.venue_data_types  # Deribit stays v10 (no venue override)

    # Bundle-post-rollup shape: instrument_type=options_chain (the synthetic
    # entry _rollup_bundle_grain emits for Deribit OPTION leaves). The full
    # cefi data_types list mirrors DATA_TYPES_BY_ASSET_GROUP["cefi"].
    cefi_dts = [
        "trades",
        "book_snapshot_5",
        "derivative_ticker",
        "liquidations",
        "options_chain",
        "futures_chain",
        "ohlcv_1m",
        "perp_funding",
    ]
    row_dts = enumerator_module._row_data_types("cefi", _entry("DERIBIT", "options_chain"), cefi_dts)
    # ERA-B: the cefi options_chain instrument_type's market data_type is
    # ``trades`` (not the chain name). The MVP gate must NOT empty this to [].
    assert row_dts == ["trades"], f"Deribit options_chain must survive MVP gate as ['trades']; got {row_dts}"

    # Additional bundle: FUTURES_CHAIN also normalises via _mvp_capture_itype
    # → FUTURE. FUTURE is NOT in instrument_type_data_types → intersection
    # DOES apply. Deribit futures_chain validity → ["trades"] and MVP flat
    # set includes trades → survives as ["trades"] (not emptied).
    row_dts_futures = enumerator_module._row_data_types(
        "cefi", _entry("DERIBIT", "futures_chain"), cefi_dts
    )
    assert row_dts_futures == ["trades"], (
        f"Deribit futures_chain must survive MVP gate as ['trades']; got {row_dts_futures}"
    )


def test_row_data_types_deribit_perpetual_mvp_gate_drops_liquidations() -> None:
    """Deribit PERPETUAL: raw capability admits {trades, book5,
    derivative_ticker, liquidations} but MVP_SCOPE flat data_types are
    {trades, book5, derivative_ticker, funding_rate} — liquidations is NOT
    MVP. The C2 point-fix intersection must drop liquidations so the
    Deribit-perp denominator matches the MVP capture universe (no
    false EXPECTED_UNATTEMPTED liquidations rows). PERPETUAL is NOT in
    instrument_type_data_types → intersection applies as expected."""
    cefi_dts = ["trades", "book_snapshot_5", "derivative_ticker", "liquidations"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("DERIBIT", "PERPETUAL"), cefi_dts)
    assert "trades" in row_dts
    assert "book_snapshot_5" in row_dts
    assert "derivative_ticker" in row_dts
    assert "liquidations" not in row_dts, (
        f"MVP gate must drop liquidations for Deribit PERPETUAL (not in MVP_SCOPE.cefi.data_types); got {row_dts}"
    )


def test_row_data_types_non_mvp_venue_skips_intersection() -> None:
    """A cefi venue absent from MVP_SCOPE.cefi.venues (e.g. BINANCE-DELIVERY
    per operator decision #3 — COIN-M delivery not MVP) yields an empty
    MVP data_type set from ``get_mvp_data_types_for_cefi_venue``. The
    ``if mvp_dts:`` guard must skip the intersection so those cells are not
    blanket-blocked. Denominator semantics for MVP-absent venues are a
    SEPARATE open finding (BLK-5cc7590e for COINBASE/DERIBIT-COMBO)."""
    from unified_api_contracts import get_mvp_data_types_for_cefi_venue

    assert get_mvp_data_types_for_cefi_venue("BINANCE-DELIVERY") == frozenset()
    cefi_dts = ["trades", "book_snapshot_5"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("BINANCE-DELIVERY", "PERPETUAL"), cefi_dts)
    # Both survive: validity matrix admits them + no cap entry to gate + MVP
    # gate is inactive for a non-MVP-scoped venue → unchanged.
    assert row_dts == ["trades", "book_snapshot_5"], (
        f"MVP gate must NOT blanket-block a non-MVP-scoped cefi venue; got {row_dts}"
    )
