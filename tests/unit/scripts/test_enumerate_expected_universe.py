"""Unit tests — Phase 3.D.4 enumerate_expected_universe.py.

Tests cover the shared present-set / row-key / bucket-resolution / MVP-gate
helpers. The v1 venue-grain per-asset-group enumerators (`_enumerate_tradfi`
etc.) were retired 2026-07-09 per
`plans/active/issues/v1_enumerator_dispatch_not_deletable_2026_07_06.md`; their
v2 per-instrument-grain equivalents (`_enumerate_v2_*`) are covered by
`tests/unit/scripts/test_enumerate_expected_universe_v2.py`, including the
closed-set-reasons + dispatch-table-completeness invariants this file used to
assert against `_ENUMERATORS`. They DO NOT touch the network or GCS — pure
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
    # long-form ``market-data-tick-prediction-<pid>`` — deleted 2026-07-12 (404 now;
    # see mdps_prediction_tick_bucket_uac_ssot_404_2026_07_14.md), pred-prd is the sole live SSOT.
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


def test_row_data_types_aster_capability_profile() -> None:
    """ASTER capability profile after uac@3652f99f (cefi-008 live-wire flip 2026-07-07):
    book_snapshot_5 IS a declared capability (live-only from 2026-06-23 via
    aster_book_liq_ws) and IS in MVP scope, so the enumerator seeds it;
    liquidations remains carved out at the MVP-scope layer (capability present
    but not in MVP for ASTER). Retains the historical guard that survivors ⊆ capabilities."""
    from unified_api_contracts.registry import VENUE_DATA_TYPE_CAPABILITIES

    cefi_dts = ["trades", "book_snapshot_5", "derivative_ticker", "liquidations", "perp_funding"]
    row_dts = enumerator_module._row_data_types("cefi", _entry("ASTER", "PERPETUAL"), cefi_dts)
    assert "book_snapshot_5" in row_dts, (
        "ASTER book_snapshot_5 is a live-wire capability (from 2026-06-23) and must be seeded"
    )
    assert "liquidations" not in row_dts, "ASTER liquidations is not in MVP scope"
    # What survives is exactly the venue's declared capability ∩ validity ∩ MVP.
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
    row_dts_futures = enumerator_module._row_data_types("cefi", _entry("DERIBIT", "futures_chain"), cefi_dts)
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
