"""Unit tests — #4: enumerate_expected_universe stamps pipeline_mode + source +
transport on seeded rows.

``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05`` (#4) +
``master_data_canonicalisation_migration_catalogue_2026_06_07`` (C-#2/C-TRANSPORT):
the ``expected_unattempted`` / ``empty_confirmed`` denominator seeds the v2
enumerator materialises MUST carry the same ``pipeline_mode`` + ``source`` (+
``transport``) as the real rows they reconcile against — derived from the cell's
primary EXTERNAL source in UAC ``SOURCE_PRIORITY``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_enumerator_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_provenance_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()
_derive = enumerator_module._derive_pm_source_transport


def test_tradfi_trades_seed_carries_databento_batch_rest() -> None:
    """TradFi trades primary external source = databento → batch_databento / rest
    (DATABENTO-FIRST per operator 2026-06-24; supersedes 2026-06-11 massive-first).
    massive was dropped from SOURCE_PRIORITY entirely 2026-07-19
    (unified-api-contracts@a2beed46) — databento is the sole tradfi source."""
    pm, source, transport = _derive("tradfi", "trades")
    assert pm == "batch_databento"
    assert source == "databento"
    assert transport == "rest"


def test_cefi_trades_seed_carries_tardis_batch_flat_file() -> None:
    """CeFi batch source = tardis → batch_tardis / flat_file (T+1 archive)."""
    pm, source, transport = _derive("cefi", "trades")
    assert pm == "batch_tardis"
    assert source == "tardis"
    assert transport == "flat_file"


def test_defi_perp_funding_seed_carries_hyperliquid_batch_rest() -> None:
    """DeFi perp_funding source = hyperliquid (NOT hyperliquid_rest) → batch_hyperliquid / rest."""
    pm, source, transport = _derive("defi", "perp_funding")
    assert pm == "batch_hyperliquid"
    assert source == "hyperliquid"
    assert transport == "rest"


def test_seed_pm_and_source_are_non_blank_for_registered_cells() -> None:
    """Every registered external cell yields a non-blank (pm, source, transport)."""
    for ag, dt in (("tradfi", "trades"), ("cefi", "trades"), ("defi", "perp_funding")):
        pm, source, transport = _derive(ag, dt)
        assert pm, f"blank pipeline_mode for {(ag, dt)}"
        assert source, f"blank source for {(ag, dt)}"
        assert transport, f"blank transport for {(ag, dt)}"


def test_unregistered_cell_with_no_writer_fallback_is_exempt_blank() -> None:
    """A cell whose asset_group has NO writer-level fallback (genuinely
    computed/service-only, e.g. calendar features) stays ("", "", "") (exempt)."""
    assert _derive("calendar", "totally_not_a_registered_data_type") == ("", "", "")
    assert _derive("features", "totally_not_a_registered_data_type") == ("", "", "")
    assert _derive("", "") == ("", "", "")


def test_unregistered_cell_with_writer_fallback_uses_asset_group_default() -> None:
    """CF-3 fix (tradfi_manifest_cf4_source_and_cf7_phantom_gaps, 2026-07-08): a cell
    with no SOURCE_PRIORITY entry but whose asset_group HAS a real-writer fallback
    (UTL ``derive_pipeline_mode_for_row`` / ``_ASSET_GROUP_FALLBACKS``) must NOT stay
    blank — the real capture writer (MTDS ``_resolve_pipeline_mode_for_sentinel``)
    stamps a concrete pipeline_mode for the same cell via that same fallback, so a
    blank seed permanently diverges from the real row it's meant to reconcile
    against. Mirrors the real bug: tradfi ``mbp_10`` / ``corporate_action_confirmed``
    / ``earnings_result`` / ``macro_result`` are genuinely Databento-sourced but were
    never registered in SOURCE_PRIORITY."""
    pm, source, transport = _derive("defi", "totally_not_a_registered_data_type")
    assert pm == "batch_onchain_rpc"
    assert source == "onchain_rpc"
    assert transport == "rest"


def test_tradfi_mbp_10_seed_carries_databento_via_writer_fallback() -> None:
    """CF-3 regression: mbp_10 has no ``("tradfi", "mbp_10")`` SOURCE_PRIORITY entry
    (genuine registry gap — it IS Databento-fetched per ``databento_fetch.py``'s
    schema_map) — the seed must fall back to the same ``batch_databento`` the real
    writer stamps, not blank."""
    pm, source, transport = _derive("tradfi", "mbp_10", venue="NASDAQ")
    assert pm == "batch_databento"
    assert source == "databento"
    assert transport == "rest"


def test_tradfi_corporate_action_and_macro_seeds_use_databento_fallback() -> None:
    """CF-3 regression: corporate_action_confirmed / earnings_result / macro_result
    have no SOURCE_PRIORITY entry for tradfi either — same fallback applies."""
    for dt in ("corporate_action_confirmed", "earnings_result", "macro_result"):
        pm, source, transport = _derive("tradfi", dt, venue="NYSE")
        assert pm == "batch_databento", dt
        assert source == "databento", dt
        assert transport == "rest", dt


def test_kalshi_scaffold_rows_carry_kalshi_provenance_not_polymarket() -> None:
    """KALSHI scaffold-provenance fix (2026-08-03,
    prediction_phantom_reconciler_wipes_bundle_atom_2026_07_10.md todo 3):
    ``SOURCE_PRIORITY[("prediction", <data_type>)]`` lists Polymarket sources FIRST
    (read-time priority), so the venue-blind ``external[0]`` branch was stamping
    every KALSHI scaffold row with Polymarket's provenance. venue="KALSHI" must
    resolve to batch_kalshi/kalshi for every real prediction data_type, including
    market_lifecycle (whose SOURCE_PRIORITY entry is polymarket_gamma_api-only)."""
    for dt in ("trades", "book_snapshot_5", "prediction_canonical_question_group", "market_lifecycle"):
        pm, source, transport = _derive("prediction", dt, venue="KALSHI")
        assert pm == "batch_kalshi", dt
        assert source == "kalshi", dt
        assert transport, dt


def test_kalshi_scaffold_rows_case_and_upper_lower_data_type_variants() -> None:
    """Sports-style upper/lower data_type variants (module docstring: 'Sports
    data_types are registered upper-case ... so both are tried') must not affect
    the KALSHI venue-first resolution."""
    pm, source, _transport = _derive("prediction", "MARKET_LIFECYCLE", venue="KALSHI")
    assert pm == "batch_kalshi"
    assert source == "kalshi"


def test_polymarket_scaffold_rows_are_unchanged_by_the_kalshi_fix() -> None:
    """The venue-first branch is a provable safe superset for POLYMARKET: it is
    deliberately absent from ``_VENUE_OVERRIDES`` (multi-source venue), so
    ``derive_pipeline_mode_for_row`` falls through to the SAME SOURCE_PRIORITY
    lookup ``external[0]`` already used — zero behavior change."""
    pm, source, transport = _derive("prediction", "trades", venue="POLYMARKET")
    assert pm == "batch_polymarket_clob"
    assert source == "polymarket_clob"
    assert transport

    pm, source, _transport = _derive("prediction", "market_lifecycle", venue="POLYMARKET")
    assert pm == "batch_polymarket_gamma_api"
    assert source == "polymarket_gamma_api"


def test_prediction_seed_with_no_venue_falls_back_to_source_priority_unchanged() -> None:
    """A venue-blank prediction seed (defensive — real prediction rows always carry
    a venue) must keep the pre-fix SOURCE_PRIORITY[0] behavior, not blank out."""
    pm, source, _transport = _derive("prediction", "trades")
    assert pm == "batch_polymarket_clob"
    assert source == "polymarket_clob"


def test_non_prediction_asset_groups_are_unreachable_by_the_kalshi_branch() -> None:
    """The new venue-first branch is gated on ``asset_group == "prediction"`` only
    — every other asset_group's scaffold-row stamping is BYTE-IDENTICAL to before
    the fix (same code path, unreachable new branch). Spot-check a venue that IS in
    ``_VENUE_OVERRIDES`` (IBKR) to prove the SOURCE_PRIORITY-first behavior for
    tradfi is untouched."""
    pm, source, transport = _derive("tradfi", "ohlcv_1d", venue="IBKR")
    assert pm == "batch_fred"
    assert source == "fred"
    assert transport
