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
    massive stays secondary fallback ([1]) in SOURCE_PRIORITY."""
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
