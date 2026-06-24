"""Unit tests for reconcile_defi_pool_manifest_dual_form_2026_06_23.py.

Credential-free + GCS-free — exercises the pure ``reconcile_frame`` + the
``_classify_pool_id`` / ``_canonical_address_from_glued_0x`` helpers (operator
Refinement 1 + 2 — converge every POOL manifest row onto the canonical
``pool_address.lower()`` form).

Plan: defi_instrument_catalogue_and_capture_pipeline_2026_06_23.md (Phase F).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_defi_pool_manifest_dual_form_2026_06_23.py"
    module_name = "_reconcile_defi_pool_manifest_dual_form_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def recon() -> ModuleType:
    return _load_script()


_COLS = [
    "date",
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "capture_status",
    "error_reason",
    "row_count",
]


def _row(**kw: object) -> dict[str, object]:
    base = dict.fromkeys(_COLS, "")
    base["row_count"] = 0
    base.update(kw)
    return base


class TestClassifyPoolId:
    def test_canonical_0x(self, recon: ModuleType) -> None:
        assert recon._classify_pool_id("0x88e6a0c2") == "canonical_0x"

    def test_glued_venuechain_0x(self, recon: ModuleType) -> None:
        assert recon._classify_pool_id("UNISWAP_V3-ETHEREUM:POOL:0xF9188AFF") == "glued_venuechain_0x"

    def test_glued_pair(self, recon: ModuleType) -> None:
        assert recon._classify_pool_id("UNISWAPV3-OPTIMISM:POOL:WSTETH-RSETH:500") == "glued_pair"


class TestCanonicalAddressExtraction:
    def test_extracts_lowercased_address(self, recon: ModuleType) -> None:
        assert recon._canonical_address_from_glued_0x("UNISWAP_V3-ETHEREUM:POOL:0xF9188AFF") == "0xf9188aff"

    def test_returns_none_for_pair_form(self, recon: ModuleType) -> None:
        assert recon._canonical_address_from_glued_0x("UNISWAPV3-OPT:POOL:AAVE-USDC:100") is None


class TestReconcileFrame:
    def test_rekeys_glued_0x_drops_glued_pair_keeps_canonical(self, recon: ModuleType) -> None:
        df = pd.DataFrame(
            [
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="ARBITRUM",
                    data_type="dex_pool_state",
                    instrument_type="pool",
                    instrument_id="0xaaa",
                    capture_status="captured",
                    row_count=10,
                ),
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="ARBITRUM",
                    data_type="dex_pool_state",
                    instrument_type="POOL",
                    instrument_id="UNISWAP_V3-ARBITRUM:POOL:0xBBB",
                    capture_status="captured",
                    row_count=5,
                ),
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="ARBITRUM",
                    data_type="dex_pool_state",
                    instrument_type="POOL",
                    instrument_id="UNISWAPV3-ARBITRUM:POOL:AAVE-USDC:100",
                    capture_status="empty_confirmed",
                    error_reason="EXPECTED_INSTRUMENT_DELISTED",
                ),
                _row(
                    date="2026-06-22",
                    venue="AAVE_V3",
                    chain="ARBITRUM",
                    data_type="lending_indices",
                    instrument_type="lending",
                    instrument_id="0xccc",
                    capture_status="captured",
                    row_count=3,
                ),
            ],
            columns=_COLS,
        )
        out, stats = recon.reconcile_frame(df)
        ids = sorted(out["instrument_id"].tolist())
        assert ids == ["0xaaa", "0xbbb", "0xccc"]
        assert stats["rekeyed_glued_0x"] == 1
        assert stats["deleted_glued_pair"] == 1
        pool_itypes = out[out["data_type"] == "dex_pool_state"]["instrument_type"].tolist()
        assert all(t == "pool" for t in pool_itypes)

    def test_dedups_glued_0x_when_bare_already_present(self, recon: ModuleType) -> None:
        """A glued_venuechain_0x row whose bare-0x twin already exists on the same cell is dropped."""
        df = pd.DataFrame(
            [
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="BASE",
                    data_type="dex_pool_swaps",
                    instrument_type="pool",
                    instrument_id="0xdup",
                    capture_status="captured",
                    row_count=2,
                ),
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="BASE",
                    data_type="dex_pool_swaps",
                    instrument_type="POOL",
                    instrument_id="UNISWAP_V3-BASE:POOL:0xDUP",
                    capture_status="captured",
                    row_count=2,
                ),
            ],
            columns=_COLS,
        )
        out, stats = recon.reconcile_frame(df)
        assert out["instrument_id"].tolist() == ["0xdup"]
        assert stats["dropped_dup_after_rekey"] == 1

    def test_idempotent(self, recon: ModuleType) -> None:
        df = pd.DataFrame(
            [
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="ARBITRUM",
                    data_type="dex_pool_state",
                    instrument_type="POOL",
                    instrument_id="UNISWAP_V3-ARBITRUM:POOL:0xBBB",
                    capture_status="captured",
                    row_count=5,
                ),
                _row(
                    date="2026-06-22",
                    venue="UNISWAP_V3",
                    chain="ARBITRUM",
                    data_type="dex_pool_state",
                    instrument_type="POOL",
                    instrument_id="UNISWAPV3-ARBITRUM:POOL:AAVE-USDC:100",
                    capture_status="empty_confirmed",
                    error_reason="EXPECTED_INSTRUMENT_DELISTED",
                ),
            ],
            columns=_COLS,
        )
        out1, _ = recon.reconcile_frame(df)
        out2, stats2 = recon.reconcile_frame(out1)
        assert stats2["rekeyed_glued_0x"] == 0
        assert stats2["deleted_glued_pair"] == 0
        assert out1["instrument_id"].tolist() == out2["instrument_id"].tolist()

    def test_empty_frame_noop(self, recon: ModuleType) -> None:
        out, stats = recon.reconcile_frame(pd.DataFrame(columns=_COLS))
        assert out.empty
        assert stats["rekeyed_glued_0x"] == 0

    def test_captured_glued_pair_kept(self, recon: ModuleType) -> None:
        """A *captured* glued_pair row (real data, no recoverable 0x) is KEPT, never dropped."""
        df = pd.DataFrame(
            [
                _row(
                    date="2026-06-22",
                    venue="CURVE",
                    chain="ETHEREUM",
                    data_type="dex_pool_state",
                    instrument_type="POOL",
                    instrument_id="CURVE-ETHEREUM:POOL:DAI-USDC:4",
                    capture_status="captured",
                    row_count=1,
                ),
            ],
            columns=_COLS,
        )
        out, stats = recon.reconcile_frame(df)
        assert len(out) == 1
        assert stats["deleted_glued_pair"] == 0
