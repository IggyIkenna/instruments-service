"""Unit tests for drain_residual_lending_rows_2026_07_17.plan_drain.

The residual ``LENDING`` rows are already-split markets whose stale original was
left behind by the 2026-07-13 split (which added twins but never removed sources).
So the drain DELETES them — but only where a twin provably supersedes the row.

The load-bearing property under test is the SAFETY guard: a LENDING row is deleted
ONLY when every required twin exists AND covers at least as much history. Losing
real lifecycle history would be far worse than a stale label, so each "keep"
branch is pinned individually.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "drain_residual_lending_rows_2026_07_17.py"
    spec = importlib.util.spec_from_file_location("drain_residual_lending_rows", script)
    if spec is None or spec.loader is None:
        msg = f"Failed to load script spec at {script}"
        raise RuntimeError(msg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


def _row(
    instrument_id: str, instrument_type: str, available_from: str, available_to: str | None = None
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "instrument_type": instrument_type,
        "venue": instrument_id.split("-", 1)[0],
        "chain": "ETHEREUM",
        "available_from": available_from,
        "available_to": available_to,
    }


class TestRequiredTwins:
    def test_lending_market_requires_both_legs(self, mod) -> None:
        twins = mod.required_twins("MORPHO-ETHEREUM:LENDING_MARKET:cbBTC-USDT:0x2b8019")
        assert twins == [
            "MORPHO-ETHEREUM:A_TOKEN:AcbBTC-USDT:0x2b8019",
            "MORPHO-ETHEREUM:DEBT_TOKEN:DEBTcbBTC-USDT:0x2b8019",
        ]

    def test_supply_and_borrow_are_one_to_one(self, mod) -> None:
        assert mod.required_twins("COMPOUND_V3-ARBITRUM:SUPPLY:CUSDC") == ["COMPOUND_V3-ARBITRUM:A_TOKEN:CUSDC"]
        assert mod.required_twins("COMPOUND_V3-ARBITRUM:BORROW:BORROWUSDC") == [
            "COMPOUND_V3-ARBITRUM:DEBT_TOKEN:BORROWUSDC"
        ]

    def test_an_unrecognised_shape_yields_no_twin(self, mod) -> None:
        assert mod.required_twins("SOMETHING-ETHEREUM:WEIRD:XYZ") is None


class TestSafetyGuard:
    def test_row_with_full_history_twins_is_deleted(self, mod) -> None:
        df = pd.DataFrame(
            [
                _row("MORPHO-ETHEREUM:LENDING_MARKET:A-B", "LENDING", "2024-01-01", "2026-06-24"),
                _row("MORPHO-ETHEREUM:A_TOKEN:AA-B", "A_TOKEN", "2024-01-01", None),
                _row("MORPHO-ETHEREUM:DEBT_TOKEN:DEBTA-B", "DEBT_TOKEN", "2024-01-01", None),
            ]
        )
        kept, blocked, stats = mod.plan_drain(df)
        assert stats["deletable"] == 1
        assert len(blocked) == 0
        assert "LENDING" not in set(kept["instrument_type"])
        assert len(kept) == 2, "only the stale original is removed; both twins survive"

    def test_row_is_kept_when_a_required_twin_is_missing(self, mod) -> None:
        """Only the A_TOKEN leg exists — the market is not fully superseded, so the
        original must survive rather than be deleted on assumption."""
        df = pd.DataFrame(
            [
                _row("MORPHO-ETHEREUM:LENDING_MARKET:A-B", "LENDING", "2024-01-01", "2026-06-24"),
                _row("MORPHO-ETHEREUM:A_TOKEN:AA-B", "A_TOKEN", "2024-01-01", None),
            ]
        )
        kept, blocked, stats = mod.plan_drain(df)
        assert stats["deletable"] == 0
        assert stats["kept_no_twin"] == 1
        assert len(blocked) == 1
        assert "LENDING" in set(kept["instrument_type"])

    def test_row_is_kept_when_a_twin_starts_later_history_would_be_lost(self, mod) -> None:
        """THE critical guard: a twin that begins AFTER the original would silently
        drop the earlier lifecycle if the original were deleted."""
        df = pd.DataFrame(
            [
                _row("MORPHO-ETHEREUM:LENDING_MARKET:A-B", "LENDING", "2023-01-01", "2026-06-24"),
                _row("MORPHO-ETHEREUM:A_TOKEN:AA-B", "A_TOKEN", "2025-06-01", None),  # LATER
                _row("MORPHO-ETHEREUM:DEBT_TOKEN:DEBTA-B", "DEBT_TOKEN", "2023-01-01", None),
            ]
        )
        kept, blocked, stats = mod.plan_drain(df)
        assert stats["deletable"] == 0
        assert stats["kept_twin_starts_later"] == 1
        assert len(blocked) == 1
        assert "LENDING" in set(kept["instrument_type"])

    def test_unknown_key_shape_is_kept(self, mod) -> None:
        df = pd.DataFrame([_row("WEIRD-ETHEREUM:MYSTERY:X", "LENDING", "2024-01-01", "2026-01-01")])
        kept, _blocked, stats = mod.plan_drain(df)
        assert stats["kept_unknown_shape"] == 1
        assert stats["deletable"] == 0
        assert len(kept) == 1

    def test_twin_starting_earlier_than_the_original_is_fine(self, mod) -> None:
        df = pd.DataFrame(
            [
                _row("COMPOUND_V3-BASE:SUPPLY:CUSDC", "LENDING", "2024-06-01", "2026-07-10"),
                _row("COMPOUND_V3-BASE:A_TOKEN:CUSDC", "A_TOKEN", "2023-01-01", None),  # EARLIER = wider
            ]
        )
        _kept, _blocked, stats = mod.plan_drain(df)
        assert stats["deletable"] == 1


class TestNoCollateralDamage:
    def test_non_lending_rows_are_never_touched(self, mod) -> None:
        df = pd.DataFrame(
            [
                _row("CURVE-ETHEREUM:POOL:0xabc", "POOL", "2021-01-01", None),
                _row("AAVE_V3-ETHEREUM:A_TOKEN:AUSDC", "A_TOKEN", "2023-01-01", None),
                _row("MORPHO-ETHEREUM:LENDING_MARKET:A-B", "LENDING", "2024-01-01", "2026-06-24"),
                _row("MORPHO-ETHEREUM:A_TOKEN:AA-B", "A_TOKEN", "2024-01-01", None),
                _row("MORPHO-ETHEREUM:DEBT_TOKEN:DEBTA-B", "DEBT_TOKEN", "2024-01-01", None),
            ]
        )
        kept, _blocked, stats = mod.plan_drain(df)
        assert stats["deletable"] == 1
        assert "CURVE-ETHEREUM:POOL:0xabc" in set(kept["instrument_id"])
        assert "AAVE_V3-ETHEREUM:A_TOKEN:AUSDC" in set(kept["instrument_id"])

    def test_idempotent_second_run_is_a_no_op(self, mod) -> None:
        df = pd.DataFrame(
            [
                _row("MORPHO-ETHEREUM:LENDING_MARKET:A-B", "LENDING", "2024-01-01", "2026-06-24"),
                _row("MORPHO-ETHEREUM:A_TOKEN:AA-B", "A_TOKEN", "2024-01-01", None),
                _row("MORPHO-ETHEREUM:DEBT_TOKEN:DEBTA-B", "DEBT_TOKEN", "2024-01-01", None),
            ]
        )
        once, _b, _s = mod.plan_drain(df)
        twice, _b2, stats2 = mod.plan_drain(once)
        assert stats2["lending_rows"] == 0
        assert stats2["deletable"] == 0
        assert len(twice) == len(once)
