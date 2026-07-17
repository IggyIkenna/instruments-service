"""Unit tests for canonicalize_cefi_split_venue_chain_2026_07_17.canonicalize_frame.

The migration collapses the CeFi index's split ``venue={PROTOCOL}, chain={CHAIN}``
rows onto the canonical ``venue={PROTOCOL}-{CHAIN}, chain=""`` shard atom (cefi has
no chain axis — UAC ``SHARD_AXIS_MATRIX`` cefi = ``("venue",)``).

Coverage: the collapse itself; collision de-duplication on the manifest's REAL
composite row key (``_ROW_KEY_COLUMNS``, of which ``chain`` is a member — which is
exactly why collapsing creates collisions); the winner rule preferring a
``captured`` row over a newer non-captured one (the footgun a sibling migration's
dry-run caught); non-split cefi rows left untouched; and idempotency (re-running
over an already-migrated frame is a no-op).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "canonicalize_cefi_split_venue_chain_2026_07_17.py"
    spec = importlib.util.spec_from_file_location("canonicalize_cefi_split_venue_chain", script)
    if spec is None or spec.loader is None:
        msg = f"Failed to load script spec at {script}"
        raise RuntimeError(msg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


def _row(**overrides: object) -> dict[str, object]:
    """A manifest row with every ``_ROW_KEY_COLUMNS`` member present, so the
    row-key join under test is the real one, not a reduced stand-in."""
    base: dict[str, object] = {
        "date": "2026-06-01",
        "venue": "LIGHTER",
        "chain": "ZKSYNC",
        "data_type": "instruments",
        "timeframe": "",
        "league_id": "",
        "instrument_type": "PERPETUAL",
        "underlying": "",
        "feature_group": "",
        "feature_family": "",
        "model_family": "",
        "training_period": "",
        "strategy_id": "",
        "client_id": "",
        "instruction_type": "",
        "instrument_id": "",
        "quote_asset": "",
        "margin_type": "",
        "combo_type": "",
        "fixture_id": "",
        "job_id": "",
        "pipeline_mode": "batch_instruments_service",
        "source": "instruments_service",
        "transport": "",
        "cadence": "",
        "asset_group": "cefi",
        "capture_status": "captured",
        "attempted_at": "2026-06-01T00:00:00+00:00",
        "row_count": 100,
        "instrument_count": 100,
    }
    base.update(overrides)
    return base


class TestCollapse:
    def test_split_rows_collapse_to_the_canonical_venue(self, mod) -> None:
        df = pd.DataFrame([_row(venue="LIGHTER", chain="ZKSYNC"), _row(venue="PACIFICA", chain="SOLANA")])
        out, stats = mod.canonicalize_frame(df)
        assert stats["split_rows"] == 2
        assert set(out["venue"]) == {"LIGHTER-ZKSYNC", "PACIFICA-SOLANA"}
        assert set(out["chain"]) == {""}, "cefi has no chain axis — chain must be blanked"

    def test_non_split_cefi_rows_are_untouched(self, mod) -> None:
        """A plain cefi venue (no chain) and an unlisted venue must pass through."""
        df = pd.DataFrame(
            [
                _row(venue="BINANCE-FUTURES", chain=""),
                _row(venue="DERIBIT", chain=""),
                _row(venue="LIGHTER", chain="ZKSYNC"),
            ]
        )
        out, stats = mod.canonicalize_frame(df)
        assert stats["split_rows"] == 1
        assert {"BINANCE-FUTURES", "DERIBIT"} <= set(out["venue"])
        assert len(out) == 3

    def test_a_venue_outside_the_allowlist_is_not_collapsed(self, mod) -> None:
        """The allowlist is explicit on purpose — an unexpected (venue, chain)
        pair must be left alone rather than silently rewritten."""
        df = pd.DataFrame([_row(venue="SOMETHING", chain="ETHEREUM")])
        out, stats = mod.canonicalize_frame(df)
        assert stats["split_rows"] == 0
        assert out.iloc[0]["venue"] == "SOMETHING"
        assert out.iloc[0]["chain"] == "ETHEREUM"


class TestCollisionDedup:
    def test_collapsing_onto_an_existing_canonical_row_dedups(self, mod) -> None:
        """`chain` is a row-key column, so the collapse changes the key and can
        land on a row that already exists — exactly the 672 real collisions."""
        df = pd.DataFrame(
            [
                _row(venue="LIGHTER-ZKSYNC", chain="", capture_status="captured"),
                _row(venue="LIGHTER", chain="ZKSYNC", capture_status="captured"),
            ]
        )
        out, stats = mod.canonicalize_frame(df)
        assert stats["collisions_dropped"] == 1
        assert len(out) == 1
        assert out.iloc[0]["venue"] == "LIGHTER-ZKSYNC"

    def test_captured_beats_a_newer_non_captured_row(self, mod) -> None:
        """Winner rule: capture_status FIRST, recency second. A newer
        empty_confirmed must never evict an older captured row."""
        df = pd.DataFrame(
            [
                _row(
                    venue="LIGHTER-ZKSYNC",
                    chain="",
                    capture_status="empty_confirmed",
                    attempted_at="2026-07-01T00:00:00+00:00",  # NEWER
                    row_count=0,
                ),
                _row(
                    venue="LIGHTER",
                    chain="ZKSYNC",
                    capture_status="captured",
                    attempted_at="2026-06-01T00:00:00+00:00",  # older
                    row_count=198,
                ),
            ]
        )
        out, _ = mod.canonicalize_frame(df)
        assert len(out) == 1
        assert out.iloc[0]["capture_status"] == "captured", "a newer empty row evicted the captured evidence"
        assert out.iloc[0]["row_count"] == 198

    def test_most_recent_wins_within_the_same_status(self, mod) -> None:
        df = pd.DataFrame(
            [
                _row(venue="LIGHTER-ZKSYNC", chain="", attempted_at="2026-06-01T00:00:00+00:00", row_count=1),
                _row(venue="LIGHTER", chain="ZKSYNC", attempted_at="2026-07-01T00:00:00+00:00", row_count=2),
            ]
        )
        out, _ = mod.canonicalize_frame(df)
        assert len(out) == 1
        assert out.iloc[0]["row_count"] == 2

    def test_a_gap_filling_row_is_kept_not_dropped(self, mod) -> None:
        """3,945 of the real split rows land on keys with NO existing row — they
        repair the manifest, so they must survive the dedup."""
        df = pd.DataFrame(
            [
                _row(venue="LIGHTER-ZKSYNC", chain="", date="2026-06-01"),
                _row(venue="LIGHTER", chain="ZKSYNC", date="2024-08-30"),  # no canonical twin
            ]
        )
        out, stats = mod.canonicalize_frame(df)
        assert stats["collisions_dropped"] == 0
        assert len(out) == 2
        assert set(out["date"]) == {"2026-06-01", "2024-08-30"}


class TestIdempotency:
    def test_rerunning_over_a_migrated_frame_is_a_no_op(self, mod) -> None:
        df = pd.DataFrame([_row(venue="LIGHTER", chain="ZKSYNC"), _row(venue="PACIFICA", chain="SOLANA")])
        once, _ = mod.canonicalize_frame(df)
        twice, stats2 = mod.canonicalize_frame(once)
        assert stats2["split_rows"] == 0
        assert stats2["collisions_dropped"] == 0
        assert len(twice) == len(once)

    def test_blank_chain_variants_normalise_to_the_same_key(self, mod) -> None:
        """Manifest string columns mix None/NaN/"" — a collapsed row's chain ("")
        must compare equal to an existing row's None chain, or the dedup silently
        misses and the duplicate venue survives."""
        df = pd.DataFrame(
            [
                _row(venue="LIGHTER-ZKSYNC", chain=None, capture_status="captured"),
                _row(venue="LIGHTER", chain="ZKSYNC", capture_status="captured"),
            ]
        )
        out, stats = mod.canonicalize_frame(df)
        assert stats["collisions_dropped"] == 1, "None-vs-'' chain broke the row-key comparison"
        assert len(out) == 1
