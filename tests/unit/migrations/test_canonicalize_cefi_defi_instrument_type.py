"""Unit tests for canonicalize_cefi_defi_instrument_type_2026_07_17.

Two cefi/defi-specific hazards (neither present in the tradfi precedent) are what this
migration exists to survive, so they carry the heaviest pinning here:

1. **The object path is dual-shaped and the LEGACY shape is stale.** Reading the legacy
   ``day=/venue=`` object in preference to the canonical
   ``day=/pipeline_mode=/asset_group=/venue=`` one is exactly what corrupted the completed
   tradfi migration (live-verified: tradfi CME 2026-06-28 re-stamped 74,005 → 2,826 off a
   stale legacy object). Canonical-first ordering is therefore a correctness contract, not a
   preference.
2. **Most blank cefi/defi rows are superseded GHOSTS**, already represented by a correct
   typed row at their own row_key. Backfilling one mints a duplicate and double-counts
   coverage. Live-measured: 7,055/13,046 cefi and 58,329/65,443 defi blank+captured rows are
   ghosts.

Honest absence (a missing/unreadable/type-less object leaves the row BLANK, never guessed)
and idempotency are pinned alongside.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "canonicalize_cefi_defi_instrument_type_2026_07_17.py"
    spec = importlib.util.spec_from_file_location("canonicalize_cefi_defi_instrument_type", script)
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
    date: str,
    venue: str,
    instrument_type: str = "",
    *,
    chain: str = "",
    capture_status: str = "captured",
    row_count: float = 10,
    instrument_count: int = 10,
    pipeline_mode: str = "batch_instruments_service",
    data_type: str = "instruments",
) -> dict[str, object]:
    return {
        "date": date,
        "venue": venue,
        "chain": chain,
        "instrument_type": instrument_type,
        "capture_status": capture_status,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "pipeline_mode": pipeline_mode,
        "data_type": data_type,
        "written_at": "2026-05-04T13:13:46+00:00",
        "service_name": "instruments-service",
    }


class _FakeStorage:
    """Serves parquet bytes for an exact {path: {type: count}} map; 404s everything else."""

    def __init__(self, objects: dict[str, dict[str, int]]) -> None:
        self._objects = objects
        self.reads: list[str] = []

    def download_bytes(self, _bucket: str, path: str) -> bytes:
        self.reads.append(path)
        if path not in self._objects:
            msg = f"404 no such object: {path}"
            raise FileNotFoundError(msg)
        types: list[str] = []
        for itype, cnt in self._objects[path].items():
            types.extend([itype] * cnt)
        import io

        buf = io.BytesIO()
        pd.DataFrame({"instrument_type": types, "raw_symbol": [f"S{i}" for i in range(len(types))]}).to_parquet(
            buf, index=False
        )
        return buf.getvalue()


def _derive(mod, rows: list[dict[str, object]], objects: dict[str, dict[str, int]], asset_group: str = "cefi"):
    storage = _FakeStorage(objects)
    df = pd.DataFrame(rows)
    out, stats = mod.derive(df, asset_group=asset_group, workers=2, bucket="b", storage=storage)
    return out, stats, storage


def _canon(ag: str, date: str, venue: str) -> str:
    return (
        f"instrument_availability/by_date/day={date}/pipeline_mode=batch_instruments_service"
        f"/asset_group={ag}/venue={venue}/instruments.parquet"
    )


def _legacy(date: str, venue: str) -> str:
    return f"instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet"


class TestPathResolution:
    def test_defi_path_venue_is_reglued_from_venue_and_chain(self, mod) -> None:
        """The manifest splits AAVE_V3-ETHEREUM into venue+chain; the OBJECT PATH keeps it glued."""
        assert mod.path_venue("AAVE_V3", "ETHEREUM") == "AAVE_V3-ETHEREUM"

    def test_cefi_path_venue_is_the_venue_itself(self, mod) -> None:
        """cefi rows carry chain="" — including on-chain perp CLOBs whose glued name IS the venue."""
        assert mod.path_venue("DERIBIT", "") == "DERIBIT"
        assert mod.path_venue("LIGHTER-ZKSYNC", "") == "LIGHTER-ZKSYNC"

    def test_canonical_path_is_tried_before_legacy(self, mod) -> None:
        """Order is load-bearing: preferring the stale legacy object is the tradfi corruption bug."""
        paths = mod.candidate_paths(
            asset_group="cefi",
            date="2019-03-30",
            venue="DERIBIT",
            chain="",
            pipeline_mode="batch_instruments_service",
        )
        assert paths[0] == _canon("cefi", "2019-03-30", "DERIBIT")
        assert paths[1] == _legacy("2019-03-30", "DERIBIT")

    def test_blank_pipeline_mode_falls_back_to_the_default_partition(self, mod) -> None:
        paths = mod.candidate_paths(
            asset_group="defi", date="2020-03-20", venue="CURVE", chain="ETHEREUM", pipeline_mode=""
        )
        assert "pipeline_mode=batch_instruments_service" in paths[0]
        assert "venue=CURVE-ETHEREUM" in paths[0]


class TestCanonicalPathWins:
    def test_stale_legacy_object_is_never_read_when_canonical_exists(self, mod) -> None:
        """THE regression guard for the tradfi bug (live case: cefi DERIBIT 2019-03-30,
        canonical=295 rows incl. 289 OPTIONs vs legacy=6 rows missing every OPTION)."""
        rows = [_row("2019-03-30", "DERIBIT", row_count=295, instrument_count=295)]
        objects = {
            _canon("cefi", "2019-03-30", "DERIBIT"): {"OPTION": 289, "FUTURE": 4, "PERPETUAL": 2},
            _legacy("2019-03-30", "DERIBIT"): {"FUTURE": 4, "PERPETUAL": 2},
        }
        out, stats, _ = _derive(mod, rows, objects)
        by_type = dict(zip(out["instrument_type"], out["row_count"], strict=True))
        assert by_type == {"OPTION": 289, "FUTURE": 4, "PERPETUAL": 2}
        assert int(out["row_count"].sum()) == 295, "the canonical object's full count must survive"
        assert stats["legacy_path_used"] == 0
        assert stats["drift_shards"] == 0

    def test_legacy_object_is_used_only_when_canonical_is_absent(self, mod) -> None:
        """defi genuinely needs this fallback — canonical existed for only 67/120 sampled targets."""
        rows = [_row("2020-03-20", "CURVE", chain="ETHEREUM", row_count=13, instrument_count=13)]
        objects = {_legacy("2020-03-20", "CURVE-ETHEREUM"): {"POOL": 13}}
        out, stats, _ = _derive(mod, rows, objects, asset_group="defi")
        assert list(out["instrument_type"]) == ["POOL"]
        assert stats["legacy_path_used"] == 1


class TestMixedTypeSplit:
    def test_a_mixed_type_shard_splits_into_one_row_per_real_type(self, mod) -> None:
        rows = [_row("2024-01-03", "CME", row_count=12819, instrument_count=12819)]
        objects = {_canon("cefi", "2024-01-03", "CME"): {"OPTION": 8494, "COMBO": 4024, "FUTURE": 301}}
        out, stats, _ = _derive(mod, rows, objects)
        assert len(out) == 3
        assert stats["split_shards"] == 1
        assert stats["split_rows_created"] == 3
        assert dict(zip(out["instrument_type"], out["row_count"], strict=True)) == {
            "OPTION": 8494,
            "COMBO": 4024,
            "FUTURE": 301,
        }

    def test_a_single_type_shard_is_updated_in_place_not_split(self, mod) -> None:
        rows = [_row("2020-03-20", "RAYDIUM", chain="SOLANA", row_count=13, instrument_count=13)]
        objects = {_canon("defi", "2020-03-20", "RAYDIUM-SOLANA"): {"POOL": 13}}
        out, stats, _ = _derive(mod, rows, objects, asset_group="defi")
        assert len(out) == 1
        assert stats["updated_in_place"] == 1
        assert stats["split_shards"] == 0
        assert out.iloc[0]["instrument_type"] == "POOL"

    def test_split_rows_copy_every_other_axis_verbatim(self, mod) -> None:
        """The cross-service safety argument depends on this: only instrument_type/row_count/
        instrument_count change, so consumers filtering without instrument_type are unaffected."""
        rows = [_row("2024-01-03", "CME", row_count=100, instrument_count=100)]
        objects = {_canon("cefi", "2024-01-03", "CME"): {"OPTION": 60, "FUTURE": 40}}
        out, _stats, _ = _derive(mod, rows, objects)
        for col in ("capture_status", "written_at", "pipeline_mode", "data_type", "service_name", "venue", "date"):
            assert set(out[col]) == {rows[0][col]}, f"{col} must be invariant across split rows"

    def test_legacy_lowercase_types_are_canonicalised_before_grouping(self, mod) -> None:
        """Mirrors writers._LEGACY_INSTRUMENT_TYPE_ALIASES — a stray lowercase value must never
        mint a second permanent row_key alongside the canonical-cased row for the same atom."""
        rows = [_row("2021-01-01", "BYBIT", row_count=30, instrument_count=30)]
        objects = {_canon("cefi", "2021-01-01", "BYBIT"): {"perpetual": 20, "spot": 10}}
        out, _stats, _ = _derive(mod, rows, objects)
        assert set(out["instrument_type"]) == {"PERPETUAL", "SPOT_PAIR"}


class TestSupersededGhosts:
    def test_a_ghost_row_is_dropped_not_duplicated(self, mod) -> None:
        """Live case: cefi UPBIT 2021-04-23 carries BOTH a blank row (7, buggy writer) and a
        correct SPOT_PAIR row (133, fixed writer). Backfilling the blank one would duplicate it."""
        rows = [
            _row("2021-04-23", "UPBIT", "", row_count=7, instrument_count=7),
            _row("2021-04-23", "UPBIT", "SPOT_PAIR", row_count=133, instrument_count=133),
        ]
        objects = {_canon("cefi", "2021-04-23", "UPBIT"): {"SPOT_PAIR": 133}}
        out, stats, _ = _derive(mod, rows, objects)
        assert len(out) == 1, "the blank ghost is dropped; the correct typed row survives alone"
        assert stats["ghost_rows_dropped"] == 1
        assert stats["duplicate_row_keys_after"] == 0
        surviving = out.iloc[0]
        assert surviving["instrument_type"] == "SPOT_PAIR"
        assert surviving["row_count"] == 133, "the existing fixed-writer row is left untouched"

    def test_a_ghost_type_not_covered_by_siblings_is_still_emitted(self, mod) -> None:
        """A ghost is only fully redundant if EVERY derived type already has a row. An uncovered
        type must still contribute rather than be silently lost with the dropped row."""
        rows = [
            _row("2024-05-05", "DERIBIT", "", row_count=100, instrument_count=100),
            _row("2024-05-05", "DERIBIT", "OPTION", row_count=90, instrument_count=90),
        ]
        objects = {_canon("cefi", "2024-05-05", "DERIBIT"): {"OPTION": 90, "FUTURE": 10}}
        out, stats, _ = _derive(mod, rows, objects)
        assert set(out["instrument_type"]) == {"OPTION", "FUTURE"}
        assert stats["ghost_types_skipped"] == 1, "OPTION already existed"
        assert stats["ghost_rows_dropped"] == 0, "the row still contributed FUTURE"
        assert dict(zip(out["instrument_type"], out["row_count"], strict=True)) == {"OPTION": 90, "FUTURE": 10}

    def test_a_ghost_on_a_different_chain_is_not_a_collision(self, mod) -> None:
        """chain is a real defi row_key axis — CURVE/ETHEREUM must not be superseded by CURVE/OPTIMISM."""
        rows = [
            _row("2020-03-20", "CURVE", "", chain="ETHEREUM", row_count=13, instrument_count=13),
            _row("2020-03-20", "CURVE", "POOL", chain="OPTIMISM", row_count=1, instrument_count=1),
        ]
        objects = {_canon("defi", "2020-03-20", "CURVE-ETHEREUM"): {"POOL": 13}}
        out, stats, _ = _derive(mod, rows, objects, asset_group="defi")
        assert len(out) == 2
        assert stats["ghost_rows_dropped"] == 0
        assert set(zip(out["chain"], out["row_count"], strict=True)) == {("ETHEREUM", 13), ("OPTIMISM", 1)}


class TestHonestAbsence:
    def test_a_missing_object_leaves_the_row_blank_and_is_never_guessed(self, mod) -> None:
        rows = [_row("2019-01-01", "GHOSTVENUE", row_count=5, instrument_count=5)]
        out, stats, _ = _derive(mod, rows, {})
        assert len(out) == 1
        assert out.iloc[0]["instrument_type"] == ""
        assert out.iloc[0]["row_count"] == 5, "the original count is preserved untouched"
        assert stats["unresolvable_left_blank"] == 1
        assert stats["blank_captured_after"] == 1

    def test_an_object_with_no_instrument_type_column_leaves_the_row_blank(self, mod) -> None:
        class _NoColumnStorage:
            def download_bytes(self, _bucket: str, _path: str) -> bytes:
                import io

                buf = io.BytesIO()
                pd.DataFrame({"raw_symbol": ["A", "B"]}).to_parquet(buf, index=False)
                return buf.getvalue()

        df = pd.DataFrame([_row("2019-01-01", "OKX", row_count=2, instrument_count=2)])
        out, stats = mod.derive(df, asset_group="cefi", workers=1, bucket="b", storage=_NoColumnStorage())
        assert out.iloc[0]["instrument_type"] == ""
        assert stats["unresolvable_left_blank"] == 1

    def test_an_empty_object_leaves_the_row_blank(self, mod) -> None:
        rows = [_row("2019-01-01", "OKX", row_count=0, instrument_count=0)]
        objects = {_canon("cefi", "2019-01-01", "OKX"): {}}
        out, stats, _ = _derive(mod, rows, objects)
        assert out.iloc[0]["instrument_type"] == ""
        assert stats["unresolvable_left_blank"] == 1

    def test_a_genuinely_blank_type_inside_the_object_stays_blank(self, mod) -> None:
        """A shard whose own records partly fail to classify keeps that fraction explicitly blank
        rather than having it silently dropped or reassigned."""
        rows = [_row("2021-01-01", "BYBIT", row_count=10, instrument_count=10)]
        objects = {_canon("cefi", "2021-01-01", "BYBIT"): {"PERPETUAL": 7, "": 3}}
        out, _stats, _ = _derive(mod, rows, objects)
        assert dict(zip(out["instrument_type"], out["row_count"], strict=True)) == {"PERPETUAL": 7, "": 3}


class TestNonCapturedRowsAreUntouched:
    @pytest.mark.parametrize("status", ["empty_confirmed", "expected_unattempted", "attempted_failed"])
    def test_blank_non_captured_rows_are_left_alone(self, mod, status: str) -> None:
        """These shards captured ZERO instruments — a blank type is HONEST and must survive."""
        rows = [_row("2020-01-01", "BINANCE", "", capture_status=status, row_count=0, instrument_count=0)]
        objects = {_canon("cefi", "2020-01-01", "BINANCE"): {"SPOT_PAIR": 99}}
        out, stats, storage = _derive(mod, rows, objects)
        assert len(out) == 1
        assert out.iloc[0]["instrument_type"] == ""
        assert out.iloc[0]["capture_status"] == status
        assert stats["targets"] == 0
        assert storage.reads == [], "a non-captured row must not even trigger an object read"

    def test_already_typed_captured_rows_are_left_alone(self, mod) -> None:
        rows = [_row("2024-01-03", "CME", "FUTURE", row_count=301, instrument_count=301)]
        out, stats, storage = _derive(mod, rows, {})
        assert stats["targets"] == 0
        assert out.iloc[0]["row_count"] == 301
        assert storage.reads == []


class TestGate:
    def _stats(self, **over: object) -> dict[str, object]:
        base: dict[str, object] = {
            "duplicate_row_keys_after": 0,
            "blank_before": 10,
            "blank_after": 4,
            "targets": 6,
            "unresolvable_left_blank": 1,
        }
        base.update(over)
        return base

    def test_clean_run_passes(self, mod) -> None:
        assert mod.gate(self._stats()) is True

    def test_duplicate_row_keys_fail_the_gate(self, mod) -> None:
        """The hard structural invariant — a duplicate means a ghost was backfilled alongside the
        correct row, double-counting coverage."""
        assert mod.gate(self._stats(duplicate_row_keys_after=1)) is False

    def test_blank_regression_fails_the_gate(self, mod) -> None:
        assert mod.gate(self._stats(blank_after=11)) is False

    def test_all_targets_unresolved_fails_the_gate(self, mod) -> None:
        assert mod.gate(self._stats(unresolvable_left_blank=6)) is False

    def test_scattered_honest_absence_still_passes(self, mod) -> None:
        assert mod.gate(self._stats(unresolvable_left_blank=5)) is True

    def test_no_targets_at_all_passes(self, mod) -> None:
        """The post-migration steady state (a re-run finds nothing) must not trip the gate."""
        assert mod.gate(self._stats(targets=0, unresolvable_left_blank=0, blank_after=10)) is True


class TestIdempotency:
    def test_a_second_run_is_a_no_op(self, mod) -> None:
        rows = [
            _row("2024-01-03", "CME", row_count=12819, instrument_count=12819),
            _row("2020-01-01", "BINANCE", "", capture_status="empty_confirmed", row_count=0, instrument_count=0),
        ]
        objects = {_canon("cefi", "2024-01-03", "CME"): {"OPTION": 8494, "COMBO": 4024, "FUTURE": 301}}
        once, stats1, _ = _derive(mod, rows, objects)
        assert stats1["targets"] == 1

        storage = _FakeStorage(objects)
        twice, stats2 = mod.derive(once, asset_group="cefi", workers=2, bucket="b", storage=storage)
        assert stats2["targets"] == 0, "nothing left to migrate"
        assert stats2["duplicate_row_keys_after"] == 0
        assert len(twice) == len(once)
        assert storage.reads == []
        pd.testing.assert_frame_equal(
            twice.sort_values("instrument_type").reset_index(drop=True),
            once.sort_values("instrument_type").reset_index(drop=True),
        )

    def test_the_honest_blank_residual_is_stable_across_runs(self, mod) -> None:
        """An unresolvable row stays blank+captured forever — it must not be re-read into a guess
        or dropped on a later run."""
        rows = [_row("2019-01-01", "GHOSTVENUE", row_count=5, instrument_count=5)]
        once, _s1, _ = _derive(mod, rows, {})
        twice, stats2, _ = _derive(mod, once.to_dict("records"), {})
        assert stats2["unresolvable_left_blank"] == 1
        assert len(twice) == 1
        assert twice.iloc[0]["instrument_type"] == ""
