"""Unit tests for repair_tradfi_instrument_type_counts_2026_07_17.

This repair exists because the COMPLETED tradfi migration
(``canonicalize_tradfi_instrument_type_2026_07_16.py``) re-stamped manifest counts from the STALE
LEGACY object instead of the CANONICAL pipeline_mode-partitioned one. Live-verified 2026-07-17:
tradfi CME 2026-06-28 canonical object = 74,005 rows (OPTION 69,212 / COMBO 4,446 / FUTURE 347)
while the legacy object = 2,826 (OPTION 2,566 / COMBO 228 / FUTURE 32) — and 2,826 is exactly what
the migration wrote. That case is pinned here directly.

The two shapes that make the OBVIOUS repairs wrong get the heaviest pinning, because each was
measured on live data and each silently destroys something:

1. **A blank-row backfill is a no-op** — tradfi has ZERO blank+captured rows left, so the damage
   lives on ALREADY-TYPED rows. These tests therefore drive typed rows, not blank ones.
2. **A snapshot restore loses fresher data** — 39 live rows across 21 atoms were written AFTER the
   snapshot (the daily job re-captures a rolling window), 14 of those atoms already existing in the
   snapshot. Pinned by ``test_rows_outside_the_touched_atom_set_are_preserved_verbatim`` and
   ``test_post_snapshot_recapture_of_a_touched_atom_is_not_reverted``.

Honest absence (both objects gone ⇒ atom untouched, never guessed/blanked/zeroed), provenance
preservation, idempotency, and the gates are pinned alongside.
"""

from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pandas as pd
import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "repair_tradfi_instrument_type_counts_2026_07_17.py"
    spec = importlib.util.spec_from_file_location("repair_tradfi_instrument_type_counts", script)
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
    capture_status: str = "captured",
    row_count: float = 10,
    instrument_count: int = 10,
    written_at: str = "2026-05-04T13:13:46+00:00",
    chain: str = "",
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
        "written_at": written_at,
        "attempted_at": written_at,
        "source": "instruments_service",
        "service_name": "instruments-service",
        "asset_group": "tradfi",
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
        buf = io.BytesIO()
        pd.DataFrame({"instrument_type": types, "raw_symbol": [f"S{i}" for i in range(len(types))]}).to_parquet(
            buf, index=False
        )
        return buf.getvalue()


def _canon(date: str, venue: str) -> str:
    return (
        f"instrument_availability/by_date/day={date}/pipeline_mode=batch_instruments_service"
        f"/asset_group=tradfi/venue={venue}/instruments.parquet"
    )


def _legacy(date: str, venue: str) -> str:
    return f"instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet"


def _derive(mod, live_rows, snapshot_rows, objects):
    storage = _FakeStorage(objects)
    out, stats = mod.derive(
        pd.DataFrame(live_rows),
        pd.DataFrame(snapshot_rows),
        workers=2,
        bucket="b",
        storage=storage,
    )
    return out, stats, storage


def _types(out: pd.DataFrame, date: str, venue: str) -> dict[str, int]:
    sel = out[(out["date"].astype(str) == date) & (out["venue"].astype(str) == venue)]
    sel = sel[sel["capture_status"].astype(str) == "captured"]
    return dict(zip(sel["instrument_type"], sel["instrument_count"].astype(int), strict=True))


class TestPathResolution:
    def test_canonical_path_is_tried_before_legacy(self, mod) -> None:
        """Order is load-bearing: preferring the stale legacy object IS the bug being repaired."""
        paths = mod.candidate_paths(date="2026-06-28", venue="CME", pipeline_mode="batch_instruments_service")
        assert paths[0] == _canon("2026-06-28", "CME")
        assert paths[1] == _legacy("2026-06-28", "CME")

    def test_blank_pipeline_mode_falls_back_to_the_default_partition(self, mod) -> None:
        paths = mod.candidate_paths(date="2026-06-28", venue="CME", pipeline_mode="")
        assert "pipeline_mode=batch_instruments_service" in paths[0]


class TestTheCmeRegression:
    """The exact live case that proved the corruption."""

    def test_stale_legacy_counts_are_restamped_from_the_canonical_object(self, mod) -> None:
        snapshot = [_row("2026-06-28", "CME", "", row_count=74005, instrument_count=74005)]
        live = [
            _row("2026-06-28", "CME", "OPTION", row_count=2566, instrument_count=2566),
            _row("2026-06-28", "CME", "FUTURE", row_count=32, instrument_count=32),
            _row("2026-06-28", "CME", "COMBO", row_count=228, instrument_count=228),
        ]
        objects = {
            _canon("2026-06-28", "CME"): {"OPTION": 69212, "COMBO": 4446, "FUTURE": 347},
            _legacy("2026-06-28", "CME"): {"OPTION": 2566, "COMBO": 228, "FUTURE": 32},
        }
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert _types(out, "2026-06-28", "CME") == {"OPTION": 69212, "COMBO": 4446, "FUTURE": 347}
        assert sum(_types(out, "2026-06-28", "CME").values()) == 74005
        assert stats["counts_restamped"] == 3
        assert stats["atoms_repaired"] == 1
        assert stats["legacy_path_used"] == 0
        assert stats["sum_instrument_count_delta"] == 74005 - 2826

    def test_the_legacy_object_is_never_read_when_the_canonical_one_exists(self, mod) -> None:
        snapshot = [_row("2026-06-28", "CME", "", instrument_count=74005)]
        live = [_row("2026-06-28", "CME", "OPTION", row_count=2566, instrument_count=2566)]
        objects = {
            _canon("2026-06-28", "CME"): {"OPTION": 69212},
            _legacy("2026-06-28", "CME"): {"OPTION": 2566},
        }
        _out, _stats, storage = _derive(mod, live, snapshot, objects)
        assert _legacy("2026-06-28", "CME") not in storage.reads

    def test_legacy_is_used_only_when_the_canonical_object_is_absent(self, mod) -> None:
        """Live-measured: 24 of 10,542 touched atoms have no canonical object."""
        snapshot = [_row("2026-07-02", "CME", "", instrument_count=74696)]
        live = [_row("2026-07-02", "CME", "OPTION", row_count=74696, instrument_count=74696)]
        objects = {_legacy("2026-07-02", "CME"): {"OPTION": 74696}}
        _out, stats, _ = _derive(mod, live, snapshot, objects)
        assert stats["legacy_path_used"] == 1


class TestMissingTypeRecovery:
    def test_a_type_the_stale_object_omitted_entirely_is_minted(self, mod) -> None:
        """The DERIBIT-shaped hazard: a counts-only repair would leave these instruments missing.
        (Sibling AG proof: cefi DERIBIT 2019-03-30 canonical=295 incl. 289 OPTIONs vs legacy=6 with
        ZERO OPTIONs.)"""
        snapshot = [_row("2019-03-30", "CME", "", instrument_count=295)]
        live = [
            _row("2019-03-30", "CME", "FUTURE", row_count=4, instrument_count=4),
            _row("2019-03-30", "CME", "COMBO", row_count=2, instrument_count=2),
        ]
        objects = {_canon("2019-03-30", "CME"): {"OPTION": 289, "FUTURE": 4, "COMBO": 2}}
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert _types(out, "2019-03-30", "CME") == {"OPTION": 289, "FUTURE": 4, "COMBO": 2}
        assert stats["rows_minted"] == 1

    def test_a_minted_row_inherits_the_donor_rows_provenance(self, mod) -> None:
        snapshot = [_row("2019-03-30", "CME", "", instrument_count=10)]
        live = [
            _row("2019-03-30", "CME", "FUTURE", row_count=4, instrument_count=4, written_at="2026-06-24T00:00:00+00:00")
        ]
        objects = {_canon("2019-03-30", "CME"): {"OPTION": 6, "FUTURE": 4}}
        out, _stats, _ = _derive(mod, live, snapshot, objects)
        minted = out[(out["instrument_type"] == "OPTION")].iloc[0]
        assert minted["written_at"] == "2026-06-24T00:00:00+00:00"
        assert minted["capture_status"] == "captured"
        assert minted["pipeline_mode"] == "batch_instruments_service"

    def test_a_type_the_object_does_not_carry_is_dropped(self, mod) -> None:
        snapshot = [_row("2024-01-03", "CME", "", instrument_count=100)]
        live = [
            _row("2024-01-03", "CME", "OPTION", row_count=60, instrument_count=60),
            _row("2024-01-03", "CME", "GHOSTTYPE", row_count=40, instrument_count=40),
        ]
        objects = {_canon("2024-01-03", "CME"): {"OPTION": 100}}
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert _types(out, "2024-01-03", "CME") == {"OPTION": 100}
        assert stats["rows_dropped"] == 1


class TestProvenanceIsPreserved:
    def test_only_the_counts_change_on_a_restamped_row(self, mod) -> None:
        """Every other axis must survive verbatim, or IS-index consumers that filter without
        instrument_type see a different row."""
        snapshot = [_row("2026-06-28", "CME", "", instrument_count=74005)]
        live = [
            _row(
                "2026-06-28",
                "CME",
                "OPTION",
                row_count=2566,
                instrument_count=2566,
                written_at="2026-06-28T13:39:24.129130+00:00",
            )
        ]
        objects = {_canon("2026-06-28", "CME"): {"OPTION": 69212}}
        out, _stats, _ = _derive(mod, live, snapshot, objects)
        got = out.iloc[0]
        assert int(got["instrument_count"]) == 69212
        assert int(got["row_count"]) == 69212
        for col in (
            "date",
            "venue",
            "capture_status",
            "written_at",
            "attempted_at",
            "source",
            "service_name",
            "pipeline_mode",
            "data_type",
        ):
            assert got[col] == live[0][col], f"{col} must be preserved verbatim"


class TestHonestAbsence:
    def test_an_atom_with_no_canonical_and_no_legacy_object_is_left_exactly_as_is(self, mod) -> None:
        snapshot = [_row("2020-01-01", "CME", "", instrument_count=500)]
        live = [_row("2020-01-01", "CME", "OPTION", row_count=500, instrument_count=500)]
        out, stats, _ = _derive(mod, live, snapshot, {})
        assert stats["unresolvable_left_as_is"] == 1
        assert stats["atoms_repaired"] == 0
        assert _types(out, "2020-01-01", "CME") == {"OPTION": 500}, "never guessed, blanked or zeroed"

    def test_an_object_carrying_only_blank_types_does_not_blank_the_index(self, mod) -> None:
        snapshot = [_row("2020-01-02", "CME", "", instrument_count=7)]
        live = [_row("2020-01-02", "CME", "OPTION", row_count=7, instrument_count=7)]
        objects = {_canon("2020-01-02", "CME"): {"": 7}}
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert stats["unresolvable_left_as_is"] == 1
        assert _types(out, "2020-01-02", "CME") == {"OPTION": 7}


class TestScopeIsTheMigrationsBlastRadius:
    def test_rows_outside_the_touched_atom_set_are_preserved_verbatim(self, mod) -> None:
        """The 39 post-snapshot rows survive because nothing targets them — no special case."""
        snapshot = [_row("2026-06-28", "CME", "", instrument_count=74005)]
        live = [
            _row("2026-06-28", "CME", "OPTION", row_count=2566, instrument_count=2566),
            _row("2026-07-17", "NYSE", "EQUITY", row_count=494, instrument_count=494),
        ]
        objects = {
            _canon("2026-06-28", "CME"): {"OPTION": 69212},
            # A canonical object exists for the untouched atom too — it must still not be read.
            _canon("2026-07-17", "NYSE"): {"EQUITY": 1},
        }
        out, _stats, storage = _derive(mod, live, snapshot, objects)
        assert _types(out, "2026-07-17", "NYSE") == {"EQUITY": 494}
        assert _canon("2026-07-17", "NYSE") not in storage.reads

    def test_post_snapshot_recapture_of_a_touched_atom_is_not_reverted(self, mod) -> None:
        """Live shape: CME 2026-07-15 OPTION was 68,500 in the snapshot and 70,552 in live after a
        07-17 re-capture. The object agrees with LIVE, so the repair must leave it alone."""
        snapshot = [_row("2026-07-15", "CME", "", instrument_count=68500)]
        live = [_row("2026-07-15", "CME", "OPTION", row_count=70552, instrument_count=70552)]
        objects = {_canon("2026-07-15", "CME"): {"OPTION": 70552}}
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert _types(out, "2026-07-15", "CME") == {"OPTION": 70552}
        assert stats["atoms_repaired"] == 0

    def test_non_captured_rows_are_never_touched(self, mod) -> None:
        """empty_confirmed / expected_unattempted / attempted_failed captured ZERO instruments and
        are honestly blank."""
        snapshot = [_row("2026-06-28", "CME", "", instrument_count=74005)]
        live = [
            _row("2026-06-28", "CME", "OPTION", row_count=2566, instrument_count=2566),
            _row("2026-06-28", "CME", "", capture_status="empty_confirmed", row_count=0, instrument_count=0),
        ]
        objects = {_canon("2026-06-28", "CME"): {"OPTION": 69212}}
        out, _stats, _ = _derive(mod, live, snapshot, objects)
        untouched = out[out["capture_status"] == "empty_confirmed"]
        assert len(untouched) == 1
        assert untouched.iloc[0]["instrument_type"] == ""
        assert int(untouched.iloc[0]["instrument_count"]) == 0


class TestIdempotency:
    def test_a_second_run_over_repaired_data_changes_nothing(self, mod) -> None:
        snapshot = [_row("2026-06-28", "CME", "", instrument_count=74005)]
        live = [
            _row("2026-06-28", "CME", "OPTION", row_count=2566, instrument_count=2566),
            _row("2026-06-28", "CME", "COMBO", row_count=228, instrument_count=228),
        ]
        objects = {_canon("2026-06-28", "CME"): {"OPTION": 69212, "COMBO": 4446}}
        first, stats1, _ = _derive(mod, live, snapshot, objects)
        assert stats1["atoms_repaired"] == 1
        _second, stats2, _ = _derive(mod, first.to_dict("records"), snapshot, objects)
        assert stats2["atoms_repaired"] == 0
        assert stats2["counts_restamped"] == 0
        assert stats2["sum_instrument_count_delta"] == 0


class TestGates:
    def test_gate_refuses_when_the_sum_would_go_down(self, mod) -> None:
        """A repair that lowers coverage is re-breaking the index, not fixing it."""
        assert not mod.gate(
            {
                "sum_instrument_count_before": 100,
                "sum_instrument_count_after": 90,
                "captured_atoms_lost": 0,
                "blank_captured_before": 0,
                "blank_captured_after": 0,
                "dup_row_keys_before": 153,
                "dup_row_keys_after": 153,
            }
        )

    def test_gate_refuses_when_the_sum_is_unchanged(self, mod) -> None:
        assert not mod.gate(
            {
                "sum_instrument_count_before": 100,
                "sum_instrument_count_after": 100,
                "captured_atoms_lost": 0,
                "blank_captured_before": 0,
                "blank_captured_after": 0,
                "dup_row_keys_before": 153,
                "dup_row_keys_after": 153,
            }
        )

    def test_gate_refuses_when_a_captured_atom_disappears(self, mod) -> None:
        assert not mod.gate(
            {
                "sum_instrument_count_before": 100,
                "sum_instrument_count_after": 200,
                "captured_atoms_lost": 1,
                "blank_captured_before": 0,
                "blank_captured_after": 0,
                "dup_row_keys_before": 153,
                "dup_row_keys_after": 153,
            }
        )

    def test_gate_tolerates_the_pre_existing_duplicate_baseline_but_not_a_new_one(self, mod) -> None:
        """tradfi carries 153 PRE-EXISTING duplicate row_keys (all KRX expected_unattempted /
        empty_confirmed pairs at count 0). Gating on ==0 would refuse forever; gating on
        no-INCREASE still catches a mint colliding with an existing row."""
        base = {
            "sum_instrument_count_before": 100,
            "sum_instrument_count_after": 200,
            "captured_atoms_lost": 0,
            "blank_captured_before": 0,
            "blank_captured_after": 0,
            "dup_row_keys_before": 153,
        }
        assert mod.gate({**base, "dup_row_keys_after": 153})
        assert not mod.gate({**base, "dup_row_keys_after": 154})

    def test_gate_refuses_a_blank_captured_regression(self, mod) -> None:
        assert not mod.gate(
            {
                "sum_instrument_count_before": 100,
                "sum_instrument_count_after": 200,
                "captured_atoms_lost": 0,
                "blank_captured_before": 0,
                "blank_captured_after": 1,
                "dup_row_keys_before": 153,
                "dup_row_keys_after": 153,
            }
        )

    def test_gate_passes_the_real_repair_shape(self, mod) -> None:
        assert mod.gate(
            {
                "sum_instrument_count_before": 46727155,
                "sum_instrument_count_after": 46798334,
                "captured_atoms_lost": 0,
                "blank_captured_before": 0,
                "blank_captured_after": 0,
                "dup_row_keys_before": 153,
                "dup_row_keys_after": 153,
            }
        )


class TestDuplicateGuard:
    def test_a_same_type_duplicate_inside_one_atom_is_collapsed_not_left_to_collide(self, mod) -> None:
        snapshot = [_row("2024-01-03", "CME", "", instrument_count=100)]
        live = [
            _row("2024-01-03", "CME", "OPTION", row_count=60, instrument_count=60),
            _row("2024-01-03", "CME", "OPTION", row_count=1, instrument_count=1),
        ]
        objects = {_canon("2024-01-03", "CME"): {"OPTION": 100}}
        out, stats, _ = _derive(mod, live, snapshot, objects)
        assert _types(out, "2024-01-03", "CME") == {"OPTION": 100}
        assert stats["dup_row_keys_after"] == 0
        assert stats["rows_dropped"] == 1
