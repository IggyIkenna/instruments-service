"""Unit tests for manifest_diff.py (CF-20 / V2 — projected-vs-current goalpost diff).

Credential-free + GCS-free: small SYNTHETIC parquet fixtures (tmp_path) exercise the
full local pipeline (load → asset_group scope → cell index → grain-aware diff →
transition matrix → group row deltas → JSON out → exit code). The single gs:// read
in ``load_index`` is thin (same UTL helper as the sibling scaffolds).

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V2
item "Manifest-diff tool (projected-vs-current)".
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "manifest_diff.py"
    module_name = "_manifest_diff_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


def _row(
    date: str = "2024-06-01",
    data_type: str = "trades",
    venue: str = "BINANCE-FUTURES",
    chain: str = "",
    instrument_type: str = "perpetual",
    capture_status: str = "captured",
    asset_group: str = "cefi",
    source: str = "tardis",
) -> dict[str, str]:
    return {
        "date": date,
        "data_type": data_type,
        "venue": venue,
        "chain": chain,
        "instrument_type": instrument_type,
        "capture_status": capture_status,
        "asset_group": asset_group,
        "source": source,
    }


def _write_parquet(tmp_path: Path, name: str, rows: list[dict[str, str]]) -> str:
    path = tmp_path / name
    pd.DataFrame(rows).to_parquet(path, index=False)
    return str(path)


def _diff(projected: list[dict[str, str]], current: list[dict[str, str]]):
    proj_rows: list[dict[str, object]] = list(projected)
    cur_rows: list[dict[str, object]] = list(current)
    return _mod.diff_cell_indexes(_mod.build_cell_index(proj_rows), _mod.build_cell_index(cur_rows))


class TestLoadAndScope:
    def test_load_index_local_parquet(self, tmp_path: Path) -> None:
        path = _write_parquet(tmp_path, "idx.parquet", [_row()])
        df = _mod.load_index(path)
        assert len(df) == 1
        assert df.iloc[0]["venue"] == "BINANCE-FUTURES"

    def test_rows_for_asset_group_keeps_blank_and_matching(self) -> None:
        df = pd.DataFrame(
            [
                _row(asset_group="cefi"),
                _row(asset_group="", venue="OKX"),  # legacy pre-v9 row — kept
                _row(asset_group="defi", venue="UNISWAP"),  # other AG — dropped
            ]
        )
        rows = _mod.rows_for_asset_group(df, "cefi")
        assert {r["venue"] for r in rows} == {"BINANCE-FUTURES", "OKX"}

    def test_rows_for_asset_group_without_column_keeps_all(self) -> None:
        df = pd.DataFrame([{k: v for k, v in _row().items() if k != "asset_group"}])
        assert len(_mod.rows_for_asset_group(df, "cefi")) == 1


class TestDiffCells:
    def test_added_cell(self) -> None:
        diff = _diff(projected=[_row(), _row(date="2024-06-02")], current=[_row()])
        assert len(diff.added) == 1
        assert diff.added[0].date == "2024-06-02"
        assert not diff.removed and not diff.changed and diff.unchanged == 1

    def test_removed_cell_is_a_regression(self) -> None:
        diff = _diff(projected=[_row()], current=[_row(), _row(data_type="book_snapshot_5")])
        assert len(diff.removed) == 1
        assert diff.removed[0].data_type == "book_snapshot_5"
        assert diff.is_regression

    def test_status_transition_matrix(self) -> None:
        diff = _diff(
            projected=[_row(capture_status="captured"), _row(date="2024-06-02", capture_status="captured")],
            current=[
                _row(capture_status="expected_unattempted"),
                _row(date="2024-06-02", capture_status="expected_unattempted"),
            ],
        )
        assert diff.transitions == {"expected_unattempted->captured": 2}
        assert len(diff.changed) == 2
        assert not diff.is_regression  # an upgrade is the expected migration shape

    def test_captured_downgrade_is_a_regression(self) -> None:
        diff = _diff(
            projected=[_row(capture_status="attempted_failed")],
            current=[_row(capture_status="captured")],
        )
        assert diff.captured_regressions == 1
        assert diff.is_regression

    def test_unchanged_cell(self) -> None:
        diff = _diff(projected=[_row()], current=[_row()])
        assert diff.unchanged == 1
        assert not diff.added and not diff.removed and not diff.changed

    def test_multi_source_rows_collapse_by_priority(self) -> None:
        # two per-source rows on the SAME cell: ≥1 captured → the cell is captured
        current = [
            _row(capture_status="attempted_failed", source="databento"),
            _row(capture_status="captured", source="massive"),
        ]
        diff = _diff(projected=[_row(capture_status="captured")], current=current)
        assert diff.unchanged == 1 and not diff.changed


class TestWildcardGrain:
    """The orphan sweep's prediction A=0 lesson: blank manifest fields are wildcards —
    coarse-vs-fine keys must never report false adds/removes."""

    def test_coarse_current_covers_fine_projected_no_false_add(self) -> None:
        coarse_current = [_row(asset_group="prediction", venue="POLYMARKET", chain="", instrument_type="")]
        fine_projected = [_row(asset_group="prediction", venue="POLYMARKET", chain="POLYGON", instrument_type="")]
        diff = _diff(projected=fine_projected, current=coarse_current)
        assert not diff.added and not diff.removed
        assert diff.unchanged == 1

    def test_fine_projected_covers_coarse_current_no_false_remove(self) -> None:
        # the reverse direction: the projection REFINES the grain (adds chain=) —
        # the coarse current cell is still covered, never reported removed
        coarse_current = [_row(asset_group="prediction", venue="POLYMARKET", chain="", instrument_type="")]
        fine_projected = [_row(asset_group="prediction", venue="POLYMARKET", chain="POLYGON", instrument_type="")]
        diff = _diff(projected=fine_projected, current=coarse_current)
        assert not diff.removed

    def test_wildcard_match_still_reports_status_change(self) -> None:
        coarse_current = [
            _row(asset_group="prediction", venue="POLYMARKET", chain="", capture_status="expected_unattempted")
        ]
        fine_projected = [
            _row(asset_group="prediction", venue="POLYMARKET", chain="POLYGON", capture_status="captured")
        ]
        diff = _diff(projected=fine_projected, current=coarse_current)
        assert diff.transitions == {"expected_unattempted->captured": 1}

    def test_different_venue_is_not_covered_by_wildcard(self) -> None:
        diff = _diff(
            projected=[_row(venue="OKX")],
            current=[_row(venue="BINANCE-FUTURES")],
        )
        assert len(diff.added) == 1 and len(diff.removed) == 1

    def test_coarse_query_scan_resolves_by_status_priority(self) -> None:
        # a coarse projected row probed against a finer index with MIXED statuses →
        # the covering union resolves captured-first (deterministic, not dict-order)
        fine_current = [
            _row(chain="ETHEREUM", capture_status="attempted_failed"),
            _row(chain="POLYGON", capture_status="captured"),
        ]
        coarse_projected = [_row(chain="", capture_status="captured")]
        diff = _diff(projected=coarse_projected, current=fine_current)
        assert diff.unchanged >= 1
        assert not diff.added


class TestGroupRowDeltas:
    def test_row_deltas_per_group(self) -> None:
        current: list[dict[str, object]] = [_row(), _row(), _row(data_type="book_snapshot_5")]
        projected: list[dict[str, object]] = [_row(), _row(), _row(), _row(venue="OKX")]
        deltas = _mod.group_row_deltas(current, projected, "cefi")
        by_key = {(d["data_type"], d["venue"]): d for d in deltas}
        assert by_key[("trades", "BINANCE-FUTURES")]["delta"] == 1
        assert by_key[("book_snapshot_5", "BINANCE-FUTURES")]["delta"] == -1
        assert by_key[("trades", "OKX")]["delta"] == 1

    def test_blank_asset_group_rows_fold_into_cli_ag(self) -> None:
        deltas = _mod.group_row_deltas([dict(_row(), asset_group="")], [], "cefi")
        assert deltas[0]["asset_group"] == "cefi"


class TestMainEndToEnd:
    def test_clean_upgrade_exits_0_and_writes_json(self, tmp_path: Path) -> None:
        current = _write_parquet(tmp_path, "current.parquet", [_row(capture_status="expected_unattempted")])
        projected = _write_parquet(
            tmp_path, "projected.parquet", [_row(capture_status="captured"), _row(date="2024-06-02")]
        )
        out = tmp_path / "diff.json"
        code = _mod.main(["--asset-group", "cefi", "--projected", projected, "--current", current, "--out", str(out)])
        assert code == 0
        report = json.loads(out.read_text())
        assert report["cells"] == {"added": 1, "removed": 0, "changed": 1, "unchanged": 0}
        assert report["status_transitions"] == {"expected_unattempted->captured": 1}
        assert report["regressions"]["is_regression"] is False
        assert report["samples"]["added"][0]["date"] == "2024-06-02"

    def test_removed_cell_exits_1(self, tmp_path: Path) -> None:
        current = _write_parquet(tmp_path, "current.parquet", [_row(), _row(data_type="book_snapshot_5")])
        projected = _write_parquet(tmp_path, "projected.parquet", [_row()])
        code = _mod.main(["--asset-group", "cefi", "--projected", projected, "--current", current])
        assert code == 1

    def test_timestamp_dates_normalise(self, tmp_path: Path) -> None:
        # parquet date columns frequently round-trip as Timestamps — both sides must
        # key on the same YYYY-MM-DD string or every cell false-diffs
        cur_df = pd.DataFrame([_row()])
        cur_df["date"] = pd.to_datetime(cur_df["date"])
        cur_path = tmp_path / "current.parquet"
        cur_df.to_parquet(cur_path, index=False)
        projected = _write_parquet(tmp_path, "projected.parquet", [_row()])
        code = _mod.main(["--asset-group", "cefi", "--projected", projected, "--current", str(cur_path)])
        assert code == 0


def test_coarse_query_unions_fine_rows_captured_dominance() -> None:
    """A blank-IT (coarse) cell whose bucket ALSO holds fine-IT captured rows resolves
    captured — the re-emitted blank-IT absence row must not shadow the cell (the tradfi
    2026-06-11 false-regression class)."""
    index = _mod.build_cell_index(
        [
            {"date": "2020-01-02", "data_type": "ohlcv_1m", "venue": "CME", "instrument_type": "", "capture_status": "attempted_failed"},
            {"date": "2020-01-02", "data_type": "ohlcv_1m", "venue": "CME", "instrument_type": "future", "capture_status": "captured"},
        ]
    )
    assert _mod.lookup_status(index, "2020-01-02", "ohlcv_1m", ("CME", "", "")) == "captured"
    # The fine query still resolves its own row first.
    assert _mod.lookup_status(index, "2020-01-02", "ohlcv_1m", ("CME", "", "future")) == "captured"
    # A coarse cell with ONLY the absence row stays attempted_failed (no false-green).
    index2 = _mod.build_cell_index(
        [{"date": "2020-01-02", "data_type": "ohlcv_1m", "venue": "CME", "instrument_type": "", "capture_status": "attempted_failed"}]
    )
    assert _mod.lookup_status(index2, "2020-01-02", "ohlcv_1m", ("CME", "", "")) == "attempted_failed"
