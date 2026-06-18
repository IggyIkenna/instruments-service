"""Unit tests for canonicalize_instruments_store_index.py (N2 / F5 / N4 remediation).

Credential-free + GCS-free: exercises the PURE transforms (dedup / blank-classification /
malformed-drop / N4 report) on synthetic DataFrames that mirror the live-index defect shapes.

Plan ref: ``plans/active/instruments_mtds_subset_consistency_remediation_2026_06_17.md`` Step 4.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "scripts"))
    script_path = repo_root / "scripts" / "canonicalize_instruments_store_index.py"
    module_name = "_canonicalize_instruments_store_index_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


def _row(**kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
        "date": "2020-01-02",
        "venue": "CME",
        "data_type": "",
        "instrument_count": 0,
        "schema_version": 8,
        "written_at": "2026-05-04T00:00:00+00:00",
        "capture_status": "captured",
        "error_reason": "",
        "league_id": "",
        "instrument_id": "",
        "underlying": "",
        "chain": "",
    }
    base.update(kwargs)
    return base


class TestClassifyOrphanBlankRow:
    def test_positive_count_is_captured(self) -> None:
        status, reason = _mod.classify_orphan_blank_row("CME", "2020-01-02", 10349)
        assert status == "captured"
        assert reason == ""

    def test_weekend_zero_count_is_expected_weekend(self) -> None:
        # 2020-01-04 is a Saturday.
        status, reason = _mod.classify_orphan_blank_row("CME", "2020-01-04", 0)
        assert status == "empty_confirmed"
        assert reason == "EXPECTED_WEEKEND"

    def test_weekend_with_carry_forward_snapshot_stays_captured(self) -> None:
        # CME Saturday carry-forward snapshot HAS instruments → captured, never EXPECTED_WEEKEND.
        status, reason = _mod.classify_orphan_blank_row("CME", "2020-01-04", 11526)
        assert status == "captured"
        assert reason == ""

    def test_trading_day_zero_count_is_source_returned_zero(self) -> None:
        # 2020-01-02 is a Thursday (trading day) — a genuine in-coverage empty.
        status, reason = _mod.classify_orphan_blank_row("CME", "2020-01-02", 0)
        assert status == "empty_confirmed"
        assert reason == "SOURCE_RETURNED_ZERO"

    def test_non_numeric_count_treated_as_zero(self) -> None:
        status, _reason = _mod.classify_orphan_blank_row("CME", "2020-01-04", None)
        assert status == "empty_confirmed"


class TestCellKeyColumns:
    def test_includes_fine_grain_axes_when_present(self) -> None:
        df = pd.DataFrame([_row(league_id="BUNDESLIGA")])
        cols = _mod.cell_key_columns(df)
        assert cols[:3] == ["date", "venue", "data_type"]
        assert "league_id" in cols

    def test_base_key_when_no_fine_grain(self) -> None:
        df = pd.DataFrame([{"date": "d", "venue": "v", "data_type": "t", "capture_status": "captured"}])
        assert _mod.cell_key_columns(df) == ["date", "venue", "data_type"]


class TestDedupCells:
    def test_per_league_rows_not_collapsed(self) -> None:
        # CRITICAL: sports is per-LEAGUE — distinct league_id rows of the same
        # (date,venue,data_type) must NOT collapse (would destroy real per-shard rows).
        df = pd.DataFrame(
            [
                _row(data_type="TEAMS", league_id="BUNDESLIGA", instrument_count=0, capture_status="captured"),
                _row(data_type="TEAMS", league_id="LA_LIGA", instrument_count=0, capture_status="captured"),
                _row(data_type="TEAMS", league_id="", instrument_count=614, capture_status="captured"),
            ]
        )
        deduped, dropped = _mod.dedup_cells(df)
        assert dropped == 0
        assert len(deduped) == 3

    def test_v4_blank_shadow_loses_to_captured_v8_sibling(self) -> None:
        df = pd.DataFrame(
            [
                _row(schema_version=4, capture_status=None, written_at="2026-04-10T00:00:00+00:00"),
                _row(schema_version=8, capture_status="captured", written_at="2026-05-04T00:00:00+00:00"),
            ]
        )
        deduped, dropped = _mod.dedup_cells(df)
        assert len(deduped) == 1
        assert dropped == 1
        assert deduped.iloc[0]["capture_status"] == "captured"
        assert deduped.iloc[0]["schema_version"] == 8

    def test_distinct_cells_both_kept(self) -> None:
        df = pd.DataFrame([_row(venue="CME"), _row(venue="FX")])
        deduped, dropped = _mod.dedup_cells(df)
        assert len(deduped) == 2
        assert dropped == 0

    def test_idempotent_on_single_row_cells(self) -> None:
        df = pd.DataFrame([_row(venue="CME"), _row(venue="FX")])
        once, _ = _mod.dedup_cells(df)
        twice, dropped = _mod.dedup_cells(once)
        assert len(twice) == 2
        assert dropped == 0

    def test_none_vs_empty_string_in_key_collapses(self) -> None:
        # The v4-vs-v8 real defect: v4 row has instrument_id=None, v8 has "" — same cell.
        df = pd.DataFrame(
            [
                _row(
                    schema_version=4,
                    capture_status="captured",
                    instrument_id=None,
                    written_at="2026-04-10T00:00:00+00:00",
                ),
                _row(
                    schema_version=8,
                    capture_status="captured",
                    instrument_id="",
                    written_at="2026-05-04T00:00:00+00:00",
                ),
            ]
        )
        deduped, dropped = _mod.dedup_cells(df)
        assert dropped == 1
        assert len(deduped) == 1
        assert deduped.iloc[0]["schema_version"] == 8

    def test_status_priority_captured_beats_empty(self) -> None:
        df = pd.DataFrame(
            [
                _row(capture_status="empty_confirmed", schema_version=8),
                _row(capture_status="captured", schema_version=8),
            ]
        )
        deduped, _ = _mod.dedup_cells(df)
        assert deduped.iloc[0]["capture_status"] == "captured"


class TestDropMalformedSportsRows:
    def test_blank_data_type_and_blank_league_dropped(self) -> None:
        df = pd.DataFrame(
            [
                _row(venue="API_FOOTBALL", data_type="", league_id="", capture_status=None),
                _row(venue="ODDS_API", data_type="ODDS", league_id="BUNDESLIGA", capture_status="captured"),
            ]
        )
        kept, dropped = _mod.drop_malformed_sports_rows(df)
        assert dropped == 1
        assert len(kept) == 1
        assert kept.iloc[0]["data_type"] == "ODDS"

    def test_reference_date_all_row_preserved(self) -> None:
        # date='all' TEAMS reference row has a real data_type → never matched as malformed.
        df = pd.DataFrame([_row(date="all", venue="", data_type="TEAMS", league_id="", capture_status="captured")])
        kept, dropped = _mod.drop_malformed_sports_rows(df)
        assert dropped == 0
        assert len(kept) == 1
        assert kept.iloc[0]["date"] == "all"

    def test_blank_data_type_but_real_league_preserved(self) -> None:
        df = pd.DataFrame([_row(data_type="", league_id="BUNDESLIGA", capture_status="captured")])
        kept, dropped = _mod.drop_malformed_sports_rows(df)
        assert dropped == 0
        assert len(kept) == 1


class TestClassifyBlankRows:
    def test_blank_rows_get_typed_status(self) -> None:
        df = pd.DataFrame(
            [
                _row(date="2020-01-04", venue="CME", instrument_count=0, capture_status=None),
                _row(date="2020-01-02", venue="CME", instrument_count=10349, capture_status=None),
            ]
        )
        out, n = _mod.classify_blank_rows(df)
        assert n == 2
        assert _mod._is_blank(out["capture_status"]).sum() == 0
        # Saturday 0-count → EXPECTED_WEEKEND empty; weekday positive → captured.
        weekend = out[out["date"] == "2020-01-04"].iloc[0]
        assert weekend["capture_status"] == "empty_confirmed"
        assert weekend["error_reason"] == "EXPECTED_WEEKEND"
        listed = out[out["date"] == "2020-01-02"].iloc[0]
        assert listed["capture_status"] == "captured"

    def test_non_blank_rows_untouched(self) -> None:
        df = pd.DataFrame([_row(capture_status="captured", error_reason="")])
        out, n = _mod.classify_blank_rows(df)
        assert n == 0
        assert out.iloc[0]["capture_status"] == "captured"


class TestCountN4CompanionRows:
    def test_counts_captured_zero_count_rows(self) -> None:
        df = pd.DataFrame(
            [
                _row(data_type="TEAMS", capture_status="captured", instrument_count=0, league_id="BUNDESLIGA"),
                _row(data_type="TEAMS", capture_status="captured", instrument_count=614, league_id=""),
                _row(data_type="TEAMS", capture_status="empty_confirmed", instrument_count=0, league_id="X"),
            ]
        )
        assert _mod.count_n4_companion_rows(df) == 1


class TestCanonicalizeTradfi:
    def test_full_tradfi_canonicalisation(self) -> None:
        df = pd.DataFrame(
            [
                # cell A: v4 blank shadow + captured v8 sibling → dedup to the captured row.
                _row(
                    date="2020-01-02",
                    venue="CME",
                    schema_version=4,
                    capture_status=None,
                    instrument_count=10349,
                    written_at="2026-04-10T00:00:00+00:00",
                ),
                _row(
                    date="2020-01-02",
                    venue="CME",
                    schema_version=8,
                    capture_status="captured",
                    instrument_count=10349,
                    written_at="2026-05-04T00:00:00+00:00",
                ),
                # cell B: orphan v4 blank, Saturday 0-count → empty_confirmed[EXPECTED_WEEKEND].
                _row(date="2020-01-04", venue="CME", schema_version=4, capture_status=None, instrument_count=0),
                # cell C: orphan v4 blank, weekday positive → captured.
                _row(date="2020-01-03", venue="CME", schema_version=4, capture_status=None, instrument_count=500),
            ]
        )
        out, stats = _mod.canonicalize(df, "tradfi")
        assert stats["rows_in"] == 4
        assert stats["rows_out"] == 3  # cell A's shadow dropped
        assert stats["dedup_rows_dropped"] == 1
        assert stats["blank_status_out"] == 0
        assert stats["cells_with_multiple_rows_out"] == 0
        statuses = set(out["capture_status"])
        assert statuses == {"captured", "empty_confirmed"}
        sat = out[out["date"] == "2020-01-04"].iloc[0]
        assert sat["capture_status"] == "empty_confirmed"
        assert sat["error_reason"] == "EXPECTED_WEEKEND"

    def test_no_source_returned_zero_for_populated_weekend(self) -> None:
        # The N2 invariant: a populated weekend carry-forward snapshot stays captured.
        df = pd.DataFrame(
            [_row(date="2020-01-04", venue="CME", schema_version=4, capture_status=None, instrument_count=11526)]
        )
        out, _stats = _mod.canonicalize(df, "tradfi")
        assert out.iloc[0]["capture_status"] == "captured"
        assert "SOURCE_RETURNED_ZERO" not in set(out["error_reason"])


class TestCanonicalizeSports:
    def test_malformed_dropped_reference_preserved(self) -> None:
        df = pd.DataFrame(
            [
                _row(date="2019-01-01", venue="API_FOOTBALL", data_type="", league_id="", capture_status=None),
                _row(
                    date="all", venue="", data_type="TEAMS", league_id="", capture_status="captured", instrument_count=1
                ),
                _row(
                    date="2020-01-01",
                    venue="ODDS_API",
                    data_type="ODDS",
                    league_id="BUNDESLIGA",
                    capture_status="captured",
                    instrument_count=5,
                ),
            ]
        )
        out, stats = _mod.canonicalize(df, "sports")
        assert stats["malformed_sports_dropped"] == 1
        assert stats["blank_status_out"] == 0
        assert "all" in set(out["date"].astype(str))
        assert stats["cells_with_multiple_rows_out"] == 0
