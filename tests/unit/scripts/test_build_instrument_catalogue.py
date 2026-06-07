"""Unit tests — build_instrument_catalogue.py roll-up + monotonic guard, and the
audit_instrument_definition_completeness.py provisional completeness summary.

Tests cover (no GCS — pure functions + module-by-path load, mirroring the v2 enumerator tests):
  - build_catalogue_dataframe lifecycle math: first/last day windows, available_to=None when
    present on the latest snapshot day, delisted instrument gets available_to stamped, metadata
    follows the most-recent snapshot, instrument_key/instrument_id id-column fallback, empty input.
  - evaluate_monotonic_guard accept (first run / growth / equal) + reject (shrink) + override.
  - The catalogue output is consumable by enumerate_expected_universe._catalog_from_dataframe
    (no schema drift).
  - summarise_completeness: status tabulation, attempted_failed gap surfacing, provisional verdict.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 1 + Phase 0 P0 tests).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rollup() -> ModuleType:
    return _load_script_module("build_instrument_catalogue.py", "_build_instrument_catalogue_test_module")


@pytest.fixture(scope="module")
def audit() -> ModuleType:
    return _load_script_module(
        "audit_instrument_definition_completeness.py",
        "_audit_instrument_definition_completeness_test_module",
    )


def _snapshot(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    import io

    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    return buf.getvalue()


class _Blob:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeStorage:
    """Minimal duck-typed StorageClient for the by_date walk + dry-run promote path."""

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = blobs

    def list_blobs(self, bucket: str, prefix: str = "", **_: object) -> list[_Blob]:
        return [_Blob(name) for name in self._blobs if name.startswith(prefix)]

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        return self._blobs[blob_path]

    def blob_exists(self, bucket: str, blob_path: str) -> bool:
        return blob_path in self._blobs


# ---------------------------------------------------------------------------
# build_catalogue_dataframe — lifecycle math
# ---------------------------------------------------------------------------


def test_rollup_first_last_day_window(rollup: ModuleType) -> None:
    """available_from = first day present; available_to = last day when not on latest day."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        (d2, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        # AAA absent on d3 → delisted; BBB present on the latest day → still active.
        (d3, _snapshot([{"instrument_key": "BBB", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}

    assert by_id["AAA"]["available_from"] == "2024-01-01"
    assert by_id["AAA"]["available_to"] == "2024-01-02"  # delisted before the latest day
    assert by_id["BBB"]["available_from"] == "2024-01-03"
    assert by_id["BBB"]["available_to"] is None  # present on the latest day → still active


def test_rollup_active_on_latest_day_has_null_available_to(rollup: ModuleType) -> None:
    """An instrument present on the latest snapshot day has available_to=None."""
    d1, d2 = date(2024, 5, 1), date(2024, 5, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "X", "venue": "V"}])),
        (d2, _snapshot([{"instrument_key": "X", "venue": "V"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["available_from"] == "2024-05-01"
    assert row["available_to"] is None


def test_rollup_metadata_follows_most_recent_snapshot(rollup: ModuleType) -> None:
    """Metadata (venue/type/chain) is taken from the instrument's most-recent definition."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d2, _snapshot([{"instrument_key": "P", "venue": "NEW_V", "instrument_type": "PERP", "chain": "ARBITRUM"}])),
        (d1, _snapshot([{"instrument_key": "P", "venue": "OLD_V", "instrument_type": "PERP", "chain": "ETHEREUM"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["venue"] == "NEW_V"
    assert row["chain"] == "ARBITRUM"


def test_rollup_supports_instrument_id_column(rollup: ModuleType) -> None:
    """The id column falls back to instrument_id when instrument_key is absent."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe([(d1, _snapshot([{"instrument_id": "ZZZ", "venue": "V"}]))])
    assert df.to_dict("records")[0]["instrument_id"] == "ZZZ"


def test_rollup_skips_blank_ids_and_empty_frames(rollup: ModuleType) -> None:
    """Rows with no usable id are skipped; empty frames contribute only their day to the axis."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "A", "venue": "V"}, {"instrument_key": "", "venue": "V"}])),
        (d2, pd.DataFrame()),  # empty frame — still advances the latest-day axis
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    assert list(df["instrument_id"]) == ["A"]
    # A last seen on d1 but latest day is d2 (empty) → A is delisted.
    assert df.to_dict("records")[0]["available_to"] == "2024-01-01"


def test_rollup_empty_input_returns_catalog_columns(rollup: ModuleType) -> None:
    df = rollup.build_catalogue_dataframe([])
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)
    assert df.empty


def test_rollup_output_consumable_by_enumerator(rollup: ModuleType) -> None:
    """The rolled-up catalogue feeds enumerate_expected_universe._catalog_from_dataframe (no drift)."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_for_catalogue_rollup_test")
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    df = rollup.build_catalogue_dataframe(
        [
            (d1, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
            (d2, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
        ]
    )
    entries = enumerator._catalog_from_dataframe(df)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.instrument_id == "BTC-USDT"
    assert entry.venue == "BINANCE"
    assert entry.available_from == "2024-01-01"
    assert entry.available_to is None  # active on the latest day
    assert entry.data_type is None  # single-grain AG → no grain-binding (legacy iterate)


# ---------------------------------------------------------------------------
# build_prediction_catalogue_dataframe — multi-grain roll-up (cqg + conditionId)
# ---------------------------------------------------------------------------


def _pred_snap(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_prediction_rollup_emits_cqg_bundle_and_per_cid_grains(rollup: ModuleType) -> None:
    """One cqg row (bundle grain) + per-conditionId rows for trades AND market_lifecycle."""
    d1 = date(2025, 3, 14)
    snapshots = [
        (
            d1,
            "POLYMARKET",
            "BTC_UP_DOWN_DAILY",
            _pred_snap(
                [
                    {"instrument_key": "0xaaa", "venue": "POLYMARKET", "instrument_type": "prediction_market"},
                    {"instrument_key": "0xbbb", "venue": "POLYMARKET", "instrument_type": "prediction_market"},
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    recs = df.to_dict("records")
    # cqg bundle: exactly one row, instrument_id=cqg, data_type=prediction_canonical_question_group.
    cqg_rows = [r for r in recs if r["data_type"] == "prediction_canonical_question_group"]
    assert len(cqg_rows) == 1
    assert cqg_rows[0]["instrument_id"] == "BTC_UP_DOWN_DAILY"
    # per-conditionId: each of the 2 cids appears under BOTH trades and market_lifecycle.
    trades = {r["instrument_id"] for r in recs if r["data_type"] == "trades"}
    lifecycle = {r["instrument_id"] for r in recs if r["data_type"] == "market_lifecycle"}
    assert trades == {"0xaaa", "0xbbb"}
    assert lifecycle == {"0xaaa", "0xbbb"}
    # No condition_id leaks into the cqg bundle (the inflation bug this guards).
    assert "0xaaa" not in {r["instrument_id"] for r in cqg_rows}


def test_prediction_rollup_cqg_lifecycle_spans_member_window(rollup: ModuleType) -> None:
    """The cqg available_from/to span the union of its member conditionIds' presence."""
    d1, d2, d3 = date(2025, 3, 14), date(2025, 3, 15), date(2025, 3, 16)
    snapshots = [
        (d1, "POLYMARKET", "G", _pred_snap([{"instrument_key": "c1", "venue": "POLYMARKET"}])),
        (d2, "POLYMARKET", "G", _pred_snap([{"instrument_key": "c2", "venue": "POLYMARKET"}])),
        # d3 has a DIFFERENT cqg present → G's last day is d2 (delisted before latest).
        (d3, "POLYMARKET", "H", _pred_snap([{"instrument_key": "c3", "venue": "POLYMARKET"}])),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    cqg = {
        r["instrument_id"]: r for r in df.to_dict("records") if r["data_type"] == "prediction_canonical_question_group"
    }
    assert cqg["G"]["available_from"] == "2025-03-14"
    assert cqg["G"]["available_to"] == "2025-03-15"  # delisted before the latest day (d3)
    assert cqg["H"]["available_from"] == "2025-03-16"
    assert cqg["H"]["available_to"] is None  # present on the latest day → active


def test_prediction_rollup_skips_blank_cqg(rollup: ModuleType) -> None:
    """A blank cqg path segment yields no bundle row (honest skip, no empty-string bundle)."""
    d1 = date(2025, 3, 14)
    df = rollup.build_prediction_catalogue_dataframe(
        [(d1, "POLYMARKET", "", _pred_snap([{"instrument_key": "c1", "venue": "POLYMARKET"}]))]
    )
    assert not [r for r in df.to_dict("records") if r["data_type"] == "prediction_canonical_question_group"]


def test_prediction_rollup_consumable_by_enumerator_grain_bound(rollup: ModuleType) -> None:
    """The cqg row round-trips through _catalog_from_dataframe carrying its data_type binding,
    and the v2 prediction enumerator seeds ONLY that data_type at the cqg grain."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_for_pred_catalogue_test")
    d1 = date(2025, 3, 14)
    df = rollup.build_prediction_catalogue_dataframe(
        [(d1, "POLYMARKET", "BTC_UP_DOWN_DAILY", _pred_snap([{"instrument_key": "0xaaa", "venue": "POLYMARKET"}]))]
    )
    entries = enumerator._catalog_from_dataframe(df)
    cqg_entry = next(e for e in entries if e.instrument_id == "BTC_UP_DOWN_DAILY")
    assert cqg_entry.data_type == "prediction_canonical_question_group"
    # Enumerate over a date the cqg is alive, with an EMPTY present_set → expect a single
    # expected_unattempted row for the bundle data_type ONLY (NOT crossed with trades/lifecycle).
    rows = list(
        enumerator._enumerate_v2_prediction(
            [cqg_entry],
            [d1],
            ["trades", "prediction_canonical_question_group", "market_lifecycle"],
            present_set=set(),
            present_cols=["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"],
        )
    )
    assert len(rows) == 1
    assert rows[0].data_type == "prediction_canonical_question_group"
    assert rows[0].instrument_id == "BTC_UP_DOWN_DAILY"
    assert rows[0].capture_status == "expected_unattempted"


# ---------------------------------------------------------------------------
# build_sports_catalogue_dataframe — LEAGUE-grain roll-up (entity=leagues)
# ---------------------------------------------------------------------------


def _league_snap(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_sports_rollup_one_row_per_league_with_lifecycle(rollup: ModuleType) -> None:
    """One league-grain row per league_id; available_from/to track first/last day seen."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    snapshots = [
        (d1, _league_snap([{"league_id": "39", "name": "Premier League", "country": "England"}])),
        (d2, _league_snap([{"league_id": "39", "name": "Premier League", "country": "England"}])),
        # league 39 absent on d3 → delisted; league 140 present on latest → still active.
        (d3, _league_snap([{"league_id": "140", "name": "La Liga", "country": "Spain"}])),
    ]
    df = rollup.build_sports_catalogue_dataframe(snapshots)
    by_id = {row["league_id"]: row for row in df.to_dict("records")}

    assert set(by_id) == {"39", "140"}
    # instrument_id mirrors league_id; instrument_type="league"; venue blank (matches captured atom).
    assert by_id["39"]["instrument_id"] == "39"
    assert by_id["39"]["instrument_type"] == rollup.SPORTS_LEAGUE_INSTRUMENT_TYPE
    assert by_id["39"]["venue"] == ""
    assert by_id["39"]["data_type"] is None  # data_type axis handled by the enumerator
    assert by_id["39"]["available_from"] == "2024-01-01"
    assert by_id["39"]["available_to"] == "2024-01-02"  # delisted before the latest day
    assert by_id["140"]["available_from"] == "2024-01-03"
    assert by_id["140"]["available_to"] is None  # present on latest day → still active


def test_sports_rollup_skips_blank_league_ids_and_empty_frames(rollup: ModuleType) -> None:
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d1, _league_snap([{"league_id": "39"}, {"league_id": ""}, {"league_id": None}])),
        (d2, pd.DataFrame()),  # empty frame still advances the latest-day axis
    ]
    df = rollup.build_sports_catalogue_dataframe(snapshots)
    assert list(df["league_id"]) == ["39"]
    assert df.to_dict("records")[0]["available_to"] == "2024-01-01"  # delisted vs empty latest day


def test_sports_rollup_empty_input_returns_catalog_columns(rollup: ModuleType) -> None:
    df = rollup.build_sports_catalogue_dataframe([])
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)
    assert df.empty


def test_sports_enumerator_reads_rollup_catalogue_and_emits_expected_unattempted(rollup: ModuleType) -> None:
    """End-to-end: producer → _catalog_from_dataframe → enumerate_v2(sports).

    Seeds expected_unattempted at LEAGUE-grain for missing (league, data_type, date)
    cells, SKIPS captured cells, and SKIPS pre-source-coverage dates (owned by v1).
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_verify")
    # api_football FIXTURES source coverage starts 2018-01-01 → use 2024 dates (in-coverage).
    d1, d2, d3 = date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
    df = rollup.build_sports_catalogue_dataframe(
        [
            (d1, _league_snap([{"league_id": "39", "name": "PL"}])),
            (d2, _league_snap([{"league_id": "39", "name": "PL"}])),
            (d3, _league_snap([{"league_id": "39", "name": "PL"}])),
        ]
    )
    catalog = enumerator._catalog_from_dataframe(df)
    assert len(catalog) == 1
    assert catalog[0].league_id == "39"

    # League-grain present_set: league 39 FIXTURES captured on d2 only.
    present_cols = ["data_type", "league_id", "date"]
    present_set = {("FIXTURES", "39", "2024-06-02")}

    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d1, d2, d3],
            data_types=["FIXTURES"],
            present_set=present_set,
            present_cols=present_cols,
        )
    )
    by_date = {r.date: r for r in rows}
    # d2 captured → NOT emitted; d1 + d3 alive + missing → expected_unattempted, league-grain blanks.
    assert "2024-06-02" not in by_date
    assert by_date["2024-06-01"].capture_status == "expected_unattempted"
    assert by_date["2024-06-01"].league_id == "39"
    assert by_date["2024-06-01"].instrument_id == ""  # blanked → matches captured atom grain
    assert by_date["2024-06-01"].venue == ""
    assert by_date["2024-06-03"].capture_status == "expected_unattempted"


def test_sports_enumerator_skips_pre_source_coverage_dates(rollup: ModuleType) -> None:
    """Dates before the data_type's source coverage start are owned by v1 → v2 emits nothing."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_precov")
    # XG → understat, coverage starts 2015-01-16. 2015-01-01 is pre-coverage.
    d_pre = date(2015, 1, 1)
    df = rollup.build_sports_catalogue_dataframe([(d_pre, _league_snap([{"league_id": "39"}]))])
    catalog = enumerator._catalog_from_dataframe(df)
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d_pre],
            data_types=["XG"],
            present_set=set(),
            present_cols=["data_type", "league_id", "date"],
        )
    )
    assert rows == []  # pre-coverage → skipped (no double-emit with v1)


def test_sports_could_exist_denominator_never_shrinks(rollup: ModuleType) -> None:
    """Regression: a captured cell is ALWAYS in the present_set → never re-seeded.

    The could-exist universe (catalogue x data_types x dates) is a SUPERSET of the
    captured manifest cells, so the enumerator never emits for a captured cell —
    the coverage denominator can only grow, never shrink below captured.
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_superset")
    d1, d2 = date(2024, 6, 1), date(2024, 6, 2)
    df = rollup.build_sports_catalogue_dataframe(
        [(d1, _league_snap([{"league_id": "39"}])), (d2, _league_snap([{"league_id": "39"}]))]
    )
    catalog = enumerator._catalog_from_dataframe(df)
    present_cols = ["data_type", "league_id", "date"]
    # Every (FIXTURES, 39, date) is captured → present_set covers the whole could-exist universe.
    present_set = {("FIXTURES", "39", "2024-06-01"), ("FIXTURES", "39", "2024-06-02")}
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d1, d2],
            data_types=["FIXTURES"],
            present_set=present_set,
            present_cols=present_cols,
        )
    )
    # No expected_unattempted for already-captured cells → denominator == captured (no shrink).
    assert [r for r in rows if r.capture_status == "expected_unattempted"] == []


# ---------------------------------------------------------------------------
# evaluate_monotonic_guard
# ---------------------------------------------------------------------------


def test_guard_accepts_first_run(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(10, None, allow_shrink=False)
    assert decision.accept
    assert decision.reason == "no_prior_catalogue"


def test_guard_accepts_growth_and_equal(rollup: ModuleType) -> None:
    assert rollup.evaluate_monotonic_guard(11, 10, allow_shrink=False).accept
    assert rollup.evaluate_monotonic_guard(10, 10, allow_shrink=False).accept


def test_guard_rejects_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=False)
    assert not decision.accept
    assert decision.reason == "shrink_blocked"


def test_guard_override_allows_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=True)
    assert decision.accept
    assert decision.reason == "shrink_overridden"


# ---------------------------------------------------------------------------
# summarise_completeness (audit tool)
# ---------------------------------------------------------------------------


def test_completeness_complete_when_no_failed_cells(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-01", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "empty_confirmed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.attempted_failed == 0
    assert report.status_counts["captured"] == 1
    assert report.status_counts["empty_confirmed"] == 1


def test_completeness_surfaces_attempted_failed_gaps(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-02", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
            {"date": "2024-01-03", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert not report.is_complete
    assert report.attempted_failed == 2
    assert report.failed_by_venue["OKX"] == 2
    assert ("OKX", "2024-01-02", "INSTRUMENTS") in report.gap_sample


def test_completeness_blank_status_coerced_to_captured(audit: ModuleType) -> None:
    """Legacy blank/NaN capture_status mirrors the manifest read path (→ captured), not a gap."""
    index_df = pd.DataFrame([{"date": "2024-01-01", "venue": "V", "data_type": "INSTRUMENTS", "capture_status": None}])
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.status_counts["captured"] == 1


def test_completeness_empty_index(audit: ModuleType) -> None:
    report = audit.summarise_completeness(pd.DataFrame(), "cefi")
    assert report.total_rows == 0
    assert report.is_complete  # vacuously — no failed cells (provisional)


# ---------------------------------------------------------------------------
# _iter_by_date_snapshots — concurrent walk + day parsing + max_blobs cap
# ---------------------------------------------------------------------------


def test_iter_by_date_walk_parses_day_and_reads_frames(rollup: ModuleType) -> None:
    blobs = {
        "instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "BTC-USDT", "venue": "BINANCE"}]
        ),
        "instrument_availability/by_date/day=2024-01-02/venue=OKX/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "ETH-USDT", "venue": "OKX"}]
        ),
        "instrument_availability/by_date/day=2024-01-02/venue=OKX/_SUCCESS": b"not-parquet",
    }
    out = list(rollup._iter_by_date_snapshots(_FakeStorage(blobs), "bkt", "instrument_availability/by_date"))
    days = sorted(str(d) for d, _ in out)
    assert days == ["2024-01-01", "2024-01-02"]  # non-parquet skipped
    assert all(not f.empty for _, f in out)


def test_iter_by_date_max_blobs_truncates(rollup: ModuleType) -> None:
    blobs = {
        f"instrument_availability/by_date/day=2024-01-0{i}/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": f"I{i}", "venue": "V"}]
        )
        for i in range(1, 6)
    }
    out = list(
        rollup._iter_by_date_snapshots(_FakeStorage(blobs), "bkt", "instrument_availability/by_date", max_blobs=2)
    )
    assert len(out) == 2  # truncated (path-sorted → earliest two days)


def test_run_rollup_max_blobs_forces_dry_run(rollup: ModuleType) -> None:
    """--max-blobs must force dry-run (a truncated walk is never promotable)."""
    blobs = {
        "instrument_availability/by_date/day=2024-01-01/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "A", "venue": "V"}]
        ),
    }
    code = rollup.run_rollup(
        "cefi",
        allow_shrink=False,
        dry_run=False,  # caller asked to write...
        max_blobs=1,  # ...but the cap forces dry-run
        storage=_FakeStorage(blobs),
    )
    assert code == 0
    # No catalog object was written (fake has only the input blob).
    assert "prod/catalog.parquet" not in _FakeStorage(blobs)._blobs


# ---------------------------------------------------------------------------
# Phase-3 cefi verification — the v2 cefi enumerator reads the rolled-up
# catalogue and emits NOT_LISTED / DELISTED / expected_unattempted correctly.
# ---------------------------------------------------------------------------


def test_cefi_enumerator_reads_rollup_catalogue_and_emits_expected_unattempted(rollup: ModuleType) -> None:
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_cefi_verify")
    d1, d2, d3, d4 = date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3), date(2024, 6, 4)

    # Producer roll-up: OLDCOIN only on d1 (delisted, latest=d3); BTC on d2,d3 (active).
    catalogue_df = rollup.build_catalogue_dataframe(
        [
            (d1, _snapshot([{"instrument_key": "OLDCOIN", "venue": "TESTVENUE", "instrument_type": "SPOT_PAIR"}])),
            (d2, _snapshot([{"instrument_key": "BTC", "venue": "TESTVENUE", "instrument_type": "SPOT_PAIR"}])),
            (d3, _snapshot([{"instrument_key": "BTC", "venue": "TESTVENUE", "instrument_type": "SPOT_PAIR"}])),
        ]
    )
    catalog = enumerator._catalog_from_dataframe(catalogue_df)

    # Manifest says BTC was captured on d3 only.
    # present_cols default: venue, chain, data_type, instrument_type, instrument_id, league_id, date
    present_set = {("TESTVENUE", "", "INSTRUMENTS", "SPOT_PAIR", "BTC", "", "2024-06-03")}

    rows = list(
        enumerator.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=[d1, d2, d3, d4],
            data_types=["INSTRUMENTS"],
            present_set=present_set,
        )
    )
    by_key = {(r.instrument_id, r.date): r for r in rows}

    # BTC before available_from (d2) → NOT_LISTED (honest empty_confirmed)
    assert by_key[("BTC", "2024-06-01")].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert by_key[("BTC", "2024-06-01")].capture_status == "empty_confirmed"
    # BTC alive on d2, no manifest row → expected_unattempted
    assert by_key[("BTC", "2024-06-02")].capture_status == "expected_unattempted"
    # BTC alive on d3 AND captured (present_set) → NOT emitted
    assert ("BTC", "2024-06-03") not in by_key
    # BTC alive on d4 (available_to=None), no manifest row → expected_unattempted
    assert by_key[("BTC", "2024-06-04")].capture_status == "expected_unattempted"
    # OLDCOIN after available_to (d1) → DELISTED
    assert by_key[("OLDCOIN", "2024-06-02")].reason == "EXPECTED_INSTRUMENT_DELISTED"


# ---------------------------------------------------------------------------
# _tune_download_pool — enlarge GCS HTTP pool (best-effort, guarded)
# ---------------------------------------------------------------------------


class _FakeHttp:
    def __init__(self) -> None:
        self.mounted: dict[str, object] = {}

    def mount(self, prefix: str, adapter: object) -> None:
        self.mounted[prefix] = adapter


class _FakeNativeClient:
    def __init__(self) -> None:
        self._http = _FakeHttp()


class _FakeGcpStorage:
    provider_name = "gcp"

    def __init__(self) -> None:
        self._client = _FakeNativeClient()


class _FakeAwsStorage:
    provider_name = "aws"


def test_tune_download_pool_mounts_adapter_on_gcp(rollup: ModuleType) -> None:
    storage = _FakeGcpStorage()
    rollup._tune_download_pool(storage, 16)
    assert set(storage._client._http.mounted) == {"https://", "http://"}
    # pool sized to the worker count
    adapter = storage._client._http.mounted["https://"]
    assert getattr(adapter, "_pool_maxsize", 16) == 16


def test_tune_download_pool_noop_on_non_gcp(rollup: ModuleType) -> None:
    # No provider / non-gcp / missing native client must not raise.
    rollup._tune_download_pool(_FakeAwsStorage(), 16)
    rollup._tune_download_pool(object(), 16)  # no provider_name / _client at all


def test_instruments_store_bucket_for_prediction_uses_flat_kind(
    rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prediction resolves the dedicated FLAT kind ``instruments-store-prediction``.

    Regression for the catalogue-rollup crash (slot-5 2026-06-07): the per-AG
    ``instruments-store`` dict has NO ``PREDICTION`` entry, so the prior
    ``get_write_bucket_name("instruments", "prediction")`` raised ``BucketNamingError``
    and the prediction roll-up crashed at bucket resolution before any walk.
    """
    captured: list[tuple[str, str | None]] = []

    def _fake(*, cloud: str, kind: str, asset_group: str | None = None) -> str:
        _ = cloud
        captured.append((kind, asset_group))
        return f"bkt-{kind}-{asset_group or 'flat'}"

    monkeypatch.setattr(rollup, "resolve_bucket_name", _fake)
    assert rollup._instruments_store_bucket_for("prediction") == "bkt-instruments-store-prediction-flat"
    assert ("instruments-store-prediction", None) in captured
    # Every OTHER AG uses the per-AG instruments-store dict (unchanged behaviour).
    captured.clear()
    rollup._instruments_store_bucket_for("cefi")
    assert ("instruments-store", "cefi") in captured


def test_instruments_store_bucket_for_unknown_raises(rollup: ModuleType) -> None:
    with pytest.raises(ValueError, match="Unknown asset_group"):
        rollup._instruments_store_bucket_for("bogus")
