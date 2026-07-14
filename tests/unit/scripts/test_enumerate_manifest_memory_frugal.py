"""Unit tests — memory-frugal manifest load path (sports enum OOM fix, 2026-07-14).

The nightly ``expected-universe-v2-sports`` job SIGKILLed at 8Gi loading the
~5.75M-row x 42-column sports availability index FULL-WIDTH (~5.6GB in pandas,
peaking ~6GB at the per-VM ``pd.concat``). The fix column-projects every
manifest parquet read to the present-set key grain + ``capture_status``
(:func:`_needed_manifest_columns` / :func:`_read_manifest_parquet_projected`)
and filters captured rows in the same select (:func:`_build_captured_set`).

These tests prove the projection is SEMANTICS-PRESERVING:

1. Old (pre-fix, full-width) vs new implementations produce byte-identical
   present/captured sets on fixture frames incl. edge cases (NaN keys, NaN /
   missing ``capture_status``, missing ``date``, duplicate rows, numeric key
   dtypes, empty frame) — so the oscillation guard drops byte-identical rows.
2. The guard's end-to-end drop behaviour through :func:`enumerate_v2` is
   identical whichever implementation built ``captured_set``.
3. Coarse memory-shape guard: the sets built from a frame pre-projected to
   ``_needed_manifest_columns`` equal the sets built from the full-width frame
   (the projection loses nothing), and the sports projection is pinned to the
   4 key columns only (never the full-width index again).

Issue: plans/active/issues/is_daily_enum_prediction_sports_fail_despite_coercion_2026_07_06.md
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module loader (mirrors the v2 test pattern — script lives outside the package)
# ---------------------------------------------------------------------------


def _load_enumerator_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "enumerate_expected_universe.py"
    module_name = "_enumerate_expected_universe_memfrugal_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


enumerator_module = _load_enumerator_module()

# The full 42-column production sports index schema (verified live 2026-07-14).
_SPORTS_INDEX_SCHEMA: list[str] = [
    "date",
    "venue",
    "data_type",
    "service_name",
    "instrument_count",
    "written_at",
    "schema_version",
    "timeframe",
    "league_id",
    "chain",
    "instrument_type",
    "capture_status",
    "error_reason",
    "attempted_at",
    "expected",
    "available",
    "underlying",
    "feature_group",
    "model_family",
    "training_period",
    "strategy_id",
    "client_id",
    "instruction_type",
    "instrument_id",
    "quote_asset",
    "margin_type",
    "combo_type",
    "leg_weights",
    "asset_group",
    "row_count",
    "enumerator_run_id",
    "feature_family",
    "service_emission_state",
    "last_emission_decision_at",
    "expected_window_completeness_fraction",
    "pipeline_mode",
    "source",
    "fixture_id",
    "job_id",
    "transport",
    "cadence",
    "available_at",
]


# ---------------------------------------------------------------------------
# Pre-fix reference implementations (verbatim copies of the shipped-at-ba306543
# full-width code paths) — the equivalence oracle.
# ---------------------------------------------------------------------------


def _legacy_build_present_set(df: pd.DataFrame, asset_group: str) -> set[tuple[str, ...]]:
    if df.empty:
        return set()
    if "date" not in df.columns:
        return set()
    available = enumerator_module._present_cols_for(asset_group, list(df.columns))
    df_subset = df[available].fillna("").astype(str)
    return {tuple(row) for row in df_subset.itertuples(index=False, name=None)}


def _legacy_build_captured_set(df: pd.DataFrame, asset_group: str) -> set[tuple[str, ...]]:
    if df.empty or "date" not in df.columns or "capture_status" not in df.columns:
        return set()
    captured = df[df["capture_status"].fillna("").astype(str) == "captured"]
    if captured.empty:
        return set()
    available = enumerator_module._present_cols_for(asset_group, list(captured.columns))
    df_subset = captured[available].fillna("").astype(str)
    return {tuple(row) for row in df_subset.itertuples(index=False, name=None)}


# ---------------------------------------------------------------------------
# Fixture frames — full-width-ish with edge cases
# ---------------------------------------------------------------------------


def _rich_manifest_frame() -> pd.DataFrame:
    """Full-width-ish manifest with the edge cases the equivalence must survive.

    Rows: captured / empty_confirmed / failed / expected_unattempted / NaN and
    "" capture_status; NaN league_id + venue; a NUMERIC league_id (astype(str)
    coercion path); duplicate captured rows (set dedup); extra non-key columns.
    """
    return pd.DataFrame(
        {
            "date": [
                "2026-06-06",
                "2026-06-06",
                "2026-06-07",
                "2026-03-18",
                "2026-03-19",
                "2026-03-19",
                "2026-04-01",
                "2026-04-02",
            ],
            "venue": ["api_football", None, "api_football", "footystats", None, None, "understat", "understat"],
            "data_type": ["MATCHES", "MATCHES", "MATCHES", "ODDS", "XG", "XG", "STANDINGS", "STANDINGS"],
            "instrument_type": ["league"] * 8,
            "instrument_id": ["", "", "", "", "", "", "", ""],
            "chain": [""] * 8,
            "league_id": ["SEGUNDA_DIVISION", "SEGUNDA_DIVISION", None, "BRASILEIRAO", 39, 39, "EPL", "EPL"],
            "underlying": [""] * 8,
            "capture_status": [
                "captured",
                "captured",  # duplicate captured atom
                "empty_confirmed",
                "captured",
                None,  # NaN capture_status
                "",  # blank capture_status
                "failed",
                "expected_unattempted",
            ],
            # Non-key columns that the projection must be free to drop.
            "written_at": ["2026-07-01T00:00:00Z"] * 8,
            "error_reason": [None] * 8,
            "row_count": [10, 10, 0, 5, None, None, 0, 0],
            "source": ["api_football"] * 8,
        }
    )


_EDGE_FRAMES: dict[str, pd.DataFrame] = {
    "rich": _rich_manifest_frame(),
    "empty": pd.DataFrame(),
    "no_date_column": pd.DataFrame({"data_type": ["MATCHES"], "league_id": ["EPL"], "capture_status": ["captured"]}),
    "no_capture_status_column": pd.DataFrame({"data_type": ["MATCHES"], "league_id": ["EPL"], "date": ["2026-06-06"]}),
    "zero_captured_rows": pd.DataFrame(
        {
            "data_type": ["MATCHES"],
            "league_id": ["EPL"],
            "date": ["2026-06-06"],
            "capture_status": ["empty_confirmed"],
        }
    ),
    "all_captured": pd.DataFrame(
        {
            "data_type": ["MATCHES", "ODDS"],
            "league_id": ["EPL", "EPL"],
            "date": ["2026-06-06", "2026-06-06"],
            "capture_status": ["captured", "captured"],
        }
    ),
}


# ---------------------------------------------------------------------------
# 1. Old-vs-new equivalence on fixture frames
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("frame_name", sorted(_EDGE_FRAMES))
@pytest.mark.parametrize("asset_group", ["sports", "cefi", "tradfi", "prediction"])
def test_captured_set_new_equals_legacy(frame_name: str, asset_group: str) -> None:
    df = _EDGE_FRAMES[frame_name].copy()
    assert enumerator_module._build_captured_set(df, asset_group) == _legacy_build_captured_set(df, asset_group)


@pytest.mark.parametrize("frame_name", sorted(_EDGE_FRAMES))
@pytest.mark.parametrize("asset_group", ["sports", "cefi", "tradfi", "prediction"])
def test_present_set_new_equals_legacy(frame_name: str, asset_group: str) -> None:
    df = _EDGE_FRAMES[frame_name].copy()
    assert enumerator_module._build_present_set(df, asset_group) == _legacy_build_present_set(df, asset_group)


def test_captured_set_rich_frame_expected_content() -> None:
    """Pin the exact drop-set on the rich fixture (not just old==new)."""
    got = enumerator_module._build_captured_set(_rich_manifest_frame(), "sports")
    assert got == {
        ("MATCHES", "SEGUNDA_DIVISION", "2026-06-06"),
        ("ODDS", "BRASILEIRAO", "2026-03-18"),
    }


# ---------------------------------------------------------------------------
# 2. Guard end-to-end: identical drops whichever impl built captured_set
# ---------------------------------------------------------------------------


def test_oscillation_guard_drops_identical_rows_old_vs_new_captured_set() -> None:
    # Add a captured atom for data_type "lineups" (NOT in
    # SPORTS_DATA_TYPE_TO_SOURCE, so the per-source season/fixture gates stay
    # out of the way and pre-listing dates deterministically emit
    # EXPECTED_INSTRUMENT_NOT_LISTED) and league EPL (must be in
    # LEAGUE_REGISTRY or the 2026-07-13 de-registration gate skips the league
    # entirely) — same choices as the shipped oscillation-guard tests.
    manifest = pd.concat(
        [
            _rich_manifest_frame(),
            pd.DataFrame(
                {
                    "date": ["2026-06-06"],
                    "venue": ["api_football"],
                    "data_type": ["lineups"],
                    "league_id": ["EPL"],
                    "capture_status": ["captured"],
                }
            ),
        ],
        ignore_index=True,
        sort=False,
    )
    catalog = [
        enumerator_module.InstrumentCatalogEntry(
            instrument_id="LEAGUE-EPL",
            instrument_type=enumerator_module._SPORTS_LEAGUE_GRAIN_INSTRUMENT_TYPE,
            venue="api_football",
            chain="",
            league_id="EPL",
            # Listing AFTER the captured atom's date but INSIDE the enumeration
            # window (a league whose available_from is beyond the window end is
            # skipped wholesale by the enumerator, emitting nothing).
            available_from="2026-06-08",
            available_to="2026-06-15",
            market_created_at=None,
            settlement_time=None,
        )
    ]
    date_axis = [date.fromisoformat(d) for d in ("2026-06-06", "2026-06-07", "2026-06-08")]
    present_cols = enumerator_module._present_cols_for("sports", list(manifest.columns))

    def _run(captured_set: set[tuple[str, ...]]) -> list:
        return list(
            enumerator_module.enumerate_v2(
                asset_group="sports",
                catalog=catalog,
                date_axis=date_axis,
                data_types=["lineups"],
                present_set=enumerator_module._build_present_set(manifest, "sports"),
                present_cols=present_cols,
                captured_set=captured_set,
            )
        )

    rows_new = _run(enumerator_module._build_captured_set(manifest, "sports"))
    rows_legacy = _run(_legacy_build_captured_set(manifest, "sports"))
    assert rows_new == rows_legacy
    # And the guard actually dropped the captured atom (2026-06-06 pre-listing
    # empty_confirmed suppressed) while siblings survive: 2026-06-07 pre-listing
    # empty_confirmed kept, 2026-06-08 alive-day expected_unattempted kept.
    by_date = {r.date: r for r in rows_new}
    assert "2026-06-06" not in by_date
    assert by_date["2026-06-07"].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert by_date["2026-06-08"].capture_status == "expected_unattempted"


# ---------------------------------------------------------------------------
# 3. Column projection — memory-shape guard
# ---------------------------------------------------------------------------


def test_needed_manifest_columns_sports_is_key_grain_plus_capture_status() -> None:
    """The sports load path reads EXACTLY 4 of the 42 index columns — the
    full-width read is the 8Gi OOM; this pin is the regression guard."""
    got = enumerator_module._needed_manifest_columns("sports", _SPORTS_INDEX_SCHEMA)
    assert got == ["data_type", "league_id", "date", "capture_status"]


def test_needed_manifest_columns_intersects_schema() -> None:
    assert enumerator_module._needed_manifest_columns("sports", ["date", "league_id"]) == ["league_id", "date"]
    assert enumerator_module._needed_manifest_columns("sports", ["foo", "bar"]) == []


def test_projected_frame_yields_identical_sets_as_full_width() -> None:
    """Coarse guard: everything the set builders consume survives the
    projection — sets from df[needed] == sets from the full-width df."""
    df = _rich_manifest_frame()
    for asset_group in ("sports", "cefi", "tradfi", "prediction"):
        needed = enumerator_module._needed_manifest_columns(asset_group, list(df.columns))
        projected = df[needed]
        assert enumerator_module._build_captured_set(projected, asset_group) == (
            enumerator_module._build_captured_set(df, asset_group)
        )
        assert enumerator_module._build_present_set(projected, asset_group) == (
            enumerator_module._build_present_set(df, asset_group)
        )


def test_read_manifest_parquet_projected_reads_key_columns_only(tmp_path: Path) -> None:
    df = _rich_manifest_frame()
    # Parquet cannot hold a mixed int/str object column — stringify the numeric
    # league_id (the in-memory mixed-dtype coercion path is covered above; this
    # test covers the parquet round-trip + projection).
    df["league_id"] = df["league_id"].map(lambda v: v if v is None else str(v))
    path = tmp_path / "index.parquet"
    df.to_parquet(path, index=False)
    got = enumerator_module._read_manifest_parquet_projected(str(path), "sports")
    assert list(got.columns) == ["data_type", "league_id", "date", "capture_status"]
    assert len(got) == len(df)
    # Round-trip through parquet + projection still yields identical sets.
    assert enumerator_module._build_captured_set(got, "sports") == _legacy_build_captured_set(df, "sports")
    assert enumerator_module._build_present_set(got, "sports") == _legacy_build_present_set(df, "sports")


def test_read_manifest_parquet_projected_falls_back_full_width(tmp_path: Path) -> None:
    """A degenerate parquet with NO needed columns keeps legacy full-width behaviour."""
    df = pd.DataFrame({"foo": ["a"], "bar": ["b"]})
    path = tmp_path / "weird.parquet"
    df.to_parquet(path, index=False)
    got = enumerator_module._read_manifest_parquet_projected(str(path), "sports")
    assert list(got.columns) == ["foo", "bar"]
