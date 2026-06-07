"""Unit tests — migrate_instruments_store_v9.py (AG-parametric instruments-store v9 single-walk migrator).

Credential-free: the v9 ``_index`` transform + the object-path transform are PURE (no GCS / no network) and
tested directly on synthetic DataFrames / rel-paths.

Coverage:
  1. CF-1…CF-TRANSPORT v9 column stamping (every canonical column present + correct, non-sports reference).
  2. source/pipeline_mode/transport round-trip (instruments_service ↔ batch_instruments_service ↔ rest).
  3. Honest capture_status relabel (CF-10/CF-11): null+count>0 → captured; count==0 → empty_confirmed;
     captured-but-empty → empty_confirmed.
  4. Typed empty reason (CF-5): blank reason on an empty cell → SOURCE_RETURNED_ZERO; typed reason preserved.
  5. Blank data_type (CF-7) → 'instruments'; an already-typed data_type (pred) is preserved.
  6. Phantom-safe (CF-10): every captured row carries instrument_count > 0.
  7. Single-walk / idempotency: a second pass is a no-op.
  8. Lean pred schema: missing v9 columns are added.
  9. CF-2: a legacy ``category`` column is dropped; asset_group column set.
 10. Object-path rewrite: pipeline_mode=/asset_group= inserted (non-sports); pipeline_mode= inserted preserving
     entity=/league= (sports); idempotent; non-parquet / no-day / wrong-tree → skipped.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_instruments_store_v9.py"
    module_name = "_migrate_instruments_store_v9_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = _load_script()


def _cefi_like_frame() -> pd.DataFrame:
    """A cefi-like v8 _index: blank data_type, blank pipeline_mode, no asset_group/source/transport cols,
    null capture_status on count>0 rows (the 40%-null class), a captured-but-empty dishonest row, a real empty,
    and a legacy ``category`` column to be dropped."""
    return pd.DataFrame(
        [
            # null capture_status + count>0 → must become captured
            {
                "date": "2020-01-01",
                "venue": "DERIBIT",
                "instrument_count": 6,
                "capture_status": None,
                "data_type": "",
                "pipeline_mode": "",
                "written_at": "2026-05-04T13:10:10+00:00",
                "error_reason": None,
                "schema_version": 8,
                "category": "cefi",
            },
            # null capture_status + count==0 → must become empty_confirmed(SOURCE_RETURNED_ZERO)
            {
                "date": "2020-01-02",
                "venue": "OKX-SPOT",
                "instrument_count": 0,
                "capture_status": None,
                "data_type": "",
                "pipeline_mode": "",
                "written_at": "2026-05-04T13:11:10+00:00",
                "error_reason": None,
                "schema_version": 8,
                "category": "cefi",
            },
            # captured but count==0 (dishonest) → must become empty_confirmed
            {
                "date": "2020-01-03",
                "venue": "BINANCE-FUTURES",
                "instrument_count": 0,
                "capture_status": "captured",
                "data_type": "",
                "pipeline_mode": "",
                "written_at": "2026-05-04T13:12:10+00:00",
                "error_reason": "",
                "schema_version": 8,
                "category": "cefi",
            },
            # genuine captured
            {
                "date": "2020-01-04",
                "venue": "DERIBIT",
                "instrument_count": 12,
                "capture_status": "captured",
                "data_type": "",
                "pipeline_mode": "",
                "written_at": "2026-05-04T13:13:10+00:00",
                "error_reason": "",
                "schema_version": 8,
                "category": "cefi",
            },
        ]
    )


def test_v9_column_stamping_and_provenance() -> None:
    out, stats = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    # CF-1
    assert (out["schema_version"] == 9).all()
    assert stats["v8_before"] == 4 and stats["v9_before"] == 0
    # CF-2 asset_group set + category dropped
    assert "category" not in out.columns
    assert (out["asset_group"] == "cefi").all()
    # CF-3 / CF-4 / CF-TRANSPORT — reference provenance round-trip
    assert (out["pipeline_mode"] == "batch_instruments_service").all()
    assert (out["source"] == "instruments_service").all()
    assert (out["transport"] == "rest").all()
    # round-trip: source string maps back from the pipeline_mode
    assert MOD.REFERENCE_SOURCE == "instruments_service"
    assert MOD.REFERENCE_PIPELINE_MODE == "batch_instruments_service"


def test_honest_capture_status_relabel() -> None:
    out, stats = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    by_date = {r["date"]: r for r in out.to_dict(orient="records")}
    assert by_date["2020-01-01"]["capture_status"] == "captured"  # null + count>0
    assert by_date["2020-01-02"]["capture_status"] == "empty_confirmed"  # null + count==0
    assert by_date["2020-01-03"]["capture_status"] == "empty_confirmed"  # captured-but-empty
    assert by_date["2020-01-04"]["capture_status"] == "captured"
    assert stats["null_capture_to_captured"] == 1
    assert stats["null_capture_to_empty"] == 1
    assert stats["captured_but_empty_to_empty"] == 1


def test_typed_empty_reason_cf5() -> None:
    out, _ = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    empties = out[out["capture_status"] == "empty_confirmed"]
    # every empty cell carries a typed reason; none blank
    assert (empties["error_reason"].astype(str).str.len() > 0).all()
    assert set(empties["error_reason"]) == {"SOURCE_RETURNED_ZERO"}
    # captured rows carry no reason
    captured = out[out["capture_status"] == "captured"]
    assert (captured["error_reason"].astype(str).str.len() == 0).all()


def test_existing_typed_reason_preserved() -> None:
    df = pd.DataFrame(
        [
            {
                "date": "2020-01-01",
                "venue": "CME",
                "instrument_count": 0,
                "capture_status": "empty_confirmed",
                "data_type": "",
                "pipeline_mode": "",
                "written_at": "2026-04-10T00:00:00+00:00",
                "error_reason": "EXPECTED_WEEKEND",
                "schema_version": 8,
            },
        ]
    )
    out, _ = MOD.transform_index_v9(df, "tradfi")
    assert out.iloc[0]["error_reason"] == "EXPECTED_WEEKEND"  # canonical reason untouched


def test_blank_data_type_to_instruments_typed_preserved() -> None:
    out, stats = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    assert (out["data_type"] == "instruments").all()
    assert stats["data_type_set"] == 4
    # an already-typed data_type (prediction) is preserved
    pred = pd.DataFrame(
        [
            {
                "date": "2025-03-14",
                "venue": "POLYMARKET",
                "instrument_count": 157,
                "capture_status": "captured",
                "data_type": "prediction_canonical_question_group",
                "written_at": "2026-05-22T13:08:04+00:00",
                "error_reason": "",
                "schema_version": 8,
            }
        ]
    )
    out2, _ = MOD.transform_index_v9(pred, "prediction")
    assert out2.iloc[0]["data_type"] == "prediction_canonical_question_group"


def test_phantom_safe_captured_implies_count_positive() -> None:
    out, _ = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    captured = out[out["capture_status"] == "captured"]
    counts = pd.to_numeric(captured["instrument_count"], errors="coerce").fillna(0)
    assert (counts > 0).all()  # CF-10: no captured cell with zero instruments


def test_single_walk_idempotency() -> None:
    out1, _ = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    out2, _ = MOD.transform_index_v9(out1, "cefi")
    assert out1.reset_index(drop=True).equals(out2.reset_index(drop=True))


def test_lean_pred_schema_columns_added() -> None:
    # pred-like lean v8 frame: no pipeline_mode/source/transport/asset_group columns
    df = pd.DataFrame(
        [
            {
                "date": "2025-03-14",
                "venue": "POLYMARKET",
                "instrument_count": 157,
                "capture_status": "captured",
                "data_type": "prediction_canonical_question_group",
                "written_at": "2026-05-22T13:08:04+00:00",
                "error_reason": "",
                "schema_version": 8,
            }
        ]
    )
    out, _ = MOD.transform_index_v9(df, "prediction")
    for col in ("asset_group", "pipeline_mode", "source", "transport", "available_at"):
        assert col in out.columns
    assert out.iloc[0]["asset_group"] == "prediction"
    assert out.iloc[0]["transport"] == "rest"


def test_available_at_from_written_at_no_lookahead() -> None:
    out, _ = MOD.transform_index_v9(_cefi_like_frame(), "cefi")
    # CF-8: available_at populated from written_at (honest write-time proxy)
    assert (out["available_at"].astype(str).str.len() > 0).all()
    assert out.iloc[0]["available_at"] == "2026-05-04T13:10:10+00:00"


def test_sports_structural_only_preserves_semantics() -> None:
    """sports is cross-owned (sports_manifest_canonicalisation): the tool stamps the STRUCTURAL v9 columns but
    preserves capture_status / error_reason / data_type (instrument_count is NOT the sports captured-signal)."""
    df = pd.DataFrame(
        [
            # a sports 'captured' cell with instrument_count==0 (legit for sports) must STAY captured
            {
                "date": "2024-09-11",
                "venue": "API_FOOTBALL",
                "instrument_count": 0,
                "capture_status": "captured",
                "data_type": "STANDINGS",
                "pipeline_mode": "",
                "written_at": "2026-05-01T00:00:00+00:00",
                "error_reason": "",
                "schema_version": 8,
                "row_count": 0,
            },
            # a sports empty with a typed reason must keep it (not re-typed to SOURCE_RETURNED_ZERO)
            {
                "date": "2024-09-11",
                "venue": "",
                "instrument_count": 0,
                "capture_status": "empty_confirmed",
                "data_type": "FIXTURE_LINEUPS",
                "pipeline_mode": "",
                "written_at": "2026-04-28T17:44:26+00:00",
                "error_reason": "EXPECTED_PRE_SEASON",
                "schema_version": 8,
                "row_count": 0,
            },
        ]
    )
    out, _ = MOD.transform_index_v9(df, "sports")
    # structural columns stamped
    assert (out["schema_version"] == 9).all()
    assert (out["asset_group"] == "sports").all()
    assert (out["pipeline_mode"].astype(str).str.len() > 0).all()
    assert (out["source"].astype(str).str.len() > 0).all()
    assert (out["transport"] == "rest").all()
    # semantic columns preserved (NOT overridden by the instrument_count heuristic)
    assert out.iloc[0]["capture_status"] == "captured"  # instrument_count==0 captured cell preserved
    assert out.iloc[0]["data_type"] == "STANDINGS"
    assert out.iloc[1]["error_reason"] == "EXPECTED_PRE_SEASON"


# ── object-path transform ─────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "rel,ag,expected",
    [
        (
            "instrument_availability/by_date/day=2019-03-30/venue=DERIBIT/instruments.parquet",
            "cefi",
            "instrument_availability/by_date/day=2019-03-30/pipeline_mode=batch_instruments_service/asset_group=cefi/venue=DERIBIT/instruments.parquet",
        ),
        (
            "instrument_availability/by_date/day=2020-01-20/venue=CURVE-ETHEREUM/instruments.parquet",
            "defi",
            "instrument_availability/by_date/day=2020-01-20/pipeline_mode=batch_instruments_service/asset_group=defi/venue=CURVE-ETHEREUM/instruments.parquet",
        ),
    ],
)
def test_object_rel_nonsports_insert(rel: str, ag: str, expected: str) -> None:
    assert MOD.canonical_object_rel(rel, ag) == expected
    # idempotent
    assert MOD.canonical_object_rel(expected, ag) == expected


def test_object_rel_sports_preserves_entity_league() -> None:
    rel = "sports_reference/by_date/day=2024-09-11/entity=fixtures/league=EPL/fixtures.parquet"
    out = MOD.canonical_object_rel(rel, "sports")
    assert out is not None
    assert "/pipeline_mode=" in out
    assert "/entity=fixtures/" in out and "/league=EPL/" in out
    assert out.index("pipeline_mode=") < out.index("entity=")  # pm inserted left of entity
    assert MOD.canonical_object_rel(out, "sports") == out  # idempotent


def test_object_rel_skips() -> None:
    # non-parquet
    assert MOD.canonical_object_rel("instrument_availability/by_date/day=2020-01-01/venue=X/_SUCCESS", "cefi") is None
    # no day= segment
    assert MOD.canonical_object_rel("instrument_availability/catalog.parquet", "cefi") is None
    # non-sports object on the wrong tree
    assert MOD.canonical_object_rel("sports_reference/by_date/day=2024-01-01/entity=x/x.parquet", "cefi") is None
    # missing venue on non-sports
    assert (
        MOD.canonical_object_rel("instrument_availability/by_date/day=2020-01-01/instruments.parquet", "cefi") is None
    )
