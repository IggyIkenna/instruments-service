"""Unit tests for backfill_prediction_question_2026_07_17.

The migration adds a ``question`` column to ``prod/catalog.parquet`` by re-reading the human
question text already at rest in the legacy Shape-B (raw-Gamma, ~47-col) prediction objects —
NO re-capture, NO adapter call. The correctness contract these tests pin:

1. **Join key = bare ``condition_id``** — matches a BARE catalogue ``0x…`` instrument_id and the
   ``0x…`` suffix of a WRAPPED ``POLYMARKET:PREDICTION_MARKET:0x…`` one; never a Kalshi row.
2. **Additive + lossless** — row count unchanged, every pre-existing column byte-identical, no
   date column touched, unmatched rows honest-NULL (never fabricated).
3. **most-recent-wins** — latest ``day=`` wins, blank never overwrites a non-blank.
4. **Idempotent** — a re-run recomputes an identical ``question`` column.
5. **Gate** — refuses a row-count change / pre-existing-column mutation / zero-fill / over-fill.
"""

from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pandas as pd
import pytest


def _load_module():
    here = Path(__file__).resolve()
    script = here.parent.parent.parent.parent / "scripts" / "backfill_prediction_question_2026_07_17.py"
    spec = importlib.util.spec_from_file_location("backfill_prediction_question_2026_07_17", script)
    if spec is None or spec.loader is None:
        msg = f"Failed to load script spec at {script}"
        raise RuntimeError(msg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


_WRAP = "POLYMARKET:PREDICTION_MARKET:"


class _FakeStorage:
    """Serves parquet bytes for a {path: DataFrame} map; lists blobs by prefix; 404s the rest."""

    def __init__(self, objects: dict[str, pd.DataFrame]) -> None:
        self._objects = objects

    class _Blob:
        def __init__(self, name: str) -> None:
            self.name = name

    def list_blobs(self, _bucket: str, prefix: str = ""):
        return [self._Blob(n) for n in self._objects if n.startswith(prefix)]

    def download_bytes(self, _bucket: str, path: str) -> bytes:
        if path not in self._objects:
            msg = f"404 no such object: {path}"
            raise FileNotFoundError(msg)
        buf = io.BytesIO()
        self._objects[path].to_parquet(buf, index=False)
        return buf.getvalue()


def _raw_gamma(cids_questions: list[tuple[str, str]]) -> pd.DataFrame:
    """A legacy 47-col-family raw-Gamma object slice (condition_id + question columns)."""
    return pd.DataFrame(
        {
            "id": [f"id{i}" for i in range(len(cids_questions))],
            "condition_id": [c for c, _ in cids_questions],
            "question": [q for _, q in cids_questions],
            "market_slug": ["slug"] * len(cids_questions),
        }
    )


def _is_normalised(cids: list[str]) -> pd.DataFrame:
    """A legacy 30-col-family IS-normalised object slice (instrument_key, NO question)."""
    return pd.DataFrame(
        {
            "instrument_key": cids,
            "venue": ["POLYMARKET"] * len(cids),
            "instrument_type": ["prediction_market"] * len(cids),
            "raw_symbol": ["s"] * len(cids),
        }
    )


# --------------------------------------------------------------------------- match_key


def test_match_key_bare_polymarket(mod):
    assert mod.match_key("0xabc123", "POLYMARKET") == "0xabc123"


def test_match_key_wrapped_polymarket_strips_prefix(mod):
    assert mod.match_key(f"{_WRAP}0xabc123", "POLYMARKET") == "0xabc123"


def test_match_key_kalshi_is_none(mod):
    assert mod.match_key("FEDHIKE-26DEC31", "KALSHI") is None
    assert mod.match_key("KALSHI:PREDICTION_MARKET:FEDHIKE-26DEC31", "KALSHI") is None


def test_match_key_non_poly_venue_none_even_if_0x(mod):
    # Defensive: a 0x id under a non-POLYMARKET venue is not a Polymarket condition_id.
    assert mod.match_key("0xabc123", "KALSHI") is None


def test_match_key_other_form_none(mod):
    assert mod.match_key("garbage", "POLYMARKET") is None


# --------------------------------------------------------------- _read_object_questions


def test_read_object_questions_extracts_pairs(mod):
    storage = _FakeStorage({"p.parquet": _raw_gamma([("0xaaa", "Q1"), ("0xbbb", "Q2")])})
    out = mod._read_object_questions(storage, "b", "p.parquet")
    assert sorted(out) == [("0xaaa", "Q1"), ("0xbbb", "Q2")]


def test_read_object_questions_drops_blanks_and_nan(mod):
    df = _raw_gamma([("0xaaa", "Q1"), ("0xbbb", "  "), ("", "Q3")])
    storage = _FakeStorage({"p.parquet": df})
    out = mod._read_object_questions(storage, "b", "p.parquet")
    assert out == [("0xaaa", "Q1")]


def test_read_object_questions_is_normalised_object_yields_nothing(mod):
    # The 30/41-col IS-normalised family has no condition_id/question columns → not a carrier.
    storage = _FakeStorage({"p.parquet": _is_normalised(["0xaaa", "0xbbb"])})
    assert mod._read_object_questions(storage, "b", "p.parquet") == []


def test_read_object_questions_missing_object_isolated(mod):
    storage = _FakeStorage({})
    assert mod._read_object_questions(storage, "b", "nope.parquet") == []


# --------------------------------------------------------------- build_question_map


def _p(day: str, extra: str = "") -> str:
    return f"instrument_availability/by_date/day={day}/{extra}venue=POLYMARKET/instruments.parquet"


def test_build_question_map_most_recent_wins(mod):
    objects = {
        _p("2025-03-13"): _raw_gamma([("0xaaa", "OLD question")]),
        _p("2025-06-01"): _raw_gamma([("0xaaa", "NEW question")]),
    }
    storage = _FakeStorage(objects)
    qmap, stats = mod.build_question_map(storage, "b", list(objects), workers=2)
    assert qmap["0xaaa"] == "NEW question"
    assert stats["distinct_condition_ids"] == 1


def test_build_question_map_tie_prefers_longest(mod):
    objects = {
        _p("2025-03-13", "market=A/"): _raw_gamma([("0xaaa", "short")]),
        _p("2025-03-13", "market=B/"): _raw_gamma([("0xaaa", "a much longer question")]),
    }
    storage = _FakeStorage(objects)
    qmap, _ = mod.build_question_map(storage, "b", list(objects), workers=2)
    assert qmap["0xaaa"] == "a much longer question"


def test_build_question_map_blank_never_overwrites(mod):
    # The newer object has only a blank for 0xaaa → the older non-blank survives.
    objects = {
        _p("2025-03-13"): _raw_gamma([("0xaaa", "real question")]),
        _p("2025-06-01"): _raw_gamma([("0xaaa", "   ")]),
    }
    storage = _FakeStorage(objects)
    qmap, _ = mod.build_question_map(storage, "b", list(objects), workers=2)
    assert qmap["0xaaa"] == "real question"


# --------------------------------------------------------------- list_legacy_objects


def test_list_legacy_objects_excludes_cqg_and_metadata(mod):
    objects = {
        _p("2025-03-13"): _raw_gamma([("0xaaa", "Q")]),
        "instrument_availability/by_date/canonical_question_group=BTC_UP_DOWN_DAILY/day=2026-07-13/venue=POLYMARKET/instruments.parquet": _raw_gamma(
            [("0xbbb", "Q")]
        ),
        "instrument_availability/by_date/day=2025-03-13/venue=POLYMARKET/prediction_market_metadata.parquet": _raw_gamma(
            [("0xccc", "Q")]
        ),
    }
    storage = _FakeStorage(objects)
    names = mod.list_legacy_objects(storage, "b")
    assert names == [_p("2025-03-13")]


# --------------------------------------------------------------- patch_catalog


def _catalog() -> pd.DataFrame:
    """A minimal catalogue: 2 markets x 2 data_types + 1 Kalshi row + 1 unmatched poly row."""
    return pd.DataFrame(
        {
            "instrument_id": [
                "0xaaa",
                "0xaaa",  # matched, bare, both data_types
                f"{_WRAP}0xbbb",
                f"{_WRAP}0xbbb",  # matched, wrapped, both data_types
                "0xzzz",  # unmatched poly (no legacy question)
                "KALSHI:PREDICTION_MARKET:FEDHIKE-26DEC31",  # kalshi — never filled
            ],
            "venue": ["POLYMARKET"] * 5 + ["KALSHI"],
            "data_type": ["trades", "market_lifecycle", "trades", "market_lifecycle", "trades", "trades"],
            "base_asset": ["BTC", "BTC", "ETH", "ETH", "OTHER", "FED"],
            "available_from": ["2025-01-01"] * 6,
            "available_to": [None, None, None, None, None, None],
        }
    )


def test_patch_catalog_additive_fills_matched_rows(mod):
    cat = _catalog()
    qmap = {"0xaaa": "Question A", "0xbbb": "Question B"}
    out, stats = mod.patch_catalog(cat, qmap)
    assert stats["rows_before"] == stats["rows_after"] == 6
    assert stats["question_nonnull"] == 4  # 2 markets x 2 data_types
    assert stats["distinct_markets_gained"] == 2
    # both data_type rows of a matched market gained the question
    aaa = out[out["instrument_id"] == "0xaaa"]
    assert set(aaa["question"]) == {"Question A"}
    bbb = out[out["instrument_id"] == f"{_WRAP}0xbbb"]
    assert set(bbb["question"]) == {"Question B"}


def test_patch_catalog_leaves_unmatched_and_kalshi_null(mod):
    cat = _catalog()
    out, _ = mod.patch_catalog(cat, {"0xaaa": "Question A", "0xbbb": "Question B"})
    assert out.loc[out["instrument_id"] == "0xzzz", "question"].isna().all()
    assert out.loc[out["venue"] == "KALSHI", "question"].isna().all()


def test_patch_catalog_preserves_columns_and_order(mod):
    cat = _catalog()
    out, _ = mod.patch_catalog(cat, {"0xaaa": "Q"})
    # every pre-existing column preserved, byte-identical, and question slotted after base_asset
    for col in cat.columns:
        assert out[col].tolist() == cat[col].tolist()
    assert list(out.columns) == [
        "instrument_id",
        "venue",
        "data_type",
        "base_asset",
        "question",
        "available_from",
        "available_to",
    ]


def test_patch_catalog_never_touches_date_columns(mod):
    cat = _catalog()
    out, _ = mod.patch_catalog(cat, {"0xaaa": "Q"})
    assert out["available_from"].tolist() == cat["available_from"].tolist()
    assert out["available_to"].tolist() == cat["available_to"].tolist()


def test_patch_catalog_idempotent(mod):
    cat = _catalog()
    qmap = {"0xaaa": "Question A", "0xbbb": "Question B"}
    once, _ = mod.patch_catalog(cat, qmap)
    twice, _ = mod.patch_catalog(once, qmap)
    pd.testing.assert_frame_equal(once, twice)


# --------------------------------------------------------------- gate


def test_gate_passes_on_clean_additive_patch(mod):
    cat = _catalog()
    pre = [c for c in cat.columns if c != "question"]
    before_ck = mod._column_checksums(cat, pre)
    out, stats = mod.patch_catalog(cat, {"0xaaa": "Q"})
    after_ck = mod._column_checksums(out, pre)
    ok, failures = mod.gate(before_ck, after_ck, stats)
    assert ok, failures


def test_gate_fails_on_zero_fill(mod):
    cat = _catalog()
    pre = [c for c in cat.columns if c != "question"]
    before_ck = mod._column_checksums(cat, pre)
    out, stats = mod.patch_catalog(cat, {})  # empty map → 0 fill
    after_ck = mod._column_checksums(out, pre)
    ok, failures = mod.gate(before_ck, after_ck, stats)
    assert not ok
    assert any("no-op" in f for f in failures)


def test_gate_fails_on_row_count_change(mod):
    stats = {"rows_before": 6, "rows_after": 5, "question_nonnull": 1, "rows_with_legacy_match": 1}
    ok, failures = mod.gate({}, {}, stats)
    assert not ok
    assert any("row count changed" in f for f in failures)


def test_gate_fails_on_column_mutation(mod):
    stats = {"rows_before": 6, "rows_after": 6, "question_nonnull": 1, "rows_with_legacy_match": 1}
    ok, failures = mod.gate({"base_asset": "aaa"}, {"base_asset": "bbb"}, stats)
    assert not ok
    assert any("mutated" in f for f in failures)


def test_gate_fails_on_fabrication_overfill(mod):
    stats = {"rows_before": 6, "rows_after": 6, "question_nonnull": 5, "rows_with_legacy_match": 2}
    ok, failures = mod.gate({}, {}, stats)
    assert not ok
    assert any("fabrication" in f for f in failures)
