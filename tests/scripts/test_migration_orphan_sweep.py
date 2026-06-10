"""Unit tests for migration_orphan_sweep.py (CF-17 / V2 scaffold).

Credential-free + GCS-free: every test exercises the PURE classification surface
(``classify_object`` / ``parse_hive_segments`` / ``build_covered_index`` /
``SizingRollup``) — the single GCS walk is thin orchestration over these. Covers the
forced 6-way taxonomy (A canonical+manifested, B legacy-duplicate, C manifest-infra,
C2 non-data, D junk, E orphan-real) + the (E)/(D) zero-row split + sizing.

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V2/CF-17.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "migration_orphan_sweep.py"
    module_name = "_migration_orphan_sweep_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()
OC = _mod.ObjectClass


def _cells():
    rows = [
        {
            "asset_group": "cefi",
            "venue": "BINANCE-FUTURES",
            "chain": "",
            "instrument_type": "perpetual",
            "data_type": "trades",
            "date": "2024-06-01",
            "capture_status": "captured",
        },
        # a non-captured row must NOT seed the manifested-cell set
        {
            "asset_group": "cefi",
            "venue": "BINANCE-FUTURES",
            "chain": "",
            "instrument_type": "perpetual",
            "data_type": "book_snapshot_5",
            "date": "2024-06-01",
            "capture_status": "attempted_failed",
        },
    ]
    return _mod.build_covered_index(rows)


_CANON = (
    "raw_tick_data/by_date/day=2024-06-01/pipeline_mode=batch_tardis/asset_group=cefi/"
    "venue=BINANCE-FUTURES/instrument_type=perpetual/data_type=trades/x.parquet"
)
_LEGACY = (
    "raw_tick_data/by_date/day=2024-06-01/asset_group=cefi/"
    "venue=BINANCE-FUTURES/instrument_type=perpetual/data_type=trades/x.parquet"
)
_ORPHAN = (
    "raw_tick_data/by_date/day=2024-07-15/pipeline_mode=batch_tardis/asset_group=cefi/"
    "venue=BINANCE-FUTURES/instrument_type=perpetual/data_type=trades/x.parquet"
)
_INVALID = (
    "raw_tick_data/by_date/day=2024-06-01/pipeline_mode=batch_tardis/asset_group=cefi/"
    "venue=BINANCE-SPOT/instrument_type=spot_pair/data_type=derivative_ticker/x.parquet"
)


def _classify(path: str, *, is_parquet: bool = True, row_count: int | None = None) -> OC:
    cls, _key, _reason = _mod.classify_object(path, "cefi", _cells(), is_parquet=is_parquet, row_count=row_count)
    return cls


class TestClassification:
    def test_a_canonical_manifested(self) -> None:
        assert _classify(_CANON) == OC.CANONICAL_MANIFESTED

    def test_b_legacy_duplicate_of_manifested_cell(self) -> None:
        assert _classify(_LEGACY) == OC.LEGACY_DUPLICATE

    def test_c_manifest_infra(self) -> None:
        assert _classify("_index/availability_index.parquet") == OC.MANIFEST_INFRA
        assert _classify("raw_tick_data/by_date/day=2024-06-01/x.parquet.tmp") == OC.MANIFEST_INFRA

    def test_c2_non_data(self) -> None:
        assert _classify("vm_logs/run-1/out.log", is_parquet=False) == OC.NON_DATA
        assert _classify("terraform/state.tfstate", is_parquet=False) == OC.NON_DATA

    def test_d_junk_invalid_shard_shape(self) -> None:
        # spot_pair cannot carry derivative_ticker → outside the valid could-exist space
        assert _classify(_INVALID) == OC.JUNK

    def test_d_junk_missing_hive_segments(self) -> None:
        assert _classify("raw_tick_data/by_date/garbage/x.parquet") == OC.JUNK

    def test_e_orphan_real_rows_present(self) -> None:
        assert _classify(_ORPHAN, row_count=10) == OC.ORPHAN_REAL

    def test_e_orphan_unknown_rowcount_treated_as_real(self) -> None:
        # row_count=None (unknown) → safe side: surface as orphan, not silently dropped
        assert _classify(_ORPHAN, row_count=None) == OC.ORPHAN_REAL

    def test_zero_row_orphan_demoted_to_junk(self) -> None:
        assert _classify(_ORPHAN, row_count=0) == OC.JUNK

    def test_non_captured_manifest_row_is_not_a_cell(self) -> None:
        # the attempted_failed book_snapshot_5 row must NOT make its object class A/B
        path = _CANON.replace("data_type=trades", "data_type=book_snapshot_5")
        assert _classify(path, row_count=5) == OC.ORPHAN_REAL

    def test_wildcard_blank_manifest_fields_cover_finer_object(self) -> None:
        """Grain-aware covering: a manifest row with BLANK chain/instrument_type covers a
        finer-grained object that carries chain=POLYGON etc. (the prediction A=0 fix)."""
        # manifest captured at the coarse grain (venue+data_type+date; blank chain/it)
        rows = [
            {
                "asset_group": "prediction",
                "venue": "POLYMARKET",
                "chain": "",
                "instrument_type": "",
                "data_type": "trades",
                "date": "2025-03-14",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        # a per-instrument object carrying chain=POLYGON + a deep hive layout
        obj = (
            "raw_tick_data/by_date/day=2025-03-14/pipeline_mode=batch_polymarket_clob/"
            "asset_group=prediction/venue=POLYMARKET/chain=POLYGON/data_type=trades/0xabc.parquet"
        )
        cls, _key, _r = _mod.classify_object(obj, "prediction", index, is_parquet=True, row_count=5)
        assert cls == OC.CANONICAL_MANIFESTED  # covered (NOT a false orphan)

    def test_fully_blank_manifest_row_wildcards_venue_too(self) -> None:
        rows = [
            {
                "asset_group": "prediction",
                "venue": "",
                "chain": "",
                "instrument_type": "",
                "data_type": "trades",
                "date": "2025-03-14",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        obj = "raw_tick_data/by_date/day=2025-03-14/asset_group=prediction/venue=POLYMARKET/chain=POLYGON/data_type=trades/x.parquet"
        cls, _key, _r = _mod.classify_object(obj, "prediction", index, is_parquet=True, row_count=5)
        assert cls == OC.LEGACY_DUPLICATE  # covered (legacy shape, no pipeline_mode)


class TestParsing:
    def test_parse_hive_segments_canonical(self) -> None:
        segs = _mod.parse_hive_segments(_CANON)
        assert segs["pipeline_mode"] == "batch_tardis"
        assert segs["asset_group"] == "cefi"
        assert segs["venue"] == "BINANCE-FUTURES"
        assert segs["data_type"] == "trades"

    def test_category_normalised_to_asset_group(self) -> None:
        segs = _mod.parse_hive_segments("raw_tick_data/by_date/day=2024-06-01/category=cefi/venue=X/data_type=trades/")
        assert segs["asset_group"] == "cefi"

    def test_defi_combined_venue_chain_split(self) -> None:
        segs = {"venue": "EIGENLAYER-ETHEREUM", "data_type": "rewards", "instrument_type": "staking"}
        key = _mod.shard_key_from_segments("defi", segs)
        assert key.venue == "EIGENLAYER"
        assert key.chain == "ETHEREUM"

    def test_source_from_pipeline_mode(self) -> None:
        assert _mod._source_from_pipeline_mode("batch_polymarket_clob") == "polymarket_clob"
        assert _mod._source_from_pipeline_mode("") == ""

    def test_taxonomy_zero_unknown_for_known_prefixes(self) -> None:
        assert _mod._taxonomy_label(_CANON) == "service-data"
        assert _mod._taxonomy_label("_index/x.parquet") == "manifest-infra"
        assert _mod._taxonomy_label("vm_logs/a.log") == "logs"
        # an unrecognised top-level prefix surfaces as unknown (a finding)
        assert _mod._taxonomy_label("mystery_prefix/x").startswith("unknown:")


class TestSizingRollup:
    def test_rollup_aggregates_bytes_and_counts(self) -> None:
        sizing = _mod.SizingRollup.empty()
        for path, size in ((_CANON, 100), (_CANON, 50)):
            cls, key, _r = _mod.classify_object(path, "cefi", _cells(), is_parquet=True)
            obj = _mod.SweptObject(
                uri=path,
                asset_group="cefi",
                obj_class=cls,
                venue=key.venue,
                chain=key.chain,
                instrument_type=key.instrument_type,
                data_type=key.data_type,
                day="2024-06-01",
                pipeline_mode="batch_tardis",
                source="tardis",
                size_bytes=size,
                crc32c="abc",
                reason="",
            )
            sizing.add(obj)
        assert sizing.total_bytes() == 150
        biggest = sizing.biggest(5)
        assert biggest[0][1] == 150 and biggest[0][2] == 2
