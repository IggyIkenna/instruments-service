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


class TestR1MatcherRefinements:
    """R1 2026-06-11: venue-token spelling normalisation + object-blank-field wildcard
    + top-level non-data labels (the refinements that collapsed defi 254,984 →
    172 / prediction 61,014 → 17 false class-E)."""

    def test_venue_separator_spelling_normalised(self) -> None:
        # manifest rows (2026-04 migration) spell UNISWAPV3; object paths spell UNISWAP_V3
        rows = [
            {
                "asset_group": "defi",
                "venue": "UNISWAPV3",
                "chain": "ETHEREUM",
                "instrument_type": "pool",
                "data_type": "dex_pool_state",
                "date": "2024-05-03",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        obj = (
            "raw_tick_data/by_date/day=2024-05-03/category=defi/venue=UNISWAP_V3/chain=ETHEREUM/"
            "instrument_type=pool/data_type=dex_pool_state/0xabc.parquet"
        )
        cls, _key, _r = _mod.classify_object(obj, "defi", index, is_parquet=True, row_count=5)
        assert cls == OC.LEGACY_DUPLICATE

    def test_object_blank_instrument_type_matches_finer_manifest(self) -> None:
        # legacy paths predate the instrument_type axis; the manifest row is FINER
        rows = [
            {
                "asset_group": "tradfi",
                "venue": "NYSE",
                "chain": "",
                "instrument_type": "equity",
                "data_type": "ohlcv_1m",
                "date": "2023-05-02",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        obj = "raw_tick_data/by_date/day=2023-05-02/category=tradfi/venue=NYSE/data_type=ohlcv_1m/ABBV.parquet"
        cls, _key, _r = _mod.classify_object(obj, "tradfi", index, is_parquet=True, row_count=5)
        assert cls == OC.LEGACY_DUPLICATE

    def test_blank_object_venue_is_never_wildcarded(self) -> None:
        # venue is IDENTITY: a blank-venue object must stay an orphan (E), never
        # auto-covered by some other venue's manifested pattern.
        rows = [
            {
                "asset_group": "tradfi",
                "venue": "NASDAQ",
                "chain": "",
                "instrument_type": "equity",
                "data_type": "ohlcv_15m",
                "date": "2025-01-02",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        key = _mod.shard_key_from_segments("tradfi", {"data_type": "ohlcv_15m"})
        assert not _mod.is_covered(index, key, "2025-01-02")

    def test_object_blank_wildcard_requires_venue_agreement(self) -> None:
        rows = [
            {
                "asset_group": "tradfi",
                "venue": "NASDAQ",
                "chain": "",
                "instrument_type": "equity",
                "data_type": "tbbo",
                "date": "2023-05-02",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        # NYSE object with blank instrument_type — NASDAQ's pattern must NOT cover it
        key = _mod.shard_key_from_segments("tradfi", {"venue": "NYSE", "data_type": "tbbo"})
        assert not _mod.is_covered(index, key, "2023-05-02")

    def test_top_level_legacy_trees_labelled_not_unknown(self) -> None:
        # top-level legacy corpora are labelled (never unknown / never raw-tick classes)
        assert _mod._taxonomy_label("dex_pools/solana/x.parquet") == "legacy-data"
        assert _mod._taxonomy_label("lending_indices/solana/x.parquet") == "legacy-data"
        assert _mod._taxonomy_label("_manifests/old.json") == "manifest-infra"
        assert _mod._taxonomy_label("configs/svc.yaml") == "configs"
        assert _mod._taxonomy_label("databento-batch-registry/job.json") == "vendor-registry"

    def test_migration_backup_prefixes_labelled_not_unknown(self) -> None:
        # 2026-07-10 finding: the 2026-07-09 canonicalization migrators' backup-first
        # pattern writes top-level ``_migration_backup*`` prefixes that must be labelled,
        # not surfaced as ``unknown`` (the tradfi single-leg product-root migrator's own
        # ``_migration_backup_2026_07_09/`` + the sibling defi/cefi ``_migration_backups/``
        # plural form).
        assert (
            _mod._taxonomy_label("_migration_backup_2026_07_09/raw_tick_data/by_date/day=2020-01-02/x.parquet")
            == "migration-backup"
        )
        assert (
            _mod._taxonomy_label("_migration_backups/cefi_dated_perps_margin_marker_2026_07_09/x.parquet")
            == "migration-backup"
        )

    def test_needs_attribution_prefix_labelled_not_unknown(self) -> None:
        # 2026-07-10 finding: the full unlimited tradfi orphan sweep surfaced 71,830
        # objects under the top-level ``_needs_attribution/`` holding prefix as
        # ``unknown``. Both ``migrate_tradfi_to_v9_canonical.py`` and the defi walk
        # migrator write un-path-attributable legacy objects here deliberately
        # (operator 2026-06-08: preserve, never lose, never guess) — must be labelled,
        # not surfaced as unknown.
        assert (
            _mod._taxonomy_label(
                "_needs_attribution/raw_tick_data/by_date/day=2020-01-01/category=tradfi/venue=FX/"
                "data_type=ohlcv_24h/ticks_migrated_20260418T131054Z.parquet"
            )
            == "needs-attribution"
        )

    def test_top_level_label_is_startswith_only_never_substring(self) -> None:
        # ``data_type=dex_pools`` deep in a real data path must NOT be swept into the
        # top-level legacy-data label (the startswith-only contract)
        obj = (
            "raw_tick_data/by_date/day=2022-11-01/asset_group=defi/venue=ORCA/chain=SOLANA/"
            "instrument_type=pool/data_type=dex_pools/orca.parquet"
        )
        assert _mod._taxonomy_label(obj) == "service-data"


class TestLegacyInstrumentTypeCanonicalisation:
    """R1 2026-06-11: legacy instrument_type vocabulary canonicalised for coverage
    matching (tradfi plural ``equities``; prediction underlying-in-the-IT-slot)."""

    def test_equities_plural_maps_to_equity(self) -> None:
        assert _mod.canonical_match_instrument_type("tradfi", "equities") == "equity"

    def test_prediction_legacy_underlying_token_maps_to_market_grain(self) -> None:
        for token in ("BTC", "ETH", "SOL", "XRP", "DOGE", "OTHER"):
            assert _mod.canonical_match_instrument_type("prediction", token) == "prediction_market"

    def test_canonical_tokens_pass_through(self) -> None:
        assert _mod.canonical_match_instrument_type("cefi", "perpetual") == "perpetual"
        assert _mod.canonical_match_instrument_type("prediction", "prediction_market") == "prediction_market"
        assert _mod.canonical_match_instrument_type("prediction", "") == ""

    def test_prediction_legacy_object_covered_by_canonical_cell(self) -> None:
        # the residual prediction E=34 class: a legacy ``category=`` object carrying
        # instrument_type=BTC must read as covered once the canonical
        # (POLYMARKET, POLYGON, prediction_market) cell is recorded
        rows = [
            {
                "asset_group": "prediction",
                "venue": "POLYMARKET",
                "chain": "POLYGON",
                "instrument_type": "prediction_market",
                "data_type": "prediction_trades",
                "date": "2025-03-27",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        obj = (
            "raw_tick_data/by_date/day=2025-03-27/category=prediction/venue=POLYMARKET/"
            "instrument_type=BTC/data_type=prediction_trades/ticks.parquet"
        )
        cls, _key, _reason = _mod.classify_object(obj, "prediction", index, is_parquet=True, row_count=500)
        assert cls == OC.LEGACY_DUPLICATE

    def test_tradfi_equities_canonical_twin_covered_by_equity_cell(self) -> None:
        # canonical-shaped twin whose preserved hive tail carries the legacy plural —
        # must read as covered (class A) by the ``equity``-keyed manifest row
        rows = [
            {
                "asset_group": "tradfi",
                "venue": "NYSE",
                "chain": "",
                "instrument_type": "equity",
                "data_type": "ohlcv_1m",
                "date": "2024-01-02",
                "capture_status": "captured",
            }
        ]
        index = _mod.build_covered_index(rows)
        obj = (
            "raw_tick_data/by_date/day=2024-01-02/pipeline_mode=batch_databento/asset_group=tradfi/"
            "venue=NYSE/instrument_type=equity/data_type=ohlcv_1m/instrument_type=equities/venue=NYSE/"
            "NYSE:EQUITY:ABBV-USD_migrated.parquet"
        )
        cls, _key, _reason = _mod.classify_object(obj, "tradfi", index, is_parquet=True, row_count=10)
        assert cls == OC.CANONICAL_MANIFESTED
