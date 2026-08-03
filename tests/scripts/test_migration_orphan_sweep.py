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

    def test_defi_venue_chain_split_guarded_against_unknown_chain_suffix(self) -> None:
        """Pins defi_cefi_venue_chain_axis_contamination_2026_07_28.md.

        A dash-bearing venue whose suffix is NOT a real UAC MAINNET_CHAIN_IDS member
        (e.g. a CeFi "BITGET-FUTURES" venue physically mis-landed in the DeFi bucket)
        must NOT be split — the unconditional partition() previously produced the
        corrupted venue="BITGET", chain="FUTURES" manifest rows this doc root-caused.
        """
        segs = {"venue": "BITGET-FUTURES", "data_type": "perp_daily_ctx", "instrument_type": "perpetual"}
        key = _mod.shard_key_from_segments("defi", segs)
        assert key.venue == "BITGET-FUTURES"
        assert key.chain == ""

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


class TestFooterVerifyConcurrency:
    """Regression for migration_orphan_sweep_performance_decay_2026_07_22.md — the
    sweep's ``workers`` parameter was threaded through but never used, so every
    footer-read ran sequentially. These prove the batched/concurrent path preserves
    the exact per-object semantics the old inline call had."""

    def test_footer_verify_pending_empty_returns_empty_dict(self) -> None:
        assert _mod._footer_verify_pending(object(), "bucket", [], workers=8) == {}

    def test_footer_verify_pending_maps_batch_index_to_row_count(self, monkeypatch) -> None:
        # index in the returned dict is the BATCH index (first tuple element), not
        # the pending-list position — the two coincide here only because every
        # object in the batch needed a footer read.
        rows_by_name = {"a.parquet": 0, "b.parquet": 5, "c.parquet": None}
        monkeypatch.setattr(_mod, "_footer_row_count", lambda _client, _bucket, name: rows_by_name[name])
        pending = [(0, "a.parquet"), (3, "b.parquet"), (7, "c.parquet")]
        result = _mod._footer_verify_pending(object(), "bucket", pending, workers=4)
        assert result == {0: 0, 3: 5, 7: None}

    def test_finalize_swept_batch_demotes_zero_row_to_junk_and_drops_from_actionable(self) -> None:
        key = _mod.ShardKey(
            asset_group="cefi", venue="BINANCE-FUTURES", chain="", instrument_type="perpetual", data_type="trades"
        )
        blob = type("Blob", (), {"size": 100, "crc32c": "abc"})()
        batch = [("day=2024-01-01/x.parquet", blob, OC.ORPHAN_REAL, key, "real data (rows>0)")]
        class_counts, sizing, actionable = _mod.Counter(), _mod.SizingRollup.empty(), []
        _mod._finalize_swept_batch(
            batch,
            {0: 0},
            bucket="bkt",
            asset_group="cefi",
            class_counts=class_counts,
            sizing=sizing,
            actionable=actionable,
        )
        assert class_counts[OC.JUNK.value] == 1
        assert class_counts[OC.ORPHAN_REAL.value] == 0
        assert actionable == []

    def test_finalize_swept_batch_keeps_orphan_real_without_a_footer_result(self) -> None:
        # large objects (>=256KB) never get a pending entry at all — footer_rows.get
        # returns None (not 0), so classification must NOT be demoted.
        key = _mod.ShardKey(
            asset_group="cefi", venue="BINANCE-FUTURES", chain="", instrument_type="perpetual", data_type="trades"
        )
        blob = type("Blob", (), {"size": 5_000_000, "crc32c": "def"})()
        batch = [("day=2024-01-01/y.parquet", blob, OC.ORPHAN_REAL, key, "real data (rows>0)")]
        class_counts, sizing, actionable = _mod.Counter(), _mod.SizingRollup.empty(), []
        _mod._finalize_swept_batch(
            batch,
            {},
            bucket="bkt",
            asset_group="cefi",
            class_counts=class_counts,
            sizing=sizing,
            actionable=actionable,
        )
        assert class_counts[OC.ORPHAN_REAL.value] == 1
        assert len(actionable) == 1
        assert actionable[0].obj_class == OC.ORPHAN_REAL


class _FakeBlob:
    def __init__(self, name: str, size: int = 100, crc32c: str = "x") -> None:
        self.name = name
        self.size = size
        self.crc32c = crc32c


class _FakeStorageClient:
    """In-memory GCS stand-in — credential-free, mirrors just the surface the sweep's
    checkpoint helpers + ``run_sweep`` touch (blob_exists/download_bytes/
    upload_from_file_obj/delete_blob/list_blobs, incl. GCS's INCLUSIVE start_offset)."""

    def __init__(self, object_names: list[str] | None = None) -> None:
        self._blobs: dict[tuple[str, str], bytes] = {}
        self._objects = sorted(object_names or [])

    def blob_exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self._blobs

    def download_bytes(self, bucket: str, path: str) -> bytes:
        return self._blobs[(bucket, path)]

    def upload_from_file_obj(self, bucket: str, path: str, file_obj) -> None:
        self._blobs[(bucket, path)] = file_obj.read()

    def delete_blob(self, bucket: str, path: str) -> bool:
        return self._blobs.pop((bucket, path), None) is not None

    def list_blobs(self, bucket: str, prefix: str = "", delimiter=None, max_results=None, start_offset: str = ""):
        # Union of the seeded WALK data objects (self._objects) and anything actually
        # uploaded via upload_from_file_obj (self._blobs, e.g. checkpoint shard/state
        # files) — real GCS list_blobs would see both; a fake that only saw the seeded
        # walk objects silently hid checkpoint shards from _delete_checkpoint's own
        # prefix-based enumeration (found 2026-07-23 chasing a real test failure).
        names = set(self._objects) | {k[1] for k in self._blobs if k[0] == bucket}
        for name in sorted(names):
            if start_offset and name < start_offset:
                continue
            if name.startswith(prefix):
                yield _FakeBlob(name)


class TestCheckpointRoundTrip:
    """Regression for migration_orphan_sweep_performance_decay_2026_07_22.md todo 4 — a
    preempted SPOT VM lost 100% of its in-memory orphan-E finds (defi hit this TWICE).
    These prove the checkpoint state + SHARDED actionable rows survive a write/load
    round trip without a live GCS/credentials dependency. Sharding itself (found
    2026-07-23: defi's e2-standard-4 VM was OOM-killed at 6.8M actionable rows resident
    in memory — the original single-growing-file checkpoint never cleared them) is
    covered by ``test_write_checkpoint_never_grows_a_single_actionable_file`` below."""

    def test_load_checkpoint_returns_none_when_absent(self) -> None:
        client = _FakeStorageClient()
        assert _mod._load_checkpoint(client, "bkt", "cefi") is None
        assert _mod._read_all_actionable_shards(client, "bkt", "cefi", 0) == []

    def _sample_obj(self, uri: str = "gs://bkt/x.parquet") -> object:
        key = _mod.ShardKey(
            asset_group="cefi", venue="BINANCE-FUTURES", chain="", instrument_type="perpetual", data_type="trades"
        )
        return _mod.SweptObject(
            uri=uri,
            asset_group="cefi",
            obj_class=OC.ORPHAN_REAL,
            venue=key.venue,
            chain=key.chain,
            instrument_type=key.instrument_type,
            data_type=key.data_type,
            day="2024-07-01",
            pipeline_mode="batch_tardis",
            source="tardis",
            size_bytes=123,
            crc32c="abc",
            reason="real data (rows>0) with NO manifest row — backfill record_captured",
        )

    def test_checkpoint_round_trip_state_and_actionable_shard(self) -> None:
        client = _FakeStorageClient()
        obj = self._sample_obj()
        new_shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="x.parquet",
            seen=1,
            class_counts=_mod.Counter({OC.ORPHAN_REAL.value: 1}),
            prefix_taxonomy=_mod.Counter({"service-data": 1}),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[obj],
            shard_index=0,
        )
        assert new_shard_count == 1
        ckpt = _mod._load_checkpoint(client, "bkt", "cefi")
        assert ckpt is not None
        assert ckpt.last_name == "x.parquet"
        assert ckpt.seen == 1
        assert ckpt.class_counts == {OC.ORPHAN_REAL.value: 1}
        assert ckpt.prefix_taxonomy == {"service-data": 1}
        assert ckpt.shard_count == 1
        loaded = _mod._read_all_actionable_shards(client, "bkt", "cefi", ckpt.shard_count)
        assert len(loaded) == 1
        assert loaded[0].uri == obj.uri
        assert loaded[0].obj_class == OC.ORPHAN_REAL
        assert loaded[0].size_bytes == 123

    def test_write_checkpoint_with_no_new_rows_does_not_bump_shard_count(self) -> None:
        client = _FakeStorageClient()
        new_shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="x.parquet",
            seen=1,
            class_counts=_mod.Counter(),
            prefix_taxonomy=_mod.Counter(),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[],
            shard_index=0,
        )
        assert new_shard_count == 0
        assert not client.blob_exists("bkt", _mod._checkpoint_actionable_shard_path("cefi", 0))

    def test_write_checkpoint_never_grows_a_single_actionable_file(self) -> None:
        """THE regression this shard design fixes: two checkpoint writes must each land
        their OWN shard (bounded size), never accumulate into one ever-growing file."""
        client = _FakeStorageClient()
        shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="a.parquet",
            seen=1,
            class_counts=_mod.Counter(),
            prefix_taxonomy=_mod.Counter(),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[self._sample_obj("gs://bkt/a.parquet")],
            shard_index=0,
        )
        shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="b.parquet",
            seen=2,
            class_counts=_mod.Counter(),
            prefix_taxonomy=_mod.Counter(),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[self._sample_obj("gs://bkt/b.parquet")],
            shard_index=shard_count,
        )
        assert shard_count == 2
        shard0 = _mod._read_actionable_parquet_at(client, "bkt", _mod._checkpoint_actionable_shard_path("cefi", 0))
        shard1 = _mod._read_actionable_parquet_at(client, "bkt", _mod._checkpoint_actionable_shard_path("cefi", 1))
        assert [o.uri for o in shard0] == ["gs://bkt/a.parquet"]
        assert [o.uri for o in shard1] == ["gs://bkt/b.parquet"]
        merged = _mod._read_all_actionable_shards(client, "bkt", "cefi", shard_count)
        assert [o.uri for o in merged] == ["gs://bkt/a.parquet", "gs://bkt/b.parquet"]

    def test_delete_checkpoint_removes_state_and_all_shards(self) -> None:
        client = _FakeStorageClient()
        shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="a.parquet",
            seen=1,
            class_counts=_mod.Counter(),
            prefix_taxonomy=_mod.Counter(),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[self._sample_obj("gs://bkt/a.parquet")],
            shard_index=0,
        )
        shard_count = _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name="b.parquet",
            seen=2,
            class_counts=_mod.Counter(),
            prefix_taxonomy=_mod.Counter(),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[self._sample_obj("gs://bkt/b.parquet")],
            shard_index=shard_count,
        )
        assert client.blob_exists("bkt", _mod._checkpoint_state_path("cefi"))
        _mod._delete_checkpoint(client, "bkt", "cefi")
        assert not client.blob_exists("bkt", _mod._checkpoint_state_path("cefi"))
        for i in range(shard_count):
            assert not client.blob_exists("bkt", _mod._checkpoint_actionable_shard_path("cefi", i))
        # idempotent — deleting an already-clean checkpoint must not raise.
        _mod._delete_checkpoint(client, "bkt", "cefi")


def _orphan_path(day: str, leaf: str) -> str:
    return (
        f"raw_tick_data/by_date/day={day}/pipeline_mode=batch_tardis/asset_group=cefi/"
        f"venue=BINANCE-FUTURES/instrument_type=perpetual/data_type=trades/{leaf}.parquet"
    )


class TestRunSweepResume:
    """``run_sweep`` resuming from a checkpoint must seed every accumulator AND resume
    the GCS walk via the (inclusive) ``start_offset`` without double-counting or
    double-writing the one boundary object — the exact off-by-one this feature lives or
    dies on."""

    def _patch_sweep_deps(self, monkeypatch, client: _FakeStorageClient) -> None:
        import unified_trading_library

        monkeypatch.setattr(unified_trading_library, "get_storage_client", lambda: client)
        monkeypatch.setattr(_mod, "_resolve_bucket", lambda asset_group, cloud="gcp": "bkt")

    def test_resume_seeds_counters_and_skips_boundary_object(self, monkeypatch) -> None:
        n1, n2, n3, n4 = (_orphan_path(f"2024-07-0{i}", c) for i, c in enumerate("abcd", start=1))
        client = _FakeStorageClient([n1, n2, n3, n4])
        self._patch_sweep_deps(monkeypatch, client)

        # Seed a checkpoint as if a prior (preempted) run already swept n1 + n2.
        obj1 = _mod.SweptObject(
            uri=f"gs://bkt/{n1}",
            asset_group="cefi",
            obj_class=OC.ORPHAN_REAL,
            venue="BINANCE-FUTURES",
            chain="",
            instrument_type="perpetual",
            data_type="trades",
            day="2024-07-01",
            pipeline_mode="batch_tardis",
            source="tardis",
            size_bytes=100,
            crc32c="x",
            reason="real data (rows>0) with NO manifest row — backfill record_captured",
        )
        obj2 = _mod.SweptObject(**{**vars(obj1), "uri": f"gs://bkt/{n2}", "day": "2024-07-02"})
        _mod._write_checkpoint(
            client,
            "bkt",
            "cefi",
            last_name=n2,
            seen=2,
            class_counts=_mod.Counter({OC.ORPHAN_REAL.value: 2}),
            prefix_taxonomy=_mod.Counter({"service-data": 2}),
            sizing=_mod.SizingRollup.empty(),
            actionable_since_last_checkpoint=[obj1, obj2],
            shard_index=0,
        )

        class_counts, _prefix_taxonomy, _sizing, actionable = _mod.run_sweep("cefi", workers=2)

        # n1 + n2 came from the checkpoint's shard (merged in only at final completion —
        # never held resident during the walk itself), n3 + n4 are newly swept — n2 must
        # NOT be double-counted despite GCS start_offset being inclusive of it.
        assert class_counts[OC.ORPHAN_REAL.value] == 4
        assert sorted(o.uri for o in actionable) == sorted(f"gs://bkt/{n}" for n in (n1, n2, n3, n4))
        # a genuinely clean full-walk completion deletes the checkpoint + every shard.
        assert not client.blob_exists("bkt", _mod._checkpoint_state_path("cefi"))
        assert not client.blob_exists("bkt", _mod._checkpoint_actionable_shard_path("cefi", 0))

    def test_limit_triggered_stop_preserves_checkpoint_for_a_future_resume(self, monkeypatch) -> None:
        n1, n2, n3 = (_orphan_path(f"2024-08-0{i}", c) for i, c in enumerate("abc", start=1))
        client = _FakeStorageClient([n1, n2, n3])
        self._patch_sweep_deps(monkeypatch, client)
        monkeypatch.setattr(_mod, "_SWEEP_BATCH_SIZE", 1)
        monkeypatch.setattr(_mod, "_CHECKPOINT_BATCH_INTERVAL", 1)

        _mod.run_sweep("cefi", workers=2, limit=1)

        # a --limit-triggered stop is NOT a clean completion — the checkpoint written at
        # the batch boundary must survive for a later resume, not be deleted.
        assert client.blob_exists("bkt", _mod._checkpoint_state_path("cefi"))

    def test_multiple_checkpoints_never_accumulate_into_one_growing_shard(self, monkeypatch) -> None:
        """THE regression this whole shard design exists for: defi's e2-standard-4 VM
        was OOM-killed at 6.8M actionable rows resident in memory because the original
        checkpoint fix wrote the WHOLE growing actionable list every interval and never
        cleared it. Forces a checkpoint after every single object and spies on every
        real ``_write_checkpoint`` call to prove each one only ever sees the ONE new row
        since the last checkpoint — never a cumulative, ever-growing list."""
        names = [_orphan_path(f"2024-09-0{i}", c) for i, c in enumerate("abcd", start=1)]
        client = _FakeStorageClient(names)
        self._patch_sweep_deps(monkeypatch, client)
        monkeypatch.setattr(_mod, "_SWEEP_BATCH_SIZE", 1)
        monkeypatch.setattr(_mod, "_CHECKPOINT_BATCH_INTERVAL", 1)

        seen_batch_sizes: list[int] = []
        real_write_checkpoint = _mod._write_checkpoint

        def _spy_write_checkpoint(*args, **kwargs):
            seen_batch_sizes.append(len(kwargs["actionable_since_last_checkpoint"]))
            return real_write_checkpoint(*args, **kwargs)

        monkeypatch.setattr(_mod, "_write_checkpoint", _spy_write_checkpoint)

        class_counts, _prefix_taxonomy, _sizing, actionable = _mod.run_sweep("cefi", workers=2)

        assert class_counts[OC.ORPHAN_REAL.value] == 4
        assert len(actionable) == 4
        # every checkpoint write saw exactly the ONE new row since the last one — never
        # a growing cumulative count (1, 2, 3, 4 would mean the OOM bug is back).
        assert seen_batch_sizes == [1, 1, 1, 1]


class TestLoadManifestedCellsColumnProjection:
    """Regression found 2026-07-24 running defi's backfill dry-run: defi's
    ``availability_index.parquet`` had grown to 23,977,316 rows / 41 columns / 988 MiB
    on-disk (~6x growth in ~24h under ongoing production capture) and a full
    ``pd.read_parquet`` (every column) thrashed even an e2-highmem-8 (64GB) VM for
    15+ minutes with zero progress — the exact "system freeze during manifest load"
    signature previously fixed for cefi only by a machine-type bump
    (``migration_orphan_sweep_performance_decay_2026_07_22.md`` todo 2/7). These prove
    ``_load_manifested_cells`` reads ONLY the 6 columns ``build_covered_index`` needs
    and still produces the correct covered index, without a live GCS dependency."""

    @staticmethod
    def _write_index_parquet(rows: list[dict[str, object]]) -> bytes:
        import io

        import pandas as pd

        buf = io.BytesIO()
        pd.DataFrame(rows).to_parquet(buf, index=False)
        return buf.getvalue()

    def test_reads_only_the_coverage_columns_from_a_wide_manifest(self) -> None:
        # Simulate the real v9 manifest's width: every row carries a handful of extra
        # columns _load_manifested_cells has no business touching (mirrors the real
        # schema's instrument_id/error_reason/service_name/etc.) — if the fix
        # regresses to a full-column read, this test still passes (the extra columns
        # are harmless), but a parse-time crash on an exotic dtype in one of them
        # would only be caught by projecting columns in the first place.
        rows = [
            {
                "date": "2026-01-01",
                "venue": "JUPITER",
                "data_type": "dex_quote",
                "chain": "SOLANA",
                "instrument_type": "dex_pool",
                "capture_status": "captured",
                "instrument_id": "some very long id " * 50,  # wide, unused column
                "error_reason": None,
                "service_name": "instruments-service",
                "row_count": 12345,
            }
        ]
        raw = self._write_index_parquet(rows)
        client = _FakeStorageClient()
        client._blobs[("bkt", "_index/availability_index.parquet")] = raw

        index = _mod._load_manifested_cells(client, "bkt")

        assert ("2026-01-01", "dex_quote") in index
        assert ("JUPITER", "SOLANA", "dex_pool") in index[("2026-01-01", "dex_quote")]

    def test_missing_index_returns_empty(self) -> None:
        client = _FakeStorageClient()
        assert _mod._load_manifested_cells(client, "bkt") == {}

    def test_ignores_non_captured_rows(self) -> None:
        rows = [
            {
                "date": "2026-01-01",
                "venue": "JUPITER",
                "data_type": "dex_quote",
                "chain": "SOLANA",
                "instrument_type": "dex_pool",
                "capture_status": "attempted_failed",
            }
        ]
        raw = self._write_index_parquet(rows)
        client = _FakeStorageClient()
        client._blobs[("bkt", "_index/availability_index.parquet")] = raw

        index = _mod._load_manifested_cells(client, "bkt")

        assert index == {}
