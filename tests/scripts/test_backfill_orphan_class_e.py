"""Unit tests for backfill_orphan_class_e.py (R1 — operator ratification 2026-06-11 #1).

Credential-free + GCS-free: exercises the PURE characterisation + canonicalisation
surfaces (``characterize_object`` / ``_build_dest_path`` / ``canonicalise_frame``).
The GCS download/upload + ManifestWriter recording are thin orchestration over these.

Plan ref: ``master_data_canonicalisation_migration_catalogue_2026_06_07.md``
§ "Ratification todos" R1.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
from unified_api_contracts import PipelineMode


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "backfill_orphan_class_e.py"
    module_name = "_backfill_orphan_class_e_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()


class TestCharacterizeDefi:
    def test_registered_data_type_uses_source_priority_primary(self) -> None:
        # (defi, lending_indices) is registered in UAC SOURCE_PRIORITY → its primary
        # source (onchain_subgraph) is the write-gate-coherent stamp (MissingSourceError
        # rejects anything else — proven on the first defi --apply 2026-06-11).
        obj = (
            "raw_tick_data/by_date/day=2022-11-01/asset_group=defi/venue=KAMINO/chain=SOLANA/"
            "instrument_type=lending/data_type=lending_indices/kamino_lending_SOLANA_20260504.parquet"
        )
        target, reason = _mod.characterize_object("defi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "KAMINO"
        assert target.chain == "SOLANA"
        assert target.instrument_type == "lending"
        assert target.pipeline_mode == PipelineMode.BATCH_ONCHAIN_SUBGRAPH
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2022-11-01/pipeline_mode=batch_onchain_subgraph/asset_group=defi/"
            "venue=KAMINO/chain=SOLANA/instrument_type=lending/data_type=lending_indices/"
            "kamino_lending_SOLANA_20260504.parquet"
        )

    def test_unregistered_data_type_falls_back_to_onchain_rpc(self) -> None:
        # (defi, dex_pools) has no SOURCE_PRIORITY entry → the Solana capture handler's
        # stamp (mtds solana_defi_handler → BATCH_ONCHAIN_RPC) is the evidence.
        obj = (
            "raw_tick_data/by_date/day=2022-11-01/asset_group=defi/venue=ORCA/chain=SOLANA/"
            "instrument_type=pool/data_type=dex_pools/orca_SOLANA_20260504.parquet"
        )
        target, reason = _mod.characterize_object("defi", obj)
        assert reason == ""
        assert target is not None
        assert target.pipeline_mode == PipelineMode.BATCH_ONCHAIN_RPC

    def test_defi_missing_chain_escalates(self) -> None:
        obj = "raw_tick_data/by_date/day=2022-11-01/asset_group=defi/venue=SOMEPOOL/instrument_type=pool/data_type=dex_pools/x.parquet"
        target, reason = _mod.characterize_object("defi", obj)
        assert target is None
        assert "chain" in reason


class TestCharacterizeTradfi:
    def test_blank_instrument_type_nyse_attributed_to_equity(self) -> None:
        obj = "raw_tick_data/by_date/day=2023-05-02/category=tradfi/venue=NYSE/data_type=tbbo/ABBV.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.instrument_type == "equity"
        assert target.pipeline_mode == PipelineMode.BATCH_DATABENTO
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2023-05-02/pipeline_mode=batch_databento/asset_group=tradfi/"
            "venue=NYSE/instrument_type=equity/data_type=tbbo/ABBV.parquet"
        )

    def test_blank_instrument_type_unknown_venue_escalates(self) -> None:
        obj = "raw_tick_data/by_date/day=2023-05-02/category=tradfi/venue=LSE/data_type=tbbo/X.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert target is None
        assert "no canonical attribution rule" in reason

    def test_underlying_hive_tail_preserved(self) -> None:
        obj = (
            "raw_tick_data/by_date/day=2020-02-09/category=tradfi/venue=CME/instrument_type=options_chain/"
            "data_type=ohlcv_1m/underlying=ES/ticks_migrated_20260418T132019Z.parquet"
        )
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.underlying == "ES"
        assert target.dest_path.endswith("data_type=ohlcv_1m/underlying=ES/ticks_migrated_20260418T132019Z.parquet")
        assert "pipeline_mode=batch_databento/" in target.dest_path

    def test_instrument_key_vix_shape_maps_to_cboe_barchart(self) -> None:
        obj = "raw_tick_data/by_date/day=2025-01-02/data_type=ohlcv_15m/indices/CBOE/CBOE:INDEX:VIX-USD.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "CBOE"
        assert target.instrument_type == "index"
        assert target.pipeline_mode == PipelineMode.BATCH_BARCHART
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2025-01-02/pipeline_mode=batch_barchart/asset_group=tradfi/"
            "venue=CBOE/instrument_type=index/data_type=ohlcv_15m/CBOE:INDEX:VIX-USD.parquet"
        )

    def test_instrument_key_vix_shape_without_venue_dir(self) -> None:
        # the same family also occurs WITHOUT the venue directory level
        obj = "raw_tick_data/by_date/day=2025-01-02/data_type=ohlcv_15m/indices/CBOE:INDEX:VIX-USD.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "CBOE"
        assert target.instrument_type == "index"

    def test_instrument_key_spot_fx_maps_to_fx_yahoo(self) -> None:
        obj = "raw_tick_data/by_date/day=2025-01-02/data_type=ohlcv_24h/spot/YAHOO_FINANCE/YAHOO_FINANCE:SPOT_PAIR:KRW-USD.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "FX"  # manifested venue for the FX spot corpus
        assert target.instrument_type == "spot_pair"
        assert target.pipeline_mode == PipelineMode.BATCH_YAHOO

    def test_instrument_key_equities_maps_to_equity_databento(self) -> None:
        obj = "raw_tick_data/by_date/day=2025-01-02/data_type=ohlcv_1m/equities/NYSE/NYSE:EQUITY:ABBV-USD.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "NYSE"
        assert target.instrument_type == "equity"
        assert target.pipeline_mode == PipelineMode.BATCH_DATABENTO

    def test_legacy_plural_instrument_type_canonicalised(self) -> None:
        obj = "raw_tick_data/by_date/day=2024-01-02/category=tradfi/venue=NYSE/instrument_type=equities/data_type=ohlcv_1m/A.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.instrument_type == "equity"

    def test_already_canonical_path_is_record_only(self) -> None:
        obj = (
            "raw_tick_data/by_date/day=2026-05-06/pipeline_mode=batch_databento/asset_group=tradfi/"
            "venue=NASDAQ/instrument_type=equity/data_type=trades/ETHA.parquet"
        )
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.pipeline_mode == PipelineMode.BATCH_DATABENTO
        assert "record-only" in target.evidence


class TestCharacterizePrediction:
    def test_underlying_in_instrument_type_slot_canonicalised(self) -> None:
        obj = (
            "raw_tick_data/by_date/day=2025-03-27/category=prediction/venue=POLYMARKET/"
            "instrument_type=BTC/data_type=prediction_trades/ticks_migrated_20260419.parquet"
        )
        target, reason = _mod.characterize_object("prediction", obj)
        assert reason == ""
        assert target is not None
        assert target.instrument_type == "prediction_market"
        assert target.underlying == "BTC"
        assert target.chain == "POLYGON"
        assert target.pipeline_mode == PipelineMode.BATCH_POLYMARKET_CLOB
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2025-03-27/pipeline_mode=batch_polymarket_clob/asset_group=prediction/"
            "venue=POLYMARKET/chain=POLYGON/instrument_type=prediction_market/data_type=prediction_trades/"
            "underlying=BTC/ticks_migrated_20260419.parquet"
        )


class TestCanonicaliseFrame:
    def test_alias_rename_applied_from_schema_spec(self) -> None:
        # the R2 source_aliases map: polymarket conditionId → canonical condition_id
        df = pd.DataFrame({"conditionId": ["0xabc"], "price": [0.5]})
        out = _mod.canonicalise_frame(
            df,
            asset_group="prediction",
            data_type="trades",
            source="polymarket_clob",
            available_at=datetime(2026, 4, 19, tzinfo=UTC),
        )
        assert "condition_id" in out.columns
        assert "conditionId" not in out.columns

    def test_rename_never_clobbers_existing_canonical_column(self) -> None:
        df = pd.DataFrame({"conditionId": ["0xraw"], "condition_id": ["0xcanon"], "price": [0.5]})
        out = _mod.canonicalise_frame(
            df,
            asset_group="prediction",
            data_type="trades",
            source="polymarket_clob",
            available_at=datetime(2026, 4, 19, tzinfo=UTC),
        )
        # both survive: canonical untouched, raw alias kept (no silent overwrite)
        assert list(out["condition_id"]) == ["0xcanon"]
        assert "conditionId" in out.columns

    def test_available_at_and_source_stamped(self) -> None:
        df = pd.DataFrame({"price": [1.0, 2.0]})
        ts = datetime(2026, 5, 12, 17, 12, 34, tzinfo=UTC)
        out = _mod.canonicalise_frame(df, asset_group="tradfi", data_type="tbbo", source="databento", available_at=ts)
        assert "available_at" in out.columns
        assert out["available_at"].notna().all()
        assert pd.Timestamp(out["available_at"].iloc[0]) == pd.Timestamp(ts)
        assert set(out["source"]) == {"databento"}

    def test_existing_available_at_and_source_untouched(self) -> None:
        ts_orig = pd.Timestamp("2025-01-01T00:00:00Z")
        df = pd.DataFrame({"price": [1.0], "available_at": [ts_orig], "source": ["massive"]})
        out = _mod.canonicalise_frame(
            df,
            asset_group="tradfi",
            data_type="tbbo",
            source="databento",
            available_at=datetime(2026, 5, 12, tzinfo=UTC),
        )
        assert out["available_at"].iloc[0] == ts_orig
        assert out["source"].iloc[0] == "massive"


class TestPlanGrouping:
    def test_cell_key_groups_by_full_shard_dims(self) -> None:
        target = _mod.CanonicalTarget(
            venue="CME",
            chain="",
            instrument_type="options_chain",
            data_type="ohlcv_1m",
            underlying="ES",
            pipeline_mode=PipelineMode.BATCH_DATABENTO,
            dest_path="x",
            evidence="",
        )
        plan = _mod.OrphanPlan(uri="gs://b/x", day="2020-02-09", status="CONVERT", target=target)
        assert _mod._cell_key(plan) == (
            "2020-02-09",
            "CME",
            "",
            "options_chain",
            "ohlcv_1m",
            "ES",
            "batch_databento",
        )


class TestInstrumentKeyVenueDirShapes:
    def test_chain_root_leaf_with_venue_dir(self) -> None:
        # futures_chain/CME/CORN.parquet — venue from the directory, underlying from
        # the bare chain-root leaf
        obj = "raw_tick_data/by_date/day=2025-01-06/data_type=ohlcv_1m/futures_chain/CME/CORN.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "CME"
        assert target.instrument_type == "futures_chain"
        assert target.underlying == "CORN"
        assert target.pipeline_mode == PipelineMode.BATCH_DATABENTO
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2025-01-06/pipeline_mode=batch_databento/asset_group=tradfi/"
            "venue=CME/instrument_type=futures_chain/data_type=ohlcv_1m/underlying=CORN/CORN.parquet"
        )

    def test_bare_root_leaf_without_venue_dir_escalates(self) -> None:
        # no venue directory + no instrument key → identity unknown → escalate
        obj = "raw_tick_data/by_date/day=2025-01-06/data_type=ohlcv_1m/futures_chain/CORN.parquet"
        target, reason = _mod.characterize_object("tradfi", obj)
        assert target is None
        assert "unrecognised path shape" in reason


class TestCharacterizeCefi:
    """R1 2026-06-11 cefi support: RECORD-ONLY always (operator-ratified) — canonical
    batch_tardis paths keep their parsed pipeline_mode; bare-legacy twins attribute to
    batch_tardis; neither ever converts/uploads."""

    def test_canonical_tardis_path_is_record_only(self) -> None:
        obj = (
            "raw_tick_data/by_date/day=2020-01-02/pipeline_mode=batch_tardis/asset_group=cefi/"
            "venue=OKX-FUTURES/instrument_type=perpetual/data_type=book_snapshot_5/BTC-USD-200103.parquet"
        )
        target, reason = _mod.characterize_object("cefi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "OKX-FUTURES"
        assert target.instrument_type == "perpetual"
        assert target.pipeline_mode == PipelineMode.BATCH_TARDIS
        assert "record-only" in target.evidence

    def test_bare_legacy_twin_is_record_only_batch_tardis(self) -> None:
        obj = (
            "raw_tick_data/by_date/day=2020-08-23/asset_group=cefi/venue=DERIBIT/"
            "instrument_type=futures_chain/data_type=trades/underlying=BTC/quote=USD/margin=inverse/ticks.parquet"
        )
        target, reason = _mod.characterize_object("cefi", obj)
        assert reason == ""
        assert target is not None
        assert target.venue == "DERIBIT"
        assert target.instrument_type == "futures_chain"
        assert target.underlying == "BTC"
        assert target.pipeline_mode == PipelineMode.BATCH_TARDIS
        assert "record-only" in target.evidence

    def test_missing_venue_escalates(self) -> None:
        obj = "raw_tick_data/by_date/day=2020-08-23/asset_group=cefi/data_type=trades/x.parquet"
        target, reason = _mod.characterize_object("cefi", obj)
        assert target is None
        assert "venue" in reason


class _FakeRangedClient:
    """In-memory storage client serving ranged reads from real parquet bytes."""

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = blobs

    def get_blob_metadata(self, bucket: str, path: str) -> object:
        class _Meta:
            size = len(self._blobs[path])

        return _Meta()

    def download_bytes_range(self, bucket: str, path: str, start: int, end: int) -> bytes:
        return self._blobs[path][start:end]


class TestFooterRead:
    def test_footer_read_returns_rows_and_columns(self, monkeypatch) -> None:
        import io as _io

        import unified_trading_library as _utl

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0], "available_at": pd.Timestamp("2026-01-01", tz="UTC")})
        buf = _io.BytesIO()
        df.to_parquet(buf, index=False)
        client = _FakeRangedClient({"a/b.parquet": buf.getvalue()})
        monkeypatch.setattr(_utl, "get_storage_client", lambda: client)
        rows, cols, schema = _mod._read_parquet_footer("bkt", "a/b.parquet")
        assert rows == 3
        assert "price" in cols and "available_at" in cols
        empty = _mod._empty_frame_from_schema(schema)
        assert list(empty.columns) == cols
        assert len(empty) == 0

    def test_footer_read_with_known_size_skips_metadata(self, monkeypatch) -> None:
        import io as _io

        import unified_trading_library as _utl

        df = pd.DataFrame({"x": list(range(10))})
        buf = _io.BytesIO()
        df.to_parquet(buf, index=False)
        raw = buf.getvalue()

        class _NoMetaClient(_FakeRangedClient):
            def get_blob_metadata(self, bucket: str, path: str) -> object:
                raise AssertionError("metadata GET must be skipped when size is known")

        monkeypatch.setattr(_utl, "get_storage_client", lambda: _NoMetaClient({"p": raw}))
        rows, cols, _schema = _mod._read_parquet_footer("bkt", "p", size=len(raw))
        assert rows == 10
        assert cols == ["x"]


class TestRecordCellsRecordsEveryCell:
    """The R1 record-pass fix: every characterised cell records via the footer-exact
    schema frame — no retained-frame dependency (tradfi 249-of-many regression)."""

    def _result(self, day: str, venue: str, dt: str, rows: int, status: str = "RECORD_ONLY") -> object:
        target = _mod.CanonicalTarget(
            venue=venue,
            chain="",
            instrument_type="perpetual",
            data_type=dt,
            underlying="",
            pipeline_mode=PipelineMode.BATCH_TARDIS,
            dest_path=f"raw/{day}/{venue}/{dt}.parquet",
            evidence="record-only",
        )
        plan = _mod.OrphanPlan(uri=f"gs://b/{day}/{venue}/x.parquet", day=day, status=status, target=target)
        return _mod.ConvertResult(
            plan=plan,
            row_count=rows,
            uploaded=False,
            schema_df=pd.DataFrame({"exchange": pd.Series(dtype=str), "price": pd.Series(dtype=float)}),
            footer_cols=["exchange", "price"],
        )

    def test_every_cell_recorded_with_footer_exact_row_counts(self, monkeypatch) -> None:
        import unified_trading_library as _utl

        calls: list[dict[str, object]] = []

        class _FakeWriter:
            def __init__(self, **kwargs: object) -> None:
                pass

            def record_captured(self, **kwargs: object) -> None:
                calls.append(kwargs)

            def close(self) -> None:
                pass

        monkeypatch.setattr(_utl, "ManifestWriter", _FakeWriter)
        results = [
            self._result("2020-01-02", "OKX-FUTURES", "trades", 100),
            self._result("2020-01-02", "OKX-FUTURES", "trades", 50),  # same cell — summed
            self._result("2020-01-03", "DERIBIT", "trades", 7),
        ]
        recorded, errors = _mod.record_cells("cefi", "bkt", results, apply=True)
        assert errors == []
        assert recorded == 2
        by_day = {str(c["row_key"]["date"]): c for c in calls}  # type: ignore[index]
        assert by_day["2020-01-02"]["row_count"] == 150
        assert by_day["2020-01-03"]["row_count"] == 7
        # the record df is footer-exact + provenance-stamped, zero rows (empty path
        # passes the available_at gate by design)
        df = by_day["2020-01-02"]["df"]
        assert len(df) == 0  # type: ignore[arg-type]
        assert "available_at" in df.columns  # type: ignore[union-attr]
        assert "source" in df.columns  # type: ignore[union-attr]
        # tradfi/prediction/cefi cells are not chain-scoped → chain omitted from row_key
        assert "chain" not in by_day["2020-01-02"]["row_key"]  # type: ignore[operator]
        assert by_day["2020-01-02"]["source"] == "tardis"
