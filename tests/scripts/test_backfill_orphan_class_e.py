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
    def test_solana_legacy_orphan_maps_to_onchain_rpc(self) -> None:
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
        assert target.pipeline_mode == PipelineMode.BATCH_ONCHAIN_RPC
        assert target.dest_path == (
            "raw_tick_data/by_date/day=2022-11-01/pipeline_mode=batch_onchain_rpc/asset_group=defi/"
            "venue=KAMINO/chain=SOLANA/instrument_type=lending/data_type=lending_indices/"
            "kamino_lending_SOLANA_20260504.parquet"
        )

    def test_defi_missing_chain_escalates(self) -> None:
        obj = "raw_tick_data/by_date/day=2022-11-01/asset_group=defi/venue=ORCA/instrument_type=pool/data_type=dex_pools/x.parquet"
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
