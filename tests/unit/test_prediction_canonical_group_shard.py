"""Unit tests for the canonical-question-group shard helpers in
``instruments_service.engine.orchestrator``.

Per ``unified-trading-pm/plans/active/predictions_master.plan.md``
Phase 1 critical-path: ``_extract_prediction_canonical_group`` /
``_compute_prediction_shards`` MUST call the UAC classifier and emit the
shard atom on the canonical-question-group axis (per CLAUDE.md
"Per-asset-group shard-key matrix → Prediction"). Legacy per-base_asset
sharding is replaced — bundled shards across recurring market_ids
mirror options-chain bundling, ready for the cluster-coverage gate at
``record_captured`` once MTDS Phase 2 wires the lifecycle reader.

These tests pin down:

* Polymarket records with HOURLY slugs route to ``BTC_UP_DOWN_HOURLY``.
* Polymarket records with daily slugs route to ``BTC_UP_DOWN_DAILY``.
* Unrecognised slugs / venues fall through to ``OTHER`` (Phase 1 plan
  body's "synthetic OTHER bucket" requirement so the data-status panel
  always renders a known bucket).
* Kalshi records (without UAC override entries) currently route to
  ``OTHER`` — the override path lights up later when consumers seed
  :data:`KALSHI_TICKER_TO_GROUP`.
* :func:`_compute_prediction_shards` aggregates per ``venue/group`` keys
  matching the manifest emit shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_api_contracts.predictions import CANONICAL_GROUP_METADATA, CanonicalQuestionGroup
from unified_trading_library import resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _build_market_lifecycle_df,
    _compute_prediction_shards,
    _extract_prediction_canonical_group,
    _get_instruments_bucket,
    _write_market_lifecycle,
    resolve_instruments_store_kind,
)


def _row(**fields: object) -> pd.Series:
    return pd.Series(fields)


class TestExtractPredictionCanonicalGroup:
    def test_polymarket_hourly_slug_routes_to_btc_hourly(self) -> None:
        row = _row(
            venue="POLYMARKET",
            instrument_key="0xabc123",
            raw_symbol="bitcoin-up-or-down-hour-march-26-2026-9am-et",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY.value

    def test_polymarket_eth_hourly_slug(self) -> None:
        row = _row(
            venue="POLYMARKET",
            instrument_key="0xeth1",
            raw_symbol="ethereum-up-or-down-hour-march-26-2026-9am-et",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.ETH_UP_DOWN_HOURLY.value

    def test_polymarket_unrecognised_slug_falls_to_other(self) -> None:
        row = _row(
            venue="POLYMARKET",
            instrument_key="0xother",
            raw_symbol="will-something-strange-happen-this-week",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.OTHER.value

    def test_kalshi_without_override_falls_to_other(self) -> None:
        """Kalshi rule classifier is override-only currently; an
        unrecognised ticker routes to OTHER per UAC SSOT.
        """
        row = _row(
            venue="kalshi",
            instrument_key="KXBTC-26MAR-90000",
            raw_symbol="KXBTC",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.OTHER.value

    def test_unknown_venue_routes_to_other(self) -> None:
        """Defensive — should never trigger in practice, but the
        helper must be robust against rows that slip through the
        prediction-venue gate at the writer.
        """
        row = _row(
            venue="UNKNOWN",
            instrument_key="some-id",
            raw_symbol="some-slug",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.OTHER.value

    def test_missing_instrument_key_routes_to_other(self) -> None:
        """No condition_id → no override hit; slug also unrecognised."""
        row = _row(
            venue="POLYMARKET",
            instrument_key="",
            raw_symbol="totally-unrecognised-slug",
        )

        assert _extract_prediction_canonical_group(row) == CanonicalQuestionGroup.OTHER.value


class TestComputePredictionShards:
    def test_aggregates_per_venue_canonical_group(self) -> None:
        """24 BTC hourly markets + 1 SPX daily + 5 OTHER → three buckets
        keyed on ``venue/group``, mirroring the manifest emit shape.
        """
        rows: list[dict[str, object]] = []
        # 24 BTC hourly market_ids — one shard atom (cluster bundle).
        for i in range(24):
            rows.append(
                {
                    "venue": "POLYMARKET",
                    "instrument_key": f"0xbtc-hourly-{i:02d}",
                    "raw_symbol": f"bitcoin-up-or-down-hour-2026-03-26-h{i:02d}",
                    "base_asset": f"BTC:UP_DOWN:2026-03-26:H{i:02d}",
                }
            )
        # 1 SPX daily.
        rows.append(
            {
                "venue": "POLYMARKET",
                "instrument_key": "0xspx-daily",
                "raw_symbol": "spx-up-or-down-daily-2026-03-26",
                "base_asset": "SPX:UP_DOWN:2026-03-26",
            }
        )
        # 5 unclassifiable markets → OTHER.
        for i in range(5):
            rows.append(
                {
                    "venue": "POLYMARKET",
                    "instrument_key": f"0xother-{i}",
                    "raw_symbol": f"will-something-strange-happen-{i}",
                    "base_asset": f"OTHER:{i}",
                }
            )

        df = pd.DataFrame(rows)
        shard_counts = _compute_prediction_shards(df)

        assert shard_counts["POLYMARKET/BTC_UP_DOWN_HOURLY"] == 24
        assert shard_counts["POLYMARKET/SPX_UP_DOWN_DAILY"] == 1
        assert shard_counts["POLYMARKET/OTHER"] == 5
        # Total preserved.
        assert sum(shard_counts.values()) == len(df)

    def test_empty_dataframe_returns_empty_dict(self) -> None:
        df = pd.DataFrame(columns=["venue", "instrument_key", "raw_symbol", "base_asset"])

        shard_counts = _compute_prediction_shards(df)

        assert shard_counts == {}

    def test_kalshi_aggregates_under_kalshi_prefix(self) -> None:
        """Kalshi rows aggregate under ``KALSHI/<group>`` (manifest
        venue-slot is uppercased) regardless of whether the source
        ``venue`` field arrived in lowercase or uppercase form.
        """
        df = pd.DataFrame(
            [
                {
                    "venue": "kalshi",
                    "instrument_key": "KXBTC-26MAR-90000",
                    "raw_symbol": "KXBTC",
                    "base_asset": "KXBTC",
                },
                {
                    "venue": "KALSHI",
                    "instrument_key": "KXSPX-26MAR-5000",
                    "raw_symbol": "KXSPX",
                    "base_asset": "KXSPX",
                },
            ]
        )

        shard_counts = _compute_prediction_shards(df)

        # Both route to OTHER (no override seeded yet) under KALSHI/OTHER.
        assert shard_counts.get("KALSHI/OTHER") == 2


def _lifecycle_df(
    instrument_keys: list[str],
    available_from: list[datetime],
    available_to: list[datetime],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "instrument_key": instrument_keys,
            "available_from_datetime": available_from,
            "available_to_datetime": available_to,
        }
    )


class TestBuildMarketLifecycleDf:
    """Unit tests for :func:`_build_market_lifecycle_df`."""

    def test_derives_required_columns(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        past = datetime(2026, 5, 20, 10, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(["0xabc"], [created], [past])

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert list(out.columns) == [
            "market_id",
            "canonical_question_group",
            "market_created_at",
            "resolution_time",
            "settlement_time",
            "status",
        ]
        assert len(out) == 1
        row = out.iloc[0]
        assert row["market_id"] == "0xabc"
        assert row["canonical_question_group"] == CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value

    def test_settlement_lag_applied(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 5, 20, 14, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(["mkt1"], [created], [settlement])

        expected_lag = CANONICAL_GROUP_METADATA[CanonicalQuestionGroup.SPX_UP_DOWN_DAILY].settlement_lag
        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.SPX_UP_DOWN_DAILY.value, now)

        row = out.iloc[0]
        assert row["resolution_time"] == pd.Timestamp(settlement) - expected_lag

    def test_status_settled_when_settlement_before_now(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        past_settlement = datetime(2026, 5, 20, 8, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(["settled-mkt"], [created], [past_settlement])

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert out.iloc[0]["status"] == "settled"

    def test_status_active_when_settlement_after_now(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        future_settlement = datetime(2026, 5, 30, 20, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(["active-mkt"], [created], [future_settlement])

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert out.iloc[0]["status"] == "active"

    def test_rows_with_null_available_from_dropped(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 5, 20, 10, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(
            ["valid-mkt", "null-from-mkt"],
            [created, None],  # type: ignore[list-item]
            [settlement, settlement],
        )

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert len(out) == 1
        assert out.iloc[0]["market_id"] == "valid-mkt"

    def test_rows_with_null_available_to_dropped(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(
            ["null-to-mkt"],
            [created],
            [None],  # type: ignore[list-item]
        )

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert out.empty

    def test_missing_required_columns_returns_empty(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        df = pd.DataFrame({"instrument_key": ["0xabc"]})

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value, now)

        assert out.empty

    def test_unknown_canonical_group_falls_back_to_other_lag(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 5, 20, 10, 0, 0, tzinfo=UTC)
        created = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        df = _lifecycle_df(["mkt"], [created], [settlement])

        other_lag = CANONICAL_GROUP_METADATA[CanonicalQuestionGroup.OTHER].settlement_lag
        out = _build_market_lifecycle_df(df, "TOTALLY_UNKNOWN_GROUP", now)

        row = out.iloc[0]
        assert row["canonical_question_group"] == "TOTALLY_UNKNOWN_GROUP"
        assert row["resolution_time"] == pd.Timestamp(settlement) - other_lag

    def test_multiple_markets_all_present(self) -> None:
        now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
        base = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        markets = [f"0xmkt{i}" for i in range(5)]
        settlements = [base + timedelta(days=i) for i in range(5)]
        createds = [base - timedelta(days=30) for _ in range(5)]
        df = _lifecycle_df(markets, createds, settlements)

        out = _build_market_lifecycle_df(df, CanonicalQuestionGroup.ETH_UP_DOWN_DAILY.value, now)

        assert len(out) == 5
        assert set(out["market_id"].tolist()) == set(markets)


class TestPredictionWriterManifestContract:
    """Verify the writer emits canonical shard atoms for prediction instruments.

    Contract (per ``data_status_coverage_gaps_and_prediction_manifest_fix_2026_05_22.md`` Phase 3.1):
      - data_type  = ``"prediction_canonical_question_group"`` (literal string)
      - underlying = ``<CanonicalQuestionGroup.value>``  (e.g. ``"BTC_UP_DOWN_HOURLY"``)

    These tests pin the bridge between ``_extract_prediction_canonical_group`` (the
    per-row classifier) and the writer's ``manifest.record_captured`` call so a future
    refactor cannot silently flip back to the legacy ``data_type=BTC`` format.
    """

    _PREDICTION_DATA_TYPE = "prediction_canonical_question_group"

    def test_hourly_btc_rows_map_to_canonical_underlying(self) -> None:
        """BTC hourly rows must resolve to ``BTC_UP_DOWN_HOURLY`` as the underlying
        (used verbatim in the ``record_captured`` row_key).
        """
        rows = [
            {
                "venue": "POLYMARKET",
                "instrument_key": f"0xbtc-h{i:02d}",
                "raw_symbol": f"bitcoin-up-or-down-hour-2026-05-22-h{i:02d}",
                "base_asset": f"BTC:H{i:02d}",
            }
            for i in range(8)
        ]
        df = pd.DataFrame(rows)

        shards = _compute_prediction_shards(df)
        group_key = "POLYMARKET/BTC_UP_DOWN_HOURLY"

        assert group_key in shards
        assert shards[group_key] == 8
        # Underlying value = canonical group string — must be a valid CanonicalQuestionGroup member.
        group_str = group_key.split("/", 1)[1]
        assert group_str == CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY.value

    def test_data_type_literal_is_canonical_constant(self) -> None:
        """The manifest data_type literal must equal 'prediction_canonical_question_group'."""
        assert self._PREDICTION_DATA_TYPE == "prediction_canonical_question_group"

    def test_underlying_is_canonical_group_enum_value(self) -> None:
        """Every shard key produced by ``_compute_prediction_shards`` has format
        ``VENUE/<CanonicalQuestionGroup.value>`` — the second segment IS the underlying
        that flows into ``record_captured(underlying=...)``.
        """
        rows = [
            {
                "venue": "POLYMARKET",
                "instrument_key": "0xspx",
                "raw_symbol": "spx-up-or-down-daily-2026-05-22",
                "base_asset": "SPX",
            },
            {
                "venue": "POLYMARKET",
                "instrument_key": "0xother",
                "raw_symbol": "who-will-win-the-tournament-2026",
                "base_asset": "OTHER",
            },
        ]
        df = pd.DataFrame(rows)
        shards = _compute_prediction_shards(df)

        valid_group_values = {g.value for g in CanonicalQuestionGroup}
        for key in shards:
            _venue, group_str = key.split("/", 1)
            assert group_str in valid_group_values, (
                f"shard key '{key}' has group_str='{group_str}' not in CanonicalQuestionGroup"
            )

    def test_mixed_prediction_venues_produce_separate_shards(self) -> None:
        """POLYMARKET and KALSHI rows go to distinct shard buckets; both use
        ``CanonicalQuestionGroup.OTHER.value`` as underlying for unrecognised slugs/tickers.
        """
        df = pd.DataFrame(
            [
                {
                    "venue": "POLYMARKET",
                    "instrument_key": "0xpoly",
                    "raw_symbol": "some-unknown-event",
                    "base_asset": "OTHER",
                },
                {
                    "venue": "kalshi",
                    "instrument_key": "KXUNKNOWN-26DEC-00000",
                    "raw_symbol": "KXUNKNOWN",
                    "base_asset": "OTHER",
                },
            ]
        )
        shards = _compute_prediction_shards(df)

        other_val = CanonicalQuestionGroup.OTHER.value
        assert shards.get(f"POLYMARKET/{other_val}") == 1
        assert shards.get(f"KALSHI/{other_val}") == 1


# ---------------------------------------------------------------------------
# _write_market_lifecycle — GCS path + columns + MTDS round-trip contract
# ---------------------------------------------------------------------------


@dataclass
class _FakeSink:
    """Minimal DataSink stand-in that captures write calls."""

    writes: list[dict[str, object]] = field(default_factory=list)

    def write(
        self,
        *,
        data: pd.DataFrame,
        partition: dict[str, str],
        format: str,
        filename: str,
    ) -> None:
        self.writes.append(
            {
                "data": data.copy(),
                "partition": dict(partition),
                "format": format,
                "filename": filename,
            }
        )


def _make_group_df(
    market_ids: list[str],
    from_dts: list[datetime],
    to_dts: list[datetime],
) -> pd.DataFrame:
    """Build a minimal prediction InstrumentRecord DataFrame with lifecycle fields."""
    return pd.DataFrame(
        {
            "instrument_key": market_ids,
            "available_from_datetime": from_dts,
            "available_to_datetime": to_dts,
        }
    )


class TestWriteMarketLifecycle:
    """Verify _write_market_lifecycle GCS path, output columns, and MTDS round-trip schema.

    The MTDS reader ``_load_market_lifecycle_for_date`` (base_prediction_adapter.py)
    expects:
      - bucket kind: ``instruments-store-prediction``
      - prefix: ``market_lifecycle/by_canonical_group/``
      - path shape: ``market_lifecycle/by_canonical_group/group={g}/day={d}/market_lifecycle.parquet``
      - columns consumed: ``market_id``, ``market_created_at``, ``settlement_time``

    IS writer ``_write_market_lifecycle`` uses:
      - sink prefix: ``market_lifecycle/by_canonical_group`` (via ``lifecycle_sink``)
      - partition: ``{"group": canonical_group_str, "day": date}``
      - filename: ``market_lifecycle.parquet``
      - output columns: ``market_id``, ``canonical_question_group``, ``market_created_at``,
                          ``resolution_time``, ``settlement_time``, ``status``, ``available_at``

    This test pins the schema contract so any drift between writer and reader is caught
    at CI rather than at runtime.
    """

    _GROUP = CanonicalQuestionGroup.BTC_UP_DOWN_DAILY.value
    _DATE = "2026-03-26"
    _VENUE = "POLYMARKET"

    def _run_write(
        self,
        market_ids: list[str],
        from_dts: list[datetime],
        to_dts: list[datetime],
    ) -> tuple[_FakeSink, MagicMock]:
        sink = _FakeSink()
        manifest = MagicMock()
        group_df = _make_group_df(market_ids, from_dts, to_dts)
        _write_market_lifecycle(
            sink=sink,  # type: ignore[arg-type]
            group_df=group_df,
            canonical_group_str=self._GROUP,
            date=self._DATE,
            manifest_venue=self._VENUE,
            manifest=manifest,
            pipeline_mode=PipelineMode.BATCH_POLYMARKET_GAMMA_API,
        )
        return sink, manifest

    def test_gcs_path_matches_mtds_reader_expectation(self) -> None:
        """Partition key + filename together construct the path
        ``market_lifecycle/by_canonical_group/group={g}/day={d}/market_lifecycle.parquet``
        that ``_load_market_lifecycle_for_date`` searches for.

        The sink prefix ``market_lifecycle/by_canonical_group`` is set in the orchestrator's
        ``lifecycle_sink = get_data_sink(..., prefix="market_lifecycle/by_canonical_group")``.
        Here we verify the partition dict and filename that compose the full object key.
        """
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        sink, _ = self._run_write(["0xabc"], [created], [settlement])

        assert len(sink.writes) == 1
        write = sink.writes[0]
        # Partition produces path segments group={g}/day={d}
        assert write["partition"] == {"group": self._GROUP, "day": self._DATE}
        assert write["filename"] == "market_lifecycle.parquet"

    def test_output_columns_superset_of_mtds_reader_required_cols(self) -> None:
        """MTDS reader reads ``market_id``, ``market_created_at``, ``settlement_time``.
        Writer output must contain all three (plus additional cols are fine).
        """
        mtds_required = {"market_id", "market_created_at", "settlement_time"}

        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        sink, _ = self._run_write(["0xbtc1", "0xbtc2"], [created, created], [settlement, settlement])

        assert len(sink.writes) == 1
        written_df = sink.writes[0]["data"]
        assert isinstance(written_df, pd.DataFrame)
        missing = mtds_required - set(written_df.columns)
        assert not missing, f"MTDS-required columns missing from writer output: {missing}"

    def test_market_id_equals_instrument_key(self) -> None:
        """market_id must be the condition_id / instrument_key so the MTDS
        lookup ``result[market_id]`` resolves correctly.
        """
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        sink, _ = self._run_write(["0xtest123"], [created], [settlement])

        written_df = sink.writes[0]["data"]
        assert written_df["market_id"].tolist() == ["0xtest123"]

    def test_datetimes_are_utc_aware(self) -> None:
        """Both ``market_created_at`` and ``settlement_time`` must be UTC-aware
        so the MTDS reader's ``_coerce_lifecycle_dt`` does not attach a
        spurious timezone (it would pass through naive datetimes unchecked).
        """
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        sink, _ = self._run_write(["0xtz"], [created], [settlement])

        written_df = sink.writes[0]["data"]
        mc_col = written_df["market_created_at"]
        st_col = written_df["settlement_time"]
        # pd.Timestamp with tz set → tz attribute is non-None
        assert mc_col.dtype.tz is not None, "market_created_at must be UTC-aware"
        assert st_col.dtype.tz is not None, "settlement_time must be UTC-aware"

    def test_available_at_column_present(self) -> None:
        """``available_at`` is stamped by ``stamp_available_at_explicit`` so UTL
        manifest assertions don't raise ``MissingAvailableAtError``.
        """
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        sink, _ = self._run_write(["0xavail"], [created], [settlement])

        written_df = sink.writes[0]["data"]
        assert "available_at" in written_df.columns

    def test_no_write_when_all_rows_missing_lifecycle_fields(self) -> None:
        """When every row has null available_from or available_to, nothing is written
        (matches ``_build_market_lifecycle_df`` contract).
        """
        none_dt: list[datetime] = [None]  # type: ignore[list-item]
        sink, _ = self._run_write(["0xnull"], none_dt, none_dt)

        assert len(sink.writes) == 0, "Expected no write when lifecycle fields are all null"

    def test_manifest_record_captured_called_on_success(self) -> None:
        """manifest.record_captured_from_counts must be called with the correct
        data_type so the prediction manifest row lands with
        ``data_type=prediction_market_lifecycle``.
        """
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        _, manifest = self._run_write(["0xmanifest"], [created], [settlement])

        manifest.record_captured_from_counts.assert_called_once()
        call_kwargs = manifest.record_captured_from_counts.call_args.kwargs
        assert call_kwargs["row_key"]["data_type"] == "prediction_market_lifecycle"
        assert call_kwargs["row_key"]["underlying"] == self._GROUP

    def test_multiple_markets_all_written(self) -> None:
        """All markets in the group end up in the single parquet file (bundled shard)."""
        n = 5
        created = datetime(2026, 3, 25, 9, 0, 0, tzinfo=UTC)
        settlement = datetime(2026, 3, 26, 13, 0, 0, tzinfo=UTC)
        market_ids = [f"0xmkt{i}" for i in range(n)]
        sink, _ = self._run_write(market_ids, [created] * n, [settlement] * n)

        assert len(sink.writes) == 1
        written_df = sink.writes[0]["data"]
        assert len(written_df) == n
        assert set(written_df["market_id"].tolist()) == set(market_ids)


# ---------------------------------------------------------------------------
# Bucket resolution — prediction routes to the dedicated flat kind so the IS
# write lands in the SAME bucket the MTDS lifecycle reader reads from.
# Root-cause regression guard for predictions item 552: the orchestrator used
# resolve_bucket_name(kind="instruments-store", asset_group="prediction"),
# which raises BucketNamingError (the instruments-store dict has no PREDICTION
# entry — prediction is a dedicated flat kind per cloud-providers.yaml), so the
# whole prediction write (instruments.parquet + market_lifecycle.parquet) crashed
# at bucket resolution and emitted ZERO objects.
# ---------------------------------------------------------------------------


class TestPredictionInstrumentsStoreBucket:
    """Verify prediction resolves the flat ``instruments-store-prediction`` kind."""

    # The kind the MTDS reader (_load_market_lifecycle_for_date) resolves.
    _MTDS_READER_KIND = "instruments-store-prediction"

    def test_resolve_kind_routes_prediction_to_flat_kind(self) -> None:
        """``prediction`` -> the dedicated flat kind (asset_group dropped, since
        flat kinds ignore it). The flat kind is what the MTDS reader + the
        prediction scripts use."""
        kind, kind_ag = resolve_instruments_store_kind("prediction")
        assert kind == self._MTDS_READER_KIND
        assert kind_ag is None

    def test_resolve_kind_leaves_other_asset_groups_on_dict_form(self) -> None:
        """Non-prediction asset_groups keep the per-asset_group ``instruments-store``
        dict form (kind unchanged, asset_group preserved)."""
        for ag in ("cefi", "defi", "tradfi", "sports"):
            kind, kind_ag = resolve_instruments_store_kind(ag)
            assert kind == "instruments-store"
            assert kind_ag == ag

    def test_resolve_kind_passes_through_none(self) -> None:
        """A None asset_group stays on the dict form (handled by resolve_bucket_name)."""
        kind, kind_ag = resolve_instruments_store_kind(None)
        assert kind == "instruments-store"
        assert kind_ag is None

    def test_get_instruments_bucket_prediction_does_not_raise(self) -> None:
        """The orchestrator's bucket resolver must NOT raise for prediction.

        Pre-fix this raised ``BucketNamingError("...no entry for
        asset_group='prediction'")``, aborting the whole prediction write.
        """
        bucket = _get_instruments_bucket("prediction")
        assert bucket  # non-empty
        assert "pred" in bucket

    def test_get_instruments_bucket_matches_mtds_reader_bucket(self) -> None:
        """The IS write bucket for prediction MUST equal the bucket the MTDS
        lifecycle reader reads from — else lifecycle parquets land where the
        reader never looks (silent 0-object miss)."""
        is_write_bucket = _get_instruments_bucket("prediction")
        mtds_read_bucket = resolve_bucket_name(cloud="gcp", kind=self._MTDS_READER_KIND)
        assert is_write_bucket == mtds_read_bucket

    def test_get_instruments_bucket_case_insensitive_prediction(self) -> None:
        """``PREDICTION`` (uppercase, as asset_groups[0] may arrive) lowercases
        and still routes to the flat kind."""
        assert _get_instruments_bucket("PREDICTION") == _get_instruments_bucket("prediction")
