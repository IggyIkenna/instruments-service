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

from datetime import UTC, datetime, timedelta

import pandas as pd
from unified_api_contracts.predictions import CANONICAL_GROUP_METADATA, CanonicalQuestionGroup

from instruments_service.engine.orchestrator import (
    _build_market_lifecycle_df,
    _compute_prediction_shards,
    _extract_prediction_canonical_group,
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
