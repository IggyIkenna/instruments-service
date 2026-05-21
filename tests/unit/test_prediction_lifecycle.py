"""Unit tests for the per-market lifecycle output of the prediction adapters.

Per ``unified-trading-pm/plans/active/predictions_master.plan.md``
Phase 1 critical-path: instruments-service Polymarket / Kalshi adapters
must capture ``market_created_at`` / ``resolution_time`` / ``settlement_time``
+ ``canonical_question_group`` membership per market_id, exposed via
:meth:`get_market_lifecycles` so the orchestrator can emit a
``MARKET_LIFECYCLE`` data_type parquet alongside the per-instrument shard.

These tests pin down:

* Polymarket :meth:`classify_lifecycle` derives canonical_question_group
  via the UAC classifier (with ``OTHER`` fallback when no classifier
  rule fires), populates settlement_time = resolution_time + group lag,
  and respects the ``closed`` / ``accepting_orders`` flags for
  current_status.
* Kalshi :meth:`classify_lifecycle` derives the same shape from the
  override dict path (currently the only routing surface for Kalshi
  tickers per UAC SSOT) with the +30d ``open_time`` fallback when the
  field is missing.
* :meth:`get_market_lifecycles` returns one row per market captured in
  ``_last_markets`` and silently drops markets that classify_lifecycle
  refuses (missing condition_id / unparseable timestamps).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from unified_api_contracts import KalshiMarket, PolymarketGammaMarket
from unified_api_contracts.predictions import (
    CANONICAL_GROUP_METADATA,
    CanonicalQuestionGroup,
)

from instruments_service.reference_data.adapters.prediction.kalshi import (
    KalshiReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.prediction.polymarket import (
    PolymarketReferenceDataAdapter,
)


def _polymarket_btc_hourly_market() -> PolymarketGammaMarket:
    """Synthetic Gamma payload for a BTC up/down hourly market.

    Question shape mirrors real Polymarket BTC up/down hourly markets so
    the rule classifier (BTC + RANGE_BRACKET + HOURLY) routes to
    ``BTC_UP_DOWN_HOURLY`` per UAC.
    """
    # Slug shape mirrors real Polymarket BTC-hourly markets:
    # ``bitcoin-up-or-down-hour-...`` activates the
    # CRYPTO_PRICE+BTC slug-prefix rule + RANGE_BRACKET (`up-or-down`
    # token) + HOURLY (`hour` token in combined slug+title).
    return PolymarketGammaMarket.model_validate(
        {
            "conditionId": "0xabc123",
            "marketSlug": "bitcoin-up-or-down-hour-march-26-2026-9am-et",
            "question": "Will Bitcoin be up or down on March 26, 2026 at 9am ET?",
            "outcomes": ["Up", "Down"],
            "createdAt": "2026-03-25T09:00:00Z",
            "closedTime": "2026-03-26T13:00:00Z",
            "endDateIso": "2026-03-26T13:00:00Z",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "event_title": "BTC up/down hourly",
            "event_slug": "btc-up-down-hourly-2026-03-26",
        }
    )


def _polymarket_unclassifiable_market() -> PolymarketGammaMarket:
    """Synthetic market that doesn't match any rule — routes to OTHER."""
    return PolymarketGammaMarket.model_validate(
        {
            "conditionId": "0xdef456",
            "marketSlug": "will-something-strange-happen",
            "question": "Will something strange happen this week?",
            "outcomes": ["Yes", "No"],
            "createdAt": "2026-03-20T00:00:00Z",
            "endDateIso": "2026-04-01T23:59:59Z",
            "active": True,
            "closed": False,
        }
    )


class TestPolymarketLifecycle:
    def test_btc_hourly_routes_to_canonical_group(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = _polymarket_btc_hourly_market()

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.market_id == "0xabc123"
        assert lifecycle.venue == "POLYMARKET"
        assert lifecycle.canonical_group == CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY
        assert lifecycle.market_created_at == datetime(2026, 3, 25, 9, 0, tzinfo=UTC)
        assert lifecycle.resolution_time == datetime(2026, 3, 26, 13, 0, tzinfo=UTC)
        # Settlement = resolution + canonical group's settlement_lag.
        # BTC_UP_DOWN_HOURLY ships +2h per UAC CANONICAL_GROUP_METADATA.
        expected_lag = CANONICAL_GROUP_METADATA[CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY].settlement_lag
        assert lifecycle.settlement_time == lifecycle.resolution_time + expected_lag
        assert expected_lag == timedelta(hours=2)
        assert lifecycle.current_status == "active"

    def test_unclassifiable_market_falls_through_to_other(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = _polymarket_unclassifiable_market()

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.canonical_group == CanonicalQuestionGroup.OTHER
        # OTHER's settlement_lag = 24h per UAC CANONICAL_GROUP_METADATA.
        other_lag = CANONICAL_GROUP_METADATA[CanonicalQuestionGroup.OTHER].settlement_lag
        assert lifecycle.settlement_time == lifecycle.resolution_time + other_lag

    def test_missing_condition_id_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "marketSlug": "no-condition-id",
                "question": "Will something happen?",
                "createdAt": "2026-03-20T00:00:00Z",
                "endDateIso": "2026-04-01T23:59:59Z",
            }
        )

        assert adapter.classify_lifecycle(market) is None

    def test_missing_created_at_returns_none(self) -> None:
        """No ``createdAt`` AND no ``startDate`` fallback → no lifecycle row.

        The lifecycle SSOT requires market_created_at — without it the
        cluster gate can't reason about per-day overlap, and ML
        compute can't enforce the per-market `LookaheadBiasError`.
        """
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xnone",
                "marketSlug": "no-created-at",
                "question": "Will Bitcoin be up?",
                "endDateIso": "2026-04-01T23:59:59Z",
            }
        )

        assert adapter.classify_lifecycle(market) is None

    def test_closed_market_status_settled_or_resolved(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xclosed",
                "marketSlug": "btc-closed-hourly",
                "question": "Will Bitcoin be up or down at 9am ET?",
                "outcomes": ["Up", "Down"],
                "createdAt": "2024-01-01T00:00:00Z",
                "closedTime": "2024-01-02T00:00:00Z",
                "endDateIso": "2024-01-02T00:00:00Z",
                "closed": True,
                "active": False,
            }
        )

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        # Long-resolved 2024 market: settlement_time has elapsed → settled.
        assert lifecycle.current_status == "settled"

    def test_get_market_lifecycles_returns_per_market_rows(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        adapter._last_markets = [  # pyright: ignore[reportPrivateUsage]
            _polymarket_btc_hourly_market(),
            _polymarket_unclassifiable_market(),
        ]

        lifecycles = adapter.get_market_lifecycles()

        assert len(lifecycles) == 2
        groups = {lc.canonical_group for lc in lifecycles}
        assert groups == {
            CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY,
            CanonicalQuestionGroup.OTHER,
        }

    def test_get_market_lifecycles_skips_unclassifiable_lifecycle_rows(self) -> None:
        """Markets missing condition_id / created_at should be silently
        dropped from the lifecycle output — they're already excluded
        from the InstrumentRecord shard for the same reasons.
        """
        adapter = PolymarketReferenceDataAdapter()
        adapter._last_markets = [  # pyright: ignore[reportPrivateUsage]
            _polymarket_btc_hourly_market(),
            PolymarketGammaMarket.model_validate(
                {
                    "marketSlug": "no-condition-id",
                    "question": "Will something happen?",
                    "createdAt": "2026-03-20T00:00:00Z",
                    "endDateIso": "2026-04-01T23:59:59Z",
                }
            ),
        ]

        lifecycles = adapter.get_market_lifecycles()

        assert len(lifecycles) == 1
        assert lifecycles[0].canonical_group == CanonicalQuestionGroup.BTC_UP_DOWN_HOURLY


class TestKalshiLifecycle:
    @staticmethod
    def _kalshi_market(**overrides: object) -> KalshiMarket:
        base: dict[str, object] = {
            "ticker": "KXBTC-26MAR-90000",
            "event_ticker": "KXBTC",
            "title": "BTC above $90,000?",
            "status": "active",
            "open_time": "2026-03-25T00:00:00Z",
            "close_time": "2026-03-26T00:00:00Z",
            "expiration_time": "2026-03-26T23:59:59Z",
        }
        base.update(overrides)
        return KalshiMarket.model_validate(base)

    def test_active_market_lifecycle_shape(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        market = self._kalshi_market()

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.market_id == "KXBTC-26MAR-90000"
        assert lifecycle.venue == "kalshi"
        # Kalshi rule classifier is override-only currently → unrecognised
        # tickers go to OTHER per UAC SSOT.
        assert lifecycle.canonical_group == CanonicalQuestionGroup.OTHER
        assert lifecycle.market_created_at == datetime(2026, 3, 25, 0, 0, tzinfo=UTC)
        assert lifecycle.resolution_time == datetime(2026, 3, 26, 0, 0, tzinfo=UTC)
        other_lag = CANONICAL_GROUP_METADATA[CanonicalQuestionGroup.OTHER].settlement_lag
        assert lifecycle.settlement_time == lifecycle.resolution_time + other_lag
        assert lifecycle.current_status == "active"

    def test_open_time_fallback_when_missing(self) -> None:
        """No ``open_time`` → fallback to close_time - 30d so the
        lifecycle window is a conservative super-set; better than
        dropping the row entirely.
        """
        adapter = KalshiReferenceDataAdapter()
        market = self._kalshi_market(open_time=None)

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.market_created_at == lifecycle.resolution_time - timedelta(days=30)

    def test_status_finalized_routes_to_settled(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        market = self._kalshi_market(status="finalized")

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.current_status == "settled"

    def test_status_closed_routes_to_resolved(self) -> None:
        """Closed (trading stopped, outcome pending) is in the
        ``resolved`` window — payouts haven't cleared yet so ML
        compute can't safely consume the post-close mid as a feature.
        """
        adapter = KalshiReferenceDataAdapter()
        market = self._kalshi_market(status="closed")

        lifecycle = adapter.classify_lifecycle(market)

        assert lifecycle is not None
        assert lifecycle.current_status == "resolved"

    def test_missing_close_time_returns_none(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        market = self._kalshi_market(close_time=None)

        assert adapter.classify_lifecycle(market) is None

    def test_missing_ticker_returns_none(self) -> None:
        market = KalshiMarket.model_validate({"event_ticker": "KXBTC"})

        assert KalshiReferenceDataAdapter().classify_lifecycle(market) is None

    def test_get_market_lifecycles_skips_unclassifiable(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        adapter._last_markets = [  # pyright: ignore[reportPrivateUsage]
            self._kalshi_market(),
            self._kalshi_market(close_time=None, ticker="KXNOCLOSE"),
        ]

        lifecycles = adapter.get_market_lifecycles()

        assert len(lifecycles) == 1
        assert lifecycles[0].market_id == "KXBTC-26MAR-90000"
