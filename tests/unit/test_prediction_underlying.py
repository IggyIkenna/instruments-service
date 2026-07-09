"""Unit tests for ``InstrumentRecord.underlying`` / ``.canonical_instrument_id``
population on the Prediction adapters.

Per ``unified-trading-pm/plans/active/prediction_canonical_identity_migration_2026_07_08.md``
todos 1 + 5 (see ``instruments-service/docs/PREDICTION_INSTRUMENTS.md`` §
"Canonical identity model"): no adapter previously called
``InstrumentRecord(underlying=...)`` at all. Both ``_parse_market()`` methods now
reuse the SAME ``classify_*_to_canonical_group()`` -> ``underlying_for_group()``
pipeline that already drove ``MarketLifecycle.canonical_group`` (no new
classification logic), applying the existing
``cross_venue_mapping.py::_build_mapping()`` convention: sports fixtures get
``underlying=None`` (no single scalar underlying, not "unclassified"); every
other market gets a real ``PredictionUnderlying`` value — a NAMED subject (BTC,
CPI, FED, TRUMP, …) for a classified market, or the honest
``PredictionUnderlying.OTHER.value`` catch-all for a genuinely-unclassified one.

Polymarket sports markets additionally get ``canonical_instrument_id`` populated
with a Sports-asset-group-aligned fixture_id (``build_fixture_id()`` — todo 5),
giving a real EPL/MLB/etc. Polymarket market byte-identical identity to the
Sports asset group's own fixture catalogue for the SAME real game.
"""

from __future__ import annotations

from datetime import UTC, datetime

from unified_api_contracts import PolymarketGammaMarket

from instruments_service.reference_data.adapters.prediction.kalshi import (
    KalshiReferenceDataAdapter,
)
from instruments_service.reference_data.adapters.prediction.polymarket import (
    PolymarketReferenceDataAdapter,
)

_NOW = datetime(2026, 6, 24, tzinfo=UTC)


class TestPolymarketUnderlying:
    def test_btc_crypto_market_gets_btc_underlying(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xbtc001",
                "marketSlug": "bitcoin-up-or-down-june-24-2026",
                "question": "Will Bitcoin be up or down on June 24, 2026?",
                "outcomes": ["Up", "Down"],
                "createdAt": "2026-06-23T00:00:00Z",
                "closedTime": "2026-06-24T00:00:00Z",
                "endDateIso": "2026-06-24T00:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            }
        )
        record = adapter._parse_market(market, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "BTC"
        assert record.canonical_instrument_id is None  # non-sports: not populated at adapter time

    def test_cpi_macro_market_gets_cpi_underlying(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xcpi001",
                "marketSlug": "cpi-inflation-above-3-percent-june-2026",
                "question": "Will June 2026 CPI inflation be above 3%?",
                "outcomes": ["Yes", "No"],
                "createdAt": "2026-06-01T00:00:00Z",
                "closedTime": "2026-07-11T00:00:00Z",
                "endDateIso": "2026-07-11T00:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            }
        )
        record = adapter._parse_market(market, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "CPI"

    def test_politics_market_with_named_underlying(self) -> None:
        """A classified politics market (Trump) gets its OWN named underlying —
        NOT a blanket OTHER for the whole politics category."""
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xtrump001",
                "marketSlug": "trump-approval-rating-above-45-june-2026",
                "question": "Will Trump approval rating be above 45% in June 2026?",
                "outcomes": ["Yes", "No"],
                "createdAt": "2026-06-01T00:00:00Z",
                "closedTime": "2026-06-30T00:00:00Z",
                "endDateIso": "2026-06-30T00:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            }
        )
        record = adapter._parse_market(market, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "TRUMP"

    def test_genuinely_unclassified_market_falls_to_other(self) -> None:
        """A market the classifier can't route (cqg=MISC_NOVELTY) honestly gets
        PredictionUnderlying.OTHER — the real catch-all, not a fabricated guess."""
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xmisc001",
                "marketSlug": "will-something-strange-happen",
                "question": "Will something strange happen this week?",
                "outcomes": ["Yes", "No"],
                "createdAt": "2026-03-20T00:00:00Z",
                "closedTime": "2026-04-01T23:59:59Z",
                "endDateIso": "2026-04-01T23:59:59Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            }
        )
        record = adapter._parse_market(market, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "OTHER"

    def test_sports_market_gets_none_underlying_and_fixture_id(self) -> None:
        """Sports fixtures don't have a single scalar underlying (None, "not
        applicable" — distinct from OTHER's "genuinely unclassified"), but DO get
        a Sports-asset-group-aligned canonical_instrument_id (todo 5): the SAME
        build_fixture_id(league_id, build_team_id(home), build_team_id(away),
        date) pipeline build_sports_fixture_team_player_catalogue() uses for the
        Sports asset group's own fixture rows.
        """
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xsports001",
                "marketSlug": "epl-arsenal-vs-chelsea-2026-03-22",
                "question": "Will Arsenal beat Chelsea?",
                "outcomes": ["Arsenal", "Chelsea"],
                "createdAt": "2026-03-01T00:00:00Z",
                "closedTime": "2026-03-22T18:00:00Z",
                "endDateIso": "2026-03-22T18:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "event_title": "Arsenal vs. Chelsea",
                "series_slug": "premier-league-2025",
                "sports_market_type": "moneyline",
            }
        )
        record = adapter._parse_market(market, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying is None
        assert record.canonical_instrument_id == "EPL:CHELSEA_v_ARSENAL:20260322"


class TestKalshiUnderlying:
    def test_btc_crypto_market_gets_btc_underlying(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXBTCD-26JUN24-T95000",
            "event_ticker": "KXBTCD-26JUN24",
            "series_ticker": "KXBTCD",
            "title": "BTC above $95,000?",
            "status": "active",
            "open_time": "2026-06-23T00:00:00Z",
            "close_time": "2026-06-24T00:00:00Z",
        }
        record = adapter._parse_market(raw, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "BTC"

    def test_cpi_macro_market_gets_cpi_underlying(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXCPIYOY-26JUL-T3",
            "event_ticker": "KXCPIYOY-26JUL",
            "series_ticker": "KXCPIYOY",
            "title": "June CPI YoY above 3%?",
            "status": "active",
            "open_time": "2026-06-01T00:00:00Z",
            "close_time": "2026-07-11T00:00:00Z",
        }
        record = adapter._parse_market(raw, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "CPI"

    def test_fed_market_gets_fed_underlying(self) -> None:
        """Real macro/politics-adjacent example: Fed rate decisions get a NAMED
        underlying (FED), not a generic politics OTHER bucket."""
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXFEDDECISION-26JUL-C",
            "event_ticker": "KXFEDDECISION-26JUL",
            "series_ticker": "KXFEDDECISION",
            "title": "Fed cuts rates in July?",
            "status": "active",
            "open_time": "2026-06-01T00:00:00Z",
            "close_time": "2026-07-30T00:00:00Z",
        }
        record = adapter._parse_market(raw, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "FED"

    def test_sports_market_gets_none_underlying(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXMLBGAME-26JUN261910SEACLE",
            "event_ticker": "KXMLBGAME-26JUN261910SEACLE",
            "series_ticker": "KXMLBGAME",
            "title": "Seattle vs Cleveland",
            "status": "active",
            "open_time": "2026-06-26T00:00:00Z",
            "close_time": "2026-06-26T23:00:00Z",
        }
        record = adapter._parse_market(raw, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying is None

    def test_genuinely_unclassified_market_falls_to_other(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXWEIRDTHING-26JUL",
            "event_ticker": "KXWEIRDTHING-26JUL",
            "series_ticker": "KXWEIRDTHING",
            "title": "Something weird happens?",
            "status": "active",
            "open_time": "2026-06-01T00:00:00Z",
            "close_time": "2026-07-01T00:00:00Z",
        }
        record = adapter._parse_market(raw, _NOW)  # pyright: ignore[reportPrivateUsage]
        assert record is not None
        assert record.underlying == "OTHER"


class TestClassifyLifecycleGroupReuse:
    """``classify_lifecycle(market, group=...)`` reuses a precomputed group
    instead of reclassifying (todo 1's explicit "reuse, don't reclassify")."""

    def test_polymarket_precomputed_group_is_reused(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xreuse001",
                "marketSlug": "bitcoin-up-or-down-june-24-2026",
                "question": "Will Bitcoin be up or down on June 24, 2026?",
                "outcomes": ["Up", "Down"],
                "createdAt": "2026-06-23T00:00:00Z",
                "closedTime": "2026-06-24T00:00:00Z",
                "endDateIso": "2026-06-24T00:00:00Z",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            }
        )
        from unified_api_contracts.predictions import CanonicalQuestionGroup

        # Force an intentionally WRONG precomputed group to prove it's honoured
        # verbatim rather than recomputed from the market's real slug/question.
        lifecycle = adapter.classify_lifecycle(market, group=CanonicalQuestionGroup.CPI_PRINT_PER_MONTH)
        assert lifecycle is not None
        assert lifecycle.canonical_group == CanonicalQuestionGroup.CPI_PRINT_PER_MONTH

        # Default (no group passed) still self-classifies for backward compatibility.
        lifecycle_default = adapter.classify_lifecycle(market)
        assert lifecycle_default is not None
        assert lifecycle_default.canonical_group == CanonicalQuestionGroup.BTC_UP_DOWN_DAILY

    def test_kalshi_precomputed_group_is_reused(self) -> None:
        from unified_api_contracts import KalshiMarket
        from unified_api_contracts.predictions import CanonicalQuestionGroup

        adapter = KalshiReferenceDataAdapter()
        market = KalshiMarket.model_validate(
            {
                "ticker": "KXBTCD-26JUN24-T95000",
                "event_ticker": "KXBTCD-26JUN24",
                "series_ticker": "KXBTCD",
                "title": "BTC above $95,000?",
                "status": "active",
                "open_time": "2026-06-23T00:00:00Z",
                "close_time": "2026-06-24T00:00:00Z",
            }
        )
        lifecycle = adapter.classify_lifecycle(market, group=CanonicalQuestionGroup.CPI_PRINT_PER_MONTH)
        assert lifecycle is not None
        assert lifecycle.canonical_group == CanonicalQuestionGroup.CPI_PRINT_PER_MONTH

        lifecycle_default = adapter.classify_lifecycle(market)
        assert lifecycle_default is not None
        assert lifecycle_default.canonical_group == CanonicalQuestionGroup.BTC_UP_DOWN_DAILY
