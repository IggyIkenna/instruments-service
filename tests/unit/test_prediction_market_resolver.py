"""Unit tests for prediction_market_resolver.py."""

from __future__ import annotations

from instruments_service.sports.prediction_market_resolver import (
    PredictionMarketResolver,
    load_cross_venue_mappings_from_dict,
)


def _sample_mappings() -> list[dict[str, object]]:
    return [
        {
            "canonical_event_id": "EPL:ARS-v-CHE:20260322",
            "category": "sports",
            "sub_category": "epl",
            "underlying": None,
            "odds_api_event_id": "odds_1034567",
            "api_football_fixture_id": 1034567,
            "polymarket_condition_id": "0xabc123",
            "polymarket_neg_risk_market_id": None,
            "kalshi_event_ticker": "SOCCER-EPL-ARS-CHE-22MAR26",
            "kalshi_market_ticker": "SOCCER-EPL-ARS-CHE-22MAR26-W1",
            "timeframe": None,
            "strike": None,
            "expiry_utc": None,
        },
        {
            "canonical_event_id": "BTC:ABOVE:95000:20260321T1400Z",
            "category": "crypto",
            "sub_category": "btc_price",
            "underlying": "BTC",
            "odds_api_event_id": None,
            "api_football_fixture_id": None,
            "polymarket_condition_id": "0xdef456",
            "polymarket_neg_risk_market_id": "0xneg789",
            "kalshi_event_ticker": None,
            "kalshi_market_ticker": "KXBTC-21MAR26-T95000",
            "timeframe": "1h",
            "strike": 95000.0,
            "expiry_utc": "2026-03-21T14:00:00+00:00",
        },
        {
            "canonical_event_id": "SPX:ABOVE:5800:20260321",
            "category": "financial",
            "sub_category": "spx_close",
            "underlying": "SPX",
            "odds_api_event_id": None,
            "api_football_fixture_id": None,
            "polymarket_condition_id": "0xghi012",
            "polymarket_neg_risk_market_id": None,
            "kalshi_event_ticker": None,
            "kalshi_market_ticker": "KXSPY-21MAR26-T5800",
            "timeframe": "1d",
            "strike": 5800.0,
            "expiry_utc": "2026-03-21T21:00:00+00:00",
        },
    ]


class TestPredictionMarketResolver:
    def test_find_by_polymarket_id(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        result = resolver.find_by_polymarket_id("0xabc123")
        assert result is not None
        assert result.canonical_event_id == "EPL:ARS-v-CHE:20260322"
        assert result.category == "sports"

    def test_find_by_kalshi_ticker(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        # Event ticker
        result = resolver.find_by_kalshi_ticker("SOCCER-EPL-ARS-CHE-22MAR26")
        assert result is not None
        assert result.canonical_event_id == "EPL:ARS-v-CHE:20260322"
        # Market ticker (crypto)
        result = resolver.find_by_kalshi_ticker("KXBTC-21MAR26-T95000")
        assert result is not None
        assert result.canonical_event_id == "BTC:ABOVE:95000:20260321T1400Z"

    def test_find_by_odds_api_event(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        result = resolver.find_by_odds_api_event("odds_1034567")
        assert result is not None
        assert result.canonical_event_id == "EPL:ARS-v-CHE:20260322"

    def test_find_by_fixture_id(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        result = resolver.find_by_fixture_id(1034567)
        assert result is not None
        assert result.category == "sports"

    def test_find_by_underlying_strike_expiry(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        result = resolver.find_by_underlying_strike_expiry("BTC", 95000.0, "2026-03-21T14:00:00+00:00")
        assert result is not None
        assert result.canonical_event_id == "BTC:ABOVE:95000:20260321T1400Z"

    def test_find_none_for_missing(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        assert resolver.find_by_polymarket_id("0xnonexistent") is None
        assert resolver.find_by_kalshi_ticker("UNKNOWN-TICKER") is None
        assert resolver.find_by_fixture_id(99999) is None

    def test_mapping_count(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        resolver = PredictionMarketResolver(mappings)
        assert resolver.mapping_count == 3
        assert resolver.sports_count == 1
        assert resolver.crypto_macro_count == 2

    def test_load_from_dict(self) -> None:
        mappings = load_cross_venue_mappings_from_dict(_sample_mappings())
        assert len(mappings) == 3
        assert mappings[0].canonical_event_id == "EPL:ARS-v-CHE:20260322"
