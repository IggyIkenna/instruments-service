"""Prediction market cross-venue resolver — finds same event across Polymarket, Kalshi, Odds API.

Mirrors TeamAliasResolver/PlayerAliasResolver pattern. Indexes PredictionMarketCrossVenueMapping
objects for O(1) lookup by any venue-specific ID.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from unified_api_contracts.canonical.domain.prediction import (  # noqa: qg-deep-import
    PredictionMarketCrossVenueMapping,
)

logger = logging.getLogger(__name__)


class PredictionMarketResolver:
    """Resolves prediction market events across venues.

    Indexes by canonical_event_id, polymarket_condition_id, kalshi_event_ticker,
    kalshi_market_ticker, odds_api_event_id, and api_football_fixture_id.
    """

    def __init__(self, mappings: Sequence[PredictionMarketCrossVenueMapping]) -> None:
        self._by_canonical: dict[str, PredictionMarketCrossVenueMapping] = {}
        self._by_polymarket: dict[str, str] = {}
        self._by_kalshi_event: dict[str, str] = {}
        self._by_kalshi_market: dict[str, str] = {}
        self._by_odds_api: dict[str, str] = {}
        self._by_fixture: dict[int, str] = {}
        self._by_underlying_strike: dict[str, str] = {}

        for m in mappings:
            self._by_canonical[m.canonical_event_id] = m
            if m.polymarket_condition_id is not None:
                self._by_polymarket[m.polymarket_condition_id] = m.canonical_event_id
            if m.kalshi_event_ticker is not None:
                self._by_kalshi_event[m.kalshi_event_ticker] = m.canonical_event_id
            if m.kalshi_market_ticker is not None:
                self._by_kalshi_market[m.kalshi_market_ticker] = m.canonical_event_id
            if m.odds_api_event_id is not None:
                self._by_odds_api[m.odds_api_event_id] = m.canonical_event_id
            if m.api_football_fixture_id is not None:
                self._by_fixture[m.api_football_fixture_id] = m.canonical_event_id
            if m.underlying is not None and m.strike is not None and m.expiry_utc is not None:
                key = f"{m.underlying}:{m.strike}:{m.expiry_utc.isoformat()}"
                self._by_underlying_strike[key] = m.canonical_event_id

    def find_by_polymarket_id(self, condition_id: str) -> PredictionMarketCrossVenueMapping | None:
        """Look up by Polymarket condition_id."""
        canonical = self._by_polymarket.get(condition_id)
        return self._by_canonical.get(canonical) if canonical else None

    def find_by_kalshi_ticker(self, ticker: str) -> PredictionMarketCrossVenueMapping | None:
        """Look up by Kalshi event or market ticker."""
        canonical = self._by_kalshi_event.get(ticker) or self._by_kalshi_market.get(ticker)
        return self._by_canonical.get(canonical) if canonical else None

    def find_by_odds_api_event(self, event_id: str) -> PredictionMarketCrossVenueMapping | None:
        """Look up by Odds API event ID (sports only)."""
        canonical = self._by_odds_api.get(event_id)
        return self._by_canonical.get(canonical) if canonical else None

    def find_by_fixture_id(self, fixture_id: int) -> PredictionMarketCrossVenueMapping | None:
        """Look up by API-Football fixture ID (sports only)."""
        canonical = self._by_fixture.get(fixture_id)
        return self._by_canonical.get(canonical) if canonical else None

    def find_by_underlying_strike_expiry(
        self, underlying: str, strike: float, expiry_iso: str
    ) -> PredictionMarketCrossVenueMapping | None:
        """Look up crypto/macro markets by underlying + strike + expiry."""
        key = f"{underlying}:{strike}:{expiry_iso}"
        canonical = self._by_underlying_strike.get(key)
        return self._by_canonical.get(canonical) if canonical else None

    def get_mapping(self, canonical_event_id: str) -> PredictionMarketCrossVenueMapping | None:
        """Look up by canonical event ID."""
        return self._by_canonical.get(canonical_event_id)

    @property
    def mapping_count(self) -> int:
        """Number of unique canonical events indexed."""
        return len(self._by_canonical)

    @property
    def sports_count(self) -> int:
        """Number of sports events (have fixture_id)."""
        return len(self._by_fixture)

    @property
    def crypto_macro_count(self) -> int:
        """Number of crypto/macro events (have underlying + strike)."""
        return len(self._by_underlying_strike)


def load_cross_venue_mappings_from_dict(
    data: list[dict[str, object]],
) -> list[PredictionMarketCrossVenueMapping]:
    """Construct mapping list from plain dicts (for testing)."""
    return [PredictionMarketCrossVenueMapping.model_validate(d) for d in data]
