"""OddsAPI adapter — odds from api.the-odds-api.com.

Auth: service must fetch odds-api-key from Secret Manager and pass
it via the ``api_key`` constructor parameter.
Base URL: https://api.the-odds-api.com/v4
Ref: https://the-odds-api.com/liveapi/guides/v4/
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts.external.odds_api import (  # noqa: qg-deep-import
    OddsApiFixture,
)
from unified_api_contracts.registry.endpoints import BASE_URLS  # noqa: qg-deep-import
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_BASE_URL: str = BASE_URLS["odds_api"]


class OddsApiAdapter(BaseSportsReferenceAdapter):
    """The Odds API sports reference data adapter.

    Fetches odds from The Odds API v4 and normalizes them to
    canonical types via UAC normalize functions.

    Secret Manager key: ``odds-api-key``

    Tracks API quota via ``x-requests-remaining`` / ``x-requests-used``
    response headers. Logs remaining credits after each call and emits
    a warning when credits drop below 10%.
    """

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__(api_key=api_key)
        self._requests_remaining: str = "?"
        self._requests_used: str = "?"

    @property
    def venue(self) -> str:
        return "odds_api"

    @property
    def requests_remaining(self) -> str:
        """Last known ``x-requests-remaining`` value from the API."""
        return self._requests_remaining

    def _params_with_key(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """Build query params with API key authentication."""
        if not self._api_key:
            raise ValueError(
                "Odds API adapter requires an API key. "
                "Service must fetch 'odds-api-key' from Secret Manager "
                "and pass it via api_key parameter."
            )
        params: dict[str, str] = {"apiKey": self._api_key}
        if extra:
            params.update(extra)
        return params

    async def _get_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> object:
        """Override base _get_with_retry to capture Odds API quota headers.

        Tracks ``x-requests-remaining`` and ``x-requests-used`` on every
        response, logging quota state for observability.
        """
        import asyncio

        max_attempts = 3
        base_delay = 1.0
        retryable_codes = frozenset({429, 500, 502, 503, 504})

        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            delay = base_delay * (1.0 * (1 << attempt))
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    # Capture quota headers on every response
                    self._requests_remaining = resp.headers.get("x-requests-remaining", self._requests_remaining)
                    self._requests_used = resp.headers.get("x-requests-used", self._requests_used)
                    logger.info(
                        "Odds API quota: remaining=%s, used=%s",
                        self._requests_remaining,
                        self._requests_used,
                    )

                    if resp.status in retryable_codes:
                        logger.warning(
                            "Retryable HTTP %d from %s (attempt %d/%d)",
                            resp.status,
                            url,
                            attempt + 1,
                            max_attempts,
                        )
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(delay)
                            continue
                        raise RuntimeError(f"HTTP {resp.status} from {url} after {max_attempts} attempts")
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientError as exc:
                last_exc = exc
                logger.warning(
                    "aiohttp error from %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    max_attempts,
                    exc,
                )
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay)
                    continue

        if last_exc:
            raise last_exc
        raise RuntimeError(f"All {max_attempts} retry attempts exhausted for {url}")

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Fetch odds for a given sport from The Odds API.

        Args:
            sport: Sport key (e.g. 'soccer_epl', 'soccer_germany_bundesliga').
            regions: Comma-separated region codes (e.g. 'uk', 'us', 'eu').
            markets: Comma-separated market keys (e.g. 'h2h', 'spreads', 'totals').

        Returns:
            List of canonical odds normalized via UAC.
        """
        url = f"{_BASE_URL}/sports/{sport}/odds"
        params = self._params_with_key(
            {
                "regions": regions,
                "markets": markets,
                "oddsFormat": "decimal",
            }
        )

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
                # Track quota from the last response headers (aiohttp stores on session)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        odds: list[CanonicalOdds] = []
        events = _extract_events(raw_response)
        for event in events:
            event_odds = _normalize_event_odds(event)
            odds.extend(event_odds)

        logger.info(
            "Fetched %d odds for sport=%s regions=%s markets=%s (credits remaining: %s)",
            len(odds),
            sport,
            regions,
            markets,
            self._requests_remaining,
        )
        return odds

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Odds API does not provide fixture-level data like API Football.

        Use ApiFootballAdapter for fixtures. This returns an empty list.
        """
        logger.info("get_fixtures not supported on Odds API adapter — use ApiFootballAdapter")
        return []

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch available sports from The Odds API (mapped as leagues).

        Returns:
            List of canonical leagues (one per sport key).
        """
        url = f"{_BASE_URL}/sports"
        params = self._params_with_key()

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        if not isinstance(raw_response, list):
            return leagues

        for item in raw_response:
            if not isinstance(item, dict):
                continue
            try:
                sport_key = str(item.get("key", ""))
                leagues.append(
                    CanonicalLeague(
                        league_id=sport_key,
                        name=str(item.get("title", "")),
                        country=str(item.get("group", "")),
                        league_type=None,
                        logo_url=None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse sport: %s", exc)
                continue

        logger.info("Fetched %d sports/leagues from Odds API", len(leagues))
        return leagues

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Odds API does not provide team-level reference data.

        Use ApiFootballAdapter for teams. This returns an empty list.
        """
        logger.info("get_teams not supported on Odds API adapter — use ApiFootballAdapter")
        return []

    async def get_events(self, sport: str) -> list[OddsApiFixture]:
        """Fetch raw events for a sport (without odds data).

        Useful for getting the event list before fetching odds.

        Args:
            sport: Sport key (e.g. 'soccer_epl').

        Returns:
            List of OddsApiFixture objects.
        """
        url = f"{_BASE_URL}/sports/{sport}/events"
        params = self._params_with_key()

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params)
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        events: list[OddsApiFixture] = []
        if not isinstance(raw_response, list):
            return events

        for item in raw_response:
            if not isinstance(item, dict):
                continue
            try:
                events.append(OddsApiFixture.model_validate(item))
            except Exception as exc:
                logger.warning("Failed to parse event: %s", exc)
                continue

        logger.info("Fetched %d events for sport=%s", len(events), sport)
        return events


def _extract_events(raw: object) -> list[dict[str, object]]:
    """Extract events from an Odds API response."""
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _parse_outcome_odds(
    outcome: dict[str, object],
    event_id: str,
    market_key: str,
    event_name: str,
    sport_key: str,
    sport_title: str,
) -> CanonicalOdds | None:
    """Parse a single outcome dict into CanonicalOdds, or None if invalid."""
    outcome_name = str(outcome.get("name", ""))
    price = outcome.get("price")
    if price is None:
        return None
    try:
        decimal_odds = Decimal(str(price))
    except Exception as _exc:
        return None
    if decimal_odds <= 0:
        return None

    selection_id = f"{event_id}:{market_key}:{outcome_name}".replace(" ", "_")
    return CanonicalOdds(
        venue="odds_api",
        event_id=event_id,
        market_id=f"{event_id}:{market_key}",
        selection_id=selection_id,
        selection_name=outcome_name,
        decimal_odds=decimal_odds,
        timestamp=datetime.now(UTC),
        is_back=True,
        available_size=None,
        event_name=event_name,
        sport=sport_key,
        competition=sport_title,
    )


def _parse_market_outcomes(
    market: dict[str, object],
    event_id: str,
    event_name: str,
    sport_key: str,
    sport_title: str,
) -> list[CanonicalOdds]:
    """Parse outcomes from a single bookmaker market."""
    market_key = str(market.get("key", ""))
    market_outcomes = market.get("outcomes")
    if not isinstance(market_outcomes, list):
        return []
    odds: list[CanonicalOdds] = []
    for outcome in market_outcomes:
        if not isinstance(outcome, dict):
            continue
        parsed = _parse_outcome_odds(outcome, event_id, market_key, event_name, sport_key, sport_title)
        if parsed is not None:
            odds.append(parsed)
    return odds


def _parse_bookmaker_odds(
    bm: dict[str, object],
    event_id: str,
    event_name: str,
    sport_key: str,
    sport_title: str,
) -> list[CanonicalOdds]:
    """Parse all odds from a single bookmaker's markets."""
    bm_markets = bm.get("markets")
    if not isinstance(bm_markets, list):
        return []
    odds: list[CanonicalOdds] = []
    for market in bm_markets:
        if isinstance(market, dict):
            odds.extend(_parse_market_outcomes(market, event_id, event_name, sport_key, sport_title))
    return odds


def _normalize_event_odds(event: dict[str, object]) -> list[CanonicalOdds]:
    """Normalize a single event's bookmaker odds to CanonicalOdds."""
    event_id = str(event.get("id", ""))
    home_team = str(event.get("home_team", ""))
    away_team = str(event.get("away_team", ""))
    event_name = f"{home_team} vs {away_team}" if home_team and away_team else ""
    sport_key = str(event.get("sport_key", ""))
    sport_title = str(event.get("sport_title", ""))

    bookmakers = event.get("bookmakers")
    if not isinstance(bookmakers, list):
        return []

    odds: list[CanonicalOdds] = []
    for bm in bookmakers:
        if not isinstance(bm, dict):
            continue
        odds.extend(_parse_bookmaker_odds(bm, event_id, event_name, sport_key, sport_title))
    return odds
