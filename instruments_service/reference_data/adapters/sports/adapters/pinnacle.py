"""Pinnacle adapter — sharp odds from Pinnacle (the sharpest bookmaker).

Auth: service must fetch pinnacle-api-key from Secret Manager and pass
it via the ``api_key`` constructor parameter. Uses Basic Auth (base64).
Base URL: https://api.pinnacle.com
Ref: https://pinnacleapi.github.io
"""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts.external.pinnacle import (  # noqa: qg-deep-import
    PinnacleEvent,
    PinnacleOddsEvent,
    PinnacleOddsResponse,
    PinnaclePeriod,
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

_BASE_URL: str = BASE_URLS["pinnacle"]

# Pinnacle sport IDs
_SPORT_ID_SOCCER: int = 29


class PinnacleAdapter(BaseSportsReferenceAdapter):
    """Pinnacle sports reference data adapter.

    Fetches sharp odds from Pinnacle REST API and normalizes them
    to canonical types. Pinnacle is considered the sharpest bookmaker
    and its odds serve as market consensus benchmarks.

    Secret Manager key: ``pinnacle-api-key``
    (format: ``username:password`` for Basic Auth)
    """

    @property
    def venue(self) -> str:
        return "pinnacle"

    def _headers(self) -> dict[str, str]:
        """Build request headers with Basic Auth.

        Pinnacle API uses HTTP Basic Authentication.
        The api_key should be in the format ``username:password``.
        """
        if not self._api_key:
            raise ValueError(
                "Pinnacle adapter requires an API key. "
                "Service must fetch 'pinnacle-api-key' from Secret Manager "
                "and pass it via api_key parameter (format: 'username:password')."
            )
        encoded = base64.b64encode(self._api_key.encode("utf-8")).decode("utf-8")
        return {
            "Authorization": f"Basic {encoded}",
            "Accept": "application/json",
        }

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Fetch sharp odds from Pinnacle.

        Args:
            sport: Sport ID as string or Pinnacle sport key (e.g. '29' for soccer).
            regions: Not used by Pinnacle (global odds). Kept for interface compat.
            markets: Not used by Pinnacle (returns all markets). Kept for interface compat.

        Returns:
            List of canonical odds from Pinnacle.
        """
        sport_id = _parse_sport_id(sport)

        events = await self._fetch_events(sport_id)
        if not events:
            return []

        odds_response = await self._fetch_odds(sport_id)
        if odds_response is None:
            return []

        event_map = _build_event_map(events)
        odds = _collect_league_odds(odds_response, event_map)

        logger.info("Fetched %d odds from Pinnacle for sport_id=%d", len(odds), sport_id)
        return odds

    async def _fetch_events(self, sport_id: int) -> list[PinnacleEvent]:
        """Fetch events/fixtures for a sport from Pinnacle."""
        url = f"{_BASE_URL}/v1/fixtures"
        params: dict[str, str] = {"sportId": str(sport_id)}

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            return []

        events: list[PinnacleEvent] = []
        league_list = _extract_leagues(raw_response)
        for league_data in league_list:
            event_list = league_data.get("events")
            if not isinstance(event_list, list):
                continue
            for evt_data in event_list:
                if not isinstance(evt_data, dict):
                    continue
                try:
                    events.append(PinnacleEvent.model_validate(evt_data))
                except Exception as exc:
                    logger.warning("Failed to parse Pinnacle event: %s", exc)
                    continue

        return events

    async def _fetch_odds(self, sport_id: int) -> PinnacleOddsResponse | None:
        """Fetch odds for a sport from Pinnacle."""
        url = f"{_BASE_URL}/v1/odds"
        params: dict[str, str] = {
            "sportId": str(sport_id),
            "oddsFormat": "DECIMAL",
        }

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            return None

        if not isinstance(raw_response, dict):
            return None

        try:
            return PinnacleOddsResponse.model_validate(raw_response)
        except Exception as exc:
            logger.warning("Failed to validate Pinnacle odds response: %s", exc)
            return None

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Pinnacle does not provide detailed fixture data.

        Use ApiFootballAdapter for fixtures. This returns an empty list.
        """
        logger.info("get_fixtures not supported on Pinnacle adapter — use ApiFootballAdapter")
        return []

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch available leagues from Pinnacle.

        Returns:
            List of canonical leagues.
        """
        url = f"{_BASE_URL}/v2/leagues"
        params: dict[str, str] = {"sportId": str(_SPORT_ID_SOCCER)}

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        league_list = _extract_league_list(raw_response)
        for item in league_list:
            if not isinstance(item, dict):
                continue
            try:
                leagues.append(
                    CanonicalLeague(
                        league_id=str(item.get("id", "")),
                        name=str(item.get("name", "")),
                        country=str(item.get("homeCountry", "")) if item.get("homeCountry") else "",
                        league_type=None,
                        logo_url=None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse Pinnacle league: %s", exc)
                continue

        logger.info("Fetched %d leagues from Pinnacle", len(leagues))
        return leagues

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Pinnacle does not provide team reference data.

        Use ApiFootballAdapter for teams. This returns an empty list.
        """
        logger.info("get_teams not supported on Pinnacle adapter — use ApiFootballAdapter")
        return []


def _parse_sport_id(sport: str) -> int:
    """Parse sport string to Pinnacle sport ID, defaulting to soccer."""
    try:
        return int(sport)
    except ValueError:
        return _SPORT_ID_SOCCER


def _build_event_map(events: list[PinnacleEvent]) -> dict[int, PinnacleEvent]:
    """Build a lookup dict from event ID to event for name resolution."""
    return {evt.id: evt for evt in events if evt.id is not None}


def _collect_league_odds(
    odds_response: PinnacleOddsResponse,
    event_map: dict[int, PinnacleEvent],
) -> list[CanonicalOdds]:
    """Collect and normalize odds from all leagues in a Pinnacle odds response."""
    odds: list[CanonicalOdds] = []
    for league in odds_response.leagues:
        for odds_event in league.events:
            event_info = event_map.get(odds_event.id)
            event_odds = _normalize_pinnacle_odds_event(odds_event, event_info)
            odds.extend(event_odds)
    return odds


def _extract_leagues(raw: object) -> list[dict[str, object]]:
    """Extract leagues list from a Pinnacle fixtures/odds response."""
    if isinstance(raw, dict):
        leagues = raw.get("league") or raw.get("leagues")
        if isinstance(leagues, list):
            return [item for item in leagues if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _extract_league_list(raw: object) -> list[dict[str, object]]:
    """Extract league list from Pinnacle leagues response."""
    if isinstance(raw, dict):
        leagues = raw.get("leagues")
        if isinstance(leagues, list):
            return [item for item in leagues if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _make_pinnacle_odds(
    event_id: str,
    market_id: str,
    selection_id: str,
    selection_name: str,
    price: Decimal,
    now: datetime,
    event_name: str,
) -> CanonicalOdds:
    """Build a single CanonicalOdds from Pinnacle fields."""
    return CanonicalOdds(
        venue="pinnacle",
        event_id=event_id,
        market_id=market_id,
        selection_id=selection_id,
        selection_name=selection_name,
        decimal_odds=price,
        timestamp=now,
        is_back=True,
        available_size=None,
        event_name=event_name,
        sport=None,
        competition=None,
    )


def _pinnacle_moneyline_odds(
    period: PinnaclePeriod,
    event_id: str,
    period_label: str,
    now: datetime,
    event_name: str,
) -> list[CanonicalOdds]:
    """Extract moneyline odds from a Pinnacle period."""
    if period.moneyline is None:
        return []
    ml = period.moneyline
    return [
        _make_pinnacle_odds(
            event_id,
            f"{event_id}:moneyline:{period_label}",
            f"{event_id}:moneyline:{period_label}:{side}",
            f"{side} ({period_label})",
            Decimal(str(price)),
            now,
            event_name,
        )
        for side, price in [("home", ml.home), ("draw", ml.draw), ("away", ml.away)]
        if price > 0
    ]


def _pinnacle_totals_odds(
    period: PinnaclePeriod,
    event_id: str,
    period_label: str,
    now: datetime,
    event_name: str,
) -> list[CanonicalOdds]:
    """Extract totals (over/under) odds from a Pinnacle period."""
    odds: list[CanonicalOdds] = []
    for total in period.totals:
        base_market = f"{event_id}:totals:{period_label}:{total.points}"
        if total.over > 0:
            odds.append(
                _make_pinnacle_odds(
                    event_id,
                    base_market,
                    f"{base_market}:over",
                    f"Over {total.points} ({period_label})",
                    Decimal(str(total.over)),
                    now,
                    event_name,
                )
            )
        if total.under > 0:
            odds.append(
                _make_pinnacle_odds(
                    event_id,
                    base_market,
                    f"{base_market}:under",
                    f"Under {total.points} ({period_label})",
                    Decimal(str(total.under)),
                    now,
                    event_name,
                )
            )
    return odds


def _pinnacle_spread_odds(
    period: PinnaclePeriod,
    event_id: str,
    period_label: str,
    now: datetime,
    event_name: str,
) -> list[CanonicalOdds]:
    """Extract spread (handicap) odds from a Pinnacle period."""
    odds: list[CanonicalOdds] = []
    for spread in period.spreads:
        base_market = f"{event_id}:spread:{period_label}:{spread.hdp}"
        if spread.home > 0:
            odds.append(
                _make_pinnacle_odds(
                    event_id,
                    base_market,
                    f"{base_market}:home",
                    f"Home {spread.hdp:+g} ({period_label})",
                    Decimal(str(spread.home)),
                    now,
                    event_name,
                )
            )
        if spread.away > 0:
            odds.append(
                _make_pinnacle_odds(
                    event_id,
                    base_market,
                    f"{base_market}:away",
                    f"Away {-spread.hdp:+g} ({period_label})",
                    Decimal(str(spread.away)),
                    now,
                    event_name,
                )
            )
    return odds


def _normalize_pinnacle_odds_event(
    odds_event: PinnacleOddsEvent,
    event_info: PinnacleEvent | None,
) -> list[CanonicalOdds]:
    """Normalize a Pinnacle odds event to CanonicalOdds."""
    event_id = str(odds_event.id)
    home_team = event_info.home or "" if event_info is not None else ""
    away_team = event_info.away or "" if event_info is not None else ""
    event_name = f"{home_team} vs {away_team}" if home_team and away_team else ""
    now = datetime.now(UTC)

    odds: list[CanonicalOdds] = []
    for period in odds_event.periods:
        label = f"p{period.number}"
        odds.extend(_pinnacle_moneyline_odds(period, event_id, label, now, event_name))
        odds.extend(_pinnacle_totals_odds(period, event_id, label, now, event_name))
        odds.extend(_pinnacle_spread_odds(period, event_id, label, now, event_name))
    return odds
