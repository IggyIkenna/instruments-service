"""Transfermarkt adapter — player values, transfer history, squad composition.

Auth: service must fetch transfermarkt-api-key from Secret Manager and pass
it via the ``api_key`` constructor parameter.

Supports TWO backends (auto-detected from key format):
  - **Apify** (key starts with ``apify_api_``): Runs Apify scraping actors.
    Ref: https://docs.apify.com/api/v2
  - **RapidAPI** (other keys): Uses RapidAPI Transfermarkt wrapper.
    Ref: https://rapidapi.com/transfermarkt/api/transfermarkt

NOTE: Transfermarkt has no official public API.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import aiohttp
from unified_api_contracts.external.transfermarkt import (  # noqa: qg-deep-import
    TransfermarktPlayer,
    TransfermarktTeamSquad,
)
from unified_api_contracts.external.transfermarkt.normalize import (  # noqa: qg-deep-import
    normalize_transfermarkt_team_from_squad,
)
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_RAPIDAPI_BASE_URL: str = "https://transfermarkt-api.p.rapidapi.com"
_APIFY_BASE_URL: str = "https://api.apify.com/v2"
_APIFY_ACTOR_ID: str = "jurso~transfermarkt-scraper"


class TransfermarktAdapter(BaseSportsReferenceAdapter):
    """Transfermarkt sports reference data adapter.

    Auto-detects backend from API key format:
    - ``apify_api_*`` → Apify actor scraping
    - Other → RapidAPI wrapper

    Secret Manager key: ``transfermarkt-api-key``
    """

    @property
    def venue(self) -> str:
        return "transfermarkt"

    @property
    def _is_apify(self) -> bool:
        return bool(self._api_key and self._api_key.startswith("apify_api_"))

    def _headers(self) -> dict[str, str]:
        """Build request headers based on backend."""
        if not self._api_key:
            raise ValueError(
                "Transfermarkt adapter requires an API key. "
                "Service must fetch 'transfermarkt-api-key' from Secret Manager "
                "and pass it via api_key parameter."
            )
        if self._is_apify:
            return {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
        return {
            "x-rapidapi-key": self._api_key,
            "x-rapidapi-host": "transfermarkt-api.p.rapidapi.com",
        }

    async def _run_apify_actor(self, club_url: str, timeout_secs: int = 120) -> list[dict[str, object]]:
        """Run Apify Transfermarkt scraper actor and return results."""
        run_url = f"{_APIFY_BASE_URL}/acts/{_APIFY_ACTOR_ID}/runs"
        actor_input = {
            "startUrls": [{"url": f"https://www.transfermarkt.com{club_url}"}],
            "proxy": {"useApifyProxy": True},
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                run_url,
                json=actor_input,
                headers=self._headers(),
                params={"waitForFinish": str(timeout_secs)},
            ) as resp:
                if resp.status != 201:
                    body = await resp.text()
                    raise RuntimeError(f"Apify actor start failed: HTTP {resp.status} — {body[:200]}")
                run_data = await resp.json()

            dataset_id = run_data.get("data", {}).get("defaultDatasetId")
            status = run_data.get("data", {}).get("status")
            if status != "SUCCEEDED":
                raise RuntimeError(f"Apify actor status: {status}")
            if not dataset_id:
                return []

            dataset_url = f"{_APIFY_BASE_URL}/datasets/{dataset_id}/items"
            async with session.get(dataset_url, headers=self._headers()) as resp2:
                if resp2.status != 200:
                    raise RuntimeError(f"Apify dataset fetch failed: HTTP {resp2.status}")
                items = await resp2.json()
                return items if isinstance(items, list) else []

    async def get_teams(self, league_id: int, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams with player value data for a league from Transfermarkt.

        Args:
            league_id: Transfermarkt competition ID.
            season: Optional season year. Defaults to current year.

        Returns:
            List of canonical teams with squad/value data.
        """
        effective_season = season if season is not None else datetime.now(UTC).year
        url = f"{_RAPIDAPI_BASE_URL}/clubs/search/{league_id}"
        params: dict[str, str] = {"season_id": str(effective_season)}

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, params=params, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        teams: list[CanonicalTeam] = []
        club_list = _extract_clubs(raw_response)
        for club in club_list:
            try:
                squad = _parse_squad(club)
                if squad is None:
                    continue
                canonical = normalize_transfermarkt_team_from_squad(squad)
                teams.append(canonical)
            except Exception as exc:
                logger.warning(
                    "Failed to normalize Transfermarkt team %s: %s",
                    club.get("id", "unknown") if isinstance(club, dict) else "unknown",
                    exc,
                )
                continue

        logger.info(
            "Fetched %d teams for league=%d season=%d",
            len(teams),
            league_id,
            effective_season,
        )
        return teams

    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Transfermarkt does not provide fixture/match data.

        Use ApiFootballAdapter for fixtures. This returns an empty list.
        """
        logger.info("get_fixtures not supported on Transfermarkt adapter — use ApiFootballAdapter")
        return []

    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch available competitions from Transfermarkt.

        Returns:
            List of canonical leagues.
        """
        url = f"{_RAPIDAPI_BASE_URL}/competitions/search/league"

        try:
            async with aiohttp.ClientSession() as session:
                raw_response = await self._get_with_retry(session, url, headers=self._headers())
        except Exception as exc:
            error_code = self._classify_error(exc)
            self._emit_fetch_failed(error_code, exc)
            raise

        leagues: list[CanonicalLeague] = []
        results = _extract_results(raw_response)
        for item in results:
            if not isinstance(item, dict):
                continue
            try:
                leagues.append(
                    CanonicalLeague(
                        league_id=str(item.get("id", "")),
                        name=str(item.get("name", "") or item.get("title", "")),
                        country=str(item.get("country", "")) if item.get("country") else "",
                        league_type=None,
                        logo_url=str(item.get("image", "")) if item.get("image") else None,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to parse Transfermarkt league: %s", exc)
                continue

        logger.info("Fetched %d leagues from Transfermarkt", len(leagues))
        return leagues

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Transfermarkt does not provide odds data.

        Use OddsApiAdapter for odds. This returns an empty list.
        """
        logger.info("get_odds not supported on Transfermarkt adapter — use OddsApiAdapter")
        return []


def _extract_clubs(raw: object) -> list[dict[str, object]]:
    """Extract clubs list from Transfermarkt response."""
    if isinstance(raw, dict):
        clubs = raw.get("clubs") or raw.get("results") or raw.get("data")
        if isinstance(clubs, list):
            return [item for item in clubs if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _extract_results(raw: object) -> list[dict[str, object]]:
    """Extract results list from Transfermarkt response."""
    if isinstance(raw, dict):
        results = raw.get("results") or raw.get("competitions") or raw.get("data")
        if isinstance(results, list):
            return [item for item in results if isinstance(item, dict)]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _parse_squad(item: dict[str, object]) -> TransfermarktTeamSquad | None:
    """Parse a club item into a TransfermarktTeamSquad."""
    try:
        team_id = item.get("id")
        if team_id is None:
            return None
        players_raw = item.get("players") or item.get("squad")
        players: list[TransfermarktPlayer] | None = None
        if isinstance(players_raw, list):
            players = []
            for p in players_raw:
                if not isinstance(p, dict):
                    continue
                players.append(
                    TransfermarktPlayer(
                        id=p.get("id"),
                        name=str(p.get("name", "") or p.get("playerName", "")),
                        position=str(p.get("position", "")) if p.get("position") else None,
                        nationality=str(p.get("nationality", "")) if p.get("nationality") else None,
                        market_value_eur=_safe_float(p.get("marketValue") or p.get("market_value_eur")),
                        market_value_currency="EUR",
                        club=str(item.get("name", "")),
                        age=_safe_int(p.get("age")),
                        contract_until=str(p.get("contractUntil", "")) if p.get("contractUntil") else None,
                        player_image_url=str(p.get("image", "")) if p.get("image") else None,
                    )
                )
        return TransfermarktTeamSquad(
            team_id=team_id,
            team_name=str(item.get("name", "")),
            squad_size=_safe_int(item.get("squadSize") or item.get("squad_size")),
            average_age=_safe_float(item.get("averageAge") or item.get("average_age")),
            foreigners_number=_safe_int(item.get("foreignersNumber") or item.get("foreigners_number")),
            total_market_value_eur=_safe_float(item.get("totalMarketValue") or item.get("total_market_value_eur")),
            players=players,
        )
    except Exception as exc:
        logger.warning("Failed to parse Transfermarkt squad: %s", exc)
        return None


def _safe_int(val: object) -> int | None:
    """Safely convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(str(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val: object) -> float | None:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return None
