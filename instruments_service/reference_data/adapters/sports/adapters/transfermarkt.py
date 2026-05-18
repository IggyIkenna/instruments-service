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

import asyncio
import logging
from datetime import UTC, datetime

from unified_api_contracts.external.transfermarkt import (
    TransfermarktPlayer,
    TransfermarktTeamSquad,
)
from unified_api_contracts.external.transfermarkt.normalize import (
    normalize_transfermarkt_team_from_squad,
)
from unified_api_contracts.sports import (
    TRANSFERMARKT_IDS,
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)

from .base import BaseSportsReferenceAdapter

logger = logging.getLogger(__name__)

_RAPIDAPI_BASE_URL: str = "https://transfermarkt-football-data-api.p.rapidapi.com"
_RAPIDAPI_HOST: str = "transfermarkt-football-data-api.p.rapidapi.com"
_APIFY_BASE_URL: str = "https://api.apify.com/v2"
_APIFY_ACTOR_ID: str = "webdatalabs~transfermarkt-scraper"


class TransfermarktAdapter(BaseSportsReferenceAdapter):
    """Transfermarkt sports reference data adapter.

    Auto-detects backend from API key format:
    - ``apify_api_*`` → Apify actor scraping
    - Other → RapidAPI wrapper

    Secret Manager key: ``transfermarkt-api-key``
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
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
            "x-rapidapi-host": _RAPIDAPI_HOST,
        }

    async def _run_apify_actor(
        self,
        start_url: str,
        timeout_secs: int = 120,
        poll_interval: float = 10.0,
    ) -> list[dict[str, object]]:
        """Run Apify Transfermarkt scraper actor and return results.

        Uses a two-step approach (start + poll) to avoid chunked transfer
        encoding errors from long-lived ``waitForFinish`` connections.
        """
        run_url = f"{_APIFY_BASE_URL}/acts/{_APIFY_ACTOR_ID}/runs"
        actor_input = {
            "startUrls": [{"url": start_url}],
            "proxy": {"useApifyProxy": True},
        }
        async with self._make_session() as session:
            # Step 1: Start the actor run (no waitForFinish)
            async with session.post(
                run_url,
                json=actor_input,
                headers=self._headers(),
            ) as resp:
                if resp.status != 201:
                    body = await resp.text()
                    raise RuntimeError(f"Apify actor start failed: HTTP {resp.status} — {body[:200]}")
                run_data = await resp.json(content_type=None)

            run_info = run_data.get("data", {})
            run_id = run_info.get("id")
            dataset_id = run_info.get("defaultDatasetId")
            if not run_id:
                raise RuntimeError("Apify actor start returned no run ID")

            # Step 2: Poll for completion
            max_polls = int(timeout_secs / poll_interval)
            poll_url = f"{_APIFY_BASE_URL}/actor-runs/{run_id}"
            for _attempt in range(max_polls):
                await asyncio.sleep(poll_interval)
                async with session.get(poll_url, headers=self._headers()) as poll_resp:
                    if poll_resp.status != 200:
                        continue
                    poll_data = await poll_resp.json(content_type=None)
                    status = poll_data.get("data", {}).get("status", "")
                    if status == "SUCCEEDED":
                        break
                    if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                        msg = poll_data.get("data", {}).get("statusMessage", "")
                        raise RuntimeError(f"Apify actor {status}: {msg[:200]}")
            else:
                raise RuntimeError(f"Apify actor timed out after {timeout_secs}s")

            # Step 3: Fetch dataset
            if not dataset_id:
                return []
            dataset_url = f"{_APIFY_BASE_URL}/datasets/{dataset_id}/items"
            async with session.get(dataset_url, headers=self._headers()) as resp2:
                if resp2.status != 200:
                    raise RuntimeError(f"Apify dataset fetch failed: HTTP {resp2.status}")
                items = await resp2.json(content_type=None)
                return items if isinstance(items, list) else []

    async def _fetch_clubs_via_rapidapi(
        self,
        league_id: str,
        season: int,
    ) -> dict[str, list[dict[str, object]]]:
        """Fetch club data via RapidAPI Transfermarkt Football Data API.

        1. ``GET /api/v1/competitions/standings`` → club IDs + names
        2. ``GET /api/v1/clubs/profile`` per club → squad metadata

        Returns ``{"clubs": [...]}``, same shape as the Apify grouping helper.
        """
        headers = self._headers()
        standings_url = f"{_RAPIDAPI_BASE_URL}/api/v1/competitions/standings"
        params = {"id": league_id, "season": str(season)}

        async with self._make_session() as session:
            standings_raw = await self._get_with_retry(session, standings_url, params=params, headers=headers)

        # Extract club IDs from standings
        standings_data = (
            standings_raw.get("data", [])
            if isinstance(standings_raw, dict)
            else standings_raw
            if isinstance(standings_raw, list)
            else []
        )
        club_entries: list[dict[str, object]] = []
        for entry in standings_data:
            if not isinstance(entry, dict):
                continue
            club_id = entry.get("clubId")
            if club_id is None:
                continue
            club_entries.append(
                {
                    "id": str(club_id),
                    "name": str(entry.get("club", "")),
                }
            )

        # Fetch club profiles for squad metadata (rate-limited)
        async with self._make_session() as session:
            for club in club_entries:
                try:
                    profile_url = f"{_RAPIDAPI_BASE_URL}/api/v1/clubs/profile"
                    profile_raw = await self._get_with_retry(
                        session, profile_url, params={"id": str(club["id"])}, headers=headers
                    )
                    profile_data = profile_raw.get("data", {}) if isinstance(profile_raw, dict) else {}
                    if isinstance(profile_data, dict):
                        club["squadSize"] = profile_data.get("squadSize")
                        club["averageAge"] = profile_data.get("averageAge")
                        club["totalMarketValue"] = profile_data.get("totalMarketValue")
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch Transfermarkt profile for club %s: %s",
                        club.get("id"),
                        exc,
                    )

        logger.info(
            "RapidAPI: fetched %d clubs for league %s season %d",
            len(club_entries),
            league_id,
            season,
        )
        return {"clubs": club_entries}

    async def get_teams(self, league_id: int | str, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams with player value data for a league from Transfermarkt.

        Supports two backends:
        - **Apify**: Scrapes the Transfermarkt competition page via Apify actor.
        - **RapidAPI**: Uses ``/competitions/{id}/clubs`` on the felipeall wrapper.

        Args:
            league_id: Transfermarkt competition code (e.g. ``GB1``, ``ES1``).
            season: Optional season year. Defaults to current year.

        Returns:
            List of canonical teams with squad/value data.
        """
        effective_season = season if season is not None else datetime.now(UTC).year

        try:
            if self._is_apify:
                # Apify actor returns player profiles, not club squads.
                # Group by currentClubId to build partial squad data.
                start_url = (
                    f"https://www.transfermarkt.com/-/startseite/wettbewerb/{league_id}"
                    f"/plus/?saison_id={effective_season}"
                )
                player_items = await self._run_apify_actor(start_url)
                raw_response: object = _group_apify_players_into_clubs(player_items)
            else:
                # RapidAPI path: get standings first (returns club IDs),
                # then fetch each club's profile for squad metadata.
                raw_response = await self._fetch_clubs_via_rapidapi(
                    league_id=str(league_id),
                    season=effective_season,
                )
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
            "Fetched %d teams for league=%s season=%d",
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
        """Return Transfermarkt competitions from the UAC SSOT.

        Transfermarkt's RapidAPI wrapper has no "list all competitions"
        endpoint (only search-by-name), so we build the list from the
        ``TRANSFERMARKT_IDS`` mapping in UAC which contains all 33 mapped
        prediction leagues with stable competition codes (e.g. GB1, ES1).

        Returns:
            List of canonical leagues keyed by Transfermarkt competition code.
        """
        leagues: list[CanonicalLeague] = []
        for canonical_name, tm_code in TRANSFERMARKT_IDS.items():
            leagues.append(
                CanonicalLeague(
                    league_id=tm_code,
                    name=canonical_name.replace("_", " ").title(),
                    country="",
                    league_type=None,
                    logo_url=None,
                )
            )
        logger.info("Transfermarkt leagues from SSOT: %d entries", len(leagues))
        return leagues

    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Transfermarkt does not provide odds data.

        This adapter is for transfer/player data only. Returns an empty list.
        """
        logger.info("get_odds not supported on Transfermarkt adapter")
        return []


def _group_apify_players_into_clubs(
    player_items: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Group Apify player-profile results into club-level entries.

    The ``webdatalabs~transfermarkt-scraper`` actor returns per-player data
    with ``currentClubId`` / ``currentClub``.  We reshape into the same
    ``{"clubs": [...]}`` structure that the RapidAPI path returns so the
    downstream ``_extract_clubs`` / ``_parse_squad`` helpers work unchanged.
    """
    clubs: dict[str, dict[str, object]] = {}
    for player in player_items:
        club_id = player.get("currentClubId")
        if club_id is None:
            continue
        club_id_str = str(club_id)
        if club_id_str not in clubs:
            clubs[club_id_str] = {
                "id": club_id_str,
                "name": str(player.get("currentClub", "")),
                "players": [],
            }
        club_entry = clubs[club_id_str]
        players_list = club_entry.get("players")
        if isinstance(players_list, list):
            players_list.append(
                {
                    "id": player.get("playerId"),
                    "name": player.get("playerName", ""),
                    "position": player.get("position", ""),
                    "nationality": player.get("nationality", ""),
                    "marketValue": player.get("marketValue"),
                    "market_value_eur": player.get("marketValue"),
                    # Do NOT default valuation_date to wall-clock-now: propagating
                    # today's date onto historical backfill rows creates a §5
                    # data-crime (writing today's value onto a 2018 fixture).
                    # Emit raw API field; downstream as-of joins handle missing.
                    "valuation_date": player.get("valuation_date"),
                    "age": player.get("age"),
                    "contractUntil": player.get("contractExpiry"),
                    "image": player.get("profileUrl"),
                }
            )
    if clubs:
        logger.info(
            "Apify: grouped %d players into %d clubs (partial data)",
            len(player_items),
            len(clubs),
        )
    return {"clubs": list(clubs.values())}


def _extract_clubs(raw: object) -> list[dict[str, object]]:
    """Extract clubs list from Transfermarkt response."""
    if isinstance(raw, dict):
        clubs = raw.get("clubs") or raw.get("results") or raw.get("data")
        if isinstance(clubs, list):
            return [item for item in clubs if isinstance(item, dict)]
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
                        market_value_eur=_parse_market_value(p.get("marketValue") or p.get("market_value_eur")),
                        market_value_currency="EUR",
                        # `valuation_date` propagates the API-provided value when
                        # present; None when absent. NEVER stamp wall-clock-now —
                        # for historical backfill, today's date on a 2023 row
                        # would be a §5 data-crime (downstream as-of join would
                        # treat 2026-04-22 value as in-scope for a 2023-03-16
                        # fixture). Let downstream NULL-propagation handle absent.
                        valuation_date=str(p.get("valuation_date")) if p.get("valuation_date") else None,
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
            total_market_value_eur=_parse_market_value(
                item.get("totalMarketValue") or item.get("total_market_value_eur")
            ),
            players=players,
        )
    except Exception as exc:
        logger.warning("Failed to parse Transfermarkt squad: %s", exc)
        return None


def _parse_market_value(val: object) -> float | None:
    """Parse Transfermarkt market value strings like '€1.23bn' or '€35.00m'.

    Also handles raw numeric values passed through from other backends.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower().replace(",", "")
    # Strip currency symbol and trailing text
    for prefix in ("€", "$", "£"):
        s = s.replace(prefix, "")
    # Remove trailing descriptive text (e.g. "total market value")
    for suffix in ("total market value", "market value"):
        s = s.replace(suffix, "")
    s = s.strip()
    if not s:
        return None
    multiplier = 1.0
    if s.endswith("bn"):
        multiplier = 1_000_000_000
        s = s[:-2].strip()
    elif s.endswith("m"):
        multiplier = 1_000_000
        s = s[:-1].strip()
    elif s.endswith("k"):
        multiplier = 1_000
        s = s[:-1].strip()
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
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
