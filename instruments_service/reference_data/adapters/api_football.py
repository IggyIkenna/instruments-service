"""API-Football reference data adapter — sports fixtures as instruments.

Fetches fixtures, teams, and leagues from api-football.com and surfaces
each fixture as an InstrumentRecord with instrument_type="sports_fixture".

Auth: service must fetch api_football_api_key from Secret Manager and pass
it via the ``api_key`` constructor parameter.
Base URL: https://v3.football.api-sports.io
Ref: https://www.api-football.com/documentation-v3
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.canonical.domain.sports.league_data import (  # noqa: qg-deep-import
    LEAGUE_REGISTRY,
)
from unified_api_contracts.external.api_football import (  # noqa: qg-deep-import
    ApiFootballFixture,
    ApiFootballFixtureStatus,
)
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._sports_normalizer import TeamNormalizer

logger = logging.getLogger(__name__)

_API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

# Map api-football numeric league ID → UAC canonical league_id string (e.g. 39 → "EPL")
# Built once at module load from LEAGUE_REGISTRY for O(1) lookup in _fixture_to_instrument.
_AF_ID_TO_CANONICAL: dict[int, str] = {
    league.api_football_id: league_key
    for league_key, league in LEAGUE_REGISTRY.items()
    if league.api_football_id is not None
}


def _classify_api_football_error(exc: Exception, status: int | None = None) -> str:
    """Map an API-Football HTTP/network error to a UAC error code."""
    msg = str(exc).lower()
    if status == 401 or "401" in msg or "authentication" in msg:
        return "INVALID_API_KEY"
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT_EXCEEDED"
    if "timeout" in msg:
        return "TIMEOUT_ERROR"
    if status == 403 or "403" in msg or "forbidden" in msg:
        return "FORBIDDEN"
    return "UNKNOWN"


class ApiFootballReferenceDataAdapter(BaseReferenceDataAdapter):
    """API-Football reference data adapter.

    Returns fixtures as InstrumentRecord instances with
    instrument_type="sports_fixture". Each fixture maps to one instrument:
      instrument_key = "api_football/{fixture_id}"
      venue          = "api_football"
      instrument_type = "sports_fixture"
      raw_symbol     = "{home_team} vs {away_team}"
      base_asset     = "football"

    Auth: service must fetch api_football_api_key from Secret Manager and
    pass it via ``api_key=`` constructor parameter.
    """

    def __init__(
        self,
        api_key: str | None = None,
        project_id: str | None = None,
        date: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._team_normalizer = TeamNormalizer()
        self._date = date  # ISO date string YYYY-MM-DD — used to fetch fixtures for a specific day

    @property
    def venue(self) -> str:
        return "API_FOOTBALL"  # canonical uppercase, matches CANONICAL_VENUE_TO_ADAPTER key

    def _require_api_key(self) -> str:
        """Return the injected API key or raise ValueError."""
        key = self._optional_api_key()
        if key is None:
            raise ValueError(
                "api_key required -- service must fetch api_football_api_key "
                "from Secret Manager and pass it via api_key= constructor parameter."
            )
        return key

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def parse_fixture(self, raw: dict[str, object]) -> ApiFootballFixture:
        """Parse raw fixture API response through api-contracts."""
        return ApiFootballFixture.model_validate(raw)

    def parse_fixture_status(self, raw: dict[str, object]) -> ApiFootballFixtureStatus:
        """Parse raw fixture status through api-contracts."""
        return ApiFootballFixtureStatus.model_validate(raw)

    def normalize_match_result(self, raw: dict[str, object]) -> dict[str, object]:
        """Parse through api-contracts and normalize to canonical match result format.

        Maps ApiFootballFixture fields:
        - fixture.id -> match_id
        - teams.home / teams.away -> team names + winner flag
        - goals.home / goals.away -> score
        - fixture.status -> match status
        """
        parsed = self.parse_fixture(raw)

        teams = parsed.teams or {}
        home_side = teams.get("home")
        away_side = teams.get("away")

        home_team = home_side.name if home_side and home_side.name else ""
        away_team = away_side.name if away_side and away_side.name else ""
        home_winner: bool | None = home_side.winner if home_side else None
        away_winner: bool | None = away_side.winner if away_side else None

        goals = parsed.goals or {}
        home_goals: int | None = goals.get("home")
        away_goals: int | None = goals.get("away")

        status_short = ""
        status_field = parsed.status
        if isinstance(status_field, ApiFootballFixtureStatus):
            status_short = status_field.short or ""
        elif isinstance(status_field, dict):
            status_short = str(status_field.get("short") or "")

        return {
            "venue": self.venue,
            "match_id": str(parsed.id or ""),
            "home_team": home_team,
            "away_team": away_team,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "home_winner": home_winner,
            "away_winner": away_winner,
            "status": status_short,
            "timestamp_utc": datetime.now(UTC).isoformat(),
        }

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    async def fetch_fixtures(
        self,
        date: str | None = None,
        league_id: int | None = None,
        season: int | None = None,
    ) -> list[dict[str, object]]:
        """Fetch fixtures from API-Football for all active prediction/reference leagues.

        When ``date`` is supplied (preferred), uses the /fixtures?date=YYYY-MM-DD
        endpoint which is efficient (one request for all leagues on that day).
        This is the standard path called by instruments-service.

        When ``league_id`` + ``season`` are supplied instead, fetches a specific
        league's season (legacy/single-league mode, still supported for direct callers).

        Leagues are sourced from UAC LEAGUE_REGISTRY (prediction + reference
        classifications) so only supported leagues are requested.

        Rate limit: 1 req/sec on free plans; batched by date here.
        """
        api_key = self._require_api_key()
        url = f"{_API_FOOTBALL_BASE_URL}/fixtures"
        headers = {"x-apisports-key": api_key}

        effective_date = date or self._date

        if effective_date:
            # Date-based fetch: returns ALL fixtures across ALL leagues for the day.
            # We then filter to only LEAGUE_REGISTRY leagues with api_football as data source.
            active_league_ids: set[int] = {
                league.api_football_id
                for league in LEAGUE_REGISTRY.values()
                if league.api_football_id is not None and "api_football" in league.data_sources
            }
            params: dict[str, str] = {"date": effective_date}
            raw_all = await self._fetch_url(url, params, headers)
            # Filter to only leagues we know about
            filtered = [item for item in raw_all if self._extract_league_id(item) in active_league_ids]
            logger.info(
                "API-Football date=%s: %d fixtures (filtered from %d total to %d known leagues)",
                effective_date,
                len(filtered),
                len(raw_all),
                len(active_league_ids),
            )
            return filtered

        # Legacy: single league/season fetch
        lid = league_id or 39
        yr = season or (datetime.now(UTC).year if datetime.now(UTC).month >= 8 else datetime.now(UTC).year - 1)
        params_legacy: dict[str, str] = {"league": str(lid), "season": str(yr)}
        return await self._fetch_url(url, params_legacy, headers)

    @staticmethod
    def _extract_league_id(fixture_item: dict[str, object]) -> int | None:
        """Extract API-Football league ID from a raw fixture response item."""
        try:
            league = fixture_item.get("league") or {}
            if isinstance(league, dict):
                lid = league.get("id")
                return int(str(lid)) if lid is not None else None
        except (ValueError, TypeError):
            pass
        return None

    async def _fetch_url(
        self,
        url: str,
        params: dict[str, str],
        headers: dict[str, str],
    ) -> list[dict[str, object]]:
        """Make one GET request and return the response list."""
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._get_with_retry(session, url, params=params, headers=headers)
            raw_dict = cast(dict[str, object], data)
            response = raw_dict.get("response") if isinstance(raw_dict, dict) else []
            return [item for item in (response or []) if isinstance(item, dict)]
        except Exception as exc:
            if isinstance(exc, (ValueError, TypeError, RuntimeError, KeyError)):
                raise
            status_code = getattr(exc, "status", None)
            error_code = _classify_api_football_error(exc, status_code)
            classification = classify_venue_error("api_football", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "API-Football request failed: %s (code: %s, action: %s, retry: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "api_football",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

    # ------------------------------------------------------------------
    # BaseReferenceDataAdapter interface
    # ------------------------------------------------------------------

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch fixtures and return as InstrumentRecord list.

        instrument_type filter: pass "sports_fixture" or None (all). Other
        values return an empty list since API-Football only exposes fixtures.
        """
        if instrument_type is not None and instrument_type != "sports_fixture":
            return []
        raw_fixtures = await self.fetch_fixtures(date=self._date)
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        for raw in raw_fixtures:
            record = self._fixture_to_instrument(raw, now)
            if record is not None:
                results.append(record)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Look up a single fixture by instrument_key or raw_symbol."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError(
            "API-Football does not provide options chains. Use get_instruments() to list available fixtures."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError(
            "API-Football does not provide expiry calendars. Fixture dates are embedded in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("API-Football does not have perpetual funding rates.")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("API-Football OHLCV is not available via reference data API.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fixture_to_instrument(
        self,
        raw: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Convert a raw API-Football fixture dict to an InstrumentRecord.

        Produces canonical instrument_key: {LEAGUE_ID}:FIXTURE:{HOME_ID}-v-{AWAY_ID}@{YYYYMMDD}
        Falls back to api_football/{fixture_id} if team or league resolution fails.
        """
        # API-Football response nests data: {fixture: {...}, league: {...}, teams: {...}}
        # Extract the inner fixture dict for ApiFootballFixture.model_validate()
        fixture_data = raw.get("fixture")
        if not isinstance(fixture_data, dict):
            return None
        fixture_id_raw = fixture_data.get("id")
        if fixture_id_raw is None:
            return None
        fixture_id = int(str(fixture_id_raw))

        teams_data = raw.get("teams") or {}
        home_side_raw = teams_data.get("home") if isinstance(teams_data, dict) else None
        away_side_raw = teams_data.get("away") if isinstance(teams_data, dict) else None
        home_team = str(home_side_raw.get("name", "")) if isinstance(home_side_raw, dict) else ""
        away_team = str(away_side_raw.get("name", "")) if isinstance(away_side_raw, dict) else ""

        raw_symbol = f"{home_team} vs {away_team}" if home_team and away_team else f"fixture_{fixture_id}"

        # Extract fixture date from the raw fixture dict
        fixture_date = self._parse_fixture_date_from_raw(fixture_data)

        # Canonical instrument_key: {LEAGUE_ID}:FIXTURE:{HOME_SLUG}-v-{AWAY_SLUG}@{YYYYMMDD}
        # e.g. EPL:FIXTURE:arsenal-v-chelsea@20260322
        # Build human-readable instrument_key using canonical ID builders.
        # Format: {LEAGUE}:{HOME}_v_{AWAY}:{YYYYMMDD}
        # Falls back to API_FOOTBALL:FIXTURE:{fixture_id} if data is incomplete.
        from unified_api_contracts.canonical.domain.sports.canonical_ids import (  # noqa: qg-deep-import
            build_fixture_id,
            build_league_id,
            build_team_id,
        )

        instrument_key = f"API_FOOTBALL:FIXTURE:{fixture_id}"
        if home_team and away_team and fixture_date:
            league_info = raw.get("league") or {}
            league_name = league_info.get("name", "") if isinstance(league_info, dict) else ""
            league_country = league_info.get("country", "") if isinstance(league_info, dict) else ""

            canonical_home = build_team_id(home_team)
            canonical_away = build_team_id(away_team)
            canonical_league = build_league_id(league_country, league_name) if league_name else "UNKNOWN"
            date_str = fixture_date.strftime("%Y%m%d")
            instrument_key = build_fixture_id(canonical_league, canonical_home, canonical_away, date_str)

        # available_since = start of fixture day (00:00 UTC);
        # available_to = end of fixture day (23:59:59 UTC).
        # The orchestrator date filter uses available_since ≤ date_dt ≤ available_to
        # where date_dt is midnight. Setting available_since to kickoff time would
        # exclude afternoon fixtures when date_dt is midnight.
        fixture_day_start = fixture_date.replace(hour=0, minute=0, second=0, microsecond=0) if fixture_date else None
        fixture_day_end = fixture_date.replace(hour=23, minute=59, second=59, microsecond=0) if fixture_date else None
        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            symbol=raw_symbol,
            raw_symbol=raw_symbol,
            instrument_type="sports_fixture",
            base_asset="football",
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            min_order_size=Decimal("1"),
            contract_size=Decimal("1"),
            settlement_asset="USD",
            expiry=fixture_date,
            available_since=fixture_day_start,
            available_to=fixture_day_end,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
        )

    def _parse_fixture_date(self, parsed: ApiFootballFixture) -> datetime | None:
        """Extract fixture date from parsed fixture. Returns None if unavailable."""
        date_str = parsed.date
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).astimezone(UTC)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_fixture_date_from_raw(fixture_data: dict[str, object]) -> datetime | None:
        """Extract fixture date from raw fixture dict. Returns None if unavailable."""
        date_str = fixture_data.get("date")
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).astimezone(UTC)
        except (ValueError, TypeError):
            return None
