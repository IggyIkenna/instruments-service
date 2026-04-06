"""API-Football URDI adapter — sports fixtures as InstrumentRecords.

Thin shim: delegates fixture fetching to the sports-domain
``ApiFootballAdapter`` (adapters/sports/adapters/api_football.py) and
wraps ``CanonicalFixture`` → ``InstrumentRecord``.

Auth: service must fetch api_football_api_key from Secret Manager and pass
it via the ``api_key`` constructor parameter.
"""

import contextlib
import logging
from datetime import datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.sports import CanonicalFixture

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from .sports.adapters.api_football import ApiFootballAdapter

# Module-level cache: populated during get_instruments(), consumed by orchestrator's
# _fetch_sports_reference_data() to avoid 33-league re-fetch (saves 33 API calls/date).
_last_completed_fixture_ids: list[int] = []

logger = logging.getLogger(__name__)


class ApiFootballReferenceDataAdapter(BaseReferenceDataAdapter):
    """API-Football URDI shim.

    Wraps the sports-domain ApiFootballAdapter, converting CanonicalFixture
    objects to InstrumentRecord for the instrument reference-data pipeline.

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
        self._date = date
        self._sports_adapter = ApiFootballAdapter(api_key=api_key)
        # Populated during get_instruments() — raw API Football numeric IDs of
        # completed fixtures (FT/AET/PEN). Avoids 33-league re-fetch in
        # _fetch_sports_reference_data (saves 33 API calls per date).
        self.completed_fixture_ids: list[int] = []

    @property
    def venue(self) -> str:
        return "API_FOOTBALL"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch fixtures and return as InstrumentRecord list.

        instrument_type filter: pass "FIXED_ODDS" or None (all). Other
        values return an empty list since API-Football only exposes fixtures.
        """
        if instrument_type is not None and instrument_type != "FIXED_ODDS":
            return []
        if not self._date:
            logger.warning("API-Football URDI adapter: no date set — returning empty list")
            return []
        global _last_completed_fixture_ids
        fixtures = await self._sports_adapter.get_fixtures(date=self._date)
        results: list[InstrumentRecord] = []
        completed_statuses = {"FT", "AET", "PEN"}
        completed_ids: list[int] = []
        for fixture in fixtures:
            # Capture completed fixture IDs for downstream per-fixture enrichment
            if fixture.status in completed_statuses and fixture.source_fixture_id:
                with contextlib.suppress(ValueError, TypeError):
                    completed_ids.append(int(fixture.source_fixture_id))
            record = _canonical_fixture_to_instrument(fixture)
            if record is not None:
                results.append(record)
        self.completed_fixture_ids = completed_ids
        # Mutate in-place so importers who did `from ... import _last_completed_fixture_ids`
        # see the update (Python rebinding semantics: assignment creates a new object,
        # but the importer's reference still points to the old one).
        _last_completed_fixture_ids.clear()
        _last_completed_fixture_ids.extend(completed_ids)
        logger.info(
            "API-Football date=%s: %d InstrumentRecords (%d completed)",
            self._date,
            len(results),
            len(completed_ids),
        )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Look up a single fixture by instrument_key or raw_symbol."""
        for inst in await self.get_instruments():
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
        instrument_type: str = "FUTURE",
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


def _canonical_fixture_to_instrument(
    fixture: CanonicalFixture,
) -> InstrumentRecord | None:
    """Convert a CanonicalFixture from the sports adapter to an InstrumentRecord."""
    from unified_api_contracts.canonical.domain.sports.canonical_ids import (
        build_fixture_id,
        build_league_id,
        build_team_id,
    )

    home_name = fixture.home_team.name if fixture.home_team and fixture.home_team.name else ""
    away_name = fixture.away_team.name if fixture.away_team and fixture.away_team.name else ""
    raw_symbol = f"{home_name} vs {away_name}" if home_name and away_name else f"fixture_{fixture.fixture_id}"

    kickoff = fixture.kickoff_utc
    instrument_key = f"API_FOOTBALL:FIXTURE:{fixture.fixture_id}"
    if home_name and away_name and kickoff:
        league_name = fixture.league.name if fixture.league and fixture.league.name else ""
        league_country = fixture.league.country if fixture.league and fixture.league.country else ""
        canonical_home = build_team_id(home_name)
        canonical_away = build_team_id(away_name)
        canonical_league = build_league_id(league_country, league_name) if league_name else "UNKNOWN"
        date_str = kickoff.strftime("%Y%m%d")
        instrument_key = build_fixture_id(canonical_league, canonical_home, canonical_away, date_str)

    day_start = kickoff.replace(hour=0, minute=0, second=0, microsecond=0) if kickoff else None
    day_end = kickoff.replace(hour=23, minute=59, second=59, microsecond=0) if kickoff else None
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue="API_FOOTBALL",
        raw_symbol=raw_symbol,
        instrument_type=InstrumentType.FIXED_ODDS,
        base_asset="football",
        quote_asset="USD",
        status=InstrumentStatus.ACTIVE,
        tick_size=Decimal("0.01"),
        min_size=Decimal("1"),
        contract_size=Decimal("1"),
        expiry=kickoff,
        available_from_datetime=day_start,
        available_to_datetime=day_end,
        strike=None,
        option_type=None,
    )
