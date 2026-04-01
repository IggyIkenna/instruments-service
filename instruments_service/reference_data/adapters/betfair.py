"""Betfair reference data adapter — sports event market catalogue.

Betfair's listMarketCatalogue JSON-RPC endpoint returns available markets
and their runners (selections). This adapter surfaces each runner as an
InstrumentRecord so downstream strategy services treat sports bets as
tradeable instruments.

Auth: session token from Secret Manager (betfair-session-token).
      App key from Secret Manager (betfair-app-key).
Base URL: https://api.betfair.com/exchange/betting/json-rpc/v1
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import (
    BetfairMarketCatalogue,
    BetfairRunnerCatalog,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BETFAIR_RPC_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
_MAX_RESULTS = 1000


def _classify_betfair_error(exc: Exception, status: int | None = None) -> str:
    """Map a Betfair HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 401 or "401" in msg or "authentication" in msg or "session" in msg:
        return "INVALID_SESSION_INFORMATION"
    if "too much data" in msg:
        return "TOO_MUCH_DATA"
    if "service busy" in msg or "busy" in msg:
        return "SERVICE_BUSY"
    if "timeout" in msg:
        return "TIMEOUT_ERROR"
    if "request size" in msg:
        return "REQUEST_SIZE_EXCEEDS_LIMIT"
    if "no session" in msg:
        return "NO_SESSION"
    return "UNKNOWN"


class BetfairReferenceDataAdapter(BaseReferenceDataAdapter):
    """Betfair reference data adapter.

    Returns available markets and runners as InstrumentRecord instances.
    Each runner (selection) within a market becomes one InstrumentRecord:
      instrument_key = "{marketId}/{selectionId}"
      venue          = "betfair"
      instrument_type = "sports_event"
      raw_symbol     = "{eventName}/{runnerName}"
      base_asset     = sport name (e.g. "Soccer")

    Auth: service must fetch betfair-session-token and betfair-app-key from
    Secret Manager and pass them via ``session_token`` and ``app_key``
    constructor parameters.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        session_token: str | None = None,
        app_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._session_token = session_token
        self._app_key = app_key

    @property
    def venue(self) -> str:
        return "betfair"

    def _get_credentials(self) -> tuple[str, str]:
        """Return (session_token, app_key) injected at construction time.

        Raises ValueError if credentials were not provided.
        """
        if self._session_token is None or self._app_key is None:
            raise ValueError(
                "api_key required — service must fetch betfair-session-token "
                "and betfair-app-key from Secret Manager and pass them in "
                "via session_token= and app_key= constructor parameters."
            )
        return self._session_token, self._app_key

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch available Betfair markets and return as InstrumentRecord list.

        instrument_type filter: pass "EXCHANGE_ODDS" or None (all). Other values
        return an empty list since Betfair only exposes sports markets.
        """
        if instrument_type is not None and instrument_type != "EXCHANGE_ODDS":
            return []
        session_token, app_key = self._get_credentials()
        raw_list = await self._fetch_markets_raw(session_token, app_key)
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        for raw_item in raw_list:
            catalogue = BetfairMarketCatalogue.model_validate(raw_item)
            records = self._parse_catalogue_item(catalogue, now)
            results.extend(records)
        return results

    async def _fetch_markets_raw(
        self,
        session_token: str,
        app_key: str,
    ) -> list[object]:
        """Post listMarketCatalogue RPC and return the raw result list."""
        headers = {
            "X-Authentication": session_token,
            "X-Application": app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {"inPlayOnly": False},
                "marketProjection": [
                    "EVENT_TYPE",
                    "EVENT",
                    "COMPETITION",
                    "RUNNER_DESCRIPTION",
                    "MARKET_START_TIME",
                ],
                "maxResults": str(_MAX_RESULTS),
                "sort": "FIRST_TO_START",
            },
            "id": 1,
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(_BETFAIR_RPC_URL, json=payload, headers=headers) as resp:
                    if resp.status == 401:
                        raise RuntimeError(
                            "Betfair authentication failed. Refresh betfair-session-token in Secret Manager."
                        )
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_betfair_error(exc)
                classification = classify_venue_error("betfair", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Betfair listMarketCatalogue failed: %s (classified: %s, action: %s)",
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "betfair",
                        "endpoint": "listMarketCatalogue",
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                return []
        raw_dict = cast(dict[str, object], raw_json)
        result_raw = raw_dict.get("result")
        return cast(list[object], result_raw) if result_raw is not None else []

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
            "Betfair does not provide options chains. Use get_instruments() to list available markets/runners."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError(
            "Betfair does not provide expiry calendars in the TradFi sense. "
            "Market start times are embedded in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Betfair does not have perpetual funding rates.")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Betfair OHLCV is not available via reference data API.")

    def _parse_catalogue_item(
        self,
        catalogue: BetfairMarketCatalogue,
        now: datetime,
    ) -> list[InstrumentRecord]:
        """Convert a BetfairMarketCatalogue item into per-runner InstrumentRecords."""
        market_id = catalogue.market_id or ""
        market_name = catalogue.market_name or ""
        runners: list[BetfairRunnerCatalog] = catalogue.runners or []
        sport_name = catalogue.event_type.name or "" if catalogue.event_type else ""
        event_name = catalogue.event.name or "" if catalogue.event else ""
        market_expiry = self._parse_market_start_time(catalogue.market_start_time)
        return [
            self._build_runner_record(runner, market_id, market_name, sport_name, event_name, market_expiry, now)
            for runner in runners
            if runner.selection_id is not None
        ]

    def _parse_market_start_time(self, start_time_raw: str | None) -> datetime | None:
        """Parse market start time ISO string to UTC datetime."""
        if not start_time_raw:
            return None
        try:
            return datetime.fromisoformat(start_time_raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    def _build_runner_record(
        self,
        runner: BetfairRunnerCatalog,
        market_id: str,
        market_name: str,
        sport_name: str,
        event_name: str,
        market_expiry: datetime | None,
        now: datetime,
    ) -> InstrumentRecord:
        """Build a single InstrumentRecord for a Betfair runner."""
        selection_id = runner.selection_id
        runner_name = runner.runner_name or f"Selection_{selection_id}"
        instrument_key = f"{market_id}/{selection_id}"
        raw_symbol = f"{event_name}/{runner_name}" if event_name else runner_name
        display_symbol = f"{market_name}/{runner_name}" if market_name else runner_name
        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            symbol=display_symbol,
            raw_symbol=raw_symbol,
            instrument_type=InstrumentType.EXCHANGE_ODDS,
            base_asset=sport_name,
            quote_asset="GBP",
            tick_size=Decimal("0.01"),
            min_size=Decimal("1"),
            min_order_size=Decimal("2"),
            contract_size=Decimal("1"),
            settlement_asset="GBP",
            expiry=market_expiry,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
        )
