"""Polygon.io reference data adapter — TradFi equities, options, futures.

Polygon.io provides reference data for US equities, options, indices, and
crypto tickers. This adapter returns instrument definitions as InstrumentRecord.

Auth: Authorization: Bearer {api_key} from Secret Manager (polygon-api-key).
Base URL: https://api.polygon.io

Supported endpoints:
  GET /v3/reference/tickers           — equity/ETF/index instrument list
  GET /v3/reference/options/contracts — US options chains
"""

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    PolygonOptionContract,
    PolygonOptionContractsResponse,
    PolygonTicker,
    PolygonTickersResponse,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_POLYGON_BASE = "https://api.polygon.io"
_PAGE_LIMIT = 1000
_MAX_PAGES = 50  # cap at 50k tickers


def _parse_expiry_date(s: str | None) -> datetime | None:
    """Parse YYYY-MM-DD expiration date string to UTC datetime (midnight)."""
    if not s:
        return None
    try:
        d = date.fromisoformat(s)
        return datetime(d.year, d.month, d.day, tzinfo=UTC)
    except ValueError:
        return None


class PolygonReferenceDataAdapter(BaseReferenceDataAdapter):
    """Polygon.io reference data adapter.

    Provides US equity and options instrument definitions.

    Instrument mapping:
      Equities/ETFs → instrument_type="SPOT_PAIR", base_asset=ticker, quote_asset="USD"
      Options       → instrument_type="OPTION", strike/expiry/option_type populated

    Auth: service must fetch polygon-api-key from Secret Manager and pass
    it via the ``api_key`` constructor parameter.
    """

    @property
    def venue(self) -> str:
        return "polygon"

    def _get_api_key(self) -> str:
        """Return the injected Polygon API key.

        Raises ValueError if no api_key was provided to the constructor.
        """
        key = self._optional_api_key()
        if key is None:
            raise ValueError(
                "api_key required — service must fetch polygon-api-key from Secret Manager and pass it in."
            )
        return key

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch Polygon.io tickers and return as InstrumentRecord list.

        instrument_type filter: "spot" fetches equity/ETF tickers.
        "option" fetches all active option contracts (very large — consider
        using get_options_chain() for a specific underlying instead).
        None returns spot + options.
        """
        api_key = self._get_api_key()
        results: list[InstrumentRecord] = []
        if instrument_type in (None, InstrumentType.SPOT_PAIR):
            results.extend(await self._fetch_tickers(api_key))
        if instrument_type in (None, InstrumentType.OPTION):
            results.extend(await self._fetch_options(api_key, underlying=None))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        api_key = self._get_api_key()
        headers = {"Authorization": f"Bearer {api_key}"}
        url = f"{_POLYGON_BASE}/v3/reference/tickers/{symbol.upper()}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                logger.error("Polygon ticker lookup failed for %s: %s", symbol, exc)
                return None
        raw_dict = cast(dict[str, object], raw_json)
        result_raw = raw_dict.get("results")
        if result_raw is None:
            return None
        ticker = PolygonTicker.model_validate(result_raw)
        return self._parse_ticker(ticker, datetime.now(UTC))

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        api_key = self._get_api_key()
        now = datetime.now(UTC)
        instruments = await self._fetch_options(api_key, underlying=underlying.upper())
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            if inst.option_type and inst.option_type.lower().startswith("c"):
                calls.append(inst)
            else:
                puts.append(inst)
        target_expiry = expiry or (calls[0].expiry if calls else now)
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=target_expiry or now,
            strikes=sorted(strikes),
            calls=calls,
            puts=puts,
            fetched_at=now,
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "OPTION",
    ) -> CanonicalExpiryCalendar:
        api_key = self._get_api_key()
        instruments = await self._fetch_options(api_key, underlying=underlying.upper())
        expiry_set: set[datetime] = set()
        for inst in instruments:
            if inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Polygon.io covers equities and options — no funding rates.")

    _POLYGON_TIMESPAN_MAP: ClassVar[dict[str, tuple[int, str]]] = {
        "1m": (1, "minute"),
        "5m": (5, "minute"),
        "15m": (15, "minute"),
        "30m": (30, "minute"),
        "1h": (1, "hour"),
        "4h": (4, "hour"),
        "1d": (1, "day"),
        "1w": (1, "week"),
        "1M": (1, "month"),
    }
    _TIMESPAN_SECONDS: ClassVar[dict[str, int]] = {
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
        "month": 2592000,
    }

    def _parse_polygon_bar(self, bar_raw: object, symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a single Polygon.io aggregate bar into OHLCVRef. Returns None if invalid."""
        if not isinstance(bar_raw, dict):
            return None
        bar: dict[str, object] = bar_raw
        ts_raw = bar.get("t")
        if ts_raw is None:
            return None
        ts = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(bar.get("o", "0") or "0")),
            high=Decimal(str(bar.get("h", "0") or "0")),
            low=Decimal(str(bar.get("l", "0") or "0")),
            close=Decimal(str(bar.get("c", "0") or "0")),
            volume=Decimal(str(bar.get("v", "0") or "0")),
            interval=interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Polygon.io Aggregates endpoint.

        Uses GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}.
        Requires API key from Secret Manager as polygon-api-key.
        """
        multiplier, timespan = self._POLYGON_TIMESPAN_MAP.get(interval, (1, "day"))
        api_key = self._get_api_key()
        now = datetime.now(UTC)
        bar_seconds = multiplier * self._TIMESPAN_SECONDS.get(timespan, 86400)
        from_ts = now.timestamp() - limit * bar_seconds
        from_date = datetime.fromtimestamp(from_ts, tz=UTC).strftime("%Y-%m-%d")
        to_date = now.strftime("%Y-%m-%d")
        url = f"{_POLYGON_BASE}/v2/aggs/ticker/{symbol.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        params = {"adjusted": "true", "sort": "asc", "limit": str(limit)}
        headers = {"Authorization": f"Bearer {api_key}"}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 403:
                        raise RuntimeError(
                            "Polygon.io authentication failed. Verify polygon-api-key in Secret Manager."
                        )
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                logger.error("Polygon OHLCV request failed for %s: %s", symbol, exc)
                return []
        raw_dict = cast(dict[str, object], raw_json)
        aggs_obj: object = raw_dict.get("results")
        aggs_raw: list[object] = aggs_obj if isinstance(aggs_obj, list) else []
        return [ref for bar_raw in aggs_raw if (ref := self._parse_polygon_bar(bar_raw, symbol, interval)) is not None]

    async def _fetch_tickers(self, api_key: str) -> list[InstrumentRecord]:
        """Fetch paginated equity/ETF tickers from /v3/reference/tickers."""
        headers = {"Authorization": f"Bearer {api_key}"}
        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        url: str | None = f"{_POLYGON_BASE}/v3/reference/tickers"
        params: dict[str, str] = {
            "active": "true",
            "market": "stocks",
            "limit": str(_PAGE_LIMIT),
        }
        pages = 0
        async with aiohttp.ClientSession() as session:
            while url and pages < _MAX_PAGES:
                try:
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 403:
                            raise RuntimeError(
                                "Polygon.io authentication failed. Verify polygon-api-key in Secret Manager."
                            )
                        resp.raise_for_status()
                        raw_json: object = cast(object, await resp.json())
                except aiohttp.ClientError as exc:
                    logger.error("Polygon tickers request failed: %s", exc)
                    break
                response = PolygonTickersResponse.model_validate(raw_json)
                for ticker in response.results or []:
                    record = self._parse_ticker(ticker, now)
                    if record is not None:
                        results.append(record)
                # Follow next_url for pagination; clear params (next_url includes them)
                url = response.next_url
                params = {}
                pages += 1
        return results

    async def _fetch_options(
        self,
        api_key: str,
        underlying: str | None,
    ) -> list[InstrumentRecord]:
        """Fetch paginated option contracts from /v3/reference/options/contracts."""
        headers = {"Authorization": f"Bearer {api_key}"}
        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        url: str | None = f"{_POLYGON_BASE}/v3/reference/options/contracts"
        params: dict[str, str] = {"expired": "false", "limit": str(_PAGE_LIMIT)}
        if underlying:
            params["underlying_ticker"] = underlying
        pages = 0
        async with aiohttp.ClientSession() as session:
            while url and pages < _MAX_PAGES:
                try:
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 403:
                            raise RuntimeError(
                                "Polygon.io authentication failed. Verify polygon-api-key in Secret Manager."
                            )
                        resp.raise_for_status()
                        raw_json_opt: object = cast(object, await resp.json())
                except aiohttp.ClientError as exc:
                    logger.error("Polygon options request failed: %s", exc)
                    break
                response_opt = PolygonOptionContractsResponse.model_validate(raw_json_opt)
                for contract in response_opt.results or []:
                    results.append(self._parse_option_contract(contract, now))
                url = response_opt.next_url
                params = {}
                pages += 1
        return results

    def _parse_option_contract(
        self,
        contract: PolygonOptionContract,
        now: datetime,
    ) -> InstrumentRecord:
        """Map a Polygon option contract to an InstrumentRecord."""
        underlying_ticker = contract.underlying_ticker or ""
        contract_type = contract.contract_type or "call"
        ticker_sym = contract.ticker or ""
        expiry = _parse_expiry_date(contract.expiration_date)
        strike = Decimal(str(contract.strike_price)) if contract.strike_price is not None else None
        return InstrumentRecord(
            instrument_key=ticker_sym,
            venue=self.venue,
            symbol=ticker_sym,
            raw_symbol=ticker_sym,
            instrument_type=InstrumentType.OPTION,
            base_asset=underlying_ticker,
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            min_size=Decimal("1"),
            min_order_size=Decimal("1"),
            contract_size=Decimal(str(contract.shares_per_contract or 100)),
            settlement_asset="USD",
            expiry=expiry,
            strike=strike,
            option_type=contract_type.lower(),
            underlying=underlying_ticker or None,
            is_active=True,
            updated_at=now,
        )

    def _parse_ticker(
        self,
        ticker: PolygonTicker,
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a PolygonTicker to an InstrumentRecord."""
        ticker_sym = ticker.ticker
        if not ticker_sym:
            return None
        name = ticker.name or ticker_sym
        is_active = bool(ticker.active)
        return InstrumentRecord(
            instrument_key=ticker_sym,
            venue=self.venue,
            symbol=ticker_sym,
            raw_symbol=ticker_sym,
            instrument_type=InstrumentType.SPOT_PAIR,
            base_asset=ticker_sym,
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            min_size=Decimal("1"),
            min_order_size=Decimal("1"),
            contract_size=Decimal("1"),
            settlement_asset="USD",
            expiry=None,
            strike=None,
            option_type=None,
            underlying=name,
            is_active=is_active,
            updated_at=now,
        )
